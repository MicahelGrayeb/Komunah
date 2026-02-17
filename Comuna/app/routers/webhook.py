import os
import re
import locale
import json
import logging
import requests
import uuid
import time
from typing import Any, Dict, Optional, List
from urllib.parse import quote
from pydantic import BaseModel
from fastapi import APIRouter, Request, HTTPException, BackgroundTasks
from ..database import SessionLocal
from sqlalchemy import and_
from ..models import Cartera, Venta, Amortizacion
from dotenv import load_dotenv
from datetime import datetime
import vertexai
from vertexai.generative_models import GenerativeModel, Part
from google.cloud import firestore

try:
    locale.setlocale(locale.LC_TIME, "es_ES.UTF-8")
except:
    pass

load_dotenv()
PROCESSED_EVENTS = set()
session = requests.Session()
db_firestore = firestore.Client(project=os.getenv("FIREBASE_PLANTILLAS_PROJECT_ID"))
# --- Configuración de Vertex AI ---
# Se requiere que la variable de entorno GOOGLE_APPLICATION_CREDENTIALS apunte al JSON de la cuenta de servicio
# o que el entorno esté autenticado mediante gcloud.
PROJECT_ID = os.getenv("VERTEX_PROJECT_ID")
LOCATION = os.getenv("VERTEX_LOCATION", "us-central1")

if PROJECT_ID:
    vertexai.init(project=PROJECT_ID, location=LOCATION)
    # Usamos gemini-1.5-flash por su velocidad y eficiencia en tareas de extracción
    model = GenerativeModel("gemini-2.5-flash")
else:
    model = None
    print("⚠️ VERTEX_PROJECT_ID no configurado en .env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/webhook", tags=["Webhook Respond.io (Vertex AI)"])

# --- Modelos ---

class StatusUpdate(BaseModel):
    status: str

class LotesRequest(BaseModel):
    telefono: str
    lotestemporales: Optional[str] = None
    
class VerificarBD(BaseModel):
    telefono: str
    loteseleccionado: Optional[str] = None

class LotesTemporalesRequest(BaseModel):
    lotestemporales: str = ""
    loteseleccionado: str = ""
    
# --- HELPERS ---

def obtener_fecha_espanol():
    meses = {
        1: "enero", 2: "febrero", 3: "marzo", 4: "abril", 5: "mayo", 6: "junio",
        7: "julio", 8: "agosto", 9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre"
    }
    ahora = datetime.now()
    # Construimos el string manualmente
    dia = ahora.day
    mes = meses[ahora.month]
    anio = ahora.year
    hora = ahora.strftime("%H:%M")
    
    return f"{dia} de {mes} de {anio} {hora}"

def enviar_whatsapp(numero: str, texto: str):
    """Envía un mensaje de texto vía Respond.io API v2."""
    token = os.getenv("RESPOND_IO_TOKEN")
    channel_id = os.getenv("RESPOND_IO_CHANNEL_ID")

    if not token or not channel_id:
        logger.error(f"❌ Error: Faltan variables de entorno para Respond.io. TOKEN: {bool(token)}, CHANNEL_ID: {bool(channel_id)}")
        return None

    identifier = quote(f"phone:{numero}")
    url = f"https://api.respond.io/v2/contact/{identifier}/message"

    payload = {
        "channelId": int(channel_id),
        "message": {
            "type": "text",
            "text": texto
        }
    }

    try:
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        response = requests.post(url, headers=headers, json=payload, timeout=15)
        response.raise_for_status()
        logger.info(f"Mensaje enviado a {numero}")
        return response
    except Exception as e:
        logger.error(f"Error al enviar WhatsApp: {e}")

def analizar_comprobante(url_imagen: str) -> Dict[str, Any]:
    """Descarga la imagen y usa Vertex AI para extraer datos estructurados."""
    if not model:
        return {"error": "Vertex AI no inicializado. Verifique VERTEX_PROJECT_ID."}

    try:
        logger.info(">>> DEBUG: Iniciando descarga de la imagen...")
        response = requests.get(url_imagen, timeout=15)
        response.raise_for_status()
        logger.info(f">>> DEBUG: Descarga finalizada. Tamaño: {len(response.content)} bytes")

        image_part = Part.from_data(
            data=response.content, 
            mime_type=response.headers.get("Content-Type", "image/jpeg")
        )
        
        logger.info(">>> DEBUG: Definiendo prompt...")
        prompt = """
            Analiza la imagen del comprobante y extrae únicamente los siguientes campos en un JSON plano:

            1. "tipo_operacion": El tipo de operación realizada.
            2. "folio": El folio de la operación.
            3. "fecha_hora": La fecha y hora concatenadas en un solo string.
            4. "beneficiario": Nombre del beneficiario.
            5. "concepto": Concepto o motivo del pago.
            6. "importe": El monto total.

            Reglas:
            - En el campo "concepto", asegúrate de incluir el símbolo "-" (guion) que separa los elementos; no lo omitas ni lo reemplaces por espacios.
            - Si el lote aparece con errores de lectura (ej. sin espacios), intenta reconstruir el formato estándar.
            - Si un dato de texto no aparece, el valor debe ser "Dato no encontrado".
            - Si el importe no aparece, el valor debe ser "$0.00".
            - Responde solo con el JSON, sin bloques de código markdown.
        """

# - IMPORTANTE: Identifica si dentro del "concepto" aparece un folio de 4 números. Si no aparece, devuelve el JSON con un campo extra: {"error": "No se encontró un folio válido en el concepto"}.

        logger.info(">>> DEBUG: Llamada a Vertex AI...")
        ai_response = model.generate_content([image_part, prompt])
        
        # Limpieza de formato markdown si el modelo lo incluye
        raw_text = ai_response.text.strip()
        if raw_text.startswith("```json"):
            raw_text = raw_text[7:-3].strip()
        logger.info(f">>> DEBUG: Respuesta Vertex AI: {raw_text}")
        
        return json.loads(raw_text)

    except Exception as e:
        logger.error(f"Error en análisis de imagen: {e}")
        return {"error": "Error técnico al procesar la imagen."}
    
    
# --- FIREBASE REST HELPERS ---

def guardar_comprobante_firebase(datos: Dict[str, Any]) -> bool:
    """Guarda en Firestore usando el SDK oficial (gRPC) para evitar errores SSL."""
    try:
        # El SDK gestiona internamente los reintentos y la estabilidad de la conexión
        doc_ref = db_firestore.collection("ComprobantePago").document()
        doc_ref.set(datos)
        return True
    except Exception as e:
        logger.error(f"❌ Error al guardar en Firestore SDK: {e}")
        
        # Notificación de error solicitada
        telefono_contacto = datos.get("Contacto", {}).get("Telefono")
        if telefono_contacto:
            mensaje_error = (
                "🤖 Hubo un error al guardar el comprobante en la base de datos. "
                "Por favor, espere a que termine el tiempo de espera. "
                "Gracias por su paciencia."
            )
            enviar_whatsapp(telefono_contacto, mensaje_error)
        return False
    
def obtener_contacto_completo(contact_id: int) -> Dict[str, Any]:
    """Consulta la API de Respond.io usando el ID interno como identificador."""
    token = os.getenv("RESPOND_IO_TOKEN")
    if not token or not contact_id:
        return {}

    # El secreto es usar el prefijo 'id:' tal como pide la documentación v2
    identifier = quote(f"id:{contact_id}")
    url = f"https://api.respond.io/v2/contact/{identifier}"
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    try:
        response = requests.get(url, headers=headers, timeout=10)
        # Si esto lanza 404 o 400, caerá al except
        response.raise_for_status()
        data = response.json()
        
        # --- LOGS DE DEBUG ---
        # 1. Ver qué llaves principales tiene el objeto (¿Es 'customFields' o 'custom_fields'?)
        logger.info(f">>> Llaves recibidas del contacto {contact_id}: {list(data.keys())}")
        
        # 2. Ver específicamente qué hay en los campos personalizados
        cf_key = "customFields" if "customFields" in data else "custom_fields"
        logger.info(f">>> Contenido de {cf_key}: {data.get(cf_key)}")
        
        return data
    except Exception as e:
        logger.error(f"❌ Error al obtener contacto por ID {contact_id}: {e}")
        return {}

def procesar_evento_background(evento: Dict[str, Any]):
    try:
        # --- 1. DATOS DEL CONTACTO Y RESPOND.IO ---
        contact = (evento.get("contact") or {})
        contact_id = contact.get("id")
        telefono_contacto = contact.get("phone")
        lote_Seleccionado = contact.get("lote_seleccionado")
        
        full_contact = obtener_contacto_completo(contact_id) if contact_id else {}
        raw_fields = (full_contact.get("custom_fields") or [])
        fields_dict = {f['name']: f['value'] for f in raw_fields if 'name' in f}

        tiene_varios_folios = str(fields_dict.get("tiene_folios")) == 1 
        # Cambiar "tiene_folios" por "varios_folios" y el comparador a "true" para la verssion automatizada del workflow

        message_wrapper = (evento.get("message") or {})
        inner_message = (message_wrapper.get("message") or {})
        
        target_attachment = None
        attachment = inner_message.get("attachment")
        attachments = (inner_message.get("attachments") or [])
        if attachment and attachment.get("type") == "image":
            target_attachment = attachment
        elif attachments:
            target_attachment = next((a for a in attachments if a.get("type") == "image"), None)

        if not target_attachment:
            return

        # --- 2. ANÁLISIS OCR ---
        url_imagen = target_attachment.get("url")
        resultado_ocr = analizar_comprobante(url_imagen)
        if "error" in resultado_ocr: return

        # Limpiamos el texto del OCR para evitar espacios extraños al inicio o fin
        concepto_ocr = resultado_ocr.get("concepto", "").strip()
        
       # \d{1,4} -> 1 a 4 dígitos iniciales
       # # \s?[A-Z]{1,2} -> 1 a 2 letras opcionales (ej: G)
       # # [\s-]* -> Cualquier combinación de espacios o guiones (o nada)
       # # [A-Z]{0,4} -> 0 a 4 letras del sub-lote (ej: CM, SUB)
       # # \s?\d{0,4} -> 0 a 4 dígitos finales
        regex_lote = r'\b\d{1,4}\s?[A-Z]{1,2}[\s-]*[A-Z]{0,4}\s?\d{0,4}\b'
        
        lote_match = re.search(regex_lote, concepto_ocr, re.IGNORECASE)
        logger.info(f">>> DEBUG: Lote match para búsqueda: '{lote_match}'")
        lote_final = (lote_match.group().strip() if lote_match else (concepto_ocr if (concepto_ocr and "encontrado" not in concepto_ocr.lower()) else lote_Seleccionado))
        logger.info(f">>> DEBUG: Lote final para búsqueda: '{lote_final}'")

        # --- 3. CONSULTA SQL CON BÚSQUEDA FLEXIBLE ---
        datos_sincronizados = {}

        with SessionLocal() as db:
            # USAMOS ilike con %lote% para permitir coincidencias parciales y no distinguir Mayús/Minús
            registro_venta = db.query(Venta).filter(
                and_(
                    Venta.numero.ilike(f"%{lote_final}%"),
                    Venta.estado_expediente.notin_(['Cancelado', 'Expirado'])
                )
            ).first()
            
            logger.info(f">>> DEBUG: Respuesta sql: {registro_venta}")
          
            if registro_venta:
                folio_db = registro_venta.folio
                nombre_cliente = registro_venta.cliente
                lote_cliente = registro_venta.numero
                
                # Guardamos datos limpios de la BD para Firebase
                datos_sincronizados = {
                    "folio": folio_db,
                    "cliente": nombre_cliente,
                    "lote": lote_cliente,
                    "proyecto": registro_venta.desarrollo,
                    "etapa": registro_venta.etapa
                }
            else:
                logger.warning(f"Lote '{lote_final}' no válido o cancelado en Cartera.")
                return

        # --- 5. GUARDADO EN FIREBASE ---
        id_comprobante = str(uuid.uuid4())
        channel_obj = (evento.get("channel") or {})
        
        datos_guardado = {
            "IDCanal": channel_obj.get("id"), 
            "IDEvento": evento.get("event_id"),
            "IDComprobante": id_comprobante,
            "IDMensaje": str(message_wrapper.get("messageId")),
            "IDCanalMensaje": message_wrapper.get("channelId"),
            "Status": "Pendiente por validar",
            "Contacto": {
                "IDContacto": contact_id,
                "NombreContacto": f"{contact.get('firstName', '')} {contact.get('lastName', '')}".strip(),
                "Telefono": telefono_contacto,
                "Lote": datos_sincronizados.get("lote"),
                "FolioExpediente": datos_sincronizados.get("folio"),
                "NombreReporte": datos_sincronizados.get("cliente")
            },
            "Ocr_analisis": resultado_ocr,
            "Sql_data_extra": {
                "EtapaActiva": datos_sincronizados.get("etapa"),
                "Proyecto": datos_sincronizados.get("proyecto")
            },
            "Archivo": target_attachment.get("fileName") or "Comprobante.jpg",
            "Url": url_imagen,
            "Fecha": obtener_fecha_espanol() 
        }

        # --- 6. NOTIFICACIÓN FINAL ---
        if guardar_comprobante_firebase(datos_guardado):
            if tiene_varios_folios:
                mensaje_final = (
                    "🤖 Su pago ha sido registrado y está a la espera de la revisión por un asesor. "
                    "Serás notificado por este medio en un plazo de 24 a 72 horas (Dias habiles). Gracias por su confianza. \n\n"
                    "_*Nota:* No es necesario contestar este mensaje._"
                )
            else:
                mensaje_final = (
                    "🤖 Su pago ha sido registrado y está a la espera de la revisión por un asesor. "
                    "Serás notificado por este medio en un plazo de 24 a 72 horas (Dias habiles). Gracias por su confianza. \n\n"
                    "_*Nota:* No es necesario contestar este mensaje._"
                )
            
            enviar_whatsapp(telefono_contacto, mensaje_final)
            logger.info(f"✅ Sincronización exitosa para Lote: {lote_final}")

    except Exception as e:
        logger.error(f"❌ Error crítico en background: {e}")


# --- COMPROBANTES ---

@router.get("/comprobantes")
def obtener_comprobantes():
    """Obtiene todos los documentos de la colección de forma eficiente."""
    try:
        docs = db_firestore.collection("ComprobantePago").stream()
        resultado = []
        for doc in docs:
            item = doc.to_dict()
            item["id"] = doc.id
            resultado.append(item)
        return resultado
    except Exception as e:
        logger.error(f"Error al obtener comprobantes: {e}")
        return []


# --- RUTAS DINAMICO---
    
@router.post("/actualizarContacto")
def actualizar_status(id: str, payload: StatusUpdate):
    """Actualiza el campo status de un documento específico."""
    try:
        doc_ref = db_firestore.collection("ComprobantePago").document(id)
        doc_ref.update({"status": payload.status})
        return {"status": "success", "id": id, "nuevo_status": payload.status}
    except Exception as e:
        logger.error(f"Error al actualizar status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/obtenerMensajeFolios")
def obtener_Mensaje_Folios(evento: LotesRequest):
    try:
        # Acceso directo a los atributos del modelo
        telefono_recibido = evento.telefono
        telefono_recibido = f"+{telefono_recibido}"
        lotes_temporales = evento.lotestemporales

        with SessionLocal() as db:
            # 2. Consulta de todos los registros activos
            registros_ventas = db.query(Venta).filter(
                and_(
                    Venta.telefono == telefono_recibido,
                    Venta.estado_expediente.notin_(['Cancelado', 'Expirado'])
                )
            ).all() 
            
            if registros_ventas:
                # Extraemos y ordenamos los lotes únicos encontrados en la BD
                lotes_encontrados = sorted(list(set([str(r.numero) for r in registros_ventas])))
                
                # --- LÓGICA DE VERIFICACIÓN DE LOTES TEMPORALES ---
                if lotes_temporales:
                    lotes_temporales_lista = sorted([l.strip() for l in lotes_temporales.split(",") if l.strip()])
                    
                    if lotes_temporales_lista != lotes_encontrados:
                        lotes_encontrados = lotes_temporales_lista
                # --------------------------------------------------

                logger.info(f">>> LOTES A MOSTRAR para {telefono_recibido}: {lotes_encontrados}")

                # 3. Construcción del mensaje numerado
                encabezado = "🤖 Consultando en mi base de datos encontré los siguientes folios:\n\n"
                lineas_folios = []
                
                for i, lote in enumerate(lotes_encontrados, 1):
                    lineas_folios.append(f"{i}.- {lote}")
                
                mensaje_final = encabezado + "\n".join(lineas_folios)

                # 4. Envío del mensaje por WhatsApp
                envio = enviar_whatsapp(telefono_recibido, mensaje_final)
                
                if envio:
                    return {
                        "status": "success",
                        "mensaje_enviado": mensaje_final,
                        "lotes": lotes_encontrados
                    }
                else:
                    return {"status": "error", "message": "No se pudo enviar el mensaje por WhatsApp"}
            
            else:
                logger.warning(f">>> INFO: No se encontró registro activo para {telefono_recibido}")
                return {"status": "not_found", "message": "No se encontró registro activo"}

    except Exception as e:
        logger.error(f"❌ Error crítico en obtener_Mensaje_Folios: {e}")
        return {"status": "error", "message": str(e)}

@router.post("/actualizarLotesTemporales")
def actualizar_LotesTemporales(evento: LotesTemporalesRequest):
    try:
        # 1. Obtener los datos del objeto validado
        lotes_temporales_raw = evento.lotestemporales
        lote_seleccionado = evento.loteseleccionado.strip()

        if not lotes_temporales_raw:
            return {"status": "error", "message": "No hay lotes temporales"}

        # 2. Convertir la cadena en una lista y limpiar espacios
        lista_lotes = [lote.strip() for lote in lotes_temporales_raw.split(",") if lote.strip()]

        # 3. Eliminar el lote seleccionado si existe en la lista
        if lote_seleccionado in lista_lotes:
            lista_lotes.remove(lote_seleccionado)
            logger.info(f">>> Lote '{lote_seleccionado}' eliminado. Restantes: {lista_lotes}")
        else:
            logger.warning(f">>> El lote '{lote_seleccionado}' no se encontró en la lista.")

        # 4. Volver a unir la lista en una cadena separada por comas
        lotes_actualizados = ", ".join(lista_lotes)

        return {
            "status": "success",
            "data": {
                "lotestemporales": lotes_actualizados
            }
        }

    except Exception as e:
        logger.error(f"❌ Error en actualizar_LotesTemporales: {e}")
        return {"status": "error", "message": str(e)}

@router.post("/verificarComprobanteGuardado")
def verificar_Comprobante_Guardado(datos: VerificarBD):
    """
    Verifica si en la base de datos de Firebase se guardó algún comprobante 
    con el Teléfono y Lote proporcionados.
    """
    project_id = os.getenv("FIREBASE_PLANTILLAS_PROJECT_ID")
    api_key = os.getenv("FIREBASE_PLANTILLAS_API_KEY")
    
    if not project_id or not api_key:
        return {"status": "error", "message": "Configuración de Firebase incompleta"}

    url = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents:runQuery"
    headers = {"X-Goog-Api-Key": api_key, "Content-Type": "application/json"}
    
    # Normalización del teléfono para asegurar coincidencia con el formato guardado (+521...)
    telefono = str(datos.telefono)
    if not telefono.startswith("+"):
        telefono = f"+{telefono}"
        
    query = {
        "structuredQuery": {
            "from": [{"collectionId": "ComprobantePago"}],
            "where": {
                "compositeFilter": {
                    "op": "AND",
                    "filters": [
                        {
                            "fieldFilter": {
                                "field": {"fieldPath": "Contacto.Telefono"},
                                "op": "EQUAL",
                                "value": {"stringValue": telefono}
                            }
                        },
                        {
                            "fieldFilter": {
                                "field": {"fieldPath": "Contacto.Lote"},
                                "op": "EQUAL",
                                "value": {"stringValue": str(datos.loteseleccionado)}
                            }
                        }
                    ]
                }
            },
            "limit": 1
        }
    }

    try:
        # LOG DE DEPURACIÓN
        print(f"DEBUG: Buscando Telefono: '{telefono}'")
        print(f"DEBUG: Buscando Lote: '{datos.loteseleccionado}'")

        response = requests.post(url, json=query, headers=headers, timeout=10)
        
        # Si la respuesta es 200 pero no hay documentos, imprimimos el JSON de Firebase
        if response.status_code == 200:
            results = response.json()
            print(f"DEBUG: Respuesta de Firebase: {results}") # Esto te dirá si viene vacío
            
            if any("document" in item for item in results):
                return {"status": "success"}
        else:
            print(f"DEBUG: Error de Firebase {response.status_code}: {response.text}")
        
        return {"status": "not_found"}

    except Exception as e:
        logger.error(f"❌ Error en verificarComprobanteGuardado: {e}")
        return {"status": "error", "message": str(e)}
    
    
# --- RUTAS ESTATICO ---

@router.get("/imagen")
def verificar_estado():
    """Endpoint de salud para el webhook."""
    return {"status": "online", "mensaje": "Webhook de Vertex AI listo."}

@router.post("/imagen")
async def WebhookImagen(request: Request, background_tasks: BackgroundTasks):
    """
    Recibe el webhook de Respond.io, valida duplicados y responde 
    de inmediato (200 OK) para evitar reintentos.
    """
    try:
        # Leemos el JSON de forma rápida
        payload = await request.json()
        eventos = payload if isinstance(payload, list) else [payload]

        for evento in eventos:
            # Extraemos el ID único del evento que envía Respond.io
            event_id = evento.get("event_id")

            if not event_id:
                # Si por alguna razón no hay ID, procesamos por precaución
                background_tasks.add_task(procesar_evento_background, evento)
                continue

            # --- CONTROL DE IDEMPOTENCIA (EVITAR DUPLICADOS) ---
            if event_id in PROCESSED_EVENTS:
                logger.info(f"⏭️  Evento duplicado detectado y omitido: {event_id}")
                continue
            
            # Registramos el ID antes de iniciar la tarea
            PROCESSED_EVENTS.add(event_id)
            
            # Agregamos la tarea a la cola de fondo
            background_tasks.add_task(procesar_evento_background, evento)

            # Opcional: Limpiar el set de IDs después de 10 minutos para no saturar la memoria
            # (En una versión más robusta usarías Redis, pero para tu escala esto funciona perfecto)

        # Respondemos de inmediato. Respond.io recibirá este 200 OK en milisegundos.
        return {"status": "success", "mensaje": "Petición recibida y en proceso."}

    except Exception as e:
        logger.error(f"Error al recibir webhook: {e}")
        return {"status": "error", "message": "Invalid JSON or server error"}
    
@router.patch("/registro/{id}/status")
def actualizar_status(id: str, payload: StatusUpdate):
    """Actualiza el estatus de un registro específico por su ID de Firebase."""
    
    # Ahora accedemos al dato como atributo: payload.status
    nuevo_status = payload.status 
    
    project_id = os.getenv("FIREBASE_PLANTILLAS_PROJECT_ID")
    api_key = os.getenv("FIREBASE_PLANTILLAS_API_KEY")
    
    url = f"https://firestore.googleapis.com/v1/projects/{project_id}/databases/(default)/documents/ComprobantePago/{id}?updateMask.fieldPaths=status"
    headers = {"X-Goog-Api-Key": api_key, "Content-Type": "application/json"}
    
    fb_payload = {"fields": {"status": {"stringValue": nuevo_status}}}
    
    response = requests.patch(url, headers=headers, json=fb_payload, timeout=10)
    
    if response.status_code != 200:
        raise HTTPException(status_code=response.status_code, detail="Error al actualizar en Firebase")
    
    return {"status": "success", "id": id, "nuevo_status": nuevo_status}