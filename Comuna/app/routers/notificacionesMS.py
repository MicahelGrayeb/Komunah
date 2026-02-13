import os
import requests
import re 
from fastapi import APIRouter, HTTPException, Depends, File, UploadFile, Form, Body
from ..schemas import EmailSchema, PlantillaBase, PlantillaUpdate, ConfigUpdate,EmailManualSchema, PlantillaWAUpdate, PlantillaWABase, WhatsAppManualSchema, SwitchEtapasSchema, EmailFolioSchema, RecordatoriosUpdate
from ..utils.datos_proveedores import get_komunah_data, set_wa_komunah_lote, set_email_komunah_lote, set_email_komunah_marketing, set_wa_komunah_marketing, get_folios_a_notificar_komunah, actualizar_switches_etapas, actualizar_switches_proyecto, get_estado_etapas_komunah, get_folios_deudores_komunah
from urllib.parse import quote
from ..database import get_db
from sqlalchemy.orm import Session
from datetime import datetime, timedelta
from sqlalchemy import text
from zoneinfo import ZoneInfo
import base64, json
from typing import List, Optional, Union, Any
import hashlib
from ..services.security import get_current_user, es_admin, es_super_admin, es_usuario
 
 
router = APIRouter(prefix="/v1/notificaciones", tags=["Motor Envios"])
router_crud = APIRouter(prefix="/v1/plantillas", tags=["CRUD Plantillas"])
router_wa = APIRouter(prefix="/v1/plantillas-wa", tags=["CRUD WhatsApp"])
router_usuario = APIRouter(prefix="/v1/preferencias-usuario", tags=["Switches Clientes"])
router_globales = APIRouter(prefix="/v1/configuracion-global", tags=["Configuración Global"])

EMPRESAS_AUTORIZADAS = ["komunah", "empresa_test"]
PROVIDERS = {
    "komunah": {
        "get": get_komunah_data,
        "get_pendientes": get_folios_a_notificar_komunah,
        "get_deudores": get_folios_deudores_komunah,
        "set_email_lote": set_email_komunah_lote,
        "set_wa_lote": set_wa_komunah_lote,
        "set_etapas_bulk": actualizar_switches_etapas,
        "set_proyecto_bulk": actualizar_switches_proyecto,
        "get_estado_etapas": get_estado_etapas_komunah
    }
}

class FirebaseRepository:
    """Maneja la comunicación técnica con Firebase Firestore."""
    
    def __init__(self):
        self.project_id = os.getenv('FIREBASE_PLANTILLAS_PROJECT_ID', '').strip()
        self.api_key = os.getenv('FIREBASE_PLANTILLAS_API_KEY')
        self.base_url = f"https://firestore.googleapis.com/v1/projects/{self.project_id}/databases/(default)/documents"
        self.headers = {"X-Goog-Api-Key": self.api_key, "Content-Type": "application/json"}

    def obtener_config_empresa(self, empresa_id: str):
        """lógica  de switches."""
        url = f"{self.base_url}/empresas/{empresa_id}/configuracion/general"
        try:
            resp = requests.get(url, headers=self.headers, timeout=5)
            if resp.status_code != 200: 
                return {"proyecto": True, "email": True, "whatsapp": True}
            f = resp.json().get("fields", {})
            return {
                "proyecto": f.get("proyecto_activo", {}).get("booleanValue", True),
                "email": f.get("email_enabled", {}).get("booleanValue", True),
                "whatsapp": f.get("whatsapp_enabled", {}).get("booleanValue", True)
            }
        except:
            return {"proyecto": True, "email": True, "whatsapp": True}

    def obtener_plantilla_segura(self, empresa_id: str, slug: str):
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas/{slug}"
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
           
            if response.status_code != 200:
                print(f"DEBUG FIREBASE - Error {response.status_code}: {response.text}")
                return None 
            data = response.json()
            return data.get("fields", {}).get("html", {}).get("stringValue", "")
        except Exception as e:
            print(f"DEBUG FIREBASE - Excepción: {e}")
            return None

    def query_categoria(self, empresa_id: str, categoria: str, coleccion: str = "plantillas"): 
        url = f"{self.base_url}/empresas/{empresa_id}:runQuery" 
        query = {
            "structuredQuery": {
                "from": [{"collectionId": coleccion}],
                "where": {
                    "fieldFilter": {
                        "field": {"fieldPath": "categoria"},
                        "op": "EQUAL",
                        "value": {"stringValue": categoria}
                    }
                }
            }
        }
        response = requests.post(url, json=query, headers=self.headers, timeout=10)
    
        if response.status_code != 200:
            print(f"--- ERROR DE FIREBASE ---")
            print(f"Status: {response.status_code}")
            print(f"Respuesta: {response.text}")
            return []
        return requests.post(url, json=query, headers=self.headers, timeout=10).json()

    def patch_activo_status(self, doc_path: str, status: bool):
        url = f"https://firestore.googleapis.com/v1/{doc_path}?updateMask.fieldPaths=activo"
        payload = {"fields": {"activo": {"booleanValue": status}}}
        return requests.patch(url, json=payload, headers=self.headers, timeout=10)
    
    def eliminar_plantilla(self, empresa_id: str, doc_id: str):
        """Elimina físicamente el documento."""
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas/{doc_id}"
        return requests.delete(url, headers=self.headers, timeout=10)

    def actualizar_plantilla(self, empresa_id: str, doc_id: str, p: PlantillaUpdate):
        """Actualiza campos específicos usando updateMask."""
        fields = {}
        mask = []
        if p.nombre: 
            fields["nombre"] = {"stringValue": p.nombre}
            mask.append("nombre")
        if p.asunto: 
            fields["asunto"] = {"stringValue": p.asunto}
            mask.append("asunto")
        if p.html: 
            fields["html"] = {"stringValue": p.html}
            mask.append("html")
        if p.categoria: 
            fields["categoria"] = {"stringValue": p.categoria}
            mask.append("categoria")
        if p.activo is not None: 
            fields["activo"] = {"booleanValue": bool(p.activo)}
            mask.append("activo")
        if p.tags_departamento is not None:

            fields["tags_departamento"] = {
                "arrayValue": {
                    "values": [{"stringValue": tag} for tag in p.tags_departamento]
                }
            }
            mask.append("tags_departamento")
        
        query_params = "&".join([f"updateMask.fieldPaths={m}" for m in mask])
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas/{doc_id}?{query_params}"
        return requests.patch(url, json={"fields": fields}, headers=self.headers, timeout=10)   

    def actualizar_configuracion(self, empresa_id: str, c: ConfigUpdate):
        fields = {}
        mask = []
        if c.proyecto_activo is not None:
            fields["proyecto_activo"] = {"booleanValue": c.proyecto_activo}; mask.append("proyecto_activo")
        if c.email_enabled is not None:
            fields["email_enabled"] = {"booleanValue": c.email_enabled}; mask.append("email_enabled")
        if c.whatsapp_enabled is not None:
            fields["whatsapp_enabled"] = {"booleanValue": c.whatsapp_enabled}; mask.append("whatsapp_enabled")

        query_params = "&".join([f"updateMask.fieldPaths={m}" for m in mask])
        url = f"{self.base_url}/empresas/{empresa_id}/configuracion/general?{query_params}"
        return requests.patch(url, json={"fields": fields}, headers=self.headers, timeout=10)
    
    def listar_todas_plantillas(self, empresa_id: str):
        """Para el GET de la lista completa."""
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas"
        resp = requests.get(url, headers=self.headers, timeout=10)
        return resp.json().get("documents", []) if resp.status_code == 200 else []

    def generar_siguiente_id(self, empresa_id: str):
        """Busca el máximo y usa 4 dígitos para que quepan hasta 9,999 plantillas."""
        docs = self.listar_todas_plantillas(empresa_id)
        prefijo = empresa_id[:2].upper()
        max_num = 0
        
       
        for d in docs:
            id_doc = d["name"].split("/")[-1]
            match = re.search(rf"{prefijo}-(\d+)", id_doc)
            if match:
                num = int(match.group(1))
                if num > max_num:
                    max_num = num
        
      
        return f"{prefijo}-{str(max_num + 1).zfill(4)}"

    def obtener_un_doc_completo(self, empresa_id: str, doc_id: str):
        """Para el GET de edición (trae todos los campos)."""
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas/{doc_id}"
        resp = requests.get(url, headers=self.headers, timeout=10)
        return resp.json() if resp.status_code == 200 else None
    
    def obtener_un_doc_completo_wa(self, empresa_id: str, doc_id: str):
        """Busca un solo documento en la colección de WhatsApp."""
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas_whatsapp/{doc_id}"
        resp = requests.get(url, headers=self.headers, timeout=10)
        return resp.json() if resp.status_code == 200 else None
    
    def listar_plantillas_wa(self, empresa_id: str):
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas_whatsapp"
        resp = requests.get(url, headers=self.headers, timeout=10)
        return resp.json().get("documents", []) if resp.status_code == 200 else []

    def generar_siguiente_id_wa(self, empresa_id: str):
        docs = self.listar_plantillas_wa(empresa_id)
        prefijo = empresa_id[:2].upper()
        max_num = 0
        for d in docs:
            id_doc = d["name"].split("/")[-1]
    
            match = re.search(rf"{prefijo}-(\d+)-WA", id_doc)
            if match:
                num = int(match.group(1))
                if num > max_num: max_num = num
        return f"{prefijo}-{str(max_num + 1).zfill(4)}-WA"

    def actualizar_plantilla_wa(self, empresa_id: str, doc_id: str, p: PlantillaWAUpdate):
        fields = {}
        mask = []
        data = p.dict(exclude_none=True)
        for key, value in data.items():
            mask.append(key)
            if key == "activo": fields[key] = {"booleanValue": bool(value)}
            elif key == "variables": 
                fields[key] = {"arrayValue": {"values": [{"stringValue": v} for v in value]}}
            else: fields[key] = {"stringValue": str(value)}
        query_params = "&".join([f"updateMask.fieldPaths={m}" for m in mask])
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas_whatsapp/{doc_id}?{query_params}"
        return requests.patch(url, json={"fields": fields}, headers=self.headers, timeout=10)
    
    def registrar_log_falla(self, empresa_id: str, mensaje: str, contexto: str):
        """Almacena fallas agrupadas en empresas/{id}/logs_fallas."""
        error_id = hashlib.md5(mensaje.encode()).hexdigest()
        url = f"{self.base_url}/empresas/{empresa_id}/logs_fallas/{error_id}"
        ahora = datetime.now(ZoneInfo("America/Mexico_City")).isoformat()

        resp = requests.get(url, headers=self.headers) # <-- CORREGIDO
        
        if resp.status_code == 200:
            fields = resp.json().get("fields", {})
            conteo = int(fields.get("contador", {}).get("integerValue", 0)) + 1
            payload = {
                "fields": {
                    "contador": {"integerValue": conteo},
                    "ultima_vez": {"stringValue": ahora},
                    "leido": {"booleanValue": False} 
                }
            }
            mask = "updateMask.fieldPaths=contador&updateMask.fieldPaths=ultima_vez&updateMask.fieldPaths=leido"
            requests.patch(f"{url}?{mask}", json=payload, headers=self.headers) 
        else:
            payload = {
                "fields": {
                    "mensaje": {"stringValue": mensaje},
                    "contexto": {"stringValue": contexto},
                    "contador": {"integerValue": 1},
                    "leido": {"booleanValue": False},
                    "ultima_vez": {"stringValue": ahora},
                    "fecha_inicial": {"stringValue": ahora}
                }
            }
            requests.patch(url, json=payload, headers=self.headers) 

    def obtener_config_recordatorios(self, empresa_id: str):
        """Trae los días de recordatorio desde Firebase."""
        url = f"{self.base_url}/empresas/{empresa_id}/configuracion/recordatorios"
        resp = requests.get(url, headers=self.headers, timeout=5)
        if resp.status_code != 200:
           return {"dias_1": 3, "dias_2": 1, "hora": 10, "minuto": 0}    
        f = resp.json().get("fields", {})
        return {
            "dias_1": int(f.get("recordatorio_1", {}).get("integerValue", 3)),
            "dias_2": int(f.get("recordatorio_2", {}).get("integerValue", 1)),
            "hora": int(f.get("hora_recordatorio", {}).get("integerValue", 10)),
            "minuto": int(f.get("minuto_recordatorio", {}).get("integerValue", 0))
        }
    
    def actualizar_config_recordatorios(self, empresa_id: str, datos: dict):
        """Recibe un diccionario y parchea solo los campos presentes en él."""
        url = f"{self.base_url}/empresas/{empresa_id}/configuracion/recordatorios"
        fields = {}
        mask = []


        mapeo = {
            "dias_1": "recordatorio_1",
            "dias_2": "recordatorio_2",
            "hora": "hora_recordatorio",
            "minuto": "minuto_recordatorio"
        }

        for key, valor in datos.items():
            if valor is not None and key in mapeo:
                fire_key = mapeo[key]
                fields[fire_key] = {"integerValue": int(valor)}
                mask.append(f"updateMask.fieldPaths={fire_key}")

        if not mask:
            return None

        query_params = "&".join(mask)
        full_url = f"{url}?{query_params}"
        
        return requests.patch(full_url, json={"fields": fields}, headers=self.headers, timeout=10)
class NotificationGateway:
    """Maneja la comunicación pura con MailerSend."""
    @staticmethod
    def enviar_email(payload: dict):
        api_key = os.getenv("MAILERSEND_API_KEY")
        url = "https://api.mailersend.com/v1/email"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        
        return requests.post(url, headers=headers, json=payload, timeout=10)
    
    @staticmethod
    def enviar_whatsapp(numero: str, template_name: str, language_code: str, parametros: list, texto_cuerpo: str = ""):
        token = os.getenv("RESPOND_IO_TOKEN")
        channel_id = os.getenv("RESPOND_IO_CHANNEL_ID")
        
        identifier = quote(f"phone:{numero}")
        url = f"https://api.respond.io/v2/contact/{identifier}/message"
        
        
        payload = {
            "channelId": int(channel_id),
            "message": {
                "type": "whatsapp_template",
                "template": {
                    "name": template_name,
                    "languageCode": language_code,
                    "components": [
                        {
                            "type": "body", 
                            "text": texto_cuerpo,
                            "parameters": [{"type": "text", "text": str(p)} for p in parametros]
                        }
                    ]
                }
            }
        }
        
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        return requests.post(url, headers=headers, json=payload, timeout=10)

class StaticNotificationUseCase:
    def __init__(self, gateway: NotificationGateway):
        self.gateway = gateway
        

    def ejecutar_envio_manual(self, empresa_id: str, datos: EmailManualSchema, db: Session):
        pack_empresa = PROVIDERS.get(empresa_id, {})
        extraer_datos = pack_empresa.get("get")
        variables = extraer_datos(datos.folio, db) if (extraer_datos and datos.folio) else {}

        
        html_procesado = self._reemplazar_etiquetas(datos.contenido_html, variables)
        asunto_procesado = self._reemplazar_etiquetas(datos.asunto, variables)
        adjuntos = getattr(datos, 'adjuntos', [])
        if adjuntos is None:
            adjuntos = []
        reporte = []
    
        for email_destino in datos.para:
            payload = {
                "from": {"email": datos.remitente, "name": f"Finanzas {empresa_id.capitalize()}"},
                "to": [{"email": email_destino}],
                "cc": [{"email": e} for e in datos.cc],
                "bcc": [{"email": e} for e in datos.cco],
                "subject": asunto_procesado,
                "html": html_procesado,
                "reply_to": {"email": datos.reply_to} if datos.reply_to else None,
                "attachments": adjuntos
            }
            
            res = self.gateway.enviar_email(payload)
            if res.status_code not in [200, 201, 202]:
                FirebaseRepository().registrar_log_falla(
                    empresa_id, 
                    f"Email Manual falló ({res.status_code}) para {email_destino}", 
                    "MANUAL_EMAIL"
                )
            reporte.append({"email": email_destino, "status_code": res.status_code})

        return {"reporte_final": reporte, "variables_detectadas": len(variables)}

    def _reemplazar_etiquetas(self, texto, vars):
        
        for tag, valor in vars.items():
            texto = texto.replace(tag, str(valor))
        return texto
 
class StaticWAUseCase:
    def __init__(self, repo: FirebaseRepository, gateway: NotificationGateway):
        self.repo = repo
        self.gateway = gateway

    def ejecutar_envio_wa(self, empresa_id: str, datos: WhatsAppManualSchema, db: Session):
        
        pack_empresa = PROVIDERS.get(empresa_id, {})
        extraer_datos = pack_empresa.get("get")
        if not extraer_datos:
            self.repo.registrar_log_falla(empresa_id, f"Empresa {empresa_id} no configurada", "MANUAL_WA") 
            raise HTTPException(status_code=400, detail=f"Empresa '{empresa_id}' no configurada.")
        
        data_sql = extraer_datos(datos.folio, db)
        if not data_sql:
            self.repo.registrar_log_falla(empresa_id, f"Folio {datos.folio} no encontrado en SQL para envío manual", "MANUAL_WA_ERROR")
            raise HTTPException(status_code=404, detail="Folio no encontrado.")

        docs_wa = self.repo.query_categoria(empresa_id, datos.categoria, "plantillas_whatsapp")
        p_wa_raw = next((d["document"] for d in docs_wa if "document" in d 
                         and (d["document"]["fields"].get("activo", {}).get("booleanValue") is True 
                              or d["document"]["fields"].get("activo", {}).get("stringValue") == "true")), None)
        
        if not p_wa_raw:
            self.repo.registrar_log_falla(empresa_id, f"Manual WA: Sin plantilla activa para '{datos.categoria}'", "MANUAL_WA_ERROR")
            raise HTTPException(status_code=400, detail="No hay plantilla activa.")

        f_wa = p_wa_raw["fields"]
        config_plantilla = {
            "id_respond": f_wa.get("id_respond", {}).get("stringValue"),
            "lenguaje": f_wa.get("lenguaje", {}).get("stringValue"),
            "texto_base": f_wa.get("mensaje", {}).get("stringValue", ""),
            "variables": [v.get("stringValue") for v in f_wa.get("variables", {}).get("arrayValue", {}).get("values", [])]
        }
        reporte = []
        for i in range(1, 7):
            nombre = data_sql.get(f"{{c{i}.client_name}}")
            telefono = data_sql.get(f"{{g{i}.telefono}}", "").replace(" ", "").replace("-", "")
            
            if not nombre or not telefono:
                continue

            parametros_finales = []
            for var_nombre in config_plantilla["variables"]:
                if var_nombre in ["{cl.cliente}", "{cliente}", "{v.cliente}"]:
                    valor = nombre
                else:
                    valor = data_sql.get(var_nombre, "N/A")
                parametros_finales.append(valor)

            texto_listo = config_plantilla["texto_base"]
            for idx, v_nombre in enumerate(config_plantilla["variables"], 1):
                texto_listo = texto_listo.replace(v_nombre, f"{{{{{idx}}}}}")

            num_wa = telefono if telefono.startswith("+") else f"+521{telefono}"
            res = self.gateway.enviar_whatsapp(
                num_wa, 
                config_plantilla["id_respond"], 
                config_plantilla["lenguaje"], 
                parametros_finales,
                texto_cuerpo=texto_listo
            )

            if res.status_code not in [200, 201, 202]:
                self.repo.registrar_log_falla(
                    empresa_id, 
                    f"WhatsApp Manual falló ({res.status_code}) para {nombre} en folio {datos.folio}", 
                    "WA_PROVIDER_ERROR"
                )

            reporte.append({"cliente": nombre, "telefono": num_wa, "status": res.status_code})
        return {
            "folio": datos.folio,
            "categoria": datos.categoria,
            "detalles": reporte
        }

class StaticEmailFolioUseCase:
    def __init__(self, repo: FirebaseRepository, gateway: NotificationGateway):
        self.repo = repo
        self.gateway = gateway

    def ejecutar_envio_email_folio(self, empresa_id: str, datos: EmailFolioSchema, db: Session):
        pack_empresa = PROVIDERS.get(empresa_id, {})
        extraer_datos = pack_empresa.get("get")

        data_sql = extraer_datos(datos.folio, db)
        if not data_sql:
            raise HTTPException(status_code=404, detail="Folio no encontrado.")

        docs_email = self.repo.query_categoria(empresa_id, datos.categoria, "plantillas")
        p_email_raw = next((d["document"] for d in docs_email if "document" in d 
                           and (d["document"]["fields"].get("activo", {}).get("booleanValue") is True)), None)
        
        if not p_email_raw:
            raise HTTPException(status_code=400, detail=f"No hay plantilla de email activa para '{datos.categoria}'")

        f_email = p_email_raw["fields"]
        reporte = []

        for i in range(1, 7):
            nombre = data_sql.get(f"{{c{i}.client_name}}")
            email = data_sql.get(f"{{g{i}.email}}")
            phone = data_sql.get(f"{{g{i}.telefono}}", "").replace(" ", "").replace("-", "")

            if not nombre or not email:
                continue

            cleaner = NotificationUseCase(self.repo, self.gateway)
            asunto_listo = cleaner._limpiar(f_email.get("asunto", {}).get("stringValue", ""), data_sql, nombre, email, phone)
            html_listo = cleaner._limpiar(f_email.get("html", {}).get("stringValue", ""), data_sql, nombre, email, phone)

            res = self.gateway.enviar_email({
                "from": {"email": os.getenv("MAILERSEND_SENDER"), "name": f"Notificaciones {empresa_id.capitalize()}"},
                "to": [{"email": email, "name": nombre}],
                "subject": asunto_listo,
                "html": html_listo
            })

            reporte.append({"cliente": nombre, "email": email, "status": res.status_code})

        return {"folio": datos.folio, "categoria": datos.categoria, "detalles": reporte}

class TemplateUseCase:
    """Maneja la lógica del switch de activación: uno true, el resto false."""
    
    @staticmethod
    def asegurar_activacion_unica(repo: FirebaseRepository, empresa_id: str, doc_id: str, categoria: str, coleccion: str):
        """Apaga el resto de la categoría si la nueva está activa."""
        docs = repo.query_categoria(empresa_id, categoria, coleccion)
        for item in docs:
            if "document" not in item: continue
            path = item["document"]["name"]
            id_actual = path.split("/")[-1]
            if id_actual != doc_id:
                repo.patch_activo_status(path, False)
    
    @staticmethod
    def contar_plantillas_por_categoria(repo: FirebaseRepository, empresa_id: str, categoria: str):
        """Cuenta documentos reales devueltos por la query."""
        res = repo.query_categoria(empresa_id, categoria)
        if not isinstance(res, list): return 0
        return len([item for item in res if "document" in item])

class NotificationUseCase:
    def __init__(self, repo: FirebaseRepository, gateway: NotificationGateway):
        self.repo = repo
        self.gateway = gateway

    def ejecutar_barrido_automatico(self, empresa_id: str, dias: int, categoria: str, db: Session, tipo: str = "normal"):
        pack_empresa = PROVIDERS.get(empresa_id, {})
        extraer_datos = pack_empresa.get("get")
        if not extraer_datos:
            self.repo.registrar_log_falla(empresa_id, f"Empresa '{empresa_id}' no configurada.", "CONFIG")
            raise HTTPException(status_code=400, detail=f"Empresa '{empresa_id}' no configurada.")

        config = self.repo.obtener_config_empresa(empresa_id)
        sistema_email_ok = config.get("email")
        sistema_wa_ok = config.get("whatsapp")
        
        if not config.get("proyecto"):
            self.repo.registrar_log_falla(empresa_id, f"Barrido cancelado: Proyecto desactivado en configuración global", "AUTO_BARRIDO")
            return {"status": "off", "msj": "Proyecto desactivado"}
        
        docs_email = self.repo.query_categoria(empresa_id, categoria, "plantillas")
        p_email = next((d["document"]["fields"] for d in docs_email if "document" in d 
                        and d["document"]["fields"].get("activo", {}).get("booleanValue")), None)
       
        docs_wa = self.repo.query_categoria(empresa_id, categoria, "plantillas_whatsapp")
        
        p_wa_raw = next((d["document"] for d in docs_wa if "document" in d 
                        and d["document"]["fields"].get("activo", {}).get("booleanValue")), None)
        
        if config.get("email") and not p_email:
            self.repo.registrar_log_falla(empresa_id, f"Email activado pero no hay plantilla activa para '{categoria}'", "AUTO_BARRIDO")
        
        if config.get("whatsapp") and not p_wa_raw:
            self.repo.registrar_log_falla(empresa_id, f"WhatsApp activado pero no hay plantilla activa para '{categoria}'", "AUTO_BARRIDO")
            
        p_wa = None
        if p_wa_raw:
            f_wa = p_wa_raw["fields"]
            p_wa = {
                "id_respond": f_wa.get("id_respond", {}).get("stringValue"),
                "lenguaje": f_wa.get("lenguaje", {}).get("stringValue"),
                "texto_base": f_wa.get("mensaje", {}).get("stringValue", ""),
                "variables": [v.get("stringValue") for v in f_wa.get("variables", {}).get("arrayValue", {}).get("values", [])]
            }
        fecha_t = (datetime.now(ZoneInfo("America/Mexico_City")) + timedelta(days=dias)).strftime('%Y-%m-%d')
        try:
            if tipo == "deudores":
                registros = pack_empresa.get("get_deudores")(db, fecha_t)
            else:
                registros = pack_empresa.get("get_pendientes")(db, fecha_t)
        except Exception as e:
            self.repo.registrar_log_falla(empresa_id, f"Error SQL: {str(e)}", "DATABASE")
            raise
        
        reporte_detallado = []

        for row in registros:
            data_sql = extraer_datos(row, db)
            

            if data_sql.get("{sys.etapa_activa}") == "0":
                motivo = data_sql.get("{sys.bloqueo_motivo}", "Bloqueo por configuración de Etapa/Proyecto")

                self.repo.registrar_log_falla(empresa_id, f"Folio {row} saltado: {motivo}", "BLOQUEO_ADMINISTRATIVO")
                continue
            
            if not data_sql:
                self.repo.registrar_log_falla(empresa_id, f"El folio {row[0]} no trajo info de SQL", "DATOS_SQL")
                continue
            
            for i in range(1, 7):
                nombre = data_sql.get(f"{{c{i}.client_name}}")
                if not nombre: continue
                
                email = data_sql.get(f"{{g{i}.email}}")
                phone = data_sql.get(f"{{g{i}.telefono}}", "").replace(" ", "").replace("-", "")
                acepta_email_lote = str(data_sql.get(f"{{g{i}.permite_email_lote}}")) in ["1", "True"]
                acepta_wa_lote = str(data_sql.get(f"{{g{i}.permite_whatsapp_lote}}")) in ["1", "True"]
    
                resultado_envio = {"cliente": nombre, "folio": row, "email": "n/a", "wa": "n/a"}
                
                        
                if not sistema_email_ok:
                    self.repo.registrar_log_falla(empresa_id, f"Email omitido para {nombre}: Switch Global OFF.", "GLOBAL_OFF")
                    resultado_envio["email"] = "GLOBAL_OFF"
                elif not p_email:
                    self.repo.registrar_log_falla(empresa_id, f"Email omitido para {nombre}: Sin plantilla activa.", "PLANTILLA_OFF")
                    resultado_envio["email"] = "NO_TEMPLATE"
                elif not acepta_email_lote:
                    self.repo.registrar_log_falla(empresa_id, f"Email omitido para {nombre}: Usuario apagó switch de lote {row}.", "USER_LOTE_OFF")
                    resultado_envio["email"] = "LOTE_OFF"
                elif not email:
                    self.repo.registrar_log_falla(empresa_id, f"Email omitido para {nombre}: No tiene correo registrado.", "DATA_MISSING")
                    resultado_envio["email"] = "NO_DATA"
                else:
                    lista_adjuntos = []
                    adjuntos_raw = p_email.get("adjuntos_url", {}).get("arrayValue", {}).get("values", [])
                    for adj in adjuntos_raw:
                        url_archivo = adj.get("stringValue")
                        info_archivo = self._descargar_a_base64(url_archivo)
                        if info_archivo:
                            lista_adjuntos.append(info_archivo)
                        
                    res_mail = self.gateway.enviar_email({
                        "from": {"email": os.getenv("MAILERSEND_SENDER"), "name": f"Notificaciones {empresa_id}"},
                        "to": [{"email": email, "name": nombre}],
                        "subject": self._limpiar(p_email["asunto"]["stringValue"], data_sql, nombre, email, phone),
                        "html": self._limpiar(p_email["html"]["stringValue"], data_sql, nombre, email, phone),
                        "attachments": lista_adjuntos
                    })
                    if res_mail.status_code not in [200, 201, 202]:
                        self.repo.registrar_log_falla(empresa_id, f"Email falló ({res_mail.status_code}) para {email}", "MAIL_PROVIDER")
                    resultado_envio["email"] = f"Status: {res_mail.status_code} | {res_mail.text[:100]}"
                        


                if not sistema_wa_ok:
                    self.repo.registrar_log_falla(empresa_id, f"WA saltado para {nombre}: Switch Global OFF en Firebase.", "GLOBAL_OFF")
                    resultado_envio["wa"] = "GLOBAL_OFF"
                elif not p_wa:
                    self.repo.registrar_log_falla(empresa_id, f"WA saltado para {nombre}: No hay plantilla activa.", "PLANTILLA_OFF")
                    resultado_envio["wa"] = "NO_TEMPLATE"
                elif not acepta_wa_lote:
                    self.repo.registrar_log_falla(empresa_id, f"WA saltado para {nombre}: Lote bloqueado en SQL.", "USER_LOTE_OFF")
                    resultado_envio["wa"] = "LOTE_OFF"
                elif not phone:
                    self.repo.registrar_log_falla(empresa_id, f"WA saltado para {nombre}: Falta número de teléfono.", "DATA_MISSING")
                    resultado_envio["wa"] = "NO_PHONE"
                else:

                    parametros_dinamicos = []
                    for var_nombre in p_wa["variables"]:
                        if var_nombre in ["{cl.cliente}", "{cliente}", "{v.cliente}"]:
                            valor = nombre
                        elif var_nombre == "{email_cliente}":
                            valor = email
                        elif var_nombre == "{telefono_cliente}": 
                            valor = phone
                        else:
                            valor = data_sql.get(var_nombre, "N/A")
                        parametros_dinamicos.append(valor)

                    num_wa = phone if "+" in phone else f"+521{phone}"
                    texto_completo = p_wa["texto_base"]
                    for idx, v_nombre in enumerate(p_wa["variables"], 1):
                        texto_completo = texto_completo.replace(v_nombre, f"{{{{{idx}}}}}")

                    res_wa = self.gateway.enviar_whatsapp(num_wa, p_wa["id_respond"], p_wa["lenguaje"], parametros_dinamicos, texto_cuerpo=texto_completo)
                    
                    if res_wa.status_code not in [200, 201, 202]:
                        self.repo.registrar_log_falla(empresa_id, f"WhatsApp falló ({res_wa.status_code}) para {phone}", "WA_PROVIDER")
                    
                    resultado_envio["wa"] = f"Status: {res_wa.status_code}"

                reporte_detallado.append(resultado_envio)


        return {
            "status": "proceso_finalizado",
            "fecha_buscada": fecha_t,
            "total_intentos": len(reporte_detallado),
            "reporte": reporte_detallado,
            "DEBUG": {
                "plantilla_email_activa": p_email is not None,
                "plantilla_wa_activa": p_wa is not None,
                "config": config
            }
        }
    
    

    def _limpiar(self, texto, vars, nombre, email_persona, tel_persona):
        texto = texto.replace("{cliente}", nombre)
        texto = texto.replace("{email_cliente}", str(email_persona))
        texto = texto.replace("{telefono_cliente}", str(tel_persona))
        for t, v in vars.items(): texto = texto.replace(t, str(v))
        return re.sub(r'\{[v|cl|p|c]\.[^}]+\}', '', texto)

    def _descargar_a_base64(self, url: str):
        """Descarga un archivo de internet y lo convierte al formato que pide MailerSend."""
        try:
            import base64
            r = requests.get(url, timeout=10)
            if r.status_code == 200:
                nombre = url.split("/")[-1].split("?")[0]
                return {
                    "content": base64.b64encode(r.content).decode('utf-8'),
                    "filename": nombre
                }
        except Exception as e:
            print(f"Error descargando adjunto: {e}")
            return None

class StaticDualUseCase:
    def __init__(self, repo: FirebaseRepository, gateway: NotificationGateway):
        self.repo = repo
        self.gateway = gateway

    def ejecutar_envio_dual(self, empresa_id: str, datos: EmailFolioSchema, db: Session):

        motor_wa = StaticWAUseCase(self.repo, self.gateway)
        motor_email = StaticEmailFolioUseCase(self.repo, self.gateway)

        try:
            reporte_wa = motor_wa.ejecutar_envio_wa(empresa_id, datos, db)
            reporte_email = motor_email.ejecutar_envio_email_folio(empresa_id, datos, db)

            return {
                "status": "completado",
                "folio": datos.folio,
                "categoria": datos.categoria,
                "resultado_whatsapp": reporte_wa.get("detalles", []),
                "resultado_email": reporte_email.get("detalles", [])
            }
        except Exception as e:
            self.repo.registrar_log_falla(empresa_id, f"Error en envío Dual: {str(e)}", "DUAL_SEND_ERROR")
            raise HTTPException(status_code=400, detail=f"Error en el proceso dual: {str(e)}")

@router_crud.get("/{empresa_id}/conteo/{categoria}")
def api_contar_plantillas(empresa_id: str, categoria: str,user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
   
    total = TemplateUseCase.contar_plantillas_por_categoria(repo, empresa_id, categoria) 
    return {"categoria": categoria, "total": total}

@router_crud.post("/{empresa_id}", status_code=201)
def api_crear_plantilla(empresa_id: str, p: PlantillaBase, user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
    
    nombre_id = repo.generar_siguiente_id(empresa_id)
    
    url = f"{repo.base_url}/empresas/{empresa_id}/plantillas?documentId={nombre_id}"
    
    payload = {"fields": {
        "id": {"stringValue": nombre_id},  
        "nombre": {"stringValue": p.nombre},     
        "categoria": {"stringValue": p.categoria}, 
        "asunto": {"stringValue": p.asunto}, 
        "html": {"stringValue": p.html}, 
        "activo": {"booleanValue": bool(p.activo)},
        "static": {"booleanValue": False},
        "tags_departamento": {
            "arrayValue": {"values": [{"stringValue": t} for t in p.tags_departamento]}
        }
    }}
    
    r = requests.post(url, json=payload, headers=repo.headers, timeout=10)
    
    if r.status_code == 200 and p.activo:
        TemplateUseCase.asegurar_activacion_unica(repo, empresa_id, nombre_id, p.categoria, "plantillas")
    
    return {"status": "creada", "id": nombre_id, "nombre": p.nombre}
    
@router_crud.patch("/{empresa_id}/{doc_id}")
def api_actualizar_plantilla(empresa_id: str, doc_id: str, datos: PlantillaUpdate, user: dict = Depends(es_admin)):

    campos = datos.dict(exclude_unset=True)
    if not campos or (len(campos) == 1 and "static" in campos):
        raise HTTPException(status_code=400, detail="No enviaste campos válidos para actualizar.")
    repo = FirebaseRepository()
    

    res = repo.actualizar_plantilla(empresa_id, doc_id, datos)
    
    if res is None:
        raise HTTPException(status_code=400, detail="Nada que actualizar.")
    
    if res.status_code == 200 and datos.activo is True:
        categoria_real = datos.categoria
        
        if not categoria_real:
            doc_actual = repo.obtener_un_doc_completo(empresa_id, doc_id)
            if doc_actual:
                categoria_real = doc_actual.get("fields", {}).get("categoria", {}).get("stringValue")

        if categoria_real:
            TemplateUseCase.asegurar_activacion_unica(repo, empresa_id, doc_id, categoria_real, "plantillas")
        
    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail=res.text)

    return {"status": "actualizada", "id": doc_id}
    
@router_crud.delete("/{empresa_id}/{doc_id}")
def api_eliminar_plantilla(empresa_id: str, doc_id: str, user: dict = Depends(es_admin)):
    """
    Elimina una plantilla permanentemente.
    BLOQUEO: No permite borrar ninguna plantilla que tenga el flag 'static' en True.
    """
   
    repo = FirebaseRepository()
    
    doc_actual = repo.obtener_un_doc_completo(empresa_id, doc_id)

    if not doc_actual:
        raise HTTPException(status_code=404, detail="La plantilla no existe en Firebase.")
    
    fields = doc_actual.get("fields", {})
    es_estatica = fields.get("static", {}).get("booleanValue", False)
    if es_estatica:
        repo.registrar_log_falla(
            empresa_id, 
            f"BLOQUEO DE SEGURIDAD: Intento de eliminar la Plantilla Base '{doc_id}'.", 
            "SEGURIDAD_CRUD"
        )
        raise HTTPException(
            status_code=403, 
            detail="Operación Prohibida: Esta es una plantilla base del sistema y no puede ser eliminada."
        )
    
    res = repo.eliminar_plantilla(empresa_id, doc_id)

    if res.status_code not in [200, 204]:
        raise HTTPException(
            status_code=400, 
            detail=f"Error al intentar eliminar en Firebase: {res.text}"
        )
        
    return {"status": "eliminada", "id": doc_id}

@router_crud.get("/{empresa_id}")
def api_get_listado_plantillas(empresa_id: str, user: dict = Depends(es_admin)):
    """Obtiene un listado básico de todas las plantillas de una empresa."""
    repo = FirebaseRepository()
    docs = repo.listar_todas_plantillas(empresa_id)
    
    resultado = []
    for d in docs:
        fields = d.get("fields", {})
        id_tecnico = d["name"].split("/")[-1] 
        
        resultado.append({
            "id": id_tecnico,
            "nombre": fields.get("nombre", {}).get("stringValue", "Sin nombre"),
            "asunto": fields.get("asunto", {}).get("stringValue", ""),
            "categoria": fields.get("categoria", {}).get("stringValue", ""),
            "activo": fields.get("activo", {}).get("booleanValue") is True or fields.get("activo", {}).get("stringValue") == "true",
            "static": fields.get("static", {}).get("booleanValue", False),
            "tags": [
                v.get("stringValue") 
                for v in fields.get("tags_departamento", {}).get("arrayValue", {}).get("values", [])
            ],
            "html": fields.get("html", {}).get("stringValue", ""),
        })
    return resultado

@router_crud.get("/{empresa_id}/{doc_id}")
def api_get_detalle_plantilla(empresa_id: str, doc_id: str, user: dict = Depends(es_admin)):
    """Obtiene todos los campos de una plantilla específica para edición.
    """
    repo = FirebaseRepository()
    doc = repo.obtener_un_doc_completo(empresa_id, doc_id)
    
    if not doc:
        raise HTTPException(status_code=404, detail="Esa plantilla no existe en Firebase")
        
    f = doc.get("fields", {})
    return {
        "id": doc["name"].split("/")[-1],
        "nombre": f.get("nombre", {}).get("stringValue", ""),
        "asunto": f.get("asunto", {}).get("stringValue", ""),
        "html": f.get("html", {}).get("stringValue", ""),
        "categoria": f.get("categoria", {}).get("stringValue", ""),
        "activo": f.get("activo", {}).get("booleanValue") is True or f.get("activo", {}).get("stringValue") == "true",
        "static": f.get("static", {}).get("booleanValue", False),
        "tags": [v.get("stringValue") for v in f.get("tags_departamento", {}).get("arrayValue", {}).get("values", [])]
    }

@router.get("/inspeccionar")
def api_inspeccionar_variables(folio: Optional[str] = None, db: Session = Depends(get_db), user: dict = Depends(es_admin)):
    """
    Succiona todos los datos de un folio real y te devuelve la lista exacta 
    de etiquetas que puedes usar en tu HTML.
    """
    
    variables = get_komunah_data(folio, db)
    
    if not variables and folio is not None:
        raise HTTPException(
            status_code=404, 
            detail=f"El folio {folio} no existe o no tiene datos asignados en SQL."
        )

    return {
        "folio_consultado": folio,
        "total_etiquetas": len(variables),
        "lista_para_copiar": list(variables.keys()),
        "vista_previa_valores": variables  
    }

@router.get("/{empresa_id}/diccionario-maestro")
def api_get_diccionario_maestro(
    empresa_id: str, 
    folio: Optional[str] = None, 
    db: Session = Depends(get_db), 
    user: dict = Depends(es_admin)
):
    """
    Endpoint Único: Devuelve el catálogo de etiquetas.
    Si mandas folio, mete los valores reales de SQL abajo.
    """
    if empresa_id == "komunah":
        from ..utils.datos_proveedores import get_komunah_data, get_komunah_diccionario_maestro

        data_real = None
        if folio:
            data_real = get_komunah_data(folio, db)
            if not data_real:
                raise HTTPException(status_code=404, detail=f"Folio {folio} no existe en SQL.")

        catalogo = get_komunah_diccionario_maestro(data_real)

        # 👇 AQUÍ METES TUS VARIABLES EXTRA QUE SIEMPRE QUIERES
        extras = [
            "{general.email}",
            "{general.telefono}",
            "{general.nombre_cliente}",
            "{general.correo_cliente}"
        ]

        catalogo.append({
            "categoria": "Variables Generales Fijas",
            "variables": extras if not data_real else [
                {"tag": tag, "valor": ""} for tag in extras
            ]
        })

        return catalogo
    
    raise HTTPException(status_code=404, detail="Empresa no configurada.")
    
@router.post("/enviar/{empresa_id}")
async def api_enviar_estatico(
    empresa_id: str,
    datos_json: str = Form(..., description="Pega aquí tu bloque de JSON completo"), 
    archivos: Optional[List[UploadFile]] = File(None), 
    db: Session = Depends(get_db), user: dict = Depends(es_usuario)
):
    gateway = NotificationGateway()
    use_case = StaticNotificationUseCase(gateway)
    try:
        d = json.loads(datos_json)
    except Exception:
        FirebaseRepository().registrar_log_falla(
            empresa_id, 
            f"Error de entrada: El JSON enviado para Email Manual no es válido.", 
            "INPUT_ERROR"
        )
        raise HTTPException(status_code=400, detail="El JSON está mal formado.")

    adjuntos_procesados = []

    if archivos:
        for f in archivos:
           
            if f.filename:
                contenido = await f.read()
                adjuntos_procesados.append({
                    "content": base64.b64encode(contenido).decode('utf-8'),
                    "filename": f.filename
                })

    from argparse import Namespace
    datos_finales = Namespace(
        remitente=d.get("remitente"),
        para=d.get("para", []),
        cc=d.get("cc", []),
        cco=d.get("cco", []),
        asunto=d.get("asunto", ""),
        contenido_html=d.get("contenido_html", ""),
        folio=str(d.get("folio", "")),
        reply_to=d.get("reply_to"),
        adjuntos=adjuntos_procesados 
    )

    return use_case.ejecutar_envio_manual(empresa_id, datos_finales, db)
@router.post("/{empresa_id}/enviar-whatsapp")
def api_enviar_wa(empresa_id: str, datos: WhatsAppManualSchema, db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    """
    Envía una plantilla de WhatsApp a una lista ilimitada de números.
    Busca automáticamente la plantilla ACTIVA de la categoría enviada.
    """
    repo = FirebaseRepository()
    gateway = NotificationGateway()
    use_case = StaticWAUseCase(repo, gateway)
    return use_case.ejecutar_envio_wa(empresa_id, datos, db)
 
@router.post("/{empresa_id}/enviar-email-folio")
def api_enviar_email(
    empresa_id: str, 
    datos: EmailFolioSchema, 
    db: Session = Depends(get_db), user: dict = Depends(es_usuario)
):
    repo = FirebaseRepository()
    gateway = NotificationGateway()
    use_case = StaticEmailFolioUseCase(repo, gateway)
    return use_case.ejecutar_envio_email_folio(empresa_id, datos, db)

@router.post("/{empresa_id}/enviar-dual")
def api_enviar_ambos_manual(
    empresa_id: str, 
    datos: EmailFolioSchema, 
    db: Session = Depends(get_db), 
    user: dict = Depends(es_usuario)
):
    """
    ENVÍO DUAL: Dispara WhatsApp y Email al mismo tiempo para un folio.
    Usa las plantillas activas de la categoría proporcionada.
    """
    repo = FirebaseRepository()
    gateway = NotificationGateway()
    use_case = StaticDualUseCase(repo, gateway)
    
    return use_case.ejecutar_envio_dual(empresa_id, datos, db)

@router.post("/auto-notificar/{empresa_id}", tags=["Motor Notificaciones"])
def api_disparar_barrido(
    empresa_id: str, 
    dias: int, 
    categoria: str,  
    tipo: str = "normal",
    db: Session = Depends(get_db), user: dict = Depends(es_usuario)
):
    """Barrido Automático Genérico: Email (Firebase) + WhatsApp (Respond.io)."""
    repo = FirebaseRepository()
    gateway = NotificationGateway()
    use_case = NotificationUseCase(repo, gateway)
    return use_case.ejecutar_barrido_automatico(empresa_id, dias, categoria, db, tipo=tipo)

@router_wa.post("/{empresa_id}", status_code=201, tags=["CRUD WhatsApp"])
def api_crear_plantilla_wa(empresa_id: str, p: PlantillaWABase, user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
    

    nombre_id = repo.generar_siguiente_id_wa(empresa_id)
    
    url = f"{repo.base_url}/empresas/{empresa_id}/plantillas_whatsapp?documentId={nombre_id}"
    
    payload = {"fields": {
        "id": {"stringValue": nombre_id},
        "nombre": {"stringValue": p.nombre},
        "id_respond": {"stringValue": p.id_respond},
        "categoria": {"stringValue": p.categoria},
        "lenguaje": {"stringValue": p.lenguaje},
        "mensaje": {"stringValue": p.mensaje},
        "activo": {"booleanValue": bool(p.activo)},
        "variables": {"arrayValue": {"values": [{"stringValue": v} for v in p.variables]}}
    }}
    
    r = requests.post(url, json=payload, headers=repo.headers, timeout=10)
    
    
    if r.status_code == 200 and p.activo:
        TemplateUseCase.asegurar_activacion_unica(repo, empresa_id, nombre_id, p.categoria, "plantillas_whatsapp")
    
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
        
    return {"status": "creada", "id": nombre_id}

@router_wa.patch("/{empresa_id}/{doc_id}")
def api_patch_wa(empresa_id: str, doc_id: str, datos: PlantillaWAUpdate, user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
    
    
    res = repo.actualizar_plantilla_wa(empresa_id, doc_id, datos)
    
  
    if res.status_code == 200 and datos.activo is True:
        doc_actual = repo.obtener_un_doc_completo_wa(empresa_id, doc_id)
        if doc_actual:
            # Extraemos la categoría del JSON de Firebase
            fields = doc_actual.get("fields", {})
            categoria = fields.get("categoria", {}).get("stringValue")
            if categoria:
                TemplateUseCase.asegurar_activacion_unica(repo, empresa_id, doc_id, categoria, "plantillas_whatsapp")
                
    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail=res.text)

    return {"status": "actualizada", "id": doc_id}

@router_wa.delete("/{empresa_id}/{doc_id}", tags=["CRUD WhatsApp"])
def api_eliminar_plantilla_wa(empresa_id: str, doc_id: str, user: dict = Depends(es_admin)):
    """Elimina permanentemente una plantilla de WhatsApp."""
    repo = FirebaseRepository()
    url = f"{repo.base_url}/empresas/{empresa_id}/plantillas_whatsapp/{doc_id}"
    res = requests.delete(url, headers=repo.headers, timeout=10)

    if res.status_code not in [200, 204]:
        raise HTTPException(
            status_code=400, 
            detail=f"Error al intentar eliminar en Firebase: {res.text}"
        )
        
    return {"status": "eliminada", "id": doc_id}

@router_wa.get("/{empresa_id}", tags=["CRUD WhatsApp"])
def api_get_listado_wa(empresa_id: str, user: dict = Depends(es_admin)):
    """Obtiene el listado completo con todos los datos de cada plantilla."""
    repo = FirebaseRepository()
    docs = repo.listar_plantillas_wa(empresa_id)
    
    resultado = []
    for d in docs:
        f = d.get("fields", {})
        resultado.append({
            "id": d["name"].split("/")[-1],
            "nombre": f.get("nombre", {}).get("stringValue", ""),
            "id_respond": f.get("id_respond", {}).get("stringValue", ""),
            "categoria": f.get("categoria", {}).get("stringValue", ""),
            "lenguaje": f.get("lenguaje", {}).get("stringValue", ""),
            "mensaje": f.get("mensaje", {}).get("stringValue", ""),
            "activo": f.get("activo", {}).get("booleanValue", False),
            "variables": [v.get("stringValue") for v in f.get("variables", {}).get("arrayValue", {}).get("values", [])]
        })
    return resultado

@router_wa.get("/{empresa_id}/{doc_id}", tags=["CRUD WhatsApp"])
def api_get_detalle_plantilla_wa(empresa_id: str, doc_id: str, user: dict = Depends(es_admin)):
    """Trae la totalidad de la información de una sola plantilla."""
    repo = FirebaseRepository()
    doc = repo.obtener_un_doc_completo_wa(empresa_id, doc_id)
    
    if not doc:
        raise HTTPException(status_code=404, detail="Esa plantilla no existe.")
        
    f = doc.get("fields", {})
    return {
        "id": doc["name"].split("/")[-1],
        "nombre": f.get("nombre", {}).get("stringValue", ""),
        "id_respond": f.get("id_respond", {}).get("stringValue", ""),
        "categoria": f.get("categoria", {}).get("stringValue", ""),
        "lenguaje": f.get("lenguaje", {}).get("stringValue", ""),
        "mensaje": f.get("mensaje", {}).get("stringValue", ""),
        "activo": f.get("activo", {}).get("booleanValue", False),
        "variables": [v.get("stringValue") for v in f.get("variables", {}).get("arrayValue", {}).get("values", [])]
    }
    

# --- SWITCHES POR LOTE ---
@router_usuario.patch("/email/{empresa_id}/{client_id}/{folio}")
def switch_email_lote_usuario(empresa_id: str, client_id: str, folio: str, estado: bool, db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    func = PROVIDERS.get(empresa_id, {}).get("set_email_lote")
    if func and func(client_id, folio, estado, db):
        return {"status": "ok", "tipo": "lote", "folio": folio, "email_lote_activo": estado}
    raise HTTPException(status_code=404, detail="Relación lote-cliente no encontrada")

# --- SWITCH POR LOTE WHATSAPP ---
@router_usuario.patch("/whatsapp/{empresa_id}/{client_id}/{folio}")
def switch_wa_lote_usuario(empresa_id: str, client_id: str, folio: str, estado: bool, db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    func = PROVIDERS.get(empresa_id, {}).get("set_wa_lote")
    if func and func(client_id, folio, estado, db):
        return {"status": "ok", "tipo": "lote", "folio": folio, "whatsapp_lote_activo": estado}
    raise HTTPException(status_code=404, detail="Relación lote-cliente no encontrada en WA")

# --- 1. VER EL ESTADO DE LOS 3 SWITCHES ---
@router_globales.get("/{empresa_id}")
def api_get_todos_los_switches(empresa_id: str, user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
    config = repo.obtener_config_empresa(empresa_id)
    return {
        "proyecto": config.get("proyecto"),
        "email": config.get("email"),
        "whatsapp": config.get("whatsapp")
    }

# --- 2. APAGAR/ENCENDER PROYECTO (MAESTRO) ---
@router_globales.patch("/proyecto-expediente/{empresa_id}")
def api_switch_expediente(empresa_id: str, estado: bool, user: dict = Depends(es_super_admin)):
    repo = FirebaseRepository()
    # Usamos el esquema ConfigUpdate para mandar solo este campo
    res = repo.actualizar_configuracion(empresa_id, ConfigUpdate(proyecto_activo=estado))
    return {"status": "ok", "proyecto_activo": estado}

# --- 3. APAGAR/ENCENDER EMAIL GLOBAL ---
@router_globales.patch("/email/{empresa_id}")
def api_switch_email(empresa_id: str, estado: bool, user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
    res = repo.actualizar_configuracion(empresa_id, ConfigUpdate(email_enabled=estado))
    return {"status": "ok", "email_global": estado}

# --- 4. APAGAR/ENCENDER WHATSAPP GLOBAL ---
@router_globales.patch("/whatsapp/{empresa_id}")
def api_switch_whatsapp(empresa_id: str, estado: bool, user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
    res = repo.actualizar_configuracion(empresa_id, ConfigUpdate(whatsapp_enabled=estado))
    return {"status": "ok", "whatsapp_global": estado}


@router.get("/monitoreo/fallas/{empresa_id}", tags=["Monitoreo de Logs"])

def api_ver_fallas_pendientes(empresa_id: str, user: dict = Depends(es_admin)):
    """
    Devuelve todos los logs con todos sus campos y un contador de pendientes (no leídos).
    """
    repo = FirebaseRepository()
    url = f"{repo.base_url}/empresas/{empresa_id}:runQuery"
    query = {
        "structuredQuery": {
            "from": [{"collectionId": "logs_fallas"}],
            "orderBy": [{"field": {"fieldPath": "ultima_vez"}, "direction": "DESCENDING"}]
        }
    }
    
    resp = requests.post(url, json=query, headers=repo.headers)
    if resp.status_code != 200: 
        return {"total_pendientes": 0, "logs": []}
    
    datos_crudos = resp.json()
    lista_completa = []
    conteo_no_leidos = 0

    for item in datos_crudos:
        if "document" not in item: continue
        
        doc = item["document"]
        fields = doc.get("fields", {})
   
        log_procesado = {"id": doc["name"].split("/")[-1]}
        
        for key, val in fields.items():
            if "stringValue" in val: log_procesado[key] = val["stringValue"]
            elif "integerValue" in val: log_procesado[key] = int(val["integerValue"])
            elif "booleanValue" in val: log_procesado[key] = val["booleanValue"]
            elif "doubleValue" in val: log_procesado[key] = float(val["doubleValue"])
            elif "timestampValue" in val: log_procesado[key] = val["timestampValue"]

        if not log_procesado.get("leido", False):
            conteo_no_leidos += 1
            
        lista_completa.append(log_procesado)

    return {
        "total_pendientes": conteo_no_leidos, 
        "logs": lista_completa               
    }
@router.patch("/monitoreo/fallas/{empresa_id}/{log_id}/leer", tags=["Monitoreo de Logs"])
def api_marcar_falla_como_leida(empresa_id: str, log_id: str, user: dict = Depends(es_admin)):
    """Cuando ya viste el error, le picas aquí para 'apagarlo'."""
    repo = FirebaseRepository()
    url = f"{repo.base_url}/empresas/{empresa_id}/logs_fallas/{log_id}?updateMask.fieldPaths=leido"
    requests.patch(url, json={"fields": {"leido": {"booleanValue": True}}}, headers=repo.headers)
    return {"status": "ok", "msj": "Notificación apagada"}


# --- SWITCH MASIVO POR ID ---
@router_globales.patch("/etapas/{empresa_id}")
def api_switch_etapas(
    empresa_id: str, 
    estado: bool,            
    ids: List[int],     
    db: Session = Depends(get_db), user: dict = Depends(es_admin)
):
    func = PROVIDERS.get(empresa_id, {}).get("set_etapas_bulk")
    
    if not func:
        raise HTTPException(status_code=404, detail="Empresa no configurada.")

    diccionario_cambios = {id_num: estado for id_num in ids}

    if func(diccionario_cambios, db):
        return {
            "status": "ok",
            "mensaje": f"Se actualizó el estado a {estado} para {len(ids)} IDs.",
            "ids_afectados": ids
        }
    
    raise HTTPException(status_code=400, detail="Error al actualizar en SQL.")


@router_globales.patch("/proyecto/{empresa_id}")
def api_switch_proyecto_completo(
    empresa_id: str, 
    estado: bool,                    
    proyectos: List[str] = Body(...), 
    db: Session = Depends(get_db), user: dict = Depends(es_admin)
):
    func = PROVIDERS.get(empresa_id, {}).get("set_proyecto_bulk")
    
    if not func:
        raise HTTPException(status_code=404, detail="Empresa no configurada.")

    # Llamamos a la lógica enviando ambos parámetros
    if func(proyectos, estado, db):
        return {
            "status": "ok",
            "mensaje": f"Se aplicó {estado} a los proyectos: {', '.join(proyectos)}"
        }
    
    raise HTTPException(status_code=400, detail="Error al actualizar en SQL.")

 
@router_globales.get("/estado-etapas/{empresa_id}")
def api_get_estado_etapas(empresa_id: str, db: Session = Depends(get_db), user: dict = Depends(es_admin)):
    func = PROVIDERS.get(empresa_id, {}).get("get_estado_etapas")
    
    if not func:
        raise HTTPException(status_code=404, detail="Empresa no configurada.")
        
    return func(db)

@router_globales.patch("/config-recordatorios/{empresa_id}")
def api_actualizar_dias_recordatorio(
    empresa_id: str, 
    datos: RecordatoriosUpdate, 
    user: dict = Depends(es_admin)
):
    repo = FirebaseRepository()
    
    res = repo.actualizar_config_recordatorios(empresa_id, datos.dict(exclude_unset=True))
    
    if res is None:
        raise HTTPException(status_code=400, detail="El JSON está vacío o no tiene campos válidos.")
        
    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail=res.text)

    return {"status": "ok", "msj": "Configuración actualizada", "campos": list(datos.dict(exclude_unset=True).keys())}

@router_globales.get("/config-recordatorios/{empresa_id}")
def api_obtener_config_recordatorios(
    empresa_id: str, 
    user: dict = Depends(es_admin)
):
    """
    Recupera los días y la hora programada para los recordatorios de una empresa.
    Si no existe configuración.
    """
    repo = FirebaseRepository()
    config = repo.obtener_config_recordatorios(empresa_id)
    
    return config