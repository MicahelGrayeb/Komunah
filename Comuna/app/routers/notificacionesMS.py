import os
import requests
import re
import base64
import asyncio
import logging
from fastapi import APIRouter, HTTPException, Depends, File, UploadFile, Form, Body
from ..schemas import (
    EmailSchema, PlantillaBase, PlantillaUpdate, ConfigUpdate, 
    EmailManualSchema, PlantillaWAUpdate, PlantillaWABase, 
    WhatsAppManualSchema, SwitchEtapasSchema, EmailFolioSchema, 
    RecordatoriosUpdate, EmailClusterSchema, SearchboxExpedienteResponse, 
    DocumentosDinamicosBase, DocumentosDinamicosUpdate, AnexosBase, AnexosUpdate,
    DocumentoDinamicoGeneracionSchema, FirmantesEmpresaBase, FirmantesEmpresaUpdate,
    MembreteParaHoja, MembreteParaHojaUpdate
)
from ..utils.datos_proveedores import (
    get_komunah_data, set_wa_komunah_lote, set_email_komunah_lote, 
    get_folios_a_notificar_komunah, actualizar_switches_etapas, 
    actualizar_switches_proyecto, get_estado_etapas_komunah, 
    get_folios_deudores_komunah, get_folios_dinamico_komunah
)
from urllib.parse import quote
from ..database import get_db
from sqlalchemy.orm import Session
from zoneinfo import ZoneInfo
import json, time
import mimetypes
from typing import List, Optional, Union, Any
import hashlib
from ..services.security import get_current_user, es_admin, es_super_admin, es_usuario
from argparse import Namespace
from ..models import Venta, Cliente
from ..utils.generacion_documentos_dinamicos import GenerarPDFUseCase, GenerarPDFDinamico
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/v1/notificaciones", tags=["Motor Envios"])
router_crud = APIRouter(prefix="/v1/plantillas", tags=["CRUD Plantillas de Correo"])
router_wa = APIRouter(prefix="/v1/plantillas-wa", tags=["CRUD Plantillas de WhatsApp"])
router_documento = APIRouter(prefix="/v1/plantillas-documento", tags=["CRUD Plantillas de documentos dinamicos"])
router_anexo = APIRouter(prefix="/v1/plantillas-anexo", tags=["CRUD Plantillas de anexos"])
router_membrete = APIRouter(prefix="/v1/membrete-hoja", tags=["CRUD Membrete de Hoja"])
router_firmantes_empresa = APIRouter(prefix="/v1/firmantes-empresa", tags=["CRUD Firmantes de empresa"])
router_usuario = APIRouter(prefix="/v1/preferencias-usuario", tags=["Switches Clientes"])
router_globales = APIRouter(prefix="/v1/configuracion-global", tags=["Configuración Global"])

EMPRESAS_AUTORIZADAS = ["komunah", "empresa_test"]
PROVIDERS = {
    "komunah": {
        "get": get_komunah_data,
        "get_pendientes": get_folios_a_notificar_komunah,
        "get_deudores": get_folios_deudores_komunah,
        "get_folios_por_cluster": get_folios_dinamico_komunah,
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
        from google.cloud import firestore
        import json
        
        # 1. Jalamos el proyecto DIRECTO del .env como pediste
        self.project_id = os.getenv('FIREBASE_PLANTILLAS_PROJECT_ID', '').strip()
        
        if os.path.exists("serviceAccountKey.json"):
            try:
                with open("serviceAccountKey.json") as f:
                    info = json.load(f)
                # Forzamos que use el ID del .env aunque el JSON sea de otro lado
                self.db = firestore.Client.from_service_account_info(info, project=self.project_id)
                logger.info(f"✅ FirebaseRepository: Conectado al proyecto '{self.project_id}'")
            except Exception as e:
                logger.error(f"❌ Error con serviceAccountKey.json: {e}")
                self.db = firestore.Client(project=self.project_id)
        else:
            self.db = firestore.Client(project=self.project_id)
            
        # Variables que tus métodos actuales (de tu compañero) necesitan para no tronar
        self.base_url = f"https://firestore.googleapis.com/v1/projects/{self.project_id}/databases/(default)/documents"
        self.headers = {"Content-Type": "application/json"}

    def obtener_config_empresa(self, empresa_id: str):
        """Si Firebase falla, retorna todo en False para detener procesos."""
        url = f"{self.base_url}/empresas/{empresa_id}/configuracion/general"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=5)
        
        if not resp or not resp.json().get("fields"):
            return {"proyecto": False, "email": False, "whatsapp": False}
            
        f = resp.json().get("fields", {})
        return {
            "proyecto": f.get("proyecto_activo", {}).get("booleanValue", False),
            "email": f.get("email_enabled", {}).get("booleanValue", False),
            "whatsapp": f.get("whatsapp_enabled", {}).get("booleanValue", False)
        }
    
#region Helpers para peticiones seguras con reintentos

    def _peticion_segura(self, method: str, url: str, **kwargs):
        """Proxy blindado contra fallos de red y permisos."""
        try:
            # Extraer ruta limpia
            path_raw = url.split("/documents/")[-1]
            path = path_raw.split("?")[0].split(":")[0]
            
            # A. Extraer documentId de los params (para crear documentos nuevos)
            params = kwargs.get("params", {})
            doc_id_param = None
            if isinstance(params, list): 
                doc_id_param = next((v for k, v in params if k == "documentId"), None)
            elif isinstance(params, dict): 
                doc_id_param = params.get("documentId")
            
            # 1. Resolver Queries
            if ":runQuery" in path_raw:
                query_body = kwargs.get("json", {}).get("structuredQuery", {})
                coll_id = query_body.get("from", [{}])[0].get("collectionId", "plantillas")
                where_clause = query_body.get("where", {}).get("fieldFilter", {})
                field = where_clause.get("field", {}).get("fieldPath", "categoria")
                value = where_clause.get("value", {}).get("stringValue")
                
                # ✅ LA LÍNEA MÁGICA ARREGLADA: Usamos .document(path)
                docs = self.db.document(path).collection(coll_id).where(field, "==", value).stream()
                
                data = [{"document": {"name": d.reference.path, "fields": self._formatear_a_rest(d.to_dict())}} for d in docs]
            
            # 2. Resolver GET
            elif method == "GET":
                parts = [p for p in path.split("/") if p]
                if len(parts) % 2 == 0: 
                    doc = self.db.document(path).get()
                    data = {"fields": self._formatear_a_rest(doc.to_dict())} if doc.exists else {}
                else: 
                    docs = self.db.collection(path).stream()
                    data = {"documents": [{"name": d.reference.path, "fields": self._formatear_a_rest(d.to_dict())} for d in docs]}
            
            # 3. PATCH/POST
            elif method in ["PATCH", "POST"]:
                payload = kwargs.get("json", {}).get("fields", {})
                clean_data = self._limpiar_payload_recursivo(payload)
                
                if doc_id_param: # Crear con ID específico
                    self.db.collection(path).document(doc_id_param).set(clean_data)
                else: # Actualizar existente
                    self.db.document(path).update(clean_data)
                data = {"fields": payload}
                
            # 4. DELETE
            elif method == "DELETE":
                self.db.document(path).delete()
                data = {}

            class MockResponse:
                def __init__(self, json_data): self.json_data = json_data; self.status_code = 200
                def json(self): return self.json_data
            return MockResponse(data)

        except Exception as e:
            # CAPTURAMOS EL ERROR SIN MATAR EL PROCESO
            logger.error(f"⚠️ Fallo en Proxy Firebase: {str(e)}")
            class ErrorResponse:
                def __init__(self): self.status_code = 500; self.text = str(e)
                def json(self): return {}
            return ErrorResponse()

    def _limpiar_payload_recursivo(self, obj):
        """Limpia la basura de {'stringValue': 'x'} para guardar datos limpios en Firebase."""
        if not isinstance(obj, dict): return obj
        # Si es un valor de Firestore, extraemos el contenido real
        for key in ['stringValue', 'integerValue', 'booleanValue', 'doubleValue']:
            if key in obj: return obj[key]
        if 'mapValue' in obj:
            return self._limpiar_payload_recursivo(obj['mapValue'].get('fields', {}))
        if 'arrayValue' in obj:
            return [self._limpiar_payload_recursivo(item) for item in obj['arrayValue'].get('values', [])]
        # Si es un dict normal (como el raíz de fields), limpiamos sus hijos
        return {k: self._limpiar_payload_recursivo(v) for k, v in obj.items()}

    def _formatear_a_rest(self, data: dict):
        """Traduce dict nativo al formato {'fields': {'key': {'stringValue': 'val'}}}."""
        if not data: return {}
        res = {}
        for k, v in data.items():
            if isinstance(v, bool): res[k] = {"booleanValue": v}
            elif isinstance(v, (int, float)): res[k] = {"integerValue": str(v)}
            elif isinstance(v, list): res[k] = {"arrayValue": {"values": [{"stringValue": str(i)} for i in v]}}
            elif isinstance(v, dict): res[k] = {"mapValue": {"fields": self._formatear_a_rest(v)}}
            else: res[k] = {"stringValue": str(v)}
        return res

    def obtener_plantilla_segura(self, empresa_id: str, slug: str):
        """Trae el HTML de una plantilla con reintentos."""
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas/{slug}"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=10)
        
        if not resp:
            return None
            
        data = resp.json()
        return data.get("fields", {}).get("html", {}).get("stringValue", "")

#endregion

#region Operaciones con plantillas y configuraciones

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
        resp = self._peticion_segura("POST", url, json=query, headers=self.headers, timeout=10)
        return resp.json() if resp else []

    def patch_activo_status(self, doc_path: str, status: bool):
        url = f"https://firestore.googleapis.com/v1/{doc_path}?updateMask.fieldPaths=activo"
        payload = {"fields": {"activo": {"booleanValue": status}}}
        return self._peticion_segura("PATCH", url, json=payload, headers=self.headers, timeout=10)
    
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
        return self._peticion_segura("PATCH", url, json={"fields": fields}, headers=self.headers, timeout=10)

#endregion

#region CRUD PLANTILLAS DE CORREO

    def eliminar_plantilla(self, empresa_id: str, doc_id: str):
        """Elimina físicamente el documento."""
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas/{doc_id}"
        return self._peticion_segura("DELETE", url, headers=self.headers, timeout=10)

    def actualizar_plantilla(self, empresa_id: str, doc_id: str, p: PlantillaUpdate):
        """Actualiza campos específicos usando updateMask de manera dinámica."""
        fields = {}
        mask = []
        data = p.dict(exclude_unset=True)
        for key, value in data.items():
            if value is None: continue 
            mask.append(key)
            if key in ["activo", "static"]:
                fields[key] = {"booleanValue": bool(value)}
            elif key in ["tags_departamento"]:
                fields[key] = {"arrayValue": {"values": [{"stringValue": str(v)} for v in value]}}
            elif key == "documentos_adjuntos":
                if isinstance(value, dict):
                    fire_map = {k: {"stringValue": str(v)} for k, v in value.items()}
                    fields[key] = {"mapValue": {"fields": fire_map}}
                else:
                    mapeo = self._get_documento_mapping_multiple(empresa_id, value)
                    if mapeo:
                        fire_map = {k: {"stringValue": v} for k, v in mapeo.items()}
                        fields[key] = {"mapValue": {"fields": fire_map}}
            else:
                fields[key] = {"stringValue": str(value)}
        
        if not mask: 
            return None 

        params = [("updateMask.fieldPaths", m) for m in mask]
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas/{doc_id}"
        return self._peticion_segura("PATCH", url, json={"fields": fields}, params=params, headers=self.headers, timeout=10)
    
    def listar_todas_plantillas(self, empresa_id: str):
        """Para el GET de la lista completa."""
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=10)
        return resp.json().get("documents", []) if resp else []

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
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=10)
        return resp.json() if resp else None
    
#endregion

#region CRUD PLANTILLAS DE WHATSAPP

    def obtener_un_doc_completo_wa(self, empresa_id: str, doc_id: str):
        """Busca un solo documento en la colección de WhatsApp."""
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas_whatsapp/{doc_id}"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=10)
        return resp.json() if resp else None
    
    def listar_plantillas_wa(self, empresa_id: str):
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas_whatsapp"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=10)
        return resp.json().get("documents", []) if resp else []

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
        """Actualiza campos de WhatsApp de manera dinámica y flexible."""
        fields = {}
        mask = []
        # Usamos model_dump (Pydantic v2) o dict() asegurando los nombres de campo internos
        data = p.model_dump(exclude_unset=True, by_alias=False) if hasattr(p, 'model_dump') else p.dict(exclude_unset=True)
        
        for key, value in data.items():
            if value is None: continue
            mask.append(key)
            
            if key in ["activo", "static"]:
                fields[key] = {"booleanValue": bool(value)}
            elif key in ["variables", "tags_departamento"]:
                fields[key] = {"arrayValue": {"values": [{"stringValue": str(v)} for v in value]}}
            elif key == "documento_adjunto_id":
                # Si es dict, va directo como MapValue (lo que envía el front)
                if isinstance(value, dict):
                    fire_map = {k: {"stringValue": str(v)} for k, v in value.items()}
                    fields[key] = {"mapValue": {"fields": fire_map}}
                else:
                    # Si es lista de IDs, buscamos el mapeo en Jurídico
                    ids = value if isinstance(value, list) else [value]
                    mapeo = self._get_documento_mapping_multiple(empresa_id, ids)
                    if mapeo:
                        fire_map = {k: {"stringValue": v} for k, v in mapeo.items()}
                        fields[key] = {"mapValue": {"fields": fire_map}}
            else:
                # Campos de texto simple (nombre, id_respond, lenguaje, mensaje, categoria)
                fields[key] = {"stringValue": str(value)}

        if not mask:
            return None

        params = [("updateMask.fieldPaths", m) for m in mask]
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas_whatsapp/{doc_id}"
        return self._peticion_segura("PATCH", url, json={"fields": fields}, params=params, headers=self.headers, timeout=10)

#endregion

#region Registro de fallas

    def registrar_log_falla(self, empresa_id: str, mensaje: str, contexto: str):
        """Almacena fallas agrupadas en empresas/{id}/logs_fallas."""
        error_id = hashlib.md5(mensaje.encode()).hexdigest()
        url = f"{self.base_url}/empresas/{empresa_id}/logs_fallas/{error_id}"
        ahora = datetime.now(ZoneInfo("America/Mexico_City")).isoformat()

        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=5)
        
        if resp and resp.status_code == 200:
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
            self._peticion_segura("PATCH", f"{url}?{mask}", json=payload, headers=self.headers, timeout=5) 
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
            self._peticion_segura("PATCH", url, json=payload, headers=self.headers, timeout=5) 

#endregion

#region Configuración de recordatorios

    def obtener_config_recordatorios(self, empresa_id: str):
        """Versión robusta: Si no hay datos o hay error de red, loguea pero no mata el startup."""
        url = f"{self.base_url}/empresas/{empresa_id}/configuracion/recordatorios"
        resp = self._peticion_segura("GET", url)
        
        # Si el Proxy regresó vacío o error 500
        if not resp or resp.status_code != 200 or not resp.json().get("fields"):
            logger.error(f"❌ No se pudo cargar config de {empresa_id}. Usando pausa de seguridad.")
            # Retornamos una hora imposible para que el scheduler no dispare nada por error
            return {"dias_1": 0, "dias_2": 0, "hora": 23, "minuto": 59}
            
        f = resp.json().get("fields", {})
        try:
            return {
                "dias_1": int(f["recordatorio_1"]["integerValue"]),
                "dias_2": int(f["recordatorio_2"]["integerValue"]),
                "hora": int(f["hora_recordatorio"]["integerValue"]),
                "minuto": int(f["minuto_recordatorio"]["integerValue"])
            }
        except Exception as e:
            logger.error(f"❌ Error parseando Firebase: {e}")
            return {"dias_1": 0, "dias_2": 0, "hora": 23, "minuto": 59}

    def obtener_config_recordatorios_seguro(self, empresa_id: str):
        """Retorna None si Firebase falla para que el sync job no haga cambios basura."""
        url = f"{self.base_url}/empresas/{empresa_id}/configuracion/recordatorios"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=5)
        
        if not resp or not resp.json().get("fields"):
            return None
            
        f = resp.json().get("fields", {})
        try:
            return {
                "hora": int(f.get("hora_recordatorio", {}).get("integerValue")),
                "minuto": int(f.get("minuto_recordatorio", {}).get("integerValue", 0))
            }
        except (TypeError, ValueError):
            return None
    
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
        
        return self._peticion_segura("PATCH", full_url, json={"fields": fields}, headers=self.headers, timeout=10)

#endregion

#region CRUD PLANTILLAS DE DOCUMENTOS DINAMICOS

    def listar_plantillas_documentos(self, empresa_id: str):
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas_juridico"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=10)
        return resp.json().get("documents", []) if resp else []

    def obtener_un_doc_completo_documentos(self, empresa_id: str, doc_id: str):
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas_juridico/{doc_id}"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=10)
        return resp.json() if resp else None

    def generar_siguiente_id_documentos(self, empresa_id: str):
        docs = self.listar_plantillas_documentos(empresa_id)
        prefijo = empresa_id[:2].upper()
        max_num = 0
        for d in docs:
            id_doc = d["name"].split("/")[-1]
            match = re.search(rf"{prefijo}-(\d+)", id_doc)
            if match:
                num = int(match.group(1))
                if num > max_num: max_num = num
        return f"{prefijo}-{str(max_num + 1).zfill(4)}"

    def actualizar_plantilla_documentos(self, empresa_id: str, doc_id: str, p: Any):
        fields = {}
        mask = []
        data = p.dict(exclude_unset=True)
        for key, value in data.items():
            if value is None: 
                continue 
            mask.append(key)
            # 1. Tipos Booleanos
            if key in ["activo", "static", "tieneAnexos", "FirmantesEmpresa", "FirmasCoopropietarios", "HojaMembretadaProyecto"]:
                fields[key] = {"booleanValue": bool(value)}
            # 2. Listas (como los tags)
            elif key in ["tags", "FirmantesPersonalizados"]:
                fields[key] = {"arrayValue": {"values": [{"stringValue": str(t)} for t in value]}}
            # 3. Mapas Especiales (Anexos - siguiendo tu lógica de documentos_adjuntos)
            elif key == "anexos":
                if isinstance(value, dict):
                    fire_map = {k: {"stringValue": str(v)} for k, v in value.items()}
                    fields[key] = {"mapValue": {"fields": fire_map}}
                else:
                    mapeo = self._get_anexo_mapping_multiple(empresa_id, value)
                    if mapeo:
                        fire_map = {k: {"stringValue": v} for k, v in mapeo.items()}
                        fields[key] = {"mapValue": {"fields": fire_map}}
            # 4. Otros Mapas (Para archivos_subidos y archivos_subidos_meta)
            # Esto evita que un {} caiga en el else de stringValue
            elif isinstance(value, dict):
                fire_map = {k: {"stringValue": str(v)} for k, v in value.items()}
                fields[key] = {"mapValue": {"fields": fire_map}}
                
            # 5. El "Else" Dinámico para strings (Nombre, HTML, categoria, tamanoDocumento, etc.)
            else:
                fields[key] = {"stringValue": str(value)}
        
        if not mask: 
            return None
        
        query_params = "&".join([f"updateMask.fieldPaths={m}" for m in mask])
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas_juridico/{doc_id}?{query_params}"
        
        return self._peticion_segura("PATCH", url, json={"fields": fields}, headers=self.headers, timeout=10)

    def _get_documento_mapping_single(self, empresa_id: str, doc_id: str):
        """Busca un solo ID en documentos y devuelve {ID: Nombre}."""
        if not doc_id or not isinstance(doc_id, str): return None
        doc = self.obtener_un_doc_completo_documentos(empresa_id, doc_id)
        if not doc: return {doc_id: "N/A"}
        nombre = doc.get("fields", {}).get("nombre", {}).get("stringValue", "N/A")
        return {doc_id: nombre}

    def _get_documento_mapping_multiple(self, empresa_id: str, ids: List[str]):
        """Busca varios IDs y devuelve un diccionario {ID: Nombre}."""
        resultado = {}
        for doc_id in (ids or []):
            mapping = self._get_documento_mapping_single(empresa_id, doc_id)
            if mapping:
                resultado.update(mapping)
        return resultado if resultado else None

#endregion

#region CRUD PLANTILLAS DE ANEXO

    def listar_plantillas_anexo(self, empresa_id: str):
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas_anexo"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=10)
        return resp.json().get("documents", []) if resp else []

    def obtener_un_doc_completo_anexos(self, empresa_id: str, doc_id: str):
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas_anexo/{doc_id}"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=10)
        return resp.json() if resp else None

    def generar_siguiente_id_anexos(self, empresa_id: str):
        docs = self.listar_plantillas_anexo(empresa_id)
        prefijo = empresa_id[:2].upper()
        max_num = 0
        for d in docs:
            id_doc = d["name"].split("/")[-1]
            match = re.search(rf"{prefijo}-(\d+)", id_doc)
            if match:
                num = int(match.group(1))
                if num > max_num: max_num = num
        return f"{prefijo}-{str(max_num + 1).zfill(4)}"

    def actualizar_plantilla_anexos(self, empresa_id: str, doc_id: str, p: Any):
        fields = {}
        mask = []

        if p.nombre: fields["nombre"] = {"stringValue": p.nombre}; mask.append("nombre")
        if p.contenido: fields["contenido"] = {"stringValue": p.contenido}; mask.append("contenido")
        if p.categoria: fields["categoria"] = {"stringValue": p.categoria}; mask.append("categoria")
        if p.subcategorianexo: fields["subcategorianexo"] = {"stringValue": p.subcategorianexo}; mask.append("subcategorianexo")

        if p.encabezado is not None:
            fields["encabezado"] = {"stringValue": p.encabezado}
            mask.append("encabezado")

        if p.footer is not None:
            fields["footer"] = {"stringValue": p.footer}
            mask.append("footer")

        if p.FirmantesEmpresa is not None:
            fields["FirmantesEmpresa"] = {"booleanValue": bool(p.FirmantesEmpresa)}
            mask.append("FirmantesEmpresa")

        if p.FirmasCoopropietarios is not None:
            fields["FirmasCoopropietarios"] = {"booleanValue": bool(p.FirmasCoopropietarios)}
            mask.append("FirmasCoopropietarios")

        if p.FirmantesPersonalizados is not None:
            fields["FirmantesPersonalizados"] = {"arrayValue": {"values": [{"stringValue": str(t)} for t in p.FirmantesPersonalizados]}}
            mask.append("FirmantesPersonalizados")

        if hasattr(p, 'HojaMembretadaProyecto') and p.HojaMembretadaProyecto is not None:
            fields["HojaMembretadaProyecto"] = {"booleanValue": bool(p.HojaMembretadaProyecto)}
            mask.append("HojaMembretadaProyecto")

        if hasattr(p, 'membrete_id') and p.membrete_id is not None:
            fields["membrete_id"] = {"stringValue": p.membrete_id}
            mask.append("membrete_id")

        if hasattr(p, 'tamanoDocumento') and p.tamanoDocumento:
            fields["tamanoDocumento"] = {"stringValue": p.tamanoDocumento}
            mask.append("tamanoDocumento")

        if hasattr(p, 'tags') and p.tags is not None:
            fields["tags"] = {"arrayValue": {"values": [{"stringValue": t} for t in p.tags]}}
            mask.append("tags")

        if not mask: return None

        query_params = "&".join([f"updateMask.fieldPaths={m}" for m in mask])
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas_anexo/{doc_id}?{query_params}"

        return self._peticion_segura("PATCH", url, json={"fields": fields}, headers=self.headers, timeout=10)

#endregion

#region CRUD MEMBRETE DE HOJA

    def listar_membretes(self, empresa_id: str):
        url = f"{self.base_url}/empresas/{empresa_id}/membrete_hoja"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=10)
        return resp.json().get("documents", []) if resp else []

    def obtener_membrete(self, empresa_id: str, doc_id: str):
        url = f"{self.base_url}/empresas/{empresa_id}/membrete_hoja/{doc_id}"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=10)
        return resp.json() if resp else None

    def generar_siguiente_id_membrete(self, empresa_id: str):
        docs = self.listar_membretes(empresa_id)
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

    def actualizar_membrete(self, empresa_id: str, doc_id: str, p: Any):
        fields = {}
        mask = []

        if p.nombre is not None:
            fields["nombre"] = {"stringValue": p.nombre}
            mask.append("nombre")

        if p.categoria is not None:
            fields["categoria"] = {"stringValue": p.categoria}
            mask.append("categoria")

        if not mask:
            return None

        query_params = "&".join([f"updateMask.fieldPaths={m}" for m in mask])
        url = f"{self.base_url}/empresas/{empresa_id}/membrete_hoja/{doc_id}?{query_params}"
        return self._peticion_segura("PATCH", url, json={"fields": fields}, headers=self.headers, timeout=10)

#endregion

#region CRUD PLANTILLAS DE ANEXO (mapping helpers)

    def _get_anexo_mapping_single(self, empresa_id: str, doc_id: str):
        """Busca un solo ID en anexos y devuelve {ID: Nombre}."""
        if not doc_id or not isinstance(doc_id, str): return None
        doc = self.obtener_un_doc_completo_anexos(empresa_id, doc_id)
        if not doc: return {doc_id: "N/A"}
        nombre = doc.get("fields", {}).get("nombre", {}).get("stringValue", "N/A")
        return {doc_id: nombre}

    def _get_anexo_mapping_multiple(self, empresa_id: str, ids: List[str]):
        """Busca varios IDs y devuelve un diccionario {ID: Nombre}."""
        resultado = {}
        for doc_id in (ids or []):
            mapping = self._get_anexo_mapping_single(empresa_id, doc_id)
            if mapping:
                resultado.update(mapping)
        return resultado if resultado else None

#endregion

#region CRUD FIRMANTES EMPRESA

    def listar_firmantes_empresa(self, empresa_id: str):
        url = f"{self.base_url}/empresas/{empresa_id}/firmantes-empresa"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=10)
        return resp.json().get("documents", []) if resp else []

    def obtener_un_doc_completo_firmantes_empresa(self, empresa_id: str, doc_id: str):
        url = f"{self.base_url}/empresas/{empresa_id}/firmantes-empresa/{doc_id}"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=10)
        return resp.json() if resp else None

    def generar_siguiente_id_firmantes_empresa(self, empresa_id: str):
        docs = self.listar_firmantes_empresa(empresa_id)
        prefijo = empresa_id[:2].upper()
        max_num = 0
        for d in docs:
            id_doc = d["name"].split("/")[-1]
            match = re.search(rf"{prefijo}-(\d+)", id_doc)
            if match:
                num = int(match.group(1))
                if num > max_num: max_num = num
        return f"{prefijo}-{str(max_num + 1).zfill(4)}"

    def actualizar_plantilla_firmantes_empresa(self, empresa_id: str, doc_id: str, p: Any):
        fields = {}
        mask = []
        
        if p.nombre: fields["nombre"] = {"stringValue": p.nombre}; mask.append("nombre")
        if p.puesto: fields["puesto"] = {"stringValue": p.puesto}; mask.append("puesto")
        if p.departamento: fields["departamento"] = {"stringValue": p.departamento}; mask.append("departamento")
        if p.email: fields["email"] = {"stringValue": p.email}; mask.append("email")
        if p.activo is not None: fields["activo"] = {"booleanValue": bool(p.activo)}; mask.append("activo")
        
        if not mask: return None
        
        query_params = "&".join([f"updateMask.fieldPaths={m}" for m in mask])
        url = f"{self.base_url}/empresas/{empresa_id}/firmantes-empresa/{doc_id}?{query_params}"
        
        return self._peticion_segura("PATCH", url, json={"fields": fields}, headers=self.headers, timeout=10)

    def _get_firmantes_empresa_mapping_single(self, empresa_id: str, doc_id: str):
        """Busca un solo ID en firmantes de empresa y devuelve {ID: Nombre}."""
        if not doc_id or not isinstance(doc_id, str): return None
        doc = self.obtener_un_doc_completo_firmantes_empresa(empresa_id, doc_id)
        if not doc: return {doc_id: "N/A"}
        nombre = doc.get("fields", {}).get("nombre", {}).get("stringValue", "N/A")
        departamento = doc.get("fields", {}).get("departamento", {}).get("stringValue", "N/A")
        return {doc_id: nombre, "departamento": departamento}

    def _get_firmantes_empresa_mapping_multiple(self, empresa_id: str, ids: List[str]):
        """Busca varios IDs y devuelve un diccionario {ID: Nombre}."""
        resultado = {}
        for doc_id in (ids or []):
            mapping = self._get_firmantes_empresa_mapping_single(empresa_id, doc_id)
            if mapping:
                resultado.update(mapping)
        return resultado if resultado else None

#endregion

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
    def enviar_email_bulk(payloads: List[dict]):
        api_key = os.getenv("MAILERSEND_API_KEY")
        url = "https://api.mailersend.com/v1/bulk-email"
        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }
        return requests.post(url, headers=headers, json=payloads, timeout=20)
    
    @staticmethod
    def enviar_whatsapp(
        numero: str,
        template_name: str,
        language_code: str,
        parametros: list,
        texto_cuerpo: str = "",
        header_document_link: Optional[str] = None,
        header_document_filename: Optional[str] = None,
    ):
        token = os.getenv("RESPOND_IO_TOKEN")
        channel_id = os.getenv("RESPOND_IO_CHANNEL_ID")
        
        identifier = quote(f"phone:{numero}")
        url = f"https://api.respond.io/v2/contact/{identifier}/message"
        
        
        components = []
        if header_document_link:
            header_component = {
                "type": "header",
                "parameters": [
                    {
                        "type": "document",
                        "document": {
                            "link": header_document_link,
                            "caption": header_document_filename or ""
                        }
                    }
                ]
            }
            components.append(header_component)

        components.append(
            {
                "type": "body",
                "text": texto_cuerpo,
                "parameters": [{"type": "text", "text": str(p)} for p in parametros]
            }
        )

        payload = {
            "channelId": int(channel_id),
            "message": {
                "type": "whatsapp_template",
                "template": {
                    "name": template_name,
                    "languageCode": language_code,
                    "components": components
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

    async def ejecutar_envio_wa(self, empresa_id: str, datos: WhatsAppManualSchema, db: Session):
        pack_empresa = PROVIDERS.get(empresa_id, {})
        extraer_datos = pack_empresa.get("get")
        data_sql = extraer_datos(datos.folio, db)

        if not data_sql: raise HTTPException(status_code=404, detail="Folio no hallado")

        # 1. Obtener Plantilla WA
        p_wa_raw = UtilsNotifications._buscar_documento_plantilla(
            self.repo,
            empresa_id,
            datos.categoria,
            "plantillas_whatsapp",
        )
        
        if not p_wa_raw: raise HTTPException(status_code=400, detail="No hay plantilla WA activa")
        f_wa = p_wa_raw["fields"]
        adjunto_pdf = None
        documentos_map = f_wa.get("documento_adjunto_id", {}).get("mapValue", {}).get("fields", {})
        id_documento_wa = next(iter(documentos_map.keys()), None)
        if id_documento_wa:
            pdf_service = GenerarPDFUseCase(self.repo)
            adjuntos_pdf = await pdf_service.generar_pdfs_desde_plantillas(
                    empresa_id=empresa_id,
                    ids_plantillas=[id_documento_wa],
                    folio=str(datos.folio),
                    db=db,
                    subir_bucket=True,
                )
            
            # WhatsApp solo permite un documento; usamos el primero generado.
            if adjuntos_pdf:
                adjunto_pdf = adjuntos_pdf[0]

        link_documento = adjunto_pdf.get("url_descarga") if adjunto_pdf else None
        nombre_documento_wa = adjunto_pdf.get("filename") if adjunto_pdf else None
        if not link_documento:
            archivo_subido = UtilsNotifications._obtener_primer_archivo_subido_como_link(
                f_wa,
                f"Komunah/StaticWA/{datos.folio}",
            )
            if archivo_subido:
                link_documento = archivo_subido.get("url_descarga")
                nombre_documento_wa = archivo_subido.get("filename")

        config_plantilla = {
            "id_respond": f_wa.get("id_respond", {}).get("stringValue"),
            "lenguaje": f_wa.get("lenguaje", {}).get("stringValue"),
            "texto_base": f_wa.get("mensaje", {}).get("stringValue", ""),
            "variables": [v.get("stringValue") for v in f_wa.get("variables", {}).get("arrayValue", {}).get("values", [])]
        }
        reporte = []
        wa_enviados_folio = set()
        for i in range(1, 7):
            nombre = data_sql.get(f"{{c{i}.client_name}}")
            telefono = data_sql.get(f"{{g{i}.telefono}}", "").replace(" ", "").replace("-", "")
            if not nombre or not telefono: continue

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
                texto_cuerpo=texto_listo,
                header_document_link=link_documento,
                header_document_filename=nombre_documento_wa,
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

    async def ejecutar_envio_email_folio(self, empresa_id: str, datos: EmailFolioSchema, db: Session):
        lista_adjuntos = []

        pack_empresa = PROVIDERS.get(empresa_id, {})
        data_sql = pack_empresa.get("get")(datos.folio, db)

        p_email_raw = UtilsNotifications._buscar_documento_plantilla(
            self.repo,
            empresa_id,
            datos.categoria,
            "plantillas",
        )
        
        if not p_email_raw: raise HTTPException(status_code=400, detail="Sin plantilla activa")
        f_email = p_email_raw["fields"]
        ids_documentos = list(f_email.get("documentos_adjuntos", {}).get("mapValue", {}).get("fields", {}).keys())
        if ids_documentos:
            pdf_service = GenerarPDFUseCase(self.repo)
            adjuntos_pdf = await pdf_service.generar_pdfs_desde_plantillas(
                empresa_id=empresa_id,
                ids_plantillas=ids_documentos,
                folio=str(datos.folio),
                db=db,
                    subir_bucket=True,
                )
            
            lista_adjuntos = [
                {
                    "content": adjunto["content"],
                    "filename": adjunto["filename"]
                }
                for adjunto in adjuntos_pdf
            ]

        lista_adjuntos.extend(UtilsNotifications._obtener_adjuntos_archivos_subidos(f_email))

        reporte = []
        emails_enviados_folio = set()
        for i in range(1, 7):
            nombre = data_sql.get(f"{{c{i}.client_name}}")
            email = data_sql.get(f"{{g{i}.email}}")
            phone = data_sql.get(f"{{g{i}.telefono}}", "").replace(" ", "").replace("-", "")

            if not nombre or not email:
                continue

            if email in emails_enviados_folio: continue
            emails_enviados_folio.add(email)

            asunto_listo = UtilsNotifications._limpiar(f_email.get("asunto", {}).get("stringValue", ""), data_sql, nombre, email, phone)
            html_listo = UtilsNotifications._limpiar(f_email.get("html", {}).get("stringValue", ""), data_sql, nombre, email, phone)


            res = self.gateway.enviar_email({
                "from": {"email": os.getenv("MAILERSEND_SENDER"), "name": f"Notificaciones {empresa_id.capitalize()}"},
                "to": [{"email": email, "name": nombre}],
                # "to": [{"email": "brandon.avila@techmaleon.mx", "name": nombre}],
                # "cc": [{"email": "cmezquita@techmaleon.mx"}],
                "subject": asunto_listo,
                "html": html_listo,
                "attachments": lista_adjuntos
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

class StaticDualUseCase:
    def __init__(self, repo: FirebaseRepository, gateway: NotificationGateway):
        self.repo = repo
        self.gateway = gateway

    async def ejecutar_envio_dual(self, empresa_id: str, datos_wa: WhatsAppManualSchema, datos_email: EmailFolioSchema, db: Session):
        motor_wa = StaticWAUseCase(self.repo, self.gateway)
        motor_email = StaticEmailFolioUseCase(self.repo, self.gateway)
        res_wa = await motor_wa.ejecutar_envio_wa(empresa_id, datos_wa, db)
        res_em = await motor_email.ejecutar_envio_email_folio(empresa_id, datos_email, db)
        return {"folio": datos_email.folio, "status": "REAL", "whatsapp": res_wa["detalles"], "email": res_em["detalles"]}

class StaticEmailClusterUseCase:
    def __init__(self, repo: FirebaseRepository, gateway: NotificationGateway):
        self.repo = repo
        self.gateway = gateway

    async def ejecutar_proceso_cluster(self, empresa_id: str, datos: Any, db: Session):
        logger.info(
            "[CLUSTER] Inicio ejecutar_proceso_cluster | empresa=%s | clusters=%s | pipeline_status=%s | simular=%s",
            empresa_id,
            getattr(datos, "clusters", []),
            getattr(datos, "pipeline_status", []),
            getattr(datos, "simular", None),
        )
        pack_empresa = PROVIDERS.get(empresa_id, {})
        buscador_dinamico = pack_empresa.get("get_folios_por_cluster")
        
        if not buscador_dinamico:
            raise HTTPException(status_code=400, detail="Empresa no configurada.")

        # 1. Obtener folios (Filtra por Cluster, Pipeline o Ambos)
        logger.info("[CLUSTER] Consultando folios por cluster/pipeline")
        folios_brutos = buscador_dinamico(datos.clusters, datos.pipeline_status, db)
        logger.info("[CLUSTER] Folios encontrados=%s", len(folios_brutos or []))
        
        # Normalizar exclusiones
        excluir_folios = {str(f).strip() for f in (datos.excluir_folios or [])}
        excluir_emails = {str(e).lower().strip() for e in (datos.excluir_emails or [])}
        excluir_nombres = {str(n).lower().strip() for n in (datos.excluir_clientes or [])}

        reporte_global = []
        conteo = {"exitosos": 0, "bloqueados_sys": 0, "omitidos_user": 0, "excluidos_manual": 0}
        ids_documentos = [str(doc_id).strip() for doc_id in (getattr(datos, "array_documentos", []) or []) if str(doc_id).strip()]
        logger.info("[CLUSTER] Documentos dinamicos solicitados=%s", len(ids_documentos))

        # Preparar la cola para el envío masivo
        cola_bulk_moderna = []

        for f in folios_brutos:
            f_str = str(f).strip()
            logger.info("[CLUSTER] Procesando folio=%s", f_str)
            if f_str in excluir_folios:
                logger.info("[CLUSTER] Folio excluido manualmente=%s", f_str)
                conteo["excluidos_manual"] += 1
                continue

            logger.info("[CLUSTER] Consultando datos SQL para folio=%s", f_str)
            data_sql = get_komunah_data(f_str, db)
            if data_sql.get("{sys.etapa_activa}") == "0":
                logger.info("[CLUSTER] Folio bloqueado por sys.etapa_activa=0 | folio=%s", f_str)
                conteo["bloqueados_sys"] += 1
                continue

            adjuntos_dinamicos = []
            if ids_documentos:
                try:
                    logger.info("[CLUSTER] Generando PDFs dinamicos | folio=%s | cantidad=%s", f_str, len(ids_documentos))
                    pdf_service = GenerarPDFUseCase(self.repo)
                    adjuntos_pdf = await pdf_service.generar_pdfs_desde_plantillas(
                        empresa_id=empresa_id,
                        ids_plantillas=ids_documentos,
                        folio=f_str,
                        db=db,
                        subir_bucket=True,
                    )
                    adjuntos_dinamicos = [
                        {
                            "content": adjunto["content"],
                            "filename": adjunto["filename"],
                        }
                        for adjunto in adjuntos_pdf
                    ]
                    logger.info("[CLUSTER] PDFs generados correctamente | folio=%s | cantidad=%s", f_str, len(adjuntos_dinamicos))
                except Exception as e:
                    logger.exception("[CLUSTER] Error generando PDFs dinamicos | folio=%s | error=%s", f_str, str(e))
                    self.repo.registrar_log_falla(
                        empresa_id,
                        f"Folio {f_str}: falló generación de PDF en envío cluster ({str(e)})",
                        "PDF_GEN_CLUSTER",
                    )

            clientes_lote = []
            for i in range(1, 7): # Mapeo dinámico c1 a c6
                nombre = data_sql.get(f"{{c{i}.client_name}}")
                
                # FALLBACK: Si no hay email en GestionClientes, usar el de la tabla Cliente
                email = data_sql.get(f"{{g{i}.email}}") or data_sql.get(f"{{c{i}.email}}")
                phone = data_sql.get(f"{{g{i}.telefono}}", "").replace(" ", "")
                
                # PERMISO: Si no existe registro en Gestion, asumimos que SI permite (1)
                permiso_raw = data_sql.get(f"{{g{i}.permite_email_lote}}")
                permiso = str(permiso_raw) in ["1", "True", "None"] if permiso_raw is not None else True

                if not nombre or not email: continue
                if nombre.lower().strip() in excluir_nombres or email.lower().strip() in excluir_emails:
                    logger.info("[CLUSTER] Cliente excluido manualmente | folio=%s | cliente=%s | email=%s", f_str, nombre, email)
                    conteo["excluidos_manual"] += 1
                    continue

                # Limpieza con tus etiquetas {cliente}, {cl.monto}, etc.
                asunto_final = UtilsNotifications._limpiar(datos.asunto, data_sql, nombre, email, phone)
                html_final = UtilsNotifications._limpiar(datos.contenido_html, data_sql, nombre, email, phone)

                # Si es envío REAL y tiene permiso, armamos el objeto para la cola
                if not datos.simular and permiso:
                    adjuntos_finales = list(datos.adjuntos) if datos.adjuntos else []
                    if adjuntos_dinamicos:
                        adjuntos_finales.extend(adjuntos_dinamicos)

                    email_obj = {
                        "from": {"email": datos.remitente, "name": f"Notificaciones {empresa_id.capitalize()}"},
                        "to": [{"email": email, "name": nombre}],
                        "subject": asunto_final,
                        "html": html_final,
                        "attachments": adjuntos_finales,
                        "reply_to": {"email": datos.reply_to} if datos.reply_to else None
                    }
                    cola_bulk_moderna.append(email_obj)
                    conteo["exitosos"] += 1
                elif datos.simular:
                    conteo["exitosos"] += 1
                else:
                    logger.info("[CLUSTER] Cliente omitido por permiso de lote OFF | folio=%s | cliente=%s", f_str, nombre)

                clientes_lote.append({"cliente": nombre, "email": email, "status": "OK"})

            if clientes_lote:
                reporte_global.append({"folio": f_str, "clientes": clientes_lote})

        
        if not datos.simular and cola_bulk_moderna:
            logger.info("[CLUSTER] Iniciando envio bulk | correos=%s", len(cola_bulk_moderna))
            for i in range(0, len(cola_bulk_moderna), 500):
                bloque = cola_bulk_moderna[i:i + 500]
                logger.info("[CLUSTER] Enviando bloque bulk | inicio=%s | tamano_bloque=%s", i, len(bloque))
                try:
                    res_bulk = self.gateway.enviar_email_bulk(bloque)
                    if res_bulk.status_code not in [200, 201, 202]:
                        raise RuntimeError(f"Bulk HTTP falló ({res_bulk.status_code}): {res_bulk.text[:200]}")
                except Exception as e:
                    logger.warning(
                        "[CLUSTER] Bulk HTTP falló, aplicando fallback a envio individual | error=%s",
                        str(e),
                    )
                    for payload in bloque:
                        res = self.gateway.enviar_email(payload)
                        if res.status_code not in [200, 201, 202]:
                            destino = payload.get("to", [{}])[0].get("email", "N/A")
                            self.repo.registrar_log_falla(
                                empresa_id,
                                f"Email cluster falló ({res.status_code}) para {destino}",
                                "MAIL_PROVIDER_CLUSTER",
                            )
                        time.sleep(0.1)
                time.sleep(0.5)
            logger.info("[CLUSTER] Envio bulk finalizado")

        logger.info("[CLUSTER] Fin ejecutar_proceso_cluster | modo=%s | resumen=%s", "SIMULACION" if datos.simular else "REAL", conteo)

        return {"modo": "SIMULACION" if datos.simular else "REAL", "resumen": conteo, "detalles": reporte_global}

class UtilsNotifications:
    @staticmethod
    def _limpiar(texto, vars, nombre, email_persona, tel_persona):
        """Reemplaza etiquetas dinámicas en plantillas."""
        if not texto:
            return texto

        data = dict(vars or {})
        # Soporte case-insensitive para etiquetas del HTML.
        for k, v in list(data.items()):
            data[str(k).lower()] = v

        # Variables generales fijas del diccionario maestro.
        data["{cliente}"] = nombre or data.get("{cliente}") or data.get("{cl.cliente}") or ""
        data["{email_cliente}"] = str(email_persona or data.get("{email_cliente}") or data.get("{g1.email}") or "")
        data["{telefono_cliente}"] = str(tel_persona or data.get("{telefono_cliente}") or data.get("{g1.telefono}") or "")

        # 1. CAMBIO: Buscamos etiquetas de forma selectiva (solo Alfanuméricos y puntos)
        regex_etiquetas = r"\{[a-zA-Z0-9_\.]+\}"

        etiquetas_en_texto = set(re.findall(regex_etiquetas, texto))
        for tag in etiquetas_en_texto:
            valor = data.get(tag)
            if valor is None:
                valor = data.get(tag.lower())
            if valor is not None:
                texto = texto.replace(tag, str(valor))

        # 2. Verificación de pendientes (ya era específica, la dejamos igual o similar)
        pendientes = set(re.findall(regex_etiquetas, texto))
        conocidas_no_resueltas = [
            t for t in pendientes
            if re.match(r"^\{(cliente|email_cliente|telefono_cliente|ven\.|v\.|p\.|cl\.|sys\.|c[1-6]\.|g[1-6]\.).+\}$", t)
        ]

        if conocidas_no_resueltas:
            logger.info(
                "[PDF] Etiquetas conocidas sin valor en _limpiar | total=%s | muestra=%s",
                len(conocidas_no_resueltas),
                conocidas_no_resueltas[:8],
            )

        # 3. CAMBIO CRÍTICO: Limpieza final selectiva.
        # Solo borra lo que parece una etiqueta de variable, ignorando bloques CSS.
        return re.sub(regex_etiquetas, "", texto)

    @staticmethod
    async def _normalizar_payload_y_archivos(
        model_cls,
        datos_modelo=None,
        datos_json: Optional[str] = None,
        archivos: Optional[List[UploadFile]] = None,
        allow_empty_payload: bool = False,
    ):
        if datos_json:
            try:
                data = json.loads(datos_json)
                payload = model_cls(**data)
            except Exception as exc:
                raise HTTPException(status_code=400, detail=f"datos_json inválido: {str(exc)}") from exc
        elif datos_modelo is not None:
            payload = datos_modelo
        else:
            if allow_empty_payload:
                payload = model_cls()
            else:
                raise HTTPException(status_code=400, detail="Debes enviar un body JSON o datos_json (multipart/form-data).")

        archivos_map = {}
        archivos_meta = {}
        if archivos:
            for f in archivos:
                if f and f.filename:
                    contenido = await f.read()
                    archivos_map[f.filename] = base64.b64encode(contenido).decode("utf-8")
                    mime_type = f.content_type or mimetypes.guess_type(f.filename)[0] or "application/octet-stream"
                    archivos_meta[f.filename] = {
                        "mime_type": mime_type,
                        "tipo_visual": "image" if str(mime_type).startswith("image/") else "file",
                    }

        return payload, archivos_map, archivos_meta

    @staticmethod
    def _obtener_adjuntos_archivos_subidos(fields: dict) -> List[dict]:
        archivos_map = fields.get("archivos_subidos", {}).get("mapValue", {}).get("fields", {})
        adjuntos = []
        for nombre_archivo, nodo in archivos_map.items():
            contenido_b64 = nodo.get("stringValue")
            if contenido_b64:
                adjuntos.append({"content": contenido_b64, "filename": nombre_archivo})
        return adjuntos

    @staticmethod
    def _obtener_primer_archivo_subido_como_link(fields: dict, carpeta_bucket: str) -> Optional[dict]:
        adjuntos = UtilsNotifications._obtener_adjuntos_archivos_subidos(fields)
        if not adjuntos:
            return None

        primer_adjunto = adjuntos[0]
        try:
            url_descarga = GenerarPDFUseCase._subir_pdf_a_bucket(
                base64.b64decode(primer_adjunto["content"]),
                carpeta_bucket,
                primer_adjunto["filename"],
            )
            return {"url_descarga": url_descarga, "filename": primer_adjunto["filename"]}
        except Exception:
            return None

    @staticmethod
    def _normalizar_categoria(valor: str) -> str:
        return str(valor or "").strip().lower()

    @staticmethod
    def _es_plantilla_activa(fields: dict) -> bool:
        raw = fields.get("activo", {})
        if "booleanValue" in raw:
            return raw.get("booleanValue") is True
        return str(raw.get("stringValue", "")).strip().lower() == "true"

    @staticmethod
    def _listar_documentos_por_coleccion(repo: FirebaseRepository, empresa_id: str, coleccion: str) -> List[dict]:
        if coleccion == "plantillas":
            return repo.listar_todas_plantillas(empresa_id)
        if coleccion == "plantillas_whatsapp":
            return repo.listar_plantillas_wa(empresa_id)
        if coleccion == "plantillas_juridico":
            return repo.listar_plantillas_documentos(empresa_id)
        return []

    @staticmethod
    def _buscar_documento_plantilla(
        repo: FirebaseRepository,
        empresa_id: str,
        categoria: str,
        coleccion: str,
        solo_activas: bool = True,
        fallback_listado: bool = True,
    ) -> Optional[dict]:
        categoria_objetivo = UtilsNotifications._normalizar_categoria(categoria)
        if not categoria_objetivo:
            return None

        docs_query = repo.query_categoria(empresa_id, categoria, coleccion)
        for item in docs_query:
            doc = item.get("document")
            if not doc:
                continue
            fields = doc.get("fields", {})
            if UtilsNotifications._normalizar_categoria(fields.get("categoria", {}).get("stringValue", "")) != categoria_objetivo:
                continue
            if solo_activas and not UtilsNotifications._es_plantilla_activa(fields):
                continue
            return doc

        if not fallback_listado:
            return None

        docs_all = UtilsNotifications._listar_documentos_por_coleccion(repo, empresa_id, coleccion)
        for doc in docs_all:
            fields = doc.get("fields", {})
            if UtilsNotifications._normalizar_categoria(fields.get("categoria", {}).get("stringValue", "")) != categoria_objetivo:
                continue
            if solo_activas and not UtilsNotifications._es_plantilla_activa(fields):
                continue
            return doc

        return None

#region CRUD Plantillas para correo

@router_crud.get("/{empresa_id}/conteo/{categoria}")
def api_contar_plantillas(empresa_id: str, categoria: str,user: dict = Depends(es_admin)):
    repo = FirebaseRepository()

    total = TemplateUseCase.contar_plantillas_por_categoria(repo, empresa_id, categoria) 
    return {"categoria": categoria, "total": total}

@router_crud.post("/Crear/{empresa_id}", status_code=201)
async def api_crear_plantilla(
    empresa_id: str,
    p: Optional[PlantillaBase] = Body(default=None),
    datos_json: Optional[str] = Form(default=None),
    archivos: Optional[List[UploadFile]] = File(None),
    user: dict = Depends(es_admin),
):
    p, archivos_map, archivos_meta = await UtilsNotifications._normalizar_payload_y_archivos(
        model_cls=PlantillaBase,
        datos_modelo=p,
        datos_json=datos_json,
        archivos=archivos,
    )

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

    if p.documentos_adjuntos:
        if isinstance(p.documentos_adjuntos, dict):
            fire_map = {k: {"stringValue": str(v)} for k, v in p.documentos_adjuntos.items()}
            payload["fields"]["documentos_adjuntos"] = {"mapValue": {"fields": fire_map}}
        else:
            mapeo = repo._get_documento_mapping_multiple(empresa_id, p.documentos_adjuntos)
            if mapeo:
                fire_map = {k: {"stringValue": v} for k, v in mapeo.items()}
                payload["fields"]["documentos_adjuntos"] = {"mapValue": {"fields": fire_map}}

    if archivos_map:
        payload["fields"]["archivos_subidos"] = {
            "mapValue": {
                "fields": {k: {"stringValue": v} for k, v in archivos_map.items()}
            }
        }
        payload["fields"]["archivos_subidos_meta"] = {
            "mapValue": {
                "fields": {
                    k: {
                        "mapValue": {
                            "fields": {
                                "mime_type": {"stringValue": v["mime_type"]},
                                "tipo_visual": {"stringValue": v["tipo_visual"]},
                            }
                        }
                    }
                    for k, v in archivos_meta.items()
                }
            }
        }
    
    r = requests.post(url, json=payload, headers=repo.headers, timeout=10)
    
    if r.status_code == 200 and p.activo:
        TemplateUseCase.asegurar_activacion_unica(repo, empresa_id, nombre_id, p.categoria, "plantillas")
    
    return {"status": "creada", "id": nombre_id, "nombre": p.nombre}
    
@router_crud.patch("/Actualizar/{empresa_id}/{doc_id}")
async def api_actualizar_plantilla(
    empresa_id: str,
    doc_id: str,
    datos: Optional[PlantillaUpdate] = Body(default=None),
    datos_json: Optional[str] = Form(default=None),
    archivos: Optional[List[UploadFile]] = File(None),
    user: dict = Depends(es_admin),
):
    datos, archivos_map, archivos_meta = await UtilsNotifications._normalizar_payload_y_archivos(
        model_cls=PlantillaUpdate,
        datos_modelo=datos,
        datos_json=datos_json,
        archivos=archivos,
        allow_empty_payload=True,
    )

    campos = datos.dict(exclude_unset=True)
    if (not campos or (len(campos) == 1 and "static" in campos)) and not archivos_map:
        raise HTTPException(status_code=400, detail="No enviaste campos válidos para actualizar.")
    repo = FirebaseRepository()

    res = None
    if campos:
        res = repo.actualizar_plantilla(empresa_id, doc_id, datos)

    if archivos_map:
        url = f"{repo.base_url}/empresas/{empresa_id}/plantillas/{doc_id}"
        payload_files = {
            "fields": {
                "archivos_subidos": {
                    "mapValue": {
                        "fields": {k: {"stringValue": v} for k, v in archivos_map.items()}
                    }
                },
                "archivos_subidos_meta": {
                    "mapValue": {
                        "fields": {
                            k: {
                                "mapValue": {
                                    "fields": {
                                        "mime_type": {"stringValue": v["mime_type"]},
                                        "tipo_visual": {"stringValue": v["tipo_visual"]},
                                    }
                                }
                            }
                            for k, v in archivos_meta.items()
                        }
                    }
                }
            }
        }
        res_files = requests.patch(
            url,
            json=payload_files,
            params=[
                ("updateMask.fieldPaths", "archivos_subidos"),
                ("updateMask.fieldPaths", "archivos_subidos_meta"),
            ],
            headers=repo.headers,
            timeout=10,
        )
        if res_files.status_code != 200:
            raise HTTPException(status_code=res_files.status_code, detail=res_files.text)
    
    if res is None:
        if archivos_map:
            return {"status": "actualizada", "id": doc_id, "mensaje": "Archivos actualizados"}
        return {"status": "actualizada", "id": doc_id, "mensaje": "No se detectaron cambios"}
    
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
    
@router_crud.delete("/Eliminar/{empresa_id}/{doc_id}")
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

@router_crud.get("/Listado/{empresa_id}")
def api_get_listado_plantillas(empresa_id: str, user: dict = Depends(es_admin)):
    """Obtiene un listado básico de todas las plantillas de una empresa incluyendo archivos."""
    repo = FirebaseRepository()
    docs = repo.listar_todas_plantillas(empresa_id)
    
    resultado = []
    for d in docs:
        fields = d.get("fields", {})
        id_tecnico = d["name"].split("/")[-1] 
        
        # Extraemos la información básica
        plantilla_data = {
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
            "documentos_adjuntos": {
                k: v.get("stringValue") 
                for k, v in fields.get("documentos_adjuntos", {}).get("mapValue", {}).get("fields", {}).items()
            },
            
            # --- NUEVOS CAMPOS AGREGADOS ---
            
            # Procesamos 'archivos_subidos' (Mapa simple de Nombre: Valor)
            "archivos_subidos": {
                k: v.get("stringValue") 
                for k, v in fields.get("archivos_subidos", {}).get("mapValue", {}).get("fields", {}).items()
            },
            
            # Procesamos 'archivos_subidos_meta' (Mapa de Mapas)
            "archivos_subidos_meta": {
                nombre_archivo: {
                    meta_k: meta_v.get("stringValue")
                    for meta_k, meta_v in info.get("mapValue", {}).get("fields", {}).items()
                }
                for nombre_archivo, info in fields.get("archivos_subidos_meta", {}).get("mapValue", {}).get("fields", {}).items()
            }
            # -------------------------------
        }
        
        resultado.append(plantilla_data)
        
    return resultado

#endregion

#region variables para HTML

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

        universales = ["{cliente}", "{email_cliente}", "{telefono_cliente}", "{fechaDeHoy}"]

        valores_universales = []
        if data_real:
            for tag in universales:
                if tag == "{fechaDeHoy}":
                    valor = data_real.get("{fechaDeHoy}") or data_real.get("{sys.fechaDeHoy}") or datetime.now(ZoneInfo("America/Mexico_City")).strftime('%d/%m/%Y')
                else:
                    valor = data_real.get(tag, "")
                valores_universales.append({"tag": tag, "valor": valor})


        bloque_fijas = {
            "categoria": "Variables Generales Fijas",
            "variables": universales if not data_real else valores_universales
        }

        catalogo.insert(0, bloque_fijas)

        return catalogo
    
    raise HTTPException(status_code=404, detail="Empresa no configurada.")

#endregion

#region Endpoints de Envío Manual de correo y WhatsApp

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

    array_documentos_raw = d.get("arrayDocumentos", [])
    array_documentos = []
    if isinstance(array_documentos_raw, str):
        # Acepta: "KO-0009", "KO-0009, KO-0010" o "KO-0009;KO-0010"
        array_documentos = [
            part.strip()
            for part in re.split(r"[,;]", array_documentos_raw)
            if part.strip()
        ]
    elif isinstance(array_documentos_raw, list):
        # Acepta listas con IDs limpios o elementos con CSV mezclado.
        for item in array_documentos_raw:
            for part in re.split(r"[,;]", str(item)):
                part_limpio = part.strip()
                if part_limpio:
                    array_documentos.append(part_limpio)

    adjuntos_procesados = []

    if archivos:
        for f in archivos:
            
            if f.filename:
                contenido = await f.read()
                adjuntos_procesados.append({
                    "content": base64.b64encode(contenido).decode('utf-8'),
                    "filename": f.filename
                })

    if array_documentos:
        pdf_service = GenerarPDFUseCase(FirebaseRepository())
        adjuntos_pdf = await pdf_service.generar_pdfs_desde_plantillas(
            empresa_id=empresa_id,
            ids_plantillas=array_documentos,
            folio=str(d.get("folio", "")),
            db=db,
            subir_bucket=True,
        )
        for adjunto_pdf in adjuntos_pdf:
            adjuntos_procesados.append({
                "content": adjunto_pdf["content"],
                "filename": adjunto_pdf["filename"],
            })

    categoria = d.get("categoria")
    if categoria:
        repo = FirebaseRepository()
        p_email_raw = UtilsNotifications._buscar_documento_plantilla(
            repo,
            empresa_id,
            categoria,
            "plantillas",
        )
        if p_email_raw:
            adjuntos_procesados.extend(UtilsNotifications._obtener_adjuntos_archivos_subidos(p_email_raw.get("fields", {})))

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
async def api_enviar_wa(empresa_id: str, datos: WhatsAppManualSchema, db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    use_case = StaticWAUseCase(FirebaseRepository(), NotificationGateway())
    return await use_case.ejecutar_envio_wa(empresa_id, datos, db)

@router.post("/{empresa_id}/enviar-email-folio")
async def api_enviar_email(empresa_id: str, datos: EmailFolioSchema, db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    use_case = StaticEmailFolioUseCase(FirebaseRepository(), NotificationGateway())
    return await use_case.ejecutar_envio_email_folio(empresa_id, datos, db)

@router.post("/{empresa_id}/enviar-dual")
async def api_enviar_ambos_manual(
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
    datos_wa = WhatsAppManualSchema(
        folio=datos.folio,
        categoria=datos.categoria,
    )
    
    return await use_case.ejecutar_envio_dual(empresa_id, datos_wa, datos, db)


EJEMPLO_FINAL = {
    "clusters": ["Planta Baja", "Etapa 1"],
    "pipeline_status": ["Contrato Firmado"], # <-- Nuevo filtro
    "remitente": "finanzas@komunah.mx",
    "asunto": "Aviso de Cobranza - Lote {v.numero}",
    "contenido_html": "<h1>Hola {cl.cliente}</h1><p>Tu saldo es {cl.monto_a_pagar}</p>",
    "reply_to": "pagos@komunah.mx",
    "simular": False,
    "excluir_folios": ["1975"],
    "excluir_emails": ["test@test.com"],
    "excluir_clientes": ["Nombre a Excluir"],
    "arrayDocumentos": ["KO-0009", "KO-0010"]  # Puede ser string CSV o lista
}

@router.post("/{empresa_id}/enviar-cluster")
async def api_proceso_cluster(
    empresa_id: str,
    datos_json: str = Form(
        default=json.dumps(EJEMPLO_FINAL, indent=2),
        description="Pega el JSON con la configuración masiva"
    ), 
    archivos: Optional[List[UploadFile]] = File(None), 
    db: Session = Depends(get_db), 
    user: dict = Depends(es_admin)
):
    logger.info("[CLUSTER_API] Entrada api_proceso_cluster | empresa=%s", empresa_id)
    try:
        data_dict = json.loads(datos_json)
        logger.info("[CLUSTER_API] JSON parseado correctamente | keys=%s", list(data_dict.keys()))
        datos_validados = EmailClusterSchema(**data_dict)
        logger.info("[CLUSTER_API] Payload validado con EmailClusterSchema")
    except Exception as e:
        logger.exception("[CLUSTER_API] Error parseando/validando JSON | empresa=%s | error=%s", empresa_id, str(e))
        raise HTTPException(status_code=400, detail=f"Error en formato JSON: {str(e)}")

    array_documentos_raw = data_dict.get("arrayDocumentos", [])
    array_documentos = []
    if isinstance(array_documentos_raw, str):
        array_documentos = [
            part.strip()
            for part in re.split(r"[,;]", array_documentos_raw)
            if part.strip()
        ]
    elif isinstance(array_documentos_raw, list):
        for item in array_documentos_raw:
            for part in re.split(r"[,;]", str(item)):
                part_limpio = part.strip()
                if part_limpio:
                    array_documentos.append(part_limpio)

    adjuntos = []
    if archivos:
        logger.info("[CLUSTER_API] Archivos recibidos=%s", len(archivos))
        for f in archivos:
            if f.filename:
                adjuntos.append({
                    "content": base64.b64encode(await f.read()).decode(),
                    "filename": f.filename
                })

    categoria = data_dict.get("categoria")
    if categoria:
        repo = FirebaseRepository()
        p_email_raw = UtilsNotifications._buscar_documento_plantilla(
            repo,
            empresa_id,
            categoria,
            "plantillas",
        )
        if p_email_raw:
            adjuntos.extend(UtilsNotifications._obtener_adjuntos_archivos_subidos(p_email_raw.get("fields", {})))

    logger.info("[CLUSTER_API] Adjuntos normalizados=%s | arrayDocumentos=%s", len(adjuntos), len(array_documentos))

    config_final = datos_validados.dict()
    config_final['adjuntos'] = adjuntos
    config_final['array_documentos'] = array_documentos

    logger.info("[CLUSTER_API] Ejecutando caso de uso de cluster")
    use_case = StaticEmailClusterUseCase(FirebaseRepository(), NotificationGateway())
    resultado = await use_case.ejecutar_proceso_cluster(empresa_id, Namespace(**config_final), db)
    logger.info("[CLUSTER_API] Fin api_proceso_cluster | empresa=%s | modo=%s", empresa_id, resultado.get("modo"))
    return resultado

#endregion

#region CRUD Plantillas para WhatsApp

@router_wa.post("/Crear/{empresa_id}", status_code=201)
async def api_crear_plantilla_wa(
    empresa_id: str,
    p: Optional[PlantillaWABase] = Body(default=None),
    datos_json: Optional[str] = Form(default=None),
    archivos: Optional[List[UploadFile]] = File(None),
    user: dict = Depends(es_admin),
):
    p, archivos_map, archivos_meta = await UtilsNotifications._normalizar_payload_y_archivos(
        model_cls=PlantillaWABase,
        datos_modelo=p,
        datos_json=datos_json,
        archivos=archivos,
    )

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

    if p.documento_adjunto_id:
        if isinstance(p.documento_adjunto_id, dict):
            fire_map = {k: {"stringValue": str(v)} for k, v in p.documento_adjunto_id.items()}
            payload["fields"]["documento_adjunto_id"] = {"mapValue": {"fields": fire_map}}
        else:
            ids = p.documento_adjunto_id if isinstance(p.documento_adjunto_id, list) else [p.documento_adjunto_id]
            mapeo = repo._get_documento_mapping_multiple(empresa_id, ids)
            if mapeo:
                fire_map = {k: {"stringValue": v} for k, v in mapeo.items()}
                payload["fields"]["documento_adjunto_id"] = {"mapValue": {"fields": fire_map}}

    if archivos_map:
        payload["fields"]["archivos_subidos"] = {
            "mapValue": {
                "fields": {k: {"stringValue": v} for k, v in archivos_map.items()}
            }
        }
        payload["fields"]["archivos_subidos_meta"] = {
            "mapValue": {
                "fields": {
                    k: {
                        "mapValue": {
                            "fields": {
                                "mime_type": {"stringValue": v["mime_type"]},
                                "tipo_visual": {"stringValue": v["tipo_visual"]},
                            }
                        }
                    }
                    for k, v in archivos_meta.items()
                }
            }
        }
    
    r = requests.post(url, json=payload, headers=repo.headers, timeout=10)
    
    
    if r.status_code == 200 and p.activo:
        TemplateUseCase.asegurar_activacion_unica(repo, empresa_id, nombre_id, p.categoria, "plantillas_whatsapp")
    
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=r.text)
        
    return {"status": "creada", "id": nombre_id}

@router_wa.patch("/Actualizar/{empresa_id}/{doc_id}")
async def api_patch_wa(
    empresa_id: str,
    doc_id: str,
    datos: Optional[PlantillaWAUpdate] = Body(default=None),
    datos_json: Optional[str] = Form(default=None),
    archivos: Optional[List[UploadFile]] = File(None),
    user: dict = Depends(es_admin),
):
    # Normalizamos la entrada (soporta JSON o Form-Data con archivos)
    datos, archivos_map, archivos_meta = await UtilsNotifications._normalizar_payload_y_archivos(
        model_cls=PlantillaWAUpdate,
        datos_modelo=datos,
        datos_json=datos_json,
        archivos=archivos,
        allow_empty_payload=True,
    )

    repo = FirebaseRepository()
    campos = datos.model_dump(exclude_unset=True) if hasattr(datos, 'model_dump') else datos.dict(exclude_unset=True)
    
    res = None
    if campos:
        res = repo.actualizar_plantilla_wa(empresa_id, doc_id, datos)

    # Si hay archivos nuevos (imágenes/PDFs), los parcheamos en una segunda llamada
    if archivos_map:
        url_files = f"{repo.base_url}/empresas/{empresa_id}/plantillas_whatsapp/{doc_id}"
        payload_files = {
            "fields": {
                "archivos_subidos": {"mapValue": {"fields": {k: {"stringValue": v} for k, v in archivos_map.items()}}},
                "archivos_subidos_meta": {
                    "mapValue": {
                        "fields": {
                            k: {
                                "mapValue": {
                                    "fields": {
                                        "mime_type": {"stringValue": v["mime_type"]},
                                        "tipo_visual": {"stringValue": v["tipo_visual"]},
                                    }
                                }
                            } for k, v in archivos_meta.items()
                        }
                    }
                }
            }
        }
        res_f = requests.patch(url_files, json=payload_files, headers=repo.headers, params=[
            ("updateMask.fieldPaths", "archivos_subidos"),
            ("updateMask.fieldPaths", "archivos_subidos_meta")
        ])
        if res_f.status_code != 200:
            raise HTTPException(status_code=res_f.status_code, detail=f"Error subiendo archivos: {res_f.text}")

    # Lógica de respuesta y activación exclusiva
    if res is None:
        return {"status": "Actualizado", "id": doc_id, "mensaje": "Archivos actualizados" if archivos_map else "Sin cambios"}

    if res.status_code == 200 and datos.activo is True:
        # Buscamos la categoría actual para apagar las demás
        doc_info = repo.obtener_un_doc_completo_wa(empresa_id, doc_id)
        if doc_info:
            cat = doc_info.get("fields", {}).get("categoria", {}).get("stringValue")
            if cat:
                TemplateUseCase.asegurar_activacion_unica(repo, empresa_id, doc_id, cat, "plantillas_whatsapp")

    if res.status_code != 200:
        raise HTTPException(status_code=res.status_code, detail=res.text)

    return {"status": "Actualizado", "id": doc_id}

@router_wa.delete("/Eliminar/{empresa_id}/{doc_id}")
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

@router_wa.get("/Listado/{empresa_id}")
def api_get_listado_wa(empresa_id: str, user: dict = Depends(es_admin)):
    """Obtiene el listado completo con todos los datos de cada plantilla de WhatsApp."""
    repo = FirebaseRepository()
    docs = repo.listar_plantillas_wa(empresa_id)
    
    resultado = []
    for d in docs:
        f = d.get("fields", {})
        
        # Procesamos la información de la plantilla
        plantilla_wa = {
            "id": d["name"].split("/")[-1],
            "nombre": f.get("nombre", {}).get("stringValue", ""),
            "id_respond": f.get("id_respond", {}).get("stringValue", ""),
            "categoria": f.get("categoria", {}).get("stringValue", ""),
            "lenguaje": f.get("lenguaje", {}).get("stringValue", ""),
            "mensaje": f.get("mensaje", {}).get("stringValue", ""),
            "activo": f.get("activo", {}).get("booleanValue", False),
            "variables": [
                v.get("stringValue") 
                for v in f.get("variables", {}).get("arrayValue", {}).get("values", [])
            ],
            "documento_adjunto_id": {
                k: v.get("stringValue") 
                for k, v in f.get("documento_adjunto_id", {}).get("mapValue", {}).get("fields", {}).items()
            },
            
            # --- NUEVOS CAMPOS PARA ARCHIVOS ---
            
            # Extraemos los archivos subidos (Nombre: Token/ID)
            "archivos_subidos": {
                k: v.get("stringValue") 
                for k, v in f.get("archivos_subidos", {}).get("mapValue", {}).get("fields", {}).items()
            },
            
            # Extraemos los metadatos de los archivos (Nombre: {mime_type, tipo_visual})
            "archivos_subidos_meta": {
                nombre_archivo: {
                    meta_key: meta_val.get("stringValue")
                    for meta_key, meta_val in info.get("mapValue", {}).get("fields", {}).items()
                }
                for nombre_archivo, info in f.get("archivos_subidos_meta", {}).get("mapValue", {}).get("fields", {}).items()
            }
        }
        
        resultado.append(plantilla_wa)
        
    return resultado

#endregion

#region Endpoints de Switches Globales y por Lote

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

#endregion

#region Endpoints de Monitoreo de Logs

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

#endregion

#region Configuración de Recordatorios

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

#endregion

#region Endpoint de Búsqueda de Expedientes para Searchbox

@router.get("/busqueda-expedientes", response_model=List[SearchboxExpedienteResponse])
def api_busqueda_expedientes(db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    # 1. Filtramos activos: Diferente a 'Expirado' y 'Cancelado'
    # Usamos notin_ para que Carlitos solo vea lo que está "vivo"
    expedientes = db.query(Venta).filter(
        Venta.estado_expediente.notin_(['Expirado', 'Cancelado', 'EXPIRADO', 'CANCELADO'])
    ).all()
    
    resultado = []

    for v in expedientes:
        coprops_nombres = []
        ids_a_buscar = []
        
        # 2. Lógica de Integrantes (del 2 al 6)
        # Revisamos si hay nombre y si hay ID para ir a buscar el correo
        for i in range(2, 7):
            nom_val = getattr(v, f"cliente_{i}")
            id_val = getattr(v, f"id_cliente_{i}")

            if nom_val and str(nom_val).strip() not in ["", "None", "NULL"]:
                coprops_nombres.append(str(nom_val))
                if id_val:
                    try:
                        # Limpiamos el ID por si viene como 1331.0
                        ids_a_buscar.append(str(int(float(id_val))))
                    except: pass

        # 3. EL SALTO A LA TABLA CLIENTES: Traemos los correos reales
        correos_list = []
        if ids_a_buscar:
            # Buscamos masivamente los correos de todos los IDs de este folio
            clientes_query = db.query(Cliente.email).filter(Cliente.client_id.in_(ids_a_buscar)).all()
            correos_list = [c.email for c in clientes_query if c.email]

        # 4. Limpieza Canal de Ventas (Quitar la diagonal / y poner N/A)
        canal = str(v.canal_ventas).strip() if v.canal_ventas else "N/A"
        if canal in ["/", "NA", "", "None", "NULL"]:
            canal = "N/A"

        # 5. Armado del JSON para el Front
        resultado.append({
            "folio": str(v.folio),
            "cliente_principal": str(v.cliente or "Sin Nombre"),
            "conteo_copropietarios": len(coprops_nombres),
            "nombres_copropietarios": coprops_nombres,
            "correos_copropietarios": correos_list,
            "proyecto": str(v.desarrollo or "N/A"),
            "cluster": str(v.etapa or "N/A"),
            "lote": str(v.numero or "N/A"),
            "estatus_expediente": str(v.estado_expediente or "Activo"),
            "m2": float(v.metros_cuadrados or 0.0),
            "canal_ventas": canal,
            "asesor": str(v.asesor or "N/A")
        })

    return resultado

#endregion

#region CRUD Plantillas para documentos dinamicos (PDFs)

@router_documento.get("/Documentos")
def listar_documentos(empresa_id: str, user: dict = Depends(es_usuario)):
    repo = FirebaseRepository()
    docs = repo.listar_plantillas_documentos(empresa_id)
    resultado = []
    for d in docs:
        f = d.get("fields", {})
        resultado.append({
            "id": d["name"].split("/")[-1],
            "nombre": f.get("nombre", {}).get("stringValue", ""),
            "categoria": f.get("categoria", {}).get("stringValue", ""),
            "tamanoDocumento": f.get("tamanoDocumento", {}).get("stringValue", ""),
            "FirmantesEmpresa": f.get("FirmantesEmpresa", {}).get("booleanValue", False),
            "FirmasCoopropietarios": f.get("FirmasCoopropietarios", {}).get("booleanValue", False),
            "FirmantesPersonalizados": [v.get("stringValue") for v in f.get("FirmantesPersonalizados", {}).get("arrayValue", {}).get("values", [])],
            "HojaMembretadaProyecto": f.get("HojaMembretadaProyecto", {}).get("booleanValue", False),
            "membrete_id": f.get("membrete_id", {}).get("stringValue", ""),
            "activo": f.get("activo", {}).get("booleanValue", False),
            "static": f.get("static", {}).get("booleanValue", False),
            "tieneAnexos": f.get("tieneAnexos", {}).get("booleanValue", False),
            "anexos": {
                k: v.get("stringValue") for k, v in f.get("anexos", {}).get("mapValue", {}).get("fields", {}).items()
            },
            "tags": [v.get("stringValue") for v in f.get("tags", {}).get("arrayValue", {}).get("values", [])],
            "html": f.get("html", {}).get("stringValue", ""),
            "encabezado": f.get("encabezado", {}).get("stringValue", ""),
            "footer": f.get("footer", {}).get("stringValue", ""),
            "archivos_subidos": {
                k: v.get("stringValue") for k, v in f.get("archivos_subidos", {}).get("mapValue", {}).get("fields", {}).items()
            },
            "archivos_subidos_meta": {
                k: {
                    meta_key: meta_val.get("stringValue")
                    for meta_key, meta_val in info.get("mapValue", {}).get("fields", {}).items()
                }                for k, info in f.get("archivos_subidos_meta", {}).get("mapValue", {}).get("fields", {}).items()
            },
        })
    return resultado

@router_documento.post("/Crear", status_code=201)
async def crear_plantilla_documento(empresa_id: str, datos_json: Optional[str] = Form(default=None), archivos: Optional[List[UploadFile]] = File(None), user: dict = Depends(es_usuario)):
    # 1. Obtenemos los datos del normalizador
    data_obj, archivos_map, archivos_meta = await UtilsNotifications._normalizar_payload_y_archivos(
        model_cls=DocumentosDinamicosBase,
        datos_modelo=None,
        datos_json=datos_json,
        archivos=archivos,
    )
    
    if hasattr(data_obj, "model_dump"):
        data = data_obj.model_dump()
    else:
        data = data_obj.dict()
    
    repo = FirebaseRepository()
    nombre_id = repo.generar_siguiente_id_documentos(empresa_id)
    url = f"{repo.base_url}/empresas/{empresa_id}/plantillas_juridico?documentId={nombre_id}"
    
    # El resto de tu código ahora funcionará porque 'data' ya es un diccionario
    anexos_firestore = {"mapValue": {"fields": {}}}
    anexos_raw = data.get("anexos")

    if anexos_raw:
        mapeo_data = {}
        if isinstance(anexos_raw, dict):
            mapeo_data = anexos_raw
        else:
            mapeo_data = repo._get_anexo_mapping_multiple(empresa_id, anexos_raw) or {}
        
        if mapeo_data:
            anexos_firestore = {
                "mapValue": {
                    "fields": {k: {"stringValue": str(v)} for k, v in mapeo_data.items()}
                }
            }

    payload = {
        "fields": {
            "id": {"stringValue": nombre_id},
            "nombre": {"stringValue": data.get("nombre", "")},
            "categoria": {"stringValue": data.get("categoria", "")},
            "tamanoDocumento": {"stringValue": data.get("tamanoDocumento", "Letter")},
            "FirmantesEmpresa": {"booleanValue": bool(data.get("FirmantesEmpresa", False))},
            "FirmasCoopropietarios": {"booleanValue": bool(data.get("FirmasCoopropietarios", False))},
            "FirmantesPersonalizados": {
                "arrayValue": {
                    "values": [{"stringValue": v} for v in data.get("FirmantesPersonalizados", [])]
                }
            },
            "HojaMembretadaProyecto": {"booleanValue": bool(data.get("HojaMembretadaProyecto", False))},
            "membrete_id": {"stringValue": data.get("membrete_id", "") or ""},
            "activo": {"booleanValue": bool(data.get("activo", False))},
            "static": {"booleanValue": False},
            "anexos": anexos_firestore,
            "tieneAnexos": {"booleanValue": bool(data.get("tieneAnexos", False))},
            "tags": {
                "arrayValue": {
                    "values": [{"stringValue": t} for t in data.get("tags", [])]
                }
            },
            "encabezado": {"stringValue": data.get("encabezado", "") or ""},
            "html": {"stringValue": data.get("html", "")},
            "footer": {"stringValue": data.get("footer", "") or ""},
        }
    }

    if archivos_map:
        payload["fields"]["archivos_subidos"] = {
            "mapValue": {
                "fields": {k: {"stringValue": v} for k, v in archivos_map.items()}
            }
        }
        payload["fields"]["archivos_subidos_meta"] = {
            "mapValue": {
                "fields": {
                    k: {
                        "mapValue": {
                            "fields": {
                                "mime_type": {"stringValue": v["mime_type"]},
                                "tipo_visual": {"stringValue": v["tipo_visual"]},
                            }
                        }
                    } for k, v in archivos_meta.items()
                }
            }
        }

    r = requests.post(url, json=payload, headers=repo.headers, timeout=10)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=f"Error creando plantilla: {r.text}")

    if r.status_code == 200 and data.get("activo"):
        TemplateUseCase.asegurar_activacion_unica(repo, empresa_id, nombre_id, data.get("categoria"), "plantillas_juridico")
        
    return {"status": "creada", "id": nombre_id}

@router_documento.patch("/Actualizar")
async def actualizar_documento(empresa_id: str, doc_id: str, datos_json: Optional[str] = Form(default=None), archivos: Optional[List[UploadFile]] = File(None), user: dict = Depends(es_usuario)):
    # 1. Normalizar datos
    data_obj, archivos_map, archivos_meta = await UtilsNotifications._normalizar_payload_y_archivos(
        model_cls=DocumentosDinamicosUpdate,
        datos_modelo=None,
        datos_json=datos_json,
        archivos=archivos,
    )
    
    # 2. CONVERSIÓN CRÍTICA: Convertir objeto Pydantic a Diccionario
    data = data_obj.model_dump(exclude_unset=True) if hasattr(data_obj, "model_dump") else data_obj.dict(exclude_unset=True)
    
    repo = FirebaseRepository()
    
    # 3. Actualizar datos principales
    res = repo.actualizar_plantilla_documentos(empresa_id, doc_id, data_obj)
    
    #region 4 y 5: Procesar archivos adjuntos y hoja membretada

    # 4. Procesar archivos si existen
    archivos_viejos = data.get("archivos_subidos", {})
    meta_vieja = data.get("archivos_subidos_meta", {})

    # Unimos (los nuevos sobreescriben si tienen el mismo nombre)
    archivos_finales = {**archivos_viejos, **archivos_map}
    meta_final = {**meta_vieja, **archivos_meta}

    if archivos_finales:
        url = f"{repo.base_url}/empresas/{empresa_id}/plantillas_juridico/{doc_id}"
        payload_files = {
            "fields": {
                "archivos_subidos": {
                    "mapValue": {
                        "fields": {k: {"stringValue": v} for k, v in archivos_finales.items()}
                    }
                },
                "archivos_subidos_meta": {
                    "mapValue": {
                        "fields": {
                            k: {
                                "mapValue": {
                                    "fields": {
                                        "mime_type": {"stringValue": v["mime_type"]},
                                        "tipo_visual": {"stringValue": v["tipo_visual"]},
                                    }
                                }
                            } for k, v in meta_final.items()
                        }
                    }
                }
            }
        }
        
        # Corregimos la updateMask para incluir AMBOS campos
        params = [
            ("updateMask.fieldPaths", "archivos_subidos"),
            ("updateMask.fieldPaths", "archivos_subidos_meta")
        ]
        
        res_files = requests.patch(
            url,
            json=payload_files,
            params=params,
            headers=repo.headers,
            timeout=10,
        )
        if res_files.status_code != 200:
            raise HTTPException(status_code=res_files.status_code, detail=f"Error subiendo archivos: {res_files.text}")

    #endregion

    # 6. Lógica de activación única
    if res and res.status_code == 200:
        return {"status": "actualizada", "id": doc_id}
    
    raise HTTPException(status_code=res.status_code if res else 500, detail="No se pudo actualizar en Firestore")

@router_documento.delete("/Eliminar")
def eliminar_documento(empresa_id: str, doc_id: str, user: dict = Depends(es_usuario)):
    repo = FirebaseRepository()
    doc = repo.obtener_un_doc_completo_documentos(empresa_id, doc_id)
    if not doc: raise HTTPException(status_code=404, detail="No existe.")
    
    if doc.get("fields", {}).get("static", {}).get("booleanValue", False):
        raise HTTPException(status_code=403, detail="No puedes borrar una plantilla base del sistema.")
    
    url = f"{repo.base_url}/empresas/{empresa_id}/plantillas_juridico/{doc_id}"
    requests.delete(url, headers=repo.headers, timeout=10)
    return {"status": "eliminada", "id": doc_id}

@router_documento.post("/generar-documento-dinamico")
async def api_generar_subir_documento_dinamico(payload: DocumentoDinamicoGeneracionSchema, db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    logger.info(
        "[PDF_DINAMICO] Endpoint /generar-subir | empresa_id=%s | id_plantilla=%s",
        payload.empresa_id,
        payload.id_plantilla,
    )

    generador = GenerarPDFDinamico(FirebaseRepository())
    return await generador.generar_pdf_por_id_plantilla(
        empresa_id=payload.empresa_id.strip(),
        id_plantilla=payload.id_plantilla.strip(),
        folio=(payload.folio or "").strip() or None,
        coleccion="DocumentosDinamicos",
        db=db,
        subir_bucket=True,
    )

#endregion

#region CRUD Plantillas para anexos

@router_anexo.get("/Listar-anexos")
def listar_anexos(empresa_id: str, user: dict = Depends(es_usuario)):
    repo = FirebaseRepository()
    docs = repo.listar_plantillas_anexo(empresa_id)
    resultado = []
    for d in docs:
        f = d.get("fields", {})
        resultado.append({
            "id": d["name"].split("/")[-1],
            "nombre": f.get("nombre", {}).get("stringValue", ""),
            "categoria": f.get("categoria", {}).get("stringValue", ""),
            "subcategorianexo": f.get("subcategorianexo", {}).get("stringValue", ""),
            "tamanoDocumento": f.get("tamanoDocumento", {}).get("stringValue", ""),
            "FirmantesEmpresa": f.get("FirmantesEmpresa", {}).get("booleanValue", False),
            "FirmasCoopropietarios": f.get("FirmasCoopropietarios", {}).get("booleanValue", False),
            "FirmantesPersonalizados": [v.get("stringValue") for v in f.get("FirmantesPersonalizados", {}).get("arrayValue", {}).get("values", [])],
            "HojaMembretadaProyecto": f.get("HojaMembretadaProyecto", {}).get("booleanValue", False),
            "membrete_id": f.get("membrete_id", {}).get("stringValue", ""),
            "static": f.get("static", {}).get("booleanValue", False),
            "tags": [v.get("stringValue") for v in f.get("tags", {}).get("arrayValue", {}).get("values", [])],
            "encabezado": f.get("encabezado", {}).get("stringValue", ""),
            "contenido": f.get("contenido", {}).get("stringValue", ""),
            "footer": f.get("footer", {}).get("stringValue", ""),
        })
    return resultado

@router_anexo.post("/Crear-anexo", status_code=201)
async def crear_plantilla_anexo(empresa_id: str, datos_json: Optional[str] = Form(default=None), user: dict = Depends(es_usuario)):
    data_obj, _, _ = await UtilsNotifications._normalizar_payload_y_archivos(
        model_cls=AnexosBase,
        datos_modelo=None,
        datos_json=datos_json,
        archivos=None,
    )

    if hasattr(data_obj, "model_dump"):
        data = data_obj.model_dump()
    else:
        data = data_obj.dict()

    repo = FirebaseRepository()
    nombre_id = repo.generar_siguiente_id_anexos(empresa_id)
    url = f"{repo.base_url}/empresas/{empresa_id}/plantillas_anexo?documentId={nombre_id}"

    payload = {"fields": {
        "id": {"stringValue": nombre_id},
        "nombre": {"stringValue": data.get("nombre", "")},
        "categoria": {"stringValue": data.get("categoria", "")},
        "subcategorianexo": {"stringValue": data.get("subcategorianexo", "")},
        "contenido": {"stringValue": data.get("contenido", "")},
        "encabezado": {"stringValue": data.get("encabezado", "") or ""},
        "footer": {"stringValue": data.get("footer", "") or ""},
        "HojaMembretadaProyecto": {"booleanValue": bool(data.get("HojaMembretadaProyecto", False))},
        "membrete_id": {"stringValue": data.get("membrete_id", "") or ""},
        "FirmantesEmpresa": {"booleanValue": bool(data.get("FirmantesEmpresa", False))},
        "FirmasCoopropietarios": {"booleanValue": bool(data.get("FirmasCoopropietarios", False))},
        "FirmantesPersonalizados": {"arrayValue": {"values": [{"stringValue": v} for v in data.get("FirmantesPersonalizados", [])]}},
        "static": {"booleanValue": False},
        "tamanoDocumento": {"stringValue": data.get("tamanoDocumento", "")},
        "tags": {"arrayValue": {"values": [{"stringValue": t} for t in data.get("tags", [])]}}
        }
    }

    r = requests.post(url, json=payload, headers=repo.headers, timeout=10)
    if r.status_code == 200 and data:
        TemplateUseCase.asegurar_activacion_unica(repo, empresa_id, nombre_id, data.get("categoria"), "plantillas_anexo")
    return {"status": "Anexo creado", "id": nombre_id}

@router_anexo.patch("/Actualizar-anexo")
async def actualizar_anexo(empresa_id: str, doc_id: str, datos_json: Optional[str] = Form(default=None), user: dict = Depends(es_usuario)):
    data_obj, _, _ = await UtilsNotifications._normalizar_payload_y_archivos(
        model_cls=AnexosUpdate,
        datos_modelo=None,
        datos_json=datos_json,
        archivos=None,
    )

    repo = FirebaseRepository()
    res = repo.actualizar_plantilla_anexos(empresa_id, doc_id, data_obj)

    if res and res.status_code == 200:
        return {"status": "Anexo actualizado", "id": doc_id}

    raise HTTPException(status_code=res.status_code, detail="No se pudo actualizar en Firestore")

@router_anexo.delete("/Eliminar-anexo")
def eliminar_anexo(empresa_id: str, doc_id: str, user: dict = Depends(es_usuario)):
    repo = FirebaseRepository()
    doc = repo.obtener_un_doc_completo_anexos(empresa_id, doc_id)
    if not doc: raise HTTPException(status_code=404, detail="No existe.")
    
    if doc.get("fields", {}).get("static", {}).get("booleanValue", False):
        raise HTTPException(status_code=403, detail="No puedes borrar una plantilla base del sistema.")
    
    url = f"{repo.base_url}/empresas/{empresa_id}/plantillas_anexo/{doc_id}"
    requests.delete(url, headers=repo.headers, timeout=10)
    return {"status": "Anexo eliminado", "id": doc_id}

@router_anexo.post("/generar-documento-anexo")
async def api_generar_subir_anexo_dinamico(payload: DocumentoDinamicoGeneracionSchema, db: Session = Depends(get_db),user: dict = Depends(es_usuario)):
    logger.info(
        "[PDF_ANEXO] Endpoint /generar-anexo | empresa_id=%s | id_plantilla=%s",
        payload.empresa_id,
        payload.id_plantilla,
    )

    generador = GenerarPDFDinamico(FirebaseRepository())
    return await generador.generar_pdf_por_id_plantilla(
        empresa_id=payload.empresa_id.strip(),
        id_plantilla=payload.id_plantilla.strip(),
        folio=(payload.folio or "").strip() or None,
        coleccion="Anexos",
        db=db,
        subir_bucket=True,
    )

#endregion

#region CRUD Membrete de Hoja

@router_membrete.get("/Listar-membretes")
def listar_membretes(empresa_id: str, user: dict = Depends(es_usuario)):
    repo = FirebaseRepository()
    docs = repo.listar_membretes(empresa_id)
    resultado = []
    for d in docs:
        f = d.get("fields", {})
        resultado.append({
            "id": d["name"].split("/")[-1],
            "nombre": f.get("nombre", {}).get("stringValue", ""),
            "categoria": f.get("categoria", {}).get("stringValue", ""),
            "ImagenMembretada": {
                k: v.get("stringValue")
                for k, v in f.get("ImagenMembretada", {}).get("mapValue", {}).get("fields", {}).items()
            },
            "ImagenMembretada_meta": {
                k: {
                    meta_key: meta_val.get("stringValue")
                    for meta_key, meta_val in info.get("mapValue", {}).get("fields", {}).items()
                }
                for k, info in f.get("ImagenMembretada_meta", {}).get("mapValue", {}).get("fields", {}).items()
            },
        })
    return resultado

@router_membrete.post("/Subir-membrete", status_code=201)
async def subir_membrete(
    empresa_id: str,
    datos_json: Optional[str] = Form(default=None),
    archivos: Optional[List[UploadFile]] = File(None),
    user: dict = Depends(es_usuario),
):
    if not datos_json:
        raise HTTPException(status_code=400, detail="Debes enviar datos_json con los datos del membrete.")
    try:
        data_obj = MembreteParaHoja(**json.loads(datos_json))
    except Exception as exc:
        raise HTTPException(status_code=400, detail=f"datos_json inválido: {str(exc)}") from exc

    imagen_map = {}
    imagen_meta = {}
    if archivos:
        for f in archivos:
            if f and f.filename:
                contenido = await f.read()
                imagen_map[f.filename] = base64.b64encode(contenido).decode("utf-8")
                mime_type = f.content_type or mimetypes.guess_type(f.filename)[0] or "application/octet-stream"
                imagen_meta[f.filename] = {
                    "mime_type": mime_type,
                    "tipo_visual": "image" if str(mime_type).startswith("image/") else "file",
                }

    repo = FirebaseRepository()
    nombre_id = repo.generar_siguiente_id_membrete(empresa_id)
    url = f"{repo.base_url}/empresas/{empresa_id}/membrete_hoja?documentId={nombre_id}"

    payload = {
        "fields": {
            "id": {"stringValue": nombre_id},
            "nombre": {"stringValue": data_obj.nombre},
            "categoria": {"stringValue": data_obj.categoria},
        }
    }

    if imagen_map:
        payload["fields"]["ImagenMembretada"] = {
            "mapValue": {"fields": {k: {"stringValue": v} for k, v in imagen_map.items()}}
        }
        payload["fields"]["ImagenMembretada_meta"] = {
            "mapValue": {
                "fields": {
                    k: {
                        "mapValue": {
                            "fields": {
                                "mime_type": {"stringValue": v["mime_type"]},
                                "tipo_visual": {"stringValue": v["tipo_visual"]},
                            }
                        }
                    }
                    for k, v in imagen_meta.items()
                }
            }
        }

    r = requests.post(url, json=payload, headers=repo.headers, timeout=10)
    if r.status_code != 200:
        raise HTTPException(status_code=r.status_code, detail=f"Error creando membrete: {r.text}")

    return {"status": "Membrete creado", "id": nombre_id}

@router_membrete.patch("/Actualizar-membrete")
async def actualizar_membrete(
    empresa_id: str,
    doc_id: str,
    datos_json: Optional[str] = Form(default=None),
    archivos: Optional[List[UploadFile]] = File(None),
    user: dict = Depends(es_usuario),
):
    # 1. Normalizar datos
    data_obj, archivos_map, archivos_meta = await UtilsNotifications._normalizar_payload_y_archivos(
        model_cls=MembreteParaHojaUpdate,
        datos_modelo=None,
        datos_json=datos_json,
        archivos=archivos,
    )

    # 2. CONVERSIÓN CRÍTICA: Convertir objeto Pydantic a Diccionario
    data = data_obj.model_dump(exclude_unset=True) if hasattr(data_obj, "model_dump") else data_obj.dict(exclude_unset=True)

    repo = FirebaseRepository()

    # 3. Actualizar datos principales
    res = repo.actualizar_membrete(empresa_id, doc_id, data_obj)

    # 4. Procesar archivos si existen
    archivos_viejos = data.get("ImagenMembretada", {})
    meta_vieja = data.get("ImagenMembretada_meta", {})

    # Unimos (los nuevos sobreescriben si tienen el mismo nombre)
    archivos_finales = {**archivos_viejos, **archivos_map}
    meta_final = {**meta_vieja, **archivos_meta}

    if archivos_finales:
        url = f"{repo.base_url}/empresas/{empresa_id}/membrete_hoja/{doc_id}"
        payload_files = {
            "fields": {
                "ImagenMembretada": {
                    "mapValue": {
                        "fields": {k: {"stringValue": v} for k, v in archivos_finales.items()}
                    }
                },
                "ImagenMembretada_meta": {
                    "mapValue": {
                        "fields": {
                            k: {
                                "mapValue": {
                                    "fields": {
                                        "mime_type": {"stringValue": v["mime_type"]},
                                        "tipo_visual": {"stringValue": v["tipo_visual"]},
                                    }
                                }
                            } for k, v in meta_final.items()
                        }
                    }
                }
            }
        }
        
        # Corregimos la updateMask para incluir AMBOS campos
        params = [
            ("updateMask.fieldPaths", "ImagenMembretada"),
            ("updateMask.fieldPaths", "ImagenMembretada_meta")
        ]
        
        res_files = requests.patch(
            url,
            json=payload_files,
            params=params,
            headers=repo.headers,
            timeout=10,
        )
        if res_files.status_code != 200:
            raise HTTPException(status_code=res_files.status_code, detail=f"Error subiendo archivos: {res_files.text}")

    return {"status": "Membrete actualizado", "id": doc_id}

@router_membrete.delete("/Eliminar-membrete")
def eliminar_membrete(empresa_id: str, doc_id: str, user: dict = Depends(es_usuario)):
    repo = FirebaseRepository()
    doc = repo.obtener_membrete(empresa_id, doc_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Membrete no encontrado.")
    url = f"{repo.base_url}/empresas/{empresa_id}/membrete_hoja/{doc_id}"
    requests.delete(url, headers=repo.headers, timeout=10)
    return {"status": "Membrete eliminado", "id": doc_id}

#endregion

#region CRUD Firmantes de empresa

@router_firmantes_empresa.get("/Listar-firmantes-empresa")
def listar_firmantes_empresa(empresa_id: str, user: dict = Depends(es_usuario)):
    repo = FirebaseRepository()
    docs = repo.listar_firmantes_empresa(empresa_id)
    resultado = []
    for d in docs:
        f = d.get("fields", {})
        resultado.append({
            "id": d["name"].split("/")[-1],
            "nombre": f.get("nombre", {}).get("stringValue", ""),
            "puesto": f.get("puesto", {}).get("stringValue", ""),
            "departamento": f.get("departamento", {}).get("stringValue", ""),
            "email": f.get("email", {}).get("stringValue", ""),
            "activo": f.get("activo", {}).get("booleanValue", False)
        })
    return resultado

@router_firmantes_empresa.post("/Agregar-firmante-empresa", status_code=201)
def agregar_firmante_empresa(empresa_id: str, datos_json: Optional[FirmantesEmpresaBase] = Body(default=None), user: dict = Depends(es_usuario)):
    repo = FirebaseRepository()
    nombre_id = repo.generar_siguiente_id_firmantes_empresa(empresa_id)
    url = f"{repo.base_url}/empresas/{empresa_id}/firmantes-empresa?documentId={nombre_id}"
    
    payload = {"fields": {
        "id": {"stringValue": nombre_id},
        "nombre": {"stringValue": datos_json.nombre},
        "puesto": {"stringValue": datos_json.puesto},
        "departamento": {"stringValue": datos_json.departamento},
        "email": {"stringValue": datos_json.email},
        "activo": {"booleanValue": False}
        }
    }
    r = requests.post(url, json=payload, headers=repo.headers, timeout=10)
    if r.status_code == 200 and datos_json:
        return {"status": "Firmante agregado", "id": nombre_id}

@router_firmantes_empresa.patch("/Actualizar-firmante-empresa")
def actualizar_firmante_empresa(empresa_id: str, doc_id: str, datos_json: Optional[FirmantesEmpresaUpdate] = Body(default=None), user: dict = Depends(es_usuario)):
    repo = FirebaseRepository()
    res = repo.actualizar_plantilla_firmantes_empresa(empresa_id, doc_id, datos_json)
    
    if res and res.status_code == 200:
        return {"status": "Firmante actualizado", "id": doc_id}
    
    # Manejo de error por si falla la API de Google
    raise HTTPException(status_code=res.status_code, detail="No se pudo actualizar en Firestore")

@router_firmantes_empresa.delete("/Eliminar-firmante-empresa")
def eliminar_firmante_empresa(empresa_id: str, doc_id: str, user: dict = Depends(es_usuario)):
    repo = FirebaseRepository()
    doc = repo.obtener_un_doc_completo_firmantes_empresa(empresa_id, doc_id)
    if not doc: raise HTTPException(status_code=404, detail="No existe.")
    
    url = f"{repo.base_url}/empresas/{empresa_id}/firmantes-empresa/{doc_id}"
    requests.delete(url, headers=repo.headers, timeout=10)
    return {"status": "Firmante eliminado", "id": doc_id}

#endregion 