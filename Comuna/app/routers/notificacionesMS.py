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
    DocumentosDinamicosBase, DocumentosDinamicosUpdate, AnexosBase, AnexosUpdate
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
from ..utils.generacion_documentos_dinamicos import GenerarPDFUseCase
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)


router = APIRouter(prefix="/v1/notificaciones", tags=["Motor Envios"])
router_crud = APIRouter(prefix="/v1/plantillas", tags=["CRUD Plantillas de Correo"])
router_wa = APIRouter(prefix="/v1/plantillas-wa", tags=["CRUD Plantillas de WhatsApp"])
router_documento = APIRouter(prefix="/v1/plantillas-documento", tags=["CRUD Plantillas de documentos dinamicos"])
router_anexo = APIRouter(prefix="/v1/plantillas-anexo", tags=["CRUD Plantillas de anexos"])
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
        self.project_id = os.getenv('FIREBASE_PLANTILLAS_PROJECT_ID', '').strip()
        self.api_key = os.getenv('FIREBASE_PLANTILLAS_API_KEY')
        self.base_url = f"https://firestore.googleapis.com/v1/projects/{self.project_id}/databases/(default)/documents"
        self.headers = {"X-Goog-Api-Key": self.api_key, "Content-Type": "application/json"}

    def obtener_config_empresa(self, empresa_id: str):
        """Lógica de switches con reintentos."""
        url = f"{self.base_url}/empresas/{empresa_id}/configuracion/general"
        defaults = {"proyecto": True, "email": True, "whatsapp": True}
        
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=5)
        if not resp:
            return defaults
            
        f = resp.json().get("fields", {})
        return {
            "proyecto": f.get("proyecto_activo", {}).get("booleanValue", True),
            "email": f.get("email_enabled", {}).get("booleanValue", True),
            "whatsapp": f.get("whatsapp_enabled", {}).get("booleanValue", True)
        }
    
#region Helpers para peticiones seguras con reintentos

    def _peticion_segura(self, method: str, url: str, **kwargs):
        """Maneja reintentos y logging detallado para peticiones a Firebase."""
        max_retries = 3
        timeout = kwargs.pop('timeout', 10)
        
        for i in range(max_retries):
            try:
                resp = requests.request(method, url, timeout=timeout, **kwargs)
                if resp.status_code == 200:
                    return resp
                
                # Si no es 200, logueamos el aviso y reintentamos si aplica
                logger.warning(f"⚠️ Firebase API ({method}) devolvió status {resp.status_code} para {url}. Intento {i+1}/{max_retries}")
                if i < max_retries - 1:
                    time.sleep(2 ** i) # Backoff exponencial: 1s, 2s, 4s...
            
            except requests.exceptions.Timeout:
                logger.warning(f"⏰ Timeout ({timeout}s) en Firebase API ({method}) para {url}. Intento {i+1}/{max_retries}")
                if i < max_retries - 1:
                    time.sleep(2 ** i)
            
            except requests.exceptions.RequestException as e:
                logger.error(f"❌ Error de red/petición en Firebase API: {str(e)}. Intento {i+1}/{max_retries}")
                if i < max_retries - 1:
                    time.sleep(2 ** i)
                    
        return None

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
        """Trae los días de recordatorio desde Firebase con reintentos."""
        url = f"{self.base_url}/empresas/{empresa_id}/configuracion/recordatorios"
        defaults = {"dias_1": 3, "dias_2": 1, "hora": 10, "minuto": 0}
        
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=5)
        if not resp:
            return defaults
            
        f = resp.json().get("fields", {})
        return {
            "dias_1": int(f.get("recordatorio_1", {}).get("integerValue", 3)),
            "dias_2": int(f.get("recordatorio_2", {}).get("integerValue", 1)),
            "hora": int(f.get("hora_recordatorio", {}).get("integerValue", 10)),
            "minuto": int(f.get("minuto_recordatorio", {}).get("integerValue", 0))
        }
    
    def obtener_config_recordatorios_seguro(self, empresa_id: str):
        """Retorna None si Firebase falla tras reintentos, para que el sync job no reprograme con defaults."""
        url = f"{self.base_url}/empresas/{empresa_id}/configuracion/recordatorios"
        resp = self._peticion_segura("GET", url, headers=self.headers, timeout=5)
        
        if not resp:
            return None
            
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
        
        if p.nombre: fields["nombre"] = {"stringValue": p.nombre}; mask.append("nombre")
        if p.html: fields["html"] = {"stringValue": p.html}; mask.append("html")
        if p.categoria: fields["categoria"] = {"stringValue": p.categoria}; mask.append("categoria")
        if p.tieneAnexos is not None: fields["tieneAnexos"] = {"booleanValue": bool(p.tieneAnexos)}; mask.append("tieneAnexos")

        if hasattr(p, 'tamanoDocumento') and p.tamanoDocumento:
            fields["tamanoDocumento"] = {"stringValue": p.tamanoDocumento}
            mask.append("tamanoDocumento")

        if p.activo is not None: fields["activo"] = {"booleanValue": bool(p.activo)}; mask.append("activo")
        
        if hasattr(p, 'tags_departamento') and p.tags_departamento is not None:
            fields["tags_departamento"] = {"arrayValue": {"values": [{"stringValue": t} for t in p.tags_departamento]}}
            mask.append("tags_departamento")
        
        if hasattr(p, 'anexos') and p.anexos is not None:
                # Decidimos si usamos el dict directo o consultamos el mapeo
                mapeo_data = p.anexos if isinstance(p.anexos, dict) else self._get_anexos_mapping_multiple(empresa_id, p.anexos)
                
                if mapeo_data is not None:
                    fields["anexos"] = {
                        "mapValue": {
                            "fields": {k: {"stringValue": str(v)} for k, v in mapeo_data.items()}
                        }
                    }
                    mask.append("anexos")

        if not mask: return None
        
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
        
        if hasattr(p, 'tamanoDocumento') and p.tamanoDocumento:
            fields["tamanoDocumento"] = {"stringValue": p.tamanoDocumento}
            mask.append("tamanoDocumento")

        if p.activo is not None: fields["activo"] = {"booleanValue": bool(p.activo)}; mask.append("activo")
        
        if hasattr(p, 'tags_departamento') and p.tags_departamento is not None:
            fields["tags_departamento"] = {"arrayValue": {"values": [{"stringValue": t} for t in p.tags_departamento]}}
            mask.append("tags_departamento")
        
        if not mask: return None
        
        query_params = "&".join([f"updateMask.fieldPaths={m}" for m in mask])
        url = f"{self.base_url}/empresas/{empresa_id}/plantillas_anexo/{doc_id}?{query_params}"
        
        return self._peticion_segura("PATCH", url, json={"fields": fields}, headers=self.headers, timeout=10)

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

            cleaner = NotificationUseCase(self.repo, self.gateway)
            asunto_listo = cleaner._limpiar(f_email.get("asunto", {}).get("stringValue", ""), data_sql, nombre, email, phone)
            html_listo = cleaner._limpiar(f_email.get("html", {}).get("stringValue", ""), data_sql, nombre, email, phone)


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

class NotificationUseCase:
    def __init__(self, repo: FirebaseRepository, gateway: NotificationGateway):
        self.repo = repo
        self.gateway = gateway

    async def ejecutar_barrido_automatico(self, empresa_id: str, dias: int, categoria: str, db: Session, tipo: str = "normal", simular: bool = False):
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

        p_email_raw = UtilsNotifications._buscar_documento_plantilla(
            self.repo,
            empresa_id,
            categoria,
            "plantillas",
        )
        p_email = p_email_raw.get("fields", {}) if p_email_raw else None

        p_wa_raw = UtilsNotifications._buscar_documento_plantilla(
            self.repo,
            empresa_id,
            categoria,
            "plantillas_whatsapp",
        )
        
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
        pdf_service = GenerarPDFUseCase(self.repo)

        for row in registros:
            data_sql = extraer_datos(row, db)

            if not data_sql:
                self.repo.registrar_log_falla(empresa_id, f"El folio {row} no trajo info de SQL", "DATOS_SQL")
                continue

            if data_sql.get("{sys.etapa_activa}") == "0":
                motivo = data_sql.get("{sys.bloqueo_motivo}", "Bloqueo por configuración de Etapa/Proyecto")
                self.repo.registrar_log_falla(empresa_id, f"Folio {row} saltado: {motivo}", "BLOQUEO_ADMINISTRATIVO")
                continue

            # --- GENERACIÓN DE PDFs POR FOLIO (una sola vez por folio, no por integrante) ---

            # PDFs para EMAIL: si la plantilla tiene documentos_adjuntos, los generamos dinámicamente
            adjuntos_email_dinamicos = []
            if p_email and sistema_email_ok:
                hijos_email = p_email.get("documentos_adjuntos", {}).get("mapValue", {}).get("fields", {})
                if hijos_email:
                    for doc_id in hijos_email.keys():
                        try:
                            pdf_gen = await pdf_service.generar_pdf_barrido_automatico(empresa_id, doc_id, str(row), db)
                            adjuntos_email_dinamicos.append({"content": pdf_gen["content"], "filename": pdf_gen["filename"]})
                        except Exception:
                            self.repo.registrar_log_falla(empresa_id, f"Folio {row}: falló generación de PDF adjunto '{doc_id}' para email.", "PDF_GEN")
                            continue
                else:
                    # Fallback: adjuntos por URL estáticos (comportamiento anterior)
                    adjuntos_raw = p_email.get("adjuntos_url", {}).get("arrayValue", {}).get("values", [])
                    for adj in adjuntos_raw:
                        info_archivo = self._descargar_a_base64(adj.get("stringValue"))
                        if info_archivo:
                            adjuntos_email_dinamicos.append(info_archivo)

                adjuntos_email_dinamicos.extend(UtilsNotifications._obtener_adjuntos_archivos_subidos(p_email))

            # PDF para WHATSAPP: si la plantilla tiene documento_adjunto_id, lo generamos y subimos al bucket
            link_wa_doc = None
            nom_wa_doc = None
            if p_wa_raw and sistema_wa_ok:
                hijos_wa = p_wa_raw["fields"].get("documento_adjunto_id", {}).get("mapValue", {}).get("fields", {})
                if hijos_wa:
                    pid_wa = list(hijos_wa.keys())[0]
                    try:
                        pdf_wa = await pdf_service.generar_pdf_barrido_automatico(empresa_id, pid_wa, str(row), db)
                        link_wa_doc = GenerarPDFUseCase._subir_pdf_a_bucket(
                            base64.b64decode(pdf_wa["content"]),
                            f"Komunah/AutoWA/{row}",
                            pdf_wa["filename"]
                        )
                        nom_wa_doc = pdf_wa["filename"]
                    except Exception:
                        self.repo.registrar_log_falla(empresa_id, f"Folio {row}: falló generación de PDF para WhatsApp.", "PDF_GEN")

                if not link_wa_doc:
                    archivo_subido_wa = UtilsNotifications._obtener_primer_archivo_subido_como_link(
                        p_wa_raw["fields"],
                        f"Komunah/AutoWA/{row}",
                    )
                    if archivo_subido_wa:
                        link_wa_doc = archivo_subido_wa.get("url_descarga")
                        nom_wa_doc = archivo_subido_wa.get("filename")

            emails_enviados_folio = set()
            wa_enviados_folio = set()

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
                elif email in emails_enviados_folio:
                    resultado_envio["email"] = "DUPLICADO_EN_ESTE_FOLIO"
                else:
                    emails_enviados_folio.add(email)
                    if simular:
                        res_status = f"SIMULADO_OK ({len(adjuntos_email_dinamicos)} PDFs listos)"
                    else:
                        res_mail = self.gateway.enviar_email({
                            "from": {"email": os.getenv("MAILERSEND_SENDER"), "name": f"Notificaciones {empresa_id}"},
                            "to": [{"email": email, "name": nombre}],
                            "subject": self._limpiar(p_email["asunto"]["stringValue"], data_sql, nombre, email, phone),
                            "html": self._limpiar(p_email["html"]["stringValue"], data_sql, nombre, email, phone),
                            "attachments": adjuntos_email_dinamicos
                        })
                        if res_mail.status_code not in [200, 201, 202]:
                            self.repo.registrar_log_falla(empresa_id, f"Email falló ({res_mail.status_code}) para {email}", "MAIL_PROVIDER")
                        res_status = f"Status: {res_mail.status_code} | {res_mail.text[:100]}"
                    
                    resultado_envio["email"] = res_status

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
                elif phone in wa_enviados_folio:
                    resultado_envio["wa"] = "DUPLICADO_EN_ESTE_FOLIO"
                else:
                    wa_enviados_folio.add(phone)
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

                    if simular:
                        res_wa_status = f"SIMULADO_OK (PDF: {nom_wa_doc if nom_wa_doc else 'Sin adjunto'})"
                    else:
                        res_wa = self.gateway.enviar_whatsapp(
                            num_wa, p_wa["id_respond"], p_wa["lenguaje"], parametros_dinamicos,
                            texto_cuerpo=texto_completo,
                            header_document_link=link_wa_doc,
                            header_document_filename=nom_wa_doc
                        )
                        if res_wa.status_code not in [200, 201, 202]:
                            self.repo.registrar_log_falla(empresa_id, f"WhatsApp falló ({res_wa.status_code}) para {phone}", "WA_PROVIDER")
                        res_wa_status = f"Status: {res_wa.status_code}"
                        
                    resultado_envio["wa"] = res_wa_status

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
        procesador = NotificationUseCase(self.repo, self.gateway)
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
                asunto_final = procesador._limpiar(datos.asunto, data_sql, nombre, email, phone)
                html_final = procesador._limpiar(datos.contenido_html, data_sql, nombre, email, phone)

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

@router.post("/auto-notificar/{empresa_id}")
async def api_disparar_barrido(
    empresa_id: str, 
    dias: int, 
    categoria: str,  
    tipo: str = "normal",
    simular: bool = False,
    db: Session = Depends(get_db),
    user: dict = Depends(es_usuario)
):
    """
    Barrido Automático Inteligente:
    1. Busca folios por fecha.
    2. Genera los PDFs vinculados dinámicamente desde Firebase (documentos_adjuntos / documento_adjunto_id).
    3. Simula o Envía según el parámetro 'simular'.
    """
    repo = FirebaseRepository()
    gateway = NotificationGateway()
    use_case = NotificationUseCase(repo, gateway)
    return await use_case.ejecutar_barrido_automatico(empresa_id, dias, categoria, db, tipo=tipo, simular=simular)

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
def listar_documentos(empresa_id: str, user: dict = Depends(es_admin)):
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
            "activo": f.get("activo", {}).get("booleanValue", False),
            "static": f.get("static", {}).get("booleanValue", False),
            "anexos": {k: v.get("stringValue") for k, v in f.get("anexos", {}).get("mapValue", {}).get("fields", {}).items()},
            "tieneAnexos": f.get("tieneAnexos", {}).get("booleanValue", False),
            "anexos": [v.get("stringValue") for v in f.get("anexos", {}).get("arrayValue", {}).get("values", [])],
            "tags": [v.get("stringValue") for v in f.get("tags_departamento", {}).get("arrayValue", {}).get("values", [])],
            "html": f.get("html", {}).get("stringValue", ""),
            "archivos_subidos": {
                k: v.get("stringValue") for k, v in f.get("archivos_subidos", {}).get("mapValue", {}).get("fields", {}).items()
            }   
        })
    return resultado

@router_documento.post("/Crear", status_code=201)
async def crear_plantilla_documento(empresa_id: str, p: DocumentosDinamicosBase, archivos: Optional[List[UploadFile]] = File(None), user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
    nombre_id = repo.generar_siguiente_id_documentos(empresa_id)
    url = f"{repo.base_url}/empresas/{empresa_id}/plantillas_juridico?documentId={nombre_id}"
    
    adjuntos_procesados = []

    if archivos:
        for f in archivos:
            if f.filename:
                contenido = await f.read()
                adjuntos_procesados.append({
                    "content": base64.b64encode(contenido).decode('utf-8'),
                    "filename": f.filename,
                    "mime_type": f.content_type,
                    "tipo_visual": "image" if str(f.content_type).startswith("image/") else "file"
                })
    anexos_firestore = {"mapValue": {"fields": {}}}
    if p.anexos:
        mapeo_data = {}
        if isinstance(p.anexos, dict):
            mapeo_data = p.anexos
        else:
            mapeo_data = repo._get_anexos_mapping_multiple(empresa_id, p.anexos) or {}
        
        if mapeo_data:
            anexos_firestore = {
                "mapValue": {
                    "fields": {k: {"stringValue": str(v)} for k, v in mapeo_data.items()}
                }
            }

    payload = {"fields": {
            "id": {"stringValue": nombre_id},
            "nombre": {"stringValue": p.nombre},
            "categoria": {"stringValue": p.categoria},
            "tamanoDocumento": {"stringValue": p.tamanoDocumento},
            "activo": {"booleanValue": bool(p.activo)},
            "static": {"booleanValue": False},
            "anexos": anexos_firestore,
            "tieneAnexos": {"booleanValue": p.tieneAnexos},
            "tags_departamento": {"arrayValue": {"values": [{"stringValue": t} for t in p.tags_departamento]}},
            "html": {"stringValue": p.html},
            "archivos_subidos": {
                "arrayValue": {
                    "values": [
                        {
                            "mapValue": {
                                "fields": {
                                    "filename": {"stringValue": a["filename"]},
                                    "mime_type": {"stringValue": a["mime_type"]},
                                    "tipo_visual": {"stringValue": a["tipo_visual"]},
                                    "content": {"stringValue": a["content"]}
                                }
                            }
                        } for a in adjuntos_procesados
                    ]
                }
            }
        }
    }
    
    r = requests.post(url, json=payload, headers=repo.headers, timeout=10)
    if r.status_code == 200 and p.activo:
        TemplateUseCase.asegurar_activacion_unica(repo, empresa_id, nombre_id, p.categoria, "plantillas_juridico")
    return {"status": "creada", "id": nombre_id}

@router_documento.patch("/Actualizar")
async def actualizar_documento(empresa_id: str, doc_id: str, datos: DocumentosDinamicosUpdate, archivos: Optional[List[UploadFile]] = File(None), user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
    res = repo.actualizar_plantilla_documentos(empresa_id, doc_id, datos)
    
    # Procesar archivos si se enviaron
    if archivos:
        adjuntos_procesados = []
        for f in archivos:
            if f.filename:
                contenido = await f.read()
                adjuntos_procesados.append({
                    "content": base64.b64encode(contenido).decode('utf-8'),
                    "filename": f.filename,
                    "mime_type": f.content_type,
                    "tipo_visual": "image" if str(f.content_type).startswith("image/") else "file"
                })
        
        if adjuntos_procesados:
            url = f"{repo.base_url}/empresas/{empresa_id}/plantillas_juridico/{doc_id}"
            payload_files = {
                "fields": {
                    "archivos_subidos": {
                        "arrayValue": {
                            "values": [
                                {
                                    "mapValue": {
                                        "fields": {
                                            "filename": {"stringValue": a["filename"]},
                                            "mime_type": {"stringValue": a["mime_type"]},
                                            "tipo_visual": {"stringValue": a["tipo_visual"]},
                                            "content": {"stringValue": a["content"]}
                                        }
                                    }
                                } for a in adjuntos_procesados
                            ]
                        }
                    }
                }
            }
            res_files = requests.patch(
                url,
                json=payload_files,
                params=[("updateMask.fieldPaths", "archivos_subidos")],
                headers=repo.headers,
                timeout=10,
            )
            if res_files.status_code != 200:
                raise HTTPException(status_code=res_files.status_code, detail=res_files.text)


    if res and res.status_code == 200:
        if datos.activo is True:
            doc = repo.obtener_un_doc_completo_documentos(empresa_id, doc_id)
            cat = doc.get("fields", {}).get("categoria", {}).get("stringValue")
            if cat:
                TemplateUseCase.asegurar_activacion_unica(repo, empresa_id, doc_id, cat, "plantillas_juridico")
        return {"status": "actualizada", "id": doc_id}
    
    # Manejo de error por si falla la API de Google
    raise HTTPException(status_code=res.status_code, detail="No se pudo actualizar en Firestore")

@router_documento.delete("/Eliminar")
def eliminar_documento(empresa_id: str, doc_id: str, user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
    doc = repo.obtener_un_doc_completo_documentos(empresa_id, doc_id)
    if not doc: raise HTTPException(status_code=404, detail="No existe.")
    
    if doc.get("fields", {}).get("static", {}).get("booleanValue", False):
        raise HTTPException(status_code=403, detail="No puedes borrar una plantilla base del sistema.")
    
    url = f"{repo.base_url}/empresas/{empresa_id}/plantillas_juridico/{doc_id}"
    requests.delete(url, headers=repo.headers, timeout=10)
    return {"status": "eliminada", "id": doc_id}

#endregion

#region CRUD Plantillas para anexos

@router_anexo.get("/Listar-anexos")
def listar_anexos(empresa_id: str, user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
    docs = repo.listar_plantillas_anexo(empresa_id)
    resultado = []
    for d in docs:
        f = d.get("fields", {})
        resultado.append({
            "id": d["name"].split("/")[-1],
            "nombre": f.get("nombre", {}).get("stringValue", ""),
            "categoria": f.get("categoria", {}).get("stringValue", ""),
            "tamanoDocumento": f.get("tamanoDocumento", {}).get("stringValue", ""),
            "static": f.get("static", {}).get("booleanValue", False),
            "tags": [v.get("stringValue") for v in f.get("tags_departamento", {}).get("arrayValue", {}).get("values", [])],
            "contenido": f.get("contenido", {}).get("stringValue", "")
        })
    return resultado

@router_anexo.post("/Crear-anexo", status_code=201)
def crear_plantilla_anexo(empresa_id: str, datos: AnexosBase, user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
    nombre_id = repo.generar_siguiente_id_anexos(empresa_id)
    url = f"{repo.base_url}/empresas/{empresa_id}/plantillas_anexo?documentId={nombre_id}"
    
    payload = {"fields": {
        "id": {"stringValue": nombre_id},
        "nombre": {"stringValue": datos.nombre},
        "categoria": {"stringValue": datos.categoria},
        "contenido": {"stringValue": datos.contenido},
        "tamanoDocumento": {"stringValue": datos.tamanoDocumento},
        "static": {"booleanValue": False},
        "tags_departamento": {"arrayValue": {"values": [{"stringValue": t} for t in datos.tags_departamento]}}
        }
    }
    r = requests.post(url, json=payload, headers=repo.headers, timeout=10)
    if r.status_code == 200 and datos.activo:
        TemplateUseCase.asegurar_activacion_unica(repo, empresa_id, nombre_id, datos.categoria, "plantillas_anexo")
    return {"status": "creada", "id": nombre_id}

@router_anexo.patch("/Actualizar-anexo")
def actualizar_anexo(empresa_id: str, doc_id: str, datos: AnexosUpdate, user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
    res = repo.actualizar_plantilla_anexos(empresa_id, doc_id, datos)
    
    if res and res.status_code == 200:
        if datos.activo is True:
            doc = repo.obtener_un_doc_completo_anexos(empresa_id, doc_id)
            cat = doc.get("fields", {}).get("categoria", {}).get("stringValue")
            if cat:
                TemplateUseCase.asegurar_activacion_unica(repo, empresa_id, doc_id, cat, "plantillas_anexo")
        return {"status": "actualizada", "id": doc_id}
    
    # Manejo de error por si falla la API de Google
    raise HTTPException(status_code=res.status_code, detail="No se pudo actualizar en Firestore")

@router_anexo.delete("/Eliminar-anexo")
def eliminar_anexo(empresa_id: str, doc_id: str, user: dict = Depends(es_admin)):
    repo = FirebaseRepository()
    doc = repo.obtener_un_doc_completo_anexos(empresa_id, doc_id)
    if not doc: raise HTTPException(status_code=404, detail="No existe.")
    
    if doc.get("fields", {}).get("static", {}).get("booleanValue", False):
        raise HTTPException(status_code=403, detail="No puedes borrar una plantilla base del sistema.")
    
    url = f"{repo.base_url}/empresas/{empresa_id}/plantillas_anexo/{doc_id}"
    requests.delete(url, headers=repo.headers, timeout=10)
    return {"status": "eliminada", "id": doc_id}

#endregion 