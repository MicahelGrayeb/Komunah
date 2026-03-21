import os
import requests
import re
import base64
import asyncio
import logging
import io
import zipfile
from playwright.async_api import async_playwright
from google.cloud import storage
from fastapi import APIRouter, HTTPException, Depends, File, UploadFile, Form, Body, Query
from fastapi.responses import StreamingResponse
from ..schemas import (
    DocumentosSchema,
    DocumentosSchemaGeneracion,
    DocumentoDescargaSchema,
    DocumentoEliminarSchema,
    DocumentoGenerarSubirSchema,
    DocumentosFiltroSchema,
)
from ..utils.datos_proveedores import (
    get_komunah_data, set_wa_komunah_lote, set_email_komunah_lote, 
    set_email_komunah_marketing, set_wa_komunah_marketing, 
    get_folios_a_notificar_komunah, actualizar_switches_etapas, 
    actualizar_switches_proyecto, get_estado_etapas_komunah, 
    get_folios_deudores_komunah, get_folios_dinamico_komunah
)
from urllib.parse import quote
from ..database import get_db
from sqlalchemy.orm import Session
from zoneinfo import ZoneInfo
import json, time
from typing import List, Optional, Union, Any, Tuple
import hashlib
from ..services.security import es_usuario
from argparse import Namespace
from ..models import Venta, Cliente, Amortizacion
from datetime import datetime, date, timedelta

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/documentos", tags=["Documentos Google Cloud Storage (Bucket)"])
BUCKET_NAME = "bucket-grupo-komunah-juridico"
BASE_PREFIX_DOCUMENTOS = "Komunah/PlantillasWeb/Categorias/"

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


class GCSDocumentosService:
    def __init__(self, bucket_name: str = BUCKET_NAME, base_prefix: str = BASE_PREFIX_DOCUMENTOS):
        self.bucket_name = bucket_name
        self.base_prefix = base_prefix
        storage_client = self._get_storage_client()
        self.bucket = storage_client.bucket(bucket_name)
        logger.info("[DOCUMENTOS] Servicio GCS inicializado | bucket=%s | base_prefix=%s", bucket_name, base_prefix)

    @staticmethod
    def _get_storage_client() -> storage.Client:
        cred_path = os.getenv("STORAGE_CREDENTIALS_PATH") or "/app/serviceAccountKeySTORAGE.json"
        if not os.path.exists(cred_path):
            cred_path = "serviceAccountKeySTORAGE.json"
        if not os.path.exists(cred_path):
            raise HTTPException(status_code=500, detail="No se encontró el archivo de credenciales de Storage.")
        return storage.Client.from_service_account_json(cred_path)

    @staticmethod
    def _parse_yyyy_mm_dd(value: Optional[str], field_name: str) -> Optional[date]:
        if not value:
            return None
        try:
            return datetime.strptime(value, "%Y-%m-%d").date()
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=f"{field_name} debe tener formato YYYY-MM-DD.") from exc

    def _list_categories(self) -> List[str]:
        categorias_iter = self.bucket.list_blobs(prefix=self.base_prefix, delimiter="/")
        list(categorias_iter)
        return [p.replace(self.base_prefix, "").rstrip("/") for p in categorias_iter.prefixes]

    def _blob_to_payload(self, blob: storage.Blob, categoria: str, cliente: str) -> dict:
        blob_path = blob.name
        return {
            "nombre": blob_path.split("/")[-1],
            "categoria": categoria,
            "cliente": cliente,
            "blob_path": blob_path,
            "tamaño_kb": round((blob.size or 0) / 1024, 1),
            "fecha_creacion": blob.time_created.isoformat() if blob.time_created else None,
            "url_descarga": blob.generate_signed_url(
                version="v4",
                expiration=timedelta(hours=1),
                method="GET",
            ),
            "endpoint_descarga_individual": f"/v1/documentos/descarga-individual?blob_path={quote(blob_path)}",
        }

    def _extract_categoria_cliente(self, blob_name: str) -> Tuple[Optional[str], Optional[str]]:
        if not blob_name.startswith(self.base_prefix):
            return None, None
        resto = blob_name[len(self.base_prefix):]
        partes = resto.split("/")
        if len(partes) < 3:
            return None, None
        return partes[0], partes[1]

    def list_documents(
        self,
        categoria: Optional[str] = None,
        cliente: Optional[str] = None,
        fecha_inicio: Optional[date] = None,
        fecha_fin: Optional[date] = None,
        clientes_permitidos: Optional[List[str]] = None,
    ) -> List[dict]:
        logger.info(
            "[DOCUMENTOS] list_documents | categoria=%s | cliente=%s | fecha_inicio=%s | fecha_fin=%s | clientes_permitidos=%s",
            categoria,
            cliente,
            fecha_inicio,
            fecha_fin,
            len(clientes_permitidos or []),
        )
        if fecha_inicio and fecha_fin and fecha_inicio > fecha_fin:
            raise HTTPException(status_code=400, detail="fecha_inicio no puede ser mayor que fecha_fin.")

        categorias = [categoria] if categoria else self._list_categories()
        documentos = []
        clientes_set = {c.strip().lower() for c in clientes_permitidos} if clientes_permitidos else None
        cliente_normalizado = cliente.strip().lower() if cliente else None

        for cat in categorias:
            prefix_cat = f"{self.base_prefix}{cat}/"
            for blob in self.bucket.list_blobs(prefix=prefix_cat):
                if blob.name.endswith("/"):
                    continue

                cat_blob, cliente_blob = self._extract_categoria_cliente(blob.name)
                if not cat_blob or not cliente_blob:
                    continue

                cliente_blob_normalizado = cliente_blob.strip().lower()

                if cliente_normalizado and cliente_blob_normalizado != cliente_normalizado:
                    continue

                if clientes_set and cliente_blob_normalizado not in clientes_set:
                    continue

                if blob.time_created:
                    created_date = blob.time_created.date()
                    if fecha_inicio and created_date < fecha_inicio:
                        continue
                    if fecha_fin and created_date > fecha_fin:
                        continue

                documentos.append(self._blob_to_payload(blob, cat_blob, cliente_blob))

        logger.info("[DOCUMENTOS] list_documents completado | total=%s", len(documentos))
        return documentos

    def get_download_url(self, blob_path: str) -> dict:
        logger.info("[DOCUMENTOS] get_download_url | blob_path=%s", blob_path)
        if not blob_path.startswith(self.base_prefix):
            raise HTTPException(status_code=400, detail="El blob_path debe pertenecer al prefijo de documentos.")

        blob = self.bucket.blob(blob_path)
        if not blob.exists():
            raise HTTPException(status_code=404, detail="El archivo solicitado no existe en el bucket.")

        signed_url = blob.generate_signed_url(version="v4", expiration=timedelta(hours=1), method="GET")
        return {
            "blob_path": blob_path,
            "nombre": blob_path.split("/")[-1],
            "url_descarga": signed_url,
        }

    def build_zip_response(self, documents: List[dict], nombre_zip: str = "documentos.zip") -> StreamingResponse:
        logger.info("[DOCUMENTOS] build_zip_response | nombre_zip=%s | total_documentos=%s", nombre_zip, len(documents or []))
        if not documents:
            raise HTTPException(status_code=404, detail="No se encontraron documentos para descargar.")

        buffer = io.BytesIO()
        with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zip_file:
            for doc in documents:
                blob = self.bucket.blob(doc["blob_path"])
                if not blob.exists():
                    continue
                data = blob.download_as_bytes()
                ruta_zip = f"{doc['categoria']}/{doc['cliente']}/{doc['nombre']}"
                zip_file.writestr(ruta_zip, data)

        buffer.seek(0)

        return StreamingResponse(
            buffer,
            media_type="application/zip",
            headers={"Content-Disposition": f'attachment; filename="{nombre_zip}"'},
        )

    def upload_document(
        self,
        categoria: str,
        cliente: str,
        nombre_archivo: str,
        contenido: bytes,
        content_type: str = "application/octet-stream",
    ) -> dict:
        blob_path = f"{self.base_prefix}{categoria}/{cliente}/{nombre_archivo}"
        blob = self.bucket.blob(blob_path)
        blob.upload_from_string(contenido, content_type=content_type)

        return {
            "mensaje": "Documento generado y subido correctamente.",
            "blob_path": blob_path,
            "url_descarga": blob.generate_signed_url(
                version="v4",
                expiration=timedelta(hours=1),
                method="GET",
            ),
        }

    def delete_document(self, blob_path: str) -> dict:
        logger.info("[DOCUMENTOS] delete_document | blob_path=%s", blob_path)
        if not blob_path.startswith(self.base_prefix):
            raise HTTPException(status_code=400, detail="El blob_path debe pertenecer al prefijo de documentos.")

        blob = self.bucket.blob(blob_path)
        if not blob.exists():
            raise HTTPException(status_code=404, detail="El archivo a eliminar no existe en el bucket.")

        blob.delete()
        return {"mensaje": "Archivo eliminado correctamente.", "blob_path": blob_path}

#region Helper Functions para Documentos

def _get_documentos_service() -> GCSDocumentosService:
    return GCSDocumentosService()

def _get_documentos_filtro(
    categoria: Optional[str] = Query(default=None),
    cliente: Optional[str] = Query(default=None),
    fecha_inicio: Optional[str] = Query(default=None, description="Formato YYYY-MM-DD"),
    fecha_fin: Optional[str] = Query(default=None, description="Formato YYYY-MM-DD"),
) -> DocumentosFiltroSchema:
    return DocumentosFiltroSchema(
        categoria=categoria,
        cliente=cliente,
        fecha_inicio=GCSDocumentosService._parse_yyyy_mm_dd(fecha_inicio, "fecha_inicio"),
        fecha_fin=GCSDocumentosService._parse_yyyy_mm_dd(fecha_fin, "fecha_fin"),
    )

def _obtener_clientes_folio(documentos_schema: DocumentosSchema, db: Session) -> Tuple[dict, List[str]]:
    pack_empresa = PROVIDERS.get(documentos_schema.empresa_id, {})
    extraer_datos = pack_empresa.get("get")
    if not extraer_datos:
        raise HTTPException(status_code=400, detail=f"Empresa '{documentos_schema.empresa_id}' no configurada.")

    data_sql = extraer_datos(documentos_schema.folio, db)
    if not data_sql:
        raise HTTPException(
            status_code=404,
            detail=f"El folio {documentos_schema.folio} no existe o no tiene datos en SQL.",
        )

    nombres_clientes = []
    for i in range(1, 7):
        nombre = data_sql.get(f"{{c{i}.client_name}}")
        if nombre and str(nombre).strip() not in ["", "None", "NULL"]:
            nombres_clientes.append(str(nombre).strip())

    if not nombres_clientes:
        raise HTTPException(status_code=404, detail=f"El folio {documentos_schema.folio} no tiene clientes asociados.")

    return data_sql, nombres_clientes

#endregion

#region Endpoints Documentos - Google Cloud Storage (Bucket)

@router.get("/todos")
def api_consultar_todos_documentos(
    documentosFiltro: DocumentosFiltroSchema = Depends(_get_documentos_filtro),
    user: dict = Depends(es_usuario)
    ):
    logger.info("[DOCUMENTOS] Endpoint /todos | filtros=%s", documentosFiltro.model_dump())
    service = _get_documentos_service()
    documentos = service.list_documents(
        categoria=documentosFiltro.categoria,
        cliente=documentosFiltro.cliente,
        fecha_inicio=documentosFiltro.fecha_inicio,
        fecha_fin=documentosFiltro.fecha_fin,
    )

    return {
        "total_documentos": len(documentos),
        "filtros": documentosFiltro.model_dump(),
        "documentos": documentos,
    }

@router.get("/descarga-individual")
def api_descarga_individual_documento(
    blob_path: str = Query(..., description="Ruta completa del blob en el bucket"),
    user: dict = Depends(es_usuario),
):
    logger.info("[DOCUMENTOS] Endpoint /descarga-individual | blob_path=%s", blob_path)
    payload = DocumentoDescargaSchema(blob_path=blob_path)
    service = _get_documentos_service()
    return service.get_download_url(payload.blob_path)

@router.get("/descarga-masiva")
def api_descarga_masiva_documentos(
    documentosFiltro: DocumentosFiltroSchema = Depends(_get_documentos_filtro), 
    nombre_zip: str = Query(default="documentos.zip"), 
    user: dict = Depends(es_usuario)
    ):
    logger.info("[DOCUMENTOS] Endpoint /descarga-masiva | filtros=%s | nombre_zip=%s", documentosFiltro.model_dump(), nombre_zip)
    
    service = _get_documentos_service()
    documentos = service.list_documents(
        categoria=documentosFiltro.categoria,
        cliente=documentosFiltro.cliente,
        fecha_inicio=documentosFiltro.fecha_inicio,
        fecha_fin=documentosFiltro.fecha_fin,
    )
    return service.build_zip_response(documentos, nombre_zip=nombre_zip)

@router.get("/descarga-masiva-folio")
def api_descarga_masiva_documentos_folio(
    documentos_schema: DocumentosSchema = Depends(),
    documentosFiltro: DocumentosFiltroSchema = Depends(_get_documentos_filtro),
    nombre_zip: str = Query(default=""),
    db: Session = Depends(get_db),
    user: dict = Depends(es_usuario),
):
    logger.info(
        "[DOCUMENTOS] Endpoint /descarga-masiva-folio | empresa_id=%s | folio=%s | filtros=%s | nombre_zip=%s",
        documentos_schema.empresa_id,
        documentos_schema.folio,
        documentosFiltro.model_dump(),
        nombre_zip,
    )
    _, nombres_clientes = _obtener_clientes_folio(documentos_schema, db)

    service = _get_documentos_service()
    documentos = service.list_documents(
        categoria=documentosFiltro.categoria,
        cliente=documentosFiltro.cliente,
        fecha_inicio=documentosFiltro.fecha_inicio,
        fecha_fin=documentosFiltro.fecha_fin,
        clientes_permitidos=nombres_clientes,
    )

    if not documentos:
        raise HTTPException(status_code=404, detail="No se encontraron documentos relacionados al folio.")

    zip_name = nombre_zip or f"documentos_folio_{documentos_schema.folio}.zip"
    return service.build_zip_response(documentos, nombre_zip=zip_name)

@router.post("/generar-subir")
async def api_generar_subir_documento(
    payload: DocumentosSchemaGeneracion,
    db: Session = Depends(get_db),
    user: dict = Depends(es_usuario),
):
    logger.info(
        "[DOCUMENTOS] Endpoint /generar-subir | empresa_id=%s | folio=%s | categoria=%s",
        payload.empresa_id,
        payload.folio,
        payload.categoria,
    )
    from .notificacionesMS import FirebaseRepository, GenerarPDFUseCase

    generador = GenerarPDFUseCase(FirebaseRepository())
    return await generador.generar_pdf_por_categoria(
        empresa_id=payload.empresa_id.strip(),
        categoria=payload.categoria.strip(),
        folio=payload.folio.strip(),
        db=db,
        subir_bucket=True
    )

@router.delete("/eliminar-archivo")
def api_eliminar_archivo(
    payload: DocumentoEliminarSchema,
    user: dict = Depends(es_usuario),
):
    logger.info("[DOCUMENTOS] Endpoint /eliminar-archivo [DELETE] | blob_path=%s", payload.blob_path)
    service = _get_documentos_service()
    return service.delete_document(payload.blob_path)

@router.get("/folio")
def api_consultar_documentos_folio(
    documentos_schema: DocumentosSchema = Depends(),
    documentosFiltro: DocumentosFiltroSchema = Depends(_get_documentos_filtro),
    db: Session = Depends(get_db),
    user: dict = Depends(es_usuario)
    ):
    """
    Consulta los documentos generados en el bucket de GCS para todos los clientes de un folio.
    Devuelve archivos agrupados por categoría con links de descarga (signed URLs, 1h).
    Busca en: Komunah/PlantillasMovil/Categorias/{categoria}/{nombre_cliente}/
    """
    logger.info(
        "[DOCUMENTOS] Endpoint /folio | empresa_id=%s | folio=%s | filtros=%s",
        documentos_schema.empresa_id,
        documentos_schema.folio,
        documentosFiltro.model_dump(),
    )
    data_sql, nombres_clientes = _obtener_clientes_folio(documentos_schema, db)

    service = _get_documentos_service()
    documentos = service.list_documents(
        categoria=documentosFiltro.categoria,
        cliente=documentosFiltro.cliente,
        fecha_inicio=documentosFiltro.fecha_inicio,
        fecha_fin=documentosFiltro.fecha_fin,
        clientes_permitidos=nombres_clientes,
    )

    resultado_categorias = {}
    for doc in documentos:
        cat = doc["categoria"]
        cliente_doc = doc["cliente"]
        if cat not in resultado_categorias:
            resultado_categorias[cat] = {}
        if cliente_doc not in resultado_categorias[cat]:
            resultado_categorias[cat][cliente_doc] = []
        resultado_categorias[cat][cliente_doc].append(doc)

    propietario = data_sql.get("{cl.cliente}") or (nombres_clientes[0] if nombres_clientes else "Cliente")
    clientes_restantes = [c for c in nombres_clientes if c != propietario]

    return {
        "folio": documentos_schema.folio,
        "propietario": propietario,
        "coopropietario": nombres_clientes,
        "total_documentos": len(documentos),
        "categorias": resultado_categorias
    }



#endregion