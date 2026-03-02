import os
import logging
import requests
from ..services.security import get_current_user, es_admin, es_usuario
from ..database import get_db
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel

load_dotenv()

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/Cobranza", tags=["Cobranza"])

_project_id = os.getenv("FIREBASE_PLANTILLAS_PROJECT_ID", "").strip()
_api_key    = os.getenv("FIREBASE_PLANTILLAS_API_KEY", "")
_base_url   = f"https://firestore.googleapis.com/v1/projects/{_project_id}/databases/(default)/documents"
_headers    = {"X-Goog-Api-Key": _api_key, "Content-Type": "application/json"}


class StatusPagoUpdate(BaseModel):
    id: str
    status: str


@router.get("/comprobantes")
def obtener_comprobantes():
    """Obtiene y limpia todos los documentos de la colección ComprobantePago en un solo paso."""
    try:
        url = f"{_base_url}/ComprobantePago"
        resp = requests.get(url, headers=_headers, timeout=10)

        if resp.status_code != 200:
            logger.error(f"Firebase error {resp.status_code}: {resp.text}")
            raise HTTPException(status_code=500, detail="Error al obtener comprobantes")

        documentos = resp.json().get("documents", [])

        # --- Lógica de limpieza compacta ---
        # Esta función lambda se encarga de entrar en cada nivel y extraer el valor real
        clean = lambda v: (
            {k: clean(val) for k, val in v["mapValue"]["fields"].items()} if isinstance(v, dict) and "mapValue" in v else
            [clean(i) for i in v["arrayValue"]["values"]] if isinstance(v, dict) and "arrayValue" in v else
            (int(v["integerValue"]) if "integerValue" in v else list(v.values())[0]) if isinstance(v, dict) else v
        )

        resultado = []
        for doc in documentos:
            # Procesamos los campos base del documento
            raw_fields = doc.get("fields", {})
            # Limpiamos recursivamente y añadimos el ID
            item = {k: clean(v) for k, v in raw_fields.items()}
            item["id"] = doc["name"].split("/")[-1]
            resultado.append(item)

        return resultado

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error al obtener comprobantes: {e}")
        raise HTTPException(status_code=500, detail="Error al obtener comprobantes")


@router.post("/actualizarStatusPagos")
def actualizar_status_pagos(payload: StatusPagoUpdate):
    """Actualiza el campo status de un comprobante por su ID en Firestore."""
    try:
        if not _project_id or not _api_key:
            raise HTTPException(status_code=500, detail="Configuración de Firebase incompleta")

        nuevo_status = payload.status.strip()
        if not nuevo_status:
            raise HTTPException(status_code=400, detail="El campo status es obligatorio")

        url = f"{_base_url}/ComprobantePago/{payload.id}?updateMask.fieldPaths=Status"
        body = {"fields": {"Status": {"stringValue": nuevo_status}}}
        resp = requests.patch(url, json=body, headers=_headers, timeout=10)

        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Comprobante no encontrado")
        if resp.status_code == 400:
            logger.error(f"Firebase error 400: {resp.text}")
            raise HTTPException(status_code=400, detail="Solicitud inválida para Firebase")
        if resp.status_code not in [200, 201]:
            logger.exception(f"Firebase error {resp.status_code}: {resp.text}")
            raise HTTPException(status_code=500, detail="Error al actualizar status")

        return {"status": "success", "id": payload.id, "nuevo_status": nuevo_status}

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error al actualizar status: {e}")
        raise HTTPException(status_code=500, detail="Error al actualizar status")