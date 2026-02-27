import os
import logging
from ..services.security import get_current_user, es_admin, es_usuario
from ..database import get_db
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from google.cloud import firestore

load_dotenv()

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/Cobranza",tags=["Cobranza"])

db_firestore = firestore.Client(project=os.getenv("FIREBASE_PLANTILLAS_PROJECT_ID"))


class StatusPagoUpdate(BaseModel):
    id: str
    status: str


@router.get("/comprobantes")
def obtener_comprobantes():
    """Obtiene todos los documentos de la colección ComprobantePago."""
    try:
        docs = db_firestore.collection("ComprobantePago").stream()
        resultado = []
        for doc in docs:
            item = doc.to_dict()
            item["id"] = doc.id
            resultado.append(item)
        return resultado
    except Exception as e:
        logger.exception(f"Error al obtener comprobantes: {e}")
        raise HTTPException(status_code=500, detail="Error al obtener comprobantes")


@router.post("/actualizarStatusPagos")
def actualizar_status_pagos(payload: StatusPagoUpdate):
    """Actualiza el campo status de un comprobante por su ID en Firestore."""
    try:
        doc_ref = db_firestore.collection("ComprobantePago").document(payload.id)
        if not doc_ref.get().exists:
            raise HTTPException(status_code=404, detail="Comprobante no encontrado")

        doc_ref.update({"status": payload.status})
        return {"status": "success", "id": payload.id, "nuevo_status": payload.status}
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error al actualizar status: {e}")
        raise HTTPException(status_code=500, detail="Error al actualizar status")