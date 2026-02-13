from fastapi import APIRouter
from app.services.sync_service import AutoSyncManager
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/plesk-push")
def force_sync():
    """Endpoint para que Plesk despierte el CPU y corra el sync."""
    logger.info("🚀 Plesk activó la sincronización manual.")
    sync = AutoSyncManager()
    sync.ejecutar_sync_total()
    return {"status": "Hecho"}