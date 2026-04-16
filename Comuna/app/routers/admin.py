from fastapi import APIRouter
from fastapi.responses import JSONResponse
from app.services.sync_service import AutoSyncManager
import logging
import traceback

router = APIRouter()
logger = logging.getLogger(__name__)

@router.get("/plesk-push")
def force_sync():
    """Endpoint para que Plesk despierte el CPU y corra el sync."""
    logger.info("🚀 Plesk activó la sincronización manual.")
    try:
        sync = AutoSyncManager()
        sync.ejecutar_sync_total()
        return {"status": "Hecho"}
    except Exception as e:
        error_detalle = traceback.format_exc()
        logger.error(f"❌ Error en sync: {error_detalle}")
        return JSONResponse(status_code=500, content={"error": str(e), "detalle": error_detalle})