from fastapi import APIRouter, Request, HTTPException
import logging
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/webhook-prueba",
    tags=["Webhook Respond.io (Prueba)"]
)

@router.get("/imagen")
def verificar_estado():
    return {"status": "online", "mensaje": "El webhook está listo para recibir peticiones"}

@router.post("/imagen")
async def WebhookImagen(request: Request):
    try:
        body_bytes = await request.body()
        if not body_bytes:
            return {"status": "error", "mensaje": "Body vacío"}

        payload = json.loads(body_bytes)
        
       
        contacto = payload.get("contact", {})
        mensaje_data = payload.get("message", {}).get("message", {})
        adjunto = mensaje_data.get("attachment", {})

        datos_limpios = {
            "nombre": contacto.get("firstName"),
            "telefono": contacto.get("phone"),
            "tipo_mensaje": mensaje_data.get("type"),
            "url_foto": adjunto.get("url"),
            "nombre_archivo": adjunto.get("fileName")
        }

        logger.info(f"✅ Procesando foto de: {datos_limpios['nombre']} ({datos_limpios['telefono']})")
        logger.info(f"🔗 URL de la imagen: {datos_limpios['url_foto']}")

    
        return {
            "status": "success", 
            "mensaje": "Imagen recibida y procesada",
            "cliente": datos_limpios['nombre']
        }

    except Exception as e:
        logger.error(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))