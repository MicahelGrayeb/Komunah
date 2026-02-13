import os
import requests
import logging
from fastapi import APIRouter, HTTPException, Depends
from ..services.security import es_admin

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1/debug", tags=["🔧 Switch de Emergencia"])

class FirebaseRepository:
    def __init__(self):
        # Variables de entorno para pegarle al proyecto correcto
        self.project_id = os.getenv('FIREBASE_PLANTILLAS_PROJECT_ID', '').strip()
        self.api_key = os.getenv('FIREBASE_PLANTILLAS_API_KEY')
        self.base_url = f"https://firestore.googleapis.com/v1/projects/{self.project_id}/databases/(default)/documents"
        self.headers = {"X-Goog-Api-Key": self.api_key, "Content-Type": "application/json"}

    def get_status(self, empresa_id: str):
        """Consulta el estado en: empresas/{empresa_id}/configuracion/debug"""
        url = f"{self.base_url}/empresas/{empresa_id}/configuracion/debug"
        try:
            resp = requests.get(url, headers=self.headers, timeout=5)
            if resp.status_code != 200:
                return False
            
            fields = resp.json().get("fields", {})
            # El campo ahora se llama simplemente 'activo'
            return fields.get("activo", {}).get("booleanValue", False)
        except Exception as e:
            logger.error(f"Error GET Debug: {e}")
            return False

    def set_status(self, empresa_id: str, estado: bool):
        """Crea o actualiza el documento 'debug' sin tocar 'general'."""
        # Al NO usar updateMask en la URL, el PATCH creará el documento si no existe    
        url = f"{self.base_url}/empresas/{empresa_id}/configuracion/debug"
        
        payload = {
            "fields": {
                "activo": {"booleanValue": estado},
                "ultima_modificacion": {"stringValue": str(os.getenv("HOSTNAME", "api-server"))}
            }
        }
        
        try:
            resp = requests.patch(url, json=payload, headers=self.headers, timeout=10)
            return resp
        except Exception as e:
            logger.error(f"Error PATCH Debug: {e}")
            return None

# --- Endpoints ---

@router.get("/{empresa_id}")
def consultar_debug(empresa_id: str, user: dict = Depends(es_admin)):
    """Revisa si el modo pruebas está encendido para la empresa."""
    repo = FirebaseRepository()
    return {"empresa": empresa_id, "modo_debug": repo.get_status(empresa_id)}

@router.patch("/{empresa_id}")
def cambiar_debug(empresa_id: str, estado: bool, user: dict = Depends(es_admin)):
    """Enciende o apaga el switch de debug."""
    repo = FirebaseRepository()
    res = repo.set_status(empresa_id, estado)
    
    if res is None or res.status_code != 200:
        detalle = res.text if res else "Falla de red"
        raise HTTPException(status_code=400, detail=f"Error al mover el switch: {detalle}")
        
    return {"status": "ok", "empresa": empresa_id, "modo_debug": estado}