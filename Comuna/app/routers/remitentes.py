import os, requests, re
from fastapi import APIRouter, HTTPException, Depends
from ..schemas import RemitenteCreate, RemitenteUpdate, RemitenteResponse
from ..services.security import es_admin
from typing import List

router = APIRouter(prefix="/v1/config/remitentes", tags=["Configuración Remitentes"])

class RemitentesManager:
    def __init__(self):
        self.project_id = os.getenv('FIREBASE_PLANTILLAS_PROJECT_ID', '').strip()
        self.api_key = os.getenv('FIREBASE_PLANTILLAS_API_KEY')
        self.base_url = f"https://firestore.googleapis.com/v1/projects/{self.project_id}/databases/(default)/documents"
        self.headers = {"X-Goog-Api-Key": self.api_key, "Content-Type": "application/json"}

    def _generar_siguiente_id(self, empresa_id: str):
        url = f"{self.base_url}/empresas/{empresa_id}/remitentes_config"
        resp = requests.get(url, headers=self.headers)
        max_num = 0
        if resp.status_code == 200:
            docs = resp.json().get("documents", [])
            for d in docs:
                id_doc = d["name"].split("/")[-1]
                match = re.search(r"REM-(\d+)", id_doc)
                if match:
                    num = int(match.group(1))
                    if num > max_num: max_num = num
        return f"REM-{str(max_num + 1).zfill(4)}"

    def crear(self, empresa_id: str, datos: RemitenteCreate, depto_aut: str):
        nuevo_id = self._generar_siguiente_id(empresa_id)
        url = f"{self.base_url}/empresas/{empresa_id}/remitentes_config/{nuevo_id}"
        payload = {
            "fields": {
                "id": {"stringValue": nuevo_id},
                "departamento": {"stringValue": depto_aut}, # Cambio aquí
                "remitente": {"stringValue": datos.remitente.lower()}
            }
        }
        return requests.patch(url, json=payload, headers=self.headers)

    def listar(self, empresa_id: str):
        url = f"{self.base_url}/empresas/{empresa_id}/remitentes_config"
        resp = requests.get(url, headers=self.headers)
        if resp.status_code != 200: return []
        return [self._formatear(d) for d in resp.json().get("documents", [])]

    def actualizar(self, empresa_id: str, doc_id: str, datos: RemitenteUpdate):
        url = f"{self.base_url}/empresas/{empresa_id}/remitentes_config/{doc_id}"
        fields = {}
        mask = []
        if datos.departamento: # Cambio aquí
            fields["departamento"] = {"stringValue": datos.departamento}
            mask.append("departamento")
        if datos.remitente:
            fields["remitente"] = {"stringValue": datos.remitente.lower()}
            mask.append("remitente")
        
        if not mask: return None
        params = "&".join([f"updateMask.fieldPaths={m}" for m in mask])
        return requests.patch(f"{url}?{params}", json={"fields": fields}, headers=self.headers)

    def eliminar(self, empresa_id: str, doc_id: str):
        url = f"{self.base_url}/empresas/{empresa_id}/remitentes_config/{doc_id}"
        return requests.delete(url, headers=self.headers)

    def _formatear(self, d: dict):
        f = d.get("fields", {})
        return {
            "id": d["name"].split("/")[-1],
            "departamento": f.get("departamento", {}).get("stringValue", "Sin Departamento"), # Cambio aquí
            "remitente": f.get("remitente", {}).get("stringValue", "")
        }

# --- ENDPOINTS ---

@router.post("/{empresa_id}", status_code=201)
def api_crear(empresa_id: str, datos: RemitenteCreate, user: dict = Depends(es_admin)):
    # Tomamos el departamento real del usuario logueado
    depto_usuario = user.get("departamento", "General")
    
    res = RemitentesManager().crear(empresa_id, datos, depto_usuario)
    if res.status_code != 200:
        raise HTTPException(status_code=400, detail=f"Error al crear: {res.text}")
        
    return {
        "status": "success", 
        "id": res.json()["name"].split("/")[-1],
        "departamento_asignado": depto_usuario
    }

@router.get("/{empresa_id}", response_model=List[RemitenteResponse])
def api_listar(empresa_id: str, user: dict = Depends(es_admin)):
    return RemitentesManager().listar(empresa_id)

@router.patch("/{empresa_id}/{doc_id}")
def api_actualizar(empresa_id: str, doc_id: str, datos: RemitenteUpdate, user: dict = Depends(es_admin)):
    res = RemitentesManager().actualizar(empresa_id, doc_id, datos)
    if res is None: return {"status": "nothing_to_update"}
    return {"status": "updated"}

@router.delete("/{empresa_id}/{doc_id}")
def api_eliminar(empresa_id: str, doc_id: str, user: dict = Depends(es_admin)):
    RemitentesManager().eliminar(empresa_id, doc_id)
    return {"status": "deleted"}