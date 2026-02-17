from fastapi import Depends, HTTPException, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from firebase_admin import auth, firestore

security = HTTPBearer()

def get_current_user(res: HTTPAuthorizationCredentials = Security(security)):
    """Valida el token y devuelve el usuario con su rol de Firestore."""
    token = res.credentials
    try:
        # Verifica el token con Firebase
        decoded_token = auth.verify_id_token(token)
        uid = decoded_token['uid']
        
        # Busca el rol en Firestore
        db = firestore.client()
        user_doc = db.collection("usuarios").document(uid).get()
        
        if not user_doc.exists:
            raise HTTPException(status_code=404, detail="Usuario no encontrado")
        
        user_data = user_doc.to_dict()
        user_data["uid"] = uid
        return user_data
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        print(f"DEBUG: Error real en auth: {str(e)}")
        raise HTTPException(status_code=401, detail="Sesión inválida")

class RoleChecker:
    def __init__(self, roles_permitidos: list):
        self.roles_permitidos = roles_permitidos

    def __call__(self, user: dict = Depends(get_current_user)):
        if user.get("rol") not in self.roles_permitidos:
            raise HTTPException(status_code=403, detail="Rango insuficiente")
        return user

# --- Instancias para usar en tus routers ---
es_super_admin = RoleChecker(["super_admin"])
es_admin = RoleChecker(["super_admin", "admin"])
es_usuario = RoleChecker(["super_admin", "admin", "usuario"])