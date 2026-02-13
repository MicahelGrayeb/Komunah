import os
import requests
from fastapi import APIRouter, HTTPException, Depends
from firebase_admin import auth, firestore
from dotenv import load_dotenv
from ..schemas import RegistroSchema, LoginSchema
from ..services.security import get_current_user
from ..services.security import es_admin, es_usuario, es_super_admin


load_dotenv()

router = APIRouter(
    prefix="/usuarios",
    tags=["Autenticación (Firebase)"]
)


FIREBASE_WEB_API_KEY = os.getenv("FIREBASE_WEB_API_KEY")


@router.post("/registrar")
async def registrar(datos: RegistroSchema, user: dict = Depends(es_admin)):
    """
    Crea usuario en Firebase Auth y guarda perfil en Firestore.
    """
    db = firestore.client()
    try:
        # Crear en Firebase Authentication
        user = auth.create_user(email=datos.email, password=datos.password)
        
        # Guardar datos adicionales en Firestore
        db.collection("usuarios").document(user.uid).set({
            "nombre": datos.nombre,
            "email": datos.email,
            "rol": datos.rol,
            "departamento": datos.departamento,
            "creado_el": firestore.SERVER_TIMESTAMP
        })
        
        return {
            "status": "success", 
            "uid": user.uid, 
            "message": "Usuario registrado correctamente"
        }
    except Exception as e:
        
        print(f"Error registro: {e}")
        raise HTTPException(status_code=400, detail=str(e))

@router.post("/login")
async def login(datos: LoginSchema, user: dict = Depends(es_usuario)):
    """
    Valida credenciales contra Google Identity Toolkit y recupera perfil de Firestore.
    """
    if not FIREBASE_WEB_API_KEY:
        raise HTTPException(status_code=500, detail="Falta configurar FIREBASE_WEB_API_KEY en .env")

    db = firestore.client()
    
    
    url = f"https://identitytoolkit.googleapis.com/v1/accounts:signInWithPassword?key={FIREBASE_WEB_API_KEY}"
    payload = {
        "email": datos.email, 
        "password": datos.password, 
        "returnSecureToken": True
    }
    
    try:
        r = requests.post(url, json=payload)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error conectando con Firebase: {str(e)}")
    
    # --- 1. SI FALLA LA CONTRASEÑA O EMAIL ---
    if r.status_code != 200:
        return {
            "autenticado": False,
            "token": None,
            "rol": None,
            "status": "credenciales_incorrectas",
            "detalle": r.json().get("error", {}).get("message", "Error desconocido")
        }
    
    # --- 2. CREDENCIALES CORRECTAS, OBTENER DATOS ---
    auth_data = r.json()
    id_token = auth_data['idToken']
    uid = auth_data['localId']
    
    # --- 3. BUSCAR PERFIL EN FIRESTORE ---
    doc_ref = db.collection("usuarios").document(uid).get()
    
    if doc_ref.exists:
        perfil = doc_ref.to_dict()
        return {
            "autenticado": True,
            "token": id_token,
            "rol": perfil.get("rol", "usuario"),
            "nombre": perfil.get("nombre", "Usuario"),
            "uid": uid,
            "departamento": perfil.get("departamento", "Sin Asignar"),
            "email": perfil.get("email", "Usuario"),
            "status": "success"
        }
    else:
        # El usuario existe en Auth pero no tiene documento en 'usuarios'
        return {
            "autenticado": False, 
            "token": id_token,
            "status": "usuario_sin_perfil_en_bd"
        }