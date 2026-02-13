import os

# Definimos la estructura y el contenido de los archivos
structure = {
    "app": {
        "__init__.py": "",
        "db.py": """import os
import firebase_admin
from firebase_admin import credentials, firestore

# --- CONFIGURACIÓN ---
# ID de tu proyecto Comuna
PROJECT_ID = "comuna-480820"
# Nombre de tu archivo de credenciales (debe estar en la carpeta raíz del proyecto)
CREDENTIALS_FILE = "credenciales_usuario.json"

db = None

def initialize_firestore():
    global db
    
    # Buscamos el archivo en la raíz del proyecto (un nivel arriba de 'app')
    path_to_cred = os.path.join(os.getcwd(), CREDENTIALS_FILE)

    if not os.path.exists(path_to_cred):
        print(f"❌ ERROR: No encuentro el archivo '{path_to_cred}'")
        return

    try:
        # Forzamos la variable de entorno para que Google use tu JSON de usuario
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = path_to_cred
        print(f"📂 Cargando credenciales desde: {CREDENTIALS_FILE}")

        if not firebase_admin._apps:
            # Al ser credencial de usuario, ApplicationDefault() la toma de la variable de entorno
            cred = credentials.ApplicationDefault()
            
            firebase_admin.initialize_app(cred, {
                'projectId': PROJECT_ID,
            })
        
        db = firestore.client()
        print(f"✅ CONEXIÓN EXITOSA A FIRESTORE ({PROJECT_ID})")

    except Exception as e:
        print(f"❌ ERROR DE CONEXIÓN: {e}")
        db = None

# Inicializamos al importar este archivo
initialize_firestore()
""",
        "main.py": """from fastapi import FastAPI
from app.db import db
from app.routers import usuarios

app = FastAPI(
    title="Backend Comuna",
    description="API para gestión financiera del proyecto Comuna.",
    version="1.0.0"
)

# --- INCLUIR RUTAS ---
app.include_router(usuarios.router)

@app.get("/")
def home():
    estado = "🟢 CONECTADO" if db is not None else "🔴 ERROR DE BDD"
    return {
        "sistema": "Backend Comuna",
        "estado_base_datos": estado,
        "docs": "/docs"
    }
""",
        "routers": {
            "__init__.py": "",
            "usuarios.py": """from fastapi import APIRouter, HTTPException
from pydantic import BaseModel
from app.db import db

router = APIRouter(prefix="/usuarios", tags=["Usuarios de Comuna"])

class UsuarioModelo(BaseModel):
    nombre: str
    email: str
    rol: str

@router.post("/crear")
async def crear_usuario(usuario: UsuarioModelo):
    if db is None:
        raise HTTPException(status_code=500, detail="Base de datos desconectada")
    
    try:
        doc_ref = db.collection("usuarios").add(usuario.dict())
        return {"mensaje": "Usuario creado", "id": doc_ref[1].id}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/listar")
async def listar_usuarios():
    if db is None:
        raise HTTPException(status_code=500, detail="Base de datos desconectada")
    
    users = []
    docs = db.collection("usuarios").stream()
    for doc in docs:
        data = doc.to_dict()
        data["id"] = doc.id
        users.append(data)
    
    return users
"""
        }
    },
    "requirements.txt": """fastapi
uvicorn
firebase-admin
google-cloud-firestore
python-multipart
requests
pydantic
"""
}

def create_structure(base_path, struct):
    for name, content in struct.items():
        path = os.path.join(base_path, name)
        if isinstance(content, dict):
            os.makedirs(path, exist_ok=True)
            create_structure(path, content)
        else:
            with open(path, "w", encoding="utf-8") as f:
                f.write(content)
            print(f"✅ Creado: {path}")

if __name__ == "__main__":
    print("🚀 Creando estructura del proyecto...")
    create_structure(".", structure)
    print("\\n✨ ¡Listo! Estructura creada.")