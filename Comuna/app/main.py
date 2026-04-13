import os
import uvicorn
import firebase_admin
from firebase_admin import credentials, firestore
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from .database import SessionLocal
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


load_dotenv()
APP_MODE = os.getenv("APP_MODE", "FULL")

from .routers import (
    login, 
    datos, 
    emails, 
    notificaciones,
    Documentos,
    notificaciones_estaticas,
    notificacionesMS,
    usuarios,
    reportes,
    webhook,
    DashboardKomunah,
    remitentes,
    admin,
    Cobranza,
    debug_config
)


# --- INICIALIZACIÓN DE FIREBASE ---
if not firebase_admin._apps:
    try:
        if os.path.exists("serviceAccountKey.json"):
            print("🔥 Iniciando Firebase con serviceAccountKey.json...")
            # Forzamos al sistema operativo a reconocer la ruta de la llave
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.path.abspath("serviceAccountKey.json")
            cred = credentials.Certificate("serviceAccountKey.json")
            firebase_admin.initialize_app(cred)
        else:
            print("☁️ Iniciando Firebase con ApplicationDefault (Cloud)...")
            p_id = os.getenv('FIREBASE_PLANTILLAS_PROJECT_ID')
            cred = credentials.ApplicationDefault()
            firebase_admin.initialize_app(cred, {'projectId': p_id})
    except Exception as e:
        print(f"⚠️ Advertencia: No se pudo iniciar Firebase: {e}")

# --- CONFIGURACIÓN DE FASTAPI ---
if APP_MODE == "NOTIFICACIONES":
    app = FastAPI(
        title="Comuna - Microservicio de Notificaciones",
        description="Puerto 8081: Solo envíos MailerSend con Copropietarios",
        version="2.0.0"
    )
else: 
    app = FastAPI(
        title="Comuna API - Clean Architecture",
        description="API conectada a Plesk (SQL) y Firebase (NoSQL)",
        version="2.0.0"
    )

origins = [
    "https://app.grupokomunah.mx",    
    "https://aistudio.google.com",
    "http://localhost:3000",          
    "http://localhost:5173",          
    "http://127.0.0.1:3000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origin_regex=r"https?://.*", 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
) 


@app.get("/")
def home():
    return {
        "servicio": "Notificaciones" if APP_MODE == "NOTIFICACIONES" else "API Principal",
        "estado": "Online 🟢",
        "modo": "Túnel SSH & Firebase",
        "cors": "Abierto a todo el mundo 🌍",
        "docs": "/docs"
    }

# --- CONECTAR LAS RUTAS ---
if APP_MODE == "NOTIFICACIONES":
    # Solo exponemos el motor de envíos en el puerto 8081
    app.include_router(notificacionesMS.router)
    app.include_router(notificacionesMS.router_usuario)
    app.include_router(notificacionesMS.router_globales)
    app.include_router(notificacionesMS.router_crud)
    app.include_router(notificacionesMS.router_wa)
    app.include_router(notificacionesMS.router_documento)
    app.include_router(notificacionesMS.router_anexo)
    app.include_router(notificacionesMS.router_firmantes_empresa)
    app.include_router(remitentes.router)
else:
    app.include_router(login.router)
    app.include_router(usuarios.router)
    app.include_router(datos.router)
    app.include_router(emails.router)
    app.include_router(DashboardKomunah.router)
    app.include_router(Documentos.router)
    app.include_router(webhook.router)
    app.include_router(Cobranza.router)
    app.include_router(reportes.router)
    app.include_router(admin.router)
    app.include_router(debug_config.router)

# --- ARRANQUE ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=False)