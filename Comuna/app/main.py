import os
import uvicorn
import firebase_admin
from firebase_admin import credentials, firestore
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware  
from dotenv import load_dotenv

from apscheduler.schedulers.background import BackgroundScheduler
from .routers.notificacionesMS import NotificationUseCase, FirebaseRepository, NotificationGateway
from .database import SessionLocal 
import logging
from zoneinfo import ZoneInfo
from .services.sync_service import AutoSyncManager


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)    


load_dotenv()
JOB_ID_BARRIDO = "barrido_automatico_diario"
APP_MODE = os.getenv("APP_MODE", "FULL")

from .routers import (
    login, 
    datos, 
    emails, 
    notificaciones, 
    notificaciones_estaticas,
    notificacionesMS,
    usuarios,
    reportes,
    webhook,
    DashboardKomunah,
    remitentes,
    admin,
    debug_config
)


# --- INICIALIZACIÓN DE FIREBASE ---
if not firebase_admin._apps:
    try:
        if os.path.exists("serviceAccountKey.json"):
            print("🔥 Iniciando Firebase con serviceAccountKey.json...")
            cred = credentials.Certificate("serviceAccountKey.json")
            firebase_admin.initialize_app(cred)
        else:
            print("☁️ Iniciando Firebase con ApplicationDefault (Cloud)...")
            cred = credentials.ApplicationDefault()
            firebase_admin.initialize_app(cred, {
                'projectId': 'comuna-480820'
            })
    except Exception as e:
        print(f"⚠️ Advertencia: No se pudo iniciar Firebase: {e}")

def sincronizar_horario_cron(scheduler_instancia):
    """
    Revisa Firebase cada 60 segundos. 
    Compara la hora actual del Job con la de Firebase usando strings.
    """
    def verificar_y_actualizar():
        try:
            repo = FirebaseRepository()
            config = repo.obtener_config_recordatorios("komunah")
            
            nueva_h = int(config.get("hora", 10))
            nueva_m = int(config.get("minuto", 0))

            job = scheduler_instancia.get_job(JOB_ID_BARRIDO)
            if job:
                # Comparamos como texto para evitar el error de 'BaseField'
                # fields[5] es la hora, fields[6] es el minuto
                hora_actual_str = str(job.trigger.fields[5])
                min_actual_str = str(job.trigger.fields[6])

                if hora_actual_str != str(nueva_h) or min_actual_str != str(nueva_m):
                    logger.info(f"🔄 SYNC: Cambio detectado en Firebase ({nueva_h}:{nueva_m}). Ajustando Cron...")
                    scheduler_instancia.reschedule_job(
                        JOB_ID_BARRIDO, 
                        trigger='cron', 
                        hour=nueva_h, 
                        minute=nueva_m
                    )
                    logger.info("✅ Cron reprogramado exitosamente.")
        except Exception as e:
            logger.error(f"❌ Error en sincronización: {e}")

    # Mantenemos el intervalo de 1 minuto
    scheduler_instancia.add_job(verificar_y_actualizar, 'interval', minutes=1, id="sync_config_job")

        
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
    allow_origins=["*"],      # Permitir CUALQUIER origen (Frontend local, IP, dominio, etc.)
    allow_credentials=True,   # Permitir cookies y credenciales
    allow_methods=["*"],      # Permitir TODOS los métodos (GET, POST, PUT, DELETE...)
    allow_headers=["*"],      # Permitir TODOS los encabezados (Authorization, Content-Type...)
)

def tarea_diaria_notificaciones():
    """Lógica del Cron Job."""
    db = SessionLocal()
    try:
        logger.info("⏰ Cron Job: Iniciando proceso de barrido automático...")
        repo = FirebaseRepository()
        gateway = NotificationGateway()
        use_case = NotificationUseCase(repo, gateway)
        
        config = repo.obtener_config_recordatorios("komunah")
        
        use_case.ejecutar_barrido_automatico("komunah", config["dias_1"], "Recordatorio de Pago", db, "normal")
        use_case.ejecutar_barrido_automatico("komunah", config["dias_1"], "Recordatorio de Pago Vencido", db, "deudores")
        use_case.ejecutar_barrido_automatico("komunah", config["dias_2"], "Recordatorio de Pago", db, "normal")
        use_case.ejecutar_barrido_automatico("komunah", config["dias_2"], "Recordatorio de Pago Vencido", db, "deudores")
            
        logger.info("✅ Cron Job: Proceso finalizado con éxito.")
    except Exception as e:
        logger.error(f"❌ Error en el Cron Job: {str(e)}")
    finally:
        db.close()

@app.on_event("startup")
def iniciar_mantenimiento():
    """Cron Job fijo: Se ejecuta todos los días a las 03:30 AM sin cambios."""
    if APP_MODE in ["NOTIFICACIONES", "FULL"]:
        logger.info("📡 Mantenimiento activado: Sincronización fija diaria a las 01:10")

        
@app.on_event("startup")
def iniciar_scheduler():
    if APP_MODE in ["NOTIFICACIONES", "FULL"]:
        mx_tz = ZoneInfo("America/Mexico_City")
        scheduler = BackgroundScheduler(timezone=mx_tz) 
        
        # 1. Carga inicial
        repo = FirebaseRepository()
        config = repo.obtener_config_recordatorios("komunah")
        
        # 2. Programar el Job principal
        scheduler.add_job(
            tarea_diaria_notificaciones, 
            'cron', 
            hour=config["hora"], 
            minute=config["minuto"],
            id=JOB_ID_BARRIDO
        ) 
        
        # 3. Activar el verificador automático cada minuto (Polling)
        sincronizar_horario_cron(scheduler)
        
        scheduler.start()
        logger.info(f"🚀 Scheduler iniciado: Barrido a las {config['hora']:02d}:{config['minuto']:02d}")

@app.get("/")
def home():
    return {
        "servicio": "Notificaciones" if APP_MODE == "NOTIFICACIONES" else "API Principal",
        "estado": "Online 🟢",
        "modo": "Túnel SSH & Firebase",
        "cors": "Abierto a todo el mundo 🌍",
        "scheduler": "Activo ⏰" if APP_MODE in ["NOTIFICACIONES", "FULL"] else "Inactivo",
        "docs": "/docs"
    }

# --- CONECTAR LAS RUTAS ---
if APP_MODE == "NOTIFICACIONES":
    # Solo exponemos el motor de envíos en el puerto 8081
    app.include_router(notificacionesMS.router)
    app.include_router(notificacionesMS.router_crud)
    app.include_router(notificacionesMS.router_wa)
    app.include_router(notificacionesMS.router_usuario)
    app.include_router(notificacionesMS.router_globales)
    app.include_router(remitentes.router)
    app.include_router(debug_config.router)
    
    
else:
    app.include_router(login.router)
    app.include_router(usuarios.router)
    app.include_router(datos.router)
    app.include_router(emails.router)
    app.include_router(notificaciones.router)
    app.include_router(notificaciones_estaticas.router)
    app.include_router(reportes.router)
    app.include_router(webhook.router)
    app.include_router(DashboardKomunah.router)
    app.include_router(admin.router)
   
# --- ARRANQUE ---
if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=False)