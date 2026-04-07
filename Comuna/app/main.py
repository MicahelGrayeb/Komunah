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
from datetime import datetime
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

def sincronizar_horario_cron(scheduler_instancia):
    """
    Revisa Firebase cada 60 segundos. 
    Solo reprograma si Firebase responde correctamente (ignora defaults).
    """
    def verificar_y_actualizar():
        import time as _time_sync
        inicio = _time_sync.time()
        try:
            logger.info("🕒 SYNC: Iniciando verificación de configuración en Firebase...")
            repo = FirebaseRepository()
            config = repo.obtener_config_recordatorios_seguro("komunah")
            
            duracion = _time_sync.time() - inicio
            
            # Si Firebase no respondió bien, NO reprogramar
            if config is None:
                logger.warning(f"⚠️ SYNC: Firebase no respondió tras {duracion:.2f}s, saltando verificación.")
                return
            
            nueva_h = int(config["hora"])
            nueva_m = int(config["minuto"])

            job = scheduler_instancia.get_job(JOB_ID_BARRIDO)
            if job:
                hora_actual_str = str(job.trigger.fields[5])
                min_actual_str = str(job.trigger.fields[6])

                logger.info(f"🔍 SYNC: Cron actual={hora_actual_str}:{min_actual_str} | Firebase={nueva_h}:{nueva_m:02d} (t={duracion:.2f}s)")

                if hora_actual_str != str(nueva_h) or min_actual_str != str(nueva_m):
                    logger.info(f"🔄 SYNC: Cambio detectado en Firebase ({nueva_h}:{nueva_m:02d}). Ajustando Cron...")
                    scheduler_instancia.reschedule_job(
                        JOB_ID_BARRIDO, 
                        trigger='cron', 
                        hour=nueva_h, 
                        minute=nueva_m
                    )
                    logger.info(f"✅ Cron reprogramado exitosamente a {nueva_h}:{nueva_m:02d}.")
                else:
                    logger.info(f"✅ SYNC: Sin cambios, horario correcto (t={duracion:.2f}s).")
            else:
                logger.warning(f"⚠️ SYNC: No se encontró el job '{JOB_ID_BARRIDO}' en el scheduler.")
        except Exception as e:
            duracion = _time_sync.time() - inicio
            logger.error(f"❌ Error en sincronización tras {duracion:.2f}s: {e}")

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
    allow_origin_regex=r"https?://.*", 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
) 

def tarea_diaria_notificaciones():  
    """Lógica distribuida con lock atómico: Solo UN Cloud Run ejecuta, los demás observan."""
    import asyncio
    import time as _time
    from google.cloud import firestore as firestore_lib

    db = SessionLocal()
    try:
        repo = FirebaseRepository()
        hoy_str = datetime.now(ZoneInfo("America/Mexico_City")).strftime('%Y-%m-%d')
        config = repo.obtener_config_recordatorios("komunah")
        lock_id = f"{hoy_str}_{config['hora']}_{config['minuto']}"
        lock_ref = repo.db.collection("empresas").document("komunah").collection("configuracion").document("lock_cron")
        
        # ══════════════════════════════════════════════════════
        # LOCK ATÓMICO: Transacción de Firestore
        # Garantiza que solo UNA instancia gane, sin importar
        # cuántas lean el lock al mismo tiempo.
        # ══════════════════════════════════════════════════════
        LOCK_TIMEOUT_SECONDS = 900  # 15 minutos

        @firestore_lib.transactional
        def intentar_tomar_lock(transaction):
            ahora = datetime.now(ZoneInfo("America/Mexico_City"))
            snapshot = lock_ref.get(transaction=transaction)
            
            if snapshot.exists:
                data = snapshot.to_dict()
                
                if data.get("last_run_id") == lock_id:
                    state = data.get("state", "")
                    ts = data.get("timestamp", "")
                    
                    # Si está RUNNING, verificar si el lock está muerto (timeout)
                    if state == "RUNNING" and ts:
                        try:
                            lock_time = datetime.fromisoformat(ts)
                            if lock_time.tzinfo is None:
                                lock_time = lock_time.replace(tzinfo=ZoneInfo("America/Mexico_City"))
                            elapsed = (ahora - lock_time).total_seconds()
                            if elapsed > LOCK_TIMEOUT_SECONDS:
                                logger.warning(f"⏰ Lock expirado ({elapsed:.0f}s). Reclamando control...")
                                transaction.set(lock_ref, {
                                    "last_run_id": lock_id,
                                    "state": "RUNNING",
                                    "current_log": f"Reclamado tras timeout de {elapsed:.0f}s",
                                    "timestamp": ahora.isoformat()
                                })
                                return "GANADOR"
                        except Exception as e:
                            logger.error(f"⚠️ Error verificando timeout del lock: {e}")
                    
                    # Lock existe para esta ejecución y está activo o terminado → ESPEJO
                    return "ESPEJO"
            
            # No hay lock o es de otra ejecución → TOMAR
            transaction.set(lock_ref, {
                "last_run_id": lock_id,
                "state": "RUNNING",
                "current_log": "Iniciando proceso...",
                "timestamp": ahora.isoformat()
            })
            return "GANADOR"

        transaction = repo.db.transaction()
        resultado_lock = intentar_tomar_lock(transaction)

        # --- MODO ESPEJO (Para los otros clones) ---
        if resultado_lock == "ESPEJO":
            logger.info(f"🔗 [CLONE_SYNC] Vinculado a ejecución activa: {lock_id}")
            
            last_msg = ""
            timeout_espejo = _time.time() + LOCK_TIMEOUT_SECONDS
            while _time.time() < timeout_espejo:
                status_doc = lock_ref.get()
                if not status_doc.exists: break
                status = status_doc.to_dict()
                state = status.get("state")
                msg = status.get("current_log", "")
                
                if msg != last_msg:
                    logger.info(f"📡 [REMOTE_LOG]: {msg}")
                    last_msg = msg
                
                if state in ["COMPLETED", "FAILED"]:
                    logger.info(f"✅ [CLONE_SYNC] Ejecución finalizada. Cerrando log espejo.")
                    break
                _time.sleep(2)
            return

        # --- MODO GANADOR (El que hace la chamba) ---
        logger.info(f"🔥 [MASTER] Tomando control de la ejecución: {lock_id}")
        gateway = NotificationGateway()
        use_case = NotificationUseCase(repo, gateway)
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            # 🚀 Inciando las fases del Ganador
            loop.run_until_complete(use_case.ejecutar_barrido_automatico("komunah", config["dias_1"], "Recordatorio de Pago", db, "normal", lock_ref=lock_ref))
            loop.run_until_complete(use_case.ejecutar_barrido_automatico("komunah", config["dias_1"], "Recordatorio de Pago Vencido", db, "deudores", lock_ref=lock_ref))
            
            if config["dias_1"] != config["dias_2"]:
                loop.run_until_complete(use_case.ejecutar_barrido_automatico("komunah", config["dias_2"], "Recordatorio de Pago", db, "normal", lock_ref=lock_ref))
                loop.run_until_complete(use_case.ejecutar_barrido_automatico("komunah", config["dias_2"], "Recordatorio de Pago Vencido", db, "deudores", lock_ref=lock_ref))

            lock_ref.update({"state": "COMPLETED", "current_log": "Barrido terminado exitosamente."})
            logger.info(f"🏆 [MASTER_DONE] Ejecución {lock_id} finalizada.")
        finally:
            loop.close()
            
    except Exception as e:
        logger.error(f"❌ [CRITICAL] Error en main: {str(e)}")
        if 'lock_ref' in locals(): lock_ref.update({"state": "FAILED", "current_log": f"Error: {str(e)}"})
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
        logger.info(f"🚀 Scheduler iniciado: Barrido configurado a las {config['hora']:02d}:{config['minuto']:02d}")

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
    app.include_router(notificacionesMS.router_usuario)
    app.include_router(notificacionesMS.router_globales)
    app.include_router(notificacionesMS.router_crud)
    app.include_router(notificacionesMS.router_wa)
    app.include_router(notificacionesMS.router_documento)
    app.include_router(notificacionesMS.router_anexo)
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