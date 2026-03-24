import os
import time
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv

load_dotenv()

# Configuración de variables de entorno
SSH_HOST = os.getenv("SSH_HOST")
SSH_USER = os.getenv("SSH_USER")
SSH_PASS = os.getenv("SSH_PASS")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

print("🔌 Inicializando sistema de base de datos...")

tunnel_server = None
engine = None

def start_tunnel():
    """Función para iniciar o reiniciar el túnel SSH"""
    global tunnel_server, engine
    
    if SSH_HOST and SSH_USER and SSH_PASS:
        try:
            # Si ya hay un túnel y no está activo, lo cerramos antes de reabrir
            if tunnel_server:
                tunnel_server.stop()
            
            print(f"Iniciando Túnel SSH hacia {SSH_HOST}...")
            tunnel_server = SSHTunnelForwarder(
                (SSH_HOST, 22),
                ssh_username=SSH_USER,
                ssh_password=SSH_PASS,
                remote_bind_address=('127.0.0.1', 3306),
                # MEJORA 1: Keepalive para que no se cierre por inactividad
                set_keepalive=60.0 
            )
            tunnel_server.start()
            
            db_port = tunnel_server.local_bind_port
            db_host = "127.0.0.1"
            print(f"✅ Túnel establecido en puerto: {db_port}")
            
            # (Re)creamos el engine con el nuevo puerto del túnel
            SQLALCHEMY_DATABASE_URL = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{db_host}:{db_port}/{DB_NAME}"
            engine = create_engine(
                SQLALCHEMY_DATABASE_URL,
                pool_recycle=3600,
                pool_pre_ping=True
            )
        except Exception as e:
            print(f"❌ Error al iniciar túnel: {e}")
            raise e
    else:
        print("⚠️ Conexión directa (sin túnel SSH)")
        db_host = os.getenv("DB_HOST", "127.0.0.1")
        db_port = os.getenv("DB_PORT", 3306)
        SQLALCHEMY_DATABASE_URL = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{db_host}:{db_port}/{DB_NAME}"
        engine = create_engine(SQLALCHEMY_DATABASE_URL, pool_recycle=3600, pool_pre_ping=True)

# Intento inicial de arranque
start_tunnel()

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    """Generador de sesión con auto-recuperación de túnel"""
    global tunnel_server
    
    # MEJORA 2: Verificar si el túnel se cayó antes de dar la sesión
    if tunnel_server is not None and not tunnel_server.is_active:
        print("🔄 Túnel detectado como inactivo. Reiniciando...")
        start_tunnel()
        # Actualizamos la conexión de la sesión con el nuevo engine
        SessionLocal.configure(bind=engine)

    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()