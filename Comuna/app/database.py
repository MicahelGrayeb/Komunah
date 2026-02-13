import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sshtunnel import SSHTunnelForwarder
from dotenv import load_dotenv

load_dotenv()


SSH_HOST = os.getenv("SSH_HOST")
SSH_USER = os.getenv("SSH_USER")
SSH_PASS = os.getenv("SSH_PASS")

DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

print("🔌 Inicializando sistema de base de datos...")

# Variable global para mantener el túnel vivo
tunnel_server = None
engine = None

try:
    if SSH_HOST and SSH_USER and SSH_PASS:
        print(f"Iniciando Túnel SSH hacia {SSH_HOST}...")
        tunnel_server = SSHTunnelForwarder(
            (SSH_HOST, 22),
            ssh_username=SSH_USER,
            ssh_password=SSH_PASS,
            remote_bind_address=('127.0.0.1', 3306)
        )
        tunnel_server.start()
        print(f"✅ Túnel establecido en puerto local: {tunnel_server.local_bind_port}")
        
        # Conectamos a localhost (puerto del túnel)
        db_port = tunnel_server.local_bind_port
        db_host = "127.0.0.1"
    else:
        # Fallback por si no usas túnel (ej. local directo)
        print("⚠️ No se detectaron credenciales SSH, intentando conexión directa...")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT", 3306)

    SQLALCHEMY_DATABASE_URL = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@{db_host}:{db_port}/{DB_NAME}"

    engine = create_engine(
        SQLALCHEMY_DATABASE_URL,
        pool_recycle=3600,
        pool_pre_ping=True
    )
    print("Motor SQL iniciado correctamente.")

except Exception as e:
    print(f"Error CRÍTICO en base de datos: {e}")
    raise e

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()