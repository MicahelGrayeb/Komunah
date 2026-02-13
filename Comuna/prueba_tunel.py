import os
import time
from sshtunnel import SSHTunnelForwarder
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Cargamos usuario/pass de la BD desde el .env
load_dotenv()

# --- 1. TUS DATOS DE ACCESO AL SERVIDOR (SSH/FTP) ---
# OJO: Este usuario NO suele ser el mismo de la base de datos.
# Es el usuario principal de tu cuenta Plesk o FTP.
SSH_HOST = '192.99.206.64'
SSH_USER = 'ialabcon'  # <--- CAMBIA ESTO por tu usuario de sistema/FTP (ej. admin, techmaleon, etc)
SSH_PASS = 'E$Kz3qKc!OK6^jJ3' # <--- CAMBIA ESTO

# --- 2. DATOS DE LA BASE DE DATOS (Vienen del .env) ---
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

print(f"🔌 Intentando conectar vía Túnel SSH...")
print(f"   Servidor: {SSH_HOST} | Usuario SSH: {SSH_USER}")

try:
    # A. Abrimos el Túnel
    server = SSHTunnelForwarder(
        (SSH_HOST, 22),           # Conectamos al puerto 22 del servidor remoto
        ssh_username=SSH_USER,
        ssh_password=SSH_PASS,
        remote_bind_address=('127.0.0.1', 3306) # Apuntamos a la BD dentro del servidor
    )
    
    server.start() # Iniciamos el túnel
    
    print(f"✅ Túnel establecido exitosamente.")
    print(f"   El puerto local {server.local_bind_port} ahora redirige a Plesk.")

    # B. Conectamos SQLAlchemy a través del túnel
    # Nota cómo usamos '127.0.0.1' y el puerto local que nos dio el túnel
    local_port = server.local_bind_port
    db_url = f"mysql+mysqlconnector://{DB_USER}:{DB_PASS}@127.0.0.1:{local_port}/{DB_NAME}"
    
    engine = create_engine(db_url)
    
    # C. Probamos la conexión
    with engine.connect() as connection:
        result = connection.execute(text("SELECT DATABASE();"))
        db_actual = result.scalar()
        print(f"\n🎉 ¡ÉXITO TOTAL! Conectado a la BD: '{db_actual}'")
        
        # Opcional: ver tablas para confirmar
        tablas = connection.execute(text("SHOW TABLES;"))
        print("📂 Tablas detectadas:", [t[0] for t in tablas])

    # Cerramos el túnel al terminar
    server.stop()
    print("\n🔌 Túnel cerrado correctamente.")

except Exception as e:
    print("\n❌ ERROR:")
    print(e)
    print("\n💡 PISTAS SI FALLA:")
    print("1. 'Authentication failed': Tu usuario SSH_USER o SSH_PASS son incorrectos (revisa credenciales de FTP/Plesk).")
    print("2. 'Could not establish connection': El puerto 22 puede estar bloqueado o el servicio SSH apagado en Plesk.")