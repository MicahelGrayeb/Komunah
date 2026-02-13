from app.database import engine
from sqlalchemy import text

try:
    print("📡 Iniciando prueba de conexión a Plesk...")
    
    # Intentamos conectar
    with engine.connect() as connection:
        result = connection.execute(text("SELECT DATABASE();"))
        db_actual = result.scalar()
        
        print(f"\n✅ ¡ÉXITO TOTAL! Conexión establecida.")
        print(f"Estás conectado a la base de datos remota: '{db_actual}'")
        
except Exception as e:
    print("\n❌ FALLO LA CONEXIÓN")
    print(f"Error: {e}")
    print("\nPOSIBLE CAUSA:")
    print("Si el error dice 'Timeout' o 'Can't connect', significa que el Firewall de Plesk está bloqueando tu IP.")