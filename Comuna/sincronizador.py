import pandas as pd
from google.cloud import bigquery
import os

def generar_respaldo_fiel_local():
    # 1. Configuración de BigQuery
    project_id = 'adaracrm-replicacq6pearyt88g'
    dataset_id = 'adaracrm_komunah'
    archivo_sql = 'Komunah_local.sql'
    
    tablas = ['amortizaciones', 'antig_saldos', 'cartera_vencida', 'clientes', 'pagos', 'ventas']
    
    # Iniciamos cliente de Google
    client = bigquery.Client(project=project_id)
    
    print(f"--- 📡 Conectando a BigQuery (Proyecto: {project_id}) ---")
    
    with open(archivo_sql, 'w', encoding='utf-8') as f:
        # Encabezados para que el SQL sea limpio
        f.write("SET FOREIGN_KEY_CHECKS=0;\nSET NAMES utf8mb4;\n\n")
        
        for table_id in tablas:
            full_table_id = f"{project_id}.{dataset_id}.{table_id}"
            
            try:
                # Descarga directa a memoria
                df = client.list_rows(full_table_id).to_dataframe()
                
                # --- AQUÍ ESTÁ EL CONTEO QUE PEDISTE ---
                total_filas = len(df)
                print(f"✅ Tabla: {table_id.upper().ljust(15)} | Filas detectadas: {total_filas}")

                if total_filas == 0:
                    print(f"   ⚠️ Saltando {table_id} (está vacía)...")
                    continue

                # --- CREACIÓN DE TABLA CON FIDELIDAD ---
                f.write(f"-- Estructura para la tabla: {table_id}\n")
                f.write(f"DROP TABLE IF EXISTS `{table_id}`;\n")
                
                columnas_def = []
                for col in df.columns:
                    # Lógica de tipos para no perder centavos ni precisión
                    col_lower = col.lower()
                    if any(x in col_lower for x in ['monto', 'total', 'saldo', 'pagado', 'pagar', 'días', 'vigente']):
                        tipo_sql = "DECIMAL(20,4)" # Fiel al centavo
                    elif 'int' in str(df[col].dtype):
                        tipo_sql = "BIGINT"
                    elif 'float' in str(df[col].dtype):
                        tipo_sql = "DOUBLE"
                    else:
                        tipo_sql = "TEXT"
                    columnas_def.append(f"  `{col}` {tipo_sql}")
                
                f.write(f"CREATE TABLE `{table_id}` (\n" + ",\n".join(columnas_def) + "\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;\n\n")

                # --- GENERACIÓN DE INSERTS ---
                f.write(f"-- Datos para la tabla: {table_id}\n")
                cols_nombres = ", ".join([f"`{c}`" for c in df.columns])
                
                # Procesamos fila por fila para asegurar la limpieza de datos
                for _, row in df.iterrows():
                    valores_limpios = []
                    for val in row:
                        if pd.isna(val):
                            valores_limpios.append("NULL")
                        else:
                            # Escapamos comillas simples para que el SQL no se rompa
                            texto = str(val).replace("'", "''").replace("\\", "\\\\")
                            valores_limpios.append(f"'{texto}'")
                    
                    f.write(f"INSERT INTO `{table_id}` ({cols_nombres}) VALUES (" + ", ".join(valores_limpios) + ");\n")
                
                f.write("\n")
                
            except Exception as e:
                print(f"❌ Error procesando {table_id}: {e}")

        f.write("SET FOREIGN_KEY_CHECKS=1;\n")

    print(f"\n--- 🏁 PROCESO TERMINADO ---")
    print(f"📄 Archivo generado localmente: '{archivo_sql}'")
    print(f"💡 Ya puedes importar este archivo en tu MySQL local o donde prefieras.")

if __name__ == "__main__":
    generar_respaldo_fiel_local()