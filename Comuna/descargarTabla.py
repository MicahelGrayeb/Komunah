import pandas as pd
from google.cloud import bigquery

def descargar_todas_las_tablas():
    
    project_id = 'adaracrm-replicacq6pearyt88g'
    dataset_id = 'adaracrm_komunah'
    
    # Lista de las 6 tablas que tienes en BigQuery
    tablas = [
        'amortizaciones', 
        'antig_saldos', 
        'cartera_vencida', 
        'clientes', 
        'pagos', 
        'ventas'
    ]
    
    print(f"🚀 Iniciando conexión con BigQuery para el proyecto: {project_id}")
    # El cliente usará tu cuenta activa de Google
    client = bigquery.Client(project=project_id)
    
    for table_id in tablas:
        full_table_id = f"{project_id}.{dataset_id}.{table_id}"
        nombre_archivo = f"reporte_{table_id}_completo.csv"
        
        print(f"\n--- Procesando tabla: {table_id} ---")
        
        try:
            # 2. Descargar los datos directamente a un DataFrame
            # list_rows es más rápido para traer todo el contenido
            df = client.list_rows(full_table_id).to_dataframe()
            
            filas = len(df)
            print(f"✅ Descarga completada: Se obtuvieron {filas} filas.")
            
            # 3. Guardar en tu computadora con codificación para Excel
            df.to_csv(nombre_archivo, index=False, encoding='utf-8-sig')
            print(f"💾 Archivo guardado como: '{nombre_archivo}'")
            
        except Exception as e:
            print(f"❌ ERROR al descargar {table_id}:")
            print(e)

if __name__ == "__main__":
    descargar_todas_las_tablas()
    print("\n🎉 ¡Proceso terminado! Ya tienes todos tus CSVs listos.")