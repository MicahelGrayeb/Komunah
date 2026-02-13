import pandas as pd
import os
import re

ARCHIVO_SALIDA_SQL = 'comunah_local.sql' 

ARCHIVOS_A_CARGAR = [
    'reporte_amortizaciones_completo.csv',
    'reporte_antig_saldos_completo.csv',   
    'reporte_cartera_vencida_completo.csv',
    'reporte_clientes_completo.csv',     
    'reporte_pagos_completo.csv',
    'reporte_ventas_completo.csv'
]



def obtener_nombre_tabla(nombre_archivo):
    """Limpia el nombre del archivo para usarlo como nombre de tabla."""
    name = re.sub(r'^reporte_|_completo\.csv$', '', nombre_archivo)
    return name

def mapear_tipo_dato(dtype):
    """Decide manualmente qué tipo de dato SQL usar."""
    dtype_str = str(dtype)
    if 'int' in dtype_str:
        return 'BIGINT'
    elif 'float' in dtype_str:
        return 'DOUBLE'
    elif 'datetime' in dtype_str:
        return 'DATETIME'
    else:
        return 'TEXT'

def generar_sql_dump():
    print(f"🔨 Iniciando generación manual de SQL en: {ARCHIVO_SALIDA_SQL}...")
    
    with open(ARCHIVO_SALIDA_SQL, 'w', encoding='utf-8') as f:
        
        f.write("-- Script generado manualmente (Fuerza Bruta)\n")
        f.write("SET FOREIGN_KEY_CHECKS=0;\n")
        f.write("SET SQL_MODE = 'NO_AUTO_VALUE_ON_ZERO';\n\n")
        
        for archivo in ARCHIVOS_A_CARGAR:
            
            nombre_tabla = obtener_nombre_tabla(archivo)
            
            if not os.path.exists(archivo):
                print(f"\nERROR: No encuentro '{archivo}'. Saltando.")
                continue

            print(f"\n--- Procesando {archivo} (Tabla: {nombre_tabla}) ---")
            
            try:
         
                df = pd.read_csv(archivo)
                print(f"   📄 {len(df)} filas leídas.")
                
       
                defs_columnas = []
                for columna, tipo in df.dtypes.items():
                    tipo_sql = mapear_tipo_dato(tipo)
                  
                    defs_columnas.append(f"`{columna}` {tipo_sql}")
                
                cols_str = ",\n  ".join(defs_columnas)
                create_cmd = f"CREATE TABLE IF NOT EXISTS `{nombre_tabla}` (\n  {cols_str}\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
                
                f.write(f"-- ESTRUCTURA: {nombre_tabla}\n")
                f.write(f"DROP TABLE IF EXISTS `{nombre_tabla}`;\n")
                f.write(create_cmd + ";\n\n")

                f.write(f"-- DATOS: {nombre_tabla}\n")
                
                columnas_sql = ", ".join([f"`{c}`" for c in df.columns])
                
                for index, row in df.iterrows():
                    valores_fila = []
                    for val in row:
                        if pd.isna(val):
                            valores_fila.append("NULL")
                        else:
                        
                            val_str = str(val).replace("\\", "\\\\").replace("'", "''")
                            valores_fila.append(f"'{val_str}'")
                    
                    vals_str = ", ".join(valores_fila)
                    f.write(f"INSERT INTO `{nombre_tabla}` ({columnas_sql}) VALUES ({vals_str});\n")

                print(f"   ✅ Tabla '{nombre_tabla}' generada correctamente.")

            except Exception as e:
                print(f"   ❌ ERROR FATAL en '{archivo}': {e}")
                
        f.write("\nSET FOREIGN_KEY_CHECKS=1;\n")
    
    print(f"\n🎉 LISTO. Archivo SQL generado sin usar librerías raras.")

if __name__ == '__main__':
    generar_sql_dump()