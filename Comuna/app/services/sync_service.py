import os
import json
import logging
import pandas as pd
from datetime import datetime
from google.cloud import bigquery
from google.oauth2 import credentials
from sqlalchemy import create_engine, text
from sqlalchemy.types import DECIMAL, BIGINT, DOUBLE, TEXT, VARCHAR

# Importación de tus modelos
from app.models import Pago, Venta, Cartera, AntigSaldos, Amortizacion, Cliente, GestionClientes, ConfigEtapa

logger = logging.getLogger(__name__)

class AutoSyncManager:
    def __init__(self):
        # 1. Parche Firebase
        import firebase_admin
        if not firebase_admin._apps:
            try:
                firebase_admin.initialize_app(options={'projectId': 'comuna-480820'})
            except Exception as e:
                logger.warning(f"Nota Firebase: {e}")

        # 2. Configuración BigQuery
        os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
        self.project_id = 'adaracrm-replicacq6pearyt88g'
        self.dataset_id = 'adaracrm_komunah'
        self.billing_project = 'comuna-480820'

        json_data = os.environ.get("GOOGLE_JSON_KEY")
        if json_data:
            try:
                info = json.loads(json_data)
                creds = credentials.Credentials.from_authorized_user_info(info)
                self.client = bigquery.Client(credentials=creds, project=self.billing_project)
            except Exception as e:
                self.client = bigquery.Client(project=self.billing_project)
        else:
            self.client = bigquery.Client(project=self.billing_project)

        from app.database import SessionLocal
        db = SessionLocal()
        self.engine = db.get_bind()
        db.close()

    def estandarizar_fechas(self, df):
        """Limpia fechas y optimiza para evitar el warning de performance."""
        for col in df.columns:
            if any(x in col.lower() for x in ['fecha', 'date', 'created_at']):
                df[col] = pd.to_datetime(df[col], errors='coerce', dayfirst=True).dt.strftime('%Y-%m-%d')
                df[col] = df[col].replace(['NaT', 'nan', 'None'], None)
        return df

    def ejecutar_sync_total(self):
        """Sincroniza las 8 tablas procesando una por una para no reventar la RAM."""
        try:
            tablas_fuente = ['ventas', 'pagos', 'antig_saldos', 'cartera_vencida', 'clientes', 'amortizaciones']
            
            with self.engine.connect() as conn:
                df_gestion_old = pd.read_sql("SELECT * FROM notificaciones_gestion_clientes", conn)
                df_etapas_old = pd.read_sql("SELECT * FROM config_etapas", conn)

            df_ventas_ref = None
            df_clientes_ref = None
            # --- PROCESO SECUENCIAL (Una por una) ---
            for t in tablas_fuente:
                logger.info(f"📡 Descargando {t}...")
                query = f"SELECT * FROM `{self.project_id}.{self.dataset_id}.{t}`"
                
                df_temp = self.estandarizar_fechas(self.client.query(query).to_dataframe())
                
                if t == 'ventas':
                    df_ventas_ref = df_temp.copy()
                if t == 'clientes':
                    # SOLO ID y datos de contacto
                    df_clientes_ref = df_temp[['client_id', 'email', 'main_phone']].copy()
                
                self._escribir_tabla_individual(t, df_temp)
                del df_temp # Liberar RAM de inmediato
                import gc; gc.collect()

            # --- TABLAS CALCULADAS ---
            if df_ventas_ref is not None and df_clientes_ref is not None:
                logger.info("⏳ Reconstruyendo Gestión y Etapas...")
                
                # 1. Gestión Clientes
                df_g = self._reconstruir_gestion(df_ventas_ref, df_clientes_ref, df_gestion_old)
                self._escribir_tabla_individual("notificaciones_gestion_clientes", df_g)
                
                # 2. Config Etapas
                df_e = self._reconstruir_etapas(df_ventas_ref, df_etapas_old)
                self._escribir_tabla_individual("config_etapas", df_e)
                
                self._generar_reporte(df_gestion_old, df_g)

            # --- OPTIMIZACIÓN FINAL (Índices idénticos al SQL) ---
            self._aplicar_indices_y_llaves()
            
            logger.info("🏁 Sincronización completa. Base de datos lista y rápida.")

        except Exception as e:
            logger.error(f"❌ Error crítico en ejecución: {e}")

    def _escribir_tabla_individual(self, name, df):
        """Escribe usando el método estándar (más compatible con el túnel SSH)."""
        dtype_map = {}
        for col in df.columns:
            col_lower = col.lower()
            if col_lower == 'id':
                dtype_map[col] = BIGINT
            elif any(x in col_lower for x in ['monto', 'total', 'saldo', 'pagado', 'pagar', 'días', 'vigente']):
                dtype_map[col] = DECIMAL(20, 4)
            elif any(x in col_lower for x in ['id', 'folio', 'folder_id', 'number']):
                dtype_map[col] = VARCHAR(150)
            else:
                dtype_map[col] = TEXT

        logger.info(f"🚀 Subiendo {len(df)} filas a `{name}`...")
        
        with self.engine.begin() as conn:
            # Quitamos method='multi' porque el driver de MySQL se apendeja con tablas anchas
            df.to_sql(
                name, 
                con=conn, 
                if_exists='replace', 
                index=False, 
                chunksize=5000, # Subimos el bloque para compensar la velocidad
                dtype=dtype_map
            )
            logger.info(f"   ✅ `{name}` actualizada correctamente.")

    def _aplicar_indices_y_llaves(self):
        """Agrega Primary Keys e Índices tal como en tu archivo SQL."""
        scripts = [
            # 1. Definir Primary Keys (Necesarias para SQLAlchemy)
            "ALTER TABLE config_etapas MODIFY id BIGINT NOT NULL, ADD PRIMARY KEY (id);",
            "ALTER TABLE notificaciones_gestion_clientes MODIFY id BIGINT NOT NULL, ADD PRIMARY KEY (id);",
            
            # 2. Índices de Relación (Los que hacen rápidas las consultas)
            "CREATE INDEX idx_ventas_folio ON ventas (FOLIO);",
            "CREATE INDEX idx_pagos_folio ON pagos (`Folio de la venta`);",
            "CREATE INDEX idx_amort_folder ON amortizaciones (folder_id);",
            "CREATE INDEX idx_gestion_folio ON notificaciones_gestion_clientes (folio);",
            "CREATE INDEX idx_antig_folio ON antig_saldos (FOLIO);",
            
            # 3. Optimizar el motor de búsqueda interno
            "ANALYZE TABLE ventas, pagos, amortizaciones, notificaciones_gestion_clientes, antig_saldos, config_etapas;"
        ]
        
        with self.engine.begin() as conn:
            for sql in scripts:
                try:
                    conn.exec_driver_sql(sql)
                except Exception as e:
                    logger.debug(f"Nota: {e}") # Ignorar si el índice ya existe

    def _reconstruir_gestion(self, df_v, df_c, df_old):
        """
        Une ventas con clientes reales, elimina basura y 
        SALVA TODOS los estados de los switches configurados.
        """
        registros = []
        
        # 1. Preparar tabla maestra de Clientes
        df_c = df_c.copy()
        df_c['client_id'] = df_c['client_id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
        df_c_datos = df_c[['client_id', 'email', 'main_phone']].drop_duplicates('client_id')

        for i in range(1, 7):
            suf = f"_{i}" if i > 1 else ""
            id_col = f'ID CLIENTE{suf}'
            name_col = f'CLIENTE{suf}'
            
            if id_col in df_v.columns:
                temp = df_v[['FOLIO', id_col, name_col, 'DESARROLLO', 'ETAPA', 'METROS CUADRADOS']].copy()
                temp.columns = ['folio', 'client_id', 'client_name', 'proyecto', 'etapa_cluster', 'm2']
                
                # Limpieza de ID
                temp['client_id'] = temp['client_id'].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()
                
                # --- FILTRO AGRESIVO: No procesar basura ---
                temp = temp[temp['client_id'].notna()]
                temp = temp[~temp['client_id'].isin(['nan', 'None', '', '<NA>', 'NaN', 'null'])]
                
                if temp.empty:
                    continue

                # 2. Cruce con datos de contacto
                temp = pd.merge(temp, df_c_datos, on='client_id', how='left')
                temp = temp.rename(columns={'main_phone': 'telefono'})
                
                if 'telefono' in temp.columns:
                    temp['telefono'] = temp['telefono'].astype(str).str.replace(r'[\s\.\-\(\)]', '', regex=True)
                
                temp['es_propietario_principal'] = 1 if i == 1 else 0
                registros.append(temp)

        if not registros:
            return pd.DataFrame()

        # 3. Unir todo
        df_new = pd.concat(registros).drop_duplicates(subset=['folio', 'client_id'])
        
        # 4. SALVAGUARDA DE SWITCHES (La clave)
        # Definimos TODOS los switches que quieres salvar
        cols_sw = [
            'permite_email_lote', 
            'permite_whatsapp_lote', 
            'permite_marketing_email', 
            'permite_marketing_whatsapp'
        ]

        # Limpiamos llaves de comparación para asegurar el match
        for col in ['folio', 'client_id']:
            df_new[col] = df_new[col].astype(str).str.strip()
            df_old[col] = df_old[col].astype(str).str.strip()

        # MERGE CON LA TABLA VIEJA:
        # Traemos los valores actuales de la base de datos local
        df_merged = pd.merge(
            df_new, 
            df_old[['folio', 'client_id'] + cols_sw], 
            on=['folio', 'client_id'], 
            how='left'
        )
        
        # Si el registro es nuevo (no estaba en df_old), le ponemos 1 (activo)
        # Si ya existía, fillna no hará nada y se quedará con el valor (0 o 1) que ya tenías
        df_merged[cols_sw] = df_merged[cols_sw].fillna(1).astype(int)
        
        # Re-insertamos el ID autoincremental para la base de datos
        if 'id' in df_merged.columns: df_merged = df_merged.drop(columns=['id'])
        df_merged.insert(0, 'id', range(1, len(df_merged) + 1))
        
        return df_merged

    def _reconstruir_etapas(self, df_v, df_old):
        """Mantiene estados activo/inactivo por proyecto y etapa."""
        df_etapas = df_v[['DESARROLLO', 'ETAPA']].drop_duplicates().copy()
        df_etapas.columns = ['proyecto', 'etapa']
        counts = df_v.groupby(['DESARROLLO', 'ETAPA'])['FOLIO'].nunique().reset_index()
        counts.columns = ['proyecto', 'etapa', 'total_folios']
        
        df_merged = pd.merge(df_etapas, counts, on=['proyecto', 'etapa'], how='left')
        df_merged = pd.merge(df_merged, df_old[['proyecto', 'etapa', 'etapa_activo', 'proyecto_activo']], on=['proyecto', 'etapa'], how='left')
        
        df_merged[['etapa_activo', 'proyecto_activo']] = df_merged[['etapa_activo', 'proyecto_activo']].fillna(1).astype(int)
        df_merged['total_folios'] = df_merged['total_folios'].fillna(0).astype(int)
        
        # Inyectamos ID PK para config_etapas
        if 'id' not in df_merged.columns:
            df_merged.insert(0, 'id', range(1, len(df_merged) + 1))
            
        return df_merged

    def _generar_reporte(self, df_old, df_new):
        """Genera un reporte detallado de altas y bajas agrupado por proyecto."""
        
        # 1. Limpieza de llaves para comparación exacta
        for df in [df_old, df_new]:
            for col in ['folio', 'client_id']:
                if col in df.columns:
                    df[col] = df[col].astype(str).str.replace(r'\.0$', '', regex=True).str.strip()

        # 2. Identificación de Altas y Bajas usando índices compuestos
        old_keys = df_old.set_index(['folio', 'client_id']).index
        new_keys = df_new.set_index(['folio', 'client_id']).index

        # Altas: están en el nuevo pero no en el viejo
        altas_idx = new_keys.difference(old_keys)
        # Bajas: están en el viejo pero no en el nuevo
        bajas_idx = old_keys.difference(new_keys)

        df_altas = df_new[df_new.set_index(['folio', 'client_id']).index.isin(altas_idx)]
        df_bajas = df_old[df_old.set_index(['folio', 'client_id']).index.isin(bajas_idx)]

        # 3. Impresión del Reporte Detallado
        print(f"\n{'='*60}")
        print(f"📊 REPORTE DETALLADO DE SINCRONIZACIÓN - {datetime.now().strftime('%d/%m/%Y %H:%M')}")
        print(f"{'='*60}")
        
        print(f"👥 Resumen General:")
        print(f"   - Total Clientes en BigQuery: {len(df_new)}")
        print(f"   - Clientes Nuevos (Altas):    {len(df_altas)}")
        print(f"   - Clientes Removidos (Bajas): {len(df_bajas)}")
        print(f"{'-'*60}")

        # Detalle de Altas agrupado por Proyecto
        if not df_altas.empty:
            print(f"\n✅ DETALLE DE ALTAS (Nuevos registros):")
            for proyecto, grupo in df_altas.groupby('proyecto'):
                print(f"\n  🏢 PROYECTO: {proyecto}")
                for _, r in grupo.iterrows():
                    print(f"    [+] Folio: {r['folio'].ljust(8)} | Cliente: {r['client_name']}")

        # Detalle de Bajas agrupado por Proyecto
        if not df_bajas.empty:
            print(f"\n⚠️ DETALLE DE BAJAS (Registros eliminados):")
            for proyecto, grupo in df_bajas.groupby('proyecto'):
                print(f"\n  🏢 PROYECTO: {proyecto}")
                for _, r in grupo.iterrows():
                    print(f"    [-] Folio: {r['folio'].ljust(8)} | Cliente: {r['client_name']}")

        print(f"\n{'='*60}")
        print("🏁 Fin del reporte de sincronización.")