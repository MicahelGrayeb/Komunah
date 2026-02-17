import logging
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, case, cast, Date
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from ..database import get_db
from ..models import Venta, Pago, Amortizacion # Asumiendo que Amortizacion/Liquidado se liga aqui
import calendar

# Configuración de logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/dashboard", tags=["Dashboard BI"])

# --- CONSTANTES Y UTILIDADES ---

CONTRACT_STATUS_WHITELIST = [
    'firmado por cliente', 'firma de testigos', 'contrato firmado', 
    'liquidado', 'proceso de escritura', 'proceso escritura', 
    'agenda escritura', 'escriturado'
]

def parse_date_param(date_str: str):
    """Convierte string YYYY-MM-DD a objeto date."""
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except ValueError:
        raise HTTPException(status_code=400, detail=f"Formato de fecha inválido: {date_str}. Use YYYY-MM-DD")

def get_full_month_range_previous_year(date_obj):
    try:
        prev_year_date = date_obj.replace(year=date_obj.year - 1)
    except ValueError:
        prev_year_date = date_obj.replace(year=date_obj.year - 1, month=2, day=28)
    
    # Primer día del mes del año pasado
    prev_start_date = prev_year_date.replace(day=1)
    
    # Último día del mes del año pasado
    # calendar.monthrange devuelve (dia_semana, dias_totales)
    _, last_day = calendar.monthrange(prev_start_date.year, prev_start_date.month)
    prev_end_date = prev_start_date.replace(day=last_day)
    
    return prev_start_date.strftime("%Y-%m-%d"), prev_end_date.strftime("%Y-%m-%d")

# --- ENDPOINT 1: KPIs GLOBALES (Tarjetas Superiores) ---
@router.get("/KPIs")
def get_dashboard_kpis(
    start_date: str = Query(..., description="Fecha inicio YYYY-MM-DD"),
    end_date: str = Query(..., description="Fecha fin YYYY-MM-DD"),
    db: Session = Depends(get_db)
):
    """
    Calcula las tarjetas de métricas principales.
    Optimizado con SQL Aggregations y comparativa YoY (Mes completo).
    """
    try:
        # 1. KPIs Generales de Ventas en el rango actual (Mes seleccionado)
        base_query = db.query(
            func.count(Venta.folio).label("total_ventas"),
            func.sum(case((func.lower(Venta.estado_expediente) == 'ventas', 1), else_=0)).label("pipeline"),
            func.sum(case((func.lower(Venta.estado_expediente).in_([s.lower() for s in CONTRACT_STATUS_WHITELIST]), 1), else_=0)).label("contratos_firmados"),
            func.sum(case((func.lower(Venta.estado_expediente).in_([s.lower() for s in CONTRACT_STATUS_WHITELIST]), Venta.precio_final), else_=0)).label("valor_contratos"),
            func.sum(case((func.lower(Venta.estado_expediente).in_(['proceso de escritura', 'proceso escritura']), 1), else_=0)).label("proceso_escritura"),
            func.sum(case((func.lower(Venta.estado_expediente) == 'agenda escritura', 1), else_=0)).label("agenda_escritura")
        ).filter(
            Venta.fecha_inicio_operacion >= start_date,
            Venta.fecha_inicio_operacion <= end_date
        ).first()

        # 2. Cancelados y Expirados (Calculo Doble: Anual y Mes Actual)
        year_start = start_date[:4] + "-01-01"
        year_end = start_date[:4] + "-12-31"
        
        # --- Cancelados ---
        cancelados_anio = db.query(func.count(Venta.folio)).filter(
            func.lower(Venta.estado_expediente) == 'cancelado',
            Venta.fecha_inicio_operacion >= year_start,
            Venta.fecha_inicio_operacion <= year_end
        ).scalar() or 0

        cancelados_mes = db.query(func.count(Venta.folio)).filter(
            func.lower(Venta.estado_expediente) == 'cancelado',
            Venta.fecha_inicio_operacion >= start_date,
            Venta.fecha_inicio_operacion <= end_date
        ).scalar() or 0
        
        # --- Expirados ---
        expirados_anio = db.query(func.count(Venta.folio)).filter(
            func.lower(Venta.estado_expediente) == 'expirado',
            Venta.fecha_inicio_operacion >= year_start,
            Venta.fecha_inicio_operacion <= year_end
        ).scalar() or 0

        expirados_mes = db.query(func.count(Venta.folio)).filter(
            func.lower(Venta.estado_expediente) == 'expirado',
            Venta.fecha_inicio_operacion >= start_date,
            Venta.fecha_inicio_operacion <= end_date
        ).scalar() or 0

        # 3. Cálculo de Crecimiento (Growth) vs MES COMPLETO AÑO ANTERIOR
        
        # A. Obtener rango del año pasado
        s_date_obj = parse_date_param(start_date)
        prev_start_str, prev_end_str = get_full_month_range_previous_year(s_date_obj)
        
        # B. Ventas Totales Mes Año Pasado
        prev_month_sales = db.query(func.count(Venta.folio)).filter(
            Venta.fecha_inicio_operacion >= prev_start_str,
            Venta.fecha_inicio_operacion <= prev_end_str
        ).scalar() or 0

        # C. Ventas Totales Mes Actual
        current_month_sales = base_query.total_ventas or 0

        # D. Cálculo %
        growth = 0
        if prev_month_sales > 0:
            growth = ((current_month_sales - prev_month_sales) / prev_month_sales) * 100
        elif current_month_sales > 0:
            growth = 100

        # 4. Liquidados
        liquidados_count = db.query(func.count(Venta.folio)).filter(
            func.lower(Venta.estado_expediente) == 'liquidado',
            Venta.fecha_fin_pago_enganche >= start_date,
            Venta.fecha_fin_pago_enganche <= end_date
        ).scalar() or 0

        return {
            "ventas_totales": base_query.total_ventas or 0,
            "pipeline": base_query.pipeline or 0,
            "contratos_firmados": base_query.contratos_firmados or 0,
            "valor_contratos": base_query.valor_contratos or 0,
            "rendimiento_mes": round(growth, 1),# Debug info (opcional, para que veas qué comparó)
            "_debug_comparativa": f"Mes Actual ({current_month_sales}) vs {prev_start_str} al {prev_end_str} ({prev_month_sales})",
            "liquidados": liquidados_count,
            "proceso_escritura": base_query.proceso_escritura or 0,
            "agenda_escritura": base_query.agenda_escritura or 0,
            "cancelados": {
                "anio": cancelados_anio,
                "mes": cancelados_mes
            },
            "expirados": {
                "anio": expirados_anio,
                "mes": expirados_mes
            }
        }

    except Exception as e:
        logger.error(f"Error en KPIs: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# --- ENDPOINT 2: GRÁFICOS FINANCIEROS (Pagos e Ingresos) ---
@router.get("/Graficos/Financieros")
def get_financial_charts(
    start_date: str = Query(..., description="Fecha inicio YYYY-MM-DD"),
    end_date: str = Query(..., description="Fecha fin YYYY-MM-DD"),
    db: Session = Depends(get_db)
):
    try:
        # 1. CÁLCULO DE DATOS ACTUALES (DIARIOS)
        # Se elimina 'recaudado' y se utiliza 'monto_pagado' para 'abonado'
        daily_stats = db.query(
            Pago.fecha_comprobante,
            func.sum(case((and_(func.lower(Pago.estatus_flujo) == 'active', func.lower(Pago.estatus) == 'active'), Pago.monto_pagado), else_=0)).label("abonado"),
            func.sum(case((and_(func.lower(Pago.estatus_flujo).in_(['canceled', 'cancelado']), func.lower(Pago.estatus).in_(['canceled', 'cancelado'])), Pago.monto_flujo), else_=0)).label("cancelado"),
            func.sum(case((and_(func.lower(Pago.estatus_flujo) == 'active', func.lower(Pago.estatus) == 'active'), 1), else_=0)).label("conteo")
        ).filter(
            Pago.fecha_comprobante >= start_date,
            Pago.fecha_comprobante <= end_date,
            Pago.fecha_comprobante != None,
            func.lower(Pago.estatus_flujo).in_(['active', 'canceled', 'cancelado']),
            func.lower(Pago.metodo_pago) != 'nota de crédito'
        ).group_by(Pago.fecha_comprobante).order_by(Pago.fecha_comprobante).all()

        # 2. PROCESAMIENTO DE SERIES
        data_map = {r.fecha_comprobante: r for r in daily_stats}
        series_abonado, series_cancelado, series_conteo, categories = [], [], [], []
        
        total_actual_abonado = 0
        total_actual_conteo = 0

        s_date_obj = parse_date_param(start_date)
        e_date_obj = parse_date_param(end_date)
        delta = e_date_obj - s_date_obj
        
        for i in range(delta.days + 1):
            day = s_date_obj + timedelta(days=i)
            day_str = day.strftime("%Y-%m-%d")
            val = data_map.get(day_str)
            
            # Mapeo de valores diarios (Abonado es ahora la métrica principal)
            abo = float(val.abonado) if val and val.abonado else 0
            can = float(val.cancelado) if val and val.cancelado else 0
            con = int(val.conteo) if val and val.conteo else 0
            
            series_abonado.append(abo)
            series_cancelado.append(can)
            series_conteo.append(con)
            categories.append(day.strftime("%d/%m"))
            
            total_actual_abonado += abo
            total_actual_conteo += con

        # 3. LÓGICA DE COMPARATIVA YoY (AÑO ANTERIOR) BASADA EN ABONADO
        prev_start, prev_end = get_full_month_range_previous_year(s_date_obj)
        
        total_prev_abonado = db.query(
            func.sum(Pago.monto_pagado)
        ).filter(
            Pago.fecha_comprobante >= prev_start,
            Pago.fecha_comprobante <= prev_end,
            func.lower(Pago.estatus_flujo) == 'active',
            func.lower(Pago.estatus) == 'active'
        ).scalar() or 0

        # 4. CÁLCULO DE RENDIMIENTO (Abonado vs Abonado Año Pasado)
        total_prev_abonado = float(total_prev_abonado)
        growth = 0
        if total_prev_abonado > 0:
            growth = ((total_actual_abonado - total_prev_abonado) / total_prev_abonado) * 100
        elif total_actual_abonado > 0:
            growth = 100

        # 5. COMPOSICIÓN (DONAS) - Cambiado a monto_pagado por consistencia
        methods_query = db.query(Pago.metodo_pago, func.sum(Pago.monto_pagado)).filter(
            Pago.fecha_comprobante >= start_date, Pago.fecha_comprobante <= end_date,
            func.lower(Pago.estatus_flujo) == 'active', func.lower(Pago.estatus) == 'active'
        ).group_by(Pago.metodo_pago).all()
        
        concepts_query = db.query(Pago.concepto_pago, func.sum(Pago.monto_pagado).label("total")).filter(
            Pago.fecha_comprobante >= start_date, Pago.fecha_comprobante <= end_date,
            func.lower(Pago.estatus_flujo) == 'active', func.lower(Pago.estatus) == 'active'
        ).group_by(Pago.concepto_pago).all()
        
        # RESPUESTA FINAL
        return {
            "Total_abonado": total_actual_abonado,
            "Pagos_activos": total_actual_conteo,
            "Rendimiento_mes": round(growth, 1),
            "Comparativa_rendimiento": f"Total abonado mes actual: ({total_actual_abonado}) vs total abonado mes del año pasado: ({total_prev_abonado})",
            "Historico_de_abonos": {
                "Categorias": categories,
                "Abonado": series_abonado,
                "Cancelado": series_cancelado,
                "Pagos_realizados": series_conteo
            },
            "Composicion_de_ingresos": {
                "Metodos_de_pagos": [{"label": m[0] or "Sin definir", "value": float(m[1] or 0)} for m in methods_query],
                "Distribucion_por_conceptos": [{"label": c.concepto_pago or "Sin definir", "value": float(c.total or 0)} for c in concepts_query]
            }
        }
    except Exception as e:
        logger.error(f"Error en Financials: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- ENDPOINT 3: CLUSTERS (Etapas de Venta) ---
@router.get("/Graficos/Clusters")
def get_clusters_chart(
    start_date: str = Query(..., description="Fecha inicio YYYY-MM-DD"),
    end_date: str = Query(..., description="Fecha fin YYYY-MM-DD"),
    db: Session = Depends(get_db)
):
    """
    Retorna la evolución de ventas agrupadas por Etapa.
    Optimizado para Stacked Bar Chart.
    """
    try:
        # Consulta: Agrupar por Fecha y Etapa
        query = db.query(
            Venta.fecha_inicio_operacion,
            Venta.etapa,
            func.count(Venta.folio).label("cantidad")
        ).filter(
            Venta.fecha_inicio_operacion >= start_date,
            Venta.fecha_inicio_operacion <= end_date
        ).group_by(
            Venta.fecha_inicio_operacion, 
            Venta.etapa
        ).all()

        # Procesamiento para ApexCharts (Pivot)
        # Necesitamos: Series por etapa, con datos alineados a las fechas
        
        # 1. Obtener todas las etapas únicas y fechas únicas en el rango
        etapas_set = set()
        data_map = {} # Clave: fecha, Valor: {etapa: cantidad}
        
        for row in query:
            fecha = row.fecha_inicio_operacion
            etapa = row.etapa or "Sin Etapa"
            cant = row.cantidad
            
            etapas_set.add(etapa)
            if fecha not in data_map: data_map[fecha] = {}
            data_map[fecha][etapa] = cant

        # 2. Generar eje X continuo
        s_date = parse_date_param(start_date)
        e_date = parse_date_param(end_date)
        delta = e_date - s_date
        
        categories = []
        series_dict = {etapa: [] for etapa in etapas_set}
        
        total_clusters = 0

        for i in range(delta.days + 1):
            day = s_date + timedelta(days=i)
            day_str = day.strftime("%Y-%m-%d")
            categories.append(day.strftime("%d/%m"))
            
            day_data = data_map.get(day_str, {})
            
            for etapa in etapas_set:
                val = day_data.get(etapa, 0)
                series_dict[etapa].append(val)
                total_clusters += val

        # 3. Formato final para ApexCharts
        final_series = [{"name": etapa, "data": data} for etapa, data in series_dict.items()]

        # Totales por Etapa (Para gráfico de Pie)
        totals_by_stage = []
        for etapa in etapas_set:
            total_etapa = sum(series_dict[etapa])
            totals_by_stage.append({"label": etapa, "value": total_etapa})

        return {
            "evolution": {
                "categories": categories,
                "series": final_series
            },
            "distribution": totals_by_stage,
            "total_general": total_clusters
        }

    except Exception as e:
        logger.error(f"Error en Clusters: {e}")
        raise HTTPException(status_code=500, detail=str(e))