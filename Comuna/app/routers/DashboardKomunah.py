import logging
from fastapi import APIRouter, Depends, Query, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func, and_, case, distinct, cast, Date
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
    proyecto: Optional[str] = Query(None, description="Filtrar por nombre del proyecto"),
    db: Session = Depends(get_db)
):
    """
    Calcula las tarjetas de métricas principales incluyendo Notas de Crédito.
    """
    try:
        # 1. KPIs Generales de Ventas (Métrica de Ventas del SQL)
        query_ventas = db.query(
            func.count(Venta.folio).label("total_ventas"),
            func.sum(case((func.lower(Venta.estado_expediente) == 'ventas', 1), else_=0)).label("pipeline"),
            func.sum(case((func.lower(Venta.estado_expediente).in_([s.lower() for s in CONTRACT_STATUS_WHITELIST]), 1), else_=0)).label("contratos_firmados"),
            func.sum(case((func.lower(Venta.estado_expediente).in_([s.lower() for s in CONTRACT_STATUS_WHITELIST]), Venta.precio_final), else_=0)).label("valor_contratos"),
            func.sum(case((func.lower(Venta.estado_expediente).in_(['proceso de escritura', 'proceso escritura']), 1), else_=0)).label("proceso_escritura"),
            func.sum(case((func.lower(Venta.estado_expediente) == 'agenda escritura', 1), else_=0)).label("agenda_escritura")
        ).filter(
            Venta.fecha_inicio_operacion >= start_date,
            Venta.fecha_inicio_operacion <= end_date
        )

        if proyecto and proyecto.lower() != "todos":
            query_ventas = query_ventas.filter(Venta.desarrollo == proyecto)
        
        base_query = query_ventas.first()

        # 2. Métricas de Pagos (Notas de Crédito del SQL)
        query_pagos = db.query(
            func.count(distinct(Pago.folio_venta)).label("notas_de_credito"),
            func.sum(Pago.monto_flujo).label("total_notas_de_credito")
        ).join(Venta, Pago.folio_venta == Venta.folio).filter(
            Pago.fecha_comprobante >= start_date,
            Pago.fecha_comprobante <= end_date,
            func.lower(Pago.metodo_pago) == 'nota de crédito',
            Pago.estatus_flujo == 'active'
        )

        if proyecto and proyecto.lower() != "todos":
            query_pagos = query_pagos.filter(Venta.desarrollo == proyecto)
            
        pagos_metrics = query_pagos.first()

        # 3. Cancelados y Expirados (Anual y Mes)
        year_start = start_date[:4] + "-01-01"
        year_end = start_date[:4] + "-12-31"
        
        def get_status_count(status, start, end):
            q = db.query(func.count(Venta.folio)).filter(
                func.lower(Venta.estado_expediente) == status,
                Venta.fecha_inicio_operacion >= start,
                Venta.fecha_inicio_operacion <= end
            )
            if proyecto and proyecto.lower() != "todos": q = q.filter(Venta.desarrollo == proyecto)
            return q.scalar() or 0

        cancelados_anio = get_status_count('cancelado', year_start, year_end)
        cancelados_mes = get_status_count('cancelado', start_date, end_date)
        expirados_anio = get_status_count('expirado', year_start, year_end)
        expirados_mes = get_status_count('expirado', start_date, end_date)

        # 4. Cálculo de Crecimiento YoY
        s_date_obj = parse_date_param(start_date)
        prev_start_str, prev_end_str = get_full_month_range_previous_year(s_date_obj)
        
        prev_month_sales_query = db.query(func.count(Venta.folio)).filter(
            Venta.fecha_inicio_operacion >= prev_start_str,
            Venta.fecha_inicio_operacion <= prev_end_str
        )
        if proyecto and proyecto.lower() != "todos": prev_month_sales_query = prev_month_sales_query.filter(Venta.desarrollo == proyecto)
        prev_month_sales = prev_month_sales_query.scalar() or 0

        current_month_sales = base_query.total_ventas or 0
        growth = ((current_month_sales - prev_month_sales) / prev_month_sales * 100) if prev_month_sales > 0 else (100 if current_month_sales > 0 else 0)

        # 5. Liquidados
        liquidados_query = db.query(func.count(Venta.folio)).filter(
            func.lower(Venta.estado_expediente) == 'liquidado',
            Venta.fecha_fin_pago_enganche >= start_date,
            Venta.fecha_fin_pago_enganche <= end_date
        )
        if proyecto and proyecto.lower() != "todos": liquidados_query = liquidados_query.filter(Venta.desarrollo == proyecto)
        liquidados_count = liquidados_query.scalar() or 0

        return {
            "ventas_totales": base_query.total_ventas or 0,
            "pipeline": base_query.pipeline or 0,
            "contratos_firmados": base_query.contratos_firmados or 0,
            "valor_contratos": base_query.valor_contratos or 0,
            "rendimiento_mes": round(growth, 1),
            "liquidados": liquidados_count,
            "proceso_escritura": base_query.proceso_escritura or 0,
            "agenda_escritura": base_query.agenda_escritura or 0,
            "notas_de_credito": pagos_metrics.notas_de_credito or 0,
            "total_notas_de_credito": round(pagos_metrics.total_notas_de_credito or 0, 2),
            "cancelados": {"anio": cancelados_anio, "mes": cancelados_mes},
            "expirados": {"anio": expirados_anio, "mes": expirados_mes},
            "_debug_comparativa": f"Mes Actual ({current_month_sales}) vs {prev_start_str} al {prev_end_str} ({prev_month_sales})"
        }

    except Exception as e:
        logger.error(f"Error en KPIs: {e}")
        raise HTTPException(status_code=500, detail=str(e))

# --- ENDPOINT 2: GRÁFICOS FINANCIEROS (Pagos e Ingresos) ---
@router.get("/Graficos/Financieros")
def get_financial_charts(
    start_date: str = Query(..., description="Fecha inicio YYYY-MM-DD"),
    end_date: str = Query(..., description="Fecha fin YYYY-MM-DD"),
    proyecto: Optional[str] = Query(None, description="Filtrar por nombre del proyecto"),
    banco: Optional[str] = Query(None, description="Filtrar por nombre del banco"), 
    db: Session = Depends(get_db)
):
    try:
        # Definir el valor de búsqueda para la DB (mapeo de "No aplica")
        db_banco = "No aplica" if banco == "Banco Mercantil del Norte, S.A." else banco

        # 1. CÁLCULO DE DATOS ACTUALES
        query_stats = db.query(
            Pago.fecha_comprobante,
            func.sum(case((and_(func.lower(Pago.estatus_flujo) == 'active', func.lower(Pago.estatus) == 'active'), Pago.monto_pagado), else_=0)).label("abonado"),
            func.sum(case((and_(func.lower(Pago.estatus_flujo).in_(['canceled', 'cancelado']), func.lower(Pago.estatus).in_(['canceled', 'cancelado'])), Pago.monto_flujo), else_=0)).label("cancelado"),
            func.sum(case((and_(func.lower(Pago.estatus_flujo) == 'active', func.lower(Pago.estatus) == 'active'), 1), else_=0)).label("conteo")
        ).filter(
            Pago.fecha_comprobante >= start_date, Pago.fecha_comprobante <= end_date,
            Pago.fecha_comprobante != None, func.lower(Pago.estatus_flujo).in_(['active', 'canceled', 'cancelado']),
            func.lower(Pago.metodo_pago) != 'nota de crédito'
        )
        if proyecto and proyecto.lower() != "todos": query_stats = query_stats.filter(Pago.proyecto == proyecto)
        if banco and banco.lower() != "todos": query_stats = query_stats.filter(Pago.banco_caja == db_banco)
        
        daily_stats = query_stats.group_by(Pago.fecha_comprobante).order_by(Pago.fecha_comprobante).all()

        # Procesamiento de series (Omitido por brevedad, se mantiene igual)
        data_map = {r.fecha_comprobante: r for r in daily_stats}
        series_abonado, series_cancelado, series_conteo, categories = [], [], [], []
        total_actual_abonado = 0
        total_actual_conteo = 0
        s_date_obj, e_date_obj = parse_date_param(start_date), parse_date_param(end_date)
        delta = e_date_obj - s_date_obj
        for i in range(delta.days + 1):
            day = s_date_obj + timedelta(days=i)
            val = data_map.get(day.strftime("%Y-%m-%d"))
            abo = float(val.abonado) if val and val.abonado else 0
            can = float(val.cancelado) if val and val.cancelado else 0
            con = int(val.conteo) if val and val.conteo else 0
            series_abonado.append(abo); series_cancelado.append(can); series_conteo.append(con)
            categories.append(day.strftime("%d/%m"))
            total_actual_abonado += abo; total_actual_conteo += con

        # 3. LÓGICA YoY
        prev_start, prev_end = get_full_month_range_previous_year(s_date_obj)
        query_prev = db.query(func.sum(Pago.monto_pagado)).filter(
            Pago.fecha_comprobante >= prev_start, Pago.fecha_comprobante <= prev_end,
            func.lower(Pago.estatus_flujo) == 'active', func.lower(Pago.estatus) == 'active'
        )
        if proyecto and proyecto.lower() != "todos": query_prev = query_prev.filter(Pago.proyecto == proyecto)
        if banco and banco.lower() != "todos": query_prev = query_prev.filter(Pago.banco_caja == db_banco)
        
        total_prev_abonado = float(query_prev.scalar() or 0)
        growth = ((total_actual_abonado - total_prev_abonado) / total_prev_abonado * 100) if total_prev_abonado > 0 else (100 if total_actual_abonado > 0 else 0)

        # 5. COMPOSICIÓN (DONAS)
        def get_composition(field):
            q = db.query(field, func.sum(Pago.monto_pagado)).filter(
                Pago.fecha_comprobante >= start_date, Pago.fecha_comprobante <= end_date,
                func.lower(Pago.estatus_flujo) == 'active', func.lower(Pago.estatus) == 'active'
            )
            if proyecto and proyecto.lower() != "todos":q = q.filter(Pago.proyecto == proyecto)
            if banco and banco.lower() != "todos": q = q.filter(Pago.banco_caja == db_banco)
            return q.group_by(field).all()

        methods_query = get_composition(Pago.metodo_pago)
        concepts_query = get_composition(Pago.concepto_pago)
        
        return {
            "Total_abonado": total_actual_abonado, "Pagos_activos": total_actual_conteo, "Rendimiento_mes": round(growth, 1), 
            "Comparativa_rendimiento": f"Total abonado mes actual: ({total_actual_abonado}) vs total abonado mes del año pasado: ({total_prev_abonado})",
            "Historico_de_abonos": {"Categorias": categories, "Abonado": series_abonado, "Cancelado": series_cancelado, "Pagos_realizados": series_conteo},
            "Composicion_de_ingresos": {
                "Metodos_de_pagos": [{"label": m[0] or "Sin definir", "value": float(m[1] or 0)} for m in methods_query],
                "Distribucion_por_conceptos": [{"label": c[0] or "Sin definir", "value": float(c[1] or 0)} for c in concepts_query]
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
    proyecto: Optional[str] = Query(None, description="Filtrar por nombre del proyecto"),
    db: Session = Depends(get_db)
):
    """
    Retorna la evolución de ventas agrupadas por Etapa.
    Optimizado para Stacked Bar Chart.
    """
    try:
        # 1. Construir la base de la consulta (SIN .all() todavía)
        query_obj = db.query(
            Venta.fecha_inicio_operacion,
            Venta.etapa,
            func.count(Venta.folio).label("cantidad")
        ).filter(
            Venta.fecha_inicio_operacion >= start_date,
            Venta.fecha_inicio_operacion <= end_date
        )
        
        # 2. Aplicar filtro de proyecto si existe
        # Nota: Asegúrate de usar Venta.proyecto o Venta.desarrollo según tu modelo
        if proyecto and proyecto.lower() != "todos": 
            query_obj = query_obj.filter(Venta.desarrollo == proyecto)
        
        # 3. Agrupar y EJECUTAR con .all()
        results = query_obj.group_by(
            Venta.fecha_inicio_operacion, 
            Venta.etapa
        ).all()
        
        # --- El resto de tu lógica de procesamiento se mantiene igual ---
        etapas_set = set()
        data_map = {} 
        
        for row in results: # 'results' ahora es la lista
            fecha = row.fecha_inicio_operacion
            etapa = row.etapa or "Sin Etapa"
            cant = row.cantidad
            
            etapas_set.add(etapa)
            if fecha not in data_map: data_map[fecha] = {}
            data_map[fecha][etapa] = cant

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

        final_series = [{"name": etapa, "data": data} for etapa, data in series_dict.items()]

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