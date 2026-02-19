from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import List, Optional
from ..database import get_db
from .. import schemas
from datetime import datetime
from typing import Any, Dict
from ..services.security import get_current_user, es_admin, es_usuario

router = APIRouter(prefix="/reportes", tags=["Reportes Financieros"])
@router.get("/pagos-historico", response_model=List[schemas.ConciliacionClienteResponse])
def get_conciliacion_clientes(anio: Optional[int] = None, folio: Optional[str] = None, db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    
    if anio:
        folio = None 
    elif not folio:
        raise HTTPException(status_code=400, detail="¿Te falla o qué? Debes proporcionar el Año o el Folio.")
   
    col = "p.`Fecha del comprobante de pago`"
    bus_anio = f"%{anio}%" if anio else None
    query = text(f"""
        SELECT 
            v.FOLIO as FOLIO,
            v.`DESARROLLO` as PROYECTO,
            NULL as `FECHA PROMESA`,
            p.`Cliente` as `NOMBRE CLIENTE`,
            v.`NÚMERO` as LOTE,
            v.`ETAPA` as CLUSTER,
            v.`METROS CUADRADOS` as M2,
            v.`PRECIO DE LISTA` as `PRECIO LISTA`,
            YEAR(p.`Fecha del comprobante de pago`) as AÑO,
            SUM(CASE WHEN ({col} LIKE '%-01-%' OR {col} LIKE '%/01/%') THEN p.`Monto pagado` ELSE 0 END) as enero,
            SUM(CASE WHEN ({col} LIKE '%-02-%' OR {col} LIKE '%/02/%') THEN p.`Monto pagado` ELSE 0 END) as febrero,
            SUM(CASE WHEN ({col} LIKE '%-03-%' OR {col} LIKE '%/03/%') THEN p.`Monto pagado` ELSE 0 END) as marzo,
            SUM(CASE WHEN ({col} LIKE '%-04-%' OR {col} LIKE '%/04/%') THEN p.`Monto pagado` ELSE 0 END) as abril,
            SUM(CASE WHEN ({col} LIKE '%-05-%' OR {col} LIKE '%/05/%') THEN p.`Monto pagado` ELSE 0 END) as mayo,
            SUM(CASE WHEN ({col} LIKE '%-06-%' OR {col} LIKE '%/06/%') THEN p.`Monto pagado` ELSE 0 END) as junio,
            SUM(CASE WHEN ({col} LIKE '%-07-%' OR {col} LIKE '%/07/%') THEN p.`Monto pagado` ELSE 0 END) as julio,
            SUM(CASE WHEN ({col} LIKE '%-08-%' OR {col} LIKE '%/08/%') THEN p.`Monto pagado` ELSE 0 END) as agosto,
            SUM(CASE WHEN ({col} LIKE '%-09-%' OR {col} LIKE '%/09/%') THEN p.`Monto pagado` ELSE 0 END) as septiembre,
            SUM(CASE WHEN ({col} LIKE '%-10-%' OR {col} LIKE '%/10/%') THEN p.`Monto pagado` ELSE 0 END) as octubre,
            SUM(CASE WHEN ({col} LIKE '%-11-%' OR {col} LIKE '%/11/%') THEN p.`Monto pagado` ELSE 0 END) as noviembre,
            SUM(CASE WHEN ({col} LIKE '%-12-%' OR {col} LIKE '%/12/%') THEN p.`Monto pagado` ELSE 0 END) as diciembre,
            SUM(p.`Monto pagado`) as TOTAL_ANIO
        FROM ventas v
        INNER JOIN pagos p ON v.FOLIO = p.`Folio de la venta`
        WHERE p.`Estatus` = 'active'
          AND p.`Método de pago` != 'Nota de Crédito'
          AND p.`Cliente` IS NOT NULL
          AND p.`Cliente` != ''
          AND {col} IS NOT NULL 
          AND {col} != ''
          AND (:anio_val IS NULL OR {col} LIKE :bus_anio)
          AND (:folio_val IS NULL OR v.FOLIO = :folio_val)
        GROUP BY v.FOLIO, p.`Cliente`, AÑO
        ORDER BY p.`Cliente` ASC
    """)

    try:
        result = db.execute(query, {"anio_val": anio, "bus_anio": bus_anio, "folio_val": folio})
        return result.mappings().all()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")

@router.get("/pagos-historico-anual", response_model=List[Dict[str, Any]])
def get_conciliacion_anual(db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    anio_inicio = 2021
    anio_actual = datetime.now().year 
    anios = list(range(anio_inicio, anio_actual + 1))
    
    col_fecha = "p.`Fecha del comprobante de pago`"
    
    sql_anios = ""
    for anio in anios:
        sql_anios += f"""
            SUM(CASE WHEN {col_fecha} LIKE '%{anio}%' THEN p.`Monto pagado` ELSE 0 END) as `{anio}`,"""

    query = text(f"""
        SELECT 
            v.FOLIO as FOLIO,
            v.`DESARROLLO` as PROYECTO,
            NULL as `FECHA PROMESA`,
            p.`Cliente` as `NOMBRE CLIENTE`,
            v.`NÚMERO` as LOTE,
            v.`ETAPA` as CLUSTER,
            v.`METROS CUADRADOS` as M2,
            v.`PRECIO DE LISTA` as `PRECIO_LISTA`,
            {sql_anios}
            SUM(p.`Monto pagado`) as TOTAL_HISTORICO
        FROM ventas v
        INNER JOIN pagos p ON v.FOLIO = p.`Folio de la venta`
        WHERE p.`Estatus` = 'active'
          AND p.`Método de pago` != 'Nota de Crédito'
          AND p.`Cliente` IS NOT NULL
          AND p.`Cliente` != ''
          AND {col_fecha} IS NOT NULL 
          AND {col_fecha} != ''
        GROUP BY v.FOLIO, p.`Cliente`
        ORDER BY p.`Cliente` ASC
    """)

    try:
        result = db.execute(query).mappings().all()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en reporte anual: {str(e)}")
    
@router.get("/contabilidad", response_model=List[schemas.ComplementoPago])
def get_complementos_pago(anio: Optional[int] = None, folio: Optional[str] = None, db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    try:
        if anio is None and folio is None:
            raise HTTPException(status_code=400, detail="Debe proporcionar el Año o el Folio.")
        
        if anio:
            folio = None
      
        col_fecha = "p.`Fecha del comprobante de pago`"
        
        query = text(f"""
            SELECT 
                v.CLIENTE, 
                p.Cliente as PAGADOR,
                v.FOLIO as FOLIO_VENTA, 
                v.`NÚMERO` as LOTE,

                p.`Proyecto` as proyecto_pago,
                p.`Número de pago` as num_pago,
                p.`Banco Caja` as banco,
                p.`Estatus` as estatus_pago,
                p.`Fecha de aplicación de pago registro en sistema` as fecha_aplicacion,

                (SELECT COUNT(*) FROM ventas v2 WHERE v2.CLIENTE = v.CLIENTE) as total_lotes,
                COALESCE(p.`Fecha del comprobante de pago`, '') as fecha_pago_real,
                COALESCE(p.`Folio de pago`, '') as folio_pago_real,
                COALESCE(p.`Método de pago`, '') as metodo_real,
                COALESCE(p.`Concepto de pago`, '') as concepto_real,
                COALESCE(p.`Monto pagado`, 0) as monto_individual,
                COALESCE(p.`ID Pago`, '') as id_pago,
                COALESCE(p.`ID Flujo`, '') as id_flujo,
                COALESCE(p.`Estatus flujo`, '') as estatus_flujo,
                COALESCE(p.`Monto flujo`, 0) as monto_flujo
            FROM pagos p
            -- INNER JOIN directo: Solo trae ventas que tengan pagos en el filtro
            INNER JOIN ventas v ON p.`Folio de la venta` = v.FOLIO
            WHERE ({col_fecha} IS NULL 
                    OR {col_fecha} = '' 
                    OR {col_fecha} = 'NULL'
                    OR (:anio_val IS NULL OR {col_fecha} LIKE :bus_anio))
              AND (:folio_val IS NULL OR v.FOLIO = :folio_val) 
            ORDER BY v.CLIENTE ASC, p.`Fecha del comprobante de pago` ASC
        """)
        
        
        rows = db.execute(query, {
            "anio_val": anio, 
            "bus_anio": f"%{anio}%" if anio else None, 
            "folio_val": folio
        }).mappings().all()
        
        return [{
            "cliente": r["CLIENTE"],
            "pagador": r["PAGADOR"],
            "fecha_pago": r["fecha_pago_real"],
            "folio_venta": r["FOLIO_VENTA"],
            "folio_pago": r["folio_pago_real"], 
            "metodo": r["metodo_real"],
            "concepto": r["concepto_real"],
            "lote": r["LOTE"],
            "varios": True if r["total_lotes"] > 1 else False,
            "total": r["total_lotes"],
            "abono": float(r["monto_individual"]) * -1 if r["metodo_real"] == "Nota de Crédito" else float(r["monto_individual"]),
            "saldo": None,
            "anio": int(str(r["fecha_pago_real"])[:4]) if r["fecha_pago_real"] else 0,
            "id_pago": r["id_pago"],
            "id_flujo": r["id_flujo"],
            "estatus_flujo": r["estatus_flujo"],
            "monto_flujo": r["monto_flujo"],

            "proyecto": r["proyecto_pago"],
            "num_pago": r["num_pago"],
            "banco": r["banco"],
            "estatus_pago": r["estatus_pago"],
            "fecha_aplicacion": r["fecha_aplicacion"]
        } for r in rows]

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en Contabilidad: {str(e)}")

@router.get("/antiguedad-completo",response_model=schemas.ReporteAntiguedadCompleto)
def get_reporte_detallado(anio: Optional[int] = None, db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    busqueda = f"%{anio}%" if anio else None
    query = text("""
        SELECT 
            a.`FOLIO` as FOLIO, 
            a.`CLIENTE` as CLIENTE,
            a.`PROYECTO` as PROYECTO,
            a.`FASE` as FASE,
            a.`ETAPA` as ETAPA, 
            a.`UNIDAD` as UNIDAD, 
            a.`CORREO ELECTRÓNICO` as `CORREO ELECTRÓNICO`, 
            a.`TELÉFONO` as TELÉFONO, 
            a.`FECHA DE PAGO` as `FECHA DE PAGO`, 
            a.`SALDO VIGENTE` as `SALDO VIGENTE`, 
            a.`01 A 30 DÍAS` as `01 A 30 DÍAS`, 
            a.`31 A 60 DÍAS` as `31 A 60 DÍAS`, 
            a.`61 A 90 DÍAS` as `61 A 90 DÍAS`, 
            a.`91 A 120 DÍAS` as `91 A 120 DÍAS`, 
            a.`MÁS DE 120 DÍAS` as `MÁS DE 120 DÍAS`, 
            a.`MENSUALIDADES VENCIDAS` as `MENSUALIDADES VENCIDAS`, 
            a.`TOTAL VENCIDO` as `TOTAL VENCIDO`, 
            a.`CARTERA TOTAL` as `CARTERA TOTAL`,
            a.`TOTAL PAGADO` as `TOTAL PAGADO`,
            v.`ESTADO DEL EXPEDIENTE` as `ESTATUS PIPELINE`,
            cv.`NÚMERO DE PARCIALIDADES VENCIDAS TOTALES` as `PARCIALIDADES_VENCIDAS_TOTALES`
            
        FROM antig_saldos a
        LEFT JOIN ventas v ON a.`FOLIO` = v.`FOLIO`
        LEFT JOIN (
            SELECT FOLIO, `NÚMERO DE PARCIALIDADES VENCIDAS TOTALES`
            FROM (
                SELECT 
                    FOLIO, 
                    `NÚMERO DE PARCIALIDADES VENCIDAS TOTALES`,
                    ROW_NUMBER() OVER (
                        PARTITION BY FOLIO 
                        -- Ordenamos por cercanía a hoy (usando tus columnas Text)
                        ORDER BY ABS(DATEDIFF(STR_TO_DATE(`FECHA DE PAGO`, '%d/%m/%Y'), CURDATE())) ASC
                    ) as rank_fecha
                FROM cartera_vencida
            ) t WHERE rank_fecha = 1
        ) cv ON a.`FOLIO` = cv.`FOLIO`
        WHERE a.`FECHA DE PAGO` IS NOT NULL 
          AND a.`FECHA DE PAGO` != ''
          AND (:anio_val IS NULL OR a.`FECHA DE PAGO` LIKE :busqueda_anio)
    """)

    try:
        result = db.execute(query, {"anio_val": anio, "busqueda_anio": busqueda}).mappings().all()
        
    
        t_vigente = 0.0
        t_01_30 = 0.0
        t_31_60 = 0.0
        t_61_90 = 0.0
        t_91_120 = 0.0
        t_mas_120 = 0.0
        t_mensualidades = 0
        t_vencido = 0.0
        t_cartera = 0.0

        detalles = []
        for row in result:
            anio_real = int(str(row["FECHA DE PAGO"])[:4])
            fila_con_anio = {**dict(row), "anio": anio_real}
            t_vigente += float(row.get('SALDO VIGENTE') or 0)
            t_01_30 += float(row.get('01 A 30 DÍAS') or 0)
            t_31_60 += float(row.get('31 A 60 DÍAS') or 0)
            t_61_90 += float(row.get('61 A 90 DÍAS') or 0)
            t_91_120 += float(row.get('91 A 120 DÍAS') or 0)
            t_mas_120 += float(row.get('MÁS DE 120 DÍAS') or 0)
            t_vencido += float(row.get('TOTAL VENCIDO') or 0)
            t_cartera += float(row.get('CARTERA TOTAL') or 0)
            t_mensualidades += int(row.get('MENSUALIDADES VENCIDAS') or 0)
            detalles.append(schemas.AntigSaldosResponse(**fila_con_anio))

        if t_vencido == 0:
            denominador = 1
        else:
            denominador = t_vencido

        pct_01_30 = (t_01_30 / denominador) * 100
        pct_31_60 = (t_31_60 / denominador) * 100
        pct_61_90 = (t_61_90 / denominador) * 100
        pct_91_120 = (t_91_120 / denominador) * 100
        pct_mas_120 = (t_mas_120 / denominador) * 100
        
        analisis_01_60 = ((t_01_30 + t_31_60) / denominador) * 100
        analisis_61_120 = ((t_61_90 + t_91_120) / denominador) * 100
        analisis_mas_120 = (t_mas_120 / denominador) * 100

        riesgo_total = ((t_31_60 + t_61_90 + t_91_120 + t_mas_120) / denominador) * 100

        return {
            "detalles": detalles,
            "total_vigente": t_vigente,
            "total_01_30": t_01_30,
            "total_31_60": t_31_60,
            "total_61_90": t_61_90,
            "total_91_120": t_91_120,
            "total_mas_120": t_mas_120,
            "total_vencido_global": t_vencido,
            "cartera_total_global": t_cartera,
            "mensualidad":t_mensualidades,
            "pct_01_30": round(pct_01_30, 2),
            "pct_31_60": round(pct_31_60, 2),
            "pct_61_90": round(pct_61_90, 2),
            "pct_91_120": round(pct_91_120, 2),
            "pct_mas_120": round(pct_mas_120, 2),
         
            "analisis_01_60": round(analisis_01_60, 2),
            "analisis_61_120": round(analisis_61_120, 2),
            "analisis_mas_120": round(analisis_mas_120, 2),
            "riesgo_total": round(riesgo_total, 2),
            "anio": anio
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando reporte: {str(e)}")
    
@router.get("/pagos-fecha-nula", response_model=List[schemas.PagoResponse])
def get_pagos_sin_fecha(db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    query = text("""
        SELECT * FROM pagos
        WHERE `Fecha del comprobante de pago` IS NULL 
           OR `Fecha del comprobante de pago` = ''
           OR `Fecha del comprobante de pago` = 'NULL'
        ORDER BY Cliente ASC
    """)

    try:
        result = db.execute(query).mappings().all()
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al obtener pagos nulos: {str(e)}")

@router.get("/reporte-expedientes-liquidados")
def get_reporte_expedientes_liquidados(db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    anio_inicio = 2021
    anio_actual = datetime.now().year 
    anios = list(range(anio_inicio, anio_actual + 1))
    
    col_fecha = "p.`Fecha del comprobante de pago`"
    
  
    sql_anios_pagos = ""
    sql_anios_anticipos = ""
    for anio in anios:
        sql_anios_pagos += f"SUM(CASE WHEN {col_fecha} LIKE '%{anio}%' AND p.`Método de pago` != 'Nota de Crédito' THEN p.`Monto pagado` ELSE 0 END) as `PAGOS {anio}`,"
        sql_anios_anticipos += f"SUM(CASE WHEN {col_fecha} LIKE '%{anio}%' AND p.`Método de pago` != 'Nota de Crédito' THEN p.`Monto pagado` ELSE 0 END) as `Anticipo {anio}`,"
        sql_anios_pagos += f"SUM(CASE WHEN {col_fecha} LIKE '%{anio}%' AND p.`Método de pago` = 'Nota de Crédito' THEN p.`Monto pagado` ELSE 0 END) as `NC {anio}`,"
    query = text(f"""
        SELECT 
            v.FOLIO as FOLIO,
            v.`FECHA DE INICIO DE OPERACIÓN` as `INICIO_OPERACIONES`,
            v.`DESARROLLO` as PROYECTO,
            v.`ETAPA` as CLUSTER,
            v.`ESTADO DEL EXPEDIENTE` as ESTATUS,
            v.`NÚMERO` as LOTE,
            v.`METROS CUADRADOS` as M2,
            v.`CLIENTE` as CLIENTE,
            NULL as `FECHA FIRMA`,
            v.`FECHA DE FINALIZACIÓN DE ENGANCHE` as `FECHA DE FINALIZACIÓN DE ENGANCHE`,
            v.`FECHA DE FINALIZACIÓN DE PAGO DE ENGANCHE` as `FECHA DE FINALIZACIÓN DE PAGO DE ENGANCHE`,
            v.`PRECIO FINAL` as `PRECIO FINAL`,
            {sql_anios_pagos}
            SUM(CASE WHEN p.`Método de pago` != 'Nota de Crédito' THEN p.`Monto pagado` ELSE 0 END) as `TOTAL PAGADO`,
            (v.`PRECIO FINAL` - SUM(CASE WHEN p.`Método de pago` != 'Nota de Crédito' THEN p.`Monto pagado` ELSE 0 END)) as VALIDACION,
            NULL as `COMENTARIO`,
            {sql_anios_anticipos}
            SUM(CASE WHEN p.`Método de pago` = 'Nota de Crédito' THEN 1 ELSE 0 END) as `CANTIDAD_TOTAL_NC`,
            MAX(CASE WHEN p.`Método de pago` = 'Nota de Crédito' THEN 'SÍ' ELSE 'NO' END) as `ALERTA NOTA CRÉDITO`,
            NULL as `% (anticipo)`,
            NULL as `% aplicable (anticipo)`,
            NULL as `Pago total 2023`,
            NULL as `% (pago 2023)`,
            NULL as `% aplicable (pago 2023)`,
            NULL as `Pago total 2024`,
            NULL as `% (pago 2024)`,
            NULL as `% aplicable (pago 2024)`,
            NULL as `Saldo 2023`
        FROM ventas v
        INNER JOIN pagos p ON v.FOLIO = p.`Folio de la venta`
        WHERE p.`Estatus` = 'active'
          AND v.`ESTADO DEL EXPEDIENTE` IN (
              'liquidado', 'proceso de escritura', 
              'agenda escritura', 'escriturado'
          )
          AND {col_fecha} IS NOT NULL 
          AND {col_fecha} != ''
        GROUP BY v.FOLIO, v.CLIENTE
        ORDER BY v.CLIENTE ASC
    """)

    try:
        result = db.execute(query).mappings().all()
        
        campos_fecha = [
            "FECHA DE FINALIZACIÓN DE ENGANCHE", 
            "FECHA DE FINALIZACIÓN DE PAGO DE ENGANCHE",
            "INICIO_OPERACIONES",
            "FECHA FIRMA"
        ]

        final_data = []
        for row in result:
            d = dict(row)
            for k, v in d.items():
                if k in campos_fecha:
                    if v is None or str(v).strip().upper() in ['NULL', '']:
                        d[k] = None
                    else:
                        d[k] = str(v)
                
                elif any(x in k.upper() for x in ['M2', 'PRECIO', 'PAGOS ', 'ANTICIPO ', 'NC ', 'TOTAL', 'VALIDACION']):
                    try:
                        d[k] = float(v) if v not in [None, ''] else 0.0
                    except:
                        d[k] = 0.0
            final_data.append(d)
            
        return final_data
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en reporte de expedientes liquidados: {str(e)}")