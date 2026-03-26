from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import text, and_, func
from typing import List, Optional
from collections import defaultdict
import re
from ..database import get_db
from .. import schemas
from datetime import datetime
from typing import Any, Dict 
from ..services.security import get_current_user, es_admin, es_usuario
from ..models import Venta, Pago, Amortizacion

router = APIRouter(prefix="/reportes", tags=["Reportes Financieros"])

def extraer_numeros_finales(valor: Optional[str]) -> str:
    texto = str(valor or "").strip()
    match = re.search(r"(\d+)\s*$", texto)
    return match.group(1) if match else ""

def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    texto = str(value).strip().replace("$", "").replace(",", "")
    if texto in ["", "None", "NULL", "nan", "NaN"]:
        return 0.0
    try:
        return float(texto)
    except Exception:
        return 0.0
    
def _traducir_concepto_amortizacion(concepto: Any) -> str:
    traducciones = {
        "financing": "Parcialidad",
        "down_payment": "Enganche",
        "initial_payment": "Apartado",
        "last_payment": "Último pago"
    }
    c = str(concepto or "").strip().lower()
    # Buscamos en el dict, si no está, devolvemos el original
    return traducciones.get(c, c).strip().lower()

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
            "anio": int(str(r["fecha_pago_real"])[:4]) if r["fecha_pago_real"] and str(r["fecha_pago_real"])[:4].isdigit() else (int(str(r["fecha_pago_real"])[-4:]) if r["fecha_pago_real"] and str(r["fecha_pago_real"])[-4:].isdigit() else 0),
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
            v.`ASESOR` as ASESOR,
            v.`ESTADO` as ESTADO,
            v.`PAÍS` as PAÍS,
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
            anio_real = int(str(row["FECHA DE PAGO"])[:4]) if row["FECHA DE PAGO"] and str(row["FECHA DE PAGO"])[:4].isdigit() else (int(str(row["FECHA DE PAGO"])[-4:]) if row["FECHA DE PAGO"] and str(row["FECHA DE PAGO"])[-4:].isdigit() else 0)
            v01 = abs(float(row.get('01 A 30 DÍAS') or 0))
            v31 = abs(float(row.get('31 A 60 DÍAS') or 0))
            v61 = abs(float(row.get('61 A 90 DÍAS') or 0))
            v91 = abs(float(row.get('91 A 120 DÍAS') or 0))
            v120 = abs(float(row.get('MÁS DE 120 DÍAS') or 0))
            
            suma_vencido_2 = v01 + v31 + v61 + v91 + v120
            fila_con_anio = {
                **dict(row), 
                "anio": anio_real, 
                "TOTAL VENCIDO 2": suma_vencido_2,
                "01 A 30 DÍAS": v01,      # <--- Obligas a que sea el positivo
                "31 A 60 DÍAS": v31,
                "61 A 90 DÍAS": v61,
                "91 A 120 DÍAS": v91,
                "MÁS DE 120 DÍAS": v120
            }
            
            t_vigente += float(row.get('SALDO VIGENTE') or 0)
            t_01_30 += v01
            t_31_60 += v31
            t_61_90 += v61
            t_91_120 += v91
            t_mas_120 += v120
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

# ENDPOINTS DE REPORTES JURÍDICOS

@router.get("/Juridico/Reporte-mensual", response_model=List[schemas.ReporteJuridicoResponse])
def get_reporte_juridico(
    start_date: str = Query(..., description="Fecha inicio YYYY-MM-DD"),
    end_date: str = Query(..., description="Fecha fin YYYY-MM-DD"),
    proyecto: Optional[str] = Query(None, description="Filtrar por nombre del proyecto"),
    db: Session = Depends(get_db),
    user: dict = Depends(es_usuario)
):
    try:
        # Consulta base con join a pagos para acceder a datos relacionados
        query = db.query(Venta, Pago).join(
            Pago,
            and_(
                Pago.folio_venta == Venta.folio,
                Pago.fecha_comprobante >= start_date,
                Pago.fecha_comprobante <= end_date,
                Pago.estatus_flujo == 'active',
                Pago.estatus == 'active',
                func.lower(Pago.metodo_pago) != 'nota de crédito'
            )
        ).filter(
            Venta.fecha_inicio_operacion >= start_date,
            Venta.fecha_inicio_operacion <= end_date,
            Venta.estado_expediente.in_(['Jurídico', 'Verificación de datos', 'Firma', 'Firmado por Cliente', 'Firma de Testigos', 'Contrato Firmado '])   
        ).order_by(Venta.folio, Pago.fecha_comprobante.desc(), Pago.numero_pago.desc())

        if proyecto and proyecto.lower() != "todos":
            query = query.filter(Venta.desarrollo == proyecto)

        filas = query.all()

        # Evita duplicados por múltiples pagos (incluyendo pagos divididos)
        ventas = {}
        for venta, pago in filas:
            if venta.folio not in ventas:
                ventas[venta.folio] = (venta, pago)

        meses = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

        reporte_final = []
        for v, p in ventas.values():
            # --- Lógica de Nombres y Copropietarios ---
            nombres_lista = []
            if v.cliente:
                nombres_lista.append(v.cliente)
            
            # Buscamos copropietarios del 2 al 6
            for i in range(2, 7):
                nombre_copro = getattr(v, f"cliente_{i}", None)
                if nombre_copro:
                    nombres_lista.append(nombre_copro)

            # Formateo del nombre final y bandera de copropietarios
            tiene_copro = len(nombres_lista) > 1 

            # Formateo del nombre
            nombre_cliente_final = ""
            if tiene_copro:
                nombre_cliente_final = ", ".join(nombres_lista[:-1]) + " y " + nombres_lista[-1]
            else:
                nombre_cliente_final = nombres_lista[0] if nombres_lista else ""

            # --- Obtener nombre del mes ---
            mes_nombre = ""
            if v.fecha_inicio_operacion:
                try:
                    mes_idx = int(str(v.fecha_inicio_operacion).split("-")[1])
                    mes_nombre = meses[mes_idx]
                except: pass

            # Construcción del objeto de respuesta
            obj = schemas.ReporteJuridicoResponse(
                Folio=str(v.folio or ""),
                # StatusPipeline=str(v.estado_expediente or ""),
                TieneCopropietarios=tiene_copro,
                Ubicacion={
                    "Mes": mes_nombre,
                    "ContratosElaborados": str(getattr(v, 'contratos_elaborados', "") or ""),
                    "Lote": str(v.numero or ""),
                    "Cluster": str(v.etapa or ""),
                    "NumRegistral": extraer_numeros_finales(getattr(v, 'clasificador', "")),
                    "M2": str(v.metros_cuadrados or "")
                },
                ClienteFinanciamiento={
                    "NombreCliente": nombre_cliente_final, 
                    "PrecioFinal": str(v.precio_final or ""),
                    "Promocion": str(getattr(p, 'promocion', "") or "")
                },
                AsesorComision={
                    "Asesor": str(v.asesor or ""),
                    "FuerzaVenta": str(v.canal_ventas or ""),
                    "EmailsAsesores": str(getattr(v, 'correo_electronicos', "") or "")
                },
                EstatusContrato={
                    "Etapa": str(getattr(p, 'estatus_expediente', "") or ""),
                    "TipoFirma": str(getattr(v, 'tipo_firma', "") or ""),
                    "AutEspecial": str(getattr(v, 'autorizacion_especial', "") or ""),
                    "FechaEntregaAut": str(getattr(v, 'fecha_entrega_autorizacion', "") or "")
                },
                GestionJuridica={
                    "FechaIngresoJuridico": str(getattr(v, 'fecha_fin_pago_enganche', "") or ""),
                    "FechaVerificacion": str(getattr(v, 'fecha_verificacion_juridico', "") or ""),
                    "TieneModificaciones": str(getattr(v, 'modificaciones_contrato', "") or ""),
                    "FechaCorreccion": str(getattr(v, 'fecha_correccion_contrato', "") or ""),
                    "ResponsableSubsanar": str(getattr(v, 'responsable_subsanar', "") or ""),
                    "FechaConfirmacion": str(getattr(v, 'fecha_confirmacion_contrato', "") or ""),
                    "FirmaCliente": str(getattr(v, 'fecha_firma_cliente', "") or ""),
                    "IntentosFirma": str(getattr(v, 'intentos_firma', "") or ""),
                    "CierreJuridico": str(getattr(v, 'fecha_cierre_juridico', "") or "")
                },
                Testigos={
                    "Blindaje": str(getattr(v, 'blindaje', "") or ""),
                    "FirmaCliente": str(getattr(v, 'fecha_firma_testigos_cliente', "") or ""),
                    "EnvioContabilidad": str(getattr(v, 'fecha_envio_contabilidad', "") or ""),
                    "Intentos": str(getattr(v, 'intentos_testigos', "") or ""),
                    "FechaEntregaAut": str(getattr(v, 'fecha_entrega_autorizacion_testigos', "") or ""),
                    "FirmaEllys": str(getattr(v, 'firma_ellys', "") or ""),
                    "StatusEllys": str(getattr(v, 'status_ellys', "") or ""),
                    "FirmaTatiana": str(getattr(v, 'firma_tatiana', "") or ""),
                    "StatusTatiana": str(getattr(v, 'status_tatiana', "") or ""),
                    "Cuzam": str(getattr(v, 'cuzam', "") or ""),
                    "StatusCuzam": str(getattr(v, 'status_cuzam', "") or ""),
                    "RiesgoEntregaPcv": str(getattr(v, 'riesgo_entrega_pcv', "") or "")
                },
                Comentarios=str(getattr(v, 'comentarios_juridico', "") or ""),
                Comentarios2=str(getattr(v, 'comentarios_testigos', "") or ""),
                Observaciones=str(getattr(v, 'observaciones_juridico', "") or "")
            )
            reporte_final.append(obj)

        return reporte_final
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en reporte jurídico: {str(e)}")

@router.get("/Juridico/ADMVentas", response_model=List[schemas.ReporteADMVentasJuridicoResponse])
def get_reporteADMVentas_juridico(
    start_date: str = Query(..., description="Fecha inicio YYYY-MM-DD"),
    end_date: str = Query(..., description="Fecha fin YYYY-MM-DD"),
    proyecto: Optional[str] = Query(None, description="Filtrar por nombre del proyecto"),
    db: Session = Depends(get_db),
    user: dict = Depends(es_usuario)
):
    try:
        query = db.query(Venta, Pago).join(
            Pago,
            and_(
                Pago.folio_venta == Venta.folio,
                Pago.fecha_comprobante >= start_date,
                Pago.fecha_comprobante <= end_date,
                Pago.estatus_flujo == 'active',
                Pago.estatus == 'active',
                func.lower(Pago.metodo_pago) != 'nota de crédito'
            )
        ).filter(
            Venta.fecha_inicio_operacion >= start_date,
            Venta.fecha_inicio_operacion <= end_date,
            Venta.estado_expediente.in_(['Jurídico', 'Verificación de datos', 'Firma', 'Firmado por Cliente', 'Firma de Testigos', 'Contrato Firmado '])
        ).order_by(Venta.folio, Pago.fecha_comprobante.desc(), Pago.numero_pago.desc())

        if proyecto and proyecto.lower() != "todos":
            query = query.filter(Venta.desarrollo == proyecto)

        filas = query.all()

        ventas = {}
        for venta, pago in filas:
            if venta.folio not in ventas:
                ventas[venta.folio] = (venta, pago)

        meses = ["", "Enero", "Febrero", "Marzo", "Abril", "Mayo", "Junio", "Julio", "Agosto", "Septiembre", "Octubre", "Noviembre", "Diciembre"]

        reporte_final = []
        for v, p in ventas.values():
            nombres_lista = []
            if v.cliente:
                nombres_lista.append(str(v.cliente))

            for i in range(2, 7):
                nombre_copro = getattr(v, f"cliente_{i}", None)
                if nombre_copro:
                    nombres_lista.append(str(nombre_copro))

            if len(nombres_lista) > 1:
                nombre_cliente_final = ", ".join(nombres_lista[:-1]) + " y " + nombres_lista[-1]
            else:
                nombre_cliente_final = nombres_lista[0] if nombres_lista else ""

            mes_nombre = ""
            if v.fecha_inicio_operacion:
                try:
                    mes_idx = int(str(v.fecha_inicio_operacion).split("-")[1])
                    mes_nombre = meses[mes_idx]
                except: pass

            reporte_final.append(
                schemas.ReporteADMVentasJuridicoResponse(
                    Mes=mes_nombre,
                    ContratosFirmados=str(getattr(v, "contratos_firmados", "") or ""),
                    Lote=str(getattr(v, "numero", "") or ""),
                    Cluster=str( getattr(v, "etapa", "") or ""),
                    NumRegistral=str(getattr(v, "numero_registro", "") or ""),
                    M2=str( getattr(v, "metros_cuadrados", "") or ""),
                    NombreCliente=nombre_cliente_final,
                    Promocion=str(getattr(p, "promocion", "") or ""),
                    NombreAsesor=str( getattr(v, "asesor", "") or ""),
                    FuerzaVenta=str(getattr(v, "canal_ventas", "") or ""),
                    PrecioFinal=str(getattr(v, "precio_final", "") or ""),
                    AutEspecial=str(getattr(v, "autorizacion_especial", "") or ""),
                    FirmaCliente=str(getattr(v, "fecha_firma_cliente", "") or "")
                )
            )

        return reporte_final
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en reporte ADM Ventas jurídico: {str(e)}")

@router.get("/Juridico/Recordatorio-firmas", response_model=List[schemas.RecordatorioFirmaJuridicoResponse])
def get_recordatorioFirma_juridico(
    start_date: str = Query(..., description="Fecha inicio YYYY-MM-DD"),
    end_date: str = Query(..., description="Fecha fin YYYY-MM-DD"),
    proyecto: Optional[str] = Query(None, description="Filtrar por nombre del proyecto"),
    db: Session = Depends(get_db),
    user: dict = Depends(es_usuario)
):
    try:
        query = db.query(Venta, Pago).join(
            Pago,
            and_(
                Pago.folio_venta == Venta.folio,
                Pago.fecha_comprobante >= start_date,
                Pago.fecha_comprobante <= end_date,
                Pago.estatus_flujo == 'active',
                Pago.estatus == 'active',
                func.lower(Pago.metodo_pago) != 'nota de crédito'
            )
        ).filter(
            Venta.fecha_inicio_operacion >= start_date,
            Venta.fecha_inicio_operacion <= end_date,
            Venta.estado_expediente.in_(['Jurídico', 'Verificación de datos', 'Firma', 'Firmado por Cliente', 'Firma de Testigos', 'Contrato Firmado '])
        ).order_by(Venta.folio, Pago.fecha_comprobante.desc(), Pago.numero_pago.desc())

        if proyecto and proyecto.lower() != "todos":
            query = query.filter(Venta.desarrollo == proyecto)

        filas = query.all()

        ventas = {}
        for venta, pago in filas:
            if venta.folio not in ventas:
                ventas[venta.folio] = (venta, pago)

        recordatorios = []
        for v, _ in ventas.values():
            recordatorios.append(
                schemas.RecordatorioFirmaJuridicoResponse(
                    FechaNotificacion=str(getattr(v, 'fecha_notificacion_juridico', "") or ""),
                    NumNotificaciones=str(getattr(v, 'intentos_firma', "") or ""),
                    FechaFirmaTestigo1=str(getattr(v, 'firma_ellys', "") or ""),
                    FechaFirmaTestigo2=str(getattr(v, 'firma_tatiana', "") or ""),
                    FechaFirmaRl=str(getattr(v, 'fecha_firma_cliente', "") or ""),
                    Comentarios=str(getattr(v, 'comentarios_juridico', "") or "")
                )
            )

        return recordatorios
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en recordatorio de firma jurídico: {str(e)}")
    
@router.get("/Juridico/Escriturados", response_model=List[schemas.EscrituradosJuridicoResponse])
def get_escriturados_juridico(
    start_date: str = Query(..., description="Fecha inicio YYYY-MM-DD"),
    end_date: str = Query(..., description="Fecha fin YYYY-MM-DD"),
    proyecto: Optional[str] = Query(None, description="Filtrar por nombre del proyecto"),
    db: Session = Depends(get_db),
    user: dict = Depends(es_usuario)
):
    try:
        query = db.query(Venta, Pago).join(
            Pago,
            and_(
                Pago.folio_venta == Venta.folio,
                Pago.fecha_comprobante >= start_date,
                Pago.fecha_comprobante <= end_date,
                Pago.estatus_flujo == 'active',
                Pago.estatus == 'active',
                func.lower(Pago.metodo_pago) != 'nota de crédito'
            )
        ).filter(
            Venta.fecha_inicio_operacion >= start_date,
            Venta.fecha_inicio_operacion <= end_date,
            Venta.estado_expediente.in_(['Escriturado'])
        ).order_by(Venta.folio, Pago.fecha_comprobante.desc(), Pago.numero_pago.desc())

        if proyecto and proyecto.lower() != "todos":
            query = query.filter(Venta.desarrollo == proyecto)

        filas = query.all()

        ventas = {}
        for venta, pago in filas:
            if venta.folio not in ventas:
                ventas[venta.folio] = (venta, pago)

        recordatorios = []
        for v, _ in ventas.values():
            recordatorios.append(
                schemas.EscrituradosJuridicoResponse(
                    NombreCliente=str(getattr(v, 'cliente', "") or ""),
                    Lote=str(getattr(v, 'numero', "") or ""),
                    Cluster=str(getattr(v, 'etapa', "") or ""),
                    FechaEscrituracion=str(getattr(v, 'fecha_escritura', "") or ""),
                    AnioEscrituracion=str(getattr(v, 'anio_escritura', "") or ""),
                    Notario=str(getattr(v, 'notario', "") or ""),
                    FechaEscrituraLista=str(getattr(v, 'fecha_escritura_lista', "") or ""),
                    FechaEscrituraEntregada=str(getattr(v, 'fecha_escritura_entregada', "") or "")
                )
            )

        return recordatorios
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en escriturados jurídico: {str(e)}")

@router.get("/Juridico/Escrituracion-financiamiento", response_model=List[schemas.EscrituracionFinanciamientoResponse])
def get_escrituracion_financiamiento(
    start_date: str = Query(..., description="Fecha inicio YYYY-MM-DD"),
    end_date: str = Query(..., description="Fecha fin YYYY-MM-DD"),
    proyecto: Optional[str] = Query(None, description="Filtrar por nombre del proyecto"),
    db: Session = Depends(get_db),
    user: dict = Depends(es_usuario)
):
    try:
        # 1) Obtener Ventas filtradas
        # Simplificamos el folio tratándolo como viene en la BD
        query_ventas = db.query(Venta).filter(
            Venta.fecha_inicio_operacion >= start_date,
            Venta.fecha_inicio_operacion <= end_date,
            Venta.estado_expediente.in_(['Contrato Firmado', 'Incidencias', 'Liquidado'])
        )

        if proyecto and proyecto.lower() != "todos":
            query_ventas = query_ventas.filter(Venta.desarrollo == proyecto)

        ventas = query_ventas.all()
        if not ventas:
            return []

        # Diccionario para acceso rápido y lista de folios para las siguientes queries
        mapa_ventas = {str(v.folio): v for v in ventas}
        lista_folios = list(mapa_ventas.keys())

        # 2) Obtener y UNIFICAR Pagos
        # Agrupamos por folio, número y concepto para sumar abonos fragmentados
        pagos_db = db.query(
            Pago.folio_venta,
            Pago.numero_pago,
            Pago.concepto_pago,
            func.sum(Pago.monto_pagado).label("monto_total_unificado")
        ).filter(
            Pago.folio_venta.in_(lista_folios),
            Pago.estatus_flujo == 'active',
            Pago.estatus == 'active',
            func.lower(Pago.metodo_pago) != 'nota de crédito'
        ).group_by(
            Pago.folio_venta,
            Pago.numero_pago,
            Pago.concepto_pago
        ).all()

        # Estructura: {(folio, num_pago, concepto): monto_total}
        pagos_acumulados = {}
        for p in pagos_db:
            key = (str(p.folio_venta), str(p.numero_pago).strip(), str(p.concepto_pago or "").strip().lower())
            pagos_acumulados[key] = float(p.monto_total_unificado)

        # 3) Obtener Amortizaciones para comparar
        amortizaciones = db.query(
            Amortizacion.folder_id,
            Amortizacion.number,
            Amortizacion.concept,
            Amortizacion.total
        ).filter(
            Amortizacion.folder_id.in_(lista_folios)
        ).all()

        # 4) Calcular mensualidades faltantes
        faltantes_por_folio = defaultdict(int)
        for am in amortizaciones:
            f_id = str(am.folder_id)
            n_pag = str(am.number).strip()
            # Traducimos el concepto de la amortización para que coincida con el del Pago
            concepto_traducido = _traducir_concepto_amortizacion(am.concept)
            
            total_requerido = _to_float(am.total)
            # Buscamos cuánto se ha pagado en total para este folio/número/concepto
            total_abonado = pagos_acumulados.get((f_id, n_pag, concepto_traducido), 0.0)

            # Si lo abonado es menor a lo requerido (con margen de 1 centavo)
            if total_abonado + 0.01 < total_requerido:
                faltantes_por_folio[f_id] += 1

        # 5) Construir Respuesta Final
        respuesta = []
        for folio_str, venta in mapa_ventas.items():
            respuesta.append(
                schemas.EscrituracionFinanciamientoResponse(
                    Folio=folio_str,
                    NombreCliente=venta.cliente or "",
                    Cluster=venta.etapa or "",
                    NumeroLote=venta.numero or "",
                    Telefono=venta.telefono or "",
                    CorreoElectronico=venta.correo_electronico or "",
                    Estado=venta.estado or "",
                    Pais=venta.pais or "",
                    MensualidadesFaltantes=faltantes_por_folio[folio_str]
                )
            )

        return respuesta

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en proceso: {str(e)}")

@router.get("/Juridico/Incidencias", response_model=List[schemas.IncidenciasResponse])
def get_incidencias(
    start_date: str = Query(..., description="Fecha inicio YYYY-MM-DD"),
    end_date: str = Query(..., description="Fecha fin YYYY-MM-DD"),
    proyecto: Optional[str] = Query(None, description="Filtrar por nombre del proyecto"),
    db: Session = Depends(get_db),
    user: dict = Depends(es_usuario)
):
    try:
        query = db.query(Venta, Pago).join(
            Pago,
            and_(
                Pago.folio_venta == Venta.folio,
                Pago.fecha_comprobante >= start_date,
                Pago.fecha_comprobante <= end_date,
                Pago.estatus_flujo == 'active',
                Pago.estatus == 'active',
                func.lower(Pago.metodo_pago) != 'nota de crédito'
            )
        ).filter(
            Venta.fecha_inicio_operacion >= start_date,
            Venta.fecha_inicio_operacion <= end_date,
            Venta.estado_expediente.in_(['Contrato Firmado', 'Incidencias', 'Liquidado'])
        ).order_by(Venta.folio, Pago.fecha_comprobante.desc(), Pago.numero_pago.desc())

        if proyecto and proyecto.lower() != "todos":
            query = query.filter(Venta.desarrollo == proyecto)

        filas = query.all()

        ventas_unicas = {}
        for venta, _ in filas:
            folio = str(getattr(venta, 'folio', '') or '')
            if folio not in ventas_unicas:
                ventas_unicas[folio] = venta

        respuesta = []
        for folio, v in ventas_unicas.items():
            respuesta.append(
                schemas.IncidenciasResponse(
                    Folio=folio,
                    NombreCliente=str(getattr(v, 'cliente', '') or ''),
                    NumeroLote=str(getattr(v, 'numero', '') or ''),
                    Cluster=str(getattr(v, 'etapa', '') or ''),
                    MotivoIncidencia=str(getattr(v, 'estado_expediente', '') or ''),
                    FechaSeguimiento1="",
                    FechaSeguimiento2=""
                )
            )

        return respuesta

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en incidencias: {str(e)}")

# ENDPOINTS DE REPORTES DE EXPEDIENTES PARA RESPOND.IO (TITULARES Y COPROPIETARIOS)

@router.get("/expedientes-detallado", response_model=List[schemas.ReporteExpedientesDetalladoResponse])
def get_reporte_expedientes_detallado(anio: Optional[int] = None, db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    try:
        busqueda = f"%{anio}%" if anio else None

        # Subconsulta: solo copropietarios reales, excluyendo al titular por flag y por nombre
        subquery_count = """
        (
            SELECT COUNT(*)
            FROM notificaciones_gestion_clientes ngc
            WHERE ngc.folio = v.FOLIO
                AND (ngc.es_propietario_principal = 0 OR ngc.es_propietario_principal IS NULL)
                AND TRIM(LOWER(COALESCE(ngc.client_name, ''))) <> TRIM(LOWER(COALESCE(v.CLIENTE, '')))
        )
        """

        query = text(f"""
            -- 1. Fila del TITULAR (Es Propietario Principal)
            SELECT 
                v.FOLIO as FOLIO,
                v.CLIENTE as CLIENTE,
                '' as DETALLES,
                {subquery_count} as `NÚMERO DE COPROPIETARIO`,
                1 as `ES TITULAR`, -- Marcamos como Verdadero
                COALESCE(v.`TELÉFONO`, '') as TELEFONO,
                COALESCE(v.`CORREO ELECTRÓNICO`, '') as CORREO,
                v.`METROS CUADRADOS` as M2,
                v.DESARROLLO as PROYECTO,
                v.ETAPA as CLUSTER,
                v.NÚMERO as LOTE,
                v.ASESOR as ASESOR,
                v.`ESTADO DEL EXPEDIENTE` as `ESTATUS PIPELINE`,
                v.`FECHA DE INICIO DE OPERACIÓN` as `INICIO OPERACIONES`,
                v.`PLAZO DE FINANCIAMIENTO` as PLAZO,
                v.`PRECIO FINAL` as `PRECIO TOTAL`
            FROM ventas v
            WHERE (:anio_val IS NULL OR v.`FECHA DE INICIO DE OPERACIÓN` LIKE :busqueda_anio)
                AND v.`ESTADO DEL EXPEDIENTE` NOT IN ('cancelado', 'expirado', 'Cancelado', 'Expirado')

            UNION ALL

            -- 2. Filas de COPROPIETARIOS (No son Propietarios Principales)
            SELECT 
                v.FOLIO as FOLIO,
                gc.client_name as CLIENTE,
                '' as DETALLES,
                {subquery_count} as `NÚMERO DE COPROPIETARIO`,
                0 as `ES TITULAR`, -- Marcamos como Falso
                COALESCE(gc.telefono, '') as TELEFONO,
                COALESCE(gc.email, '') as CORREO,
                v.`METROS CUADRADOS` as M2,
                v.DESARROLLO as PROYECTO,
                v.ETAPA as CLUSTER,
                v.NÚMERO as LOTE,
                v.ASESOR as ASESOR,
                v.`ESTADO DEL EXPEDIENTE` as `ESTATUS PIPELINE`,
                v.`FECHA DE INICIO DE OPERACIÓN` as `INICIO OPERACIONES`,
                v.`PLAZO DE FINANCIAMIENTO` as PLAZO,
                v.`PRECIO FINAL` as `PRECIO TOTAL`
            FROM ventas v
            INNER JOIN notificaciones_gestion_clientes gc ON v.FOLIO = gc.folio
            WHERE (:anio_val_2 IS NULL OR v.`FECHA DE INICIO DE OPERACIÓN` LIKE :busqueda_anio_2)
              AND v.`ESTADO DEL EXPEDIENTE` NOT IN ('cancelado', 'expirado', 'Cancelado', 'Expirado')
              AND (gc.es_propietario_principal = 0 OR gc.es_propietario_principal IS NULL)
                            AND TRIM(LOWER(COALESCE(gc.client_name, ''))) <> TRIM(LOWER(COALESCE(v.CLIENTE, '')))
            
            ORDER BY FOLIO ASC, `ES TITULAR` DESC 
        """)

        params = {
            "anio_val": anio, "busqueda_anio": busqueda,
            "anio_val_2": anio, "busqueda_anio_2": busqueda
        }

        result = db.execute(query, params).mappings().all()
        return result

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error en reporte detallado: {str(e)}")