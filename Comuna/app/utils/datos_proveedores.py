from sqlalchemy import text, cast, BigInteger, func
from ..models import Venta, Cliente, Amortizacion, GestionClientes, ConfigEtapa, Pago, Cartera
from ..services.pagos_utils import encontrar_pago_actual, encontrar_pago_actual_mes
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session, sessionmaker
from typing import List
from datetime import datetime
from zoneinfo import ZoneInfo
import logging
import re
from num2words import num2words
from sqlalchemy import text, func

logger = logging.getLogger(__name__)


def _normalizar_lista_entrada(valores: List[str]) -> List[str]:
    """Normaliza entradas que pueden venir como lista o CSV dentro de cada item."""
    salida: List[str] = []
    for item in (valores or []):
        for parte in str(item).replace(";", ",").split(","):
            limpio = parte.strip()
            if limpio:
                salida.append(limpio)
    return salida

def aplicar_formato_etiqueta(tag: str, valor: any):
    if valor in [None, "", "None", "NULL"]:
        return ""
    
    tag_l = tag.lower()
    val_str = str(valor).strip()

    try:
        # --- REGLAS DE TEXTO ---
        if "nombre_del_cliente" in tag_l or "{cl.cliente}" in tag_l or "{f.nombre_cliente}" in tag_l:
            return val_str.upper()

        # --- REGLAS DE NÚMEROS (CORRECCIÓN: Se quita el split plano para usar lo ya procesado) ---
        # No modificamos v.numero aquí, ya que Jurídico usa sus propias etiquetas f. procesadas con Regex.

        # --- REGLAS DE FECHAS ---
        if "formato_fechas" in tag_l or "{cl.fecha}" in tag_l or "{f.fecha_mensualidades}" in tag_l:
            # Si es una lista de fechas (Jurídico), no aplicamos fecha_a_letra a todo el string
            if "{f.fecha_mensualidades}" in tag_l: return val_str
            return fecha_a_letra(val_str)

        # --- REGLAS NUMÉRICAS ---
        # Manejamos etiquetas que piden conversión a LETRAS (Jurídico)
        if "_letra" in tag_l:
            return monto_a_letra(val_str)

        num_val = float(val_str)
        
        # Metros cuadrados
        if "metros_cuadrados" in tag_l or "m2" in tag_l:
            return f"{num_val:.2f}"
            
        # Moneda (Comas, puntos y signo $)
        if any(x in tag_l for x in ["precio", "monto", "saldo", "importe", "enganche", "apartado", "pago_72", "pena"]):
            if "letra" in tag_l:
                return monto_a_letra(num_val)
            return f"${num_val:,.2f}"

    except (ValueError, TypeError):
        pass
        
    return valor

def fecha_a_letra(fecha_str):
    """Convierte 2022-10-03 a 'tres de octubre de dos mil veintidos'"""
    try:
        dt = datetime.strptime(fecha_str, '%Y-%m-%d')
        meses = ["enero", "febrero", "marzo", "abril", "mayo", "junio", 
                "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
        dia_letra = num2words(dt.day, lang='es')
        mes_letra = meses[dt.month - 1]
        anio_letra = num2words(dt.year, lang='es')
        return f"{dia_letra} de {mes_letra} de {anio_letra}"
    except:
        return fecha_str

def fecha_estilo_contrato(fecha_input):
    """Convierte 2022-10-03 a '3 de octubre del 2022'"""
    if not fecha_input: return ""
    try:
        if isinstance(fecha_input, str):
            dt = datetime.strptime(fecha_input.split(" ")[0], '%Y-%m-%d')
        else:
            dt = fecha_input
            
        meses = ["enero", "febrero", "marzo", "abril", "mayo", "junio", 
                    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"]
        
        return f"{dt.day} de {meses[dt.month - 1]} del {dt.year}"
    except:
        return str(fecha_input)

def monto_a_letra(monto):
    """Convierte 664929.24 a formato legal MONEDA NACIONAL"""
    try:
        monto = float(monto)
        entero = int(monto)
        decimales = int(round((monto - entero) * 100))
        letras = num2words(entero, lang='es').upper()
        return f"({letras} PESOS {decimales:02d}/100 MONEDA NACIONAL)"
    except:
        return ""

def get_komunah_data(folio_ref: str, db: Session):
    logger.info("[DATOS_KOMUNAH] Entrada get_komunah_data | folio=%s", folio_ref)
    # 1. BUSCAR VENTA

    if folio_ref is None or str(folio_ref).upper() == "NULL":
        data = {}
        
        for col in inspect(Venta).mapper.column_attrs:
            data[f"{{v.{col.key.lower()}}}"] = ""


        for col in inspect(Amortizacion).mapper.column_attrs:
            data[f"{{p.{col.key.lower()}}}"] = ""

    
        for i in range(1, 7):
            for col in inspect(Cliente).mapper.column_attrs:
                data[f"{{c{i}.{col.key.lower()}}}"] = ""
            for col in inspect(GestionClientes).mapper.column_attrs:
                data[f"{{g{i}.{col.key.lower()}}}"] = ""

    
        data.update({
            "{sys.etapa_activa}": "",
            "{sys.bloqueo_motivo}": "",
            "{cl.unidad}": "",
            "{cl.monto}": "",
            "{cl.cliente}": "",
            "{cl.num}": "",
            "{cl.fecha}": "",
            "{cl.fecha_pago}": "",
            "{cl.dias_para_pago}": "",
            "{cl.concepto}": "",
            "{cl.proyecto}": ""
        })
        logger.info("[DATOS_KOMUNAH] Salida get_komunah_data sin folio | tags=%s", len(data))
        return data

        
    logger.info("[DATOS_KOMUNAH] Paso: consultar venta")
    venta = db.query(Venta).filter(Venta.folio == folio_ref).first()
    if not venta:
        logger.warning("[DATOS_KOMUNAH] Venta no encontrada | folio=%s", folio_ref)
        return {}

    data = {}
    
    conf_cluster = db.query(ConfigEtapa).filter(ConfigEtapa.etapa == venta.etapa).first()
    
    def _es_activo(val):
        """Normaliza valores decimales/booleanos/string a bool. 
        Necesario porque MySQL guarda DECIMAL(10,4): 0.0000 ó 1.0000."""
        if val is None:
            return False
        try:
            return bool(float(str(val)))
        except (ValueError, TypeError):
            return str(val).strip().lower() not in ('false', '0', '')

    etapa_permiso = "1"
    motivo_bloqueo = None 

    if not conf_cluster:
        etapa_permiso = "0"
        motivo_bloqueo = f"CONFIG_FALTANTE: Etapa '{venta.etapa}' no existe en SQL"
    elif not _es_activo(conf_cluster.proyecto_activo):
        etapa_permiso = "0"
        motivo_bloqueo = f"PROYECTO_OFF: Desarrollo '{conf_cluster.proyecto}' desactivado"
    elif not _es_activo(conf_cluster.etapa_activo):
        etapa_permiso = "0"
        motivo_bloqueo = f"ETAPA_OFF: Cluster '{conf_cluster.etapa}' desactivado"
    
    data["{sys.etapa_activa}"] = etapa_permiso
    if motivo_bloqueo:
        data["{sys.bloqueo_motivo}"] = motivo_bloqueo
    

    for col in inspect(venta).mapper.column_attrs:
        val = getattr(venta, col.key)
        if val is not None and str(val).strip() not in ["", "None", "NULL"]:
        
            data[f"{{v.{col.key.lower()}}}"] = str(val)


    logger.info("[DATOS_KOMUNAH] Paso: consultar amortizaciones")
    amortizaciones = db.query(Amortizacion).filter(Amortizacion.folder_id == folio_ref)\
                    .order_by(Amortizacion.date.asc()).all()
    
    p_act = encontrar_pago_actual(amortizaciones)
    
    # --- CÁLCULO: Prefijo cl. ---
    monto_val = 0.0
    pagado_parcial = 0.0
    if p_act and hasattr(p_act, 'total') and p_act.total is not None:
        monto_val = float(p_act.total)
        # Diccionario para mapear conceptos de amortizaciones a conceptos de pagos en SQL
        tradu_sql = {"financing": "Parcialidad", "down_payment": "Enganche", "initial_payment": "Apartado", "last_payment": "Último pago"}
        conc_traducido = tradu_sql.get(p_act.concept, p_act.concept)
        
        res_suma = db.query(func.sum(Pago.monto_pagado)).filter(
            Pago.folio_venta == int(folio_ref), 
            Pago.numero_pago == p_act.number,
            Pago.concepto_pago == conc_traducido,
            Pago.estatus == 'active' 
        ).scalar()
        pagado_parcial = float(res_suma or 0)
    saldo_actual_vigente = monto_val - pagado_parcial

    # --- CÁLCULO DE DÍAS RESTANTES (Dentro de get_komunah_data) ---
    dias_para_vencer = 0
    fecha_pago_humanizada = ""
    hoy_dt = datetime.now(ZoneInfo("America/Mexico_City"))
    
    if p_act and hasattr(p_act, 'date') and p_act.date:
        try:
            # Convertimos la fecha del pago (str) a objeto datetime
            fecha_vencimiento = datetime.strptime(str(p_act.date), '%Y-%m-%d').date()
            fecha_hoy = hoy_dt.date()
            
            # Calculamos la diferencia
            delta = (fecha_vencimiento - fecha_hoy).days
            dias_para_vencer = delta
            
            # Formateamos la fecha (ej. 15/03/2026)
            fecha_pago_humanizada = fecha_vencimiento.strftime('%d/%m/%Y')
        except Exception as e:
            logger.error(f"[DATOS_KOMUNAH] Error calculando dias_para_vencer: {e}")

    # Mapeo manual con etiquetas estandarizadas
    data.update({
        "{cl.unidad}": str(getattr(venta, 'numero', "")),
        "{cl.monto}": f"${monto_val:,.2f}",
        "{cl.monto_a_pagar}": f"${saldo_actual_vigente:,.2f}",
        "{cl.cliente}": str(getattr(venta, 'cliente', "")),
        "{cl.num}": str(getattr(p_act, 'number', "")) if p_act else "",
        "{cl.fecha}": str(getattr(p_act, 'date', "")) if p_act else "",
        "{cl.fecha_pago}": fecha_pago_humanizada if fecha_pago_humanizada else "N/A",
        "{cl.dias_para_pago}": dias_para_vencer if dias_para_vencer is not None else "",
        "{cl.concepto}": str(getattr(p_act, 'concept', "")) if p_act else "",
        "{cl.proyecto}": str(getattr(venta, 'desarrollo', ""))
    })

    # --- PAGOS: Prefijo p. ---
    if p_act:
        traducciones = {
            "financing": "Parcialidad",
            "down_payment": "Enganche",
            "initial_payment": "Apartado",
            "last_payment": "Último pago"
        }
        for col in inspect(p_act).mapper.column_attrs:
            val_p = getattr(p_act, col.key)
            if val_p is not None:
                if col.key == "concept":
                    val_p = traducciones.get(str(val_p).strip(), val_p)
                    
                data[f"{{p.{col.key.lower()}}}"] = str(val_p)

    
    id_fields = ['id_cliente', 'id_cliente_2', 'id_cliente_3', 'id_cliente_4', 'id_cliente_5', 'id_cliente_6']
    
    for i, field in enumerate(id_fields, start=1):
        c_id_raw = getattr(venta, field, None)
        if not c_id_raw: continue
        
        try:
            c_id_limpio = str(int(float(c_id_raw)))
        except:
            c_id_limpio = str(c_id_raw)

        # Algunas columnas Float en la tabla clientes tienen '' en vez de NULL.
        # SQLAlchemy nativo falla con ValueError al hacer el type-cast.
        # Intentamos ORM primero; si falla, caemos a SQL crudo que no castea tipos.
        try:
            logger.info("[DATOS_KOMUNAH] Paso: consultar cliente | client_id=%s", c_id_limpio)
            cliente_db = db.query(Cliente).filter(Cliente.client_id == c_id_limpio).first()
        except ValueError:
            from types import SimpleNamespace
            row_raw = db.execute(
                text("SELECT * FROM clientes WHERE client_id = :id"),
                {"id": c_id_limpio}
            ).mappings().first()
            cliente_db = SimpleNamespace(**{k: (None if v == '' else v) for k, v in dict(row_raw).items()}) if row_raw else None
        except Exception as e:
            logger.exception(
                "[DATOS_KOMUNAH] Error consultando clientes por client_id=%s | folio=%s | error=%s",
                c_id_limpio, folio_ref, str(e),
            )
            cliente_db = None

        if cliente_db:
            prefijo = f"c{i}."
            if hasattr(cliente_db, '_sa_instance_state'):
                for col in inspect(cliente_db).mapper.column_attrs:
                    val_c = getattr(cliente_db, col.key)
                    if val_c is not None:
                        data[f"{{{prefijo}{col.key.lower()}}}"] = str(val_c)
            else:
                for key, val_c in vars(cliente_db).items():
                    if val_c is not None:
                        data[f"{{{prefijo}{key.lower()}}}"] = str(val_c)
        
        try:
            logger.info("[DATOS_KOMUNAH] Paso: consultar gestion cliente | folio=%s | client_id=%s", folio_ref, c_id_limpio)
            gestion_db = db.query(GestionClientes).filter(
                GestionClientes.folio == folio_ref,
                GestionClientes.client_id == c_id_limpio
            ).first()
        except Exception as e:
            logger.exception(
                "[DATOS_KOMUNAH] Error consultando gestion clientes | folio=%s | client_id=%s | error=%s",
                folio_ref,
                c_id_limpio,
                str(e),
            )
            gestion_db = None

        if gestion_db:
            prefijo_g = f"g{i}."
            for col in inspect(gestion_db).mapper.column_attrs:
                val_g = getattr(gestion_db, col.key)
                
                if isinstance(val_g, bool):
                    val_g = "1" if val_g else "0"
                
                if val_g is not None:
                    data[f"{{{prefijo_g}{col.key.lower()}}}"] = str(val_g)       
    # hoy_dt ya fue definido arriba para cl. y ven.
    hoy_str = hoy_dt.strftime('%Y-%m-%d')
    from ..models import Cartera
    # Buscamos el resumen oficial en la tabla Cartera para este folio
    cv = db.query(Cartera).filter(Cartera.folio == int(folio_ref)).first()

    # Si el CRM dice que debe, jalamos sus totales; si no, es 0
    ven_meses_atraso = int(float(cv.parcialidades_vencidas or 0)) if cv else 0
    ven_saldo_vencido = float(cv.total_vencido_sin_pen or 0) if cv else 0.0
    saldo_total_vencido = float(cv.total_vencido_con_pen or 0) if cv else 0.0
    ven_penalizacion_acumulada = saldo_total_vencido - ven_saldo_vencido
    if saldo_total_vencido <= 0:
        ven_meses_atraso = 0
        ven_saldo_vencido = 0.0
        ven_penalizacion_acumulada = 0.0
        fecha_mas_antigua = None

    ven_monto_mes_puro = 0.0       
    ven_monto_mes_pendiente = 0.0  
    ven_penalizacion_mes_actual = 0.0
    fecha_mas_antigua = None


    for amt in amortizaciones:
        # Suma TODOS los abonos activos de esta letra Y CONCEPTO (fix crítico: Enganche 1 vs Parcialidad 1)
        tradu_sql = {"financing": "Parcialidad", "down_payment": "Enganche", "initial_payment": "Apartado", "last_payment": "Último pago"}
        conc_amt = tradu_sql.get(amt.concept, amt.concept)
        
        pagado = float(db.query(func.sum(Pago.monto_pagado)).filter(
            Pago.folio_venta == int(folio_ref), 
            Pago.numero_pago == amt.number,
            Pago.concepto_pago == conc_amt,
            Pago.estatus == 'active' 
        ).scalar() or 0)
        
        total_deberia = float(amt.total or 0)
        esta_pendiente = pagado < total_deberia

        # Buscamos la fecha de mora REAL (solo si el CRM dice que debe)
        if amt.date < hoy_str and esta_pendiente and ven_meses_atraso > 0:
            if not fecha_mas_antigua:
                fecha_mas_antigua = datetime.strptime(amt.date, '%Y-%m-%d')
        
        # Datos para las variables del mes (cl.)
        if p_act and amt.number == p_act.number:
            ven_monto_mes_puro = total_deberia 
            ven_penalizacion_mes_actual = float(amt.penalized_amount or 0)
            ven_monto_mes_pendiente = float(total_deberia - pagado)


    dias_atraso = (hoy_dt.replace(tzinfo=None) - fecha_mas_antigua).days if fecha_mas_antigua else 0
    saldo_total_mes = ven_monto_mes_pendiente + ven_penalizacion_mes_actual
    saldo_total_vencido = ven_saldo_vencido + ven_penalizacion_acumulada
    saldo_total_a_pagar = saldo_total_vencido + saldo_total_mes

    data.update({
        "{ven.saldo_vencido}": f"${ven_saldo_vencido:,.2f}",     #
        "{ven.penalizacion_del_mes}": f"${ven_penalizacion_mes_actual:,.2f}",#
        "{ven.penalizacion_vencida}": f"${ven_penalizacion_acumulada:,.2f}",#
        "{ven.saldo_total_a_pagar}": f"${saldo_total_a_pagar:,.2f}",#           
        "{ven.mensualidades_vencidas}": ven_meses_atraso,     #
        "{ven.importe_del_mes}": f"${ven_monto_mes_puro:,.2f}",     #
        "{ven.cuota_mes_pendiente}": f"${ven_monto_mes_pendiente:,.2f}",#lo que le falta por pagar de esta letra
        "{ven.saldo_total_mes}": f"${saldo_total_mes:,.2f}",#
        "{ven.dias_atraso}": dias_atraso,  #dias desde la primera vez que cayo en moroso               
        "{ven.saldo_total_vencido}": f"${saldo_total_vencido:,.2f}",#

    })

    # --- LÓGICA DE EXTRACCIÓN Y CÁLCULOS JURÍDICO ---
    
    # 1. Filtramos solo mensualidades de financiamiento ('financing')
    finance_amts = [a for a in amortizaciones if a.concept == "financing"]
    num_pagos_financiamiento = len(finance_amts)
    
    # 2. Fechas de financiamiento (Primera y Última)
    f_inicio_finan = ""
    f_fin_finan = ""
    rango_texto = ""
    rango_letra = ""
    primer_enganche = "" 
    monto_parcialidad = ""
    
    if finance_amts:
        f_inicio_finan = str(finance_amts[0].date)
        f_fin_finan = str(finance_amts[-1].date)
        rango_texto = f"{f_inicio_finan} - {f_fin_finan}"
        rango_letra = f"{fecha_estilo_contrato(f_inicio_finan)} al {fecha_estilo_contrato(f_fin_finan)}"
        primer_enganche = next((str(a.date) for a in amortizaciones if a.concept == "down_payment"), "")
        monto_parcialidad = next((str(a.total) for a in amortizaciones if a.concept == "financing"), "")  

    # 3. Cálculo de Penas (Dinero basado en el monto de la mensualidad)
    # Tomamos el monto de la primera mensualidad de financiamiento
    monto_mensual = float(finance_amts[0].total) if finance_amts else 0.0
    mitad_plazo = num_pagos_financiamiento // 2
    pena_val = mitad_plazo * monto_mensual
    pena_2_val = (mitad_plazo + 1) * monto_mensual

    # 4. Procesamiento de Número y Comercial (Regex)
    num_original = str(getattr(venta, 'numero', "")).strip()
    num_comercial = num_original
    num_registral = ""
    if "-" in num_original or "–" in num_original:
        partes = re.split(r'\s*[\-\–]\s*', num_original)
        num_comercial = partes[0].strip()
        if len(partes) > 1:
            match_reg = re.search(r'(\d+)$', partes[1].strip())
            num_registral = match_reg.group(1) if match_reg else ""

    # 5. CAlculo de porcentaje
    monto_apartado = sum(float(a.total or 0) for a in amortizaciones if a.concept == "initial_payment")
    monto_enganche = sum(float(a.total or 0) for a in amortizaciones if a.concept == "down_payment")
    total_enganche_a_pagar = monto_apartado + monto_enganche
    precio_total_venta = float(getattr(venta, 'precio_final', 1) or 1)
    porcentaje_calculado = (total_enganche_a_pagar / precio_total_venta) * 100

    # 5. Actualización de data
    data.update({
        "{j.cliente}": getattr(venta, 'cliente', "").upper(),
        "{j.coopropietario_1}": getattr(venta, 'cliente_2', "").upper(),
        "{j.coopropietario_2}": getattr(venta, 'cliente_3', "").upper(),
        "{j.coopropietario_3}": getattr(venta, 'cliente_4', "").upper(),
        "{j.coopropietario_4}": getattr(venta, 'cliente_5', "").upper(),
        "{j.coopropietario_5}": getattr(venta, 'cliente_6', "").upper(),
        "{j.numero_comercial}": num_comercial,
        "{j.numero_registral}": num_registral,
        "{j.precio_final_letra}": float(getattr(venta, 'precio_final', 0) or 0),
        "{j.apartado_letra}": float(getattr(venta, 'apartado', 0) or 0),
        "{j.enganche_letra}": float(getattr(venta, 'total_enganche', 0) or 0),
        "{j.monto_sin_interes_letra}": float(getattr(venta, 'monto_sin_interes', 0) or 0),
        "{j.referencia_lote}": f"010100R{num_comercial}",
        "{j.saldo_final}": (float(getattr(venta, 'precio_final', 0) or 0) - sum(float(a.total or 0) for a in amortizaciones[:-1])) if amortizaciones else 0,
        "{j.saldo_final_letra}": float(getattr(venta, 'precio_final', 0) or 0) - sum(float(a.total or 0) for a in amortizaciones[:-1]) if amortizaciones else 0,
        "{j.fecha_mensualidades}": rango_texto, 
        "{j.numero_de_pagos}": num_pagos_financiamiento,
        "{j.primera_mensualidad}": f_inicio_finan,
        "{j.primer_enganche}": primer_enganche,
        "{j.monto_parcialidad}": monto_parcialidad,
        "{j.porcentaje}": f"{porcentaje_calculado:.2f}%",
        "{j.pena}": pena_val,
        "{j.pena_2}": pena_2_val,
        "{j.fecha_hoy_escrita}": fecha_estilo_contrato(datetime.now(ZoneInfo("America/Mexico_City"))),
        "{j.fecha_hoy_escrita_Mayusculas}": fecha_estilo_contrato(datetime.now(ZoneInfo("America/Mexico_City"))).upper(),
        "{j.paginacion}": "",
        "{j.total_paginas}": ""
    })

    #-- ETIQQUETAS PARA TABLA DE AMORTIZACIONES ---

    data.update({
        "{pagos.numero}": "",
        "{pagos.fecha}": "",
        "{pagos.capital}": "",
        "{pagos.enganche}": "",
        "{pagos.total}": "",
        "{pagos.saldo_capital}": "",
        "{totales.suma_capital}": "",
        "{totales.saldo_enganche}": "",
        "{totales.suma_total}": "",
    })
    
    logger.info("[DATOS_KOMUNAH] Paso: salida get_komunah_data | folio=%s | tags=%s", folio_ref, len(data))
    return data

def get_folios_a_notificar_komunah(db: Session, fecha: str):
    """
    RECORDATORIO AMISTOSO: folios que vencen en 'fecha' sin deuda de meses anteriores.
    CORREGIDO: usa SUM de pagos para detectar deuda real (no registro individual),
    evitando clasificar mal a clientes que pagan en varios abonos.
    """
    logger.info("[DATOS_KOMUNAH] Entrada get_folios_a_notificar_komunah | fecha=%s", fecha)
    query = text("""
        SELECT DISTINCT a.folder_id 
            FROM amortizaciones a
            JOIN ventas v ON v.FOLIO = a.folder_id
            JOIN config_etapas ce ON ce.etapa = v.ETAPA
            WHERE a.date = :f
            AND CAST(ce.etapa_activo AS DECIMAL(10,4)) > 0
            AND CAST(ce.proyecto_activo AS DECIMAL(10,4)) > 0
            AND v.`ESTADO DEL EXPEDIENTE` IN ('Incidencias', 'Contrato Firmado', 'Firma', 'Firma de Testigos', 'Firmado por Cliente', 'Agenda Escritura')
            
            -- INCLUSIÓN: amortización actual no está liquidada (filtrando por concepto para no mezclar Enganche con Parcialidad)
            AND a.total > (
                SELECT IFNULL(SUM(p.`Monto pagado`), 0)
                FROM pagos p
                WHERE p.`Folio de la venta` = a.folder_id
                    AND p.`Número de pago` = a.number
                    AND p.`Concepto de pago` = (
                        CASE a.concept 
                            WHEN 'financing' THEN 'Parcialidad'
                            WHEN 'down_payment' THEN 'Enganche'
                            WHEN 'initial_payment' THEN 'Apartado'
                            WHEN 'last_payment' THEN 'Último pago'
                            ELSE a.concept 
                        END
                    )
                    AND IFNULL(p.`Estatus`, '') != 'canceled'
            )
            
            -- EXCLUSIÓN: no tiene letras anteriores con deuda REAL (sumando todos sus abonos por concepto)
            AND NOT EXISTS (
                SELECT 1 FROM amortizaciones a2
                WHERE a2.folder_id = a.folder_id
                AND a2.date < a.date
                AND a2.total > (
                    SELECT IFNULL(SUM(px.`Monto pagado`), 0)
                    FROM pagos px
                    WHERE px.`Folio de la venta` = a2.folder_id
                        AND px.`Número de pago` = a2.number
                        AND px.`Concepto de pago` = (
                            CASE a2.concept 
                                WHEN 'financing' THEN 'Parcialidad'
                                WHEN 'down_payment' THEN 'Enganche'
                                WHEN 'initial_payment' THEN 'Apartado'
                                WHEN 'last_payment' THEN 'Último pago'
                                ELSE a2.concept 
                            END
                        )
                        AND IFNULL(px.`Estatus`, '') != 'canceled'
                )
            );
    """)
    registros = db.execute(query, {"f": fecha}).fetchall()
    resultado = [row[0] for row in registros]
    logger.info("[DATOS_KOMUNAH] Salida get_folios_a_notificar_komunah | fecha=%s | folios=%s", fecha, len(resultado))
    return resultado

def get_folios_deudores_komunah(db: Session, fecha: str):
    """
    COBRANZA: folios que vencen en 'fecha' y YA tienen deuda real de meses anteriores.
    CORREGIDO: usa SUM de pagos para detectar deuda real (no registro individual).
    """
    logger.info("[DATOS_KOMUNAH] Entrada get_folios_deudores_komunah | fecha=%s", fecha)
    query = text("""
        SELECT DISTINCT a.folder_id 
        FROM amortizaciones a
        JOIN ventas v ON v.FOLIO = a.folder_id
        JOIN config_etapas ce ON ce.etapa = v.ETAPA
        WHERE a.date = :f
        AND CAST(ce.etapa_activo AS DECIMAL(10,4)) > 0
        AND CAST(ce.proyecto_activo AS DECIMAL(10,4)) > 0
        AND v.`ESTADO DEL EXPEDIENTE` IN ('Incidencias', 'Contrato Firmado', 'Firma', 'Firma de Testigos', 'Firmado por Cliente')
        -- INCLUSIÓN: tiene al menos una letra anterior con deuda REAL
        AND EXISTS (
            SELECT 1 FROM amortizaciones a2
            WHERE a2.folder_id = a.folder_id 
            AND a2.date < a.date
            AND a2.total > (
                -- Debe existir al menos una letra vencida cuya suma de pagos sea menor al total
                SELECT IFNULL(SUM(p2.`Monto pagado`), 0)
                FROM pagos p2
                WHERE p2.`Folio de la venta` = a2.folder_id
                  AND p2.`Número de pago` = a2.number
                  AND p2.`Concepto de pago` = (
                        CASE a2.concept 
                            WHEN 'financing' THEN 'Parcialidad'
                            WHEN 'down_payment' THEN 'Enganche'
                            WHEN 'initial_payment' THEN 'Apartado'
                            WHEN 'last_payment' THEN 'Último pago'
                            ELSE a2.concept 
                        END
                  )
                  AND IFNULL(p2.Estatus, '') != 'canceled'
            )
        )
    """)
    registros = db.execute(query, {"f": fecha}).fetchall()
    resultado = [row[0] for row in registros]
    logger.info("[DATOS_KOMUNAH] Salida get_folios_deudores_komunah | fecha=%s | folios=%s", fecha, len(resultado))
    return resultado

def get_komunah_diccionario_maestro(flat_data: dict = None):
    """
    Escanea las tablas SQL y devuelve el catálogo.
    Si hay flat_data, FILTRA lo vacío y devuelve {tag, valor}.
    """
    logger.info("[DATOS_KOMUNAH] Entrada get_komunah_diccionario_maestro | con_data=%s", flat_data is not None)
    
    # Helper para extraer tags de SQL y filtrar si no hay data
    def extraer_tags(modelo, prefijo):
        resultado = []
        for col in inspect(modelo).mapper.column_attrs:
            tag = f"{{{prefijo}.{col.key.lower()}}}"
            if flat_data:
                valor = flat_data.get(tag)
                # SOLO añadimos si el valor no es None ni vacío
                if valor not in [None, "", "None", "NULL"]:
                    valor_formateado = aplicar_formato_etiqueta(tag, valor)
                    resultado.append({"tag": tag, "valor": valor_formateado})
            else:
                resultado.append(tag)
        return resultado
    
    # Helper para variables manuales
    def procesar_manual(lista_tags):
        resultado = []
        for t in lista_tags:
            if flat_data:
                valor = flat_data.get(t)
                if valor not in [None, "", "None", "NULL"]:
                    valor_formateado = aplicar_formato_etiqueta(t, valor)
                    resultado.append({"tag": t, "valor": valor_formateado})
            else:
                resultado.append(t)
        return resultado
    
    catalogo = []

    # 1. Cálculos de Cobranza (ven.)
    vars_ven = procesar_manual([
        "{ven.saldo_vencido}",
        "{ven.saldo_total_vencido}",
        "{ven.saldo_total_a_pagar}",
        "{ven.saldo_total_mes}",
        "{ven.penalizacion_del_mes}",
        "{ven.penalizacion_vencida}",
        "{ven.mensualidades_vencidas}",
        "{ven.importe_del_mes}",
        "{ven.cuota_mes_pendiente}",
        "{ven.dias_atraso}"
    ])

    if vars_ven: catalogo.append({"categoria": "Cálculos de Cobranza y Deuda (ven.)", "variables": vars_ven})

    # 2. Información de Venta (v.)
    vars_v = extraer_tags(Venta, "v")
    if vars_v: catalogo.append({"categoria": "Información de Venta y Contrato (v.)", "variables": vars_v})

    # 3. Detalle de Pagos (p.)
    vars_p = extraer_tags(Amortizacion, "p")
    if vars_p: catalogo.append({"categoria": "Detalle de Pagos y Mensualidad (p.)", "variables": vars_p})

    # 4. Datos Formateados (cl.)
    vars_cl = procesar_manual([
        "{cl.unidad}", 
        "{cl.monto}", 
        "{cl.monto_a_pagar}", 
        "{cl.cliente}", 
        "{cl.num}", 
        "{cl.fecha}", 
        "{cl.fecha_pago}", 
        "{cl.dias_para_pago}", 
        "{cl.concepto}", 
        "{cl.proyecto}"
    ])

    if vars_cl: catalogo.append({"categoria": "Datos Formateados para el Cliente (cl.)", "variables": vars_cl})

    # 5. Control (sys.)
    vars_sys = procesar_manual(["{sys.etapa_activa}", "{sys.bloqueo_motivo}"])
    if vars_sys: catalogo.append({"categoria": "Variables de Control y Bloqueo (sys.)", "variables": vars_sys})

    # 6. Datos Personales de Integrantes (Solo si existen)
    for i in range(1, 7):
        vars_ci = extraer_tags(Cliente, f"c{i}")
        if vars_ci:
            catalogo.append({"categoria": f"Datos Personales del Integrante {i} (c{i}.)", "variables": vars_ci})

    # 7. Gestión de Integrantes (Solo si existen)
    for i in range(1, 7):
        vars_gi = extraer_tags(GestionClientes, f"g{i}")
        if vars_gi:
            catalogo.append({"categoria": f"Switches y Gestión del Integrante {i} (g{i}.)", "variables": vars_gi})

    # 8. Etiquetas Jurídico
    etiquetas_juridico = [
        "{j.cliente}",
        "{j.coopropietario_1}",
        "{j.coopropietario_2}",
        "{j.coopropietario_3}",
        "{j.coopropietario_4}",
        "{j.coopropietario_5}",
        "{j.numero_comercial}",
        "{j.numero_registral}",
        "{j.precio_final_letra}",
        "{j.apartado_letra}",
        "{j.enganche_letra}",
        "{j.monto_sin_interes_letra}",
        "{j.referencia_lote}",
        "{j.saldo_final}",
        "{j.saldo_final_letra}",
        "{j.fecha_mensualidades}",
        "{j.fecha_hoy_escrita}",
        "{j.fecha_hoy_escrita_Mayusculas}",
        "{j.primera_mensualidad}",
        "{j.primer_enganche}",
        "{j.monto_parcialidad}",
        "{j.numero_de_pagos}",
        "{j.porcentaje}",
        "{j.pena}",
        "{j.pena_2}",
        "{j.paginacion}",
        "{j.total_paginas}"
    ]

    if flat_data:
        # Estas etiquetas pueden llegar vacias y aun asi deben verse en el diccionario.
        etiquetas_juridico_siempre_visibles = {"{j.paginacion}", "{j.total_paginas}"}
        vars_f = []
        for tag in etiquetas_juridico:
            valor = flat_data.get(tag)
            if tag in etiquetas_juridico_siempre_visibles:
                vars_f.append({"tag": tag, "valor": "" if valor in [None, "None", "NULL"] else valor})
                continue

            if valor not in [None, "", "None", "NULL"]:
                valor_formateado = aplicar_formato_etiqueta(tag, valor)
                vars_f.append({"tag": tag, "valor": valor_formateado})
    else:
        vars_f = etiquetas_juridico

    if vars_f: 
        catalogo.append({"categoria": "Etiquetas Jurídico", "variables": vars_f })

    etiquetas_tabla_amortizaciones = [
        "{pagos.numero}",
        "{pagos.fecha}",
        "{pagos.capital}",
        "{pagos.enganche}",
        "{pagos.total}",
        "{pagos.saldo_capital}",
        "{totales.suma_capital}",
        "{totales.saldo_enganche}",
        "{totales.suma_total}",
    ]

    if flat_data:
        # Estas etiquetas se calculan durante la generacion de documentos; en diccionario deben verse aunque esten vacias.
        vars_TA = [
            {"tag": tag, "valor": flat_data.get(tag, "")}
            for tag in etiquetas_tabla_amortizaciones
        ]
    else:
        vars_TA = etiquetas_tabla_amortizaciones

    if vars_TA:
        catalogo.append({"categoria": "Etiquetas para Tabla de amortizaciónes", "variables": vars_TA})


    logger.info("[DATOS_KOMUNAH] Salida get_komunah_diccionario_maestro | categorias=%s", len(catalogo))

    return catalogo

def set_email_komunah_marketing(client_id: str, estado: bool, db: Session):
    """AFECTA TOdo: Apaga el permiso para todos los folios de este cliente"""
    logger.info("[DATOS_KOMUNAH] Entrada set_email_komunah_marketing | client_id=%s | estado=%s", client_id, estado)
    filas = db.query(GestionClientes).filter(
        GestionClientes.client_id == client_id
    ).update({"permite_marketing_email": estado})
    db.commit()
    logger.info("[DATOS_KOMUNAH] Salida set_email_komunah_marketing | filas=%s", filas)
    return True

def set_wa_komunah_marketing(client_id: str, estado: bool, db: Session):
    """AFECTA TOdo: Apaga WhatsApp para todos los folios de este cliente"""
    logger.info("[DATOS_KOMUNAH] Entrada set_wa_komunah_marketing | client_id=%s | estado=%s", client_id, estado)
    filas = db.query(GestionClientes).filter(
        GestionClientes.client_id == client_id
    ).update({"permite_marketing_whatsapp": estado})
    db.commit()
    logger.info("[DATOS_KOMUNAH] Salida set_wa_komunah_marketing | filas=%s", filas)
    return True

def set_email_komunah_lote(client_id: str, folio: str, estado: bool, db: Session):
    """ Solo apaga el permiso para ESTE folio específico"""
    logger.info("[DATOS_KOMUNAH] Entrada set_email_komunah_lote | client_id=%s | folio=%s | estado=%s", client_id, folio, estado)
    filas = db.query(GestionClientes).filter(
        GestionClientes.client_id == client_id,
        GestionClientes.folio == folio
    ).update({"permite_email_lote": estado})
    db.commit()
    logger.info("[DATOS_KOMUNAH] Salida set_email_komunah_lote | filas=%s", filas)
    return True

def set_wa_komunah_lote(client_id: str, folio: str, estado: bool, db: Session):
    """Solo apaga WhatsApp para ESTE folio específico"""
    logger.info("[DATOS_KOMUNAH] Entrada set_wa_komunah_lote | client_id=%s | folio=%s | estado=%s", client_id, folio, estado)
    filas = db.query(GestionClientes).filter(
        GestionClientes.client_id == client_id,
        GestionClientes.folio == folio
    ).update({"permite_whatsapp_lote": estado})
    db.commit()
    logger.info("[DATOS_KOMUNAH] Salida set_wa_komunah_lote | filas=%s", filas)
    return True


def actualizar_switches_etapas(cambios: dict, db: Session):
    """
    Actualiza el switch individual de cada ETAPA usando su ID.
    """
    logger.info("[DATOS_KOMUNAH] Entrada actualizar_switches_etapas | cambios=%s", len(cambios or {}))
    for id_etapa, nuevo_estado in cambios.items():
        db.query(ConfigEtapa).filter(ConfigEtapa.id == id_etapa).update(
            {"etapa_activo": nuevo_estado} # <--- NOMBRE ACTUALIZADO
        )
    db.commit()
    logger.info("[DATOS_KOMUNAH] Salida actualizar_switches_etapas | cambios_aplicados=%s", len(cambios or {}))
    return True

def actualizar_switches_proyecto(nombres_proyectos: List[str], nuevo_estado: bool, db: Session):
    """
    Aplica el valor del selector (True/False) a la columna 'proyecto_activo' 
    para todos los proyectos recibidos en la lista.
    """
    logger.info("[DATOS_KOMUNAH] Entrada actualizar_switches_proyecto | proyectos=%s | estado=%s", len(nombres_proyectos or []), nuevo_estado)
    filas = db.query(ConfigEtapa).filter(ConfigEtapa.proyecto.in_(nombres_proyectos)).update(
        {"proyecto_activo": nuevo_estado}, 
        synchronize_session=False
    )
    db.commit()
    logger.info("[DATOS_KOMUNAH] Salida actualizar_switches_proyecto | filas=%s", filas)
    return True     


def get_estado_etapas_komunah(db: Session):
    logger.info("[DATOS_KOMUNAH] Entrada get_estado_etapas_komunah")
    resultados = db.query(ConfigEtapa).all()
    
    def to_bool(val):
        if val is None: 
            return False
        
        # 1. Limpiamos espacios y pasamos a string
        str_val = str(val).strip().lower()
        
        # 2. Casos explícitos de "falsedad"
        if str_val in ("false", "none", "", "0", "0.0", "0.0000"):
            return False
            
        # 3. Intentamos conversión numérica por si viene como "0.0000"
        try:
            return bool(float(str_val))
        except (ValueError, TypeError):
            # Si es texto puro que no es "0", devolvemos True
            return True

    salida = [
        {
            "id": r.id,
            "proyecto": r.proyecto,
            "etapa": r.etapa,
            "etapa_activo": to_bool(r.etapa_activo),   # <--- Ahora sí será False
            "proyecto_activo": to_bool(r.proyecto_activo), 
            "total_folios": int(float(r.total_folios or 0))
        } for r in resultados
    ]
    logger.info("[DATOS_KOMUNAH] Salida get_estado_etapas_komunah | etapas=%s", len(salida))
    return salida

def get_folios_dinamico_komunah(clusters: List[str], pipeline_status: List[str], db: Session):
    """
    Busca folios únicos filtrando por Etapa y/o Estatus de Pipeline.
    Usa .distinct() para evitar mandar correos repetidos por cada deuda.
    """
    
    logger.info(
        "[DATOS_KOMUNAH] Entrada get_folios_dinamico_komunah | clusters_raw=%s | pipeline_raw=%s",
        clusters,
        pipeline_status,
    )

    clusters_limpios = _normalizar_lista_entrada(clusters)
    pipeline_limpios = _normalizar_lista_entrada(pipeline_status)

    logger.info(
        "[DATOS_KOMUNAH] Filtros normalizados get_folios_dinamico_komunah | clusters=%s | pipeline=%s",
        clusters_limpios,
        pipeline_limpios,
    )

    # IMPORTANTE: Usamos .distinct() para que el folio 87 solo salga UNA vez
    query = db.query(Venta.folio).distinct()
    
    estados_prohibidos = ["cancelado", "expirado"]
    query = query.filter(func.lower(Venta.estado_expediente).notin_(estados_prohibidos))

    # Si hay clusters, filtramos en Ventas
    if clusters_limpios and len(clusters_limpios) > 0:
        query = query.filter(Venta.etapa.in_(clusters_limpios))

    # Si hay pipeline_status, filtramos con case-insensitivity
    if pipeline_limpios and len(pipeline_limpios) > 0:
        pipeline_status_lower = [s.lower().strip() for s in pipeline_limpios]
        query = query.filter(func.lower(Venta.estado_expediente).in_(pipeline_status_lower))

    registros = query.all()

    # Limpiamos los resultados para devolver solo la lista de strings
    resultado = [str(row.folio) for row in registros]
    logger.info("[DATOS_KOMUNAH] Salida get_folios_dinamico_komunah | folios=%s", len(resultado))
    return resultado