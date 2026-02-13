from sqlalchemy import text
from ..models import Venta, Cliente, Amortizacion, GestionClientes, ConfigEtapa, Pago
from ..services.pagos_utils import encontrar_pago_actual, encontrar_pago_actual_mes
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import Session, sessionmaker
from typing import List
from datetime import datetime
from zoneinfo import ZoneInfo
def get_komunah_data(folio_ref: str, db: Session):
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
            "{cl.concepto}": "",
            "{cl.proyecto}": ""
        })
        return data

        
    venta = db.query(Venta).filter(Venta.folio == folio_ref).first()
    if not venta:
        return {}

    data = {}
    
    conf_cluster = db.query(ConfigEtapa).filter(ConfigEtapa.etapa == venta.etapa).first()
    
    
    etapa_permiso = "1"
    motivo_bloqueo = None 

    if not conf_cluster:
        etapa_permiso = "0"
        motivo_bloqueo = f"CONFIG_FALTANTE: Etapa '{venta.etapa}' no existe en SQL"
    elif conf_cluster.proyecto_activo == False:
        etapa_permiso = "0"
        motivo_bloqueo = f"PROYECTO_OFF: Desarrollo '{conf_cluster.proyecto}' desactivado"
    elif conf_cluster.etapa_activo == False:
        etapa_permiso = "0"
        motivo_bloqueo = f"ETAPA_OFF: Cluster '{conf_cluster.etapa}' desactivado"
    
    data["{sys.etapa_activa}"] = etapa_permiso
    if motivo_bloqueo:
        data["{sys.bloqueo_motivo}"] = motivo_bloqueo
    

    for col in inspect(venta).mapper.column_attrs:
        val = getattr(venta, col.key)
        if val is not None and str(val).strip() not in ["", "None", "NULL"]:
        
            data[f"{{v.{col.key.lower()}}}"] = str(val)


    amortizaciones = db.query(Amortizacion).filter(Amortizacion.folder_id == folio_ref)\
                    .order_by(Amortizacion.date.asc()).all()
    
    p_act = encontrar_pago_actual_mes(amortizaciones)
    
    # --- CÁLCULO: Prefijo cl. ---
    monto_val = 0.0
    pagado_parcial = 0.0
    if p_act and hasattr(p_act, 'total') and p_act.total is not None:
        monto_val = float(p_act.total)
        p_v_hoy = db.query(Pago).filter(
            Pago.folio_venta == int(folio_ref), 
            Pago.numero_pago == p_act.number,
            Pago.estatus == 'active' 
        ).first()
        if p_v_hoy:
            pagado_parcial = float(p_v_hoy.monto_pagado or 0)
    saldo_actual_vigente = monto_val - pagado_parcial
    # Mapeo manual con etiquetas estandarizadas
    data.update({
        "{cl.unidad}": str(getattr(venta, 'numero', "")),
        "{cl.monto}": f"${monto_val:,.2f}",
        "{cl.monto_a_pagar}": f"${saldo_actual_vigente:,.2f}",
        "{cl.cliente}": str(getattr(venta, 'cliente', "")),
        "{cl.num}": str(getattr(p_act, 'number', "")) if p_act else "",
        "{cl.fecha}": str(getattr(p_act, 'date', "")) if p_act else "",
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

        cliente_db = db.query(Cliente).filter(Cliente.client_id == c_id_limpio).first()
        if cliente_db:
            prefijo = f"c{i}." 
            for col in inspect(cliente_db).mapper.column_attrs:
                val_c = getattr(cliente_db, col.key)
                if val_c is not None: 
                    data[f"{{{prefijo}{col.key.lower()}}}"] = str(val_c)
        
        gestion_db = db.query(GestionClientes).filter(
            GestionClientes.folio == folio_ref,
            GestionClientes.client_id == c_id_limpio
        ).first()

        if gestion_db:
            prefijo_g = f"g{i}."
            for col in inspect(gestion_db).mapper.column_attrs:
                val_g = getattr(gestion_db, col.key)
                
                if isinstance(val_g, bool):
                    val_g = "1" if val_g else "0"
                
                if val_g is not None:
                    data[f"{{{prefijo_g}{col.key.lower()}}}"] = str(val_g)       
    hoy_dt = datetime.now(ZoneInfo("America/Mexico_City")) 
    hoy_str = hoy_dt.strftime('%Y-%m-%d')
    from ..models import Cartera
    # Buscamos el resumen oficial en la tabla Cartera para este folio
    cv = db.query(Cartera).filter(Cartera.folio == int(folio_ref)).first()

    # Si el CRM dice que debe, jalamos sus totales; si no, es 0
    ven_meses_atraso = int(float(cv.parcialidades_vencidas or 0)) if cv else 0
    ven_saldo_vencido = float(cv.total_vencido_sin_pen or 0) if cv else 0.0
    saldo_total_vencido = float(cv.total_vencido_con_pen or 0) if cv else 0.0
    ven_penalizacion_acumulada = saldo_total_vencido - ven_saldo_vencido

    ven_monto_mes_puro = 0.0       
    ven_monto_mes_pendiente = 0.0  
    ven_penalizacion_mes_actual = 0.0
    fecha_mas_antigua = None


    for amt in amortizaciones:
        p_v = db.query(Pago).filter(
            Pago.folio_venta == int(folio_ref), 
            Pago.numero_pago == amt.number,
            Pago.estatus == 'active' 
        ).first()
        
        pagado = float(p_v.monto_pagado or 0) if p_v else 0.0
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
            ven_monto_mes_pendiente = (total_deberia - pagado)


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
    
    return data

def get_folios_a_notificar_komunah(db: Session, fecha: str):
    """
    PARA EL RECORDATORIO AMISTOSO:
    Busca folios que vencen en 'fecha', pero EXCLUYE a los que ya deben
    mensualidades anteriores (porque esos van para cobranza).
    """
    query = text("""
        SELECT DISTINCT a.folder_id 
        FROM amortizaciones a
        LEFT JOIN pagos p ON a.folder_id = p.`Folio de la venta` AND a.number = p.`Número de pago`
        WHERE a.date = :f
        AND (IFNULL(p.`Estatus expediente`, '') != 'Liquidado')
        AND (
            p.`Folio de la venta` IS NULL -- No hay registro de pago
            OR IFNULL(p.`Monto pagado`, 0) < IFNULL(p.`Monto a pagar`, 0) -- Debe dinero
            OR p.Estatus = 'canceled' -- El pago se canceló
        )
        -- EXCLUSIÓN: No debe tener ninguna letra anterior con deuda
        AND NOT EXISTS (
            SELECT 1 FROM amortizaciones a2
            LEFT JOIN pagos p2 ON a2.folder_id = p2.`Folio de la venta` AND a2.number = p2.`Número de pago`
            WHERE a2.folder_id = a.folder_id 
            AND a2.date < a.date
            AND (p2.`Folio de la venta` IS NULL OR IFNULL(p2.`Monto pagado`, 0) < IFNULL(p2.`Monto a pagar`, 0))
        )
    """)
    registros = db.execute(query, {"f": fecha}).fetchall()
    return [row[0] for row in registros]

def get_folios_deudores_komunah(db: Session, fecha: str):
    """
    COBRANZA SELECCIONADA: 
    Busca folios que vencen en 'fecha', pero que ya traen atrasos de meses anteriores.
    """
    query = text("""
        SELECT DISTINCT a.folder_id 
        FROM amortizaciones a
        WHERE a.date = :f  
        AND EXISTS (
            SELECT 1 FROM amortizaciones a2
            LEFT JOIN pagos p2 ON a2.folder_id = p2.`Folio de la venta` AND a2.number = p2.`Número de pago`
            WHERE a2.folder_id = a.folder_id 
            AND a2.date < a.date
            AND (p2.`Folio de la venta` IS NULL OR IFNULL(p2.`Monto pagado`, 0) < IFNULL(p2.`Monto a pagar`, 0))
        )
    """)
    registros = db.execute(query, {"f": fecha}).fetchall()
    return [row[0] for row in registros]

def get_komunah_diccionario_maestro(flat_data: dict = None):
    """
    Escanea las tablas SQL y devuelve el catálogo.
    Si hay flat_data, FILTRA lo vacío y devuelve {tag, valor}.
    """
    from sqlalchemy.inspection import inspect

    # Helper para extraer tags de SQL y filtrar si no hay data
    def extraer_tags(modelo, prefijo):
        resultado = []
        for col in inspect(modelo).mapper.column_attrs:
            tag = f"{{{prefijo}.{col.key.lower()}}}"
            if flat_data:
                valor = flat_data.get(tag)
                # SOLO añadimos si el valor no es None ni vacío
                if valor not in [None, "", "None", "NULL"]:
                    resultado.append({"tag": tag, "valor": valor})
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
                    resultado.append({"tag": t, "valor": valor})
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
    vars_cl = procesar_manual(["{cl.unidad}", "{cl.monto}", "{cl.monto_a_pagar}", "{cl.cliente}", "{cl.num}", "{cl.fecha}", "{cl.concepto}", "{cl.proyecto}"])
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

    return catalogo

def set_email_komunah_marketing(client_id: str, estado: bool, db: Session):
    """AFECTA TOdo: Apaga el permiso para todos los folios de este cliente"""
    db.query(GestionClientes).filter(
        GestionClientes.client_id == client_id
    ).update({"permite_marketing_email": estado})
    db.commit()
    return True

def set_wa_komunah_marketing(client_id: str, estado: bool, db: Session):
    """AFECTA TOdo: Apaga WhatsApp para todos los folios de este cliente"""
    db.query(GestionClientes).filter(
        GestionClientes.client_id == client_id
    ).update({"permite_marketing_whatsapp": estado})
    db.commit()
    return True

def set_email_komunah_lote(client_id: str, folio: str, estado: bool, db: Session):
    """ Solo apaga el permiso para ESTE folio específico"""
    db.query(GestionClientes).filter(
        GestionClientes.client_id == client_id,
        GestionClientes.folio == folio
    ).update({"permite_email_lote": estado})
    db.commit()
    return True

def set_wa_komunah_lote(client_id: str, folio: str, estado: bool, db: Session):
    """Solo apaga WhatsApp para ESTE folio específico"""
    db.query(GestionClientes).filter(
        GestionClientes.client_id == client_id,
        GestionClientes.folio == folio
    ).update({"permite_whatsapp_lote": estado})
    db.commit()
    return True


def actualizar_switches_etapas(cambios: dict, db: Session):
    """
    Actualiza el switch individual de cada ETAPA usando su ID.
    """
    for id_etapa, nuevo_estado in cambios.items():
        db.query(ConfigEtapa).filter(ConfigEtapa.id == id_etapa).update(
            {"etapa_activo": nuevo_estado} # <--- NOMBRE ACTUALIZADO
        )
    db.commit()
    return True

def actualizar_switches_proyecto(nombres_proyectos: List[str], nuevo_estado: bool, db: Session):
    """
    Aplica el valor del selector (True/False) a la columna 'proyecto_activo' 
    para todos los proyectos recibidos en la lista.
    """
    db.query(ConfigEtapa).filter(ConfigEtapa.proyecto.in_(nombres_proyectos)).update(
        {"proyecto_activo": nuevo_estado}, 
        synchronize_session=False
    )
    db.commit()
    return True     


def get_estado_etapas_komunah(db: Session):
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

    return [
        {
            "id": r.id,
            "proyecto": r.proyecto,
            "etapa": r.etapa,
            "etapa_activo": to_bool(r.etapa_activo),   # <--- Ahora sí será False
            "proyecto_activo": to_bool(r.proyecto_activo), 
            "total_folios": int(float(r.total_folios or 0))
        } for r in resultados
    ]