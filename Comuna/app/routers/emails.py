from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from datetime import date, datetime


from ..database import get_db
from ..models import Venta, Amortizacion
from ..services.email_utils import (
    generar_csv_b64_final, 
    obtener_html_komunah_final, 
    enviar_correo_mailer_send
)
from ..services.pagos_utils import encontrar_pago_actual

router = APIRouter(prefix="/emails", tags=["Envío de Correos"])

# --- FUNCIONES AUXILIARES INTERNAS ---
def _obtener_datos_cliente(folder_id: int, db: Session):
    venta = db.query(Venta).filter(Venta.folio == folder_id).first()
    pagos = db.query(Amortizacion).filter(Amortizacion.folder_id == folder_id).order_by(Amortizacion.date.asc()).all()
    if not venta:
        raise HTTPException(status_code=404, detail="Cliente/Venta no encontrada")
    if not pagos:
        raise HTTPException(status_code=404, detail="No hay pagos registrados para este folio")
    return venta, pagos

def _preparar_contenido(venta, pagos):
    unidad = venta.numero if venta.numero else "S/N"
    pago_principal = encontrar_pago_actual(pagos) 
    
    html_content = obtener_html_komunah_final(unidad, pago_principal)
    csv_b64 = generar_csv_b64_final(pagos)
    nombre_csv = f"Tabla_Amortizacion_{unidad}.csv"
    
    return unidad, html_content, csv_b64, nombre_csv



# 1. ENVIAR AMORTIZACIÓN (Al correo registrado del cliente)
@router.post("/enviar-amortizacion/{folder_id}")
def enviar_amortizacion_cliente(folder_id: int, db: Session = Depends(get_db)):
    venta, pagos = _obtener_datos_cliente(folder_id, db)
    
    if not venta.correo_electronico:
        raise HTTPException(status_code=400, detail="El cliente no tiene correo registrado")

    unidad, html, csv, nombre_csv = _preparar_contenido(venta, pagos)
    nombre_cliente = venta.cliente if venta.cliente else "Cliente"

    response = enviar_correo_mailer_send(
        destinatario_email=venta.correo_electronico,
        destinatario_nombre=nombre_cliente,
        asunto=f"Estado de Cuenta: {venta.desarrollo or 'Proyecto'} - {unidad}",
        html_content=html,
        csv_b64=csv,
        nombre_archivo_csv=nombre_csv
    )
    return {"mensaje": "Correo enviado", "status": response.status_code}

# 2. ENVIAR AMORTIZACIÓN PERSONALIZADO (A un correo manual)
@router.post("/enviar-amortizacion-personalizado/{folder_id}")
def enviar_amortizacion_personalizado(folder_id: int, email_destino: str, db: Session = Depends(get_db)):
    venta, pagos = _obtener_datos_cliente(folder_id, db)
    
    unidad, html, csv, nombre_csv = _preparar_contenido(venta, pagos)
    nombre_cliente = venta.cliente if venta.cliente else "Cliente"

    response = enviar_correo_mailer_send(
        destinatario_email=email_destino, 
        destinatario_nombre=nombre_cliente,
        asunto=f"Estado de Cuenta: {venta.desarrollo or 'Proyecto'} - {unidad}",
        html_content=html,
        csv_b64=csv,
        nombre_archivo_csv=nombre_csv
    )
    return {
        "mensaje": f"Datos del ID {folder_id} enviados a {email_destino}", 
        "status": response.status_code
    }

# 3. ENVIAR RECORDATORIO FINAL (A un correo manual)
@router.post("/enviar-recordatorio-final/{folder_id}")
def enviar_recordatorio_final(folder_id: int, email_destino: str, db: Session = Depends(get_db)):

    venta, pagos = _obtener_datos_cliente(folder_id, db)
    
    unidad, html, csv, nombre_csv = _preparar_contenido(venta, pagos)
    nombre_cliente = venta.cliente if venta.cliente else "Cliente"

    response = enviar_correo_mailer_send(
        destinatario_email=email_destino,
        destinatario_nombre=nombre_cliente,
        asunto=f"Recordatorio de Pago - Unidad {unidad}",
        html_content=html,
        csv_b64=csv,
        nombre_archivo_csv=nombre_csv
    )
    return {"mensaje": "Correo enviado", "destinatario": email_destino, "status": response.status_code}

# 4. ENVIAR RECORDATORIO CLIENTE BD (Al correo registrado)
@router.post("/enviar-recordatorio-cliente-bd/{folder_id}")
def enviar_recordatorio_cliente_bd(folder_id: int, db: Session = Depends(get_db)):
    venta, pagos = _obtener_datos_cliente(folder_id, db)
    
    if not venta.correo_electronico:
        raise HTTPException(status_code=400, detail="El cliente NO tiene correo registrado")

    unidad, html, csv, nombre_csv = _preparar_contenido(venta, pagos)
    nombre_cliente = venta.cliente if venta.cliente else "Estimado Cliente"

    response = enviar_correo_mailer_send(
        destinatario_email=venta.correo_electronico,
        destinatario_nombre=nombre_cliente,
        asunto=f"Recordatorio de Pago - Unidad {unidad}",
        html_content=html,
        csv_b64=csv,
        nombre_archivo_csv=nombre_csv
    )
    return {
        "mensaje": "Correo enviado al cliente",
        "email_enviado": venta.correo_electronico,
        "status": response.status_code
    }