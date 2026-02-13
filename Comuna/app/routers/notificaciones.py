import os
import requests
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from dotenv import load_dotenv
from ..database import get_db
from ..models import Venta, Amortizacion
from ..schemas import EmailManualSchema
from ..services.pagos_utils import encontrar_pago_actual

load_dotenv()

router = APIRouter(
    prefix="/notificaciones",
    tags=["Notificaciones Manuales"]
)


MAILERSEND_API_KEY = os.getenv("MAILERSEND_API_KEY")

@router.post("/enviar")
def enviar_notificacion_directa(
    datos: EmailManualSchema, 
    db: Session = Depends(get_db)
):
    """
    Envía un correo con HTML personalizado desde el frontend.
    Intenta rellenar variables dinámicas ({cliente}, {monto}) si encuentra al usuario en la BD.
    """
    if not MAILERSEND_API_KEY:
        raise HTTPException(status_code=500, detail="Falta MAILERSEND_API_KEY en .env")

    
    email_destino = datos.para[0] if datos.para else None
    venta = None
    
    if email_destino:
        venta = db.query(Venta).filter(Venta.correo_electronico == email_destino).first()

    
    vars_db = {
        "{cliente}": "Cliente",
        "{unidad}": "S/N",
        "{proyecto}": "Komunah",
        "{monto}": "$0.00",
        "{fecha}": "-",
        "{num}": "-",
        "{concepto}": "Pago"
    }


    if venta:
        vars_db["{cliente}"] = venta.cliente or "Cliente"
        vars_db["{unidad}"] = venta.numero or "S/N"
        vars_db["{proyecto}"] = venta.desarrollo or "Komunah"

        pagos = db.query(Amortizacion).filter(Amortizacion.folder_id == venta.folio)\
                  .order_by(Amortizacion.date.asc()).all()
        
        
        pago_actual = encontrar_pago_actual(pagos)

        if pago_actual:
            vars_db["{monto}"] = f"${pago_actual.total:,.2f}" if pago_actual.total else "$0.00"
            vars_db["{fecha}"] = str(pago_actual.date)
            vars_db["{num}"] = str(pago_actual.number)
            vars_db["{concepto}"] = pago_actual.concept or "Pago"

    html_final = datos.contenido_html
    for clave, valor in vars_db.items():
        html_final = html_final.replace(clave, str(valor))

 
    url = "https://api.mailersend.com/v1/email"
    headers = {
        "Authorization": f"Bearer {MAILERSEND_API_KEY}",
        "Content-Type": "application/json"
    }

    payload = {
        "from": {
            "email": datos.remitente,
            "name": "Grupo Komunah"
        },
        "to": [{"email": email} for email in datos.para],
        "subject": datos.asunto,
        "html": html_final
    }

    # Agregamos CC y BCC solo si existen 
    if datos.cc:
        payload["cc"] = [{"email": email} for email in datos.cc]
    if datos.cco:
        payload["bcc"] = [{"email": email} for email in datos.cco]

    try:
        response = requests.post(url, headers=headers, json=payload)
        
        if 200 <= response.status_code < 300:
            return {
                "estado": "enviado", 
                "encontrado_en_bdd": bool(venta),
                "mensaje": "Correo enviado exitosamente"
            }
        else:
            print(f"Error MailerSend: {response.text}")
            raise HTTPException(status_code=400, detail=f"Proveedor de correo rechazó el envío: {response.text}")
            
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error interno: {str(e)}")