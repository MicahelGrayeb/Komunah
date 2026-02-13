import os
import requests
from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session
from dotenv import load_dotenv

from ..schemas import EmailManualSchema
from ..database import get_db 
from ..models import Venta

load_dotenv()

router = APIRouter(
    prefix="/notificaciones-estaticas", 
    tags=["Notificaciones Estáticas"]
)

MAILERSEND_API_KEY = os.getenv("MAILERSEND_API_KEY")
RESPOND_IO_TOKEN = os.getenv("RESPOND_IO_TOKEN")
RESPOND_IO_CHANNEL_ID = os.getenv("RESPOND_IO_CHANNEL_ID")

def _enviar_whatsapp_respond_io(telefono, mensaje: str):
    """
    Envía mensaje vía Respond.io usando el identificador phone:+60...
    Asegura el formato +521 para móviles en México.
    """
    if not RESPOND_IO_TOKEN or telefono is None:
        return None
    
    tel_str = str(telefono).strip()
    
    
    if tel_str.startswith('52') and not tel_str.startswith('521'):
        tel_final = f"+521{tel_str[2:]}"
    elif not tel_str.startswith('52'):
        tel_final = f"+521{tel_str}"
    else:
        tel_final = f"+{tel_str}"


    url = f"https://api.respond.io/v2/contact/phone:{tel_final}/message"
    
    headers = {
        "Authorization": f"Bearer {RESPOND_IO_TOKEN}",
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    payload = {
        "channelId": int(RESPOND_IO_CHANNEL_ID) if RESPOND_IO_CHANNEL_ID else None,
        "message": {
            "type": "text",
            "text": mensaje
        }
    }

    try:
        response = requests.post(url, json=payload, headers=headers, timeout=10)
      
        print(f"DEBUG RESPOND.IO - Status: {response.status_code}, Response: {response.text}")
        return response
    except Exception as e:
        print(f"ERROR DE CONEXIÓN A RESPOND.IO: {e}")
        return None

@router.post("/enviar")
def enviar_notificacion_estatica(datos: EmailManualSchema, db: Session = Depends(get_db)):
    if not MAILERSEND_API_KEY:
        raise HTTPException(status_code=500, detail="Falta MAILERSEND_API_KEY")

  
    email_ref = datos.para[0] if datos.para else None
    venta = db.query(Venta).filter(Venta.correo_electronico == email_ref).first() if email_ref else None

    html_final = datos.contenido_html
    if venta:
  
        unidad = str(getattr(venta, 'numero', "S/N") or "S/N")
        monto_raw = getattr(venta, 'precio_final', 0)
        monto_fmt = f"{float(monto_raw):,.2f}" if monto_raw else "0.00"
        
        reemplazos = {
            "{unidad}": unidad,
            "{monto}": monto_fmt,
            "{cliente}": str(getattr(venta, 'cliente', "Cliente")),
            "{num}": str(getattr(venta, 'folio', "---")),
            "{fecha}": str(getattr(venta, 'fecha_de_venta', "Pendiente")),
            "{concepto}": "Recordatorio de Pago"
        }
        for clave, valor in reemplazos.items():
            html_final = html_final.replace(clave, valor)
            
    url_mail = "https://api.mailersend.com/v1/email"
    headers_mail = {"Authorization": f"Bearer {MAILERSEND_API_KEY}", "Content-Type": "application/json"}
    
    payload_mail = {
        "from": {"email": datos.remitente, "name": "Finanzas Komunah"},
        "to": [{"email": email} for email in datos.para],
        "subject": datos.asunto,
        "html": html_final
    }

    if datos.cc:
        payload_mail["cc"] = [{"email": email} for email in datos.cc]
    if datos.cco:
        payload_mail["bcc"] = [{"email": email} for email in datos.cco]

    try:
        res_mail = requests.post(url_mail, headers=headers_mail, json=payload_mail, timeout=10)
        
        if 200 <= res_mail.status_code < 300:
            if venta and getattr(venta, 'telefono', None):
                nombre_ws = getattr(venta, 'cliente', "Cliente")
                unidad_ws = getattr(venta, 'numero', "S/N")
                msg_ws = f"Hola {nombre_ws}, enviamos un recordatorio de pago de la unidad {unidad_ws} a su correo. Revíselo."
                _enviar_whatsapp_respond_io(venta.telefono, msg_ws)

            return {"estado": "exitoso", "mensaje": "Correo enviado."}
        else:
            raise HTTPException(status_code=400, detail=f"Error MailerSend: {res_mail.text}")

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error: {str(e)}")