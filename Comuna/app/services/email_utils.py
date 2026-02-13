import io
import csv
import base64
import requests
import os
from datetime import date, datetime
from dotenv import load_dotenv


load_dotenv()


MAILERSEND_API_KEY = os.getenv("MAILERSEND_API_KEY")
MAILERSEND_SENDER = os.getenv("MAILERSEND_SENDER", "info@techmaleonmx.com")

def generar_csv_b64_final(lista_pagos):
    """Genera el CSV en memoria y devuelve el string Base64."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(['Numero', 'Fecha', 'Concepto', 'Capital', 'Interes', 'Total'])
    
    for p in lista_pagos:
        fecha_str = p.date if p.date else "Sin Fecha"
        writer.writerow([
            p.number, fecha_str, p.concept, 
            p.capital, p.interest, p.total
        ])
    
    csv_bytes = output.getvalue().encode('utf-8')
    return base64.b64encode(csv_bytes).decode('utf-8')

def obtener_html_komunah_final(unidad, pago):
    """Genera el HTML del correo."""
    monto = f"${pago.total:,.2f}" if pago and pago.total else "$0.00"
    fecha = pago.date if pago and pago.date else "Sin fecha"
    num = pago.number if pago and pago.number else "-"
    concepto = pago.concept if pago and pago.concept else "Pago"
    
    # HTML COMPLETO
    html = f"""
    <table border="0" width="100%" cellspacing="0" cellpadding="0" bgcolor="#ffffff">
    <tbody>
    <tr>
    <td style="padding: 40px 10px;" align="center">
    <table class="container" style="background-color: #efeaf2; margin: 0 auto; border: 1px solid #e0e0e0; border-radius: 8px;" border="0" width="600" cellspacing="0" cellpadding="0" bgcolor="#efeaf2">
    <tbody>
    <tr>
    <td style="padding-bottom: 30px; padding-top: 30px;" align="center"><img style="display: block; width: 250px; max-width: 100%;" src="https://grupokomunah.mx/assets/logos/grupokomunah-negro.png" alt="Grupo Komunah" width="250" /></td>
    </tr>
    <tr>
    <td>
    <table border="0" width="100%" cellspacing="0" cellpadding="0">
    <tbody>
    <tr>
    <td class="column" style="vertical-align: top;" valign="top" width="50%">
    <table border="0" width="100%" cellspacing="0" cellpadding="0">
    <tbody>
    <tr>
    <td class="text-center-mobile" style="padding-bottom: 10px; padding-left: 20px;" align="left">
    <h2 style="margin: 0; font-size: 24px; color: #171f36; font-weight: 800; line-height: 1.2;">RECORDATORIO<br />DE PAGO</h2>
    </td>
    </tr>
    <tr>
    <td class="text-center-mobile" style="padding-bottom: 20px; padding-left: 20px;" align="left"><img class="img-center-mobile" style="display: block;" src="https://grupokomunah.mx/assets/img/noti-alerta.png" alt="Alerta" width="100" /></td>
    </tr>
    <tr>
    <td style="padding-top: 10px;" align="center"><img style="display: block;" src="https://grupokomunah.mx/assets/img/phone-creditcard.png" alt="App Pago" width="260" /></td>
    </tr>
    </tbody>
    </table>
    </td>
    <td class="column" style="vertical-align: top;" valign="top" width="50%">
    <table border="0" width="100%" cellspacing="0" cellpadding="0">
    <tbody>
    <tr>
    <td class="mobile-padding-top" style="padding-bottom: 20px;" align="center">
    <div style="font-weight: bold; font-size: 16px; margin-bottom: 5px; color: #171f36;">LOTE / UNIDAD</div>
    <table border="0" cellspacing="0" cellpadding="0">
    <tbody>
    <tr>
    <td style="background-color: #fb647e; padding: 8px 30px; border-radius: 50px; color: #171f36; font-weight: bold; font-size: 18px;" bgcolor="#fb647e">{unidad}</td>
    </tr>
    </tbody>
    </table>
    </td>
    </tr>
    <tr>
    <td style="padding-bottom: 20px; color: #171f36; font-size: 16px; line-height: 1.5;" align="center">
    <strong>Concepto:</strong> {concepto}<br>
    <strong>Pago No:</strong> {num}<br>
    <strong>Fecha Límite:</strong> {fecha}<br>
    <br>
    <span style="font-size: 22px; font-weight: bold;">Monto: {monto} MXN</span>
    </td>
    </tr>
    <tr>
    <td style="padding: 0 20px 30px 20px; color: #171f36; font-size: 14px; line-height: 1.4;" align="center">Pedimos de su apoyo realizando el pago en la cuenta agregada y solicitamos de su apoyo enviando el comprobante de pago a Whatsapp o correo.</td>
    </tr>
    <tr>
    <td align="center">
    <table class="bank-info-box" style="background-color: #fb647e; border-top-left-radius: 40px; border-bottom-left-radius: 40px; border-top-right-radius: 0px; border-bottom-right-radius: 0px;" border="0" width="95%" cellspacing="0" cellpadding="0" bgcolor="#fb647e">
    <tbody>
    <tr>
    <td style="padding: 30px 20px; color: #171f36;" align="center">
    <p style="margin: 0 0 15px 0; font-size: 14px;">BANCO SANTANDER M&Eacute;XICO, S.A.</p>
    <p style="margin: 0 0 5px 0; font-size: 14px;"><strong>N&Uacute;MERO DE CUENTA:</strong><br /><span style="font-size: 16px;">65509718359</span></p>
    <p style="margin: 0 0 15px 0; font-size: 14px;"><strong>CLABE:</strong><br /><span style="font-size: 16px;">014910655097183597</span></p>
    <p style="margin: 0; font-size: 12px; line-height: 1.3;"><strong>NOMBRE:</strong> COMERCIALIZADORA DE<br />TIERRA YUCAT&Aacute;N S.A DE C.V.</p>
    </td>
    </tr>
    </tbody>
    </table>
    </td>
    </tr>
    </tbody>
    </table>
    </td>
    </tr>
    </tbody>
    </table>
    </td>
    </tr>
    <tr>
    <td height="30">&nbsp;</td>
    </tr>
    <tr>
    <td style="background-color: #ffffff; padding: 20px; border-bottom-left-radius: 8px; border-bottom-right-radius: 8px;" bgcolor="#ffffff">
    <table border="0" width="100%" cellspacing="0" cellpadding="0">
    <tbody>
    <tr>
    <td class="column" align="left" valign="middle" width="50%">
    <table class="footer-contact-table" border="0" cellspacing="0" cellpadding="0" align="left">
    <tbody>
    <tr>
    <td style="padding-right: 10px;" valign="middle"><span style="font-size: 24px; color: #fb647e;">📞</span></td>
    <td style="font-size: 13px; color: #171f36; font-weight: bold;" valign="middle">Atenci&oacute;n a clientes<br /><a style="text-decoration: none; color: #171f36;" href="tel:9992562834">999 256 2834</a> / <a style="text-decoration: none; color: #171f36;" href="tel:9994867939">999 486 79 39</a></td>
    </tr>
    </tbody>
    </table>
    </td>
    <td class="column footer-text-center" style="font-size: 12px; color: #171f36; font-weight: bold; line-height: 1.4;" align="right" valign="middle" width="50%"><span class="footer-spacer" style="display: none; font-size: 0; line-height: 0;">&nbsp;</span>
    <div class="hours-block">Lunes a viernes 09:00 am a 06:30 pm<br />S&aacute;bado 09:00 am a 01:30 pm</div>
    </td>
    </tr>
    </tbody>
    </table>
    </td>
    </tr>
    </tbody>
    </table>
    </td>
    </tr>
    </tbody>
    </table>
    """
    return html

def enviar_correo_mailer_send(destinatario_email, destinatario_nombre, asunto, html_content, csv_b64, nombre_archivo_csv):
    """Función genérica para enviar el correo."""
    if not MAILERSEND_API_KEY:
        print("ERROR: No se encontró MAILERSEND_API_KEY en variables de entorno")
       
    
    url = "https://api.mailersend.com/v1/email"
    headers = {
        "Authorization": f"Bearer {MAILERSEND_API_KEY}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "from": {"email": MAILERSEND_SENDER, "name": "Finanzas Komunah"},
        "to": [{"email": destinatario_email, "name": destinatario_nombre}],
        "subject": asunto,
        "html": html_content,
        "attachments": [
            {
                "filename": nombre_archivo_csv,
                "content": csv_b64,
                "disposition": "attachment"
            }
        ]
    }
    
    response = requests.post(url, headers=headers, json=payload)
    return response