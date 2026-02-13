from datetime import date, datetime

def encontrar_pago_actual(lista_pagos):
    """
    Dada una lista de amortizaciones, encuentra el pago próximo a vencer o vencido recientemente.
    """
    hoy = date.today()
    pago_actual = None
    
    if not lista_pagos:
        return None

    for p in lista_pagos:
        if not p.date: continue
        
        fecha_pago = p.date
        if isinstance(fecha_pago, str):
            try: 
                fecha_pago = datetime.strptime(fecha_pago, "%Y-%m-%d").date()
            except: 
                continue
        
        if fecha_pago >= hoy:
            pago_actual = p
            break
            
  
    if not pago_actual: 
        pago_actual = lista_pagos[-1]
        
    return pago_actual

def encontrar_pago_actual_mes(lista_pagos):
    hoy = date.today()

    for p in lista_pagos:
        if not p.date:
            continue

        fecha = p.date
        if isinstance(fecha, str):
            try:
                fecha = datetime.strptime(fecha, "%Y-%m-%d").date()
            except:
                continue

        # Coincidencia estricta por mes y año
        if fecha.month == hoy.month and fecha.year == hoy.year:
            return p

    # Si no hay del mes actual, devolver None
    return None