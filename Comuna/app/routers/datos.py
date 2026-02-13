from http.client import HTTPException
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from sqlalchemy import text
from typing import Optional, List
from ..services.security import get_current_user, es_admin, es_usuario, es_super_admin

from ..database import get_db

router = APIRouter(prefix="/datos", tags=["Consultas BD"])

@router.get("/clientes")
def listar_clientes(
    anio: Optional[int] = None,
    db: Session = Depends(get_db),
    user: dict = Depends(es_usuario)
):
    try:
        total = db.execute(
            text("SELECT COUNT(*) FROM clientes")
        ).scalar()

        if anio:
            filtro = f"{anio}-%"

            result = db.execute(
                text("""
                    SELECT *
                    FROM clientes
                    WHERE created_at LIKE :f
                """),
                {"f": filtro}
            )
        else:
            result = db.execute(
                text("SELECT * FROM clientes")
            )

        data = [dict(row) for row in result.mappings()]

        return {
            "total": total,   # siempre el total absoluto
            "anio": anio,
            "items": data
        }

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error SQL: {str(e)}")


    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error SQL: {str(e)}")



@router.get("/pagos")
def listar_pagos(db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    result = db.execute(text("SELECT * FROM pagos LIMIT 10000"))
    return [dict(row) for row in result.mappings()]

@router.get("/ventas")
def listar_ventas(anio: Optional[int] = None, db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    try:
        if anio:
    
            filtro = f"{anio}-%" 
            query = text("SELECT * FROM ventas WHERE `FECHA DE INICIO DE OPERACIÓN` LIKE :f")
            result = db.execute(query, {"f": filtro})
        else:

            result = db.execute(text("SELECT * FROM ventas"))
        
        raw_rows = result.mappings().all()
        final_data = []

        for row in raw_rows:
            d = dict(row)
            for k, v in d.items():
                k_up = k.upper()
                
                if 'FECHA' in k_up:
                    if v is None or str(v).strip().upper() in ['NULL', '']:
                        d[k] = "" 
                    else:
                        d[k] = str(v)
                    continue 
                
                numericos = [
                    'FOLIO', 'METROS CUADRADOS', 'M2', 'PRECIO', 
                    'ENGANCHE', 'FINANCIADO', 'APARTADO', 'MONTO', 'FLUJO'
                ]
                
                if any(x in k_up for x in numericos):
                    try:
                        d[k] = float(v) if v not in [None, '', 'NULL'] else 0.0
                    except:
                        d[k] = 0.0
                    continue

                textos_obligatorios = [
                    'NACIMIENTO', 'LUGAR', 'OCUPACIÓN', 'ESTADO CIVIL', 
                    'ESTADO', 'PAÍS', 'CLIENTE', 'TELÉFONO', 'CANAL', 
                    'COORDINADOR', 'GERENTE', 'SUCURSALES', 'RESPONSABLES'
                ]
                
                if any(x in k_up for x in textos_obligatorios):
                    if v is None or str(v).strip().upper() in ['NULL', '']:
                        d[k] = ""
                    else:
                        d[k] = str(v)
            
            final_data.append(d)
            
        return final_data
    except Exception as e:
     
        raise HTTPException(status_code=500, detail=f"Error SQL: {str(e)}")
    

@router.get("/cartera")
def listar_cartera(db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    result = db.execute(text("SELECT * FROM cartera_vencida LIMIT 1500"))
    return [dict(row) for row in result.mappings()]

@router.get("/amortizaciones") 
def listar_amortizaciones(db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    result = db.execute(text("SELECT * FROM amortizaciones LIMIT 1500"))
    return [dict(row) for row in result.mappings()]

@router.get("/antiguedad")
def listar_antiguedad(db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    result = db.execute(text("SELECT * FROM antig_saldos LIMIT 1500"))
    return [dict(row) for row in result.mappings()]


@router.get("/gestion-clientes")
def listar_gestion_clientes(folio: Optional[str] = None, db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    try:
        query_str = "SELECT * FROM notificaciones_gestion_clientes"
        params = {}
        
        if folio:
            query_str += " WHERE folio = :f"
            params["f"] = folio

        result = db.execute(text(query_str), params)
        
        data = [dict(row) for row in result.mappings()]

        return {
            "total_encontrados": len(data),"folio_filtrado": folio if folio else "Todos","items": data}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error al consultar notificaciones_gestion_clientes: {str(e)}")