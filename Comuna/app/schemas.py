from pydantic import BaseModel, Field, StrictBool, ConfigDict
from typing import List, Any, Optional, Dict
from pydantic import EmailStr
from decimal import Decimal,ROUND_HALF_UP
from pydantic import BaseModel

class ConfigBase:
    from_attributes = True
    populate_by_name = True
    extra = "allow" 


class PagoResponse(BaseModel):
    folio_venta: Any = Field(alias="Folio de la venta", default=None)
    fecha_estatus: Any = Field(alias="Fecha del estatus finalizado", default=None)
    cliente: Any = Field(alias="Cliente", default=None)
    proyecto: Any = Field(alias="Proyecto", default=None)
    etapa: Any = Field(alias="Etapa", default=None)
    privada: Any = Field(alias="Privada", default=None)
    unidad: Any = Field(alias="Unidad", default=None)
    superficie: Any = Field(alias="Superficie M2", default=None)
    plazo_enganche: Any = Field(alias="Plazo enganche", default=None)
    apartado: Any = Field(alias="Apartado", default=None)
    promocion: Any = Field(alias="Promoción", default=None)
    numero_pago: Any = Field(alias="Número de pago", default=None)
    fecha_amortizacion: Any = Field(alias="Fecha de amortización", default=None)
    concepto_pago: Any = Field(alias="Concepto de pago", default=None)
    monto_a_pagar: Any = Field(alias="Monto a pagar", default=None)
    folio_pago: Any = Field(alias="Folio de pago", default=None)
    fecha_comprobante: Any = Field(alias="Fecha del comprobante de pago", default=None)
    metodo_pago: Any = Field(alias="Método de pago", default=None)
    tipo_pago: Any = Field(alias="Tipo de pago", default=None)
    banco_caja: Any = Field(alias="Banco Caja", default=None)
    monto_pagado: Any = Field(alias="Monto pagado", default=None)
    fecha_aplicacion: Any = Field(alias="Fecha de aplicación de pago registro en sistema", default=None)
    estatus: Any = Field(alias="Estatus", default=None)
    estatus_expediente: Any = Field(alias="Estatus expediente", default=None)
    fecha_cancelacion: Any = Field(alias="Fecha de cancelación", default=None)
    cancelado_por: Any = Field(alias="Cancelado por", default=None)
    asesor: Any = Field(alias="Asesor", default=None)
    tipo_moneda: Any = Field(alias="Tipo de moneda", default=None)
    observaciones: Any = Field(alias="Observaciones", default=None)
    class Config(ConfigBase): pass

# Mapeamos TODAS las columnas de Cartera
class CarteraResponse(BaseModel):
    folio: Any = Field(alias="FOLIO", default=None)
    reporte: Any = Field(alias="REPORTE", default=None)
    proyecto: Any = Field(alias="PROYECTO", default=None)
    fase: Any = Field(alias="FASE", default=None)
    etapa: Any = Field(alias="ETAPA", default=None)
    unidad: Any = Field(alias="UNIDAD", default=None)
    cliente: Any = Field(alias="CLIENTE", default=None)
    telefono: Any = Field(alias="TELÉFONO", default=None)
    fecha_pago: Any = Field(alias="FECHA DE PAGO", default=None)
    dias_vencidos: Any = Field(alias="DÍAS DE VENCIDOS", default=None)
    concepto: Any = Field(alias="CONCEPTO", default=None)
    monto_a_pagar: Any = Field(alias="MONTO A PAGAR", default=None)
    estatus_pipeline: Any = Field(alias="ESTATUS DE PIPELINE", default=None)
    monto_pagado: Any = Field(alias="MONTO PAGADO", default=None)
    penalizacion_generada: Any = Field(alias="PENALIZACIÓN GENERADA", default=None)
    penalizacion_pagada: Any = Field(alias="PENALIZACIÓN PAGADA", default=None)
    saldo_sin_pen: Any = Field(alias="SALDO PENDIENTE SIN PENALIZACIÓN", default=None)
    saldo_con_pen: Any = Field(alias="SALDO PENDIENTE CON PENALIZACIÓN", default=None)
    parcialidades_vencidas: Any = Field(alias="NÚMERO DE PARCIALIDADES VENCIDAS TOTALES", default=None)
    total_vencido_sin_pen: Any = Field(alias="MONTO TOTAL VENCIDO SIN PENALIZACIÓN", default=None)
    total_vencido_con_pen: Any = Field(alias="MONTO TOTAL VENCIDO CON PENALIZACIÓN", default=None)
    moneda: Any = Field(alias="MONEDA", default=None)
    class Config(ConfigBase): pass


class VentaResponse(BaseModel):
    folio: Any = Field(alias="FOLIO", default=None)
    desarrollo: Any = Field(alias="DESARROLLO", default=None)
    etapa: Any = Field(alias="ETAPA", default=None)
    numero: Any = Field(alias="NÚMERO", default=None)
    clasificador: Any = Field(alias="CLASIFICADOR", default=None)
    m2: Any = Field(alias="METROS CUADRADOS", default=None)
    cliente: Any = Field(alias="CLIENTE", default=None)
    telefono: Any = Field(alias="TELÉFONO", default=None)
    asesor: Any = Field(alias="ASESOR", default=None)
    canal_ventas: Any = Field(alias="CANAL DE VENTAS", default=None)
    precio_lista: Any = Field(alias="PRECIO DE LISTA", default=None)
    descuento: Any = Field(alias="DESCUENTO", default=None)
    total_enganche: Any = Field(alias="TOTAL GENERAL DE ENGANCHE", default=None)
    flujo_enganche: Any = Field(alias="FLUJO DE ENGANCHE", default=None)
    meses_enganche: Any = Field(alias="MESES DE ENGANCHE", default=None)
    monto_fin_sin_interes: Any = Field(alias="MONTO FINANCIADO SIN INTERESES", default=None)
    monto_fin_con_interes: Any = Field(alias="MONTO FINANCIADO CON INTERESES", default=None)
    precio_final: Any = Field(alias="PRECIO FINAL", default=None)
    apartado: Any = Field(alias="APARTADO", default=None)
    fecha_inicio_operacion: Any = Field(alias="FECHA DE INICIO DE OPERACIÓN", default=None)
    fecha_pago_apartado: Any = Field(alias="FECHA DE PAGO DE APARTADO", default=None)
    fecha_finalizacion_enganche: Any = Field(alias="FECHA DE FINALIZACIÓN DE ENGANCHE", default=None)
    fecha_venta: Any = Field(alias="FECHA DE VENTA", default=None)
    fecha_finalizacion_financiamiento: Any = Field(alias="FECHA DE FINALIZACIÓN DE FINANCIAMIENTO", default=None)
    sucursales: Any = Field(alias="SUCURSALES", default=None)
    responsables: Any = Field(alias="RESPONSABLES", default=None)
    fecha_firma: Any = Field(alias="FECHA DE FIRMA DE PROMESA DE COMPRAVENTA", default=None)
    fecha_fin_pago_enganche: Any = Field(alias="FECHA DE FINALIZACIÓN DE PAGO DE ENGANCHE", default=None)
    plazo_financiamiento: Any = Field(alias="PLAZO DE FINANCIAMIENTO", default=None)
    tipo_moneda: Any = Field(alias="TIPO DE MONEDA", default=None)
    estado_expediente: Any = Field(alias="ESTADO DEL EXPEDIENTE", default=None)
    correo_electronico: Any = Field(alias="CORREO ELECTRÓNICO", default=None)
    genero: Any = Field(alias="GÉNERO", default=None)
    fecha_nacimiento: Any = Field(alias="FECHA DE NACIMIENTO", default=None)
    lugar_nacimiento: Any = Field(alias="LUGAR DE NACIMIENTO", default=None)
    ocupacion: Any = Field(alias="OCUPACIÓN", default=None)
    estado_civil: Any = Field(alias="ESTADO CIVIL", default=None)
    estado: Any = Field(alias="ESTADO", default=None)
    pais: Any = Field(alias="PAÍS", default=None)
    coordinador: Any = Field(alias="COORDINADOR COMERCIAL", default=None)
    gerente: Any = Field(alias="GERENTE COMERCIAL", default=None)
    class Config(ConfigBase): pass
    

class AmortizacionResponse(BaseModel):
    folder_id: Any = Field(alias="folder_id", default=None)
    number: Any = Field(alias="number", default=None)
    concept: Any = Field(alias="concept", default=None)
    date: Any = Field(alias="date", default=None)
    capital: Any = Field(alias="capital", default=None)
    interest: Any = Field(alias="interest", default=None)
    down_payment: Any = Field(alias="down_payment", default=None)
    total: Any = Field(alias="total", default=None)
    penalized_amount: Any = Field(alias="penalized_amount", default=None)
    
    class Config(ConfigBase): pass
    
class AntigSaldosResponse(BaseModel):
    folio: Any = Field(alias="FOLIO", default=None)
    cliente: Any = Field(alias="CLIENTE", default=None)
    proyecto: Any = Field(alias="PROYECTO", default=None)
    fase: Any = Field(alias="FASE", default=None)
    etapa: Any = Field(alias="ETAPA", default=None)
    unidad: Any = Field(alias="UNIDAD", default=None)
    correo: Any = Field(alias="CORREO ELECTRÓNICO", default=None)
    telefono: Any = Field(alias="TELÉFONO", default=None)
    fecha_pago: Any = Field(alias="FECHA DE PAGO", default=None)
    saldo_vigente: Any = Field(alias="SALDO VIGENTE", default=None)
    dias_1_30: Any = Field(alias="01 A 30 DÍAS", default=None)
    dias_31_60: Any = Field(alias="31 A 60 DÍAS", default=None)
    dias_61_90: Any = Field(alias="61 A 90 DÍAS", default=None)
    dias_91_120: Any = Field(alias="91 A 120 DÍAS", default=None)
    mas_120_dias: Any = Field(alias="MÁS DE 120 DÍAS", default=None)
    mensualidades_vencidas: Any = Field(alias="MENSUALIDADES VENCIDAS", default=None)
    total_vencido: Any = Field(alias="TOTAL VENCIDO", default=None)
    cartera_total: Any = Field(alias="CARTERA TOTAL", default=None)
    total_pagado: Any = Field(alias="TOTAL PAGADO", default=None)
    estatus_pipeline: Optional[str] = Field(alias="ESTATUS PIPELINE", default="Sin Estatus")
    parcialidades_vencidas_totales: Any = Field(alias="PARCIALIDADES_VENCIDAS_TOTALES", default=None)

    class Config(ConfigBase): pass

class AdjuntoSchema(BaseModel):
    content: str  
    filename: str 
 
       
class EmailManualSchema(BaseModel):
    remitente: EmailStr 
    para: List[EmailStr] 
    cc: Optional[List[EmailStr]] = [] 
    cco: Optional[List[EmailStr]] = [] 
    asunto: str
    contenido_html: str 
    reply_to: Optional[EmailStr] = None 
    folio: Optional[str] = None
    adjuntos: Optional[List[AdjuntoSchema]] = []

   
class EmailSchema(BaseModel):
    remitente: EmailStr 
    para: List[EmailStr] 
    cc: Optional[List[EmailStr]] = [] 
    cco: Optional[List[EmailStr]] = []
    asunto: str
    empresa_id: str    
    plantilla_slug: str
    
        
class RegistroSchema(BaseModel):
    email: EmailStr
    password: str
    nombre: str
    rol: str = "usuario"
    departamento: str

class LoginSchema(BaseModel):
    email: EmailStr
    password: str

class UsuarioResponse(BaseModel):
    id: str
    nombre: str
    email: str
    rol: str
    creado_el: Any = None
    departamento: str

class ConciliacionClienteResponse(BaseModel):
    folio: Any = Field(alias="FOLIO")
    proyecto: str = Field(alias="PROYECTO")
    fecha_promesa: Optional[Any] = Field(alias="FECHA PROMESA", default=None)
    cliente: str = Field(alias="NOMBRE CLIENTE")
    lote: Any = Field(alias="LOTE")
    cluster: Any = Field(alias="CLUSTER")
    m2: float = Field(alias="M2")
    precio_lista: float = Field(alias="PRECIO LISTA")
    anio: Optional[int] = Field(alias="AÑO", default=None)
    enero: float = 0.0
    febrero: float = 0.0
    marzo: float = 0.0
    abril: float = 0.0
    mayo: float = 0.0
    junio: float = 0.0
    julio: float = 0.0
    agosto: float = 0.0
    septiembre: float = 0.0
    octubre: float = 0.0
    noviembre: float = 0.0
    diciembre: float = 0.0
    total_anio: float = Field(alias="TOTAL_ANIO")

    class Config:
        from_attributes = True
        populate_by_name = True

class ComplementoPago(BaseModel):
    cliente: str
    pagador: Optional[str] = None
    proyecto: str
    fecha_pago: Optional[str] = None      
    fecha_aplicacion: Optional[str] = None            
    folio_venta: int           
    folio_pago: str            
    metodo: str
    concepto: Optional[str] = None     
    banco: Optional[str] = None
    num_pago: Optional[str] = None
    estatus_pago: str
    lote: str
    varios: bool
    total: int
    abono: float
    saldo: Optional[str] = None  
    anio: int    
    id_pago: Optional[str] = None
    id_flujo: Optional[str] = None
    estatus_flujo: Optional[str] = None
    monto_flujo: Optional[float] = None           

    class Config:
        from_attributes = True

class ReporteAntiguedadCompleto(BaseModel):
    detalles: List[AntigSaldosResponse]
    total_vigente: float = 0.0
    total_01_30: float = 0.0
    total_31_60: float = 0.0
    total_61_90: float = 0.0
    total_91_120: float = 0.0
    total_mas_120: float = 0.0
    total_vencido_global: float = 0.0
    cartera_total_global: float = 0.0
    mensualidad: int = 0
    pct_01_30: float = 0.0
    pct_31_60: float = 0.0
    pct_61_90: float = 0.0
    pct_91_120: float = 0.0
    pct_mas_120: float = 0.0

    pct_01_60_dias: float = Field(alias="analisis_01_60", default=0.0)
    pct_61_120_dias: float = Field(alias="analisis_61_120", default=0.0)
    pct_mas_120_dias: float = Field(alias="analisis_mas_120", default=0.0)

    analisis_riesgo_31_mas: float = Field(alias="riesgo_total", default=0.0)
    anio: Optional[int] = None

    class Config:
        from_attributes = True
        populate_by_name = True
        
class PlantillaBase(BaseModel):
    nombre: Optional[str] = None
    categoria: str
    asunto: str
    html: str
    activo: StrictBool
    tags_departamento: List[str] = []
    static: bool = False

class PlantillaUpdate(BaseModel):
    nombre: Optional[str] = None
    asunto: Optional[str] = None
    html: Optional[str] = None
    categoria: Optional[str] = None
    activo: Optional[StrictBool] = None
    tags_departamento: Optional[List[str]] = None

class ConfigUpdate(BaseModel):
    proyecto_activo: Optional[bool] = None
    email_enabled: Optional[bool] = None
    whatsapp_enabled: Optional[bool] = None
    

class PlantillaWABase(BaseModel):
    nombre: str             # Nombre interno (Ej: Recordatorio )
    id_respond: str         # El nombre de la plantilla en Respond.io
    categoria: str          # Ej: aviso_vencimiento
    lenguaje: str           # Ej: es, en
    variables: List[str]    # Ej: ["{cliente}", "{cl.monto}"]
    mensaje: str            # Texto para previsualizar
    activo: bool = False

class PlantillaWAUpdate(BaseModel):
    nombre: Optional[str] = None
    id_respond: Optional[str] = None
    categoria: Optional[str] = None
    lenguaje: Optional[str] = None
    variables: Optional[List[str]] = None
    mensaje: Optional[str] = None
    activo: Optional[bool] = None
    
class WhatsAppManualSchema(BaseModel):
    folio: str
    categoria: str
    
    
class UsuarioUpdate(BaseModel):
    nombre: Optional[str] = None
    email: Optional[EmailStr] = None
    rol: Optional[str] = None
    password: Optional[str] = None
    departamento: Optional[str] = None

class SwitchEtapasSchema(BaseModel):
    cambios: Dict[str, bool]


class GlobalMassiveUpdate(BaseModel):
    switches: List[str] 
    estado: bool       


class EmailFolioSchema(BaseModel):
    folio: str
    categoria: str

class RemitenteCreate(BaseModel):
    remitente: EmailStr

class RemitenteUpdate(BaseModel):
    remitente: Optional[EmailStr] = None
    departamento: Optional[str] = None

class RemitenteResponse(BaseModel):
    id: str
    departamento: str
    remitente: str


class RecordatoriosUpdate(BaseModel):
    dias_1: Optional[int] = None
    dias_2: Optional[int] = None
    hora: Optional[int] = None
    minuto: Optional[int] = None