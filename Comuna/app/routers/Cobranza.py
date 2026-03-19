import os
import logging
import re
import requests
from ..services.security import get_current_user, es_admin, es_usuario
from ..database import get_db
from ..utils.datos_proveedores import get_komunah_data
from dotenv import load_dotenv
from sqlalchemy.orm import Session
from sqlalchemy import text
from fastapi import APIRouter, HTTPException, Depends
from pydantic import BaseModel
from playwright.sync_api import sync_playwright
from google.cloud import storage
from datetime import date, timedelta
from typing import Optional

load_dotenv()

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/Cobranza", tags=["Cobranza"])

_project_id = os.getenv("FIREBASE_PLANTILLAS_PROJECT_ID", "").strip()
_api_key    = os.getenv("FIREBASE_PLANTILLAS_API_KEY", "")
_base_url   = f"https://firestore.googleapis.com/v1/projects/{_project_id}/databases/(default)/documents"
_headers    = {"X-Goog-Api-Key": _api_key, "Content-Type": "application/json"}
_bucket_name = "bucket-grupo-komunah-juridico"


class CobranzaStatusUpdate(BaseModel):
    id_empresa: str
    id: str
    status: str
    observacionesPago: Optional[str] = None


class StatusPagoUpdate(BaseModel):
    id: str
    status: str


class CobranzaService:
    def __init__(self):
        self.base_url = _base_url
        self.headers = _headers
        self.project_id = _project_id
        self.api_key = _api_key

    @staticmethod
    def _clean_firestore_value(v):
        if not isinstance(v, dict):
            return v
        if "mapValue" in v:
            fields = v["mapValue"].get("fields", {})
            return {k: CobranzaService._clean_firestore_value(val) for k, val in fields.items()}
        if "arrayValue" in v:
            return [CobranzaService._clean_firestore_value(i) for i in v["arrayValue"].get("values", [])]
        if "stringValue" in v:
            return v["stringValue"]
        if "integerValue" in v:
            return int(v["integerValue"])
        if "doubleValue" in v:
            return float(v["doubleValue"])
        if "booleanValue" in v:
            return v["booleanValue"]
        if "timestampValue" in v:
            return v["timestampValue"]
        return list(v.values())[0] if v else None

    @staticmethod
    def _normalizar_fragmento(valor, fallback="N/A") -> str:
        texto = str(valor).strip() if valor is not None else ""
        if not texto:
            texto = fallback
        return re.sub(r"[\\/:*?\"<>|]", "_", texto)

    def _validar_config(self):
        if not self.project_id or not self.api_key:
            raise HTTPException(status_code=500, detail="Configuración de Firebase incompleta")

    def obtener_comprobante(self, comprobante_id: str):
        self._validar_config()
        url = f"{self.base_url}/ComprobantePago/{comprobante_id}"
        resp = requests.get(url, headers=self.headers, timeout=10)
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Comprobante no encontrado")
        if resp.status_code != 200:
            logger.error(f"Firebase error {resp.status_code}: {resp.text}")
            raise HTTPException(status_code=500, detail="Error al obtener comprobante")
        return resp.json()

    def actualizar_status(self, comprobante_id: str, nuevo_status: str, observaciones_pago: Optional[str] = None):
        self._validar_config()
        mask_paths = ["Status"]
        body_fields = {"Status": {"stringValue": nuevo_status}}

        if observaciones_pago is not None:
            mask_paths.append("observacionesPago")
            body_fields["observacionesPago"] = {"stringValue": str(observaciones_pago)}

        mask_query = "&".join([f"updateMask.fieldPaths={m}" for m in mask_paths])
        url = f"{self.base_url}/ComprobantePago/{comprobante_id}?{mask_query}"
        body = {"fields": body_fields}
        resp = requests.patch(url, json=body, headers=self.headers, timeout=10)

        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail="Comprobante no encontrado")
        if resp.status_code == 400:
            logger.error(f"Firebase error 400: {resp.text}")
            raise HTTPException(status_code=400, detail="Solicitud inválida para Firebase")
        if resp.status_code not in [200, 201]:
            logger.exception(f"Firebase error {resp.status_code}: {resp.text}")
            raise HTTPException(status_code=500, detail="Error al actualizar status")

    def obtener_plantilla_juridico(self, id_empresa: str, plantilla_id: str = "KO-0005"):
        self._validar_config()
        url = f"{self.base_url}/empresas/{id_empresa}/plantillas_juridico/{plantilla_id}"
        resp = requests.get(url, headers=self.headers, timeout=10)
        if resp.status_code == 404:
            raise HTTPException(status_code=404, detail=f"Plantilla jurídica {plantilla_id} no encontrada")
        if resp.status_code != 200:
            logger.error(f"Firebase error plantilla {resp.status_code}: {resp.text}")
            raise HTTPException(status_code=500, detail="Error al obtener plantilla jurídica")
        return resp.json()

    def obtener_parcialidad_por_folio(self, folio: str, db: Session) -> str:
        query = text("""
            SELECT t.`NÚMERO DE PARCIALIDADES VENCIDAS TOTALES` AS parcialidad
            FROM (
                SELECT
                    cv.`FOLIO`,
                    cv.`NÚMERO DE PARCIALIDADES VENCIDAS TOTALES`,
                    ROW_NUMBER() OVER (
                        PARTITION BY cv.`FOLIO`
                        ORDER BY ABS(DATEDIFF(STR_TO_DATE(cv.`FECHA DE PAGO`, '%d/%m/%Y'), CURDATE())) ASC
                    ) AS rank_fecha
                FROM cartera_vencida cv
                WHERE cv.`FOLIO` = :folio
            ) t
            WHERE t.rank_fecha = 1
            LIMIT 1
        """)
        result = db.execute(query, {"folio": str(folio)}).mappings().first()
        if not result:
            return "0"
        parcialidad = result.get("parcialidad")
        return str(parcialidad).strip() if parcialidad is not None else "0"

    @staticmethod
    def reemplazar_etiquetas(texto: str, variables: dict):
        if not texto:
            return texto
        for tag, valor in variables.items():
            texto = texto.replace(tag, str(valor))
        return re.sub(r"\{[^}]+\}", "", texto)

    def generar_pdf_bytes(self, html: str) -> bytes:
        with sync_playwright() as p:
            browser = p.chromium.launch(args=["--no-sandbox", "--disable-setuid-sandbox"])
            page = browser.new_page()
            page.set_content(html, wait_until="networkidle")
            page.emulate_media(media="screen")
            page.wait_for_load_state("networkidle")
            page.wait_for_timeout(400)
            pdf_bytes = page.pdf(format="A4", print_background=True)
            browser.close()
        return pdf_bytes

    def _obtener_bucket(self):
        cred_path = os.getenv("STORAGE_CREDENTIALS_PATH")
        if not cred_path:
            raise HTTPException(status_code=500, detail="Falta STORAGE_CREDENTIALS_PATH")
        storage_client = storage.Client.from_service_account_json(cred_path)
        return storage_client.bucket(_bucket_name)

    def _obtener_siguiente_id_archivo(self, ruta_carpeta: str) -> str:
        bucket = self._obtener_bucket()
        prefix = f"{ruta_carpeta}/"
        max_id = 0

        for blob in bucket.list_blobs(prefix=prefix):
            nombre = blob.name.rsplit("/", 1)[-1]
            match = re.match(r"^(\d+)_", nombre)
            if match:
                max_id = max(max_id, int(match.group(1)))

        return f"{max_id + 1:02d}"

    def subir_a_bucket(self, pdf_bytes: bytes, ruta_carpeta: str, nombre_archivo: str) -> str:
        bucket = self._obtener_bucket()
        ruta_completa = f"{ruta_carpeta}/{nombre_archivo}"
        blob = bucket.blob(ruta_completa)

        if not blob.exists():
            blob.upload_from_string(pdf_bytes, content_type="application/pdf")

        return blob.generate_signed_url(version="v4", expiration=timedelta(hours=1), method="GET")

    def generar_y_subir_pdf(self, id_empresa: str, comprobante_id: str, db: Session, status_pago: str):
        comprobante_doc = self.obtener_comprobante(comprobante_id)
        comprobante_fields = comprobante_doc.get("fields", {})
        comprobante_limpio = {k: self._clean_firestore_value(v) for k, v in comprobante_fields.items()}

        folio = str(comprobante_limpio.get("Contacto", {}).get("FolioExpediente", "")).strip()
        if not folio:
            raise HTTPException(status_code=400, detail="El comprobante no contiene folio")

        plantilla = self.obtener_plantilla_juridico(id_empresa, "KO-0005")
        p_fields = plantilla.get("fields", {})
        html_raw = p_fields.get("html", {}).get("stringValue", "")
        categoria = p_fields.get("categoria", {}).get("stringValue", "")
        if not html_raw:
            raise HTTPException(status_code=400, detail="La plantilla KO-0005 no contiene html")

        data_sql = get_komunah_data(folio, db)
        if not data_sql:
            raise HTTPException(status_code=404, detail=f"Folio {folio} no encontrado en SQL")

        html_final = self.reemplazar_etiquetas(html_raw, data_sql)
        pdf_bytes = self.generar_pdf_bytes(html_final)

        cliente = (
            data_sql.get("{c1.client_name}")
            or data_sql.get("{cliente}")
            or data_sql.get("{cl.cliente}")
            or data_sql.get("{v.cliente}")
            or "Cliente"
        )
        lote = (
            data_sql.get("{v.numero}")
            or data_sql.get("{lote}")
            or data_sql.get("{unidad}")
            or "SinLote"
        )

        fecha_hoy = date.today().isoformat()

        status_normalizado = str(status_pago or "").strip().lower()
        if status_normalizado == "aceptado":
            carpeta_status = "Aceptados"
        elif status_normalizado == "rechazado":
            carpeta_status = "Rechazados"
        else:
            raise HTTPException(status_code=400, detail="Status inválido. Use 'Aceptado' o 'Rechazado'.")

        ruta = (
            f"Komunah/PlantillasMovil/Categorias/"
            f"{self._normalizar_fragmento(categoria, 'categoria')}/"
            f"{self._normalizar_fragmento(cliente, 'Cliente')}/"
            f"{carpeta_status}"
        )

        siguiente_id = self._obtener_siguiente_id_archivo(ruta)
        nombre_pdf = (
            f"{siguiente_id}_"
            f"{self._normalizar_fragmento(folio, 'SinFolio')}_"
            f"{self._normalizar_fragmento(lote, 'SinLote')}_"
            f"{fecha_hoy}.pdf"
        )

        url_descarga = self.subir_a_bucket(pdf_bytes, ruta, nombre_pdf)
        return {
            "folio": folio,
            "categoria": categoria,
            "cliente": cliente,
            "lote": lote,
            "ruta": ruta,
            "filename": nombre_pdf,
            "url_descarga": url_descarga,
        }


@router.get("/comprobantes")
def obtener_comprobantes(user: dict = Depends(es_usuario)):
    """Obtiene y limpia todos los documentos de la colección ComprobantePago en un solo paso."""
    try:
        url = f"{_base_url}/ComprobantePago"
        resp = requests.get(url, headers=_headers, timeout=10)

        if resp.status_code != 200:
            logger.error(f"Firebase error {resp.status_code}: {resp.text}")
            raise HTTPException(status_code=500, detail="Error al obtener comprobantes")

        documentos = resp.json().get("documents", [])

        # --- Lógica de limpieza compacta ---
        # Esta función lambda se encarga de entrar en cada nivel y extraer el valor real
        clean = lambda v: (
            {k: clean(val) for k, val in v["mapValue"]["fields"].items()} if isinstance(v, dict) and "mapValue" in v else
            [clean(i) for i in v["arrayValue"]["values"]] if isinstance(v, dict) and "arrayValue" in v else
            (int(v["integerValue"]) if "integerValue" in v else list(v.values())[0]) if isinstance(v, dict) else v
        )

        resultado = []
        for doc in documentos:
            # Procesamos los campos base del documento
            raw_fields = doc.get("fields", {})
            # Limpiamos recursivamente y añadimos el ID
            item = {k: clean(v) for k, v in raw_fields.items()}
            item["id"] = doc["name"].split("/")[-1]
            resultado.append(item)

        return resultado

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error al obtener comprobantes: {e}")
        raise HTTPException(status_code=500, detail="Error al obtener comprobantes")


@router.post("/actualizarStatusPagos")
def actualizar_status_pagos(payload: CobranzaStatusUpdate, db: Session = Depends(get_db), user: dict = Depends(es_usuario)):
    """Actualiza status de comprobante y genera/sube PDF jurídico KO-0005."""
    try:
        service = CobranzaService()

        nuevo_status = payload.status.strip()
        if not nuevo_status:
            raise HTTPException(status_code=400, detail="El campo status es obligatorio")
        if nuevo_status.lower() not in ["aceptado", "rechazado"]:
            raise HTTPException(status_code=400, detail="Status inválido. Use 'Aceptado' o 'Rechazado'.")

        observaciones = payload.observacionesPago.strip() if payload.observacionesPago is not None else None
        service.actualizar_status(payload.id, nuevo_status, observaciones)
        pdf_info = service.generar_y_subir_pdf(payload.id_empresa, payload.id, db, nuevo_status)

        return {
            "status": "success",
            "id": payload.id,
            "id_empresa": payload.id_empresa,
            "nuevo_status": nuevo_status,
            "observacionesPago": observaciones,
            "pdf": pdf_info,
        }
    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Error al actualizar status: {e}")
        raise HTTPException(status_code=500, detail="Error al actualizar status")