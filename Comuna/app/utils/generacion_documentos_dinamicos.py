import os
import re
import base64
import logging
import io
from datetime import datetime, timedelta
from typing import Any, List, Optional
from zoneinfo import ZoneInfo
from pypdf import PdfWriter, PdfReader
from sqlalchemy.orm import Session
from ..utils.datos_proveedores import (
    get_komunah_data, set_wa_komunah_lote, set_email_komunah_lote, 
    get_folios_a_notificar_komunah, actualizar_switches_etapas, 
    actualizar_switches_proyecto, get_estado_etapas_komunah, 
    get_folios_deudores_komunah, get_folios_dinamico_komunah
)
from fastapi import HTTPException
from google.cloud import storage
from playwright.async_api import async_playwright
from ..models import Amortizacion, Venta

logger = logging.getLogger(__name__)
BUCKET_NAME = "bucket-grupo-komunah-juridico"

def _get_utils_notifications():
    from ..routers.notificacionesMS import UtilsNotifications
    return UtilsNotifications

def _get_providers():
    from ..routers.notificacionesMS import PROVIDERS
    return PROVIDERS

class GenerarPDFUseCase:
    def __init__(self, repo):
        self.repo = repo

#region Funciones auxiliares de normalización, formateo y construcción de HTML

    @staticmethod
    def _normalizar_fragmento(valor: Any, fallback: str = "N/A") -> str:
        texto = str(valor).strip() if valor is not None else ""
        if not texto:
            texto = fallback
        # Evitar separadores invalidos en nombre/ruta.
        return re.sub(r"[\\/:*?\"<>|]", "_", texto)

    @staticmethod
    def _normalizar_variables_para_html(variables: dict) -> dict:
        """Prepara variables para reemplazo robusto en HTML (incluye aliases del diccionario maestro)."""
        base = dict(variables or {})

        # Mapa case-insensitive para tolerar diferencias de mayúsculas/minúsculas en plantillas.
        for k, v in list(base.items()):
            base[str(k).lower()] = v

        # Variables generales fijas del diccionario maestro.
        if "{cliente}" not in base:
            base["{cliente}"] = (
                base.get("{cl.cliente}")
                or base.get("{v.cliente}")
                or base.get("{c1.client_name}")
                or ""
            )

        if "{email_cliente}" not in base:
            base["{email_cliente}"] = (
                base.get("{g1.email}")
                or base.get("{c1.email}")
                or base.get("{v.correo_electronico}")
                or ""
            )

        if "{telefono_cliente}" not in base:
            base["{telefono_cliente}"] = (
                base.get("{g1.telefono}")
                or base.get("{c1.main_phone}")
                or base.get("{v.telefono}")
                or ""
            )

        ahora = datetime.now(ZoneInfo("America/Mexico_City"))
        base["{fechadehoy}"] = ahora.strftime("%d/%m/%Y")

        return base

    @staticmethod
    def _reemplazar_etiquetas(texto: str, variables: dict):
        if not texto:
            return texto

        vars_html = GenerarPDFUseCase._normalizar_variables_para_html(variables)

        # REGEX ACTUALIZADO: Soporta { }, {{ }}, puntos y guiones '-'
        regex_seguro = r"\{{1,2}[a-zA-Z0-9_\.\-]+\}{1,2}"

        etiquetas_en_html = set(re.findall(regex_seguro, texto))
        for tag in etiquetas_en_html:
            # Limpiamos el tag (quitamos todas las { y }) para buscar en el dict
            tag_limpio = tag.replace("{", "").replace("}", "").strip()

            # Buscamos la variable (probablemente guardada con formato {nombre} en tu dict)
            valor = vars_html.get(f"{{{tag_limpio}}}")
            if valor is None:
                valor = vars_html.get(f"{{{tag_limpio.lower()}}}")

            if valor is not None:
                texto = texto.replace(tag, str(valor))

        # Limpieza final segura para CSS
        return re.sub(regex_seguro, "", texto)

    @staticmethod
    def _a_float(valor: Any) -> float:
        try:
            return float(valor)
        except (TypeError, ValueError):
            return 0.0

    @staticmethod
    def _formatear_moneda(valor: Any) -> str:
        numero = GenerarPDFUseCase._a_float(valor)
        return f"${numero:,.2f}"

    @staticmethod
    def _obtener_archivos_subidos_desde_fields(fields: dict) -> List[dict]:
        logger.info("[PDF_GENERADOR] Entrada _obtener_archivos_subidos_desde_fields")
        adjuntos = []

        # Formato clásico: mapValue { "archivo.pdf": {"stringValue": "...base64..."} }
        archivos_map = fields.get("archivos_subidos", {}).get("mapValue", {}).get("fields", {})
        logger.info("[PDF_GENERADOR] Paso: procesar archivos_subidos mapValue | total=%s", len(archivos_map))
        for nombre_archivo, nodo in archivos_map.items():
            contenido_b64 = nodo.get("stringValue")
            if contenido_b64:
                adjuntos.append({"content": contenido_b64, "filename": nombre_archivo})

        # Formato nuevo: arrayValue [{ mapValue: { fields: { filename, content, ... } } }]
        archivos_array = fields.get("archivos_subidos", {}).get("arrayValue", {}).get("values", [])
        logger.info("[PDF_GENERADOR] Paso: procesar archivos_subidos arrayValue | total=%s", len(archivos_array))
        for item in archivos_array:
            f = item.get("mapValue", {}).get("fields", {})
            nombre = f.get("filename", {}).get("stringValue")
            contenido = f.get("content", {}).get("stringValue")
            if nombre and contenido:
                adjuntos.append({"content": contenido, "filename": nombre})

        logger.info("[PDF_GENERADOR] Salida _obtener_archivos_subidos_desde_fields | adjuntos=%s", len(adjuntos))
        return adjuntos

    @staticmethod
    def _obtener_ids_anexos_desde_fields(fields: dict) -> List[str]:
        """Identifica IDs de anexos guardados en Firestore (mapValue o arrayValue)."""
        logger.info("[PDF_GENERADOR] Entrada _obtener_ids_anexos_desde_fields")
        ids_anexos: List[str] = []

        anexos_map = fields.get("anexos", {}).get("mapValue", {}).get("fields", {})
        logger.info("[PDF_GENERADOR] Paso: leer anexos mapValue | total=%s", len(anexos_map))
        for anexo_id in anexos_map.keys():
            anexo_id_norm = str(anexo_id).strip()
            if anexo_id_norm:
                ids_anexos.append(anexo_id_norm)

        anexos_array = fields.get("anexos", {}).get("arrayValue", {}).get("values", [])
        logger.info("[PDF_GENERADOR] Paso: leer anexos arrayValue | total=%s", len(anexos_array))
        for item in anexos_array:
            anexo_id = item.get("stringValue")
            anexo_id_norm = str(anexo_id).strip() if anexo_id is not None else ""
            if anexo_id_norm:
                ids_anexos.append(anexo_id_norm)

        # Preserva orden y evita duplicados.
        ids_unicos = list(dict.fromkeys(ids_anexos))
        logger.info("[PDF_GENERADOR] Salida _obtener_ids_anexos_desde_fields | total_ids=%s", len(ids_unicos))
        return ids_unicos

    @staticmethod
    def _es_booleano_activo(valor: Any) -> bool:
        if isinstance(valor, bool):
            return valor
        if valor is None:
            return False
        return str(valor).strip().lower() in {"true", "1", "si", "yes"}

    @staticmethod
    def _normalizar_lista_nombres(nombres: List[Any]) -> List[str]:
        resultado: List[str] = []
        for nombre in nombres or []:
            nombre_txt = str(nombre).strip() if nombre is not None else ""
            if nombre_txt and nombre_txt.lower() not in {"none", "null", "n/a", "na"}:
                resultado.append(nombre_txt)
        return list(dict.fromkeys(resultado))

    @staticmethod
    def _inyectar_membretada_fondo(html: str, imagen_base64: str) -> str:
        """Inyecta una imagen como fondo fijo con opacidad 40% en el HTML."""
        if not html or not imagen_base64:
            return html

        # Crear div de fondo con la imagen en base64
        fondo_html = f"""<div style="position: fixed; top: 0; left: 0; right: 0; bottom: 0; background-image: url('data:image/png;base64,{imagen_base64}'); background-size: cover; background-attachment: fixed; opacity: 0.4; z-index: -1; pointer-events: none;"></div>"""

        # Buscar el tag <body y insertar el div inmediatamente después
        match = re.search(r"<body[^>]*>", html, re.IGNORECASE)
        if match:
            pos = match.end()
            return html[:pos] + fondo_html + html[pos:]

        # Si no hay <body>, inserta al inicio del HTML
        return fondo_html + html

    def _construir_bloque_firmas_html(self, nombres: List[str], titulo: str) -> str:
        logger.info("[PDF_FIRMAS] Entrada _construir_bloque_firmas_html | titulo=%s | total_nombres=%s", titulo, len(nombres or []))
        if not nombres:
            logger.info("[PDF_FIRMAS] Salida _construir_bloque_firmas_html | resultado=bloque_vacio")
            return ""

        firmas = []
        for nombre in nombres:
            firmas.append(
                f"""
                <div style=\"break-inside: avoid; min-height: 90px;\">
                    <div style=\"border-top: 1px solid #222; width: 100%; margin-top: 44px;\"></div>
                    <div style=\"margin-top: 8px; font-size: 12px; text-align: center;\">{nombre}</div>
                </div>
                """
            )

        bloque = f"""
        <section style=\"margin-top: 40px; page-break-inside: avoid;\">
            <h3 style=\"margin: 0 0 20px 0; font-size: 14px;\">{titulo}</h3>
            <div style=\"display: grid; grid-template-columns: repeat(2, minmax(220px, 1fr)); gap: 34px 42px;\">
                {''.join(firmas)}
            </div>
        </section>
        """
        logger.info("[PDF_FIRMAS] Salida _construir_bloque_firmas_html | resultado=bloque_generado | total_firmas=%s", len(firmas))
        return bloque

    @staticmethod
    def _insertar_al_final_del_documento(html_raw: str, bloque_html: str) -> str:
        logger.info("[PDF_FIRMAS] Entrada _insertar_al_final_del_documento | html_len=%s | bloque_len=%s", len(html_raw or ""), len(bloque_html or ""))
        if not bloque_html:
            logger.info("[PDF_FIRMAS] Salida _insertar_al_final_del_documento | resultado=sin_cambios")
            return html_raw

        if "</body>" in html_raw:
            resultado = html_raw.replace("</body>", f"{bloque_html}</body>", 1)
            logger.info("[PDF_FIRMAS] Salida _insertar_al_final_del_documento | ancla=body")
            return resultado

        if "</html>" in html_raw:
            resultado = html_raw.replace("</html>", f"{bloque_html}</html>", 1)
            logger.info("[PDF_FIRMAS] Salida _insertar_al_final_del_documento | ancla=html")
            return resultado

        resultado = f"{html_raw}\n{bloque_html}"
        logger.info("[PDF_FIRMAS] Salida _insertar_al_final_del_documento | ancla=append_final")
        return resultado

    def _construir_seccion_firmantes_empresa(self, html_raw: str, empresa_id: str, db: Session) -> str:
        logger.info("[PDF_FIRMAS] Entrada _construir_seccion_firmantes_empresa | empresa=%s", empresa_id)
        del db  # Se mantiene por compatibilidad de firma con el flujo actual.
        try:
            logger.info("[PDF_FIRMAS] Paso: consultar coleccion firmantes-empresa")
            docs = self.repo.listar_firmantes_empresa(empresa_id)
            logger.info("[PDF_FIRMAS] Paso: documentos firmantes-empresa recibidos=%s", len(docs or []))

            nombres = []
            for doc in docs or []:
                fields = doc.get("fields", {})
                activo = fields.get("activo", {}).get("booleanValue")
                if not self._es_booleano_activo(activo):
                    continue
                nombre = fields.get("nombre", {}).get("stringValue")
                if nombre:
                    nombres.append(nombre)

            nombres = self._normalizar_lista_nombres(nombres)
            logger.info("[PDF_FIRMAS] Paso: firmantes empresa activos normalizados=%s", len(nombres))
            bloque = self._construir_bloque_firmas_html(nombres, "Firmantes de Empresa")
            resultado = self._insertar_al_final_del_documento(html_raw, bloque)
            logger.info("[PDF_FIRMAS] Salida _construir_seccion_firmantes_empresa | firmas_agregadas=%s", len(nombres))
            return resultado
        except Exception as e:
            logger.exception("[PDF_FIRMAS] Error _construir_seccion_firmantes_empresa | empresa=%s | error=%s", empresa_id, str(e))
            return html_raw

    def _construir_seccion_firmantes_coopropietarios(self, html_raw: str, folio: str, db: Session) -> str:
        logger.info("[PDF_FIRMAS] Entrada _construir_seccion_firmantes_coopropietarios | folio=%s", folio)
        try:
            logger.info("[PDF_FIRMAS] Paso: consultar venta por folio para cliente y copropietarios")
            venta = db.query(Venta).filter(Venta.folio == str(folio)).first()
            if not venta:
                logger.warning("[PDF_FIRMAS] Sin datos de venta para firmas de copropietarios | folio=%s", folio)
                return html_raw

            nombres = [
                venta.cliente,
                venta.cliente_2,
                venta.cliente_3,
                venta.cliente_4,
                venta.cliente_5,
                venta.cliente_6,
            ]
            nombres = self._normalizar_lista_nombres(nombres)
            logger.info("[PDF_FIRMAS] Paso: firmantes cliente/copropietarios normalizados=%s", len(nombres))
            bloque = self._construir_bloque_firmas_html(nombres, "Firmas de Cliente y Copropietarios")
            resultado = self._insertar_al_final_del_documento(html_raw, bloque)
            logger.info("[PDF_FIRMAS] Salida _construir_seccion_firmantes_coopropietarios | firmas_agregadas=%s", len(nombres))
            return resultado
        except Exception as e:
            logger.exception("[PDF_FIRMAS] Error _construir_seccion_firmantes_coopropietarios | folio=%s | error=%s", folio, str(e))
            return html_raw

    def _construir_seccion_firmantes_personalizados(self, html_raw: str, fields: dict, db: Session) -> str:
        logger.info("[PDF_FIRMAS] Entrada _construir_seccion_firmantes_personalizados")
        del db  # Se mantiene por compatibilidad de firma con el flujo actual.
        try:
            logger.info("[PDF_FIRMAS] Paso: leer array FirmantesPersonalizados desde fields")
            valores = fields.get("FirmantesPersonalizados", {}).get("arrayValue", {}).get("values", [])
            logger.info("[PDF_FIRMAS] Paso: elementos crudos en FirmantesPersonalizados=%s", len(valores or []))
            nombres = [item.get("stringValue") for item in valores if isinstance(item, dict)]
            nombres = self._normalizar_lista_nombres(nombres)
            logger.info("[PDF_FIRMAS] Paso: firmantes personalizados normalizados=%s", len(nombres))
            bloque = self._construir_bloque_firmas_html(nombres, "Firmantes Personalizados")
            resultado = self._insertar_al_final_del_documento(html_raw, bloque)
            logger.info("[PDF_FIRMAS] Salida _construir_seccion_firmantes_personalizados | firmas_agregadas=%s", len(nombres))
            return resultado
        except Exception as e:
            logger.exception("[PDF_FIRMAS] Error _construir_seccion_firmantes_personalizados | error=%s", str(e))
            return html_raw

    def _construir_tabla_pagos_cotizaciones(self, html_raw: str, folio: str, db: Session):
        logger.info("[PDF_COTIZACIONES] Entrada _construir_tabla_pagos_cotizaciones | folio=%s", folio)
        try:
            logger.info("[PDF_COTIZACIONES] Paso: consultar amortizaciones por folio")
            amortizaciones = (
                db.query(
                    Amortizacion.number,
                    Amortizacion.date,
                    Amortizacion.concept,
                    Amortizacion.capital,
                    Amortizacion.down_payment,
                    Amortizacion.total
                )
                .filter(Amortizacion.folder_id == str(folio))
                .order_by(Amortizacion.date.asc())
                .all()
            )
        except Exception as e:
            logger.exception("[PDF_COTIZACIONES] Error consultando amortizaciones | folio=%s | error=%s", folio, str(e))
            raise HTTPException(status_code=500, detail=f"Error al consultar amortizaciones para folio {folio}: {str(e)}")

        # --- INICIO DE BÚSQUEDA CORREGIDA ---
        # 1. Encontramos la posición de la primera variable de la fila
        idx_pago = html_raw.find("{pago.numero}")

        if idx_pago == -1:
            logger.info("[PDF_COTIZACIONES] Paso: no se encontró plantilla de fila de pagos en HTML")
            return html_raw, {
                "{totales.suma_capital}": self._formatear_moneda(0),
                "{totales.suma_enganche}": self._formatear_moneda(0),
                "{totales.suma_total}": self._formatear_moneda(0),
            }

        # 2. Buscamos el inicio de esa fila hacia atrás y el final hacia adelante
        idx_tr_start = html_raw.rfind("<tr", 0, idx_pago)
        idx_tr_end = html_raw.find("</tr>", idx_pago)

        if idx_tr_start == -1 or idx_tr_end == -1:
            logger.error("[PDF_COTIZACIONES] Paso: No se pudo delimitar la fila <tr> en el HTML")
            return html_raw, {
                "{totales.suma_capital}": self._formatear_moneda(0),
                "{totales.suma_enganche}": self._formatear_moneda(0),
                "{totales.suma_total}": self._formatear_moneda(0),
            }

        # 3. Extraemos exactamente la fila (sumamos 5 para incluir los caracteres de "</tr>")
        fila_template = html_raw[idx_tr_start:idx_tr_end + 5]
        # --- FIN DE BÚSQUEDA CORREGIDA ---

        pagos_apartado = [a for a in amortizaciones if str(getattr(a, "concept", "")).strip() == "initial_payment"]
        pagos_enganche = [a for a in amortizaciones if str(getattr(a, "concept", "")).strip() == "down_payment"]
        pagos_restantes = [
            a
            for a in amortizaciones
            if str(getattr(a, "concept", "")).strip() not in {"initial_payment", "down_payment"}
        ]

        secuencia = []
        for idx, pago in enumerate(pagos_apartado, 1):
            etiqueta = "A" if idx == 1 else f"A{idx}"
            secuencia.append((etiqueta, pago))
        for idx, pago in enumerate(pagos_enganche, 1):
            secuencia.append((f"{idx}E", pago))
        for pago in pagos_restantes:
            secuencia.append((str(getattr(pago, "number", "")), pago))

        total_capital = sum(
            self._a_float(getattr(p, "capital", 0))
            for p in pagos_restantes
        )
        saldo_capital = total_capital

        suma_capital = 0.0
        suma_enganche = 0.0
        suma_total = 0.0
        filas_renderizadas = []

        for etiqueta, pago in secuencia:
            concepto = str(getattr(pago, "concept", "")).strip()
            capital = self._a_float(getattr(pago, "capital", 0))
            enganche = self._a_float(getattr(pago, "down_payment", 0))
            total = self._a_float(getattr(pago, "total", 0))

            suma_capital += capital
            suma_enganche += enganche
            suma_total += total

            if concepto in {"initial_payment", "down_payment"}:
                capital_txt = ""
                saldo_txt = ""
                enganche_txt = self._formatear_moneda(enganche)
            else:
                saldo_capital = max(saldo_capital - capital, 0)
                capital_txt = self._formatear_moneda(capital)
                enganche_txt = self._formatear_moneda(enganche)
                saldo_txt = self._formatear_moneda(saldo_capital)

            total_txt = self._formatear_moneda(total) if total else ""

            fila = fila_template
            fila = fila.replace("{pago.numero}", str(etiqueta))
            fila = fila.replace("{pago.fecha}", str(getattr(pago, "date", "") or ""))
            fila = fila.replace("{pago.capital}", capital_txt)
            fila = fila.replace("{pago.enganche}", enganche_txt)
            fila = fila.replace("{pago.total}", total_txt)
            fila = fila.replace("{pago.saldo_capital}", saldo_txt)
            filas_renderizadas.append(fila)

        if not filas_renderizadas:
            fila = fila_template
            fila = fila.replace("{pago.numero}", "")
            fila = fila.replace("{pago.fecha}", "")
            fila = fila.replace("{pago.capital}", "")
            fila = fila.replace("{pago.enganche}", "")
            fila = fila.replace("{pago.total}", "")
            fila = fila.replace("{pago.saldo_capital}", "")
            filas_renderizadas.append(fila)

        html_con_filas = html_raw.replace(fila_template, "\n".join(filas_renderizadas), 1)
        totales = {
            "{totales.suma_capital}": self._formatear_moneda(suma_capital),
            "{totales.suma_enganche}": self._formatear_moneda(suma_enganche),
            "{totales.suma_total}": self._formatear_moneda(suma_total),
        }
        logger.info(
            "[PDF_COTIZACIONES] Paso: tabla construida | filas=%s | suma_capital=%s | suma_enganche=%s | suma_total=%s",
            len(filas_renderizadas),
            totales["{totales.suma_capital}"],
            totales["{totales.suma_enganche}"],
            totales["{totales.suma_total}"],
        )
        return html_con_filas, totales

#endregion

#region Funciones auxiliares de obtención de plantillas y manejo de bucket

    def _obtener_plantillas_por_categoria(self, empresa_id: str, categoria: str):
        logger.info(
            "[PDF_GENERADOR] Entrada _obtener_plantillas_por_categoria | empresa=%s | categoria=%s",
            empresa_id,
            categoria,
        )
        UtilsNotifications = _get_utils_notifications()

        logger.info("[PDF_GENERADOR] Paso: buscar plantilla juridica activa")
        doc_activo = UtilsNotifications._buscar_documento_plantilla(
            self.repo,
            empresa_id,
            categoria,
            "plantillas_juridico",
            solo_activas=True,
            fallback_listado=True,
        )
        if doc_activo:
            logger.info("[PDF_GENERADOR] Salida _obtener_plantillas_por_categoria | fuente=activa")
            return doc_activo

        logger.info("[PDF_GENERADOR] Paso: buscar plantilla juridica sin filtro de activa")
        doc_cualquiera = UtilsNotifications._buscar_documento_plantilla(
            self.repo,
            empresa_id,
            categoria,
            "plantillas_juridico",
            solo_activas=False,
            fallback_listado=True,
        )
        if doc_cualquiera:
            logger.info("[PDF_GENERADOR] Salida _obtener_plantillas_por_categoria | fuente=fallback")
            return doc_cualquiera

        if not doc_cualquiera:
            logger.error(
                "[PDF_GENERADOR] Error _obtener_plantillas_por_categoria | empresa=%s | categoria=%s | motivo=sin_plantilla",
                empresa_id,
                categoria,
            )
            raise HTTPException(status_code=404, detail=f"No existe plantilla jurídica para categoría '{categoria}'.")

    @staticmethod
    def _subir_pdf_a_bucket(pdf_bytes: bytes, ruta_carpeta: str, nombre_archivo: str) -> str:
        logger.info(
            "[PDF_GENERADOR] Entrada _subir_pdf_a_bucket | ruta=%s | archivo=%s",
            ruta_carpeta,
            nombre_archivo,
        )
        try:
            cred_path = os.getenv("STORAGE_CREDENTIALS_PATH")
            if not cred_path:
                logger.error("[PDF_GENERADOR] Error _subir_pdf_a_bucket | motivo=sin_credenciales")
                raise HTTPException(status_code=500, detail="Falta STORAGE_CREDENTIALS_PATH")

            storage_client = storage.Client.from_service_account_json(cred_path)
            bucket = storage_client.bucket(BUCKET_NAME)
            ruta_completa = f"{ruta_carpeta}/{nombre_archivo}"
            blob = bucket.blob(ruta_completa)

            if not blob.exists():
                logger.info("[PDF_GENERADOR] Paso: subiendo blob nuevo a bucket")
                blob.upload_from_string(pdf_bytes, content_type="application/pdf")
            else:
                logger.info("[PDF_GENERADOR] Paso: blob existente, se omite upload")

            url = blob.generate_signed_url(version="v4", expiration=timedelta(days=3), method="GET")
            logger.info("[PDF_GENERADOR] Salida _subir_pdf_a_bucket | url_generada=true")
            return url
        except HTTPException:
            logger.exception(
                "[PDF_GENERADOR] Error HTTP en _subir_pdf_a_bucket | ruta=%s | archivo=%s",
                ruta_carpeta,
                nombre_archivo,
            )
            raise
        except Exception as e:
            logger.exception(
                "[PDF_GENERADOR] Error inesperado en _subir_pdf_a_bucket | ruta=%s | archivo=%s | error=%s",
                ruta_carpeta,
                nombre_archivo,
                str(e),
            )
            raise HTTPException(status_code=500, detail=f"Error al subir PDF al bucket: {str(e)}")

#endregion

#region Logica principal de generación de PDF

    async def generar_pdf_por_categoria(self, empresa_id: str, categoria: str, folio: str, db: Session, subir_bucket: bool = False):
        logger.info(
            "[PDF_GENERADOR] Entrada generar_pdf_por_categoria | empresa=%s | categoria=%s | folio=%s | subir_bucket=%s",
            empresa_id,
            categoria,
            folio,
            subir_bucket,
        )
        try:
            logger.info("[PDF_GENERADOR] Paso: obtener plantilla por categoria")
            plantilla = self._obtener_plantillas_por_categoria(empresa_id, categoria)

            logger.info("[PDF_GENERADOR] Paso: obtener proveedor de datos")
            pack_empresa = _get_providers().get(empresa_id, {})
            extraer_datos = pack_empresa.get("get")
            if not extraer_datos:
                raise HTTPException(status_code=400, detail="Empresa no configurada.")

            logger.info("[PDF_GENERADOR] Paso: extraer datos SQL por folio")
            data_sql = extraer_datos(folio, db)
            if not data_sql:
                raise HTTPException(status_code=404, detail="Folio no encontrado.")

            cliente = (
                    data_sql.get("{c1.client_name}")
                    or data_sql.get("{cliente}")
                    or data_sql.get("{cl.cliente}")
                    or data_sql.get("{v.cliente}")
                    or "Cliente"
                )

            fields = plantilla.get("fields", {})
            html_raw = fields.get("html", {}).get("stringValue", "")
            if not html_raw:
                raise HTTPException(status_code=400, detail="La plantilla no tiene HTML.")

            variables_html = dict(data_sql)

            # 1. Formatear montos a Moneda ($X,XXX.XX)
            claves_monto = ["{v.total_enganche}", "{v.precio_lista}", "{v.apartado}", "{v.flujo_enganche}", "{v.total_enganche_pagar}"]
            for k in claves_monto:
                if k in variables_html:
                    variables_html[k] = self._formatear_moneda(variables_html[k])

            # 2. Limpiar duplicados de "meses"
            plazo_key = "{v.plazo_financiamiento}"
            if plazo_key in variables_html:
                valor_plazo = str(variables_html[plazo_key]).lower().replace("meses", "").strip()
                variables_html[plazo_key] = valor_plazo

            if fields.get("categoria", {}).get("stringValue", "").strip().lower() == "cotizaciones":
                logger.info("[PDF_GENERADOR] Paso: construir tabla de pagos para Cotizaciones")
                html_raw, totales = self._construir_tabla_pagos_cotizaciones(html_raw, folio, db)
                variables_html.update(totales)

            logger.info("[PDF_GENERADOR] Paso: reemplazar etiquetas y generar PDF")
            html_final = self._reemplazar_etiquetas(html_raw, variables_html)

            # Aplicar fondo membretada si está habilitada
            hoja_membretada = fields.get("HojaMembretadaProyecto", {}).get("booleanValue", False)
            if hoja_membretada:
                logger.info("[PDF_GENERADOR] Paso: HojaMembretadaProyecto activa, inyectando fondo")
                imagen_map = fields.get("ImagenMembretada", {}).get("mapValue", {}).get("fields", {})
                if imagen_map:
                    # Obtener la primera imagen disponible
                    primera_imagen_b64 = next(iter(imagen_map.values()), {}).get("stringValue", "")
                    if primera_imagen_b64:
                        html_final = self._inyectar_membretada_fondo(html_final, primera_imagen_b64)

            encabezado_raw = fields.get("encabezado", {}).get("stringValue", "") or ""
            footer_raw = fields.get("footer", {}).get("stringValue", "") or ""
            encabezado_final = self._reemplazar_etiquetas(encabezado_raw, variables_html) if encabezado_raw else ""
            footer_final = self._reemplazar_etiquetas(footer_raw, variables_html) if footer_raw else ""

            nombre_plantilla = fields.get("categoria", {}).get("stringValue", "documento")
            tamanoDocumento = fields.get("tamanoDocumento", {}).get("stringValue", "A4")
            nombre_pdf = f"{self._normalizar_fragmento(nombre_plantilla, fallback='documento')} - {self._normalizar_fragmento(cliente, 'Cliente')}.pdf"

            pdf_kwargs = {
                "format": tamanoDocumento,
                "print_background": True,
                "prefer_css_page_size": True,
                "scale": 1.0,
                "margin": {"top": "0px", "bottom": "0px", "left": "0px", "right": "0px"},
            }
            if encabezado_final or footer_final:
                pdf_kwargs["display_header_footer"] = True
                pdf_kwargs["header_template"] = encabezado_final or "<span></span>"
                pdf_kwargs["footer_template"] = footer_final or "<span></span>"

            async with async_playwright() as p:
                browser = await p.chromium.launch(args=["--no-sandbox", "--disable-setuid-sandbox"])
                page = await browser.new_page()
                await page.set_content(html_final, wait_until="networkidle")
                await page.emulate_media(media="screen")
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(400)
                pdf_bytes = await page.pdf(**pdf_kwargs)
                await browser.close()

            respuesta = {
                "filename": nombre_pdf,
                "content": base64.b64encode(pdf_bytes).decode("utf-8"),
                "content_type": "application/pdf",
                "tamanoDocumento": tamanoDocumento
            }

            if subir_bucket:
                logger.info("[PDF_GENERADOR] Paso: subir PDF a bucket")
                ruta = f"Komunah/PlantillasWeb/Categorias/{self._normalizar_fragmento(categoria, 'general')}/{self._normalizar_fragmento(cliente, 'Cliente')}"
                respuesta["url_descarga"] = self._subir_pdf_a_bucket(pdf_bytes, ruta, nombre_pdf)

            logger.info("[PDF_GENERADOR] Paso: PDF generado correctamente | filename=%s", nombre_pdf)
            return respuesta
        except HTTPException:
            logger.exception(
                "[PDF_GENERADOR] Error HTTP en generar_pdf_por_categoria | empresa=%s | categoria=%s | folio=%s",
                empresa_id,
                categoria,
                folio,
            )
            raise
        except Exception as e:
            logger.exception(
                "[PDF_GENERADOR] Error inesperado en generar_pdf_por_categoria | empresa=%s | categoria=%s | folio=%s | error=%s",
                empresa_id,
                categoria,
                folio,
                str(e),
            )
            raise HTTPException(status_code=500, detail=f"Error en generar_pdf_por_categoria (folio={folio}, categoria={categoria}): {str(e)}")

    async def _generar_pdf_desde_documento(self, empresa_id: str, id_plantilla: str, folio: str, db: Session, subir_bucket: bool = False):
        """Genera un PDF desde un documento jurídico por ID."""
        logger.info(
            "[PDF_GENERADOR] Entrada _generar_pdf_desde_documento | empresa=%s | plantilla=%s | folio=%s | subir_bucket=%s",
            empresa_id,
            id_plantilla,
            folio,
            subir_bucket,
        )
        try:
            logger.info("[PDF_GENERADOR] Paso: obtener plantilla por id")
            plantilla = self.repo.obtener_un_doc_completo_documentos(empresa_id, id_plantilla)
            if not plantilla:
                raise HTTPException(status_code=404, detail=f"No existe la plantilla jurídica seleccionada: {id_plantilla}.")

            pack_empresa = _get_providers().get(empresa_id, {})
            extraer_datos = pack_empresa.get("get")
            if not extraer_datos:
                raise HTTPException(status_code=400, detail="Empresa no configurada.")

            logger.info("[PDF_GENERADOR] Paso: extraer datos SQL por folio")
            data_sql = extraer_datos(folio, db)
            if not data_sql:
                raise HTTPException(status_code=404, detail="Folio no encontrado.")

            fields = plantilla.get("fields", {})
            html_raw = fields.get("html", {}).get("stringValue", "")
            if not html_raw:
                raise HTTPException(status_code=400, detail="La plantilla no tiene HTML.")

            tiene_anexos = fields.get("tieneAnexos", {}).get("booleanValue", False)
            ids_anexos = []
            archivos_subidos_adjuntos = []
            if tiene_anexos:
                logger.info("[PDF_GENERADOR] Paso: plantilla con anexos, identificando ids de anexos")
                ids_anexos = self._obtener_ids_anexos_desde_fields(fields)
                logger.info("[PDF_GENERADOR] Paso: plantilla con anexos, obteniendo archivos_subidos")
                archivos_subidos_adjuntos = self._obtener_archivos_subidos_desde_fields(fields)

            variables_html = dict(data_sql)

            # 1. Formatear montos a Moneda ($X,XXX.XX)
            claves_monto = ["{v.total_enganche}", "{v.precio_lista}", "{v.apartado}", "{v.flujo_enganche}", "{v.total_enganche_pagar}"]
            for k in claves_monto:
                if k in variables_html:
                    variables_html[k] = self._formatear_moneda(variables_html[k])

            # 2. Limpiar duplicados de "meses"
            plazo_key = "{v.plazo_financiamiento}"
            if plazo_key in variables_html:
                valor_plazo = str(variables_html[plazo_key]).lower().replace("meses", "").strip()
                variables_html[plazo_key] = valor_plazo
            
            categoria_plantilla = fields.get("categoria", {}).get("stringValue", "")

            if fields.get("categoria", {}).get("stringValue", "").strip().lower() == "cotizaciones":
                logger.info("[PDF_GENERADOR] Paso: construir tabla de pagos para Cotizaciones desde ID")
                html_raw, totales = self._construir_tabla_pagos_cotizaciones(html_raw, folio, db)
                variables_html.update(totales)

            logger.info("[PDF_GENERADOR] Paso: reemplazar etiquetas HTML")
            html_final = self._reemplazar_etiquetas(html_raw, variables_html)

            # Aplicar fondo membretada si está habilitada
            hoja_membretada = fields.get("HojaMembretadaProyecto", {}).get("booleanValue", False)
            if hoja_membretada:
                logger.info("[PDF_GENERADOR] Paso: HojaMembretadaProyecto activa, inyectando fondo")
                imagen_map = fields.get("ImagenMembretada", {}).get("mapValue", {}).get("fields", {})
                if imagen_map:
                    # Obtener la primera imagen disponible
                    primera_imagen_b64 = next(iter(imagen_map.values()), {}).get("stringValue", "")
                    if primera_imagen_b64:
                        html_final = self._inyectar_membretada_fondo(html_final, primera_imagen_b64)

            encabezado_raw = fields.get("encabezado", {}).get("stringValue", "") or ""
            footer_raw = fields.get("footer", {}).get("stringValue", "") or ""
            encabezado_final = self._reemplazar_etiquetas(encabezado_raw, variables_html) if encabezado_raw else ""
            footer_final = self._reemplazar_etiquetas(footer_raw, variables_html) if footer_raw else ""

            nombre_plantilla = fields.get("nombre", {}).get("stringValue", "documento")
            nombre_pdf = f"{self._normalizar_fragmento(nombre_plantilla, fallback='documento')}.pdf"
            tamanoDocumento = fields.get("tamanoDocumento", {}).get("stringValue", "A4")

            pdf_kwargs = {
                "format": tamanoDocumento,
                "print_background": True,
                "prefer_css_page_size": True,
                "scale": 1.0,
                "margin": {"top": "0px", "bottom": "0px", "left": "0px", "right": "0px"},
            }
            if encabezado_final or footer_final:
                pdf_kwargs["display_header_footer"] = True
                pdf_kwargs["header_template"] = encabezado_final or "<span></span>"
                pdf_kwargs["footer_template"] = footer_final or "<span></span>"

            logger.info("[PDF_GENERADOR] Paso: render PDF con Playwright")
            async with async_playwright() as p:
                browser = await p.chromium.launch(args=["--no-sandbox", "--disable-setuid-sandbox"])
                page = await browser.new_page()
                await page.set_content(html_final, wait_until="networkidle")
                await page.emulate_media(media="screen")
                await page.wait_for_load_state("networkidle")
                await page.wait_for_timeout(400)
                pdf_bytes = await page.pdf(**pdf_kwargs)
                await browser.close()

            respuesta = {
                "id_plantilla": id_plantilla,
                "filename": nombre_pdf,
                "content": base64.b64encode(pdf_bytes).decode("utf-8"),
                "content_type": "application/pdf",
                "tamanoDocumento": tamanoDocumento
            }

            if subir_bucket:
                cliente = (
                    data_sql.get("{c1.client_name}")
                    or data_sql.get("{cliente}")
                    or data_sql.get("{cl.cliente}")
                    or data_sql.get("{v.cliente}")
                    or "Cliente"
                )
                ruta = f"Komunah/PlantillasWeb/Categorias/{self._normalizar_fragmento(categoria_plantilla, 'general')}/{self._normalizar_fragmento(cliente, 'Cliente')}"
                respuesta["url_descarga"] = self._subir_pdf_a_bucket(pdf_bytes, ruta, nombre_pdf)

            if archivos_subidos_adjuntos:
                respuesta["archivos_subidos_adjuntos"] = archivos_subidos_adjuntos

            if ids_anexos:
                respuesta["anexos_ids"] = ids_anexos

            logger.info(
                "[PDF_GENERADOR] Salida _generar_pdf_desde_documento | plantilla=%s | filename=%s | anexos_adjuntos=%s | anexos_ids=%s",
                id_plantilla,
                nombre_pdf,
                len(archivos_subidos_adjuntos),
                len(ids_anexos),
            )
            return respuesta
        except HTTPException:
            logger.exception(
                "[PDF_GENERADOR] Error HTTP en _generar_pdf_desde_documento | empresa=%s | plantilla=%s | folio=%s",
                empresa_id,
                id_plantilla,
                folio,
            )
            raise
        except Exception as e:
            logger.exception(
                "[PDF_GENERADOR] Error inesperado en _generar_pdf_desde_documento | empresa=%s | plantilla=%s | folio=%s | error=%s",
                empresa_id,
                id_plantilla,
                folio,
                str(e),
            )
            raise HTTPException(status_code=500, detail=f"Error en _generar_pdf_desde_documento (folio={folio}, plantilla={id_plantilla}): {str(e)}")

    async def _generar_pdf_desde_anexo_id(self, empresa_id: str, id_anexo: str, folio: str, db: Session, subir_bucket: bool = False):
            logger.info(
                "[PDF_GENERADOR] Entrada _generar_pdf_desde_anexo_id | empresa=%s | anexo=%s | folio=%s | subir_bucket=%s",
                empresa_id,
                id_anexo,
                folio,
                subir_bucket,
            )
            try:
                logger.info("[PDF_GENERADOR] Paso: obtener plantilla anexo por id")
                anexo_doc = self.repo.obtener_un_doc_completo_anexos(empresa_id, id_anexo)
                if not anexo_doc:
                    raise HTTPException(status_code=404, detail=f"No existe la plantilla de anexo: {id_anexo}.")

                pack_empresa = _get_providers().get(empresa_id, {})
                extraer_datos = pack_empresa.get("get")
                if not extraer_datos:
                    raise HTTPException(status_code=400, detail="Empresa no configurada.")

                logger.info("[PDF_GENERADOR] Paso: extraer datos SQL por folio para anexo")
                data_sql = extraer_datos(folio, db)
                if not data_sql:
                    raise HTTPException(status_code=404, detail="Folio no encontrado.")

                fields = anexo_doc.get("fields", {})
                html_raw = (
                    fields.get("contenido", {}).get("stringValue")
                    or fields.get("html", {}).get("stringValue", "")
                )
                if not html_raw:
                    raise HTTPException(status_code=400, detail=f"El anexo {id_anexo} no tiene contenido HTML.")
                
                variables_html = dict(data_sql)

                # 1. Formatear montos a Moneda ($X,XXX.XX)
                claves_monto = ["{v.total_enganche}", "{v.precio_lista}", "{v.apartado}", "{v.flujo_enganche}", "{v.total_enganche_pagar}"]
                for k in claves_monto:
                    if k in variables_html:
                        variables_html[k] = self._formatear_moneda(variables_html[k])

                # 2. Limpiar duplicados de "meses"
                plazo_key = "{v.plazo_financiamiento}"
                if plazo_key in variables_html:
                    valor_plazo = str(variables_html[plazo_key]).lower().replace("meses", "").strip()
                    variables_html[plazo_key] = valor_plazo

                if fields.get("subcategorianexo", {}).get("stringValue", "").strip().lower() == "cotizaciones":
                    logger.info("[PDF_GENERADOR] Paso: construir tabla de pagos para Cotizaciones")
                    html_raw, totales = self._construir_tabla_pagos_cotizaciones(html_raw, folio, db)
                    variables_html.update(totales)

                logger.info("[PDF_GENERADOR] Paso: reemplazar etiquetas HTML para anexo")
                html_final = self._reemplazar_etiquetas(html_raw, variables_html)

                # Aplicar fondo membretada si está habilitada
                hoja_membretada = fields.get("HojaMembretadaProyecto", {}).get("booleanValue", False)
                if hoja_membretada:
                    logger.info("[PDF_GENERADOR] Paso: HojaMembretadaProyecto activa, inyectando fondo")
                    imagen_map = fields.get("ImagenMembretada", {}).get("mapValue", {}).get("fields", {})
                    if imagen_map:
                        # Obtener la primera imagen disponible
                        primera_imagen_b64 = next(iter(imagen_map.values()), {}).get("stringValue", "")
                        if primera_imagen_b64:
                            html_final = self._inyectar_membretada_fondo(html_final, primera_imagen_b64)

                nombre_anexo = fields.get("nombre", {}).get("stringValue", "anexo")
                nombre_pdf = f"{self._normalizar_fragmento(nombre_anexo, fallback='anexo')}.pdf"
                tamano_documento = fields.get("tamanoDocumento", {}).get("stringValue", "A4")

                encabezado_raw = fields.get("encabezado", {}).get("stringValue", "") or ""
                footer_raw = fields.get("footer", {}).get("stringValue", "") or ""
                encabezado_final = self._reemplazar_etiquetas(encabezado_raw, variables_html) if encabezado_raw else ""
                footer_final = self._reemplazar_etiquetas(footer_raw, variables_html) if footer_raw else ""

                pdf_kwargs = {
                    "format": tamano_documento,
                    "print_background": True,
                    "prefer_css_page_size": True,
                    "scale": 1.0,
                    "margin": {"top": "0px", "bottom": "0px", "left": "0px", "right": "0px"},
                }
                if encabezado_final or footer_final:
                    pdf_kwargs["display_header_footer"] = True
                    pdf_kwargs["header_template"] = encabezado_final or "<span></span>"
                    pdf_kwargs["footer_template"] = footer_final or "<span></span>"

                logger.info("[PDF_GENERADOR] Paso: render PDF de anexo con Playwright")
                async with async_playwright() as p:
                    browser = await p.chromium.launch(args=["--no-sandbox", "--disable-setuid-sandbox"])
                    page = await browser.new_page()
                    await page.set_content(html_final, wait_until="networkidle")
                    await page.emulate_media(media="screen")
                    await page.wait_for_load_state("networkidle")
                    await page.wait_for_timeout(400)
                    pdf_bytes = await page.pdf(**pdf_kwargs)
                    await browser.close()

                respuesta = {
                    "id_anexo": id_anexo,
                    "filename": nombre_pdf,
                    "content": base64.b64encode(pdf_bytes).decode("utf-8"),
                    "content_type": "application/pdf",
                    "tamanoDocumento": tamano_documento,
                }

                if subir_bucket:
                    cliente = (
                        data_sql.get("{c1.client_name}")
                        or data_sql.get("{cliente}")
                        or data_sql.get("{cl.cliente}")
                        or data_sql.get("{v.cliente}")
                        or "Cliente"
                    )
                    categoria_anexo = fields.get("categoria", {}).get("stringValue", "general")
                    ruta = f"Komunah/Documentos/Anexos/{self._normalizar_fragmento(categoria_anexo, 'general')}/{self._normalizar_fragmento(cliente, 'Cliente')}"
                    respuesta["url_descarga"] = self._subir_pdf_a_bucket(pdf_bytes, ruta, nombre_pdf)

                logger.info(
                    "[PDF_GENERADOR] Salida _generar_pdf_desde_anexo_id | anexo=%s | filename=%s",
                    id_anexo,
                    nombre_pdf,
                )
                return respuesta
            except HTTPException:
                logger.exception(
                    "[PDF_GENERADOR] Error HTTP en _generar_pdf_desde_anexo_id | empresa=%s | anexo=%s | folio=%s",
                    empresa_id,
                    id_anexo,
                    folio,
                )
                raise
            except Exception as e:
                logger.exception(
                    "[PDF_GENERADOR] Error inesperado en _generar_pdf_desde_anexo_id | empresa=%s | anexo=%s | folio=%s | error=%s",
                    empresa_id,
                    id_anexo,
                    folio,
                    str(e),
                )
                raise HTTPException(status_code=500, detail=f"Error generando anexo {id_anexo} para folio {folio}: {str(e)}")

#endregion

#region Llamadas principales

    async def generar_pdfs_desde_plantillas(self, empresa_id: str, ids_plantillas: Optional[List[str]], folio: str, db: Session, subir_bucket: bool = False):
        """Genera una lista de PDFs usando una lista de IDs de plantillas jurídicas."""
        logger.info(
            "[PDF_GENERADOR] Entrada generar_pdfs_desde_plantillas | empresa=%s | folio=%s | subir_bucket=%s",
            empresa_id,
            folio,
            subir_bucket,
        )
        try:
            ids_limpios = [str(doc_id).strip() for doc_id in (ids_plantillas or []) if str(doc_id).strip()]
            logger.info("[PDF_GENERADOR] Paso: normalizar ids de plantillas | total=%s", len(ids_limpios))
            if not ids_limpios:
                logger.info("[PDF_GENERADOR] Salida generar_pdfs_desde_plantillas | resultado=sin_plantillas")
                return []

            resultado = []
            for doc_id in ids_limpios:
                logger.info("[PDF_GENERADOR] Paso: generar PDF para plantilla | id=%s", doc_id)
                pdf = await self._generar_pdf_desde_documento(
                    empresa_id=empresa_id,
                    id_plantilla=doc_id,
                    folio=folio,
                    db=db,
                    subir_bucket=subir_bucket,
                )
                resultado.append(pdf)
                if pdf.get("archivos_subidos_adjuntos"):
                    logger.info(
                        "[PDF_GENERADOR] Paso: anexar archivos_subidos al resultado | plantilla=%s | total_adjuntos=%s",
                        doc_id,
                        len(pdf["archivos_subidos_adjuntos"]),
                    )
                    resultado.extend(pdf["archivos_subidos_adjuntos"])

                if pdf.get("anexos_ids"):
                    logger.info(
                        "[PDF_GENERADOR] Paso: generar PDFs desde anexos IDs | plantilla=%s | total_ids=%s",
                        doc_id,
                        len(pdf["anexos_ids"]),
                    )
                    pdfs_anexos = await self.generar_pdfs_desde_anexos(
                        empresa_id=empresa_id,
                        ids_anexos=pdf["anexos_ids"],
                        folio=folio,
                        db=db,
                        subir_bucket=subir_bucket,
                    )
                    resultado.extend(pdfs_anexos)

            logger.info("[PDF_GENERADOR] Salida generar_pdfs_desde_plantillas | total_documentos=%s", len(resultado))
            return resultado
        except HTTPException:
            logger.exception(
                "[PDF_GENERADOR] Error HTTP en generar_pdfs_desde_plantillas | empresa=%s | folio=%s",
                empresa_id,
                folio,
            )
            raise
        except Exception as e:
            logger.exception(
                "[PDF_GENERADOR] Error inesperado en generar_pdfs_desde_plantillas | empresa=%s | folio=%s | error=%s",
                empresa_id,
                folio,
                str(e),
            )
            raise HTTPException(status_code=500, detail=f"Error en generar_pdfs_desde_plantillas (folio={folio}): {str(e)}")

    async def generar_pdf_barrido_automatico(self, empresa_id: str, id_plantilla: str, folio: str, db: Session):
        logger.info(
            "[PDF_GENERADOR] Entrada generar_pdf_barrido_automatico | empresa=%s | plantilla=%s | folio=%s",
            empresa_id,
            id_plantilla,
            folio,
        )
        try:
            return await self._generar_pdf_desde_documento(
                empresa_id=empresa_id,
                id_plantilla=id_plantilla,
                folio=folio,
                db=db,
                subir_bucket=True,
            )
        except HTTPException:
            logger.exception(
                "[PDF_GENERADOR] Error HTTP en generar_pdf_barrido_automatico | empresa=%s | plantilla=%s | folio=%s",
                empresa_id,
                id_plantilla,
                folio,
            )
            raise
        except Exception as e:
            logger.exception(
                "[PDF_GENERADOR] Error inesperado en generar_pdf_barrido_automatico | empresa=%s | plantilla=%s | folio=%s | error=%s",
                empresa_id,
                id_plantilla,
                folio,
                str(e),
            )
            raise HTTPException(status_code=500, detail=f"Error en generar_pdf_barrido_automatico (folio={folio}, plantilla={id_plantilla}): {str(e)}")

    async def generar_pdfs_desde_anexos(self, empresa_id: str, ids_anexos: Optional[List[str]], folio: str, db: Session, subir_bucket: bool = False):
            logger.info(
                "[PDF_GENERADOR] Entrada generar_pdfs_desde_anexos | empresa=%s | folio=%s | subir_bucket=%s",
                empresa_id,
                folio,
                subir_bucket,
            )
            try:
                ids_limpios = [str(anexo_id).strip() for anexo_id in (ids_anexos or []) if str(anexo_id).strip()]
                logger.info("[PDF_GENERADOR] Paso: normalizar ids de anexos | total=%s", len(ids_limpios))
                if not ids_limpios:
                    logger.info("[PDF_GENERADOR] Salida generar_pdfs_desde_anexos | resultado=sin_anexos")
                    return []

                resultado = []
                for anexo_id in ids_limpios:
                    logger.info("[PDF_GENERADOR] Paso: generar PDF para anexo | id=%s", anexo_id)
                    pdf_anexo = await self._generar_pdf_desde_anexo_id(
                        empresa_id=empresa_id,
                        id_anexo=anexo_id,
                        folio=folio,
                        db=db,
                        subir_bucket=subir_bucket,
                    )
                    resultado.append(pdf_anexo)

                logger.info("[PDF_GENERADOR] Salida generar_pdfs_desde_anexos | total_documentos=%s", len(resultado))
                return resultado
            except HTTPException:
                logger.exception(
                    "[PDF_GENERADOR] Error HTTP en generar_pdfs_desde_anexos | empresa=%s | folio=%s",
                    empresa_id,
                    folio,
                )
                raise
            except Exception as e:
                logger.exception(
                    "[PDF_GENERADOR] Error inesperado en generar_pdfs_desde_anexos | empresa=%s | folio=%s | error=%s",
                    empresa_id,
                    folio,
                    str(e),
                )
                raise HTTPException(status_code=500, detail=f"Error en generar_pdfs_desde_anexos (folio={folio}): {str(e)}")

#endregion

class GenerarPDFDinamico(GenerarPDFUseCase):
    async def generar_pdf_por_id_plantilla(self, empresa_id: str, id_plantilla: str, folio: str, coleccion: str, db: Session, subir_bucket: bool):
        """Genera una lista de PDFs usando una lista de IDs de plantillas jurídicas."""
        logger.info(
            "[PDF_GENERADOR] Entrada generar_pdf_por_id_plantilla | empresa=%s | folio=%s | plantilla=%s | coleccion=%s | subir_bucket=%s",
            empresa_id,
            folio,
            id_plantilla,
            coleccion,
            subir_bucket,
        )
        try:
            logger.info("[PDF_GENERADOR] Paso: generar PDF para colección | colección=%s", coleccion)
            resultado = []
            pdf = await self.generar_pdfs(empresa_id=empresa_id, id_plantilla=id_plantilla, folio=folio, coleccion=coleccion, db=db, subir_bucket=subir_bucket)
            resultado.append(pdf)

            logger.info("[PDF_GENERADOR] Salida generar_pdf_por_id_plantilla | coleccion=%s | total_documentos=%s", coleccion, len(resultado))
            return resultado
        except HTTPException:
            logger.exception(
                "[PDF_GENERADOR] Error HTTP en generar_pdf_por_id_plantilla | empresa=%s | folio=%s | plantilla=%s | coleccion=%s",
                empresa_id,
                folio,
                id_plantilla,
                coleccion,
            )
            raise
        except Exception as e:
            logger.exception(
                "[PDF_GENERADOR] Error inesperado en generar_pdf_por_id_plantilla | empresa=%s | folio=%s | plantilla=%s | coleccion=%s | error=%s",
                empresa_id,
                folio,
                id_plantilla,
                coleccion,
                str(e),
            )
            raise HTTPException(status_code=500, detail=f"Error en generar_pdf_por_id_plantilla (folio={folio}): {str(e)}")

    async def generar_pdfs(self, empresa_id: str, id_plantilla: str, folio: str, coleccion: str, db: Session, subir_bucket: bool):
        """Genera un PDF unificado (Principal + Anexos) desde un documento dinamico."""
        logger.info("[PDF_GENERADOR] Entrada generar_pdfs | empresa=%s | plantilla=%s | folio=%s", empresa_id, id_plantilla, folio)
        try:
            # --- 1. OBTENCIÓN DE PLANTILLA Y DATOS (Tu lógica original) ---
            if coleccion == "DocumentosDinamicos":
                plantilla = self.repo.obtener_un_doc_completo_documentos(empresa_id, id_plantilla)
            else:
                plantilla = self.repo.obtener_un_doc_completo_anexos(empresa_id, id_plantilla)

            if not plantilla:
                raise HTTPException(status_code=404, detail=f"No existe la plantilla: {id_plantilla}")

            pack_empresa = _get_providers().get(empresa_id, {})
            extraer_datos = pack_empresa.get("get")
            data_sql = extraer_datos(folio, db)

            cliente = (
                    data_sql.get("{c1.client_name}")
                    or data_sql.get("{cliente}")
                    or data_sql.get("{cl.cliente}")
                    or data_sql.get("{v.cliente}")
                    or "Cliente"
                )

            fields = plantilla.get("fields", {})
            html_raw = fields.get("html", {}).get("stringValue", "") if coleccion == "DocumentosDinamicos" else fields.get("contenido", {}).get("stringValue", "")

            # --- 2. PROCESAMIENTO DE VARIABLES Y RENDERIZADO PRINCIPAL ---
            variables_html = dict(data_sql)

            claves_monto = ["{v.total_enganche}", "{v.precio_lista}", "{v.apartado}", "{v.flujo_enganche}", "{v.total_enganche_pagar}"]
            for k in claves_monto:
                if k in variables_html: variables_html[k] = self._formatear_moneda(variables_html[k])

            plazo_key = "{v.plazo_financiamiento}"
            if plazo_key in variables_html:
                variables_html[plazo_key] = str(variables_html[plazo_key]).lower().replace("meses", "").strip()

            if fields.get("categoria", {}).get("stringValue", "").strip().lower() == "cotizaciones" or (fields.get("subcategorianexo", {}).get("stringValue", "").strip().lower() == "cotizaciones"):
                html_raw, totales = self._construir_tabla_pagos_cotizaciones(html_raw, folio, db)
                variables_html.update(totales)

            if fields.get("FirmantesEmpresa", {}).get("booleanValue", False) == True:
                html_raw = self._construir_seccion_firmantes_empresa(html_raw, empresa_id, db)
            elif fields.get("FirmasCoopropietarios", {}).get("booleanValue", False) == True:
                html_raw = self._construir_seccion_firmantes_coopropietarios(html_raw, folio, db)
            elif bool(fields.get("FirmantesPersonalizados", {}).get("arrayValue", {}).get("values", [])) and fields.get("FirmantesEmpresa", {}).get("booleanValue", False) == False:
                html_raw = self._construir_seccion_firmantes_personalizados(html_raw, fields, db)
            else:
                pass

            html_final = self._reemplazar_etiquetas(html_raw, variables_html)

            # Aplicar fondo membretada si está habilitada
            hoja_membretada = fields.get("HojaMembretadaProyecto", {}).get("booleanValue", False)
            if hoja_membretada:
                logger.info("[PDF_GENERADOR] Paso: HojaMembretadaProyecto activa, inyectando fondo")
                imagen_map = fields.get("ImagenMembretada", {}).get("mapValue", {}).get("fields", {})
                if imagen_map:
                    # Obtener la primera imagen disponible
                    primera_imagen_b64 = next(iter(imagen_map.values()), {}).get("stringValue", "")
                    if primera_imagen_b64:
                        html_final = self._inyectar_membretada_fondo(html_final, primera_imagen_b64)

            encabezado_raw = fields.get("encabezado", {}).get("stringValue", "") or ""
            footer_raw = fields.get("footer", {}).get("stringValue", "") or ""
            encabezado_final = self._reemplazar_etiquetas(encabezado_raw, variables_html) if encabezado_raw else ""
            footer_final = self._reemplazar_etiquetas(footer_raw, variables_html) if footer_raw else ""

            tamanoDocumento = fields.get("tamanoDocumento", {}).get("stringValue", "A4")

            pdf_kwargs_principal = {
                "format": tamanoDocumento,
                "print_background": True,
                "prefer_css_page_size": True,
                "scale": 1.0,
                "margin": {"top": "0px", "bottom": "0px", "left": "0px", "right": "0px"},
            }
            if encabezado_final or footer_final:
                pdf_kwargs_principal["display_header_footer"] = True
                pdf_kwargs_principal["header_template"] = encabezado_final or "<span></span>"
                pdf_kwargs_principal["footer_template"] = footer_final or "<span></span>"

            # Iniciamos Playwright una sola vez para ser eficientes
            async with async_playwright() as p:
                browser = await p.chromium.launch(args=["--no-sandbox", "--disable-setuid-sandbox"])
                page = await browser.new_page()
                await page.emulate_media(media="screen")

                # Render Principal
                await page.set_content(html_final, wait_until="networkidle")

                pdf_bytes_principal = await page.pdf(**pdf_kwargs_principal)

                merger = PdfWriter()
                merger.append(io.BytesIO(pdf_bytes_principal))

                # --- 3. UNIFICACIÓN CON ANEXOS JURÍDICOS ---
                ids_anexos = []
                if coleccion == "DocumentosDinamicos" and fields.get("tieneAnexos", {}).get("booleanValue", False):
                    ids_anexos = self._obtener_ids_anexos_desde_fields(fields)
                    for id_anexo in ids_anexos:
                        res_anexo = await self.generar_pdfs_anexos(empresa_id, id_anexo, folio, db, subir_bucket=False)
                        merger.append(io.BytesIO(base64.b64decode(res_anexo["content"])))

                # --- 4. NUEVO: PROCESAMIENTO DE IMÁGENES (archivos_subidos) ---
                if coleccion == "DocumentosDinamicos":
                    archivos_map = fields.get("archivos_subidos", {}).get("mapValue", {}).get("fields", {})
                    archivos_meta = fields.get("archivos_subidos_meta", {}).get("mapValue", {}).get("fields", {})

                    for nombre_archivo, nodo_b64 in archivos_map.items():
                        base64_data = nodo_b64.get("stringValue")
                        if not base64_data: continue

                        # Obtener mime_type del meta (ej: image/png)
                        meta_data = archivos_meta.get(nombre_archivo, {}).get("mapValue", {}).get("fields", {})
                        mime_type = meta_data.get("mime_type", {}).get("stringValue", "image/jpeg")

                        logger.info("[PDF_GENERADOR] Agregando imagen como página PDF: %s", nombre_archivo)

                        # HTML simple para centrar la imagen en la página
                        html_imagen = f"""
                        <html>
                            <body style="margin:0; padding:0; display:flex; justify-content:center; align-items:center; height:100vh;">
                                <img src="data:{mime_type};base64,{base64_data}" style="max-width:100%; max-height:100%; object-fit:contain;">
                            </body>
                        </html>
                        """
                        await page.set_content(html_imagen, wait_until="networkidle")
                        pdf_bytes_img = await page.pdf(
                            format=tamanoDocumento, 
                            print_background=True, 
                            prefer_css_page_size=True, 
                            scale=1.0, 
                            margin={"top": "0px", "bottom": "0px", "left": "0px", "right": "0px"}
                        )
                        merger.append(io.BytesIO(pdf_bytes_img))

                await browser.close()

            # --- 5. EXPORTACIÓN Y RESPUESTA ---
            output_stream = io.BytesIO()
            merger.write(output_stream)
            pdf_final_bytes = output_stream.getvalue()
            merger.close()

            # --- 4. RESPUESTA FINAL (Subida a bucket del archivo unificado) ---
            nombre_plantilla = fields.get("nombre", {}).get("stringValue", "documento")
            nombre_pdf = f"{self._normalizar_fragmento(nombre_plantilla)}.pdf"

            respuesta = {
                "id_plantilla": id_plantilla,
                "filename": nombre_pdf,
                "content": base64.b64encode(pdf_final_bytes).decode("utf-8"),
                "content_type": "application/pdf",
                "anexos_ids": ids_anexos
            }

            if subir_bucket:
                categoria = fields.get("categoria", {}).get("stringValue", "general")
                subcategorianexo = fields.get("subcategorianexo", {}).get("stringValue", "general")

                if coleccion == "DocumentosDinamicos":
                    if fields.get("tieneAnexos", {}).get("booleanValue", False) == True:
                        ruta = f"Komunah/Documentos/Completos/{self._normalizar_fragmento(categoria)}/{self._normalizar_fragmento(cliente)}"
                    else:
                        ruta = f"Komunah/Documentos/UnoSolo/{self._normalizar_fragmento(categoria)}/{self._normalizar_fragmento(cliente)}"
                else:
                    ruta = f"Komunah/Documentos/Anexos/{self._normalizar_fragmento(subcategorianexo)}/{self._normalizar_fragmento(cliente)}"

                respuesta["url_descarga"] = self._subir_pdf_a_bucket(pdf_final_bytes, ruta, nombre_pdf)
            respuesta.pop("content", None)
            return respuesta

        except Exception as e:
            logger.exception("[PDF_GENERADOR] Error en generar_pdfs: %s", str(e))
            raise HTTPException(status_code=500, detail=str(e))

    async def generar_pdfs_anexos(self, empresa_id: str, id_anexo: str, folio: str, db: Session, subir_bucket: bool):
        logger.info(
                "[PDF_GENERADOR] Entrada generar_pdfs_anexos | empresa=%s | anexo=%s | folio=%s | subir_bucket=%s",
                empresa_id,
                id_anexo,
                folio,
                subir_bucket,
            )
        try:
            logger.info("[PDF_GENERADOR] Paso: obtener plantilla anexo por id")
            anexo_doc = self.repo.obtener_un_doc_completo_anexos(empresa_id, id_anexo)
            if not anexo_doc:
                raise HTTPException(status_code=404, detail=f"No existe la plantilla de anexo: {id_anexo}.")

            pack_empresa = _get_providers().get(empresa_id, {})
            extraer_datos = pack_empresa.get("get")
            if not extraer_datos:
                raise HTTPException(status_code=400, detail="Empresa no configurada.")

            logger.info("[PDF_GENERADOR] Paso: extraer datos SQL por folio para anexo")
            data_sql = extraer_datos(folio, db)
            if not data_sql:
                raise HTTPException(status_code=404, detail="Folio no encontrado.")
            
            cliente = (
                    data_sql.get("{c1.client_name}")
                    or data_sql.get("{cliente}")
                    or data_sql.get("{cl.cliente}")
                    or data_sql.get("{v.cliente}")
                    or "Cliente"
                )

            fields = anexo_doc.get("fields", {})
            html_raw = fields.get("contenido", {}).get("stringValue")
            if not html_raw:
                raise HTTPException(status_code=400, detail=f"El anexo {id_anexo} no tiene contenido HTML.")

            variables_html = dict(data_sql)

            # 1. Formatear montos a Moneda ($X,XXX.XX)
            claves_monto = ["{v.total_enganche}", "{v.precio_lista}", "{v.apartado}", "{v.flujo_enganche}", "{v.total_enganche_pagar}"]
            for k in claves_monto:
                if k in variables_html:
                    variables_html[k] = self._formatear_moneda(variables_html[k])

            # 2. Limpiar duplicados de "meses"
            plazo_key = "{v.plazo_financiamiento}"
            if plazo_key in variables_html:
                valor_plazo = str(variables_html[plazo_key]).lower().replace("meses", "").strip()
                variables_html[plazo_key] = valor_plazo

            if fields.get("subcategorianexo", {}).get("stringValue", "").strip().lower() == "cotizaciones":
                logger.info("[PDF_GENERADOR] Paso: construir tabla de pagos para Cotizaciones")
                html_raw, totales = self._construir_tabla_pagos_cotizaciones(html_raw, folio, db)
                variables_html.update(totales)

            if fields.get("FirmantesEmpresa", {}).get("booleanValue", False) == True:
                html_raw = self._construir_seccion_firmantes_empresa(html_raw, empresa_id, db)
            elif fields.get("FirmasCoopropietarios", {}).get("booleanValue", False) == True:
                html_raw = self._construir_seccion_firmantes_coopropietarios(html_raw, folio, db)
            elif bool(fields.get("FirmantesPersonalizados", {}).get("arrayValue", {}).get("values", [])) and fields.get("FirmantesEmpresa", {}).get("booleanValue", False) == False:
                html_raw = self._construir_seccion_firmantes_personalizados(html_raw, fields, db)
            else:
                pass

            logger.info("[PDF_GENERADOR] Paso: reemplazar etiquetas HTML para anexo")
            html_final = self._reemplazar_etiquetas(html_raw, variables_html)

            # Aplicar fondo membretada si está habilitada
            hoja_membretada = fields.get("HojaMembretadaProyecto", {}).get("booleanValue", False)
            if hoja_membretada:
                logger.info("[PDF_GENERADOR] Paso: HojaMembretadaProyecto activa, inyectando fondo")
                imagen_map = fields.get("ImagenMembretada", {}).get("mapValue", {}).get("fields", {})
                if imagen_map:
                    # Obtener la primera imagen disponible
                    primera_imagen_b64 = next(iter(imagen_map.values()), {}).get("stringValue", "")
                    if primera_imagen_b64:
                        html_final = self._inyectar_membretada_fondo(html_final, primera_imagen_b64)

            nombre_anexo = fields.get("nombre", {}).get("stringValue", "anexo")
            nombre_pdf = f"{self._normalizar_fragmento(nombre_anexo, fallback='anexo')}.pdf"
            tamano_documento = fields.get("tamanoDocumento", {}).get("stringValue", "A4")

            encabezado_raw = fields.get("encabezado", {}).get("stringValue", "") or ""
            footer_raw = fields.get("footer", {}).get("stringValue", "") or ""
            encabezado_final = self._reemplazar_etiquetas(encabezado_raw, variables_html) if encabezado_raw else ""
            footer_final = self._reemplazar_etiquetas(footer_raw, variables_html) if footer_raw else ""

            pdf_kwargs_anexo = {
                "format": tamano_documento,
                "print_background": True,
                "prefer_css_page_size": True,
                "scale": 1.0,
                "margin": {"top": "0px", "bottom": "0px", "left": "0px", "right": "0px"},
            }
            if encabezado_final or footer_final:
                pdf_kwargs_anexo["display_header_footer"] = True
                pdf_kwargs_anexo["header_template"] = encabezado_final or "<span></span>"
                pdf_kwargs_anexo["footer_template"] = footer_final or "<span></span>"

            logger.info("[PDF_GENERADOR] Paso: render PDF de anexo con Playwright")
            async with async_playwright() as p:
                browser = await p.chromium.launch(args=["--no-sandbox", "--disable-setuid-sandbox"])
                page = await browser.new_page()
                await page.emulate_media(media="screen")
                await page.set_content(html_final, wait_until="networkidle")
                pdf_bytes = await page.pdf(**pdf_kwargs_anexo)
                await browser.close()

            respuesta = {
                    "id_anexo": id_anexo,
                    "filename": nombre_pdf,
                    "content": base64.b64encode(pdf_bytes).decode("utf-8"),
                    "content_type": "application/pdf",
                    "tamanoDocumento": tamano_documento,
                }

            if subir_bucket:
                
                categoria_anexo = fields.get("categoria", {}).get("stringValue", "general")
                ruta = f"Komunah/Documentos/Anexos/{self._normalizar_fragmento(categoria_anexo, 'general')}/{self._normalizar_fragmento(cliente, 'Cliente')}"
                respuesta["url_descarga"] = self._subir_pdf_a_bucket(pdf_bytes, ruta, nombre_pdf)

                respuesta.pop("content", None)

            logger.info(
                    "[PDF_GENERADOR] Salida generar_pdfs_anexos | anexo=%s | filename=%s",
                    id_anexo,
                    nombre_pdf,
                )
            return respuesta
        except HTTPException:
            logger.exception(
                    "[PDF_GENERADOR] Error HTTP en generar_pdfs_anexos | empresa=%s | anexo=%s | folio=%s",
                    empresa_id,
                    id_anexo,
                    folio,
                )
            raise
        except Exception as e:
            logger.exception(
                    "[PDF_GENERADOR] Error inesperado en generar_pdfs_anexos | empresa=%s | anexo=%s | folio=%s | error=%s",
                    empresa_id,
                    id_anexo,
                    folio,
                    str(e),
                )
            raise HTTPException(status_code=500, detail=f"Error generando anexo {id_anexo} para folio {folio}: {str(e)}")
