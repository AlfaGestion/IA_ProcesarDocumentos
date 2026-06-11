# -*- coding: utf-8 -*-
"""
configurar_layout_factura.py

Editor visual de layouts de facturas por proveedor.

- Carga imagen o PDF de ejemplo.
- Permite marcar zonas y columnas con el mouse.
- Busca/proponer proveedor en SQL Server (Vt_Proveedores).
- Guarda el layout JSON en TA_CONFIGURACION como IA_LYT_<codigo>.

Dependencias sugeridas:
  pip install pyodbc pillow pytesseract pdfplumber pymupdf
"""

from __future__ import annotations

import argparse
import base64
import concurrent.futures
import dataclasses
import io
import json
import os
import queue
import re
import shutil
import sys
import threading
import webbrowser
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

try:
    import tkinter as tk
    from tkinter import filedialog, messagebox, ttk
except Exception:
    tk = None
    filedialog = None
    messagebox = None
    ttk = None

try:
    from PIL import Image, ImageDraw, ImageOps, ImageTk
except Exception:
    Image = None
    ImageDraw = None
    ImageOps = None
    ImageTk = None

try:
    import pyodbc
except Exception:
    pyodbc = None

try:
    import pdfplumber
except Exception:
    pdfplumber = None

try:
    import fitz
except Exception:
    fitz = None

try:
    import pytesseract
    import subprocess as _sp, os as _os
    def _find_tesseract_cmd() -> None:
        try:
            _sp.run([pytesseract.pytesseract.tesseract_cmd, "--version"], capture_output=True, timeout=5)
            return
        except Exception:
            pass
        for _p in [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
            _os.path.join(_os.environ.get("LOCALAPPDATA", ""), "Tesseract-OCR", "tesseract.exe"),
        ]:
            if _p and _os.path.isfile(_p):
                pytesseract.pytesseract.tesseract_cmd = _p
                return
    _find_tesseract_cmd()
    del _find_tesseract_cmd, _sp, _os
except Exception:
    pytesseract = None

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None

from ia_backend_transport import backend_enabled, call_backend


APP_TITLE = "Configurar Layout Factura"
LAST_CONN_FILE = (
    Path(sys.executable).resolve().parent
    if getattr(sys, "frozen", False)
    else Path(__file__).resolve().parent
) / "layout_config_last_connection.json"
DEFAULT_SERVER = "SERVER-ALFAVB6"
DEFAULT_DATABASE = "ALFANET"
DEFAULT_USER = "ALFANET"
DEFAULT_PASSWORD = "ALFANET"
DEFAULT_DRIVER = "ODBC Driver 18 for SQL Server"
DEFAULT_LAYOUT_GROUP = "IA_COMPRAS"

ZONE_TYPES = [
    "proveedor",
    "cabecera",
    "cliente",
    "detalle",
    "totales",
    "cae",
    "observaciones",
]

DETAIL_FIELDS = [
    "Codigo_Articulo",
    "Descripcion",
    "UD",
    "Importe_Lista",
    "Cantidad",
    "% Dto1",
    "% Dto2",
    "Importe_Neto",
    "Total",
    "AuxNroLote",
    "AuxNroSerie",
    "IVA",
    "Impuestos internos",
    "Bl/Pq",
    "Moneda",
    "Tot.Imp.Int",
]

ZONE_COLORS = {
    "proveedor": "#D33F49",
    "cabecera": "#2A6F97",
    "cliente": "#4C956C",
    "detalle": "#FF9F1C",
    "totales": "#6A4C93",
    "cae": "#1982C4",
    "observaciones": "#5C677D",
}

ZONE_HELP = {
    "proveedor": "Emisor del comprobante: razón social, CUIT, domicilio y datos fiscales.",
    "cabecera": "Datos generales: tipo, letra, punto de venta, número, fecha, condición de venta.",
    "cliente": "Receptor/comprador: nombre, CUIT/documento, domicilio, localidad, condición IVA.",
    "detalle": "Tabla de ítems: renglones con artículos, cantidades, precios y totales.",
    "totales": "Subtotales, neto gravado, IVA, total general y percepciones.",
    "cae": "Código de Autorización Electrónica (CAE) y fecha de vencimiento.",
    "observaciones": "Notas, leyendas, instrucciones de pago u otros textos adicionales.",
}

DETAIL_FIELD_HELP = {
    "Codigo_Articulo": "Código del artículo o producto en la factura.",
    "Descripcion": "Descripción o nombre del artículo.",
    "UD": "Unidad de medida (kg, un, caja, etc.).",
    "Importe_Lista": "Precio unitario de lista antes de descuentos.",
    "Cantidad": "Cantidad de unidades del artículo.",
    "% Dto1": "Primer porcentaje de descuento.",
    "% Dto2": "Segundo porcentaje de descuento.",
    "Importe_Neto": "Importe neto por renglón (después de descuentos).",
    "Total": "Total de la línea (precio × cantidad aplicando descuentos).",
    "AuxNroLote": "Número de lote (dato auxiliar opcional).",
    "AuxNroSerie": "Número de serie (dato auxiliar opcional).",
    "IVA": "Porcentaje o importe de IVA aplicado al renglón.",
    "Impuestos internos": "Impuestos internos adicionales sobre el artículo.",
    "Bl/Pq": "Bultos o paquetes del renglón.",
    "Moneda": "Moneda de la operación.",
    "Tot.Imp.Int": "Total de impuestos internos de la línea.",
}

PROVIDER_CODE_CANDIDATES = ["CODIGO", "CUENTA", "IDPROVEEDOR", "ID", "COD_PROVEEDOR"]
PROVIDER_NAME_CANDIDATES = ["NOMBRE", "RAZON_SOCIAL", "RAZONSOCIAL", "DESCRIPCION"]
PROVIDER_CUIT_CANDIDATES = ["CUIT", "NUMERO_CUIT", "NROCUIT", "DOCUMENTO", "NRODOCUMENTO", "NUMERO_DOCUMENTO"]
PROVIDER_ADDR_CANDIDATES = ["DOMICILIO", "DIRECCION", "CALLE", "LOCALIDAD"]
DEFAULT_AI_MODEL = "gpt-4.1-mini"
DEFAULT_DETECTION_MODEL = "gpt-4.1"
AI_MODELS = [
    "gpt-4.1",
    "gpt-4.1-mini",
    "gpt-4o",
    "gpt-4o-mini",
]


@dataclass
class AppConfig:
    server: str = DEFAULT_SERVER
    database: str = DEFAULT_DATABASE
    user: str = DEFAULT_USER
    password: str = DEFAULT_PASSWORD
    driver: str = DEFAULT_DRIVER
    tesseract_cmd: str = ""
    last_file: str = ""


@dataclass
class DocumentState:
    path: str = ""
    file_type: str = ""
    page: int = 1
    image_original: Optional[Any] = None
    image_display: Optional[Any] = None
    display_scale: float = 1.0
    display_offset_x: int = 0
    display_offset_y: int = 0
    original_width: int = 0
    original_height: int = 0
    ocr_text_cache: Dict[str, str] = field(default_factory=dict)


@dataclass
class ProviderMatch:
    code: str
    name: str
    cuit: str = ""
    address: str = ""
    score: float = 0.0


def load_last_connection() -> AppConfig:
    if LAST_CONN_FILE.exists():
        try:
            data = json.loads(LAST_CONN_FILE.read_text(encoding="utf-8"))
            return AppConfig(
                server=str(data.get("server") or DEFAULT_SERVER),
                database=str(data.get("database") or DEFAULT_DATABASE),
                user=str(data.get("user") or DEFAULT_USER),
                password=str(data.get("password") or DEFAULT_PASSWORD),
                driver=str(data.get("driver") or DEFAULT_DRIVER),
                tesseract_cmd=str(data.get("tesseract_cmd") or ""),
                last_file=str(data.get("last_file") or ""),
            )
        except Exception:
            pass
    return AppConfig()


def save_last_connection(cfg: AppConfig) -> None:
    LAST_CONN_FILE.write_text(
        json.dumps(dataclasses.asdict(cfg), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )


def normalize_text_key(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(value or "").upper())


def only_digits(value: str) -> str:
    return re.sub(r"\D+", "", str(value or ""))


def extract_cuit_candidates(text: str) -> List[str]:
    if not text:
        return []
    found: List[str] = []
    patterns = [
        r"\b(\d{2}-\d{8}-\d)\b",
        r"\b(\d{11})\b",
    ]
    for pattern in patterns:
        for match in re.findall(pattern, text):
            digits = only_digits(match)
            if len(digits) == 11 and digits not in found:
                found.append(digits)
    return found


def normalize_rect(x1: float, y1: float, x2: float, y2: float) -> Dict[str, float]:
    return {
        "x1": round(min(x1, x2), 6),
        "y1": round(min(y1, y2), 6),
        "x2": round(max(x1, x2), 6),
        "y2": round(max(y1, y2), 6),
    }


def rect_to_text(rect: Dict[str, float]) -> str:
    return f"x1={rect['x1']:.4f}, y1={rect['y1']:.4f}, x2={rect['x2']:.4f}, y2={rect['y2']:.4f}"


def app_dir() -> Path:
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def load_env_near_app() -> None:
    if load_dotenv is None:
        return
    env_path = app_dir() / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=str(env_path), override=False)
    else:
        load_dotenv(override=False)


def sanitize_json_text(s: str) -> str:
    s = s.strip()
    s = s.lstrip("\ufeff")
    return re.sub(r",\s*([}\]])", r"\1", s)


def extract_first_json(text: str) -> Dict[str, Any]:
    if not text:
        raise ValueError("Respuesta vacía del modelo.")
    s = text.strip()
    if s.startswith("{") and s.endswith("}"):
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return json.loads(sanitize_json_text(s))
    start = s.find("{")
    end = s.rfind("}")
    if start >= 0 and end > start:
        candidate = s[start:end + 1]
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            return json.loads(sanitize_json_text(candidate))
    raise ValueError("No se pudo extraer JSON de la respuesta IA.")


def pil_image_to_content_block(image: Any, *, max_side: int = 2200, jpeg_quality: int = 88) -> Dict[str, Any]:
    img = image.convert("RGB")
    w, h = img.size
    if max(w, h) > max_side:
        img.thumbnail((max_side, max_side))
    buf = io.BytesIO()
    img.save(buf, format="JPEG", quality=max(50, min(int(jpeg_quality), 95)))
    b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
    return {"type": "input_image", "image_url": f"data:image/jpeg;base64,{b64}"}


def call_backend_with_hard_timeout(
    *,
    content_blocks: List[Dict[str, Any]],
    model: str,
    max_output_tokens: int,
    text: Optional[Dict[str, Any]] = None,
    source_filename: Optional[str] = None,
    timeout_seconds: int = 120,
) -> str:
    def _run() -> str:
        return call_backend(
            content_blocks=content_blocks,
            model=model,
            max_output_tokens=max_output_tokens,
            text=text,
            source_filename=source_filename,
            timeout_seconds=timeout_seconds,
        )

    with concurrent.futures.ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(_run)
        try:
            return future.result(timeout=max(1, int(timeout_seconds)))
        except concurrent.futures.TimeoutError as e:
            executor.shutdown(wait=False, cancel_futures=True)
            raise RuntimeError(f"Timeout de IA: superó el límite de {int(timeout_seconds)}s.") from e


def build_layout_json(
    *,
    provider_code: str,
    provider_name: str,
    provider_cuit: str,
    source_file: str,
    file_type: str,
    page: int,
    original_width: int,
    original_height: int,
    zones: Dict[str, Dict[str, float]],
    detail_columns: List[Dict[str, Any]],
    prompt_file: str,
) -> Dict[str, Any]:
    zone_payload: Dict[str, Any] = {z: None for z in ZONE_TYPES}
    zone_payload.update(zones)
    return {
        "version": 1,
        "proveedor": {
            "codigo": provider_code.strip(),
            "nombre": provider_name.strip(),
            "cuit": provider_cuit.strip(),
        },
        "origen": {
            "archivo_ejemplo": source_file,
            "tipo_archivo": file_type,
            "pagina": int(page),
        },
        "imagen_base": {
            "ancho": int(original_width),
            "alto": int(original_height),
        },
        "zonas": zone_payload,
        "detalle_columnas": detail_columns,
        "reglas": {
            "ignorar_columnas": ["Precio Sug", "Precio Sugerido"],
            "validar_suma": True,
            "columna_suma": "Total",
            "comparar_con_total": "Neto gravado",
            "tolerancia": 0.05,
            "detalle_columnas_relativas_a": "imagen_base.ancho",
        },
        "prompt_file": prompt_file.strip(),
    }


class SqlServerClient:
    def __init__(self, config: AppConfig):
        self.config = config

    def build_conn_str(self) -> str:
        return (
            f"DRIVER={{{self.config.driver}}};"
            f"SERVER={self.config.server};"
            f"DATABASE={self.config.database};"
            f"UID={self.config.user};"
            f"PWD={self.config.password};"
            "TrustServerCertificate=yes;"
        )

    def connect(self):
        if pyodbc is None:
            raise RuntimeError("pyodbc no está instalado. Ejecutá: pip install pyodbc")
        return pyodbc.connect(self.build_conn_str(), timeout=10)

    def test_connection(self) -> Tuple[bool, str]:
        try:
            with self.connect():
                return True, f"Conexión OK a {self.config.server} / {self.config.database}"
        except Exception as e:
            return False, str(e)

    def get_view_columns(self, object_name: str) -> List[str]:
        with self.connect() as conn:
            cur = conn.cursor()
            cur.execute(f"SELECT TOP 0 * FROM {object_name}")
            return [str(col[0]) for col in (cur.description or [])]

    def list_required_insert_columns(self, table_name: str) -> List[str]:
        sql = """
        SELECT c.name
        FROM sys.columns c
        INNER JOIN sys.objects o ON o.object_id = c.object_id
        LEFT JOIN sys.default_constraints d ON d.parent_object_id = c.object_id AND d.parent_column_id = c.column_id
        WHERE o.name = ?
          AND o.type = 'U'
          AND c.is_nullable = 0
          AND c.is_identity = 0
          AND d.object_id IS NULL
          AND c.name NOT IN ('Clave', 'ValorAux', 'Grupo')
        ORDER BY c.column_id
        """
        with self.connect() as conn:
            rows = conn.cursor().execute(sql, table_name).fetchall()
        return [str(r[0]) for r in rows]

    def upsert_layout(self, clave: str, valor_aux: str) -> None:
        required = self.list_required_insert_columns("TA_CONFIGURACION")
        if required:
            raise RuntimeError(
                "TA_CONFIGURACION requiere columnas NOT NULL sin default además de Clave/ValorAux/Grupo: "
                + ", ".join(required)
            )

        # VALOR = código de proveedor (la parte después de "IA_LYT_"), para legibilidad
        codigo = clave.removeprefix("IA_LYT_")

        sql_exists = "SELECT 1 FROM TA_CONFIGURACION WHERE Clave = ?"
        sql_update = "UPDATE TA_CONFIGURACION SET Valor = ?, ValorAux = ?, Grupo = ? WHERE Clave = ?"
        sql_insert = "INSERT INTO TA_CONFIGURACION (Clave, Grupo, Valor, ValorAux) VALUES (?, ?, ?, ?)"

        with self.connect() as conn:
            cur = conn.cursor()
            exists = cur.execute(sql_exists, clave).fetchone() is not None
            if exists:
                cur.execute(sql_update, codigo, valor_aux, DEFAULT_LAYOUT_GROUP, clave)
            else:
                cur.execute(sql_insert, clave, DEFAULT_LAYOUT_GROUP, codigo, valor_aux)
            conn.commit()

    def get_layout_payload(self, clave: str) -> Optional[str]:
        sql = """
        SELECT TOP 1 CAST(ValorAux AS NVARCHAR(MAX))
        FROM TA_CONFIGURACION
        WHERE Clave = ? AND Grupo = ?
        """
        with self.connect() as conn:
            row = conn.cursor().execute(sql, clave, DEFAULT_LAYOUT_GROUP).fetchone()
            if row and row[0] is not None:
                return str(row[0])

        sql_fallback = """
        SELECT TOP 1 CAST(ValorAux AS NVARCHAR(MAX))
        FROM TA_CONFIGURACION
        WHERE Clave = ?
        """
        with self.connect() as conn:
            row = conn.cursor().execute(sql_fallback, clave).fetchone()
            return str(row[0]) if row and row[0] is not None else None

    def delete_layout(self, clave: str) -> None:
        with self.connect() as conn:
            conn.cursor().execute(
                "DELETE FROM TA_CONFIGURACION WHERE Clave = ? AND Grupo = ?",
                clave,
                DEFAULT_LAYOUT_GROUP,
            )
            conn.commit()


class ProviderLookup:
    def __init__(self, sql_client: SqlServerClient):
        self.sql_client = sql_client
        self.columns = {}

    def detect_provider_columns(self) -> Dict[str, Optional[str]]:
        cols = self.sql_client.get_view_columns("Vt_Proveedores")
        normalized = {normalize_text_key(col): col for col in cols}

        def pick(candidates: Sequence[str]) -> Optional[str]:
            for cand in candidates:
                key = normalize_text_key(cand)
                if key in normalized:
                    return normalized[key]
            for key, original in normalized.items():
                if any(key.startswith(normalize_text_key(c)) or normalize_text_key(c) in key for c in candidates):
                    return original
            return None

        self.columns = {
            "code": pick(PROVIDER_CODE_CANDIDATES),
            "name": pick(PROVIDER_NAME_CANDIDATES),
            "cuit": pick(PROVIDER_CUIT_CANDIDATES),
            "address": pick(PROVIDER_ADDR_CANDIDATES),
        }
        return self.columns

    def _select_columns(self) -> Tuple[str, str, str, str]:
        if not self.columns:
            self.detect_provider_columns()
        code_col = self.columns.get("code")
        name_col = self.columns.get("name")
        if not code_col or not name_col:
            raise RuntimeError(
                "No se pudieron detectar las columnas principales de Vt_Proveedores "
                "(código y nombre). Podés cargar el código manualmente."
            )
        cuit_col = self.columns.get("cuit") or name_col
        addr_col = self.columns.get("address") or name_col
        return code_col, name_col, cuit_col, addr_col

    def search_provider(
        self,
        *,
        cuit: str = "",
        text: str = "",
        limit: int = 50,
    ) -> List[ProviderMatch]:
        code_col, name_col, cuit_col, addr_col = self._select_columns()
        sql = f"""
        SELECT TOP {int(limit)}
            CAST([{code_col}] AS NVARCHAR(100)) AS code,
            CAST([{name_col}] AS NVARCHAR(250)) AS name,
            CAST([{cuit_col}] AS NVARCHAR(100)) AS cuit,
            CAST([{addr_col}] AS NVARCHAR(250)) AS address
        FROM Vt_Proveedores
        WHERE 1=1
        """
        params: List[Any] = []
        if cuit:
            sql += f" AND REPLACE(REPLACE(REPLACE(CAST([{cuit_col}] AS NVARCHAR(100)), '-', ''), '.', ''), ' ', '') LIKE ?"
            params.append(f"%{only_digits(cuit)}%")
        if text.strip():
            sql += (
                f" AND (CAST([{name_col}] AS NVARCHAR(250)) LIKE ? "
                f"OR CAST([{addr_col}] AS NVARCHAR(250)) LIKE ?)"
            )
            like_value = f"%{text.strip()}%"
            params.extend([like_value, like_value])
        sql += f" ORDER BY CAST([{name_col}] AS NVARCHAR(250))"

        with self.sql_client.connect() as conn:
            rows = conn.cursor().execute(sql, *params).fetchall()

        results: List[ProviderMatch] = []
        for row in rows:
            match = ProviderMatch(
                code=str(row[0] or "").strip(),
                name=str(row[1] or "").strip(),
                cuit=only_digits(str(row[2] or "")),
                address=str(row[3] or "").strip(),
            )
            match.score = self._score_match(match, cuit=cuit, text=text)
            results.append(match)
        results.sort(key=lambda x: (-x.score, x.name.upper(), x.code))
        return results

    def search_best_effort(
        self,
        *,
        cuit: str = "",
        text: str = "",
        limit: int = 50,
    ) -> List[ProviderMatch]:
        if cuit:
            by_cuit = self.search_provider(cuit=cuit, text="", limit=limit)
            if by_cuit:
                return by_cuit
        if text.strip():
            by_text = self.search_provider(cuit="", text=text, limit=limit)
            if by_text:
                return by_text
        if cuit and text.strip():
            return self.search_provider(cuit=cuit, text=text, limit=limit)
        return []

    def search_saved_layouts(
        self,
        *,
        cuit: str = "",
        text: str = "",
        limit: int = 50,
    ) -> List[ProviderMatch]:
        code_col, name_col, cuit_col, addr_col = self._select_columns()
        sql = f"""
        SELECT TOP {int(limit)}
            CAST(v.[{code_col}] AS NVARCHAR(100)) AS code,
            CAST(v.[{name_col}] AS NVARCHAR(250)) AS name,
            CAST(v.[{cuit_col}] AS NVARCHAR(100)) AS cuit,
            CAST(v.[{addr_col}] AS NVARCHAR(250)) AS address
        FROM Vt_Proveedores v
        WHERE EXISTS (
            SELECT 1
            FROM TA_CONFIGURACION t
            WHERE t.Clave = 'IA_LYT_' + CAST(v.[{code_col}] AS NVARCHAR(100))
              AND (t.Grupo = ? OR t.Grupo IS NULL OR LTRIM(RTRIM(CAST(t.Grupo AS NVARCHAR(100)))) = '')
        )
        """
        params: List[Any] = [DEFAULT_LAYOUT_GROUP]
        if cuit:
            sql += f" AND REPLACE(REPLACE(REPLACE(CAST(v.[{cuit_col}] AS NVARCHAR(100)), '-', ''), '.', ''), ' ', '') LIKE ?"
            params.append(f"%{only_digits(cuit)}%")
        if text.strip():
            sql += (
                f" AND (CAST(v.[{name_col}] AS NVARCHAR(250)) LIKE ? "
                f"OR CAST(v.[{addr_col}] AS NVARCHAR(250)) LIKE ?)"
            )
            like_value = f"%{text.strip()}%"
            params.extend([like_value, like_value])
        sql += f" ORDER BY CAST(v.[{name_col}] AS NVARCHAR(250))"

        with self.sql_client.connect() as conn:
            rows = conn.cursor().execute(sql, *params).fetchall()

        results: List[ProviderMatch] = []
        for row in rows:
            match = ProviderMatch(
                code=str(row[0] or "").strip(),
                name=str(row[1] or "").strip(),
                cuit=only_digits(str(row[2] or "")),
                address=str(row[3] or "").strip(),
            )
            match.score = self._score_match(match, cuit=cuit, text=text)
            results.append(match)
        results.sort(key=lambda x: (-x.score, x.name.upper(), x.code))
        return results

    @staticmethod
    def _score_match(match: ProviderMatch, *, cuit: str = "", text: str = "") -> float:
        score = 0.0
        cuit_digits = only_digits(cuit)
        if cuit_digits and match.cuit:
            if match.cuit == cuit_digits:
                score += 100.0
            elif cuit_digits in match.cuit:
                score += 60.0
        text_norm = text.strip().upper()
        if text_norm:
            if text_norm in match.name.upper():
                score += 30.0
            if text_norm in match.address.upper():
                score += 15.0
        return score


class DocumentImageLoader:
    def open_document_as_image(self, path: str, page: int = 1) -> Tuple[Any, Dict[str, Any]]:
        if Image is None:
            raise RuntimeError("Pillow no está instalado. Ejecutá: pip install pillow")
        p = Path(path)
        ext = p.suffix.lower()
        if ext in {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}:
            img = Image.open(path).convert("RGB")
            return img, {
                "file_type": "imagen",
                "page": 1,
                "original_width": img.width,
                "original_height": img.height,
            }
        if ext == ".pdf":
            if fitz is None:
                raise RuntimeError("Para abrir PDF como imagen necesitás pymupdf/fitz: pip install pymupdf")
            doc = fitz.open(path)
            try:
                total_pages = max(1, doc.page_count)
                page_index = max(0, min(page - 1, total_pages - 1))
                pdf_page = doc.load_page(page_index)
                pix = pdf_page.get_pixmap(matrix=fitz.Matrix(2, 2), alpha=False)
                img = Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
                return img, {
                    "file_type": "pdf",
                    "page": page_index + 1,
                    "total_pages": total_pages,
                    "original_width": img.width,
                    "original_height": img.height,
                }
            finally:
                doc.close()
        raise RuntimeError(f"Tipo de archivo no soportado: {ext}")


class OCRService:
    def __init__(self, tesseract_cmd: str = ""):
        self.tesseract_cmd = (tesseract_cmd or "").strip()

    def _configure_tesseract(self) -> None:
        if pytesseract is None:
            raise RuntimeError(
                "pytesseract no está instalado. Ejecutá: pip install pytesseract "
                "y asegurate de tener Tesseract OCR instalado en Windows."
            )
        if self.tesseract_cmd:
            pytesseract.pytesseract.tesseract_cmd = self.tesseract_cmd

    def extract_text_from_document(self, path: str, page: int = 1) -> str:
        p = Path(path)
        ext = p.suffix.lower()
        if ext == ".pdf" and pdfplumber is not None:
            try:
                with pdfplumber.open(path) as pdf:
                    if 0 <= page - 1 < len(pdf.pages):
                        txt = pdf.pages[page - 1].extract_text() or ""
                        if txt.strip():
                            return txt
            except Exception:
                pass
        image, _ = DocumentImageLoader().open_document_as_image(path, page=page)
        return self.ocr_image(image)

    def ocr_image(self, image: Any) -> str:
        self._configure_tesseract()
        try:
            gray = ImageOps.grayscale(image) if ImageOps is not None else image
            return pytesseract.image_to_string(gray, lang="spa")
        except Exception as e:
            raise RuntimeError(
                "No se pudo ejecutar OCR. Verificá la instalación de Tesseract OCR, su PATH "
                "o configurá manualmente la ruta de tesseract.exe."
            ) from e


class LayoutDetectionAIService:
    PROMPT = f"""
Analizá visualmente esta FACTURA DE COMPRA argentina y proponé las zonas de layout.
Respondé SOLO JSON válido, sin texto adicional.

ESQUEMA DE RESPUESTA:
{{
  "proveedor": {{"nombre": "", "cuit": ""}},
  "zonas": {{
    "proveedor":     {{"x1": 0.0, "y1": 0.0, "x2": 0.0, "y2": 0.0}},
    "cabecera":      {{"x1": 0.0, "y1": 0.0, "x2": 0.0, "y2": 0.0}},
    "cliente":       {{"x1": 0.0, "y1": 0.0, "x2": 0.0, "y2": 0.0}},
    "detalle":       {{"x1": 0.0, "y1": 0.0, "x2": 0.0, "y2": 0.0}},
    "totales":       {{"x1": 0.0, "y1": 0.0, "x2": 0.0, "y2": 0.0}},
    "cae":           {{"x1": 0.0, "y1": 0.0, "x2": 0.0, "y2": 0.0}},
    "observaciones": null
  }},
  "detalle_columnas": [],
  "comentario": ""
}}

═══════════════════════════════
DEFINICIÓN DE ZONAS
═══════════════════════════════

PROVEEDOR (emisor = quien VENDIÓ y emitió la factura):
  - Es SIEMPRE el que aparece en el encabezado superior con su logo, nombre/razón social, domicilio, CUIT y condición de IVA.
  - En Argentina, la empresa que imprimió la factura es el proveedor. Si ves "BEBIDAS DEL LAGO S.A." grande arriba → eso es el proveedor.
  - NO confundir con el bloque "Cliente:" o "Sr.:" que aparece MÁS ABAJO: ese es el receptor/comprador.
  - Incluir toda la zona con nombre, domicilio, CUIT y condición IVA del EMISOR.

CABECERA (datos del comprobante):
  - Tipo de comprobante (FACTURA / REMITO), letra (A/B/C), punto de venta y número.
  - Fecha de emisión, código de barras o QR si está en esa área.
  - Suele estar en la parte superior derecha o en una caja destacada.
  - NO incluir los datos del proveedor ni del cliente.

CLIENTE (receptor = quien COMPRÓ):
  - Bloque con razón social, CUIT/documento, domicilio y condición IVA del comprador.
  - Suele ir precedido de "Cliente:", "Sr.:", "Señores:", "Comprador:", "A:", etc.
  - Siempre está DEBAJO del bloque del proveedor.
  - Si hay datos de vendedor, zona, reparto o condición de pago junto al cliente, incluilos.

DETALLE (tabla de ítems):
  - Tabla con renglones de artículos/productos.
  - Debe comenzar DESDE la fila de encabezados de columna (CODIGO, DESCRIPCION, etc.)
  - hasta el último renglón de datos, SIN incluir los totales.

TOTALES (pie de la factura):
  - Línea o bloque con subtotales, neto gravado, IVA 21%, IVA 10.5%, percepciones, IIBB, total final.
  - Suele estar en una franja horizontal justo después del detalle.

CAE / CAI:
  - Número de CAE o CAI y su vencimiento. Suele estar al pie, debajo de los totales.
  - Incluir el texto "CAE:", "CAI:", "VTO CAE:", código de barras del CAE si está agrupado.

OBSERVACIONES:
  - Solo si hay un bloque claro de notas, leyendas o instrucciones de pago SEPARADO de las zonas anteriores.
  - Si no existe ese bloque claramente diferenciado, devolver null.

═══════════════════════════════
REGLAS DE COORDENADAS
═══════════════════════════════
- Todas las coordenadas son relativas al tamaño total de la imagen (valores entre 0.0 y 1.0).
  x1/y1 = esquina superior izquierda, x2/y2 = esquina inferior derecha.
- Las zonas NO deben solaparse entre sí.
- Cada zona debe quedar contenida dentro de los límites de su bloque real visible.
- Si una zona no existe o no se puede identificar con confianza, devolvela como null.

═══════════════════════════════
COLUMNAS DEL DETALLE
═══════════════════════════════
Para cada columna visible en la tabla de detalle, mapeala al campo interno más adecuado:

  Codigo_Articulo  → código, cód., art., artículo, ítem
  Descripcion      → descripción, detalle, producto, artículo (texto)
  Cantidad         → cant., cantidad, unidades, qty
  Bl/Pq            → bultos, blt., bl., paq., cajas, fardos, pq.
  Importe_Lista    → precio, p.unit., precio unit., precio lista, valor unit.
  % Dto1           → desc.%, dto1, bonif.%, bon.%, descuento 1
  % Dto2           → dto2, descuento 2, bonif. 2
  Importe_Neto     → neto, imp.neto, precio neto, p.neto (precio unitario luego de descuentos)
  Total            → total, subtotal, importe, imp. total (total por renglón = precio x cantidad)
  IVA              → iva%, alíc. iva, tasa iva
  Impuestos internos → imp.int., i.int., imp.internos, ii.ii.
  Tot.Imp.Int      → tot.imp.int., total imp.int.
  AuxNroLote       → lote, nro.lote, lot.
  AuxNroSerie      → serie, nro.serie
  Moneda           → moneda, divisa

- Devolvé SOLO las columnas que puedas identificar con razonable certeza.
- Los x1/x2 de columnas son relativos al ancho TOTAL de la imagen (no al ancho del detalle).
- El orden en el array debe ser de izquierda a derecha.
- No inventes columnas que no estén visibles.

En "comentario" incluí observaciones relevantes (ej: zona observaciones no identificada, columnas ambiguas, etc.).
""".strip()

    def __init__(self, model: str = DEFAULT_AI_MODEL):
        self.model = model.strip() or DEFAULT_AI_MODEL

    def detect_from_image(self, image: Any, source_filename: str = "comprobante.jpg") -> Dict[str, Any]:
        load_env_near_app()
        if not backend_enabled():
            raise RuntimeError(
                "No hay transporte IA configurado. Definí IA_BACKEND_URL + credenciales "
                "o bien OPENAI_API_KEY en el .env."
            )

        content = [
            {"type": "input_text", "text": self.PROMPT},
            pil_image_to_content_block(image),
        ]
        out_text = call_backend_with_hard_timeout(
            content_blocks=content,
            model=self.model,
            max_output_tokens=4000,
            text={"format": {"type": "json_object"}},
            source_filename=source_filename,
            timeout_seconds=120,
        )
        data = extract_first_json(out_text)
        if not isinstance(data, dict):
            raise RuntimeError("La IA devolvió un formato inválido para detección de layout.")
        return data


class LayoutRepository:
    def __init__(self, sql_client: SqlServerClient):
        self.sql_client = sql_client

    def save_layout_to_sql(self, provider_code: str, layout_json: Dict[str, Any]) -> str:
        code = provider_code.strip()
        if not code:
            raise RuntimeError("Ingresá o seleccioná un código de proveedor antes de guardar.")
        key = f"IA_LYT_{code}"
        payload = json.dumps(layout_json, ensure_ascii=False, indent=2)
        self.sql_client.upsert_layout(key, payload)
        return key

    def load_layout_from_sql(self, provider_code: str) -> Tuple[str, Dict[str, Any]]:
        code = provider_code.strip()
        if not code:
            raise RuntimeError("Ingresá un código de proveedor para cargar el layout.")
        key = f"IA_LYT_{code}"
        payload = self.sql_client.get_layout_payload(key)
        if not payload:
            raise RuntimeError(f"No existe layout guardado para la clave {key}.")
        try:
            data = json.loads(payload)
        except Exception as e:
            raise RuntimeError(f"El JSON guardado en {key} no es válido.") from e
        if not isinstance(data, dict):
            raise RuntimeError(f"El contenido guardado en {key} no tiene formato JSON objeto.")
        return key, data


class LayoutBuilder:
    @staticmethod
    def build_detail_column(field_name: str, x1: float, x2: float) -> Dict[str, Any]:
        return {
            "campo": field_name,
            "x1": round(min(x1, x2), 6),
            "x2": round(max(x1, x2), 6),
        }


class LayoutManagerDialog:
    def __init__(self, parent_app: Any) -> None:
        self.app = parent_app
        self._results: List[ProviderMatch] = []

        self.win = tk.Toplevel(parent_app.root)
        self.win.title("Gestionar layouts guardados")
        self.win.transient(parent_app.root)
        self.win.grab_set()
        self.win.geometry("660x520")
        self.win.resizable(True, True)
        self.win.columnconfigure(0, weight=1)
        self.win.rowconfigure(1, weight=1)

        self._build_ui()
        self.win.after(100, self._load_all)

    def _build_ui(self) -> None:
        search_frame = ttk.Frame(self.win, padding=(10, 10, 10, 0))
        search_frame.grid(row=0, column=0, sticky="ew")
        search_frame.columnconfigure(1, weight=1)
        ttk.Label(search_frame, text="Buscar:").grid(row=0, column=0, sticky="w")
        self.var_search = tk.StringVar()
        entry = ttk.Entry(search_frame, textvariable=self.var_search)
        entry.grid(row=0, column=1, sticky="ew", padx=(6, 4))
        entry.bind("<Return>", lambda _e: self._search())
        ttk.Button(search_frame, text="Buscar", command=self._search).grid(row=0, column=2)

        list_frame = ttk.LabelFrame(self.win, text="Proveedores con layout configurado", padding=8)
        list_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=10)
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)

        self.tree = ttk.Treeview(
            list_frame,
            columns=("codigo", "nombre", "cuit"),
            show="headings",
            selectmode="browse",
        )
        for col, title, width in [
            ("codigo", "Código", 80),
            ("nombre", "Nombre / Razón Social", 340),
            ("cuit", "CUIT", 130),
        ]:
            self.tree.heading(col, text=title)
            self.tree.column(col, width=width, anchor="w")
        self.tree.grid(row=0, column=0, sticky="nsew")
        self.tree.bind("<Double-1>", lambda _e: self._on_modificar())
        sb = ttk.Scrollbar(list_frame, orient="vertical", command=self.tree.yview)
        sb.grid(row=0, column=1, sticky="ns")
        self.tree.configure(yscrollcommand=sb.set)

        self.status_label = ttk.Label(list_frame, text="", foreground="#4B5563")
        self.status_label.grid(row=1, column=0, columnspan=2, sticky="w", pady=(4, 0))

        btn_frame = ttk.Frame(self.win, padding=(10, 0, 10, 10))
        btn_frame.grid(row=2, column=0, sticky="ew")
        ttk.Button(btn_frame, text="Modificar (cargar)", command=self._on_modificar).pack(side="left")
        ttk.Button(btn_frame, text="Eliminar", command=self._on_eliminar).pack(side="left", padx=(6, 0))
        ttk.Button(btn_frame, text="Cerrar", command=self.win.destroy).pack(side="right")

    def _load_all(self) -> None:
        self._search()

    def _search(self) -> None:
        text = self.var_search.get().strip()
        self.status_label.configure(text="Buscando...")
        self.win.update_idletasks()
        try:
            lookup = ProviderLookup(self.app._sql_client())
            results = lookup.search_saved_layouts(text=text, limit=500)
            self._results = results
            self.tree.delete(*self.tree.get_children())
            for r in results:
                cuit_fmt = r.cuit if r.cuit else ""
                self.tree.insert("", "end", values=(r.code, r.name, cuit_fmt))
            self.status_label.configure(text=f"{len(results)} layout(s) encontrado(s).")
        except Exception as exc:
            self.status_label.configure(text=f"Error: {exc}")

    def _selected_match(self) -> Optional[ProviderMatch]:
        sel = self.tree.selection()
        if not sel:
            return None
        idx = self.tree.index(sel[0])
        if 0 <= idx < len(self._results):
            return self._results[idx]
        return None

    def _on_modificar(self) -> None:
        match = self._selected_match()
        if match is None:
            messagebox.showwarning(APP_TITLE, "Seleccioná un proveedor de la lista.", parent=self.win)
            return
        self.app.var_provider_code.set(match.code)
        self.app.var_provider_name.set(match.name)
        self.app.var_provider_cuit.set(match.cuit)
        self.win.destroy()
        self.app.on_load_layout()

    def _on_eliminar(self) -> None:
        match = self._selected_match()
        if match is None:
            messagebox.showwarning(APP_TITLE, "Seleccioná un proveedor de la lista.", parent=self.win)
            return
        clave = f"IA_LYT_{match.code}"
        nombre_display = match.name or match.code
        if not messagebox.askyesno(
            APP_TITLE,
            f"¿Eliminar el layout de\n{nombre_display}\n({clave})?\n\nEsta acción no se puede deshacer.",
            parent=self.win,
        ):
            return
        try:
            self.app._sql_client().delete_layout(clave)
            self._search()
            self.app.set_status(f"Layout {clave} eliminado.")
        except Exception as exc:
            messagebox.showerror(APP_TITLE, str(exc), parent=self.win)


class LayoutEditorApp:
    def __init__(self, root: Any, config: AppConfig):
        self.root = root
        self.config = config
        self.document = DocumentState()
        self.loader = DocumentImageLoader()
        self.zones: Dict[str, Dict[str, float]] = {}
        self.detail_columns: List[Dict[str, Any]] = []
        self.canvas_items: Dict[str, int] = {}
        self.column_item_ids: List[int] = []
        self._selection_rect_id: Optional[int] = None
        self._selection_start: Optional[Tuple[int, int]] = None
        self._mode: str = ""
        self.selected_zone_type: str = ""
        self.selected_column_field: str = ""
        self._busy_window: Optional[Any] = None
        self._busy_label_var: Optional[Any] = None
        self._zoom_factor: float = 1.0
        self._busy_cancel_event: Optional[Any] = None
        self._total_pages: int = 1
        self._doc_files: List[str] = []   # todos los archivos del comprobante

        self.var_server = tk.StringVar(value=config.server)
        self.var_database = tk.StringVar(value=config.database)
        self.var_user = tk.StringVar(value=config.user)
        self.var_password = tk.StringVar(value=config.password)
        self.var_driver = tk.StringVar(value=config.driver)
        self.var_tesseract_cmd = tk.StringVar(value=config.tesseract_cmd)
        self.var_ai_model = tk.StringVar(value=DEFAULT_DETECTION_MODEL)
        self.var_prompt_file = tk.StringVar(value="")
        self.var_file = tk.StringVar(value=config.last_file)
        self.var_page = tk.IntVar(value=1)
        self.var_zone_type = tk.StringVar(value=ZONE_TYPES[0])
        self.var_column_field = tk.StringVar(value=DETAIL_FIELDS[0])
        self.var_provider_code = tk.StringVar(value="")
        self.var_provider_name = tk.StringVar(value="")
        self.var_provider_cuit = tk.StringVar(value="")
        self.var_status = tk.StringVar(value="Listo.")
        self.var_search_text = tk.StringVar(value="")

        self._build_ui()
        self._install_variable_traces()
        self._refresh_layout_summary()
        self._bind_canvas()
        self.root.after(100, self._fit_image_to_canvas)

    def _build_ui(self) -> None:
        self.root.title(APP_TITLE)
        self.root.geometry("1440x920")
        self.root.minsize(1180, 760)
        self.root.columnconfigure(1, weight=1)
        self.root.rowconfigure(0, weight=1)

        left = ttk.Frame(self.root, padding=10, width=390)
        left.grid(row=0, column=0, sticky="nsew")
        left.grid_propagate(False)
        right = ttk.Frame(self.root, padding=(0, 10, 10, 10))
        right.grid(row=0, column=1, sticky="nsew")
        right.columnconfigure(0, weight=1)
        right.rowconfigure(0, weight=1)

        self._build_left_panel(left)
        self._build_canvas_panel(right)
        self._build_status_bar()

    def _build_left_panel(self, parent: Any) -> None:
        parent.columnconfigure(0, weight=1)
        header = ttk.LabelFrame(parent, text="Asistente", padding=8)
        header.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        header.columnconfigure(0, weight=1)
        ttk.Label(
            header,
            text="1. Documento  2. Zonas  3. Detalle  4. Proveedor  5. Guardar  6. Prueba",
            anchor="w",
        ).grid(row=0, column=0, sticky="ew")
        nav = ttk.Frame(header)
        nav.grid(row=0, column=1, sticky="e")
        ttk.Button(nav, text="Nuevo layout", command=self.on_new_layout).pack(side="left")
        ttk.Button(nav, text="Gestionar layouts...", command=self.on_manage_layouts).pack(side="left", padx=(6, 0))
        ttk.Button(nav, text="Configuración...", command=self.on_open_config_dialog).pack(side="left", padx=(6, 0))

        notebook = ttk.Notebook(parent)
        notebook.grid(row=1, column=0, sticky="nsew")
        parent.rowconfigure(1, weight=1)
        self.left_notebook = notebook

        bottom_nav = ttk.Frame(parent)
        bottom_nav.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        ttk.Button(bottom_nav, text="← Anterior", command=self.on_prev_tab).pack(side="left")
        ttk.Button(bottom_nav, text="Siguiente →", command=self.on_next_tab).pack(side="right")

        tab_documento = ttk.Frame(notebook, padding=10)
        tab_zonas = ttk.Frame(notebook, padding=10)
        tab_detalle = ttk.Frame(notebook, padding=10)
        tab_proveedor = ttk.Frame(notebook, padding=10)
        tab_guardar = ttk.Frame(notebook, padding=10)
        tab_prueba = ttk.Frame(notebook, padding=10)

        notebook.add(tab_documento, text="1. Documento")
        notebook.add(tab_zonas, text="2. Zonas")
        notebook.add(tab_detalle, text="3. Detalle")
        notebook.add(tab_proveedor, text="4. Proveedor")
        notebook.add(tab_guardar, text="5. Guardar")
        notebook.add(tab_prueba, text="6. Prueba")

        self._build_tab_documento(tab_documento)
        self._build_tab_zonas(tab_zonas)
        self._build_tab_detalle(tab_detalle)
        self._build_tab_proveedor(tab_proveedor)
        self._build_tab_guardar(tab_guardar)
        self._build_tab_prueba(tab_prueba)

    def _build_tab_documento(self, parent: Any) -> None:
        parent.columnconfigure(1, weight=1)
        ttk.Label(parent, text="Archivo:").grid(row=0, column=0, sticky="w")
        ttk.Entry(parent, textvariable=self.var_file).grid(row=0, column=1, sticky="ew", padx=(6, 4))
        ttk.Button(parent, text="Abrir archivo", command=self.on_open_file).grid(row=0, column=2)

        ttk.Label(parent, text="Página PDF:").grid(row=1, column=0, sticky="w", pady=(8, 0))
        nav_frame = ttk.Frame(parent)
        nav_frame.grid(row=1, column=1, columnspan=2, sticky="w", padx=(6, 0), pady=(8, 0))
        ttk.Button(nav_frame, text="◀", width=3, command=self.on_page_prev).pack(side="left")
        _spb = ttk.Spinbox(nav_frame, textvariable=self.var_page, from_=1, to=9999, width=5,
                           command=lambda: self.after(0, self.on_reload_page))
        _spb.pack(side="left", padx=(4, 2))
        _spb.bind("<Return>",   lambda _e: self.after(0, self.on_reload_page))
        _spb.bind("<KP_Enter>", lambda _e: self.after(0, self.on_reload_page))
        self.lbl_total_pages = ttk.Label(nav_frame, text="/ 1")
        self.lbl_total_pages.pack(side="left")
        ttk.Button(nav_frame, text="▶", width=3, command=self.on_page_next).pack(side="left", padx=(4, 0))

        ttk.Label(parent, text="Prompt file:").grid(row=2, column=0, sticky="w", pady=(8, 0))
        ttk.Entry(parent, textvariable=self.var_prompt_file).grid(row=2, column=1, sticky="ew", padx=(6, 4), pady=(8, 0))
        ttk.Button(parent, text="...", width=3, command=self.on_pick_prompt_file).grid(row=2, column=2, pady=(8, 0))

        ttk.Label(parent, text="Modelo IA:").grid(row=3, column=0, sticky="w", pady=(8, 0))
        ttk.Combobox(
            parent, textvariable=self.var_ai_model, values=AI_MODELS, state="readonly", width=20
        ).grid(row=3, column=1, sticky="w", padx=(6, 4), pady=(8, 0))
        ttk.Button(parent, text="Detectar zonas IA", command=self.on_detect_layout_with_ai).grid(row=3, column=2, pady=(8, 0))

        note = ttk.Label(
            parent,
            text="Abrí un comprobante y, si querés, empezá con una propuesta automática de zonas.",
            foreground="#4B5563",
            wraplength=520,
            justify="left",
        )
        note.grid(row=4, column=0, columnspan=3, sticky="ew", pady=(14, 0))

    def _build_tab_zonas(self, parent: Any) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)
        left = ttk.LabelFrame(parent, text="Elegir zona", padding=8)
        left.grid(row=0, column=0, sticky="nsew")
        left.columnconfigure(0, weight=1)
        left.rowconfigure(0, weight=1)
        self.zone_type_listbox = tk.Listbox(left, exportselection=False, height=7)
        self.zone_type_listbox.grid(row=0, column=0, sticky="nsew")
        for item in ZONE_TYPES:
            self.zone_type_listbox.insert("end", item)
        self.zone_type_listbox.selection_set(0)
        self.zone_type_listbox.bind("<<ListboxSelect>>", self.on_zone_type_selected)
        zone_btns = ttk.Frame(left)
        zone_btns.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        ttk.Button(zone_btns, text="Modificar zona", command=self.on_edit_selected_zone).pack(side="left")
        ttk.Button(zone_btns, text="Eliminar zona", command=self.on_delete_selected_zone).pack(side="left", padx=(6, 0))
        ttk.Button(zone_btns, text="Marcar zona", command=self.on_start_mark_zone).pack(side="left", padx=(6, 0))
        self.zone_help_label = ttk.Label(
            left,
            text=ZONE_HELP.get(ZONE_TYPES[0], ""),
            foreground="#4B5563",
            wraplength=340,
            justify="left",
        )
        self.zone_help_label.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        self.zone_draw_banner = ttk.Label(
            left,
            text="",
            foreground="#FFFFFF",
            background="#2A6F97",
            wraplength=340,
            justify="left",
            padding=(6, 4),
        )
        self.zone_draw_banner.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        self.zone_draw_banner.grid_remove()

        list_frame = ttk.LabelFrame(parent, text="Zonas marcadas", padding=8)
        list_frame.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)
        self.zone_listbox = tk.Listbox(list_frame, height=3)
        self.zone_listbox.grid(row=0, column=0, sticky="nsew")
        self.zone_listbox.bind("<<ListboxSelect>>", self.on_marked_zone_selected)
        sb = ttk.Scrollbar(list_frame, orient="vertical", command=self.zone_listbox.yview)
        sb.grid(row=0, column=1, sticky="ns")
        self.zone_listbox.configure(yscrollcommand=sb.set)
    def _build_tab_detalle(self, parent: Any) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)
        left = ttk.LabelFrame(parent, text="Elegir campo", padding=8)
        left.grid(row=0, column=0, sticky="nsew")
        left.columnconfigure(0, weight=1)
        left.rowconfigure(0, weight=1)
        self.column_field_listbox = tk.Listbox(left, exportselection=False, height=10)
        self.column_field_listbox.grid(row=0, column=0, sticky="nsew")
        for item in DETAIL_FIELDS:
            self.column_field_listbox.insert("end", item)
        self.column_field_listbox.selection_set(0)
        self.column_field_listbox.bind("<<ListboxSelect>>", self.on_column_field_selected)
        detail_btns = ttk.Frame(left)
        detail_btns.grid(row=1, column=0, sticky="ew", pady=(8, 0))
        ttk.Button(detail_btns, text="Modificar columna", command=self.on_edit_selected_column).pack(side="left")
        ttk.Button(detail_btns, text="Eliminar columna", command=self.on_delete_selected_column).pack(side="left", padx=(6, 0))
        ttk.Button(detail_btns, text="Marcar columna", command=self.on_start_mark_column).pack(side="left", padx=(6, 0))
        self.column_help_label = ttk.Label(
            left,
            text=DETAIL_FIELD_HELP.get(DETAIL_FIELDS[0], ""),
            foreground="#4B5563",
            wraplength=340,
            justify="left",
        )
        self.column_help_label.grid(row=2, column=0, sticky="ew", pady=(8, 0))
        self.column_draw_banner = ttk.Label(
            left,
            text="",
            foreground="#FFFFFF",
            background="#FF9F1C",
            wraplength=340,
            justify="left",
            padding=(6, 4),
        )
        self.column_draw_banner.grid(row=3, column=0, sticky="ew", pady=(8, 0))
        self.column_draw_banner.grid_remove()

        list_frame = ttk.LabelFrame(parent, text="Columnas detectadas / marcadas", padding=8)
        list_frame.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        list_frame.columnconfigure(0, weight=1)
        list_frame.rowconfigure(0, weight=1)
        self.columns_listbox = tk.Listbox(list_frame, height=3)
        self.columns_listbox.grid(row=0, column=0, sticky="nsew")
        self.columns_listbox.bind("<<ListboxSelect>>", self.on_marked_column_selected)
        sb = ttk.Scrollbar(list_frame, orient="vertical", command=self.columns_listbox.yview)
        sb.grid(row=0, column=1, sticky="ns")
        self.columns_listbox.configure(yscrollcommand=sb.set)
        ttk.Button(list_frame, text="Limpiar columnas", command=self.on_clear_columns).grid(
            row=1, column=0, columnspan=2, sticky="ew", pady=(8, 0)
        )

    def _build_tab_proveedor(self, parent: Any) -> None:
        parent.columnconfigure(1, weight=1)
        parent.rowconfigure(5, weight=1)
        ttk.Label(parent, text="Código:").grid(row=0, column=0, sticky="w")
        ttk.Entry(parent, textvariable=self.var_provider_code).grid(row=0, column=1, sticky="ew", padx=(6, 0))
        ttk.Label(parent, text="Nombre:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(parent, textvariable=self.var_provider_name).grid(row=1, column=1, sticky="ew", padx=(6, 0), pady=(6, 0))
        ttk.Label(parent, text="CUIT:").grid(row=2, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(parent, textvariable=self.var_provider_cuit).grid(row=2, column=1, sticky="ew", padx=(6, 0), pady=(6, 0))
        ttk.Label(parent, text="Buscar:").grid(row=3, column=0, sticky="w", pady=(6, 0))
        ttk.Entry(parent, textvariable=self.var_search_text).grid(row=3, column=1, sticky="ew", padx=(6, 0), pady=(6, 0))

        btn_row = ttk.Frame(parent)
        btn_row.grid(row=4, column=0, columnspan=2, sticky="ew", pady=(10, 0))
        ttk.Button(btn_row, text="Buscar proveedor", command=self.on_search_provider).pack(side="left")
        ttk.Button(btn_row, text="Probar OCR zona proveedor", command=self.on_ocr_provider_zone).pack(side="left", padx=(6, 0))

        result_frame = ttk.LabelFrame(parent, text="Resultados proveedor", padding=8)
        result_frame.grid(row=5, column=0, columnspan=2, sticky="nsew", pady=(12, 0))
        parent.rowconfigure(5, weight=1)
        result_frame.columnconfigure(0, weight=1)
        result_frame.rowconfigure(0, weight=1)
        self.provider_tree = ttk.Treeview(
            result_frame,
            columns=("codigo", "nombre", "cuit", "domicilio"),
            show="headings",
            height=10,
        )
        for col, title, width in [
            ("codigo", "Código", 80),
            ("nombre", "Nombre", 220),
            ("cuit", "CUIT", 110),
            ("domicilio", "Domicilio", 220),
        ]:
            self.provider_tree.heading(col, text=title)
            self.provider_tree.column(col, width=width, anchor="w")
        self.provider_tree.grid(row=0, column=0, sticky="nsew")
        self.provider_tree.bind("<<TreeviewSelect>>", self.on_provider_selected)
        sb = ttk.Scrollbar(result_frame, orient="vertical", command=self.provider_tree.yview)
        sb.grid(row=0, column=1, sticky="ns")
        self.provider_tree.configure(yscrollcommand=sb.set)

    def _build_tab_guardar(self, parent: Any) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(1, weight=1)
        ttk.Label(
            parent,
            text="Revisá el resumen antes de grabar el layout en TA_CONFIGURACION.",
            foreground="#4B5563",
            wraplength=520,
            justify="left",
        ).grid(row=0, column=0, sticky="ew")
        self.summary_text = tk.Text(parent, height=18, wrap="word")
        self.summary_text.grid(row=1, column=0, sticky="nsew", pady=(10, 0))
        self.summary_text.configure(state="disabled")
        ttk.Button(parent, text="Guardar layout", command=self.on_save_layout).grid(row=2, column=0, sticky="e", pady=(10, 0))

    def _build_tab_prueba(self, parent: Any) -> None:
        parent.columnconfigure(0, weight=1)
        parent.rowconfigure(2, weight=1)

        info = ttk.Label(
            parent,
            text="Procesa el comprobante abierto usando el layout actual (sin guardar en SQL) y muestra el JSON resultante.",
            foreground="#4B5563",
            wraplength=520,
            justify="left",
        )
        info.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 8))

        btn_frame = ttk.Frame(parent)
        btn_frame.grid(row=1, column=0, columnspan=2, sticky="ew")
        ttk.Button(btn_frame, text="Procesar con layout", command=self.on_run_test_layout).pack(side="left")
        ttk.Button(btn_frame, text="Procesar con IA", command=self.on_run_test_ia).pack(side="left", padx=(8, 0))
        self.var_prueba_status = tk.StringVar(value="")
        ttk.Label(btn_frame, textvariable=self.var_prueba_status, foreground="#6B7280").pack(side="left", padx=(12, 0))

        self.prueba_text = tk.Text(parent, wrap="none", font=("Consolas", 9))
        self.prueba_text.grid(row=2, column=0, sticky="nsew", pady=(8, 0))
        sb_y = ttk.Scrollbar(parent, orient="vertical", command=self.prueba_text.yview)
        sb_y.grid(row=2, column=1, sticky="ns", pady=(8, 0))
        sb_x = ttk.Scrollbar(parent, orient="horizontal", command=self.prueba_text.xview)
        sb_x.grid(row=3, column=0, sticky="ew")
        self.prueba_text.configure(yscrollcommand=sb_y.set, xscrollcommand=sb_x.set)

    def _run_v5(self, extra_args: List[str]) -> None:
        """Ejecuta el v5 en un hilo y muestra el JSON resultante."""
        import subprocess
        import tempfile

        doc_path = self.document.path if self.document else ""
        if not doc_path or not Path(doc_path).is_file():
            messagebox.showwarning("Sin documento", "Abrí un comprobante primero.")
            return

        is_ia = "--layout-only" not in extra_args
        self.var_prueba_status.set("Procesando...")
        self.prueba_text.configure(state="normal")
        self.prueba_text.delete("1.0", "end")
        if is_ia:
            n_files = len(self._doc_files) if len(self._doc_files) > 1 else 1
            self.prueba_text.insert(
                "end",
                f"Procesando con IA ({n_files} imagen{'es' if n_files > 1 else ''})...\n"
                f"Esto puede tardar varios minutos. Por favor esperá.\n\n"
                f"(Llamada 1 de {n_files} en progreso...)"
            )
        else:
            self.prueba_text.insert("end", "Procesando con layout...")
        self.prueba_text.configure(state="disabled")

        _stop_anim = threading.Event()

        def _animate():
            dots = 0
            while not _stop_anim.wait(2.0):
                dots = (dots % 3) + 1
                if is_ia:
                    msg = (
                        f"Procesando con IA ({n_files} imagen{'es' if n_files > 1 else ''})...\n"
                        f"Esto puede tardar varios minutos. Por favor esperá.\n\n"
                        f"{'.' * dots}"
                    )
                else:
                    msg = f"Procesando con layout{'.' * dots}"
                self.root.after(0, lambda m=msg: _update_anim(m))

        def _update_anim(msg: str) -> None:
            self.prueba_text.configure(state="normal")
            self.prueba_text.delete("1.0", "end")
            self.prueba_text.insert("end", msg)
            self.prueba_text.configure(state="disabled")

        threading.Thread(target=_animate, daemon=True).start()

        def _worker():
            try:
                base = app_dir()
                exe = base / "lector_facturas_to_json_v5.exe"
                script = base / "lector_facturas_to_json_v5.py"
                if exe.is_file():
                    cmd = [str(exe)]
                elif script.is_file():
                    cmd = [sys.executable, str(script)]
                else:
                    self.root.after(0, lambda: self._prueba_set_result("No se encontró lector_facturas_to_json_v5.exe ni .py junto al configurador."))
                    return

                outdir = tempfile.mkdtemp(prefix="lyt_prueba_")

                # Determinar lista de archivos a procesar
                input_files = [doc_path]
                temp_pages_dir = None
                if len(self._doc_files) > 1:
                    # Múltiples archivos cargados → mandarlos todos en orden
                    input_files = list(self._doc_files)
                elif Path(doc_path).suffix.lower() == ".pdf" and fitz is not None:
                    try:
                        doc_fitz = fitz.open(doc_path)
                        n_pages = doc_fitz.page_count
                        if n_pages > 1:
                            temp_pages_dir = tempfile.mkdtemp(prefix="lyt_pages_")
                            stem = Path(doc_path).stem
                            input_files = []
                            for pg_idx in range(n_pages):
                                single = fitz.open()
                                single.insert_pdf(doc_fitz, from_page=pg_idx, to_page=pg_idx)
                                pg_path = Path(temp_pages_dir) / f"{stem}__page_{pg_idx + 1:03d}.pdf"
                                single.save(str(pg_path))
                                single.close()
                                input_files.append(str(pg_path))
                        doc_fitz.close()
                    except Exception:
                        input_files = [doc_path]

                # Layout: hasta 3 min. IA con imágenes: hasta 20 min (N JPEGs × API call).
                _timeout = 180 if "--layout-only" in extra_args else 1200
                result = subprocess.run(
                    cmd + input_files + extra_args + ["--outdir", outdir],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=_timeout,
                )
                json_path = result.stdout.strip()
                if json_path and Path(json_path).is_file():
                    content = Path(json_path).read_text(encoding="utf-8-sig")
                    try:
                        parsed = json.loads(content)
                        content = json.dumps(parsed, ensure_ascii=False, indent=2)
                    except Exception:
                        pass
                    self.root.after(0, lambda c=content: self._prueba_set_result(c))
                else:
                    err = result.stderr.strip() or result.stdout.strip() or "Sin salida."
                    self.root.after(0, lambda e=err: self._prueba_set_result(f"ERROR:\n{e}"))
            except subprocess.TimeoutExpired:
                mins = _timeout // 60
                self.root.after(0, lambda m=mins: self._prueba_set_result(
                    f"Tiempo de espera agotado ({m} min).\n"
                    f"Con {len(input_files)} imagen(es) y procesamiento por IA puede demorar más.\n"
                    f"Intentá con menos archivos o usá 'Procesar con layout' que es instantáneo."
                ))
            except Exception as ex:
                self.root.after(0, lambda e=str(ex): self._prueba_set_result(f"Error inesperado:\n{e}"))
            finally:
                _stop_anim.set()
                if temp_pages_dir:
                    import shutil
                    try:
                        shutil.rmtree(temp_pages_dir, ignore_errors=True)
                    except Exception:
                        pass

        threading.Thread(target=_worker, daemon=True).start()

    def _prueba_set_result(self, text: str) -> None:
        self.prueba_text.configure(state="normal")
        self.prueba_text.delete("1.0", "end")
        self.prueba_text.insert("end", text)
        self.prueba_text.configure(state="disabled")
        self.var_prueba_status.set("")

    def on_run_test_layout(self) -> None:
        import tempfile
        try:
            layout = self.build_layout()
        except RuntimeError as e:
            messagebox.showwarning("Layout incompleto", str(e))
            return
        tmp = Path(tempfile.mktemp(suffix=".json", prefix="lyt_test_"))
        tmp.write_text(json.dumps(layout, ensure_ascii=False, indent=2), encoding="utf-8")
        self._run_v5(["--layout-file", str(tmp), "--layout-only"])

    def on_run_test_ia(self) -> None:
        self._run_v5(["--all-pages"])

    def _build_canvas_panel(self, parent: Any) -> None:
        canvas_frame = ttk.LabelFrame(parent, text="Vista del comprobante", padding=8)
        canvas_frame.grid(row=0, column=0, sticky="nsew")
        canvas_frame.columnconfigure(0, weight=1)
        canvas_frame.rowconfigure(1, weight=1)

        toolbar = ttk.Frame(canvas_frame)
        toolbar.grid(row=0, column=0, columnspan=2, sticky="ew", pady=(0, 6))
        ttk.Button(toolbar, text="-", width=3, command=self.on_zoom_out).pack(side="left")
        ttk.Button(toolbar, text="+", width=3, command=self.on_zoom_in).pack(side="left", padx=(6, 0))
        ttk.Button(toolbar, text="Ajustar", command=self.on_zoom_fit).pack(side="left", padx=(6, 0))

        self.canvas = tk.Canvas(canvas_frame, background="#F4F6F8", highlightthickness=1, highlightbackground="#D0D7DE")
        self.canvas.grid(row=1, column=0, sticky="nsew")
        xsb = ttk.Scrollbar(canvas_frame, orient="horizontal", command=self.canvas.xview)
        ysb = ttk.Scrollbar(canvas_frame, orient="vertical", command=self.canvas.yview)
        xsb.grid(row=2, column=0, sticky="ew")
        ysb.grid(row=1, column=1, sticky="ns")
        self.canvas.configure(xscrollcommand=xsb.set, yscrollcommand=ysb.set)
        self.canvas.bind("<Configure>", lambda _e: self._fit_image_to_canvas())

    def _build_status_bar(self) -> None:
        status = ttk.Label(self.root, textvariable=self.var_status, anchor="w", relief="sunken")
        status.grid(row=1, column=0, columnspan=2, sticky="ew")

    def _bind_canvas(self) -> None:
        self.canvas.bind("<ButtonPress-1>", self.on_canvas_press)
        self.canvas.bind("<B1-Motion>", self.on_canvas_drag)
        self.canvas.bind("<ButtonRelease-1>", self.on_canvas_release)
        self.root.bind("<Escape>", self._cancel_draw_mode)

    def _current_config(self) -> AppConfig:
        return AppConfig(
            server=self.var_server.get().strip(),
            database=self.var_database.get().strip(),
            user=self.var_user.get().strip(),
            password=self.var_password.get(),
            driver=self.var_driver.get().strip() or DEFAULT_DRIVER,
            tesseract_cmd=self.var_tesseract_cmd.get().strip(),
            last_file=self.var_file.get().strip(),
        )

    def _sql_client(self) -> SqlServerClient:
        return SqlServerClient(self._current_config())

    def _ocr_service(self) -> OCRService:
        return OCRService(self.var_tesseract_cmd.get().strip())

    def _install_variable_traces(self) -> None:
        for var in [
            self.var_provider_code,
            self.var_provider_name,
            self.var_provider_cuit,
            self.var_prompt_file,
            self.var_file,
            self.var_search_text,
        ]:
            var.trace_add("write", lambda *_args: self._refresh_layout_summary())

    def _select_left_tab(self, index: int) -> None:
        try:
            self.left_notebook.select(index)
        except Exception:
            pass

    def _current_tab_index(self) -> int:
        try:
            return self.left_notebook.index(self.left_notebook.select())
        except Exception:
            return 0

    def on_next_tab(self) -> None:
        idx = self._current_tab_index()
        if idx < 4:
            self._select_left_tab(idx + 1)

    def on_prev_tab(self) -> None:
        idx = self._current_tab_index()
        if idx > 0:
            self._select_left_tab(idx - 1)

    def on_zone_type_selected(self, _event: Any = None) -> None:
        selection = self.zone_type_listbox.curselection()
        if selection:
            zone_type = str(self.zone_type_listbox.get(selection[0]))
            # Guard: si es la misma zona (selección programática desde _refresh_zones_list),
            # no redibujar para evitar el loop infinito de eventos <<ListboxSelect>>.
            if zone_type == self.selected_zone_type:
                return
            self.var_zone_type.set(zone_type)
            self.selected_zone_type = zone_type
            if hasattr(self, "zone_help_label"):
                self.zone_help_label.configure(text=ZONE_HELP.get(zone_type, ""))
            self._redraw_canvas()

    def on_column_field_selected(self, _event: Any = None) -> None:
        selection = self.column_field_listbox.curselection()
        if selection:
            field_name = str(self.column_field_listbox.get(selection[0]))
            # Guard: misma selección programática → evitar loop infinito de <<ListboxSelect>>
            if field_name == self.selected_column_field:
                return
            self.var_column_field.set(field_name)
            self.selected_column_field = field_name
            if hasattr(self, "column_help_label"):
                self.column_help_label.configure(text=DETAIL_FIELD_HELP.get(field_name, ""))
            self._redraw_canvas()

    def on_zoom_in(self) -> None:
        if self.document.image_original is None:
            return
        self._zoom_factor = min(4.0, self._zoom_factor * 1.2)
        self._fit_image_to_canvas()

    def on_zoom_out(self) -> None:
        if self.document.image_original is None:
            return
        self._zoom_factor = max(0.25, self._zoom_factor / 1.2)
        self._fit_image_to_canvas()

    def on_zoom_fit(self) -> None:
        self._zoom_factor = 1.0
        self._fit_image_to_canvas()

    def set_status(self, text: str) -> None:
        self.var_status.set(text)

    def show_error(self, text: str) -> None:
        self.set_status(text)
        messagebox.showerror(APP_TITLE, text)

    def show_info(self, text: str) -> None:
        self.set_status(text)
        messagebox.showinfo(APP_TITLE, text)

    def _show_busy(self, title: str, message: str, *, cancellable: bool = False, on_cancel=None) -> None:
        self._close_busy()
        win = tk.Toplevel(self.root)
        win.title(title)
        win.transient(self.root)
        win.grab_set()
        win.resizable(False, False)
        win.geometry("420x170" if cancellable else "420x130")
        cancel_event = threading.Event() if cancellable else None

        def _cancel() -> None:
            if cancel_event is not None:
                cancel_event.set()
            if callable(on_cancel):
                try:
                    on_cancel()
                except Exception:
                    pass
            self._close_busy()

        win.protocol("WM_DELETE_WINDOW", _cancel if cancellable else (lambda: None))
        win.columnconfigure(0, weight=1)

        msg_var = tk.StringVar(value=message)
        ttk.Label(win, textvariable=msg_var, anchor="w", justify="left", wraplength=380).grid(
            row=0, column=0, sticky="ew", padx=16, pady=(18, 10)
        )
        pb = ttk.Progressbar(win, mode="indeterminate")
        pb.grid(row=1, column=0, sticky="ew", padx=16, pady=(0, 12))
        pb.start(10)
        ttk.Label(
            win,
            text="Esto puede tardar unos segundos. La ventana sigue activa mientras procesa.",
            foreground="#4B5563",
            wraplength=380,
            justify="left",
        ).grid(row=2, column=0, sticky="ew", padx=16, pady=(0, 14))

        if cancellable:
            btns = ttk.Frame(win)
            btns.grid(row=3, column=0, sticky="e", padx=16, pady=(0, 14))
            ttk.Button(btns, text="Cancelar", command=_cancel).pack(side="right")

        self.root.configure(cursor="watch")
        self._busy_window = win
        self._busy_label_var = msg_var
        self._busy_cancel_event = cancel_event
        try:
            win.update_idletasks()
        except Exception:
            pass

    def _update_busy_message(self, message: str) -> None:
        if self._busy_label_var is not None:
            try:
                self._busy_label_var.set(message)
            except Exception:
                pass

    def _close_busy(self) -> None:
        self.root.configure(cursor="")
        if self._busy_window is not None:
            try:
                self._busy_window.grab_release()
            except Exception:
                pass
            try:
                self._busy_window.destroy()
            except Exception:
                pass
        self._busy_window = None
        self._busy_label_var = None
        self._busy_cancel_event = None

    def on_open_config_dialog(self) -> None:
        win = tk.Toplevel(self.root)
        win.title("Configuración")
        win.transient(self.root)
        win.grab_set()
        win.geometry("780x320")
        win.minsize(680, 280)
        win.columnconfigure(1, weight=1)

        fields = [
            ("Servidor",      self.var_server,       False),
            ("Base",          self.var_database,     False),
            ("Usuario",       self.var_user,         False),
            ("Password",      self.var_password,     True),
            ("Driver",        self.var_driver,       False),
        ]
        for i, (label, var, is_secret) in enumerate(fields):
            ttk.Label(win, text=f"{label}:").grid(row=i, column=0, sticky="w", padx=10, pady=(10 if i == 0 else 6, 0))
            ttk.Entry(win, textvariable=var, show="*" if is_secret else "").grid(
                row=i, column=1, sticky="ew", padx=(6, 10), pady=(10 if i == 0 else 6, 0)
            )

        # Tesseract EXE — fila con Entry + botón Buscar + auto-detect
        tess_row = len(fields)
        ttk.Label(win, text="Tesseract EXE:").grid(row=tess_row, column=0, sticky="w", padx=10, pady=(6, 0))
        tess_frame = ttk.Frame(win)
        tess_frame.grid(row=tess_row, column=1, sticky="ew", padx=(6, 10), pady=(6, 0))
        tess_frame.columnconfigure(0, weight=1)
        ttk.Entry(tess_frame, textvariable=self.var_tesseract_cmd).grid(row=0, column=0, sticky="ew")
        ttk.Button(
            tess_frame, text="Buscar...",
            command=lambda: self.var_tesseract_cmd.set(
                filedialog.askopenfilename(
                    title="Seleccionar tesseract.exe",
                    filetypes=[("Ejecutable", "tesseract.exe"), ("Todos", "*.*")],
                    initialdir=r"C:\Program Files\Tesseract-OCR",
                ) or self.var_tesseract_cmd.get()
            ),
        ).grid(row=0, column=1, padx=(4, 0))

        # Auto-detectar Tesseract si el campo está vacío
        if not self.var_tesseract_cmd.get().strip():
            detected = _find_tesseract_exe()
            if detected:
                self.var_tesseract_cmd.set(detected)

        # Advertencia si el ODBC Driver configurado no está instalado
        if pyodbc is not None:
            try:
                driver_name = self.var_driver.get().strip() or DEFAULT_DRIVER
                if driver_name not in pyodbc.drivers():
                    warn_row = tess_row + 1
                    warn_frame = ttk.Frame(win)
                    warn_frame.grid(row=warn_row, column=0, columnspan=2, sticky="ew", padx=10, pady=(8, 0))
                    ttk.Label(
                        warn_frame,
                        text=f"⚠  Driver '{driver_name}' no encontrado.",
                        foreground="#c0392b",
                        font=("Segoe UI", 9, "bold"),
                    ).pack(side="left")
                    ttk.Button(
                        warn_frame, text="Descargar ODBC Driver 18",
                        command=lambda: webbrowser.open("https://aka.ms/odbc18"),
                    ).pack(side="left", padx=(8, 0))
            except Exception:
                pass

        btns = ttk.Frame(win, padding=10)
        btns.grid(row=tess_row + 2, column=0, columnspan=2, sticky="ew", pady=(12, 0))
        ttk.Button(btns, text="Probar conexión", command=self.on_test_connection).pack(side="left")
        ttk.Button(btns, text="Guardar configuración", command=self.on_save_connection).pack(side="left", padx=(6, 0))
        ttk.Button(btns, text="Cerrar", command=win.destroy).pack(side="right")

    def on_save_connection(self) -> None:
        cfg = self._current_config()
        save_last_connection(cfg)
        self.show_info("Conexión guardada en layout_config_last_connection.json")

    def on_test_connection(self) -> None:
        ok, msg = self._sql_client().test_connection()
        if ok:
            self.show_info(msg)
        else:
            self.show_error(msg)

    def on_pick_prompt_file(self) -> None:
        path = filedialog.askopenfilename(
            title="Seleccionar prompt",
            filetypes=[("Texto", "*.txt"), ("Todos", "*.*")],
        )
        if path:
            self.var_prompt_file.set(path)

    def on_open_file(self) -> None:
        paths = filedialog.askopenfilenames(
            title="Seleccionar comprobante (podés elegir varios archivos a la vez)",
            filetypes=[
                ("Documentos", "*.pdf;*.png;*.jpg;*.jpeg;*.webp;*.bmp;*.tif;*.tiff"),
                ("Todos", "*.*"),
            ],
        )
        if not paths:
            return
        self._doc_files = sorted(paths)   # orden alfabético = orden de páginas
        self._total_pages = len(self._doc_files)
        self._update_page_nav()
        self.var_page.set(1)
        self.var_file.set(self._doc_files[0])
        self.load_document(self._doc_files[0], 1)
        self._select_left_tab(0)

    def on_reload_page(self) -> None:
        if not self._doc_files and not self.var_file.get().strip():
            self.show_error("Seleccioná primero un archivo.")
            return
        page = self.var_page.get()
        if len(self._doc_files) > 1:
            # Modo multi-archivo: cada "página" es un archivo distinto
            idx = max(0, min(len(self._doc_files) - 1, page - 1))
            path = self._doc_files[idx]
            self.var_file.set(path)
            self.load_document(path, 1)
        else:
            path = self.var_file.get().strip()
            if path:
                self.load_document(path, page)

    def on_page_prev(self) -> None:
        p = max(1, self.var_page.get() - 1)
        self.var_page.set(p)
        self.on_reload_page()

    def on_page_next(self) -> None:
        limit = self._total_pages if self._total_pages > 1 else 9999
        p = min(limit, self.var_page.get() + 1)
        self.var_page.set(p)
        self.on_reload_page()

    def _count_pdf_pages(self, path: str) -> int:
        if fitz is not None:
            try:
                doc = fitz.open(path)
                n = doc.page_count
                doc.close()
                if n > 0:
                    return n
            except Exception:
                pass
        if pdfplumber is not None:
            try:
                with pdfplumber.open(path) as _pdf:
                    return max(1, len(_pdf.pages))
            except Exception:
                pass
        return 1

    def _update_page_nav(self) -> None:
        if hasattr(self, "lbl_total_pages"):
            self.lbl_total_pages.configure(text=f"/ {self._total_pages}")

    def load_document(self, path: str, page: int) -> None:
        try:
            image, meta = self.loader.open_document_as_image(path, page=page)
        except Exception as e:
            self.show_error(str(e))
            return

        self.document = DocumentState(
            path=path,
            file_type=str(meta["file_type"]),
            page=int(meta["page"]),
            image_original=image,
            original_width=int(meta["original_width"]),
            original_height=int(meta["original_height"]),
        )
        if len(self._doc_files) > 1:
            self._total_pages = len(self._doc_files)   # multi-archivo: no pisar
        else:
            self._total_pages = int(meta.get("total_pages") or 1)
        self._update_page_nav()
        self.config.last_file = path
        self._zoom_factor = 1.0
        save_last_connection(self._current_config())
        self._fit_image_to_canvas()
        self._refresh_layout_summary()
        n_info = f" ({self._total_pages} pág.)" if self._total_pages > 1 else ""
        self.set_status(f"Documento cargado: {Path(path).name}{n_info} | {self.document.original_width}x{self.document.original_height}")

    def _fit_image_to_canvas(self) -> None:
        if self.document.image_original is None or ImageTk is None:
            return
        canvas_w = max(200, self.canvas.winfo_width())
        canvas_h = max(200, self.canvas.winfo_height())
        src = self.document.image_original
        fit_ratio = min(canvas_w / src.width, canvas_h / src.height)
        ratio = fit_ratio * self._zoom_factor
        ratio = max(0.1, min(ratio, 6.0))
        new_size = (max(1, int(src.width * ratio)), max(1, int(src.height * ratio)))
        display = src.resize(new_size, Image.Resampling.LANCZOS if hasattr(Image, "Resampling") else Image.LANCZOS)
        self.document.image_display = display
        self.document.display_scale = ratio
        self.document.display_offset_x = 0
        self.document.display_offset_y = 0
        self._redraw_canvas()

    def _redraw_canvas(self) -> None:
        self.canvas.delete("all")
        self.canvas_items.clear()
        self.column_item_ids.clear()
        if self.document.image_display is None or ImageTk is None:
            return
        self.tk_img = ImageTk.PhotoImage(self.document.image_display)
        self.canvas.create_image(0, 0, image=self.tk_img, anchor="nw")
        self.canvas.configure(scrollregion=(0, 0, self.document.image_display.width, self.document.image_display.height))

        for zone_type, rect in self.zones.items():
            self._draw_zone(zone_type, rect)
        for col in self.detail_columns:
            self._draw_detail_column(col)
        self._refresh_zones_list()

    def _normalize_ai_zone_rect(self, raw_rect: Any) -> Optional[Dict[str, float]]:
        if not isinstance(raw_rect, dict):
            return None
        try:
            rect = normalize_rect(
                float(raw_rect.get("x1", 0.0)),
                float(raw_rect.get("y1", 0.0)),
                float(raw_rect.get("x2", 0.0)),
                float(raw_rect.get("y2", 0.0)),
            )
        except Exception:
            return None
        if abs(rect["x2"] - rect["x1"]) < 0.01 or abs(rect["y2"] - rect["y1"]) < 0.01:
            return None
        return rect

    def _apply_ai_detection(self, data: Dict[str, Any]) -> Tuple[int, int]:
        proveedor = data.get("proveedor") if isinstance(data.get("proveedor"), dict) else {}
        zonas = data.get("zonas") if isinstance(data.get("zonas"), dict) else {}
        detalle_columnas = data.get("detalle_columnas") if isinstance(data.get("detalle_columnas"), list) else []

        zone_count = 0
        for zone_type in ZONE_TYPES:
            rect = self._normalize_ai_zone_rect(zonas.get(zone_type))
            if rect:
                self.zones[zone_type] = rect
                zone_count += 1

        if str(proveedor.get("nombre") or "").strip() and not self.var_provider_name.get().strip():
            self.var_provider_name.set(str(proveedor.get("nombre") or "").strip())
            self.var_search_text.set(self.var_provider_name.get())
        if str(proveedor.get("cuit") or "").strip() and not self.var_provider_cuit.get().strip():
            self.var_provider_cuit.set(only_digits(str(proveedor.get("cuit") or "")))

        valid_fields = set(DETAIL_FIELDS)
        loaded_columns: List[Dict[str, Any]] = []
        for item in detalle_columnas:
            if not isinstance(item, dict):
                continue
            campo = str(item.get("campo") or "").strip()
            if not campo or campo not in valid_fields:
                continue
            try:
                x1 = max(0.0, min(1.0, float(item.get("x1", 0.0))))
                x2 = max(0.0, min(1.0, float(item.get("x2", 0.0))))
            except Exception:
                continue
            if abs(x2 - x1) < 0.01:
                continue
            loaded_columns.append(LayoutBuilder.build_detail_column(campo, x1, x2))

        # Reemplaza columnas homónimas, pero conserva las demás si la IA devolvió pocas.
        by_field = {c["campo"]: c for c in self.detail_columns}
        for col in loaded_columns:
            by_field[col["campo"]] = col
        self.detail_columns = sorted(by_field.values(), key=lambda x: x["x1"])
        self._redraw_canvas()
        return zone_count, len(loaded_columns)

    def _draw_zone(self, zone_type: str, rect: Dict[str, float]) -> None:
        scale = self.document.display_scale
        x1 = rect["x1"] * self.document.original_width * scale
        y1 = rect["y1"] * self.document.original_height * scale
        x2 = rect["x2"] * self.document.original_width * scale
        y2 = rect["y2"] * self.document.original_height * scale
        color = ZONE_COLORS.get(zone_type, "#333333")
        is_selected = zone_type == self.selected_zone_type
        rect_id = self.canvas.create_rectangle(
            x1,
            y1,
            x2,
            y2,
            outline=color,
            width=4 if is_selected else 2,
            dash=(6, 3) if is_selected else (),
        )
        label_id = self.canvas.create_text(x1 + 6, y1 + 6, text=zone_type, fill=color, anchor="nw", font=("Segoe UI", 9, "bold"))
        if is_selected:
            self.canvas.create_rectangle(
                x1 - 2,
                y1 - 2,
                x2 + 2,
                y2 + 2,
                outline="#111111",
                width=1,
                dash=(2, 2),
            )
        self.canvas_items[f"zone:{zone_type}:rect"] = rect_id
        self.canvas_items[f"zone:{zone_type}:label"] = label_id

    def _draw_detail_column(self, col: Dict[str, Any]) -> None:
        scale = self.document.display_scale
        x1 = col["x1"] * self.document.original_width * scale
        x2 = col["x2"] * self.document.original_width * scale
        if "detalle" in self.zones:
            detail_rect = self.zones["detalle"]
            y1 = detail_rect["y1"] * self.document.original_height * scale
            y2 = detail_rect["y2"] * self.document.original_height * scale
        else:
            y1, y2 = 0, self.document.image_display.height
        is_selected = col.get("campo") == self.selected_column_field
        item = self.canvas.create_rectangle(
            x1,
            y1,
            x2,
            y2,
            outline="#E36414",
            dash=(4, 2) if not is_selected else (7, 3),
            width=2 if not is_selected else 4,
        )
        text = self.canvas.create_text(
            x1 + 4,
            y1 + 4,
            text=col["campo"],
            fill="#E36414",
            anchor="nw",
            font=("Segoe UI", 8, "bold"),
        )
        self.column_item_ids.extend([item, text])

    def _refresh_zones_list(self) -> None:
        self.zone_listbox.delete(0, "end")
        for zone_type in ZONE_TYPES:
            rect = self.zones.get(zone_type)
            if rect:
                self.zone_listbox.insert("end", f"Zona {zone_type}: {rect_to_text(rect)}")
        if self.selected_zone_type:
            for i in range(self.zone_listbox.size()):
                text = self.zone_listbox.get(i)
                if text.startswith(f"Zona {self.selected_zone_type}:"):
                    self.zone_listbox.selection_clear(0, "end")
                    self.zone_listbox.selection_set(i)
                    break
        self.columns_listbox.delete(0, "end")
        for idx, col in enumerate(self.detail_columns, start=1):
            self.columns_listbox.insert("end", f"Col {idx} {col['campo']}: x1={col['x1']:.4f}, x2={col['x2']:.4f}")
        if self.selected_column_field:
            for i in range(self.columns_listbox.size()):
                text = self.columns_listbox.get(i)
                if f" {self.selected_column_field}:" in text:
                    self.columns_listbox.selection_clear(0, "end")
                    self.columns_listbox.selection_set(i)
                    break
        self._refresh_layout_summary()

    def _refresh_layout_summary(self) -> None:
        lines: List[str] = []
        file_name = self.var_file.get().strip() or "(sin archivo)"
        lines.append(f"Archivo: {file_name}")
        lines.append(f"Página: {self.var_page.get()}")
        lines.append("")
        lines.append(f"Proveedor código: {self.var_provider_code.get().strip() or '(vacío)'}")
        lines.append(f"Proveedor nombre: {self.var_provider_name.get().strip() or '(vacío)'}")
        lines.append(f"Proveedor CUIT: {self.var_provider_cuit.get().strip() or '(vacío)'}")
        lines.append("")
        lines.append(f"Clave destino: IA_LYT_{self.var_provider_code.get().strip() or '...'}")
        lines.append(f"Grupo destino: {DEFAULT_LAYOUT_GROUP}")
        lines.append("")
        lines.append("Zonas:")
        for zone_type in ZONE_TYPES:
            rect = self.zones.get(zone_type)
            lines.append(f"- {zone_type}: {rect_to_text(rect) if rect else '(sin marcar)'}")
        lines.append("")
        lines.append("Columnas detalle:")
        if self.detail_columns:
            for col in self.detail_columns:
                lines.append(f"- {col['campo']}: x1={col['x1']:.4f}, x2={col['x2']:.4f}")
        else:
            lines.append("- (sin columnas)")
        lines.append("")
        lines.append(f"Prompt file: {self.var_prompt_file.get().strip() or '(vacío)'}")

        self.summary_text.configure(state="normal")
        self.summary_text.delete("1.0", "end")
        self.summary_text.insert("1.0", "\n".join(lines))
        self.summary_text.configure(state="disabled")

    def _show_draw_banner(self, mode: str, name: str) -> None:
        msg = f"✏  Arrastrá sobre la imagen para marcar: {name}    [Esc para cancelar]"
        if mode == "zone" and hasattr(self, "zone_draw_banner"):
            self.zone_draw_banner.configure(text=msg)
            self.zone_draw_banner.grid()
        elif mode == "column" and hasattr(self, "column_draw_banner"):
            self.column_draw_banner.configure(text=msg)
            self.column_draw_banner.grid()

    def _hide_draw_banners(self) -> None:
        if hasattr(self, "zone_draw_banner"):
            self.zone_draw_banner.grid_remove()
        if hasattr(self, "column_draw_banner"):
            self.column_draw_banner.grid_remove()

    def _cancel_draw_mode(self, _event: Any = None) -> None:
        if self._mode:
            self._mode = ""
            self._selection_start = None
            if self._selection_rect_id is not None:
                try:
                    self.canvas.delete(self._selection_rect_id)
                except Exception:
                    pass
                self._selection_rect_id = None
            self._hide_draw_banners()
            self.set_status("Marcación cancelada.")

    def on_start_mark_zone(self) -> None:
        if self.document.image_original is None:
            self.show_error("Abrí un archivo antes de marcar zonas.")
            return
        self._mode = "zone"
        self.selected_zone_type = self.var_zone_type.get()
        self._select_left_tab(1)
        self._show_draw_banner("zone", self.var_zone_type.get())
        self.set_status(f"Marcando zona: {self.var_zone_type.get()}. Arrastrá sobre la imagen.")

    def on_marked_zone_selected(self, _event: Any = None) -> None:
        selection = self.zone_listbox.curselection()
        if not selection:
            return
        text = self.zone_listbox.get(selection[0])
        if text.startswith("Zona "):
            zone_type = text.split(":", 1)[0].replace("Zona ", "").strip()
            # Guard: misma zona → evitar loop infinito de <<ListboxSelect>>
            if zone_type == self.selected_zone_type:
                return
            self.selected_zone_type = zone_type
            self.var_zone_type.set(zone_type)
            try:
                idx = ZONE_TYPES.index(zone_type)
                self.zone_type_listbox.selection_clear(0, "end")
                self.zone_type_listbox.selection_set(idx)
                self.zone_type_listbox.see(idx)
            except Exception:
                pass
            self._redraw_canvas()

    def on_edit_selected_zone(self) -> None:
        if not self.selected_zone_type:
            selection = self.zone_listbox.curselection()
            if selection:
                self.on_marked_zone_selected()
        if not self.selected_zone_type:
            self.show_error("Seleccioná una zona marcada para modificar.")
            return
        self.var_zone_type.set(self.selected_zone_type)
        self.on_start_mark_zone()

    def on_start_mark_column(self) -> None:
        if self.document.image_original is None:
            self.show_error("Abrí un archivo antes de marcar columnas.")
            return
        if "detalle" not in self.zones:
            self.show_error("Primero marcá la zona 'detalle'.")
            return
        self._mode = "column"
        self.selected_column_field = self.var_column_field.get()
        self._select_left_tab(2)
        self._show_draw_banner("column", self.var_column_field.get())
        self.set_status(f"Marcando columna detalle: {self.var_column_field.get()}. Arrastrá sobre la imagen.")

    def on_marked_column_selected(self, _event: Any = None) -> None:
        selection = self.columns_listbox.curselection()
        if not selection:
            return
        text = self.columns_listbox.get(selection[0])
        if text.startswith("Col "):
            field_name = text.split(":", 1)[0].split(" ", 2)[-1].strip()
            # Guard: mismo campo → evitar loop infinito de <<ListboxSelect>>
            if field_name == self.selected_column_field:
                return
            self.selected_column_field = field_name
            self.var_column_field.set(field_name)
            try:
                idx = DETAIL_FIELDS.index(field_name)
                self.column_field_listbox.selection_clear(0, "end")
                self.column_field_listbox.selection_set(idx)
                self.column_field_listbox.see(idx)
            except Exception:
                pass
            self._redraw_canvas()

    def on_edit_selected_column(self) -> None:
        if not self.selected_column_field:
            selection = self.columns_listbox.curselection()
            if selection:
                self.on_marked_column_selected()
        if not self.selected_column_field:
            self.show_error("Seleccioná una columna marcada para modificar.")
            return
        self.var_column_field.set(self.selected_column_field)
        self.on_start_mark_column()

    def on_clear_columns(self) -> None:
        self.detail_columns.clear()
        self.selected_column_field = ""
        self._redraw_canvas()
        self._select_left_tab(2)
        self.set_status("Columnas detalle eliminadas.")

    def on_canvas_press(self, event: Any) -> None:
        if self._mode not in {"zone", "column"}:
            return
        self._selection_start = (int(self.canvas.canvasx(event.x)), int(self.canvas.canvasy(event.y)))
        if self._selection_rect_id is not None:
            self.canvas.delete(self._selection_rect_id)
        self._selection_rect_id = self.canvas.create_rectangle(
            self._selection_start[0],
            self._selection_start[1],
            self._selection_start[0],
            self._selection_start[1],
            outline="#111111",
            dash=(3, 2),
        )

    def on_canvas_drag(self, event: Any) -> None:
        if self._selection_start is None or self._selection_rect_id is None:
            return
        x, y = int(self.canvas.canvasx(event.x)), int(self.canvas.canvasy(event.y))
        self.canvas.coords(self._selection_rect_id, self._selection_start[0], self._selection_start[1], x, y)

    def on_canvas_release(self, event: Any) -> None:
        if self._selection_start is None:
            return
        end = (int(self.canvas.canvasx(event.x)), int(self.canvas.canvasy(event.y)))
        start = self._selection_start
        self._selection_start = None
        if self._selection_rect_id is not None:
            self.canvas.delete(self._selection_rect_id)
            self._selection_rect_id = None

        try:
            rx1, ry1, rx2, ry2 = self._display_rect_to_relative(start[0], start[1], end[0], end[1])
        except Exception as e:
            self.show_error(str(e))
            return
        if abs(rx2 - rx1) < 0.002 or abs(ry2 - ry1) < 0.002:
            self.set_status("Selección demasiado pequeña; intentá de nuevo.")
            return

        if self._mode == "zone":
            zone_type = self.var_zone_type.get()
            self.zones[zone_type] = normalize_rect(rx1, ry1, rx2, ry2)
            self.selected_zone_type = zone_type
            self.set_status(f"Zona '{zone_type}' guardada.")
        elif self._mode == "column":
            field_name = self.var_column_field.get()
            self.detail_columns = [c for c in self.detail_columns if c.get("campo") != field_name]
            self.detail_columns.append(LayoutBuilder.build_detail_column(field_name, rx1, rx2))
            self.detail_columns.sort(key=lambda x: x["x1"])
            self.selected_column_field = field_name
            self.set_status(f"Columna '{field_name}' guardada.")
        self._mode = ""
        self._hide_draw_banners()
        self._redraw_canvas()

    def _display_rect_to_relative(self, x1: int, y1: int, x2: int, y2: int) -> Tuple[float, float, float, float]:
        if self.document.image_display is None:
            raise RuntimeError("No hay imagen cargada.")
        scale = self.document.display_scale
        w = self.document.original_width
        h = self.document.original_height
        rx1 = max(0.0, min(1.0, x1 / scale / w))
        ry1 = max(0.0, min(1.0, y1 / scale / h))
        rx2 = max(0.0, min(1.0, x2 / scale / w))
        ry2 = max(0.0, min(1.0, y2 / scale / h))
        return rx1, ry1, rx2, ry2

    def crop_zone(self, zone_type: str) -> Any:
        rect = self.zones.get(zone_type)
        if not rect:
            raise RuntimeError(f"No está marcada la zona '{zone_type}'.")
        if self.document.image_original is None:
            raise RuntimeError("No hay documento cargado.")
        x1 = int(rect["x1"] * self.document.original_width)
        y1 = int(rect["y1"] * self.document.original_height)
        x2 = int(rect["x2"] * self.document.original_width)
        y2 = int(rect["y2"] * self.document.original_height)
        return self.document.image_original.crop((x1, y1, x2, y2))

    def on_ocr_provider_zone(self) -> None:
        try:
            img = self.crop_zone("proveedor")
            text = self._ocr_service().ocr_image(img)
        except Exception as e:
            self.show_error(str(e))
            return
        self.document.ocr_text_cache["proveedor"] = text
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        preview = " | ".join(lines[:3])[:250]
        cuit_candidates = extract_cuit_candidates(text)
        if cuit_candidates and not self.var_provider_cuit.get().strip():
            self.var_provider_cuit.set(cuit_candidates[0])
        if lines and not self.var_provider_name.get().strip():
            self.var_provider_name.set(lines[0])
        self.show_info(f"OCR zona proveedor OK.\n{preview or '(sin texto)'}")

    def on_detect_layout_with_ai(self) -> None:
        if self.document.image_original is None:
            self.show_error("Abrí un archivo antes de pedir detección automática por IA.")
            return
        self._select_left_tab(1)
        self.set_status("Analizando layout con IA...")
        self._show_busy(
            "Detección IA",
            "Analizando el comprobante y proponiendo zonas automáticamente...",
            cancellable=True,
            on_cancel=lambda: self.set_status("Detección IA cancelada por el usuario."),
        )

        result_queue: "queue.Queue[Tuple[str, Any]]" = queue.Queue()
        image_copy = self.document.image_original.copy()
        source_filename = Path(self.document.path or "comprobante.jpg").name
        model_name = self.var_ai_model.get().strip() or DEFAULT_AI_MODEL

        def _worker() -> None:
            try:
                service = LayoutDetectionAIService(model=model_name)
                data = service.detect_from_image(image_copy, source_filename=source_filename)
                result_queue.put(("ok", data))
            except Exception as e:
                result_queue.put(("error", str(e)))

        threading.Thread(target=_worker, daemon=True).start()

        def _poll() -> None:
            cancel_event = self._busy_cancel_event
            if cancel_event is not None and cancel_event.is_set():
                return
            try:
                status, payload = result_queue.get_nowait()
            except queue.Empty:
                if self._busy_window is not None:
                    self.root.after(150, _poll)
                return

            if cancel_event is not None and cancel_event.is_set():
                return
            self._close_busy()
            if status == "error":
                self.show_error(str(payload))
                return

            data = payload
            zone_count, column_count = self._apply_ai_detection(data)
            comentario = str(data.get("comentario") or "").strip() if isinstance(data, dict) else ""
            msg = f"IA aplicada: {zone_count} zona(s) y {column_count} columna(s) sugerida(s)."
            if comentario:
                msg += f"\n\nObservación IA: {comentario[:500]}"
            self.show_info(msg + "\n\nRevisá y corregí manualmente antes de guardar.")

        self.root.after(150, _poll)

    def on_search_provider(self) -> None:
        try:
            lookup = ProviderLookup(self._sql_client())
            lookup.detect_provider_columns()
            cuit = self.var_provider_cuit.get().strip()
            text = self.var_search_text.get().strip() or self.var_provider_name.get().strip()
            results = lookup.search_best_effort(cuit=cuit, text=text)
        except Exception as e:
            self.show_error(str(e))
            return
        self._populate_provider_results(results)
        self._select_left_tab(3)
        self.set_status(f"Proveedores encontrados: {len(results)}")

    def on_manage_layouts(self) -> None:
        LayoutManagerDialog(self)

    def on_search_saved_layouts(self) -> None:
        try:
            lookup = ProviderLookup(self._sql_client())
            lookup.detect_provider_columns()
            cuit = self.var_provider_cuit.get().strip()
            text = self.var_search_text.get().strip() or self.var_provider_name.get().strip()
            results = lookup.search_saved_layouts(cuit=cuit, text=text)
        except Exception as e:
            self.show_error(str(e))
            return
        self._populate_provider_results(results)
        self._select_left_tab(3)
        self.set_status(f"Layouts encontrados: {len(results)}")

    def _populate_provider_results(self, results: List[ProviderMatch]) -> None:
        for item in self.provider_tree.get_children():
            self.provider_tree.delete(item)
        for match in results:
            self.provider_tree.insert("", "end", values=(match.code, match.name, match.cuit, match.address))

    def on_propose_provider(self) -> None:
        ocr_text = self.document.ocr_text_cache.get("proveedor", "")
        if not ocr_text:
            try:
                img = self.crop_zone("proveedor")
                ocr_text = self._ocr_service().ocr_image(img)
                self.document.ocr_text_cache["proveedor"] = ocr_text
            except Exception as e:
                self.show_error(str(e))
                return

        cuit_candidates = extract_cuit_candidates(ocr_text)
        cuit = cuit_candidates[0] if cuit_candidates else ""
        lines = [line.strip() for line in ocr_text.splitlines() if line.strip()]
        sample_text = ""
        for line in lines:
            if len(line) >= 4 and "CUIT" not in line.upper():
                sample_text = line
                break
        if cuit and not self.var_provider_cuit.get().strip():
            self.var_provider_cuit.set(cuit)
        if sample_text and not self.var_provider_name.get().strip():
            self.var_provider_name.set(sample_text)

        try:
            lookup = ProviderLookup(self._sql_client())
            lookup.detect_provider_columns()
            results = lookup.search_best_effort(cuit=cuit, text=sample_text)
        except Exception as e:
            self.show_error(str(e))
            return

        self._populate_provider_results(results)
        if len(results) == 1 and results[0].score >= 60:
            self._apply_provider_match(results[0])
            self.show_info(f"Proveedor propuesto automáticamente: {results[0].code} - {results[0].name}")
            return
        if results:
            self.set_status(f"Se encontraron {len(results)} coincidencias. Seleccioná una.")
        else:
            self.set_status("No se encontraron proveedores. Podés ingresar el código manualmente.")
            self.show_info("No se encontró proveedor automático. Cargalo manualmente si corresponde.")

    def on_provider_selected(self, _event: Any) -> None:
        selected = self.provider_tree.selection()
        if not selected:
            return
        values = self.provider_tree.item(selected[0], "values")
        match = ProviderMatch(
            code=str(values[0]),
            name=str(values[1]),
            cuit=str(values[2]),
            address=str(values[3]),
        )
        self._apply_provider_match(match)
        self.set_status(f"Proveedor seleccionado: {match.code} - {match.name}")

    def _apply_provider_match(self, match: ProviderMatch) -> None:
        self.var_provider_code.set(match.code)
        self.var_provider_name.set(match.name)
        if match.cuit:
            self.var_provider_cuit.set(match.cuit)
        self.var_search_text.set(match.name)
        self._refresh_layout_summary()

    def _apply_loaded_layout(self, layout_json: Dict[str, Any]) -> None:
        proveedor = layout_json.get("proveedor") if isinstance(layout_json.get("proveedor"), dict) else {}
        origen = layout_json.get("origen") if isinstance(layout_json.get("origen"), dict) else {}
        zonas = layout_json.get("zonas") if isinstance(layout_json.get("zonas"), dict) else {}
        detalle_columnas = layout_json.get("detalle_columnas") if isinstance(layout_json.get("detalle_columnas"), list) else []

        self.var_provider_code.set(str(proveedor.get("codigo") or self.var_provider_code.get()).strip())
        self.var_provider_name.set(str(proveedor.get("nombre") or "").strip())
        self.var_provider_cuit.set(str(proveedor.get("cuit") or "").strip())
        self.var_search_text.set(self.var_provider_name.get())
        self.var_prompt_file.set(str(layout_json.get("prompt_file") or "").strip())

        loaded_zones: Dict[str, Dict[str, float]] = {}
        for zone_type in ZONE_TYPES:
            rect = zonas.get(zone_type)
            if isinstance(rect, dict):
                try:
                    loaded_zones[zone_type] = normalize_rect(
                        float(rect.get("x1", 0.0)),
                        float(rect.get("y1", 0.0)),
                        float(rect.get("x2", 0.0)),
                        float(rect.get("y2", 0.0)),
                    )
                except Exception:
                    continue
        self.zones = loaded_zones

        loaded_columns: List[Dict[str, Any]] = []
        for item in detalle_columnas:
            if not isinstance(item, dict):
                continue
            campo = str(item.get("campo") or "").strip()
            if not campo:
                continue
            try:
                loaded_columns.append(
                    LayoutBuilder.build_detail_column(
                        campo,
                        float(item.get("x1", 0.0)),
                        float(item.get("x2", 0.0)),
                    )
                )
            except Exception:
                continue
        self.detail_columns = sorted(loaded_columns, key=lambda x: x["x1"])

        source_file = str(origen.get("archivo_ejemplo") or "").strip()
        page = int(origen.get("pagina") or 1)
        if source_file:
            self.var_file.set(source_file)
            self.var_page.set(page)
            if Path(source_file).exists():
                self.load_document(source_file, page)
                self._select_left_tab(4)
                return
        self._redraw_canvas()
        self._select_left_tab(4)

    def on_load_layout(self) -> None:
        try:
            repo = LayoutRepository(self._sql_client())
            key, layout_json = repo.load_layout_from_sql(self.var_provider_code.get())
            self._apply_loaded_layout(layout_json)
        except Exception as e:
            self.show_error(str(e))
            return

        source_file = str(layout_json.get("origen", {}).get("archivo_ejemplo") or "").strip() if isinstance(layout_json.get("origen"), dict) else ""
        if source_file and not Path(source_file).exists():
            self.show_info(
                f"Layout {key} cargado. El archivo de ejemplo guardado ya no existe en disco, "
                "pero las zonas y columnas fueron recuperadas."
            )
        else:
            self.show_info(f"Layout {key} cargado correctamente.")

    def on_delete_selected_zone(self) -> None:
        selection = self.zone_listbox.curselection()
        if not selection:
            self.show_error("Seleccioná una zona en la lista.")
            return
        text = self.zone_listbox.get(selection[0])
        if text.startswith("Zona "):
            zone_type = text.split(":", 1)[0].replace("Zona ", "").strip()
            self.zones.pop(zone_type, None)
            if self.selected_zone_type == zone_type:
                self.selected_zone_type = ""
            self.set_status(f"Zona '{zone_type}' eliminada.")
        self._redraw_canvas()

    def on_delete_selected_column(self) -> None:
        selection = self.columns_listbox.curselection()
        if not selection:
            self.show_error("Seleccioná una columna en la lista.")
            return
        text = self.columns_listbox.get(selection[0])
        if text.startswith("Col "):
            field_name = text.split(":", 1)[0].split(" ", 2)[-1].strip()
            self.detail_columns = [c for c in self.detail_columns if c.get("campo") != field_name]
            if self.selected_column_field == field_name:
                self.selected_column_field = ""
            self.set_status(f"Columna '{field_name}' eliminada.")
            self._redraw_canvas()

    def build_layout(self) -> Dict[str, Any]:
        if self.document.image_original is None:
            raise RuntimeError("No hay documento cargado.")
        if "detalle" not in self.zones:
            raise RuntimeError("Falta marcar la zona 'detalle'.")
        return build_layout_json(
            provider_code=self.var_provider_code.get(),
            provider_name=self.var_provider_name.get(),
            provider_cuit=self.var_provider_cuit.get(),
            source_file=self.document.path,
            file_type=self.document.file_type,
            page=self.document.page,
            original_width=self.document.original_width,
            original_height=self.document.original_height,
            zones=self.zones,
            detail_columns=self.detail_columns,
            prompt_file=self.var_prompt_file.get(),
        )

    def _new_layout(self) -> None:
        self.document = DocumentState()
        self.zones = {}
        self.detail_columns = []
        self.canvas_items = {}
        self.column_item_ids = []
        self._mode = ""
        self.selected_zone_type = ""
        self.selected_column_field = ""
        self._zoom_factor = 1.0
        self._total_pages = 1
        self._doc_files = []
        self._update_page_nav()
        self._selection_start = None
        if self._selection_rect_id is not None:
            try:
                self.canvas.delete(self._selection_rect_id)
            except Exception:
                pass
            self._selection_rect_id = None
        self._hide_draw_banners()

        self.var_file.set("")
        self.var_page.set(1)
        self.var_provider_code.set("")
        self.var_provider_name.set("")
        self.var_provider_cuit.set("")
        self.var_prompt_file.set("")
        self.var_search_text.set("")

        self.zone_listbox.delete(0, "end")
        self.columns_listbox.delete(0, "end")
        self.provider_tree.delete(*self.provider_tree.get_children())
        self.zone_type_listbox.selection_clear(0, "end")
        self.zone_type_listbox.selection_set(0)
        self.column_field_listbox.selection_clear(0, "end")
        self.column_field_listbox.selection_set(0)

        self.canvas.delete("all")
        self.canvas.configure(scrollregion=(0, 0, 0, 0))
        self._refresh_layout_summary()
        self._select_left_tab(0)
        self.set_status("Listo para un nuevo layout.")

    def on_new_layout(self) -> None:
        if self.zones or self.detail_columns or self.var_provider_code.get().strip():
            if not messagebox.askyesno(
                APP_TITLE,
                "¿Descartár el trabajo actual y comenzar un nuevo layout?",
            ):
                return
        self._new_layout()

    def on_save_layout(self) -> None:
        try:
            layout_json = self.build_layout()
            repo = LayoutRepository(self._sql_client())
            key = repo.save_layout_to_sql(self.var_provider_code.get(), layout_json)
            save_last_connection(self._current_config())
        except Exception as e:
            self.show_error(str(e))
            return
        if messagebox.askyesno(
            APP_TITLE,
            f"Layout guardado con clave {key}\n\n¿Querés comenzar un nuevo layout?",
        ):
            self._new_layout()
        else:
            self._select_left_tab(0)
            self.set_status(f"Layout {key} guardado.")


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Editor visual de layouts de factura por proveedor.")
    parser.add_argument("--server", default="")
    parser.add_argument("--database", default="")
    parser.add_argument("--user", default="")
    parser.add_argument("--password", default="")
    parser.add_argument("--driver", default="")
    parser.add_argument("--tesseract-cmd", default="")
    return parser.parse_args(argv)


def build_app_config(args: argparse.Namespace) -> AppConfig:
    cli_conn = args.server or args.database or args.user or args.password
    cfg = AppConfig() if cli_conn else load_last_connection()
    cfg.server   = args.server   or cfg.server   or DEFAULT_SERVER
    cfg.database = args.database or cfg.database or DEFAULT_DATABASE
    cfg.user     = args.user     or cfg.user     or DEFAULT_USER
    cfg.password = args.password or cfg.password or DEFAULT_PASSWORD
    cfg.driver   = args.driver   or cfg.driver   or DEFAULT_DRIVER
    if args.tesseract_cmd:
        cfg.tesseract_cmd = args.tesseract_cmd
    return cfg


def _find_tesseract_exe() -> str:
    """Busca tesseract.exe en PATH y en ubicaciones estándar de Windows."""
    found = shutil.which("tesseract")
    if found:
        return found
    candidates = [
        r"C:\Program Files\Tesseract-OCR\tesseract.exe",
        r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
    ]
    for c in candidates:
        if Path(c).exists():
            return c
    return ""


_TESSERACT_COMMON_PATHS = [
    r"C:\Program Files\Tesseract-OCR\tesseract.exe",
    r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
]


def _find_tesseract_exe(configured_cmd: str = "") -> str:
    """Devuelve la ruta al exe de Tesseract o '' si no se encuentra."""
    candidates = []
    if configured_cmd:
        candidates.append(configured_cmd)
    if shutil.which("tesseract"):
        candidates.append(shutil.which("tesseract"))
    candidates.extend(_TESSERACT_COMMON_PATHS)
    for p in candidates:
        if p and Path(p).is_file():
            return p
    return ""


def _check_optional_deps(parent: "tk.Tk", cfg: "AppConfig | None" = None) -> None:
    """Detecta dependencias opcionales faltantes y ofrece descargarlas."""
    missing = []

    # Tesseract OCR
    tesseract_ok = False
    configured_cmd = (cfg.tesseract_cmd if cfg else "") or ""
    exe = _find_tesseract_exe(configured_cmd)
    if exe and pytesseract is not None:
        try:
            pytesseract.pytesseract.tesseract_cmd = exe
            pytesseract.get_tesseract_version()
            tesseract_ok = True
        except Exception:
            pass
    if not tesseract_ok:
        missing.append({
            "name": "Tesseract OCR",
            "desc": 'Necesario para "Probar OCR zona proveedor".',
            "url": "https://github.com/UB-Mannheim/tesseract/wiki",
            "btn": "Descargar Tesseract",
        })

    # ODBC Driver para SQL Server — verificar el driver exacto configurado por defecto
    odbc_ok = False
    if pyodbc is not None:
        try:
            odbc_ok = DEFAULT_DRIVER in pyodbc.drivers()
        except Exception:
            pass
    if not odbc_ok:
        missing.append({
            "name": DEFAULT_DRIVER,
            "desc": "Necesario para conectar a la base de datos ALFANET.",
            "url": "https://aka.ms/odbc18",
            "btn": "Descargar ODBC Driver 18",
        })

    if not missing:
        return

    dlg = tk.Toplevel(parent)
    dlg.title("Dependencias faltantes")
    dlg.resizable(False, False)
    dlg.grab_set()
    dlg.focus_set()

    # Centrar respecto al padre
    parent.update_idletasks()
    pw, ph = parent.winfo_width(), parent.winfo_height()
    px, py = parent.winfo_x(), parent.winfo_y()
    dlg.update_idletasks()
    dw, dh = 480, 80 + len(missing) * 72
    dlg.geometry(f"{dw}x{dh}+{px + (pw - dw)//2}+{py + (ph - dh)//2}")

    ttk.Label(
        dlg,
        text="Se detectaron dependencias opcionales no instaladas:",
        font=("Segoe UI", 10, "bold"),
    ).grid(row=0, column=0, columnspan=2, sticky="w", padx=16, pady=(14, 8))

    for i, dep in enumerate(missing):
        row_base = 1 + i * 3
        ttk.Label(dlg, text=f"• {dep['name']}", font=("Segoe UI", 9, "bold")).grid(
            row=row_base, column=0, sticky="w", padx=20, pady=(6, 0)
        )
        ttk.Label(dlg, text=dep["desc"], foreground="#555555").grid(
            row=row_base + 1, column=0, sticky="w", padx=32, pady=(0, 2)
        )
        ttk.Button(
            dlg,
            text=dep["btn"],
            command=lambda url=dep["url"]: webbrowser.open(url),
        ).grid(row=row_base, column=1, rowspan=2, padx=(8, 16), pady=4, sticky="ew")

    sep_row = 1 + len(missing) * 3
    ttk.Separator(dlg, orient="horizontal").grid(
        row=sep_row, column=0, columnspan=2, sticky="ew", padx=12, pady=10
    )
    ttk.Button(dlg, text="Continuar de todos modos", command=dlg.destroy).grid(
        row=sep_row + 1, column=0, columnspan=2, pady=(0, 14)
    )

    parent.wait_window(dlg)


def main(argv: Optional[Sequence[str]] = None) -> int:
    if tk is None or ttk is None or messagebox is None or filedialog is None:
        print("ERROR: tkinter no está disponible en este entorno.")
        return 1
    if Image is None:
        print("ERROR: Pillow no está instalado. Ejecutá: pip install pillow")
        return 1

    args = parse_args(argv)
    cfg = build_app_config(args)
    root = tk.Tk()
    root.state("zoomed")
    _check_optional_deps(root, cfg)
    LayoutEditorApp(root, cfg)
    root.mainloop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
