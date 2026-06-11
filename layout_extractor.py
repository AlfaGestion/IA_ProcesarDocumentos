# -*- coding: utf-8 -*-
"""
layout_extractor.py

Extrae datos de un comprobante usando el layout configurado para el proveedor,
sin llamar a IA.  Se invoca como pre-proceso desde lector_facturas_to_json_v5.py.

Contrato:
  try_layout_extraction(layout_data, files, log_fn=None) -> dict | None

  - Recibe el dict del layout ya cargado (leído de un archivo JSON por el llamador).
  - Retorna None ante cualquier error, falta de dependencia o resultado poco
    confiable.  El lector debe usar IA en ese caso.
  - No lanza excepciones al llamador.

Dependencias (todas opcionales — si no están instaladas, retorna None):
  pillow, pymupdf (fitz), pdfplumber, pytesseract
"""

from __future__ import annotations

import json
import re
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# --------------------------------------------------------------------------- #
# Dependencias opcionales
# --------------------------------------------------------------------------- #
try:
    from PIL import Image, ImageOps
except ImportError:
    Image = None
    ImageOps = None

try:
    import fitz  # pymupdf
except ImportError:
    fitz = None

try:
    import pdfplumber
except ImportError:
    pdfplumber = None

try:
    import pytesseract
    # En Windows Tesseract suele no estar en PATH aunque esté instalado.
    # Lo buscamos en las rutas más comunes y configuramos el cmd si hace falta.
    import subprocess as _sp, os as _os
    def _find_tesseract() -> None:
        try:
            _sp.run(
                [pytesseract.pytesseract.tesseract_cmd, "--version"],
                capture_output=True, timeout=5,
            )
            return  # ya funciona
        except Exception:
            pass
        _candidates = [
            r"C:\Program Files\Tesseract-OCR\tesseract.exe",
            r"C:\Program Files (x86)\Tesseract-OCR\tesseract.exe",
            _os.path.join(_os.environ.get("LOCALAPPDATA", ""), "Tesseract-OCR", "tesseract.exe"),
            _os.path.join(_os.environ.get("USERPROFILE", ""), "AppData", "Local", "Tesseract-OCR", "tesseract.exe"),
        ]
        for _p in _candidates:
            if _p and _os.path.isfile(_p):
                pytesseract.pytesseract.tesseract_cmd = _p
                return
    _find_tesseract()
    del _find_tesseract, _sp, _os
except ImportError:
    pytesseract = None

# --------------------------------------------------------------------------- #
# Constantes
# --------------------------------------------------------------------------- #
FITZ_DPI_MATRIX = 2          # mismo que en configurar_layout_factura.py
MIN_RELIABLE_ROWS = 1        # mínimo de ROWS para considerar el resultado válido

# pdfplumber codifica caracteres no estándar como (cid:NN) — los limpiamos siempre
_CID_RE = re.compile(r"\(cid:\d+\)")

def _strip_cid(text: str) -> str:
    return _CID_RE.sub("", text).strip()

# --------------------------------------------------------------------------- #
# Apertura de imágenes
# --------------------------------------------------------------------------- #

def _open_as_pil(file_path: str, page: int = 1) -> Optional[Any]:
    """Convierte archivo a PIL Image RGB. None si falla."""
    if Image is None:
        return None
    try:
        ext = Path(file_path).suffix.lower()
        if ext in {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}:
            return Image.open(file_path).convert("RGB")
        if ext == ".pdf" and fitz is not None:
            import io
            doc = fitz.open(file_path)
            try:
                idx = max(0, min(page - 1, doc.page_count - 1))
                pix = doc.load_page(idx).get_pixmap(
                    matrix=fitz.Matrix(FITZ_DPI_MATRIX, FITZ_DPI_MATRIX), alpha=False
                )
                return Image.open(io.BytesIO(pix.tobytes("png"))).convert("RGB")
            finally:
                doc.close()
    except Exception:
        pass
    return None


# --------------------------------------------------------------------------- #
# Texto de una zona – dos caminos: pdfplumber o OCR
# --------------------------------------------------------------------------- #

def _zone_text_pdfplumber(file_path: str, page: int, zone: Dict[str, float]) -> str:
    """Extrae texto de una zona usando pdfplumber (PDFs nativos)."""
    if pdfplumber is None:
        return ""
    try:
        with pdfplumber.open(file_path) as pdf:
            if page - 1 >= len(pdf.pages):
                return ""
            p = pdf.pages[page - 1]
            pw, ph = p.width, p.height
            bbox = (
                zone["x1"] * pw,
                zone["y1"] * ph,
                zone["x2"] * pw,
                zone["y2"] * ph,
            )
            cropped = p.within_bbox(bbox)
            return _strip_cid(cropped.extract_text() or "")
    except Exception:
        return ""


def _totales_text_pdfplumber(file_path: str, page: int, zone: Dict[str, float]) -> str:
    """
    Extrae texto de la zona totales agrupando palabras por fila (Y).
    Expande el bbox hacia arriba por si las etiquetas están en la fila
    inmediatamente superior al límite configurado.
    """
    if pdfplumber is None:
        return ""
    try:
        with pdfplumber.open(file_path) as pdf:
            if page - 1 >= len(pdf.pages):
                return ""
            p = pdf.pages[page - 1]
            pw, ph = p.width, p.height
            zone_h_pt = (zone["y2"] - zone["y1"]) * ph
            # Expandir hacia arriba por 1x la altura de la zona
            bbox = (
                zone["x1"] * pw,
                max(0.0, zone["y1"] * ph - zone_h_pt),
                zone["x2"] * pw,
                zone["y2"] * ph,
            )
            words = p.within_bbox(bbox).extract_words(
                x_tolerance=5, y_tolerance=3, keep_blank_chars=False
            )
            if not words:
                return ""
            # Agrupar palabras en filas por Y
            rows_map: Dict[int, List[str]] = {}
            for w in words:
                row_key = round(w["top"] / 5) * 5  # cuantizar a 5pt
                rows_map.setdefault(row_key, []).append((w["x0"], w["text"]))
            lines = []
            for y_key in sorted(rows_map):
                row_words = sorted(rows_map[y_key], key=lambda x: x[0])
                lines.append(" ".join(t for _, t in row_words))
            return "\n".join(lines)
    except Exception:
        return ""


def _zone_text_ocr(image: Any, zone: Dict[str, float]) -> str:
    """Recorta zona de la imagen y aplica OCR."""
    if pytesseract is None or image is None:
        return ""
    try:
        w, h = image.size
        x1 = max(0, int(zone["x1"] * w))
        y1 = max(0, int(zone["y1"] * h))
        x2 = min(w, int(zone["x2"] * w))
        y2 = min(h, int(zone["y2"] * h))
        if x2 <= x1 or y2 <= y1:
            return ""
        crop = image.crop((x1, y1, x2, y2))
        if ImageOps is not None:
            crop = ImageOps.grayscale(crop)
        try:
            return pytesseract.image_to_string(crop, lang="spa")
        except Exception:
            return pytesseract.image_to_string(crop)
    except Exception:
        return ""


def _get_zone_text(
    file_path: str,
    page: int,
    zone: Dict[str, float],
    image: Optional[Any],
    *,
    combine_ocr: bool = False,
) -> str:
    """
    Intenta pdfplumber primero (PDF nativo, más preciso).
    Si retorna vacío o el archivo no es PDF, usa OCR sobre la imagen.
    Con combine_ocr=True (zona proveedor), combina ambos: útil cuando el nombre
    del proveedor es una imagen/logo que pdfplumber no puede leer.
    """
    ext = Path(file_path).suffix.lower()
    if ext == ".pdf":
        text = _zone_text_pdfplumber(file_path, page, zone)
        if text.strip() and not combine_ocr:
            return text
        ocr_text = _zone_text_ocr(image, zone)
        if combine_ocr:
            combined = "\n".join(filter(None, [ocr_text.strip(), text.strip()]))
            return combined if combined else text
        return text
    return _zone_text_ocr(image, zone)


# --------------------------------------------------------------------------- #
# --------------------------------------------------------------------------- #
# Limpieza de valores numéricos en filas de detalle
# --------------------------------------------------------------------------- #

_AMOUNT_FIELDS = {
    "Importe_Lista", "Importe_Neto", "Total", "Impuestos internos",
    "Tot.Imp.Int", "Moneda",
}
_PCT_FIELDS = {"% Dto1", "% Dto2", "IVA"}
_QTY_FIELDS = {"Cantidad", "Bl/Pq"}


def _clean_numeric(val: str) -> str:
    """Extrae el primer número decimal de una cadena, normalizando coma→punto."""
    v = val.replace(",", ".")
    m = re.search(r"\d[\d.]*", v)
    return m.group(0).rstrip(".") if m else ""


def _clean_row_values(row: Dict[str, str]) -> Dict[str, str]:
    """Limpia símbolos no numéricos en campos de importe, porcentaje y cantidad."""
    out = {}
    for campo, val in row.items():
        v = val.strip()
        if campo in _AMOUNT_FIELDS:
            v = _clean_numeric(v)
        elif campo in _PCT_FIELDS:
            # quitar % y espacios, conservar número
            v = _clean_numeric(v.replace("%", ""))
        elif campo in _QTY_FIELDS:
            # "2/" → "2", "/ 6" → "6", "12" → "12"
            parts = v.split("/")
            for part in parts:
                cleaned = _clean_numeric(part)
                if cleaned:
                    v = cleaned
                    break
            else:
                v = ""
        out[campo] = v
    return out


# --------------------------------------------------------------------------- #
# Parsers por zona
# --------------------------------------------------------------------------- #

def _only_digits(s: str) -> str:
    return re.sub(r"\D", "", s)


def _find_cuit(text: str) -> str:
    for pattern in [r"\b(\d{2}-\d{8}-\d)\b", r"\b(\d{11})\b"]:
        for m in re.findall(pattern, text):
            d = _only_digits(m)
            if len(d) == 11:
                return d
    return ""


def _find_dates(text: str) -> List[str]:
    return [
        m.replace("-", "/").replace(".", "/")
        for m in re.findall(r"\b(\d{1,2}[/\-\.]\d{1,2}[/\-\.]\d{2,4})\b", text)
    ]


def _find_cae(text: str) -> str:
    m = re.search(r"\b(\d{14})\b", text)
    return m.group(1) if m else ""


def _find_invoice_ref(text: str) -> Tuple[str, str, str]:
    """Retorna (letra, sucursal, numero)."""
    letra = suc = num = ""
    m = re.search(r"(?i)factura\s+([ABC])\b", text)
    if not m:
        m = re.search(r"\b([ABC])\s*\d{4}[\s\-]*\d{8}\b", text)
    if m:
        letra = m.group(1).upper()
    # con guión: 0013-00132269
    m2 = re.search(r"\b(\d{4})\s*-\s*(\d{8})\b", text)
    if not m2:
        # con espacio: 0013 00132269
        m2 = re.search(r"\b(\d{4})\s+(\d{8})\b", text)
    if not m2:
        # sin separador: 001300132269 (12 dígitos seguidos)
        m2 = re.search(r"\b(\d{4})(\d{8})\b", text)
    if m2:
        suc = m2.group(1)
        num = m2.group(2)
    return letra, suc, num


_COMPANY_SUFFIX_RE = re.compile(
    r"(?i)\b(s\.?\s*a\.?|s\.?\s*r\.?\s*l\.?|s\.?\s*a\.?\s*s\.?|s\.?\s*c\.?|"
    r"ltda?\.?|e\.?\s*i\.?\s*r\.?\s*l\.?|s\.?\s*a\.?\s*c\.?|sociedad|corp\.?)\b"
)
_ADDR_RE = re.compile(
    r"(?i)(calle|av\b|avda|aveni|ruta\b|bv\b|blvd|dr\.\s|nro\.?\s*\d|n°\s*\d|\bn[°º]\b)"
)


def _parse_proveedor(text: str) -> Dict[str, str]:
    lines = [l.strip() for l in text.splitlines() if l.strip()]
    out: Dict[str, str] = {
        "Nombre": "", "DOMICILIO": "", "CONDICIONIVA": "", "NUMERO_CUIT": "",
    }
    cuit = _find_cuit(text)
    if cuit:
        out["NUMERO_CUIT"] = cuit

    # Nombre: preferir línea con sufijo societario; si no, primera línea no-numérica no-dirección
    for line in lines:
        if _COMPANY_SUFFIX_RE.search(line):
            out["Nombre"] = line
            break
    if not out["Nombre"]:
        for line in lines:
            if (len(line) > 3
                    and not re.match(r"^[\d\s\-\.]+$", line)
                    and "CUIT" not in line.upper()
                    and not _ADDR_RE.search(line)
                    and not re.search(r"(?i)(factura|remito|comprobante|tel[eé])", line)):
                out["Nombre"] = line
                break

    # Dirección: buscar en TODAS las líneas
    for line in lines:
        if _ADDR_RE.search(line) and line != out["Nombre"]:
            out["DOMICILIO"] = line
            break

    for line in lines:
        if re.search(r"(?i)(responsable|monotributo|exento|consumidor)", line):
            out["CONDICIONIVA"] = line
            break
    return out


def _parse_cabecera(text: str) -> Dict[str, str]:
    out: Dict[str, str] = {
        "SUCURSAL": "", "NUMERO": "", "LETRA": "",
        "Fecha": "", "FechaSubdiario": "", "Vencimiento": "",
        "NROCAI": "", "FHVToCAI": "",
        "NUMERO_CUIT": "",
    }
    letra, suc, num = _find_invoice_ref(text)
    out["LETRA"] = letra
    out["SUCURSAL"] = suc
    out["NUMERO"] = num
    cuit = _find_cuit(text)
    if cuit:
        out["NUMERO_CUIT"] = cuit
    dates = _find_dates(text)
    if dates:
        out["Fecha"] = dates[0]
        out["FechaSubdiario"] = dates[0]
    if len(dates) >= 2:
        out["Vencimiento"] = dates[1]
    cae = _find_cae(text)
    if cae:
        out["NROCAI"] = cae
    return out


def _parse_cae_zone(text: str) -> Dict[str, str]:
    out = {"NROCAI": "", "FHVToCAI": ""}
    cae = _find_cae(text)
    if cae:
        out["NROCAI"] = cae
    dates = _find_dates(text)
    if dates:
        out["FHVToCAI"] = dates[-1]
    return out


_TOTALES_PATTERNS = [
    (r"(?i)\bneto\s+gravado\b",              "Neto gravado"),
    (r"(?i)\bneto\s+no\s+gravado\b",         "Neto no gravado"),
    (r"(?i)\bexento\b",                      "Exento"),
    (r"(?i)\biva\s*21\b",                    "IVA 21%"),
    (r"(?i)\biva\s*10[,.]5\b",               "IVA 10.5%"),
    (r"(?i)\biva\s*27\b",                    "IVA 27%"),
    (r"(?i)\bpercep\w*\.?\s+iva\b",          "Percepcion IVA"),
    (r"(?i)\bpercep\w*\.?\s+iibb\b",         "Percepcion IIBB"),
    (r"(?i)\bpercep\w*\.?\s+ganancias?\b",   "Percepcion Ganancias"),
    (r"(?i)\bpercep\w*\.?\s+rg\s*\d+",      "Percepcion IIBB"),    # PERCEPCION RG 5329
    (r"(?i)\bimp\.?\s*int\w*\b",             "Impuestos internos"), # IMP INT / IMP INTERNOS
    (r"(?i)\bimpuestos?\s+internos\b",       "Impuestos internos"),
    (r"(?i)\bing\.?\s*brutos?\b",            "Ing Brutos"),         # ING BRUTOS
    (r"(?i)\biva\b",                         "IVA"),                # IVA sin porcentaje explícito
    (r"(?i)\bsubtotal\b",                    "Subtotal"),
    (r"(?i)\btotal\s+final\b",               "Total final"),
    (r"(?i)\btotal\b",                       "Total"),
]


def _extract_decimal_amounts(text: str) -> List[str]:
    """Devuelve importes decimales en orden de aparición, limpiando artefactos PDF."""
    cleaned = _strip_cid(text or "").replace("\r", " ").replace("\n", " ")
    return re.findall(r"\d+(?:[.,]\d{2})", cleaned)


def _parse_totales(text: str) -> Dict[str, str]:
    """
    Extrae totales del texto de la zona.
    Soporta dos layouts:
      - Valor a la derecha del label (misma línea)
      - Labels en una línea, valores en la línea siguiente
    """
    out: Dict[str, str] = {}
    text = _strip_cid(text or "")
    lines = [l.strip() for l in text.splitlines() if l.strip()]

    i = 0
    while i < len(lines):
        line = lines[i]

        # Buscar todos los labels en la línea en orden de aparición.
        # Se blanquea el texto ya matcheado para evitar solapamientos.
        temp = line
        matched: List[Tuple[int, str]] = []  # (pos, key)
        for pattern, key in _TOTALES_PATTERNS:
            if key in out:
                continue
            m = re.search(pattern, temp, re.IGNORECASE)
            if m:
                matched.append((m.start(), key))
                temp = temp[:m.start()] + " " * (m.end() - m.start()) + temp[m.end():]

        if not matched:
            i += 1
            continue

        matched.sort()
        # temp ahora tiene los labels removidos; los números residuales son los valores
        inline_nums = re.findall(r"[\d.,]+", temp)

        if inline_nums:
            # Caso: label  valor  (en la misma línea)
            for idx, (_, key) in enumerate(matched):
                if idx < len(inline_nums):
                    out[key] = inline_nums[idx]
        elif i + 1 < len(lines):
            # Caso: labels arriba, valores en la línea siguiente
            next_nums = re.findall(r"[\d.,]+", lines[i + 1])
            if next_nums:
                for idx, (_, key) in enumerate(matched):
                    if idx < len(next_nums):
                        out[key] = next_nums[idx]
                i += 1  # consumir la línea de valores

        i += 1

    if out:
        return out

    # Fallback: algunos PDFs nativos dejan en la zona solo la línea de importes.
    # En ese caso usamos el orden visual más habitual del pie:
    # SUBTOTAL | PERCEPCION/IIBB | IMP INTERNOS | IVA | ING BRUTOS | TOTAL
    amounts = _extract_decimal_amounts(text)
    if len(amounts) >= 6:
        last = amounts[-6:]
        return {
            "Subtotal": last[0],
            "Percepcion IIBB": last[1],
            "Impuestos internos": last[2],
            "IVA": last[3],
            "Ing Brutos": last[4],
            "Total": last[5],
            "Total final": last[5],
        }

    return out


# --------------------------------------------------------------------------- #
# Extracción del detalle por columnas
# --------------------------------------------------------------------------- #

def _detail_rows_pdfplumber(
    file_path: str,
    page: int,
    layout: Dict[str, Any],
) -> List[Dict[str, str]]:
    """
    Extrae filas del detalle usando posiciones de texto nativo del PDF.
    Las coordenadas del layout son relativas (0-1) al tamaño de la imagen base.
    pdfplumber usa puntos (72 DPI); los convertimos via el tamaño de página.
    """
    if pdfplumber is None:
        return []
    zones = layout.get("zonas") or {}
    dz = zones.get("detalle")
    columns = layout.get("detalle_columnas") or []
    if not dz or not columns:
        return []
    try:
        with pdfplumber.open(file_path) as pdf:
            if page - 1 >= len(pdf.pages):
                return []
            p = pdf.pages[page - 1]
            pw, ph = p.width, p.height

            # Zona detalle en puntos
            bbox = (dz["x1"] * pw, dz["y1"] * ph, dz["x2"] * pw, dz["y2"] * ph)
            words = p.within_bbox(bbox).extract_words(
                x_tolerance=4, y_tolerance=4, keep_blank_chars=False
            )
            if not words:
                return []

            # Columnas en puntos (x relativo a ancho de página completa)
            col_defs = [
                {
                    "campo": c["campo"],
                    "x1": c["x1"] * pw,
                    "x2": c["x2"] * pw,
                }
                for c in columns
            ]

            return _assign_words_to_columns(words, col_defs, x_key="x0")
    except Exception:
        return []


def _detail_rows_ocr(
    image: Any,
    layout: Dict[str, Any],
    log_fn=None,
    diag: Optional[List[str]] = None,
) -> List[Dict[str, str]]:
    """
    Extrae filas del detalle usando OCR con bounding boxes de pytesseract.
    diag: lista opcional donde se acumulan mensajes de diagnóstico para observaciones.
    """
    def _log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        if diag is not None:
            diag.append(msg)

    if pytesseract is None or Image is None or image is None:
        _log("OCR: pytesseract o PIL no disponibles.")
        return []
    zones = layout.get("zonas") or {}
    dz = zones.get("detalle")
    columns = layout.get("detalle_columnas") or []
    if not dz or not columns:
        _log(f"OCR: zona detalle={'definida' if dz else 'AUSENTE'}, columnas={len(columns)}.")
        return []
    try:
        img_w, img_h = image.size

        # Recortar zona detalle
        x1 = max(0, int(dz["x1"] * img_w))
        y1 = max(0, int(dz["y1"] * img_h))
        x2 = min(img_w, int(dz["x2"] * img_w))
        y2 = min(img_h, int(dz["y2"] * img_h))
        if x2 <= x1 or y2 <= y1:
            return []
        crop = image.crop((x1, y1, x2, y2))
        if ImageOps is not None:
            crop = ImageOps.grayscale(crop)

        try:
            data = pytesseract.image_to_data(
                crop, lang="spa", output_type=pytesseract.Output.DICT
            )
        except Exception as _e_spa:
            _log(f"Layout OCR: lang=spa falló ({_e_spa}), reintentando sin idioma.")
            try:
                data = pytesseract.image_to_data(
                    crop, output_type=pytesseract.Output.DICT
                )
            except Exception as _e2:
                _log(f"Layout OCR: pytesseract.image_to_data falló: {_e2}")
                return []

        # Las columnas del layout son relativas al ancho COMPLETO de la imagen.
        # Dentro del crop, el offset horizontal es x1 (en píxeles).
        col_defs = [
            {
                "campo": c["campo"],
                # posición dentro del crop = coord_abs - offset_crop
                "x1": max(0, int(c["x1"] * img_w) - x1),
                "x2": min(x2 - x1, int(c["x2"] * img_w) - x1),
            }
            for c in columns
        ]

        n = len(data["text"])
        words = []
        for i in range(n):
            txt = str(data["text"][i]).strip()
            if not txt or int(data.get("conf", [0] * n)[i]) < 0:
                continue
            words.append({
                "text": txt,
                "x0": data["left"][i],
                "top": data["top"][i],
                "block_num": data["block_num"][i],
                "par_num":   data["par_num"][i],
                "line_num":  data["line_num"][i],
            })

        _log(f"Layout OCR detalle: {len(words)} palabras detectadas en zona detalle ({img_w}x{img_h}px).")
        if words and diag is not None:
            sample = words[:5]
            sample_str = "; ".join(f"'{w['text']}'@x{w['x0']}" for w in sample)
            col_str = "; ".join(f"{c['campo']}[{c['x1']}-{c['x2']}]" for c in col_defs)
            _log(f"Muestra palabras: {sample_str}")
            _log(f"Rangos columnas en crop: {col_str}")
        rows = _assign_words_to_columns(words, col_defs, x_key="x0")
        _log(f"Layout OCR detalle: {len(rows)} filas asignadas a columnas.")
        return rows
    except Exception as _e_outer:
        _log(f"Layout OCR detalle: error inesperado: {_e_outer}")
        return []


def _assign_words_to_columns(
    words: List[Dict],
    col_defs: List[Dict],
    *,
    x_key: str = "x0",
    y_key: str = "top",
) -> List[Dict[str, str]]:
    """
    Agrupa palabras en filas por posición vertical y las asigna a columnas
    por posición horizontal.  Descarta líneas sin ningún número.
    """
    if not words or not col_defs:
        return []

    # Agrupar en líneas (similar valor de y)
    Y_TOLERANCE = 6
    lines: List[List[Dict]] = []
    for w in sorted(words, key=lambda x: x[y_key]):
        # Limpiar secuencias CID antes de procesar; descartar palabras vacías
        clean_text = _strip_cid(str(w.get("text", "")))
        if not clean_text:
            continue
        w = dict(w)
        w["text"] = clean_text
        placed = False
        for line in lines:
            if abs(w[y_key] - line[0][y_key]) <= Y_TOLERANCE:
                line.append(w)
                placed = True
                break
        if not placed:
            lines.append([w])

    rows: List[Dict[str, str]] = []
    for line_words in lines:
        line_words.sort(key=lambda w: w[x_key])
        row: Dict[str, List[str]] = {c["campo"]: [] for c in col_defs}
        for w in line_words:
            cx = w[x_key]
            for col in col_defs:
                if col["x1"] <= cx <= col["x2"]:
                    row[col["campo"]].append(w["text"])
                    break
        built = _clean_row_values({campo: " ".join(vals).strip() for campo, vals in row.items()})
        # Solo incluir filas que tengan al menos un dígito en algún campo
        if any(re.search(r"\d", v) for v in built.values()):
            rows.append(built)

    return rows


# --------------------------------------------------------------------------- #
# Confiabilidad del resultado
# --------------------------------------------------------------------------- #

def _is_reliable(data: Optional[Dict[str, Any]]) -> bool:
    """
    Criterio mínimo: al menos MIN_RELIABLE_ROWS filas con algún dato numérico,
    y al menos uno de CUIT o número de comprobante en CAB.
    """
    if not data:
        return False
    try:
        cab = data.get("CAB") or {}
        rows = [
            r for r in (data.get("ROWS") or [])
            if isinstance(r, dict) and any(str(v).strip() for v in r.values())
        ]
        has_id = bool(cab.get("NUMERO_CUIT") or cab.get("NUMERO"))
        return len(rows) >= MIN_RELIABLE_ROWS and has_id
    except Exception:
        return False


# --------------------------------------------------------------------------- #
# Función principal
# --------------------------------------------------------------------------- #

def try_layout_extraction(
    layout_data: Dict[str, Any],
    files: List[str],
    *,
    log_fn=None,
) -> Optional[Dict[str, Any]]:
    """
    Extrae datos del comprobante usando el layout recibido, sin IA ni SQL.

    Retorna el dict normalizado {CAB, ROWS, TOTALES, meta}
    o None si algo falla o el resultado no es confiable.
    El llamador debe usar IA cuando recibe None.
    """

    def _log(msg: str) -> None:
        if callable(log_fn):
            log_fn(msg)

    if not layout_data:
        return None

    try:
        # 1 ── Layout recibido directamente (ya cargado por el llamador)
        layout = layout_data
        if not layout:
            _log("Layout: dict vacío, se usará IA.")
            return None

        zones = layout.get("zonas") or {}
        columns = layout.get("detalle_columnas") or []
        if not zones:
            _log("Layout: sin zonas definidas, se usará IA.")
            return None

        if not files:
            return None

        # ── Build (file, page) pairs ─────────────────────────────────────────
        # Multiple expanded files → each is a 1-page temp PDF, always use page 1.
        # Single multi-page PDF  → count pages with pdfplumber, iterate each.
        # Single-page file       → use the page recorded in the layout origin.
        if len(files) > 1:
            file_page_pairs: List[Tuple[str, int]] = [(f, 1) for f in files]
        elif Path(files[0]).suffix.lower() == ".pdf":
            n_pdf_pages = 1
            try:
                with pdfplumber.open(files[0]) as _tmp_pdf:
                    n_pdf_pages = max(1, len(_tmp_pdf.pages))
            except Exception:
                pass
            if n_pdf_pages > 1:
                file_page_pairs = [(files[0], pg) for pg in range(1, n_pdf_pages + 1)]
            else:
                layout_page = int((layout.get("origen") or {}).get("pagina") or 1)
                file_page_pairs = [(files[0], layout_page)]
        else:
            file_page_pairs = [(files[0], 1)]

        first_fp, first_pg = file_page_pairs[0]
        last_fp,  last_pg  = file_page_pairs[-1]

        # 2 ── Abrir imágenes (para OCR de fallback)
        _log("Layout: abriendo imágenes...")
        image_first = _open_as_pil(first_fp, first_pg)
        image_last = (
            _open_as_pil(last_fp, last_pg)
            if (last_fp, last_pg) != (first_fp, first_pg)
            else image_first
        )

        cab: Dict[str, str] = {}
        totales: Dict[str, str] = {}
        totales_raw_text = ""

        # 3a ── CAB: proveedor y cabecera desde la PRIMERA página
        for zone_name, parser in [
            ("proveedor", _parse_proveedor),
            ("cabecera",  _parse_cabecera),
        ]:
            zone_def = zones.get(zone_name)
            if not zone_def:
                continue
            _log(f"Layout: zona {zone_name} (pág {first_pg})...")
            text = _get_zone_text(
                first_fp, first_pg, zone_def, image_first,
                combine_ocr=(zone_name == "proveedor"),
            )
            parsed = parser(text)
            for k, v in parsed.items():
                if v and not cab.get(k):
                    cab[k] = v

        # 3b ── TOTALES y CAE desde la ÚLTIMA página
        n_pairs = len(file_page_pairs)
        for zone_name, parser, target in [
            ("cae",     _parse_cae_zone, cab),
            ("totales", _parse_totales,  totales),
        ]:
            zone_def = zones.get(zone_name)
            if not zone_def:
                continue
            _log(f"Layout: zona {zone_name} (pág {last_pg})...")
            if zone_name == "totales":
                text = _totales_text_pdfplumber(last_fp, last_pg, zone_def)
                if not text:
                    text = _get_zone_text(last_fp, last_pg, zone_def, image_last)
                totales_raw_text = text
            else:
                text = _get_zone_text(last_fp, last_pg, zone_def, image_last)
            parsed = parser(text)
            for k, v in parsed.items():
                if v and not target.get(k):
                    target[k] = v

        # Fallback: usar metadatos del propio layout cuando la zona no pudo extraerlos
        prov_meta = layout.get("proveedor") or {}
        if not cab.get("Nombre") and prov_meta.get("nombre"):
            cab["Nombre"] = prov_meta["nombre"]
        if not cab.get("NUMERO_CUIT") and prov_meta.get("cuit"):
            cab["NUMERO_CUIT"] = prov_meta["cuit"]

        # Asegurar FechaSubdiario
        if cab.get("Fecha") and not cab.get("FechaSubdiario"):
            cab["FechaSubdiario"] = cab["Fecha"]

        # 4 ── Extraer filas del detalle desde TODAS las páginas
        rows: List[Dict[str, str]] = []
        diag_msgs: List[str] = []
        if zones.get("detalle") and columns:
            for i, (f, pg) in enumerate(file_page_pairs):
                img = (
                    image_first if i == 0
                    else (image_last if i == n_pairs - 1
                          else _open_as_pil(f, pg))
                )
                _log(f"Layout: extrayendo detalle pág {i + 1}/{n_pairs}...")
                ext = Path(f).suffix.lower()
                page_rows: List[Dict[str, str]] = []
                if ext == ".pdf":
                    page_rows = _detail_rows_pdfplumber(f, pg, layout)
                if not page_rows:
                    ocr_diag: List[str] = []
                    page_rows = _detail_rows_ocr(img, layout, log_fn=log_fn, diag=ocr_diag)
                    if ocr_diag:
                        diag_msgs.extend(ocr_diag)
                _log(f"Layout: {len(page_rows)} fila(s) en pág {i + 1}.")
                rows.extend(page_rows)
            _log(f"Layout: {len(rows)} fila(s) total.")

        # 5 ── Armar resultado
        n_pages = n_pairs
        obs = "Extraído con layout sin IA." if n_pages == 1 else f"Extraído con layout sin IA ({n_pages} páginas)."
        # Incluir diagnóstico OCR en observaciones cuando no hay filas
        if not rows and diag_msgs:
            obs = obs + " DIAG-OCR: " + " | ".join(diag_msgs)
        result: Dict[str, Any] = {
            "CAB": cab,
            "ROWS": rows,
            "TOTALES": totales,
            "meta": {
                "comprobante_raw": "",
                "moneda_detectada": "",
                "observaciones": obs,
                "totales_raw": totales_raw_text,
                "orden_columnas": [c["campo"] for c in columns],
                "layout_usado": True,
            },
        }

        # 6 ── Anotar confiabilidad en observaciones (nunca se descarta el resultado)
        if not _is_reliable(result):
            _log("Layout: resultado insuficiente (pocas filas o sin identificadores).")
            meta = result.setdefault("meta", {})
            obs = str(meta.get("observaciones") or "").strip()
            aviso = "Resultado de layout insuficiente: pocas filas o sin CUIT/comprobante detectados."
            meta["observaciones"] = f"{obs} {aviso}".strip() if obs else aviso

        _log("Layout: extracción OK.")
        return result

    except Exception as exc:
        _log(f"Layout: error inesperado ({exc!r}), se usará IA.")
        return None
