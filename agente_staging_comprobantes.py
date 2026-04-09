#!/usr/bin/env python3
"""
Agente staging de comprobantes de compra.

Lee archivos (PDF/imágenes) de una carpeta, extrae datos con el lector IA,
busca el proveedor en Vt_Proveedores, graba en IA_Compras_CAB / IA_Compras_DET
y renombra los archivos.

Si se ejecuta sin parámetros abre una interfaz gráfica.
"""
from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import subprocess
import sys
import tempfile
import threading
import time
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

try:
    import pyodbc
except ImportError:
    pyodbc = None

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
SUPPORTED_EXTS = {".pdf", ".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}
INVALID_WIN_CHARS = '<>:"/\\|?*'
READER_MAX_INPUT_FILES = 10

VERSION          = "1.1"

DEFAULT_SERVER   = "SERVER-ALFAVB6"
DEFAULT_DATABASE = "ALFANET"
DEFAULT_USER     = "ALFANET"
DEFAULT_PASSWORD = "ALFANET"
DEFAULT_DRIVER   = "ODBC Driver 18 for SQL Server"

PRECLASSIFY_PROMPT = r"""
Vas a analizar 1 documento o 1 pagina de un documento comercial/administrativo.
Puede ser factura, remito, nota de credito, nota de debito, presupuesto, lista de precios,
extracto bancario, liquidacion de tarjeta, recibo, orden de pago u otro.

Responde SOLO JSON valido con exactamente este formato:
{
  "CAB": {
    "Proveedor": "",
    "CUIT": "",
    "Domicilio": "",
    "CondicionIVA": "",
    "TipoComprobante": "",
    "Letra": "",
    "PuntoVenta": "",
    "Numero": "",
    "Fecha": "",
    "Vencimiento": "",
    "Moneda": "",
    "CAE": "",
    "VtoCAE": "",
    "Observaciones": ""
  },
  "ROWS": [],
  "TOTALES": {
    "Neto gravado": "",
    "Neto no gravado": "",
    "Exento": "",
    "IVA 21%": "",
    "IVA 10.5%": "",
    "IVA 27%": "",
    "Otros": [],
    "Percepcion IVA": "",
    "Percepcion IIBB": "",
    "Percepcion Ganancias": "",
    "Impuestos internos": "",
    "Otros impuestos": "",
    "Total": "",
    "Total final": "",
    "Moneda": ""
  },
  "meta": {
    "comprobante_raw": "",
    "moneda_detectada": "",
    "observaciones": "",
    "totales_raw": "",
    "orden_columnas": []
  }
}

Reglas:
- "TipoComprobante" debe clasificar el documento con el valor mas util posible.
- Usar valores como: FACTURA, REMITO, NOTA DE CREDITO, NOTA DE DEBITO, PRESUPUESTO,
  LISTA DE PRECIOS, EXTRACTO BANCARIO, LIQUIDACION TARJETA, RECIBO, ORDEN DE PAGO, OTRO.
- Si es una factura o comprobante numerado, completar Letra, PuntoVenta y Numero si aparecen.
- Si el proveedor/emisor aparece claro, ponerlo en Proveedor y CUIT.
- Si una pagina no trae todos los datos, completar solo lo confiable y dejar el resto vacio.
- No inventar datos.
- ROWS debe quedar vacio en esta etapa.
"""


# ---------------------------------------------------------------------------
# Argumentos
# ---------------------------------------------------------------------------
def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Agente staging: lee, renombra y graba comprobantes en SQL."
    )
    p.add_argument("root", help="Carpeta con archivos a procesar.")
    p.add_argument("--apply",           action="store_true",
                   help="Aplica renombrado real y graba en SQL (sin esto solo simula).")
    p.add_argument("--max-files",       type=int, default=10,
                   help="Máx. archivos por grupo multipágina (default: 10).")
    p.add_argument("--limit",           type=int, default=0,
                   help="Procesa solo los primeros N grupos (0 = sin límite).")
    p.add_argument("--server",          default=DEFAULT_SERVER)
    p.add_argument("--database",        default=DEFAULT_DATABASE)
    p.add_argument("--user",            default=DEFAULT_USER)
    p.add_argument("--password",        default=DEFAULT_PASSWORD)
    p.add_argument("--driver",          default=DEFAULT_DRIVER)
    p.add_argument("--keep-accents",    action="store_true")
    p.add_argument("--log-file",        default="agente_staging_comprobantes.log")
    return p.parse_args(argv)


# ---------------------------------------------------------------------------
# Utilidades de texto / nombres
# ---------------------------------------------------------------------------
def _strip_accents(text: str) -> str:
    return "".join(
        ch for ch in unicodedata.normalize("NFKD", text)
        if not unicodedata.combining(ch)
    )


def _clean_piece(text: str, *, keep_accents: bool) -> str:
    value = (text or "").strip()
    if not value:
        return ""
    value = re.sub(r"\s+", " ", value)
    if not keep_accents:
        value = _strip_accents(value)
    value = value.upper()
    for ch in INVALID_WIN_CHARS:
        value = value.replace(ch, " ")
    value = re.sub(r"[^\w\s\-.()]", " ", value, flags=re.UNICODE)
    return re.sub(r"\s+", " ", value).strip(" ._-")


def _digits_only(value: str) -> str:
    return re.sub(r"\D+", "", value or "")


def _format_date_yyyymmdd(raw: str) -> str:
    value = (raw or "").strip()
    if not value:
        return ""
    m = re.match(r"^\s*(\d{2})[/-](\d{2})[/-](\d{4})\s*$", value)
    if m:
        dd, mm, yyyy = m.groups()
        return f"{yyyy}{mm}{dd}"
    m = re.match(r"^\s*(\d{4})[/-](\d{2})[/-](\d{2})\s*$", value)
    if m:
        yyyy, mm, dd = m.groups()
        return f"{yyyy}{mm}{dd}"
    digits = _digits_only(value)
    if len(digits) == 8:
        return digits if 1900 <= int(digits[:4]) <= 2100 else f"{digits[4:]}{digits[2:4]}{digits[:2]}"
    return ""


def _parse_date(raw: str) -> Optional[dt.datetime]:
    value = (raw or "").strip()
    if not value:
        return None
    for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y%m%d"):
        try:
            return dt.datetime.strptime(value, fmt)
        except ValueError:
            pass
    digits = _digits_only(value)
    if len(digits) == 8:
        try:
            return dt.datetime.strptime(digits, "%Y%m%d")
        except ValueError:
            try:
                return dt.datetime.strptime(digits[4:] + digits[2:4] + digits[:2], "%Y%m%d")
            except ValueError:
                pass
    return None


def _parse_money(value) -> Optional[float]:
    if value is None or str(value).strip() == "":
        return None
    s = re.sub(r"[$\s]", "", str(value))
    if not s:
        return None
    if "." in s and "," in s:
        s = s.replace(".", "").replace(",", ".") if s.rfind(",") > s.rfind(".") else s.replace(",", "")
    elif "," in s:
        parts = s.split(",")
        s = s.replace(",", ".") if len(parts) == 2 and len(parts[1]) <= 2 else s.replace(",", "")
    try:
        return float(s)
    except ValueError:
        return None


def _cab_pick(cab: dict, *keys: str) -> str:
    for k in keys:
        v = str(cab.get(k) or "").strip()
        if v:
            return v
    return ""


# ---------------------------------------------------------------------------
# Agrupación de archivos (igual que organizar_carpetas_compras)
# ---------------------------------------------------------------------------
def _natural_sort_key(value: str) -> List[object]:
    parts = re.split(r"(\d+)", value.casefold())
    return [int(p) if p.isdigit() else p for p in parts]


def _collect_supported_files(root: Path) -> List[Path]:
    files = [p for p in root.iterdir() if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS]
    return sorted(files, key=lambda p: _natural_sort_key(p.name))


def _looks_already_renamed(value: str) -> bool:
    return bool(re.compile(
        r"^.+\s-\s.+\s-\s\d{4}-\d{8}(?:-[A-Z])?(?:\s-\s\d{8})?(?:\s\(\d+\))?$",
        re.IGNORECASE,
    ).match(value.strip()))


def _group_key_from_page_pattern(stem: str) -> Optional[Tuple[str, int]]:
    for pat in [
        re.compile(r"^(?P<base>.+?)[ _-]*(?P<idx>\d+)de(?P<total>\d+)$", re.IGNORECASE),
        re.compile(r"^(?P<base>.+?)[ _-]*(?:pag|page|hoja)[ _-]*(?P<idx>\d+)(?:de(?P<total>\d+))?$", re.IGNORECASE),
    ]:
        m = pat.match(stem)
        if m:
            base = m.group("base").rstrip(" _-")
            idx = int(m.group("idx"))
            if base and idx >= 1:
                return base.casefold(), idx
    return None


def _group_key_from_trailing_index(stem: str) -> Optional[Tuple[str, int]]:
    m = re.match(r"^(?P<base>.*[A-Za-z_])(?P<idx>\d{1,2})$", stem)
    if not m:
        return None
    base = m.group("base").rstrip(" _-")
    idx = int(m.group("idx"))
    return (base.casefold(), idx) if base and idx >= 1 else None


def _group_key_from_copy_suffix(stem: str) -> str:
    """
    Normaliza nombres tipo:
    - archivo
    - archivo (2)
    - archivo (15)
    para poder tratarlos como variantes del mismo documento.
    """
    base = re.sub(r"\s*\(\d+\)\s*$", "", stem).strip()
    return base.casefold()


def _strip_copy_suffix(stem: str) -> str:
    return re.sub(r"\s*\(\d+\)\s*$", "", stem).strip()


def _parse_page_hint(stem: str) -> Tuple[str, Optional[int], bool]:
    raw = stem.strip()
    no_copy = _strip_copy_suffix(raw)
    explicit = _group_key_from_page_pattern(no_copy)
    if explicit:
        return explicit[0], explicit[1], raw != no_copy
    trailing = _group_key_from_trailing_index(no_copy)
    if trailing:
        return trailing[0], trailing[1], raw != no_copy
    return no_copy.casefold(), None, raw != no_copy


def _group_loose_files(files: List[Path], max_files: int) -> List[Dict]:
    exact_groups: Dict[str, List[Tuple[int, Path]]] = {}
    candidate_groups: Dict[str, List[Tuple[int, Path]]] = {}
    copy_groups: Dict[str, List[Path]] = {}
    singles: List[Path] = []

    for fp in files:
        if _looks_already_renamed(fp.stem):
            singles.append(fp)
            continue
        explicit = _group_key_from_page_pattern(fp.stem)
        if explicit:
            exact_groups.setdefault(explicit[0], []).append((explicit[1], fp))
            continue
        if fp.suffix.lower() == ".pdf":
            copy_groups.setdefault(_group_key_from_copy_suffix(fp.stem), []).append(fp)
            continue
        trailing = _group_key_from_trailing_index(fp.stem)
        if trailing:
            candidate_groups.setdefault(trailing[0], []).append((trailing[1], fp))
            continue
        copy_groups.setdefault(_group_key_from_copy_suffix(fp.stem), []).append(fp)

    groups: List[List[Path]] = []
    used: set = set()

    for items in exact_groups.values():
        ordered = [p for _, p in sorted(items)]
        if 1 < len(ordered) <= max_files:
            groups.append(ordered)
            used.update(ordered)
        else:
            singles.extend(ordered)

    for items in candidate_groups.values():
        ordered_items = sorted(items)
        idxs = [i for i, _ in ordered_items]
        ordered = [p for _, p in ordered_items]
        if 1 < len(ordered) <= max_files and idxs == list(range(1, len(idxs) + 1)):
            groups.append(ordered)
            used.update(ordered)
        else:
            singles.extend(ordered)

    for items in copy_groups.values():
        ordered = sorted(items, key=lambda p: _natural_sort_key(p.name))
        if len(ordered) > 1:
            for start in range(0, len(ordered), max_files):
                chunk = ordered[start:start + max_files]
                if len(chunk) > 1:
                    groups.append(chunk)
                    used.update(chunk)
                else:
                    singles.extend(chunk)
        else:
            singles.extend(ordered)

    for fp in files:
        if fp not in used and fp not in singles:
            singles.append(fp)
    for fp in singles:
        groups.append([fp])

    out = []
    for paths in groups:
        ordered = sorted(paths, key=lambda p: _natural_sort_key(p.name))
        out.append({"source_names": ", ".join(p.name for p in ordered), "files": ordered})
    return sorted(out, key=lambda g: _natural_sort_key(str(g["source_names"])))


# ---------------------------------------------------------------------------
# Lector IA
# ---------------------------------------------------------------------------
def _resolve_reader_script(base_dir: Path) -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().with_name("lector_facturas_to_json_v5.exe")
    return base_dir / "lector_facturas_to_json_v5.py"


def _load_json_from_reader(reader_script: Path, source_files: List[Path],
                           prompt_file: Optional[Path] = None) -> dict:
    with tempfile.TemporaryDirectory(prefix="staging_compras_") as tmpdir:
        if reader_script.suffix.lower() == ".exe":
            cmd = [str(reader_script), *[str(p) for p in source_files], "--outdir", tmpdir, "--auto"]
        else:
            cmd = [sys.executable, str(reader_script), *[str(p) for p in source_files], "--outdir", tmpdir, "--auto"]
        if prompt_file and prompt_file.exists():
            cmd += ["--prompt-file", str(prompt_file)]
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or f"código {proc.returncode}").strip()
            raise RuntimeError(detail)
        json_files = sorted(Path(tmpdir).glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not json_files:
            raise RuntimeError("el lector terminó OK pero no generó JSON")
        return json.loads(json_files[0].read_text(encoding="utf-8", errors="replace"))


def _chunk_paths(files: List[Path], size: int) -> List[List[Path]]:
    return [files[i:i + size] for i in range(0, len(files), size)]


def _row_signature(row: dict) -> Tuple[str, str, str, str, str]:
    return (
        str(row.get("Codigo_Articulo") or "").strip(),
        str(row.get("Descripcion") or "").strip(),
        str(row.get("Cantidad") or "").strip(),
        str(row.get("Importe_Neto") or "").strip(),
        str(row.get("Total") or "").strip(),
    )


def _merge_reader_payloads(datas: List[dict]) -> dict:
    if not datas:
        return {"CAB": {}, "ROWS": [], "TOTALES": {}, "meta": {}}

    out = {"CAB": {}, "ROWS": [], "TOTALES": {}, "meta": {}}
    seen_rows: Set[Tuple[str, str, str, str, str]] = set()

    for data in datas:
        cab = data.get("CAB") if isinstance(data, dict) else {}
        rows = data.get("ROWS") if isinstance(data, dict) else []
        tot = data.get("TOTALES") if isinstance(data, dict) else {}
        meta = data.get("meta") if isinstance(data, dict) else {}

        cab = cab if isinstance(cab, dict) else {}
        rows = rows if isinstance(rows, list) else []
        tot = tot if isinstance(tot, dict) else {}
        meta = meta if isinstance(meta, dict) else {}

        for k, v in cab.items():
            if not str(out["CAB"].get(k) or "").strip() and str(v or "").strip():
                out["CAB"][k] = v
            elif k not in out["CAB"]:
                out["CAB"][k] = v

        for row in rows:
            if not isinstance(row, dict):
                continue
            sig = _row_signature(row)
            if not any(sig) or sig in seen_rows:
                continue
            seen_rows.add(sig)
            out["ROWS"].append(row)

        for k, v in tot.items():
            if str(v or "").strip():
                out["TOTALES"][k] = v
            elif k not in out["TOTALES"]:
                out["TOTALES"][k] = v

        if isinstance(meta.get("orden_columnas"), list) and meta.get("orden_columnas") and not out["meta"].get("orden_columnas"):
            out["meta"]["orden_columnas"] = list(meta["orden_columnas"])

        for k, v in meta.items():
            if k == "orden_columnas":
                continue
            if k == "totales_raw":
                if str(v or "").strip():
                    out["meta"][k] = v
                elif k not in out["meta"]:
                    out["meta"][k] = v
                continue
            if not str(out["meta"].get(k) or "").strip() and str(v or "").strip():
                out["meta"][k] = v
            elif k not in out["meta"]:
                out["meta"][k] = v

    return out


def _build_inline_prompt_file(prompt_text: str) -> Path:
    tmp = tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".txt", delete=False)
    try:
        tmp.write(prompt_text)
        return Path(tmp.name)
    finally:
        tmp.close()


def _load_json_from_reader_merged(
    reader_script: Path,
    source_files: List[Path],
    prompt_file: Optional[Path] = None,
) -> dict:
    if len(source_files) <= READER_MAX_INPUT_FILES:
        return _load_json_from_reader(reader_script, source_files, prompt_file)

    payloads = []
    for chunk in _chunk_paths(source_files, READER_MAX_INPUT_FILES):
        payloads.append(_load_json_from_reader(reader_script, chunk, prompt_file))
    return _merge_reader_payloads(payloads)


def _preclassify_files(reader_script: Path, files: List[Path]) -> Dict[Path, dict]:
    prompt_path = _build_inline_prompt_file(PRECLASSIFY_PROMPT)
    try:
        classified: Dict[Path, dict] = {}
        total = len(files)
        for idx, fp in enumerate(files, 1):
            print(f"Prelectura {idx}/{total}: {fp.name} ...")
            try:
                classified[fp] = _load_json_from_reader_merged(reader_script, [fp], prompt_path)
            except Exception as exc:
                classified[fp] = {
                    "CAB": {"TipoComprobante": "", "Proveedor": "", "CUIT": "", "Letra": "", "PuntoVenta": "", "Numero": ""},
                    "ROWS": [],
                    "TOTALES": {},
                    "meta": {"observaciones": f"prelectura falló: {exc}"},
                }
        return classified
    finally:
        try:
            prompt_path.unlink(missing_ok=True)
        except Exception:
            pass


def _is_groupable_tipo(tipo: str) -> bool:
    upper = (tipo or "").strip().upper()
    return upper in {
        "FACTURA",
        "REMITO",
        "NOTA DE CREDITO",
        "NOTA DE DEBITO",
        "PRESUPUESTO",
        "NOTA DE PEDIDO",
        "RECIBO",
        "ORDEN DE PAGO",
    }


def _content_group_key(data: dict) -> Optional[Tuple[str, str, str, str, str]]:
    cab = data.get("CAB") if isinstance(data, dict) else {}
    cab = cab if isinstance(cab, dict) else {}
    cab_data = {
        "Cuenta_Contable": None,
        "Proveedor_CUIT": _digits_only(_cab_pick(cab, "CUIT", "NUMERO_CUIT")),
        "Proveedor_Nombre": _cab_pick(cab, "Proveedor", "Nombre"),
        "TipoComprobante": _infer_tipo_comprobante(cab, data) or None,
        "Letra": _cab_pick(cab, "Letra", "LETRA")[:1] or None,
        "PuntoVenta": _digits_only(_cab_pick(cab, "PuntoVenta", "SUCURSAL"))[:4] or None,
        "Numero": _digits_only(_cab_pick(cab, "Numero", "NUMERO"))[:8] or None,
    }
    if not _is_groupable_tipo(str(cab_data["TipoComprobante"] or "")):
        return None
    return _normalize_doc_identity(cab_data)


def _describe_preclassification(fp: Path, data: dict) -> str:
    cab = data.get("CAB") if isinstance(data, dict) else {}
    meta = data.get("meta") if isinstance(data, dict) else {}
    cab = cab if isinstance(cab, dict) else {}
    meta = meta if isinstance(meta, dict) else {}

    tipo = _infer_tipo_comprobante(cab, data) or "SIN_TIPO"
    proveedor = _cab_pick(cab, "Proveedor", "Nombre") or "SIN_PROVEEDOR"
    cuit = _digits_only(_cab_pick(cab, "CUIT", "NUMERO_CUIT"))
    letra = _cab_pick(cab, "Letra", "LETRA")[:1]
    pv = _digits_only(_cab_pick(cab, "PuntoVenta", "SUCURSAL"))[:4]
    numero = _digits_only(_cab_pick(cab, "Numero", "NUMERO"))[:8]
    obs = str(meta.get("observaciones") or "").strip()

    parts = [fp.name, f"tipo={tipo}", f"proveedor={proveedor}"]
    if cuit:
        parts.append(f"cuit={cuit}")
    if pv or numero:
        letra_txt = letra or "?"
        parts.append(f"cpte={letra_txt} {pv or '????'}-{numero or '????????'}")
    if obs:
        parts.append(f"obs={obs[:120]}")
    return " | ".join(parts)


def _group_files_by_content(
    files: List[Path],
    reader_script: Path,
    max_files: int,
) -> List[Dict]:
    if not files:
        return []

    preclassified = _preclassify_files(reader_script, files)
    grouped_by_key: Dict[Tuple[str, str, str, str, str], List[Path]] = {}
    unresolved: List[Path] = []

    for fp in files:
        print(f"  PRE: {_describe_preclassification(fp, preclassified.get(fp, {}))}")
        doc_key = _content_group_key(preclassified.get(fp, {}))
        if doc_key is None:
            unresolved.append(fp)
            continue
        grouped_by_key.setdefault(doc_key, []).append(fp)

    groups: List[Dict] = []
    for paths in grouped_by_key.values():
        ordered = sorted(paths, key=lambda p: _natural_sort_key(p.name))
        for chunk in _chunk_paths(ordered, max_files):
            groups.append({"source_names": ", ".join(p.name for p in chunk), "files": chunk})

    groups.extend(_group_loose_files(unresolved, max_files))
    return sorted(groups, key=lambda g: _natural_sort_key(str(g["source_names"])))


def _pick_primary_and_duplicates(files: List[Path]) -> Tuple[List[Path], List[Path]]:
    if len(files) <= 1:
        return list(files), []

    numbered_groups: Dict[str, Dict[int, List[Path]]] = {}
    for fp in files:
        base_key, page_idx, _is_copy = _parse_page_hint(fp.stem)
        if page_idx is None:
            continue
        numbered_groups.setdefault(base_key, {}).setdefault(page_idx, []).append(fp)

    best_base = None
    best_score = None
    for base_key, pages in numbered_groups.items():
        unique_pages = len(pages)
        non_copy = sum(
            1
            for variants in pages.values()
            for p in variants
            if not _parse_page_hint(p.stem)[2]
        )
        score = (unique_pages, non_copy)
        if unique_pages >= 2 and (best_score is None or score > best_score):
            best_base = base_key
            best_score = score

    if best_base is None:
        return list(files), []

    primary: List[Path] = []
    duplicates: List[Path] = []
    selected: Set[Path] = set()

    for page_idx in sorted(numbered_groups[best_base]):
        variants = sorted(numbered_groups[best_base][page_idx], key=lambda p: (_parse_page_hint(p.stem)[2], _natural_sort_key(p.name)))
        winner = variants[0]
        primary.append(winner)
        selected.add(winner)
        duplicates.extend(v for v in variants[1:] if v not in selected)

    for fp in files:
        if fp in selected:
            continue
        duplicates.append(fp)

    primary = sorted(primary, key=lambda p: (_parse_page_hint(p.stem)[1] or 0, _natural_sort_key(p.name)))
    dedup_dups: List[Path] = []
    seen_dup: Set[Path] = set()
    for fp in duplicates:
        if fp in seen_dup:
            continue
        seen_dup.add(fp)
        dedup_dups.append(fp)
    return primary, dedup_dups


# ---------------------------------------------------------------------------
# Nombre de archivo renombrado (igual lógica que organizar_carpetas_compras)
# ---------------------------------------------------------------------------
GENERIC_FILE_TOKENS = {"archivo","compra","comprobante","doc","documento","fact","factura",
                       "foto","image","img","pdf","rem","remito","scan","ticket"}


# Mapa de tipo comprobante → abreviación para nombre de archivo
TIPO_ABREV: Dict[str, str] = {
    "FACTURA":         "FC",
    "NOTA DE CREDITO": "NC",
    "NOTA DE DEBITO":  "ND",
    "REMITO":          "RM",
    "TICKET":          "TK",
    "PRESUPUESTO":     "PR",
    "NOTA DE PEDIDO":  "NP",
}


def _infer_tipo_comprobante(cab: dict, data: Optional[dict] = None) -> str:
    direct = _cab_pick(cab, "TipoComprobante", "CONCEPTO")
    if direct:
        return direct
    raw = ""
    if isinstance(data, dict):
        meta = data.get("meta")
        if isinstance(meta, dict):
            raw = str(meta.get("comprobante_raw") or "")
    upper = raw.upper()
    if "NOTA DE CREDITO" in upper or "N/C" in upper:
        return "NOTA DE CREDITO"
    if "NOTA DE DEBITO" in upper or "N/D" in upper:
        return "NOTA DE DEBITO"
    if "REMITO" in upper:
        return "REMITO"
    if "FACTURA" in upper:
        return "FACTURA"
    if "TICKET" in upper:
        return "TICKET"
    if "PRESUPUESTO" in upper:
        return "PRESUPUESTO"
    if "NOTA DE PEDIDO" in upper or "NP" in upper:
        return "NOTA DE PEDIDO"
    return ""


def _tipo_to_abrev(tipo: str) -> str:
    upper = tipo.strip().upper()
    for key, abrev in TIPO_ABREV.items():
        if key in upper:
            return abrev
    return "CPTE"


def _provider_from_filename(file_paths: List[Path], *, keep_accents: bool) -> str:
    if not file_paths:
        return ""
    stem = file_paths[0].stem
    cleaned = re.sub(r"\(\d+\)$", "", stem).strip()
    cleaned = re.sub(r"[_\-]+", " ", cleaned)
    cleaned = re.sub(r"\b\d+\s*(?:de|/)\s*\d+\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\b(?:pag|page|hoja)\s*\d+\b", " ", cleaned, flags=re.IGNORECASE)
    cleaned = re.sub(r"\d+", " ", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    if not cleaned:
        return ""
    normalized = _clean_piece(cleaned, keep_accents=keep_accents)
    tokens = [t for t in re.split(r"\s+", normalized.casefold()) if t]
    useful = [t for t in tokens if t not in GENERIC_FILE_TOKENS]
    if not useful or len(" ".join(useful)) < 4:
        return ""
    return _clean_piece(" ".join(useful), keep_accents=keep_accents)


def _provider_name_from_reader(cab: dict, file_paths: List[Path], *, keep_accents: bool) -> str:
    """Nombre del proveedor extraído por el lector (fallback cuando no matchea en SQL)."""
    provider = _clean_piece(_cab_pick(cab, "Proveedor", "Nombre"), keep_accents=keep_accents)
    if provider:
        return provider
    from_fn = _provider_from_filename(file_paths, keep_accents=keep_accents)
    if from_fn:
        return from_fn
    cuit = _digits_only(_cab_pick(cab, "CUIT", "NUMERO_CUIT"))
    if cuit:
        return f"CUIT {cuit}"
    return "SIN PROVEEDOR"


def _build_doc_number(cab: dict) -> str:
    pv = _digits_only(_cab_pick(cab, "PuntoVenta", "SUCURSAL"))
    number = _digits_only(_cab_pick(cab, "Numero", "NUMERO"))
    letter = _clean_piece(_cab_pick(cab, "Letra", "LETRA"), keep_accents=True)
    if pv:
        pv = pv.zfill(5)
    if number:
        number = number.zfill(8)
    # Formato: 00020-00076690A  (letra pegada al número, sin guión)
    num_letra = f"{number}{letter}" if letter else number
    return f"{pv}-{num_letra}" if pv and num_letra else (pv or num_letra)


def _build_file_base_name(
    cab: dict,
    file_paths: List[Path],
    data: Optional[dict],
    *,
    keep_accents: bool,
    razon_social_sql: Optional[str] = None,
) -> str:
    """
    Formato: ABREV PVENTA-NUMEROLETRA AAAAMMDD PROVEEDOR
    Ejemplo: FC 00020-00076690A 20260311 GO SHOP SA
    """
    tipo_raw = _infer_tipo_comprobante(cab, data)
    abrev    = _tipo_to_abrev(tipo_raw) if tipo_raw else "CPTE"
    numero   = _build_doc_number(cab)
    fecha    = _format_date_yyyymmdd(_cab_pick(cab, "Fecha", "FechaSubdiario"))

    # Nombre: SQL tiene prioridad, sino el del lector
    if razon_social_sql:
        nombre = _clean_piece(razon_social_sql, keep_accents=keep_accents)
    else:
        nombre = _provider_name_from_reader(cab, file_paths, keep_accents=keep_accents)

    return " ".join(p for p in (abrev, numero, fecha, nombre) if p)


def _ensure_unique_target(target: Path) -> Path:
    if not target.exists():
        return target
    base = target.stem
    suffix = target.suffix
    for idx in range(2, 1000):
        candidate = target.with_name(f"{base} ({idx}){suffix}")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"no se encontró nombre libre para {target}")


def _dest_folder(root: Path, fecha: Optional[dt.datetime] = None) -> Path:
    """Devuelve root/PROC_AGENTE_IA, creando la carpeta si no existe."""
    folder = root / "PROC_AGENTE_IA"
    folder.mkdir(parents=True, exist_ok=True)
    return folder


def _build_target_names(files: List[Path], base_name: str, dest_dir: Path) -> List[Path]:
    total = len(files)
    if total == 1:
        fp = files[0]
        return [_ensure_unique_target(dest_dir / f"{base_name}{fp.suffix.lower()}")]
    targets = []
    for idx, fp in enumerate(files, 1):
        suffix = f" - PAG{idx:02d}DE{total:02d}"
        targets.append(_ensure_unique_target(dest_dir / f"{base_name}{suffix}{fp.suffix.lower()}"))
    return targets


# ---------------------------------------------------------------------------
# SQL Server
# ---------------------------------------------------------------------------
def _build_conn_str(server: str, database: str, user: str, password: str, driver: str) -> str:
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={database};"
        f"UID={user};PWD={password};"
        "TrustServerCertificate=yes;"
    )


def _test_connection(conn_str: str) -> Optional[str]:
    if pyodbc is None:
        return "pyodbc no está instalado (pip install pyodbc)"
    try:
        with pyodbc.connect(conn_str, timeout=10):
            return None
    except Exception as e:
        return str(e)


def _cfg(conn_str: str, clave: str, *,
         grupo: Optional[str] = None,
         valor_filter: Optional[str] = None,
         field: str = "Valor") -> Optional[str]:
    """
    Helper equivalente a la función cfg() de VB6.
    Busca en TA_CONFIGURACION y devuelve el campo indicado (Valor por defecto).
    Con field='ValorAux' devuelve el campo ntext.
    """
    if pyodbc is None:
        return None
    conditions = ["Clave = ?"]
    params: list = [clave]
    if grupo is not None:
        conditions.append("Grupo = ?")
        params.append(grupo)
    if valor_filter is not None:
        conditions.append("Valor = ?")
        params.append(valor_filter)
    sql = f"SELECT {field} FROM TA_CONFIGURACION WHERE {' AND '.join(conditions)}"
    try:
        with pyodbc.connect(conn_str, timeout=10) as conn:
            row = conn.cursor().execute(sql, *params).fetchone()
            return str(row[0]) if row and row[0] is not None else None
    except Exception:
        return None


def _build_prompt_file(conn_str: str, cuenta_contable: str) -> Optional[Path]:
    """
    Genera Prompt_{cuenta_contable}.txt combinando el prompt base de
    IA_PROMPT_COMPRAS + la excepción del proveedor (si existe).
    Lo guarda en la ruta configurada en 'RutaDocumentosCompras'.
    Devuelve el Path del archivo o None si no hay prompt base.
    """
    prompt_base = _cfg(conn_str, "IA_PROMPT_COMPRAS", valor_filter="DEFAULT", field="ValorAux")
    if not prompt_base:
        return None

    prompt_exc = _cfg(conn_str, cuenta_contable, grupo="Compras", field="ValorAux") if cuenta_contable else None
    prompt_text = prompt_base
    if prompt_exc:
        prompt_text = prompt_base.rstrip() + "\n\n" + prompt_exc.strip()

    ruta_str = _cfg(conn_str, "RutaDocumentosCompras")
    if not ruta_str:
        return None

    dest = Path(ruta_str)
    dest.mkdir(parents=True, exist_ok=True)
    prompt_path = dest / f"Prompt_{cuenta_contable}.txt"
    prompt_path.write_text(prompt_text, encoding="utf-8")
    return prompt_path


def _lookup_provider(conn_str: str, cuit: str, nombre: str, domicilio: str) -> Tuple[Optional[str], Optional[str], str]:
    """Devuelve (CODIGO, RAZON_SOCIAL, metodo) o (None, None, '') si no encontró."""
    if pyodbc is None:
        return None, None, ""
    cuit_clean = _digits_only(cuit)
    try:
        with pyodbc.connect(conn_str, timeout=10) as conn:
            cur = conn.cursor()
            # 1. Por CUIT exacto
            if cuit_clean:
                cur.execute(
                    "SELECT TOP 1 CODIGO, RAZON_SOCIAL FROM Vt_Proveedores "
                    "WHERE REPLACE(REPLACE(ISNULL(NUMERO_DOCUMENTO,''),'-',''),' ','') = ? "
                    "AND Dada_De_Baja = 0 AND TITULO = 0",
                    cuit_clean,
                )
                row = cur.fetchone()
                if row:
                    return str(row[0]).strip(), str(row[1]).strip(), "CUIT"
            # 2. Por nombre LIKE
            nombre_clean = re.sub(r"\s+", " ", (nombre or "").strip())
            if len(nombre_clean) >= 4:
                cur.execute(
                    "SELECT TOP 1 CODIGO, RAZON_SOCIAL FROM Vt_Proveedores "
                    "WHERE RAZON_SOCIAL LIKE ? "
                    "AND Dada_De_Baja = 0 AND TITULO = 0 "
                    "ORDER BY RAZON_SOCIAL",
                    f"%{nombre_clean}%",
                )
                row = cur.fetchone()
                if row:
                    return str(row[0]).strip(), str(row[1]).strip(), "NOMBRE"
            # 3. Por domicilio / localidad
            dom_clean = re.sub(r"\s+", " ", (domicilio or "").strip())
            if len(dom_clean) >= 5:
                cur.execute(
                    "SELECT TOP 1 CODIGO, RAZON_SOCIAL FROM Vt_Proveedores "
                    "WHERE (CALLE LIKE ? OR LOCALIDAD LIKE ?) "
                    "AND Dada_De_Baja = 0 AND TITULO = 0 "
                    "ORDER BY RAZON_SOCIAL",
                    f"%{dom_clean}%", f"%{dom_clean}%",
                )
                row = cur.fetchone()
                if row:
                    return str(row[0]).strip(), str(row[1]).strip(), "DOMICILIO"
    except Exception as e:
        print(f"  [WARN] lookup proveedor falló: {e}")
    return None, None, ""


def _already_in_staging(conn_str: str, file_names: List[str]) -> Optional[str]:
    """
    Devuelve Archivo_NombreRenombrado si alguno de los archivos del grupo ya está
    en staging con estado distinto a ERROR_LECTURA. Devuelve None si no está.
    """
    if pyodbc is None:
        return None
    try:
        with pyodbc.connect(conn_str, timeout=10) as conn:
            cur = conn.cursor()
            for name in file_names:
                cur.execute(
                    "SELECT TOP 1 Archivo_NombreRenombrado FROM IA_Compras_CAB "
                    "WHERE Estado <> 'ERROR_LECTURA' AND ("
                    "  Archivo_NombreOriginal = ? OR "
                    "  Archivo_NombreOriginal LIKE ? OR "
                    "  Archivo_NombreOriginal LIKE ? OR "
                    "  Archivo_NombreOriginal LIKE ?"
                    ")",
                    name,
                    f"{name}, %",
                    f"%, {name}, %",
                    f"%, {name}",
                )
                row = cur.fetchone()
                if row:
                    return str(row[0]).strip() if row[0] else ""
    except Exception:
        pass
    return None


def _normalize_doc_identity(cab_data: dict) -> Optional[Tuple[str, str, str, str, str]]:
    tipo = str(cab_data.get("TipoComprobante") or "").strip().upper()
    letra = str(cab_data.get("Letra") or "").strip().upper()
    pv = _digits_only(str(cab_data.get("PuntoVenta") or "").strip())
    numero = _digits_only(str(cab_data.get("Numero") or "").strip())
    if not (tipo and pv and numero):
        return None

    supplier = (
        str(cab_data.get("Cuenta_Contable") or "").strip().upper()
        or _digits_only(str(cab_data.get("Proveedor_CUIT") or "").strip())
        or re.sub(r"\s+", " ", str(cab_data.get("Proveedor_Nombre") or "").strip()).upper()
    )
    if not supplier:
        return None
    return supplier, tipo, letra, pv, numero


def _doc_identity_text(doc_key: Tuple[str, str, str, str, str]) -> str:
    supplier, tipo, letra, pv, numero = doc_key
    letra_txt = f" {letra}" if letra else ""
    return f"{supplier} | {tipo}{letra_txt} {pv}-{numero}"


def _find_duplicate_by_identity(conn_str: str, cab_data: dict) -> Optional[str]:
    """
    Busca un comprobante ya presente en staging por identidad de negocio:
    proveedor + tipo + letra + punto de venta + número.
    """
    if pyodbc is None:
        return None

    doc_key = _normalize_doc_identity(cab_data)
    if doc_key is None:
        return None

    supplier, tipo, letra, pv, numero = doc_key
    where_supplier = ""
    params: List[str] = [tipo, letra, pv, numero]

    if str(cab_data.get("Cuenta_Contable") or "").strip():
        where_supplier = "Cuenta_Contable = ?"
        params.append(str(cab_data["Cuenta_Contable"]).strip())
    elif _digits_only(str(cab_data.get("Proveedor_CUIT") or "").strip()):
        where_supplier = "Proveedor_CUIT = ?"
        params.append(_digits_only(str(cab_data["Proveedor_CUIT"]).strip()))
    else:
        where_supplier = "UPPER(LTRIM(RTRIM(ISNULL(Proveedor_Nombre, '')))) = ?"
        params.append(re.sub(r"\s+", " ", str(cab_data.get("Proveedor_Nombre") or "").strip()).upper())

    sql = (
        "SELECT TOP 1 Archivo_NombreRenombrado "
        "FROM IA_Compras_CAB "
        "WHERE Estado <> 'ERROR_LECTURA' "
        "  AND UPPER(LTRIM(RTRIM(ISNULL(TipoComprobante, '')))) = ? "
        "  AND UPPER(LTRIM(RTRIM(ISNULL(Letra, '')))) = ? "
        "  AND ISNULL(PuntoVenta, '') = ? "
        "  AND ISNULL(Numero, '') = ? "
        f"  AND {where_supplier} "
        "ORDER BY ID DESC"
    )
    try:
        with pyodbc.connect(conn_str, timeout=10) as conn:
            cur = conn.cursor()
            cur.execute(sql, *params)
            row = cur.fetchone()
            if row:
                return str(row[0]).strip() if row[0] else ""
    except Exception:
        pass
    return None


def _move_group_to_processed(files: List[Path], registered_name: Optional[str]) -> List[str]:
    dest_dir = _dest_folder(files[0].parent)
    moved: List[str] = []
    reg_parts = [p.strip() for p in (registered_name or "").split(",") if p.strip()]

    for fp in files:
        if not fp.exists():
            continue
        tgt_name = None
        if reg_parts:
            tgt_name = next(
                (p for p in reg_parts if Path(p).suffix.lower() == fp.suffix.lower()),
                None,
            )
        tgt = dest_dir / (tgt_name if tgt_name else fp.name)
        tgt = _ensure_unique_target(tgt)
        try:
            fp.rename(tgt)
            moved.append(tgt.name)
        except Exception as e:
            print(f"  AVISO: no se pudo mover {fp.name}: {e}")
    return moved


def _move_files_to_duplicates(files: List[Path]) -> List[str]:
    if not files:
        return []
    dest_dir = _dest_folder(files[0].parent) / "DUPLICADOS"
    dest_dir.mkdir(parents=True, exist_ok=True)
    moved: List[str] = []
    for fp in files:
        if not fp.exists():
            continue
        tgt = _ensure_unique_target(dest_dir / fp.name)
        try:
            fp.rename(tgt)
            moved.append(tgt.name)
        except Exception as e:
            print(f"  AVISO: no se pudo mover duplicado {fp.name}: {e}")
    return moved


def _insert_staging(conn_str: str, cab_data: dict, det_rows: List[dict]) -> int:
    """Inserta CAB + DET. Devuelve el ID generado."""
    with pyodbc.connect(conn_str, timeout=15) as conn:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO IA_Compras_CAB (
                Estado, FechaHora_Proceso,
                Archivo_RutaOriginal, Archivo_NombreOriginal, Archivo_NombreRenombrado,
                Proveedor_Nombre, Proveedor_CUIT, Proveedor_Domicilio, Proveedor_CondIVA,
                Cuenta_Contable, Match_Metodo,
                TipoComprobante, Letra, PuntoVenta, Numero, Fecha, Vencimiento,
                CAE, VtoCAE, Moneda,
                NetoGravado, NetoNoGravado, Exento,
                IVA_21, IVA_105, IVA_27,
                Percepcion_IVA, Percepcion_IIBB, Percepcion_Ganancias,
                ImpuestosInternos, OtrosImpuestos, Total,
                Lector_Observaciones, Lector_Error
            )
            OUTPUT INSERTED.ID
            VALUES (?,GETDATE(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            cab_data["Estado"],
            cab_data["Archivo_RutaOriginal"],
            cab_data["Archivo_NombreOriginal"],
            cab_data["Archivo_NombreRenombrado"],
            cab_data["Proveedor_Nombre"],
            cab_data["Proveedor_CUIT"],
            cab_data["Proveedor_Domicilio"],
            cab_data["Proveedor_CondIVA"],
            cab_data["Cuenta_Contable"],
            cab_data["Match_Metodo"],
            cab_data["TipoComprobante"],
            cab_data["Letra"],
            cab_data["PuntoVenta"],
            cab_data["Numero"],
            cab_data["Fecha"],
            cab_data["Vencimiento"],
            cab_data["CAE"],
            cab_data["VtoCAE"],
            cab_data["Moneda"],
            cab_data["NetoGravado"],
            cab_data["NetoNoGravado"],
            cab_data["Exento"],
            cab_data["IVA_21"],
            cab_data["IVA_105"],
            cab_data["IVA_27"],
            cab_data["Percepcion_IVA"],
            cab_data["Percepcion_IIBB"],
            cab_data["Percepcion_Ganancias"],
            cab_data["ImpuestosInternos"],
            cab_data["OtrosImpuestos"],
            cab_data["Total"],
            cab_data["Lector_Observaciones"],
            cab_data["Lector_Error"],
        )
        id_cab = int(cur.fetchone()[0])

        for idx, row in enumerate(det_rows, 1):
            cur.execute(
                """
                INSERT INTO IA_Compras_DET (
                    ID_CAB, NroRenglon,
                    Cantidad, Codigo_Articulo, Descripcion, UD,
                    Importe_Lista, Dto1, Dto2, Importe_Neto, IVA,
                    ImpuestosInternos, Total, AuxNroLote, AuxNroSerie
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                id_cab, idx,
                str(row.get("Cantidad") or "")[:20] or None,
                str(row.get("Codigo_Articulo") or "")[:50] or None,
                str(row.get("Descripcion") or "")[:200] or None,
                str(row.get("UD") or "")[:10] or None,
                _parse_money(row.get("Importe_Lista")),
                _parse_money(row.get("% Dto1")),
                _parse_money(row.get("% Dto2")),
                _parse_money(row.get("Importe_Neto")),
                _parse_money(row.get("IVA")),
                _parse_money(row.get("Impuestos internos")),
                _parse_money(row.get("Total")),
                str(row.get("AuxNroLote") or "")[:50] or None,
                str(row.get("AuxNroSerie") or "")[:50] or None,
            )
        conn.commit()
    return id_cab


# ---------------------------------------------------------------------------
# Log
# ---------------------------------------------------------------------------
def _resolve_log_path(root: Path, log_file_name: str) -> Path:
    month_prefix = dt.datetime.now().strftime("%Y%m")
    clean_name = Path(log_file_name).name or "agente_staging_comprobantes.log"
    if not clean_name.lower().endswith(".log"):
        clean_name += ".log"
    return root / "LOG" / f"{month_prefix}_{clean_name}"


def _append_log(log_path: Path, *, status: str, old_name: str, new_name: str, detail: str) -> None:
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp}\t{status}\t{old_name}\t{new_name}\t{detail}\n"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8", errors="replace") as fh:
        fh.write(line)


# ---------------------------------------------------------------------------
# Procesamiento de un grupo
# ---------------------------------------------------------------------------
def _process_group(
    files: List[Path],
    source_names: str,
    reader_script: Path,
    conn_str: str,
    *,
    apply: bool,
    keep_accents: bool,
    max_files: int,
    seen_doc_keys: Optional[Set[Tuple[str, str, str, str, str]]] = None,
) -> Tuple[str, str, str]:
    """Devuelve (status, new_name, detail)."""

    primary_files, duplicate_files = _pick_primary_and_duplicates(files)
    if duplicate_files:
        print(
            "  Duplicados detectados: "
            f"usar={', '.join(p.name for p in primary_files)} | "
            f"duplicados={', '.join(p.name for p in duplicate_files)}"
        )
    files = primary_files
    source_names = ", ".join(f.name for f in files)

    # Ya está en staging y no es error → skip y mover si siguen en origen
    if apply:
        registered_name = _already_in_staging(conn_str, [f.name for f in files + duplicate_files])
        if registered_name is not None:
            moved = _move_group_to_processed(files, registered_name)
            dup_moved = _move_files_to_duplicates(duplicate_files)
            detail = "ya existe en staging"
            if moved:
                detail += f" | movido a PROC_AGENTE_IA: {', '.join(moved)}"
            if dup_moved:
                detail += f" | duplicados→DUPLICADOS: {', '.join(dup_moved)}"
            return "SKIP", source_names, detail

    def _do_read(prompt_file: Optional[Path] = None) -> dict:
        data = _load_json_from_reader_merged(reader_script, files, prompt_file)
        cab_ = data.get("CAB") if isinstance(data, dict) else None
        tot_ = data.get("TOTALES") if isinstance(data, dict) else {}
        rows_ = data.get("ROWS") if isinstance(data, dict) else []
        meta_ = data.get("meta") if isinstance(data, dict) else {}
        return (
            data,
            cab_ if isinstance(cab_, dict) else {},
            tot_ if isinstance(tot_, dict) else {},
            rows_ if isinstance(rows_, list) else [],
            meta_ if isinstance(meta_, dict) else {},
        )

    # Primera lectura con prompt base (DEFAULT)
    default_prompt = _build_prompt_file(conn_str, "DEFAULT") if conn_str else None
    print(f"  Leyendo: {source_names} ...")
    try:
        data, cab, tot, rows, meta = _do_read(default_prompt)
    except Exception as exc:
        cab_data = _build_cab_data_error(files, str(exc))
        if apply:
            try:
                _insert_staging(conn_str, cab_data, [])
            except Exception as e2:
                return "ERROR", source_names, f"lector falló: {exc} | SQL falló: {e2}"
        return "ERROR", source_names, f"lector: {exc}"

    # Lookup proveedor
    cuit    = _digits_only(_cab_pick(cab, "CUIT", "NUMERO_CUIT"))
    nombre  = _cab_pick(cab, "Proveedor", "Nombre")
    domicil = _cab_pick(cab, "Domicilio")
    print(f"  Buscando proveedor: {nombre or cuit or '?'} ...")
    cuenta_contable, razon_social_sql, match_metodo = _lookup_provider(conn_str, cuit, nombre, domicil)

    # Si el proveedor tiene excepción de prompt, re-leer con prompt combinado
    if cuenta_contable:
        exc_text = _cfg(conn_str, cuenta_contable, grupo="Compras", field="ValorAux")
        if exc_text:
            print(f"  Re-leyendo con prompt específico del proveedor ...")
            combined_prompt = _build_prompt_file(conn_str, cuenta_contable)
            try:
                data, cab, tot, rows, meta = _do_read(combined_prompt)
            except Exception as exc:
                print(f"  AVISO: re-lectura con prompt específico falló, usando resultado anterior. {exc}")

    estado = "PENDIENTE"
    if not cuenta_contable:
        estado = "SIN_PROVEEDOR"

    # Nombre de archivo: SQL si matcheó, sino el del lector
    base_name = _build_file_base_name(
        cab, files, data,
        keep_accents=keep_accents,
        razon_social_sql=razon_social_sql,
    )
    fecha_cpte = _parse_date(_cab_pick(cab, "Fecha", "FechaSubdiario"))
    dest_dir = _dest_folder(files[0].parent, fecha_cpte)
    targets = _build_target_names(files, base_name, dest_dir) if base_name else []
    new_name = ", ".join(t.name for t in targets) if targets else source_names

    # Armar dict CAB para SQL
    cab_data = {
        "Estado":                 estado,
        "Archivo_RutaOriginal":   str(files[0].parent),
        "Archivo_NombreOriginal": ", ".join(f.name for f in files),
        "Archivo_NombreRenombrado": new_name if base_name else None,
        "Proveedor_Nombre":       nombre or None,
        "Proveedor_CUIT":         cuit or None,
        "Proveedor_Domicilio":    domicil or None,
        "Proveedor_CondIVA":      _cab_pick(cab, "CondicionIVA") or None,
        "Cuenta_Contable":        cuenta_contable,
        "Match_Metodo":           match_metodo or None,
        "TipoComprobante":        _infer_tipo_comprobante(cab, data) or None,
        "Letra":                  _cab_pick(cab, "Letra", "LETRA")[:1] or None,
        "PuntoVenta":             _digits_only(_cab_pick(cab, "PuntoVenta", "SUCURSAL"))[:4] or None,
        "Numero":                 _digits_only(_cab_pick(cab, "Numero", "NUMERO"))[:8] or None,
        "Fecha":                  _parse_date(_cab_pick(cab, "Fecha")),
        "Vencimiento":            _parse_date(_cab_pick(cab, "Vencimiento")),
        "CAE":                    _cab_pick(cab, "CAE")[:14] or None,
        "VtoCAE":                 _parse_date(_cab_pick(cab, "VtoCAE")),
        "Moneda":                 _cab_pick(cab, "Moneda")[:4] or None,
        "NetoGravado":            _parse_money(tot.get("Neto gravado")),
        "NetoNoGravado":          _parse_money(tot.get("Neto no gravado")),
        "Exento":                 _parse_money(tot.get("Exento")),
        "IVA_21":                 _parse_money(tot.get("IVA 21%")),
        "IVA_105":                _parse_money(tot.get("IVA 10.5%")),
        "IVA_27":                 _parse_money(tot.get("IVA 27%")),
        "Percepcion_IVA":         _parse_money(tot.get("Percepcion IVA")),
        "Percepcion_IIBB":        _parse_money(tot.get("Percepcion IIBB")),
        "Percepcion_Ganancias":   _parse_money(tot.get("Percepcion Ganancias")),
        "ImpuestosInternos":      _parse_money(tot.get("Impuestos internos")),
        "OtrosImpuestos":         _parse_money(tot.get("Otros impuestos")),
        "Total":                  _parse_money(tot.get("Total final") or tot.get("Total")),
        "Lector_Observaciones":   str(meta.get("observaciones") or "")[:500] or None,
        "Lector_Error":           None,
    }

    detail_parts = [f"proveedor={nombre or '?'}", f"match={match_metodo or 'NINGUNO'}"]
    if base_name:
        detail_parts.append(f"renombrar→{new_name}")
    if duplicate_files:
        detail_parts.append(f"duplicados={len(duplicate_files)}")

    doc_key = _normalize_doc_identity(cab_data)
    if doc_key and seen_doc_keys is not None and doc_key in seen_doc_keys:
        return "SKIP", new_name, f"duplicado en este lote por clave { _doc_identity_text(doc_key) }"

    registered_name = _find_duplicate_by_identity(conn_str, cab_data) if conn_str else None
    if registered_name is not None:
        if apply:
            moved = _move_group_to_processed(files, registered_name)
            dup_moved = _move_files_to_duplicates(duplicate_files)
            detail = f"ya existe en staging por clave { _doc_identity_text(doc_key) if doc_key else 'documento' }"
            if moved:
                detail += f" | movido a PROC_AGENTE_IA: {', '.join(moved)}"
            if dup_moved:
                detail += f" | duplicados→DUPLICADOS: {', '.join(dup_moved)}"
            return "SKIP", new_name, detail
        return "SKIP", new_name, f"ya existe en staging por clave { _doc_identity_text(doc_key) if doc_key else 'documento' }"

    if apply:
        # Grabar en SQL
        print(f"  Grabando en SQL ...")
        try:
            _insert_staging(conn_str, cab_data, rows)
        except Exception as e:
            return "ERROR", source_names, f"SQL insert falló: {e}"
        # Renombrar archivos
        if base_name and targets:
            print(f"  Moviendo archivos a {targets[0].parent} ...")
            try:
                for src, tgt in zip(files, targets):
                    src.rename(tgt)
            except Exception as e:
                return "ERROR", new_name, f"grabado en SQL pero renombrado falló: {e}"
        dup_moved = _move_files_to_duplicates(duplicate_files)
        if dup_moved:
            detail_parts.append(f"duplicados→DUPLICADOS: {', '.join(dup_moved)}")
        # Guardar JSON del lector con el mismo nombre base en la carpeta de procesados
        if base_name and dest_dir:
            try:
                json_path = dest_dir / f"{base_name}.json"
                json_path.write_text(
                    json.dumps(data, ensure_ascii=False, indent=2),
                    encoding="utf-8",
                )
            except Exception as e:
                print(f"  AVISO: no se pudo guardar JSON: {e}")
    elif doc_key and seen_doc_keys is not None:
        seen_doc_keys.add(doc_key)

    status = "OK" if estado == "PENDIENTE" else estado
    return status, new_name, " | ".join(detail_parts)


def _build_cab_data_error(files: List[Path], error_msg: str) -> dict:
    return {
        "Estado": "ERROR_LECTURA",
        "Archivo_RutaOriginal":   str(files[0].parent),
        "Archivo_NombreOriginal": ", ".join(f.name for f in files),
        "Archivo_NombreRenombrado": None,
        "Proveedor_Nombre": None, "Proveedor_CUIT": None,
        "Proveedor_Domicilio": None, "Proveedor_CondIVA": None,
        "Cuenta_Contable": None, "Match_Metodo": None,
        "TipoComprobante": None, "Letra": None, "PuntoVenta": None,
        "Numero": None, "Fecha": None, "Vencimiento": None,
        "CAE": None, "VtoCAE": None, "Moneda": None,
        "NetoGravado": None, "NetoNoGravado": None, "Exento": None,
        "IVA_21": None, "IVA_105": None, "IVA_27": None,
        "Percepcion_IVA": None, "Percepcion_IIBB": None, "Percepcion_Ganancias": None,
        "ImpuestosInternos": None, "OtrosImpuestos": None, "Total": None,
        "Lector_Observaciones": None,
        "Lector_Error": error_msg[:500],
    }


# ---------------------------------------------------------------------------
# GUI config persistence
# ---------------------------------------------------------------------------
_GUI_CONFIG_FILE = Path(__file__).with_suffix(".gui.json")

def _load_gui_config() -> dict:
    try:
        import json
        if _GUI_CONFIG_FILE.exists():
            return json.loads(_GUI_CONFIG_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _save_gui_config(data: dict) -> None:
    try:
        import json
        _GUI_CONFIG_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass


# ---------------------------------------------------------------------------
# GUI
# ---------------------------------------------------------------------------
def _run_gui() -> int:
    try:
        import tkinter as tk
        from tkinter import ttk, filedialog, scrolledtext
        import tkinter.messagebox as mb
    except ImportError:
        print("ERROR: tkinter no disponible.")
        return 1

    _cfg = _load_gui_config()

    root_win = tk.Tk()
    root_win.title(f"Agente Staging Comprobantes  v{VERSION}")
    root_win.minsize(680, 600)

    var_root      = tk.StringVar(value=_cfg.get("last_folder", ""))
    var_apply     = tk.BooleanVar(value=False)
    var_max_files = tk.IntVar(value=10)
    var_limit     = tk.IntVar(value=0)
    var_keep_acc  = tk.BooleanVar(value=False)
    var_server    = tk.StringVar(value=DEFAULT_SERVER)
    var_database  = tk.StringVar(value=DEFAULT_DATABASE)
    var_user      = tk.StringVar(value=DEFAULT_USER)
    var_password  = tk.StringVar(value=DEFAULT_PASSWORD)

    # --- Carpeta ---
    folder_frame = ttk.LabelFrame(root_win, text="Carpeta de documentos", padding=8)
    folder_frame.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 4))
    folder_frame.columnconfigure(1, weight=1)
    ttk.Label(folder_frame, text="Carpeta:").grid(row=0, column=0, sticky="w")
    ttk.Entry(folder_frame, textvariable=var_root, width=55).grid(row=0, column=1, sticky="ew", padx=(5, 2))
    ttk.Button(folder_frame, text="Examinar...",
               command=lambda: var_root.set(filedialog.askdirectory(title="Seleccionar carpeta") or var_root.get())
               ).grid(row=0, column=2)

    # --- Conexión SQL ---
    sql_frame = ttk.LabelFrame(root_win, text="Conexión SQL Server", padding=8)
    sql_frame.grid(row=1, column=0, sticky="ew", padx=10, pady=4)
    sql_frame.columnconfigure(1, weight=1)
    sql_frame.columnconfigure(3, weight=1)
    for col, (lbl, var, show) in enumerate([
        ("Servidor:", var_server, ""),
        ("Base:",     var_database, ""),
    ]):
        ttk.Label(sql_frame, text=lbl).grid(row=0, column=col*2, sticky="w", padx=(0 if col==0 else 10, 0))
        ttk.Entry(sql_frame, textvariable=var, show=show, width=25).grid(row=0, column=col*2+1, sticky="ew", padx=(4, 0))
    for col, (lbl, var, show) in enumerate([
        ("Usuario:", var_user, ""),
        ("Contraseña:", var_password, "*"),
    ]):
        ttk.Label(sql_frame, text=lbl).grid(row=1, column=col*2, sticky="w", padx=(0 if col==0 else 10, 0), pady=(4,0))
        ttk.Entry(sql_frame, textvariable=var, show=show, width=25).grid(row=1, column=col*2+1, sticky="ew", padx=(4, 0), pady=(4,0))

    def _test_conn():
        conn_str = _build_conn_str(var_server.get(), var_database.get(), var_user.get(), var_password.get(), DEFAULT_DRIVER)
        err = _test_connection(conn_str)
        if err:
            mb.showerror("Error de conexión", err)
        else:
            mb.showinfo("Conexión OK", f"Conectado a {var_server.get()} / {var_database.get()}")
    ttk.Button(sql_frame, text="Probar conexión", command=_test_conn).grid(row=1, column=4, padx=(10, 0), pady=(4,0))

    # --- Opciones ---
    opt_frame = ttk.LabelFrame(root_win, text="Opciones", padding=8)
    opt_frame.grid(row=2, column=0, sticky="ew", padx=10, pady=4)
    ttk.Checkbutton(opt_frame, text="Aplicar cambios reales  (renombra archivos y graba en SQL)", variable=var_apply).grid(row=0, column=0, columnspan=4, sticky="w")
    ttk.Checkbutton(opt_frame, text="Conservar acentos", variable=var_keep_acc).grid(row=1, column=0, sticky="w", pady=(4,0))
    ttk.Label(opt_frame, text="Máx. archivos/grupo:").grid(row=1, column=1, sticky="w", padx=(20, 0), pady=(4,0))
    ttk.Spinbox(opt_frame, textvariable=var_max_files, from_=1, to=50, width=5).grid(row=1, column=2, pady=(4,0))
    ttk.Label(opt_frame, text="Límite grupos (0=todos):").grid(row=1, column=3, sticky="w", padx=(10, 0), pady=(4,0))
    ttk.Spinbox(opt_frame, textvariable=var_limit, from_=0, to=10000, width=7).grid(row=1, column=4, pady=(4,0))

    # --- Progreso ---
    prog_frame = ttk.Frame(root_win, padding=(10, 0, 10, 0))
    prog_frame.grid(row=3, column=0, sticky="ew")
    prog_frame.columnconfigure(0, weight=1)

    var_prog_text = tk.StringVar(value="")
    ttk.Label(prog_frame, textvariable=var_prog_text, anchor="w").grid(row=0, column=0, sticky="ew")
    prog_bar = ttk.Progressbar(prog_frame, mode="determinate", maximum=100)
    prog_bar.grid(row=1, column=0, sticky="ew", pady=(2, 4))

    def _update_progress(done: int, total: int, elapsed: float) -> None:
        prog_bar["maximum"] = total
        prog_bar["value"] = done
        remaining_str = ""
        if done > 0 and done < total:
            avg = elapsed / done
            secs_left = int(avg * (total - done))
            h, rem = divmod(secs_left, 3600)
            m, s = divmod(rem, 60)
            if h > 0:
                remaining_str = f"  |  Tiempo restante: {h}:{m:02d}:{s:02d}"
            else:
                remaining_str = f"  |  Tiempo restante: {m:02d}:{s:02d}"
        elif done == total and total > 0:
            remaining_str = "  |  Completado"
        var_prog_text.set(
            f"Procesados: {done} / {total}   Restantes: {total - done}{remaining_str}"
        )

    def _reset_progress() -> None:
        prog_bar["value"] = 0
        var_prog_text.set("")

    # --- Salida ---
    out_frame = ttk.LabelFrame(root_win, text="Salida", padding=5)
    out_frame.grid(row=4, column=0, sticky="nsew", padx=10, pady=4)
    txt = scrolledtext.ScrolledText(out_frame, width=85, height=16, state="disabled", wrap="word")
    txt.pack(fill="both", expand=True)

    # --- Botones ---
    btn_frame = ttk.Frame(root_win, padding=(10, 0, 10, 10))
    btn_frame.grid(row=5, column=0, sticky="ew")
    result_code = [0]

    def _append(text: str) -> None:
        txt.configure(state="normal")
        txt.insert("end", text)
        txt.see("end")
        txt.configure(state="disabled")

    _PROGRESS_PREFIX = "\x00PROGRESS:"

    class _GuiStream:
        def write(self, s: str) -> None:
            if not s:
                return
            if s.startswith(_PROGRESS_PREFIX):
                try:
                    rest = s[len(_PROGRESS_PREFIX):]
                    frac, elapsed_str = rest.rsplit(":", 1)
                    done, total = map(int, frac.split("/"))
                    elapsed = float(elapsed_str)
                    root_win.after(0, _update_progress, done, total, elapsed)
                except Exception:
                    pass
                return
            root_win.after(0, _append, s)
        def flush(self) -> None:
            pass

    def _run() -> None:
        root_val = var_root.get().strip()
        if not root_val:
            mb.showerror("Error", "Seleccioná la carpeta de documentos.")
            return
        argv = [
            root_val,
            "--max-files", str(var_max_files.get()),
            "--limit",     str(var_limit.get()),
            "--server",    var_server.get().strip(),
            "--database",  var_database.get().strip(),
            "--user",      var_user.get().strip(),
            "--password",  var_password.get().strip(),
        ]
        if var_apply.get():
            argv.append("--apply")
        if var_keep_acc.get():
            argv.append("--keep-accents")

        _save_gui_config({"last_folder": root_val})

        btn_run.configure(state="disabled")
        _reset_progress()
        txt.configure(state="normal")
        txt.delete("1.0", "end")
        txt.configure(state="disabled")

        def _worker() -> None:
            old_stdout = sys.stdout
            sys.stdout = _GuiStream()
            try:
                code = main(argv)
                result_code[0] = code
            except Exception as exc:
                sys.stdout = old_stdout
                root_win.after(0, _append, f"\nERROR inesperado: {exc}\n")
                root_win.after(0, lambda: btn_run.configure(state="normal"))
                return
            finally:
                sys.stdout = old_stdout
            label = "FINALIZADO" if code == 0 else f"FINALIZADO CON ADVERTENCIAS (código {code})"
            root_win.after(0, _append, f"\n--- {label} ---\n")
            root_win.after(0, lambda: btn_run.configure(state="normal"))

        threading.Thread(target=_worker, daemon=True).start()

    btn_run = ttk.Button(btn_frame, text="Ejecutar", command=_run)
    btn_run.pack(side="left", padx=(0, 5))
    ttk.Button(btn_frame, text="Cerrar", command=root_win.destroy).pack(side="left")

    root_win.columnconfigure(0, weight=1)
    root_win.rowconfigure(4, weight=1)
    root_win.mainloop()
    return result_code[0]


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main(argv: Optional[List[str]] = None) -> int:
    if argv is None and not sys.argv[1:]:
        return _run_gui()

    args = _parse_args(argv)
    root = Path(args.root).expanduser().resolve()
    base_dir = Path(__file__).resolve().parent if not getattr(sys, "frozen", False) else Path(sys.executable).resolve().parent
    reader_script = _resolve_reader_script(base_dir)
    log_path = _resolve_log_path(root, args.log_file)

    if not root.exists() or not root.is_dir():
        print(f"ERROR: no existe la carpeta: {root}")
        return 1
    if not reader_script.exists():
        print(f"ERROR: no se encontró el lector: {reader_script}")
        return 1

    conn_str = _build_conn_str(args.server, args.database, args.user, args.password, args.driver)

    print(f"Agente Staging Comprobantes  v{VERSION}")
    print()

    if args.apply:
        err = _test_connection(conn_str)
        if err:
            print(f"ERROR: no se puede conectar a SQL Server.\n{err}")
            return 1
        print(f"SQL Server: {args.server} / {args.database} — conexión OK")
    else:
        print(f"[SIMULACIÓN] SQL Server: {args.server} / {args.database} (no se grabará ni renombrará)")

    loose_files = _collect_supported_files(root)
    if not loose_files:
        print("No se encontraron archivos para procesar.")
        return 0

    groups = _group_files_by_content(loose_files, reader_script, args.max_files)
    if args.limit > 0:
        groups = groups[:args.limit]

    total_groups = len(groups)
    print(f"Grupos a procesar: {total_groups}")
    print()

    ok = skipped = errors = sin_prov = 0
    processed = 0
    t_start = time.monotonic()
    seen_doc_keys: Set[Tuple[str, str, str, str, str]] = set()

    for group in groups:
        files = list(group["files"])
        source_names = str(group["source_names"])

        print(f"Procesando: {source_names}")
        if len(files) > args.max_files:
            print(f"[SKIP] {source_names}: supera --max-files={args.max_files}")
            _append_log(log_path, status="SKIP", old_name=source_names, new_name=source_names,
                        detail=f"supera max-files={args.max_files}")
            skipped += 1
            processed += 1
            elapsed = time.monotonic() - t_start
            print(f"\x00PROGRESS:{processed}/{total_groups}:{elapsed:.2f}", flush=True)
            continue

        status, new_name, detail = _process_group(
            files, source_names, reader_script, conn_str,
            apply=args.apply,
            keep_accents=args.keep_accents,
            max_files=args.max_files,
            seen_doc_keys=seen_doc_keys,
        )

        label = f"[{status}] {source_names}"
        if new_name != source_names:
            label += f" → {new_name}"
        print(f"{label}: {detail}")
        _append_log(log_path, status=status, old_name=source_names, new_name=new_name, detail=detail)

        if status in ("OK", "PENDIENTE"):
            ok += 1
        elif status == "SKIP":
            skipped += 1
        elif status == "SIN_PROVEEDOR":
            sin_prov += 1
            ok += 1  # grabado igual, solo sin cuenta contable
        else:
            errors += 1

        processed += 1
        elapsed = time.monotonic() - t_start
        print(f"\x00PROGRESS:{processed}/{total_groups}:{elapsed:.2f}", flush=True)

    mode = "APLICADO" if args.apply else "SIMULACION"
    print()
    print(f"Resumen {mode}: OK={ok}  SIN_PROVEEDOR={sin_prov}  SKIP={skipped}  ERROR={errors}")
    print(f"Log: {log_path}")
    return 0 if errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
