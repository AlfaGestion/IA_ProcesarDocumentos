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
import unicodedata
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    import pyodbc
except ImportError:
    pyodbc = None

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
SUPPORTED_EXTS = {".pdf", ".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}
INVALID_WIN_CHARS = '<>:"/\\|?*'

DEFAULT_SERVER   = "SERVER-ALFAVB6"
DEFAULT_DATABASE = "ALFANET"
DEFAULT_USER     = "ALFANET"
DEFAULT_PASSWORD = "ALFANET"
DEFAULT_DRIVER   = "SQL Server Native Client 11.0"


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
    p.add_argument("--max-files",       type=int, default=5,
                   help="Máx. archivos por grupo multipágina (default: 5).")
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


def _group_loose_files(files: List[Path], max_files: int) -> List[Dict]:
    exact_groups: Dict[str, List[Tuple[int, Path]]] = {}
    candidate_groups: Dict[str, List[Tuple[int, Path]]] = {}
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
            singles.append(fp)
            continue
        trailing = _group_key_from_trailing_index(fp.stem)
        if trailing:
            candidate_groups.setdefault(trailing[0], []).append((trailing[1], fp))
            continue
        singles.append(fp)

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


def _load_json_from_reader(reader_script: Path, source_files: List[Path]) -> dict:
    with tempfile.TemporaryDirectory(prefix="staging_compras_") as tmpdir:
        if reader_script.suffix.lower() == ".exe":
            cmd = [str(reader_script), *[str(p) for p in source_files], "--outdir", tmpdir, "--auto"]
        else:
            cmd = [sys.executable, str(reader_script), *[str(p) for p in source_files], "--outdir", tmpdir, "--auto"]
        proc = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", check=False)
        if proc.returncode != 0:
            detail = (proc.stderr or proc.stdout or f"código {proc.returncode}").strip()
            raise RuntimeError(detail)
        json_files = sorted(Path(tmpdir).glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not json_files:
            raise RuntimeError("el lector terminó OK pero no generó JSON")
        return json.loads(json_files[0].read_text(encoding="utf-8", errors="replace"))


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
    base = target.name
    for idx in range(2, 1000):
        candidate = target.with_name(f"{base} ({idx})")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"no se encontró nombre libre para {target}")


def _dest_folder(root: Path, fecha: Optional[dt.datetime] = None) -> Path:
    """Devuelve root/PROCESADOS/YYYY-MM, creando la carpeta si no existe."""
    ref = fecha or dt.datetime.now()
    folder = root / "PROCESADOS" / ref.strftime("%Y-%m")
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


def _already_in_staging(conn_str: str, nombre_original: str) -> bool:
    """Devuelve True si el archivo ya fue grabado en staging y no está en ERROR_LECTURA."""
    if pyodbc is None:
        return False
    try:
        with pyodbc.connect(conn_str, timeout=10) as conn:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(1) FROM IA_Compras_CAB "
                "WHERE Archivo_NombreOriginal = ? AND Estado <> 'ERROR_LECTURA'",
                nombre_original,
            )
            row = cur.fetchone()
            return bool(row and row[0] > 0)
    except Exception:
        return False


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
            ) VALUES (?,GETDATE(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
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
        cur.execute("SELECT SCOPE_IDENTITY()")
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
) -> Tuple[str, str, str]:
    """Devuelve (status, new_name, detail)."""

    # Ya está en staging y no es error → skip
    if apply and _already_in_staging(conn_str, files[0].name):
        return "SKIP", source_names, "ya existe en staging"

    # Llamar al lector
    try:
        data = _load_json_from_reader(reader_script, files)
    except Exception as exc:
        cab_data = _build_cab_data_error(files, str(exc))
        if apply:
            try:
                _insert_staging(conn_str, cab_data, [])
            except Exception as e2:
                return "ERROR", source_names, f"lector falló: {exc} | SQL falló: {e2}"
        return "ERROR", source_names, f"lector: {exc}"

    cab = data.get("CAB") if isinstance(data, dict) else None
    tot = data.get("TOTALES") if isinstance(data, dict) else None
    rows = data.get("ROWS") if isinstance(data, dict) else []
    meta = data.get("meta") if isinstance(data, dict) else {}
    if not isinstance(cab, dict):
        cab = {}
    if not isinstance(tot, dict):
        tot = {}
    if not isinstance(rows, list):
        rows = []
    if not isinstance(meta, dict):
        meta = {}

    # Lookup proveedor
    cuit    = _digits_only(_cab_pick(cab, "CUIT", "NUMERO_CUIT"))
    nombre  = _cab_pick(cab, "Proveedor", "Nombre")
    domicil = _cab_pick(cab, "Domicilio")
    cuenta_contable, razon_social_sql, match_metodo = _lookup_provider(conn_str, cuit, nombre, domicil)

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

    if apply:
        # Grabar en SQL
        try:
            _insert_staging(conn_str, cab_data, rows)
        except Exception as e:
            return "ERROR", source_names, f"SQL insert falló: {e}"
        # Renombrar archivos
        if base_name and targets:
            try:
                for src, tgt in zip(files, targets):
                    src.rename(tgt)
            except Exception as e:
                return "ERROR", new_name, f"grabado en SQL pero renombrado falló: {e}"

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

    root_win = tk.Tk()
    root_win.title("Agente Staging Comprobantes")
    root_win.minsize(680, 600)

    var_root      = tk.StringVar()
    var_apply     = tk.BooleanVar(value=False)
    var_max_files = tk.IntVar(value=5)
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

    # --- Salida ---
    out_frame = ttk.LabelFrame(root_win, text="Salida", padding=5)
    out_frame.grid(row=3, column=0, sticky="nsew", padx=10, pady=4)
    txt = scrolledtext.ScrolledText(out_frame, width=85, height=16, state="disabled", wrap="word")
    txt.pack(fill="both", expand=True)

    # --- Botones ---
    btn_frame = ttk.Frame(root_win, padding=(10, 0, 10, 10))
    btn_frame.grid(row=4, column=0, sticky="ew")
    result_code = [0]

    def _append(text: str) -> None:
        txt.configure(state="normal")
        txt.insert("end", text)
        txt.see("end")
        txt.configure(state="disabled")

    class _GuiStream:
        def write(self, s: str) -> None:
            if s:
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

        btn_run.configure(state="disabled")
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
    root_win.rowconfigure(3, weight=1)
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

    groups = _group_loose_files(loose_files, args.max_files)
    if args.limit > 0:
        groups = groups[:args.limit]

    print(f"Grupos a procesar: {len(groups)}")
    print()

    ok = skipped = errors = sin_prov = 0

    for group in groups:
        files = list(group["files"])
        source_names = str(group["source_names"])

        if len(files) > args.max_files:
            print(f"[SKIP] {source_names}: supera --max-files={args.max_files}")
            _append_log(log_path, status="SKIP", old_name=source_names, new_name=source_names,
                        detail=f"supera max-files={args.max_files}")
            skipped += 1
            continue

        status, new_name, detail = _process_group(
            files, source_names, reader_script, conn_str,
            apply=args.apply,
            keep_accents=args.keep_accents,
            max_files=args.max_files,
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

    mode = "APLICADO" if args.apply else "SIMULACION"
    print()
    print(f"Resumen {mode}: OK={ok}  SIN_PROVEEDOR={sin_prov}  SKIP={skipped}  ERROR={errors}")
    print(f"Log: {log_path}")
    return 0 if errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
