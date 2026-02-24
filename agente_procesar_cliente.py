#!/usr/bin/env python3
"""
Agente de automatizacion para procesar tarjetas y compras por cliente.

Reglas:
- Lee RUTA_CLIENTE o RUTAS_CLIENTE desde variables de entorno.
- Procesa solo archivos en carpeta raiz de:
  - <RUTA_CLIENTE>/TARJETAS
  - <RUTA_CLIENTE>/COMPRAS
- Para multiples clientes: RUTAS_CLIENTE con rutas separadas por ';'.
- Ejecuta en secuencia:
  - lector_liquidaciones_to_json_v1.py para TARJETAS
  - lector_facturas_to_json_v5.py para COMPRAS
- Usa defaults de cada script (sin --gui).
- Genera <archivo>.log como marca de procesado:
  - Si existe, se salta.
  - Registra OK o ERROR y descripcion.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
import subprocess
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    import pyodbc
except Exception:
    pyodbc = None


# Extensiones soportadas actualmente por ambos scripts lectores.
SUPPORTED_EXTS = {".pdf", ".jpg", ".jpeg", ".png", ".webp"}
PROC_SUBDIR_NAME = "PROC_AGENTE_IA"
RETENTION_DAYS = 7
FILE_STABLE_SECONDS = 120
LOCK_STALE_HOURS = 12
DEFAULT_AGENT_IA_TASK = "Proceso_automatico"
DEFAULT_CONFIG_TABLE = "clientes"
DEFAULT_CONFIG_ID_COL = "idcliente"
DEFAULT_CONFIG_ROUTE_COL = "RutaIA_procesar"


def _now() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _runtime_base_dir() -> Path:
    # En PyInstaller --onefile, __file__ puede vivir en carpeta temporal.
    # Para logs/config conviene la carpeta del ejecutable real.
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _write_log(log_path: Path, lines: Iterable[str]) -> None:
    text = "\n".join(lines).rstrip() + "\n"
    log_path.write_text(text, encoding="utf-8", errors="replace")


def _append_text(log_path: Path, text: str) -> None:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8", errors="replace") as f:
        f.write(text.rstrip() + "\n")


def _load_dotenv_file(dotenv_path: Path) -> None:
    """Carga variables desde .env solo si no existen en el entorno actual."""
    if not dotenv_path.exists() or not dotenv_path.is_file():
        return
    for raw_line in dotenv_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and os.getenv(key) is None:
            os.environ[key] = value


def _split_env_paths(raw: str) -> List[str]:
    if not raw:
        return []
    unified = raw.replace("\r", ";").replace("\n", ";")
    parts = []
    for piece in unified.split(";"):
        p = piece.strip().strip('"').strip("'").strip()
        if not p:
            continue
        parts.append(p)
    return parts


def _parse_client_paths() -> List[Path]:
    rutas_cliente = os.getenv("RUTAS_CLIENTE", "")
    ruta_cliente = os.getenv("RUTA_CLIENTE", "")

    raw_paths: List[str] = []
    raw_paths.extend(_split_env_paths(rutas_cliente))
    raw_paths.extend(_split_env_paths(ruta_cliente))

    unique_paths: List[Path] = []
    seen: set[str] = set()
    for raw in raw_paths:
        key = os.path.normcase(os.path.normpath(raw))
        if key in seen:
            continue
        seen.add(key)
        unique_paths.append(Path(raw))

    return unique_paths


def _normalize_base_client_path(raw_path: Path) -> Path:
    """Si pasan ...\\TARJETAS o ...\\COMPRAS como base, sube un nivel."""
    p = raw_path
    tail = p.name.strip().lower()
    if tail in {"tarjetas", "compras"} and p.parent != p:
        return p.parent
    return p


def _normalize_client_base_list(paths: List[Path]) -> List[Path]:
    unique_paths: List[Path] = []
    seen: set[str] = set()
    for p in paths:
        base = _normalize_base_client_path(p)
        key = os.path.normcase(os.path.normpath(str(base)))
        if key in seen:
            continue
        seen.add(key)
        unique_paths.append(base)
    return unique_paths


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        add_help=True,
        description="Agente para procesar TARJETAS/COMPRAS por rutas de cliente.",
    )
    parser.add_argument(
        "--idcliente",
        type=int,
        default=None,
        help="Si se informa, busca la RutaIA_procesar en configuracion y procesa solo ese cliente.",
    )
    return parser.parse_args(argv)


def _norm_path_str(value: str) -> str:
    raw = (value or "").strip().strip('"').strip("'")
    if not raw:
        return ""
    norm = os.path.normcase(os.path.normpath(raw))
    return norm.rstrip("\\/")


def _sql_connection_string_from_env() -> str:
    driver = (os.getenv("SQL_DRIVER") or "").strip() or "ODBC Driver 17 for SQL Server"
    server = (os.getenv("SQL_SERVER") or "").strip()
    database = (os.getenv("SQL_DATABASE") or "").strip()
    user = (os.getenv("SQL_USER") or "").strip()
    password = (os.getenv("SQL_PASSWORD") or "").strip()

    if not server or not database:
        return ""

    if user and password:
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};DATABASE={database};UID={user};PWD={password};"
            "TrustServerCertificate=yes;"
        )

    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={database};Trusted_Connection=yes;"
        "TrustServerCertificate=yes;"
    )


def _load_client_config() -> Tuple[Dict[str, int], Dict[int, str], Optional[str]]:
    if pyodbc is None:
        return {}, {}, "pyodbc no disponible."

    conn_str = _sql_connection_string_from_env()
    if not conn_str:
        return {}, {}, "Falta configurar SQL_SERVER/SQL_DATABASE."

    table_name = DEFAULT_CONFIG_TABLE
    id_col = DEFAULT_CONFIG_ID_COL
    route_col = DEFAULT_CONFIG_ROUTE_COL

    by_route: Dict[str, int] = {}
    by_id: Dict[int, str] = {}

    try:
        with pyodbc.connect(conn_str, timeout=10) as conn:
            cur = conn.cursor()
            # Resolver esquema real para tabla de clientes sin depender de .env.
            cur.execute(
                """
                SELECT c.TABLE_SCHEMA, c.TABLE_NAME
                FROM INFORMATION_SCHEMA.COLUMNS c
                WHERE c.COLUMN_NAME IN (?, ?)
                GROUP BY c.TABLE_SCHEMA, c.TABLE_NAME
                HAVING COUNT(DISTINCT c.COLUMN_NAME) = 2
                ORDER BY CASE WHEN LOWER(c.TABLE_NAME) = ? THEN 0 ELSE 1 END, c.TABLE_SCHEMA, c.TABLE_NAME
                """,
                id_col,
                route_col,
                table_name.lower(),
            )
            candidates = cur.fetchall()
            if not candidates:
                return {}, {}, (
                    "No se encontro tabla con columnas requeridas "
                    f"({id_col}, {route_col}) para resolver idcliente por ruta."
                )
            schema_name = str(candidates[0][0]).strip()
            resolved_table = str(candidates[0][1]).strip()
            table_ref = f"[{schema_name}].[{resolved_table}]"
            query = f"SELECT [{id_col}], [{route_col}] FROM {table_ref} WHERE [{route_col}] IS NOT NULL"
            cur.execute(query)
            for row in cur.fetchall():
                try:
                    rid = int(row[0])
                except Exception:
                    continue
                route = str(row[1] or "").strip()
                if not route:
                    continue
                norm = _norm_path_str(route)
                if norm:
                    by_route[norm] = rid
                    by_id[rid] = route
    except Exception as e:
        return {}, {}, f"No se pudo consultar configuracion SQL: {e}"

    return by_route, by_id, None


def _safe_log_dir_name(base: Path) -> str:
    name = (base.name or str(base)).strip()
    safe = "".join(ch if ch.isalnum() or ch in ("-", "_", ".") else "_" for ch in name)
    return safe or "cliente"


def _lock_is_stale(lock_path: Path, stale_hours: int) -> bool:
    if not lock_path.exists():
        return False
    try:
        age_seconds = dt.datetime.now().timestamp() - lock_path.stat().st_mtime
        return age_seconds > (stale_hours * 60 * 60)
    except Exception:
        return False


def _try_acquire_lock(lock_path: Path) -> bool:
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        return False
    with os.fdopen(fd, "w", encoding="utf-8", errors="replace") as f:
        payload = {
            "pid": os.getpid(),
            "timestamp": _now(),
            "host": os.getenv("COMPUTERNAME", ""),
        }
        f.write(json.dumps(payload, ensure_ascii=True))
    return True


def _release_lock(lock_path: Path) -> None:
    try:
        if lock_path.exists():
            lock_path.unlink()
    except Exception:
        # No corta el proceso por un fallo al liberar lock.
        pass


def _is_file_stable(src_file: Path, stable_seconds: int) -> bool:
    try:
        stat = src_file.stat()
    except Exception:
        return False
    if stat.st_size <= 0:
        return False
    age_seconds = dt.datetime.now().timestamp() - stat.st_mtime
    return age_seconds >= stable_seconds


def _iter_root_files(folder: Path) -> List[Path]:
    if not folder.exists() or not folder.is_dir():
        return []
    files = []
    for p in folder.iterdir():
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS:
            files.append(p)
    files.sort(key=lambda x: x.name.lower())
    return files


def _cleanup_old_files(folder: Path, days: int = RETENTION_DAYS) -> int:
    if not folder.exists() or not folder.is_dir():
        return 0
    cutoff = dt.datetime.now().timestamp() - (days * 24 * 60 * 60)
    deleted = 0
    for p in folder.iterdir():
        if not p.is_file():
            continue
        try:
            if p.stat().st_mtime < cutoff:
                p.unlink()
                deleted += 1
        except Exception:
            # No corta el proceso por fallos de limpieza puntuales.
            continue
    return deleted


def _build_reader_env(ia_task: str, idcliente: int) -> Dict[str, str]:
    env = os.environ.copy()
    # Auditoria backend/DB: identificar invocaciones automaticas del agente.
    env["IA_TASK"] = ia_task
    env["IA_IDCLIENTE"] = str(idcliente)
    env["IDCLIENTE"] = str(idcliente)
    return env


def _run_reader(reader_script: Path, src_file: Path, outdir: Path, ia_task: str, idcliente: int) -> Tuple[bool, str]:
    cmd = [
        sys.executable,
        str(reader_script),
        str(src_file),
        "--outdir",
        str(outdir),
    ]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(reader_script.parent),
        env=_build_reader_env(ia_task, idcliente),
    )
    merged = (proc.stdout or "").strip()
    if proc.stderr:
        merged = (merged + "\n" + proc.stderr.strip()).strip()
    ok = proc.returncode == 0
    return ok, merged


def _run_reader_many(
    reader_script: Path, src_files: List[Path], outdir: Path, ia_task: str, idcliente: int
) -> Tuple[bool, str]:
    cmd = [sys.executable, str(reader_script), *[str(p) for p in src_files], "--outdir", str(outdir)]
    proc = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        cwd=str(reader_script.parent),
        env=_build_reader_env(ia_task, idcliente),
    )
    merged = (proc.stdout or "").strip()
    if proc.stderr:
        merged = (merged + "\n" + proc.stderr.strip()).strip()
    ok = proc.returncode == 0
    return ok, merged


def _looks_like_date_token(s: str) -> Optional[str]:
    m = re.search(r"\b(\d{1,2}[-_/]\d{1,2}(?:[-_/]\d{2,4})?)\b", s)
    return m.group(1).replace("_", "-").replace("/", "-") if m else None


def _looks_like_comprobante_token(s: str) -> Optional[str]:
    m = re.search(r"\b(\d{1,4}\s*[-_/]\s*\d{4,8})\b", s)
    if m:
        return re.sub(r"\s+", "", m.group(1)).replace("_", "-").replace("/", "-")
    m = re.search(r"\b(\d{8,14})\b", s)
    return m.group(1) if m else None


def _extract_provider_date_comprobante(stem: str) -> Optional[Tuple[str, str, str]]:
    s = re.sub(r"\s+", " ", stem).strip()
    date_tok = _looks_like_date_token(s)
    comp_tok = _looks_like_comprobante_token(s)
    if not date_tok or not comp_tok:
        return None

    provider = s.upper()
    provider = re.sub(r"\b(FAC|FACT|FACTURA|NC|NOTA|CREDITO|COMPROBANTE|OK)\b", " ", provider, flags=re.IGNORECASE)
    provider = provider.replace(date_tok.upper(), " ").replace(comp_tok.upper(), " ")
    provider = re.sub(r"[\W_]+", " ", provider, flags=re.UNICODE)
    provider = re.sub(r"\s+", " ", provider).strip()
    if len(provider) < 3:
        return None
    return provider, date_tok, comp_tok


def _group_compras_candidates(files: List[Path]) -> List[List[Path]]:
    grouped: Dict[Tuple[str, str, str], List[Path]] = defaultdict(list)
    singles: List[Path] = []
    for f in files:
        key = _extract_provider_date_comprobante(f.stem)
        if key is None:
            singles.append(f)
            continue
        grouped[key].append(f)

    out: List[List[Path]] = []
    for _, group in grouped.items():
        group.sort(key=lambda p: (p.stat().st_mtime, p.name.lower()))
        out.append(group)
    for f in singles:
        out.append([f])
    out.sort(key=lambda g: g[0].name.lower())
    return out


def _read_status_from_log(log_path: Path) -> Optional[str]:
    try:
        for line in log_path.read_text(encoding="utf-8", errors="replace").splitlines():
            if line.startswith("STATUS="):
                return line.split("=", 1)[1].strip().upper()
    except Exception:
        return None
    return None


def _process_folder(
    folder: Path,
    reader_script: Path,
    label: str,
    stable_seconds: int,
    force_reprocess: bool,
    pregroup_compras: bool,
    ia_task: str,
    idcliente: int,
) -> Tuple[int, int, int, int, List[str]]:
    processed = 0
    skipped = 0
    errors = 0
    not_ready = 0
    events: List[str] = []
    proc_dir = folder / PROC_SUBDIR_NAME
    proc_dir.mkdir(parents=True, exist_ok=True)
    deleted = _cleanup_old_files(proc_dir, RETENTION_DAYS)

    files = _iter_root_files(folder)
    print(f"[{_now()}] {label}: encontrados {len(files)} archivos en {folder}")
    events.append(f"[{_now()}] {label}|INFO|Encontrados={len(files)}|Folder={folder}")
    if deleted:
        print(f"[{_now()}] {label}: limpieza en {proc_dir.name}, eliminados {deleted} archivos (> {RETENTION_DAYS} dias)")
        events.append(f"[{_now()}] {label}|INFO|Limpieza={deleted}|Folder={proc_dir}")

    pending_files: List[Path] = []
    for src_file in files:
        log_path = proc_dir / f"{src_file.name}.log"
        if log_path.exists():
            if force_reprocess:
                print(f"[{_now()}] {label}: REPROCESAR {src_file.name} (ignora {log_path.name})")
                events.append(f"[{_now()}] {label}|REPROCESS|{src_file.name}|IgnoraLog={log_path.name}")
            else:
                prev_status = _read_status_from_log(log_path)
                if prev_status == "ERROR":
                    print(f"[{_now()}] {label}: REINTENTO {src_file.name} (log previo en ERROR)")
                    events.append(f"[{_now()}] {label}|RETRY|{src_file.name}|PrevStatus=ERROR")
                else:
                    skipped += 1
                    print(f"[{_now()}] {label}: SKIP {src_file.name} (ya existe {log_path.name})")
                    events.append(f"[{_now()}] {label}|SKIP|{src_file.name}|YaProcesadoLog={log_path.name}")
                    continue

        if not _is_file_stable(src_file, stable_seconds):
            not_ready += 1
            print(f"[{_now()}] {label}: SKIP {src_file.name} (archivo reciente/en subida)")
            events.append(f"[{_now()}] {label}|SKIP_NOT_READY|{src_file.name}|StableSec={stable_seconds}")
            continue

        pending_files.append(src_file)

    use_grouping = pregroup_compras and label.startswith("COMPRAS[")
    groups: List[List[Path]] = [[p] for p in pending_files]
    if use_grouping and pending_files:
        groups = _group_compras_candidates(pending_files)
        multi = sum(1 for g in groups if len(g) > 1)
        events.append(f"[{_now()}] {label}|INFO|PreAgrupado={len(groups)}|Multipagina={multi}")
        if multi:
            print(f"[{_now()}] {label}: pre-agrupado activo, grupos={len(groups)}, multipagina={multi}")

    for group in groups:
        if len(group) == 1:
            src_file = group[0]
            log_path = proc_dir / f"{src_file.name}.log"
            print(f"[{_now()}] {label}: PROCESANDO {src_file.name}")
            ok, output = _run_reader(reader_script, src_file, proc_dir, ia_task, idcliente)
            processed += 1
            group_desc = src_file.name
        else:
            names = " | ".join(p.name for p in group)
            print(f"[{_now()}] {label}: PROCESANDO GRUPO ({len(group)}): {names}")
            ok, output = _run_reader_many(reader_script, group, proc_dir, ia_task, idcliente)
            processed += len(group)
            group_desc = names

        if ok:
            for src_file in group:
                log_path = proc_dir / f"{src_file.name}.log"
                _write_log(
                    log_path,
                    [
                        f"STATUS=OK",
                        f"TIMESTAMP={_now()}",
                        f"READER={reader_script.name}",
                        f"FILE={src_file}",
                        f"OUTPUT_DIR={proc_dir}",
                        f"GROUP_SIZE={len(group)}",
                        f"MESSAGE=Procesado correctamente",
                        "OUTPUT_BEGIN",
                        output if output else "(sin salida)",
                        "OUTPUT_END",
                    ],
                )
                events.append(f"[{_now()}] {label}|OK|{src_file.name}|GroupSize={len(group)}")
            print(f"[{_now()}] {label}: OK {group_desc}")
        else:
            errors += len(group)
            for src_file in group:
                log_path = proc_dir / f"{src_file.name}.log"
                _write_log(
                    log_path,
                    [
                        f"STATUS=ERROR",
                        f"TIMESTAMP={_now()}",
                        f"READER={reader_script.name}",
                        f"FILE={src_file}",
                        f"OUTPUT_DIR={proc_dir}",
                        f"GROUP_SIZE={len(group)}",
                        f"MESSAGE=Error durante el procesamiento",
                        "OUTPUT_BEGIN",
                        output if output else "(sin detalle de error)",
                        "OUTPUT_END",
                    ],
                )
                events.append(f"[{_now()}] {label}|ERROR|{src_file.name}|Log={log_path.name}|GroupSize={len(group)}")
            print(f"[{_now()}] {label}: ERROR {group_desc}")

    return processed, skipped, errors, not_ready, events


def main(argv: Optional[List[str]] = None) -> int:
    args = _parse_args(argv)
    project_dir = _runtime_base_dir()
    day_stamp = dt.datetime.now().strftime("%Y%m%d")
    log_root = project_dir / "LOG"
    agent_log_path = log_root / f"agente_{day_stamp}.log"
    lock_path = log_root / "agente_procesar_cliente.lock"
    run_start = _now()
    _load_dotenv_file(project_dir / ".env")
    stable_seconds = int(os.getenv("ARCHIVO_ESTABLE_SEGUNDOS", str(FILE_STABLE_SECONDS)) or FILE_STABLE_SECONDS)
    lock_stale_hours = int(os.getenv("LOCK_STALE_HORAS", str(LOCK_STALE_HOURS)) or LOCK_STALE_HOURS)
    force_reprocess = os.getenv("REPROCESAR_TODO", "0").strip().lower() in {"1", "true", "yes", "si", "y"}
    pregroup_compras = os.getenv("PREAGRUPAR_COMPRAS", "1").strip().lower() in {"1", "true", "yes", "si", "y"}
    agent_ia_task = (os.getenv("AGENTE_IA_TASK", DEFAULT_AGENT_IA_TASK) or "").strip() or DEFAULT_AGENT_IA_TASK
    route_to_id, id_to_route, config_error = _load_client_config()

    if _lock_is_stale(lock_path, lock_stale_hours):
        try:
            lock_path.unlink()
            _append_text(agent_log_path, f"[{run_start}] WARN | Lock viejo eliminado: {lock_path}")
        except Exception:
            pass

    if not _try_acquire_lock(lock_path):
        print("INFO: ya hay una ejecucion en curso. Se cancela esta corrida.")
        _append_text(agent_log_path, f"[{run_start}] RESULT=SKIP | Motivo=Lock activo {lock_path}")
        return 0

    try:
        if args.idcliente is not None:
            if config_error:
                print(f"ERROR: no se puede consultar configuracion para --idcliente. Detalle: {config_error}")
                _append_text(
                    agent_log_path,
                    f"[{run_start}] RESULT=ERROR | Motivo=Fallo configuracion SQL para --idcliente | Detalle={config_error}",
                )
                return 2
            route = id_to_route.get(int(args.idcliente))
            if not route:
                print(f"INFO: idcliente={args.idcliente} no tiene RutaIA_procesar informada. No se procesa.")
                _append_text(
                    agent_log_path,
                    f"[{run_start}] RESULT=SKIP | Motivo=idcliente={args.idcliente} sin RutaIA_procesar en configuracion",
                )
                return 0
            client_bases = _normalize_client_base_list([Path(route)])
        else:
            client_bases = _parse_client_paths()
            if not client_bases:
                print("ERROR: falta variable de entorno RUTA_CLIENTE o RUTAS_CLIENTE.")
                print(r"Definila en .env (RUTA_CLIENTE=... o RUTAS_CLIENTE=ruta1;ruta2) o en entorno de PowerShell.")
                _append_text(agent_log_path, f"[{run_start}] RESULT=ERROR | Motivo=Faltan rutas de cliente")
                return 2
            client_bases = _normalize_client_base_list(client_bases)

        reader_tarjetas = project_dir / "lector_liquidaciones_to_json_v1.py"
        reader_compras = project_dir / "lector_facturas_to_json_v5.py"

        if not reader_tarjetas.exists():
            print(f"ERROR: no existe {reader_tarjetas}")
            _append_text(agent_log_path, f"[{run_start}] RESULT=ERROR | Motivo=No existe {reader_tarjetas}")
            return 2
        if not reader_compras.exists():
            print(f"ERROR: no existe {reader_compras}")
            _append_text(agent_log_path, f"[{run_start}] RESULT=ERROR | Motivo=No existe {reader_compras}")
            return 2

        total_processed = 0
        total_skipped = 0
        total_errors = 0
        total_not_ready = 0

        global_events: List[str] = []
        if config_error:
            print(f"[{_now()}] WARN: no se pudo leer configuracion SQL ({config_error})")
            global_events.append(f"[{_now()}] CONFIG|WARN|{config_error}")

        for base in client_bases:
            norm_base = _norm_path_str(str(base))
            if args.idcliente is not None:
                resolved_idcliente = int(args.idcliente)
                ruta_no_informada = False
            else:
                resolved_idcliente = route_to_id.get(norm_base, 1)
                ruta_no_informada = resolved_idcliente == 1 and norm_base not in route_to_id
            tarjetas_dir = base / "TARJETAS"
            compras_dir = base / "COMPRAS"
            client_log_path = log_root / _safe_log_dir_name(base) / f"agente_{day_stamp}.log"
            client_events: List[str] = [f"[{_now()}] CLIENTE|INICIO|Base={base}|IdCliente={resolved_idcliente}"]
            client_processed = 0
            client_skipped = 0
            client_errors = 0
            client_not_ready = 0

            print(f"\n[{_now()}] CLIENTE: {base} | idcliente={resolved_idcliente}")
            if ruta_no_informada:
                msg = "No esta informada la carpeta en configuracion (RutaIA_procesar). Se usa idcliente=1."
                print(f"[{_now()}] WARN: {msg}")
                client_events.append(f"[{_now()}] CLIENTE|ERROR|Base={base}|IdCliente=1|{msg}")
                client_errors += 1
                total_errors += 1

            p, s, e, nr, ev = _process_folder(
                tarjetas_dir,
                reader_tarjetas,
                f"TARJETAS[{base.name}]",
                stable_seconds,
                force_reprocess,
                False,
                agent_ia_task,
                resolved_idcliente,
            )
            client_processed += p
            client_skipped += s
            client_errors += e
            client_not_ready += nr
            total_processed += p
            total_skipped += s
            total_errors += e
            total_not_ready += nr
            client_events.extend(ev)

            p, s, e, nr, ev = _process_folder(
                compras_dir,
                reader_compras,
                f"COMPRAS[{base.name}]",
                stable_seconds,
                force_reprocess,
                pregroup_compras,
                agent_ia_task,
                resolved_idcliente,
            )
            client_processed += p
            client_skipped += s
            client_errors += e
            client_not_ready += nr
            total_processed += p
            total_skipped += s
            total_errors += e
            total_not_ready += nr
            client_events.extend(ev)

            client_result = "OK" if client_errors == 0 else "ERROR"
            client_lines = [
                f"[{run_start}] INICIO | CLIENTE={base} | IdCliente={resolved_idcliente} | StableSec={stable_seconds} | ReprocesarTodo={int(force_reprocess)} | PreAgruparCompras={int(pregroup_compras)} | IATask={agent_ia_task}",
                *client_events,
                f"[{_now()}] RESULT={client_result} | Procesados={client_processed} | Saltados={client_skipped} | NoListos={client_not_ready} | Errores={client_errors}",
                "",
            ]
            _append_text(client_log_path, "\n".join(client_lines))
            global_events.append(
                f"[{_now()}] CLIENTE|RESULT|Base={base}|IdCliente={resolved_idcliente}|Procesados={client_processed}|Saltados={client_skipped}|NoListos={client_not_ready}|Errores={client_errors}"
            )

        print("\n=== RESUMEN ===")
        print("RUTAS_CLIENTE:")
        for base in client_bases:
            print(f"- {base}")
        print(f"Procesados: {total_processed}")
        print(f"Saltados (.log existente): {total_skipped}")
        print(f"Saltados (archivo en subida/reciente): {total_not_ready}")
        print(f"Errores: {total_errors}")

        run_end = _now()
        result = "OK" if total_errors == 0 else "ERROR"
        rutas_str = ";".join(str(p) for p in client_bases)
        log_lines = [
            f"[{run_start}] INICIO | RUTAS_CLIENTE={rutas_str} | StableSec={stable_seconds} | ReprocesarTodo={int(force_reprocess)} | PreAgruparCompras={int(pregroup_compras)} | IATask={agent_ia_task}",
            *global_events,
            f"[{run_end}] RESULT={result} | Procesados={total_processed} | Saltados={total_skipped} | NoListos={total_not_ready} | Errores={total_errors}",
            "",
        ]
        _append_text(agent_log_path, "\n".join(log_lines))
        return 0
    finally:
        _release_lock(lock_path)


if __name__ == "__main__":
    raise SystemExit(main())
