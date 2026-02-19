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

import datetime as dt
import os
import subprocess
import sys
from pathlib import Path
from typing import Iterable, List, Tuple


# Extensiones soportadas actualmente por ambos scripts lectores.
SUPPORTED_EXTS = {".pdf", ".jpg", ".jpeg", ".png", ".webp"}
PROC_SUBDIR_NAME = "PROC_AGENTE_IA"
RETENTION_DAYS = 7


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


def _parse_client_paths() -> List[Path]:
    rutas_cliente = os.getenv("RUTAS_CLIENTE", "").strip()
    ruta_cliente = os.getenv("RUTA_CLIENTE", "").strip()

    raw_paths: List[str] = []
    if rutas_cliente:
        raw_paths.extend(part.strip() for part in rutas_cliente.split(";") if part.strip())
    if ruta_cliente:
        raw_paths.append(ruta_cliente)

    unique_paths: List[Path] = []
    seen: set[str] = set()
    for raw in raw_paths:
        norm = str(Path(raw).resolve())
        key = norm.lower()
        if key in seen:
            continue
        seen.add(key)
        unique_paths.append(Path(raw))

    return unique_paths


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


def _run_reader(reader_script: Path, src_file: Path, outdir: Path) -> Tuple[bool, str]:
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
    )
    merged = (proc.stdout or "").strip()
    if proc.stderr:
        merged = (merged + "\n" + proc.stderr.strip()).strip()
    ok = proc.returncode == 0
    return ok, merged


def _process_folder(folder: Path, reader_script: Path, label: str) -> Tuple[int, int, int, List[str]]:
    processed = 0
    skipped = 0
    errors = 0
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

    for src_file in files:
        log_path = proc_dir / f"{src_file.name}.log"
        if log_path.exists():
            skipped += 1
            print(f"[{_now()}] {label}: SKIP {src_file.name} (ya existe {log_path.name})")
            events.append(f"[{_now()}] {label}|SKIP|{src_file.name}|YaProcesadoLog={log_path.name}")
            continue

        print(f"[{_now()}] {label}: PROCESANDO {src_file.name}")
        ok, output = _run_reader(reader_script, src_file, proc_dir)
        processed += 1

        if ok:
            _write_log(
                log_path,
                [
                    f"STATUS=OK",
                    f"TIMESTAMP={_now()}",
                    f"READER={reader_script.name}",
                    f"FILE={src_file}",
                    f"OUTPUT_DIR={proc_dir}",
                    f"MESSAGE=Procesado correctamente",
                    "OUTPUT_BEGIN",
                    output if output else "(sin salida)",
                    "OUTPUT_END",
                ],
            )
            print(f"[{_now()}] {label}: OK {src_file.name}")
            events.append(f"[{_now()}] {label}|OK|{src_file.name}")
        else:
            errors += 1
            _write_log(
                log_path,
                [
                    f"STATUS=ERROR",
                    f"TIMESTAMP={_now()}",
                    f"READER={reader_script.name}",
                    f"FILE={src_file}",
                    f"OUTPUT_DIR={proc_dir}",
                    f"MESSAGE=Error durante el procesamiento",
                    "OUTPUT_BEGIN",
                    output if output else "(sin detalle de error)",
                    "OUTPUT_END",
                ],
            )
            print(f"[{_now()}] {label}: ERROR {src_file.name} (ver {log_path.name})")
            events.append(f"[{_now()}] {label}|ERROR|{src_file.name}|Log={log_path.name}")

    return processed, skipped, errors, events


def main() -> int:
    project_dir = _runtime_base_dir()
    day_stamp = dt.datetime.now().strftime("%Y%m%d")
    agent_log_path = project_dir / "LOG" / f"agente_{day_stamp}.log"
    run_start = _now()
    _load_dotenv_file(project_dir / ".env")

    client_bases = _parse_client_paths()
    if not client_bases:
        print("ERROR: falta variable de entorno RUTA_CLIENTE o RUTAS_CLIENTE.")
        print(r"Definila en .env (RUTA_CLIENTE=... o RUTAS_CLIENTE=ruta1;ruta2) o en entorno de PowerShell.")
        _append_text(agent_log_path, f"[{run_start}] RESULT=ERROR | Motivo=Faltan rutas de cliente")
        return 2

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

    run_events: List[str] = []

    for base in client_bases:
        tarjetas_dir = base / "TARJETAS"
        compras_dir = base / "COMPRAS"

        print(f"\n[{_now()}] CLIENTE: {base}")
        run_events.append(f"[{_now()}] CLIENTE|INICIO|Base={base}")

        p, s, e, ev = _process_folder(tarjetas_dir, reader_tarjetas, f"TARJETAS[{base.name}]")
        total_processed += p
        total_skipped += s
        total_errors += e
        run_events.extend(ev)

        p, s, e, ev = _process_folder(compras_dir, reader_compras, f"COMPRAS[{base.name}]")
        total_processed += p
        total_skipped += s
        total_errors += e
        run_events.extend(ev)

    print("\n=== RESUMEN ===")
    print("RUTAS_CLIENTE:")
    for base in client_bases:
        print(f"- {base}")
    print(f"Procesados: {total_processed}")
    print(f"Saltados (.log existente): {total_skipped}")
    print(f"Errores: {total_errors}")

    run_end = _now()
    result = "OK" if total_errors == 0 else "ERROR"
    rutas_str = ";".join(str(p) for p in client_bases)
    log_lines = [
        f"[{run_start}] INICIO | RUTAS_CLIENTE={rutas_str}",
        *run_events,
        f"[{run_end}] RESULT={result} | Procesados={total_processed} | Saltados={total_skipped} | Errores={total_errors}",
        "",
    ]
    _append_text(agent_log_path, "\n".join(log_lines))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
