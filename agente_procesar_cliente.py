#!/usr/bin/env python3
"""
Agente de automatizacion para procesar tarjetas y compras por cliente.

Reglas:
- Lee RUTA_CLIENTE desde variable de entorno.
- Procesa solo archivos en carpeta raiz de:
  - <RUTA_CLIENTE>/TARJETAS
  - <RUTA_CLIENTE>/COMPRAS
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


def _now() -> str:
    return dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _write_log(log_path: Path, lines: Iterable[str]) -> None:
    text = "\n".join(lines).rstrip() + "\n"
    log_path.write_text(text, encoding="utf-8", errors="replace")


def _iter_root_files(folder: Path) -> List[Path]:
    if not folder.exists() or not folder.is_dir():
        return []
    files = []
    for p in folder.iterdir():
        if p.is_file() and p.suffix.lower() in SUPPORTED_EXTS:
            files.append(p)
    files.sort(key=lambda x: x.name.lower())
    return files


def _run_reader(reader_script: Path, src_file: Path) -> Tuple[bool, str]:
    cmd = [
        sys.executable,
        str(reader_script),
        str(src_file),
        "--outdir",
        str(src_file.parent),
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


def _process_folder(folder: Path, reader_script: Path, label: str) -> Tuple[int, int, int]:
    processed = 0
    skipped = 0
    errors = 0

    files = _iter_root_files(folder)
    print(f"[{_now()}] {label}: encontrados {len(files)} archivos en {folder}")

    for src_file in files:
        log_path = src_file.with_suffix(".log")
        if log_path.exists():
            skipped += 1
            print(f"[{_now()}] {label}: SKIP {src_file.name} (ya existe {log_path.name})")
            continue

        print(f"[{_now()}] {label}: PROCESANDO {src_file.name}")
        ok, output = _run_reader(reader_script, src_file)
        processed += 1

        if ok:
            _write_log(
                log_path,
                [
                    f"STATUS=OK",
                    f"TIMESTAMP={_now()}",
                    f"READER={reader_script.name}",
                    f"FILE={src_file}",
                    f"MESSAGE=Procesado correctamente",
                    "OUTPUT_BEGIN",
                    output if output else "(sin salida)",
                    "OUTPUT_END",
                ],
            )
            print(f"[{_now()}] {label}: OK {src_file.name}")
        else:
            errors += 1
            _write_log(
                log_path,
                [
                    f"STATUS=ERROR",
                    f"TIMESTAMP={_now()}",
                    f"READER={reader_script.name}",
                    f"FILE={src_file}",
                    f"MESSAGE=Error durante el procesamiento",
                    "OUTPUT_BEGIN",
                    output if output else "(sin detalle de error)",
                    "OUTPUT_END",
                ],
            )
            print(f"[{_now()}] {label}: ERROR {src_file.name} (ver {log_path.name})")

    return processed, skipped, errors


def main() -> int:
    ruta_cliente = os.getenv("RUTA_CLIENTE", "").strip()
    if not ruta_cliente:
        print("ERROR: falta variable de entorno RUTA_CLIENTE.")
        print(r"Ejemplo PowerShell: $env:RUTA_CLIENTE='H:\Mi unidad\CLIENTES\OLIVA'")
        return 2

    base = Path(ruta_cliente)
    tarjetas_dir = base / "TARJETAS"
    compras_dir = base / "COMPRAS"

    project_dir = Path(__file__).resolve().parent
    reader_tarjetas = project_dir / "lector_liquidaciones_to_json_v1.py"
    reader_compras = project_dir / "lector_facturas_to_json_v5.py"

    if not reader_tarjetas.exists():
        print(f"ERROR: no existe {reader_tarjetas}")
        return 2
    if not reader_compras.exists():
        print(f"ERROR: no existe {reader_compras}")
        return 2

    total_processed = 0
    total_skipped = 0
    total_errors = 0

    p, s, e = _process_folder(tarjetas_dir, reader_tarjetas, "TARJETAS")
    total_processed += p
    total_skipped += s
    total_errors += e

    p, s, e = _process_folder(compras_dir, reader_compras, "COMPRAS")
    total_processed += p
    total_skipped += s
    total_errors += e

    print("\n=== RESUMEN ===")
    print(f"RUTA_CLIENTE: {base}")
    print(f"Procesados: {total_processed}")
    print(f"Saltados (.log existente): {total_skipped}")
    print(f"Errores: {total_errors}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
