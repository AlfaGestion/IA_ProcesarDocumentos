#!/usr/bin/env python3
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


SUPPORTED_EXTS = {".pdf", ".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tif", ".tiff"}
INVALID_WIN_CHARS = '<>:"/\\|?*'


def _parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Analiza PDF e imagenes sueltas dentro de una carpeta base y renombra "
            "los archivos originales segun proveedor, tipo de comprobante, numero y fecha."
        )
    )
    parser.add_argument("root", help="Carpeta base con archivos sueltos a ordenar.")
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Ejecuta los renombrados reales. Sin este flag solo muestra una simulacion.",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=5,
        help="Maximo de archivos soportados por grupo para enviar al lector (default: 5).",
    )
    parser.add_argument(
        "--include-pdfs-only",
        action="store_true",
        help="Si se indica, ignora imagenes y toma solo PDFs.",
    )
    parser.add_argument(
        "--keep-accents",
        action="store_true",
        help="Conserva acentos en el nombre final. Por defecto se normaliza a ASCII.",
    )
    parser.add_argument(
        "--log-file",
        default="organizar_carpetas_compras.log",
        help="Nombre base del log dentro de la subcarpeta LOG.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Procesa solo los primeros N grupos (0 = sin limite).",
    )
    return parser.parse_args(argv)


def _natural_sort_key(value: str) -> List[object]:
    parts = re.split(r"(\d+)", value.casefold())
    out: List[object] = []
    for piece in parts:
        out.append(int(piece) if piece.isdigit() else piece)
    return out


def _collect_supported_files(root: Path, include_pdfs_only: bool) -> List[Path]:
    exts = {".pdf"} if include_pdfs_only else SUPPORTED_EXTS
    files = [p for p in root.iterdir() if p.is_file() and p.suffix.lower() in exts]
    return sorted(files, key=lambda p: _natural_sort_key(p.name))


def _load_json_from_reader(reader_script: Path, source_files: List[Path]) -> dict:
    with tempfile.TemporaryDirectory(prefix="organizar_compras_") as tmpdir:
        if reader_script.suffix.lower() == ".exe":
            cmd = [str(reader_script), *[str(p) for p in source_files], "--outdir", tmpdir, "--auto"]
        else:
            cmd = [sys.executable, str(reader_script), *[str(p) for p in source_files], "--outdir", tmpdir, "--auto"]
        proc = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
        if proc.returncode != 0:
            stderr = (proc.stderr or "").strip()
            stdout = (proc.stdout or "").strip()
            detail = stderr or stdout or f"codigo {proc.returncode}"
            raise RuntimeError(detail)

        json_files = sorted(Path(tmpdir).glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not json_files:
            raise RuntimeError("el lector termino OK pero no genero JSON")

        return json.loads(json_files[0].read_text(encoding="utf-8", errors="replace"))


def _strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


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
    value = re.sub(r"\s+", " ", value).strip(" ._-")
    return value


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
        if 1900 <= int(digits[:4]) <= 2100:
            return digits
        return f"{digits[4:8]}{digits[2:4]}{digits[0:2]}"
    return ""


def _looks_already_renamed(value: str) -> bool:
    name = value.strip()
    if not name:
        return False
    pattern = re.compile(
        r"^.+\s-\s.+\s-\s\d{4}-\d{8}(?:-[A-Z])?(?:\s-\s\d{8})?(?:\s\(\d+\))?$",
        flags=re.IGNORECASE,
    )
    return bool(pattern.match(name))


GENERIC_FILE_TOKENS = {
    "archivo",
    "compra",
    "comprobante",
    "doc",
    "documento",
    "fact",
    "factura",
    "foto",
    "image",
    "img",
    "pdf",
    "rem",
    "remito",
    "scan",
    "ticket",
}


def _cab_pick(cab: dict, *keys: str) -> str:
    for key in keys:
        value = str(cab.get(key) or "").strip()
        if value:
            return value
    return ""


def _infer_tipo_comprobante(cab: dict, data: Optional[dict] = None) -> str:
    direct = _cab_pick(cab, "TipoComprobante", "CONCEPTO")
    if direct:
        return direct

    meta = data.get("meta") if isinstance(data, dict) else None
    raw = ""
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
    return ""


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
    if not normalized:
        return ""

    tokens = [tok for tok in re.split(r"\s+", normalized.casefold()) if tok]
    useful_tokens = [tok for tok in tokens if tok not in GENERIC_FILE_TOKENS]
    if not useful_tokens:
        return ""
    if len(" ".join(useful_tokens)) < 4:
        return ""
    return _clean_piece(" ".join(useful_tokens), keep_accents=keep_accents)


def _resolve_provider_name(cab: dict, file_paths: List[Path], *, keep_accents: bool) -> str:
    provider = _clean_piece(_cab_pick(cab, "Proveedor", "Nombre"), keep_accents=keep_accents)
    if provider:
        return provider

    from_filename = _provider_from_filename(file_paths, keep_accents=keep_accents)
    if from_filename:
        return from_filename

    cuit = _digits_only(_cab_pick(cab, "CUIT", "NUMERO_CUIT"))
    if cuit:
        return f"CUIT_{cuit}"

    return "SIN_PROVEEDOR"


def _build_doc_number(cab: dict) -> str:
    pv = _digits_only(_cab_pick(cab, "PuntoVenta", "SUCURSAL"))
    number = _digits_only(_cab_pick(cab, "Numero", "NUMERO"))
    letter = _clean_piece(_cab_pick(cab, "Letra", "LETRA"), keep_accents=True)

    if pv:
        pv = pv.zfill(4)
    if number:
        number = number.zfill(8)

    parts = [piece for piece in (pv, number, letter) if piece]
    return "-".join(parts)


def _build_folder_name(
    cab: dict,
    file_paths: List[Path],
    data: Optional[dict] = None,
    *,
    keep_accents: bool,
) -> str:
    proveedor = _resolve_provider_name(cab, file_paths, keep_accents=keep_accents)
    tipo = _clean_piece(_infer_tipo_comprobante(cab, data), keep_accents=keep_accents)
    numero = _build_doc_number(cab)
    fecha = _format_date_yyyymmdd(_cab_pick(cab, "Fecha", "FechaSubdiario"))
    parts = [piece for piece in (proveedor, tipo, numero, fecha) if piece]
    return " - ".join(parts)


def _ensure_unique_target(target: Path) -> Path:
    if not target.exists():
        return target
    base = target.name
    for idx in range(2, 1000):
        candidate = target.with_name(f"{base} ({idx})")
        if not candidate.exists():
            return candidate
    raise RuntimeError(f"no se encontro un nombre libre para {target}")


def _append_log(log_path: Path, *, status: str, old_name: str, new_name: str, detail: str) -> None:
    timestamp = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"{timestamp}\t{status}\t{old_name}\t{new_name}\t{detail}\n"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8", errors="replace") as fh:
        fh.write(line)


def _resolve_log_path(root: Path, log_file_name: str) -> Path:
    month_prefix = dt.datetime.now().strftime("%Y%m")
    clean_name = Path(log_file_name).name or "organizar_carpetas_compras.log"
    if not clean_name.lower().endswith(".log"):
        clean_name = f"{clean_name}.log"
    return root / "LOG" / f"{month_prefix}_{clean_name}"


def _group_key_from_page_pattern(stem: str) -> Optional[Tuple[str, int]]:
    patterns = [
        re.compile(r"^(?P<base>.+?)[ _-]*(?P<idx>\d+)de(?P<total>\d+)$", re.IGNORECASE),
        re.compile(r"^(?P<base>.+?)[ _-]*(?:pag|page|hoja)[ _-]*(?P<idx>\d+)(?:de(?P<total>\d+))?$", re.IGNORECASE),
    ]
    for pattern in patterns:
        match = pattern.match(stem)
        if not match:
            continue
        base = match.group("base").rstrip(" _-")
        idx = int(match.group("idx"))
        if base and idx >= 1:
            return base.casefold(), idx
    return None


def _group_key_from_trailing_index(stem: str) -> Optional[Tuple[str, int]]:
    match = re.match(r"^(?P<base>.*[A-Za-z_])(?P<idx>\d{1,2})$", stem)
    if not match:
        return None
    base = match.group("base").rstrip(" _-")
    idx = int(match.group("idx"))
    if not base or idx < 1:
        return None
    return base.casefold(), idx


def _group_loose_files(files: List[Path], max_files: int) -> List[Dict[str, object]]:
    exact_groups: Dict[str, List[Tuple[int, Path]]] = {}
    candidate_groups: Dict[str, List[Tuple[int, Path]]] = {}
    singles: List[Path] = []

    for file_path in files:
        if _looks_already_renamed(file_path.stem):
            singles.append(file_path)
            continue

        stem = file_path.stem
        explicit = _group_key_from_page_pattern(stem)
        if explicit:
            key, idx = explicit
            exact_groups.setdefault(key, []).append((idx, file_path))
            continue

        if file_path.suffix.lower() == ".pdf":
            singles.append(file_path)
            continue

        trailing = _group_key_from_trailing_index(stem)
        if trailing:
            key, idx = trailing
            candidate_groups.setdefault(key, []).append((idx, file_path))
            continue

        singles.append(file_path)

    groups: List[List[Path]] = []
    used_files: set[Path] = set()

    for items in exact_groups.values():
        ordered = [path for _, path in sorted(items, key=lambda item: item[0])]
        if len(ordered) > 1 and len(ordered) <= max_files:
            groups.append(ordered)
            used_files.update(ordered)
        else:
            singles.extend(ordered)

    for items in candidate_groups.values():
        ordered_items = sorted(items, key=lambda item: item[0])
        indexes = [idx for idx, _ in ordered_items]
        ordered = [path for _, path in ordered_items]
        contiguous = indexes == list(range(1, len(indexes) + 1))
        if len(ordered) > 1 and contiguous and len(ordered) <= max_files:
            groups.append(ordered)
            used_files.update(ordered)
        else:
            singles.extend(ordered)

    for file_path in files:
        if file_path not in used_files and file_path not in singles:
            singles.append(file_path)

    for file_path in singles:
        groups.append([file_path])

    out: List[Dict[str, object]] = []
    for paths in groups:
        ordered = sorted(paths, key=lambda p: _natural_sort_key(p.name))
        out.append({"source_names": ", ".join(p.name for p in ordered), "files": ordered})

    return sorted(out, key=lambda item: _natural_sort_key(str(item["source_names"])))


def _build_target_file_names(files: List[Path], base_name: str) -> List[Path]:
    targets: List[Path] = []
    total = len(files)

    if total == 1:
        file_path = files[0]
        targets.append(_ensure_unique_target(file_path.with_name(f"{base_name}{file_path.suffix.lower()}")))
        return targets

    for idx, file_path in enumerate(files, start=1):
        suffix = f" - PAG{idx:02d}DE{total:02d}"
        candidate = file_path.with_name(f"{base_name}{suffix}{file_path.suffix.lower()}")
        targets.append(_ensure_unique_target(candidate))

    return targets


def _rename_group_files(files: List[Path], base_name: str, apply_changes: bool) -> tuple[str, str]:
    targets = _build_target_file_names(files, base_name)
    target_names = ", ".join(target.name for target in targets)

    if not apply_changes:
        return f"preview: renombrar {len(files)} archivo(s)", target_names

    for source, target in zip(files, targets):
        source.rename(target)
    return f"renombrados {len(files)} archivo(s)", target_names


def _log_existing_dirs(root: Path, log_path: Path) -> None:
    dirs = sorted([p for p in root.iterdir() if p.is_dir()], key=lambda p: _natural_sort_key(p.name))
    for folder in dirs:
        if _looks_already_renamed(folder.name):
            detail = "subcarpeta ya ordenada; no se toca"
        else:
            detail = "subcarpeta preexistente; este script solo procesa archivos sueltos en la raiz"
        _append_log(log_path, status="SKIP", old_name=folder.name, new_name=folder.name, detail=detail)


def _run_gui() -> int:
    try:
        import tkinter as tk
        from tkinter import ttk, filedialog, scrolledtext
        import tkinter.messagebox as mb
    except ImportError:
        print("ERROR: tkinter no está disponible. Use parámetros por línea de comandos.")
        return 1

    root_win = tk.Tk()
    root_win.title("Organizar Carpetas Compras")
    root_win.minsize(620, 520)

    var_root = tk.StringVar()
    var_apply = tk.BooleanVar(value=False)
    var_max_files = tk.IntVar(value=5)
    var_pdfs_only = tk.BooleanVar(value=False)
    var_keep_accents = tk.BooleanVar(value=False)
    var_log_file = tk.StringVar(value="organizar_carpetas_compras.log")
    var_limit = tk.IntVar(value=0)

    # --- Formulario ---
    form = ttk.LabelFrame(root_win, text="Parámetros", padding=10)
    form.grid(row=0, column=0, sticky="ew", padx=10, pady=(10, 5))
    form.columnconfigure(1, weight=1)

    ttk.Label(form, text="Carpeta base:").grid(row=0, column=0, sticky="w", pady=3)
    ttk.Entry(form, textvariable=var_root, width=50).grid(row=0, column=1, sticky="ew", padx=(5, 2), pady=3)

    def _browse() -> None:
        folder = filedialog.askdirectory(title="Seleccionar carpeta base")
        if folder:
            var_root.set(folder)

    ttk.Button(form, text="Examinar...", command=_browse).grid(row=0, column=2, padx=(2, 0), pady=3)

    ttk.Checkbutton(form, text="Aplicar cambios reales  (sin esto solo simula)", variable=var_apply).grid(
        row=1, column=0, columnspan=3, sticky="w", pady=2
    )
    ttk.Checkbutton(form, text="Solo PDFs  (ignorar imágenes)", variable=var_pdfs_only).grid(
        row=2, column=0, columnspan=3, sticky="w", pady=2
    )
    ttk.Checkbutton(form, text="Conservar acentos en el nombre final", variable=var_keep_accents).grid(
        row=3, column=0, columnspan=3, sticky="w", pady=2
    )

    num_frame = ttk.Frame(form)
    num_frame.grid(row=4, column=0, columnspan=3, sticky="w", pady=4)
    ttk.Label(num_frame, text="Máx. archivos por grupo:").pack(side="left")
    ttk.Spinbox(num_frame, textvariable=var_max_files, from_=1, to=50, width=6).pack(side="left", padx=(5, 20))
    ttk.Label(num_frame, text="Límite de grupos (0 = sin límite):").pack(side="left")
    ttk.Spinbox(num_frame, textvariable=var_limit, from_=0, to=10000, width=8).pack(side="left", padx=5)

    ttk.Label(form, text="Archivo de log:").grid(row=5, column=0, sticky="w", pady=3)
    ttk.Entry(form, textvariable=var_log_file, width=40).grid(row=5, column=1, columnspan=2, sticky="ew", padx=5, pady=3)

    # --- Salida ---
    out_frame = ttk.LabelFrame(root_win, text="Salida", padding=5)
    out_frame.grid(row=1, column=0, sticky="nsew", padx=10, pady=5)
    txt = scrolledtext.ScrolledText(out_frame, width=80, height=18, state="disabled", wrap="word")
    txt.pack(fill="both", expand=True)

    # --- Botones ---
    btn_frame = ttk.Frame(root_win, padding=(10, 0, 10, 10))
    btn_frame.grid(row=2, column=0, sticky="ew")

    result_code = [0]

    def _append(text: str) -> None:
        txt.configure(state="normal")
        txt.insert("end", text)
        txt.see("end")
        txt.configure(state="disabled")

    class _GuiStream:
        def write(self, text: str) -> None:
            if text:
                root_win.after(0, _append, text)

        def flush(self) -> None:
            pass

    def _run() -> None:
        root_val = var_root.get().strip()
        if not root_val:
            mb.showerror("Error", "Debe seleccionar la carpeta base.")
            return

        argv: List[str] = [root_val]
        if var_apply.get():
            argv.append("--apply")
        argv += ["--max-files", str(var_max_files.get())]
        if var_pdfs_only.get():
            argv.append("--include-pdfs-only")
        if var_keep_accents.get():
            argv.append("--keep-accents")
        argv += ["--log-file", var_log_file.get().strip() or "organizar_carpetas_compras.log"]
        argv += ["--limit", str(var_limit.get())]

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
    root_win.rowconfigure(1, weight=1)
    root_win.mainloop()
    return result_code[0]


def main(argv: Optional[List[str]] = None) -> int:
    if argv is None and not sys.argv[1:]:
        return _run_gui()
    args = _parse_args(argv)
    root = Path(args.root).expanduser().resolve()
    if getattr(sys, "frozen", False):
        reader_script = Path(sys.executable).resolve().with_name("lector_facturas_to_json_v5.exe")
    else:
        reader_script = Path(__file__).resolve().with_name("lector_facturas_to_json_v5.py")
    log_path = _resolve_log_path(root, args.log_file)

    if not root.exists() or not root.is_dir():
        print(f"ERROR: no existe la carpeta base: {root}")
        return 1
    if not reader_script.exists():
        print(f"ERROR: no se encontro el lector requerido: {reader_script}")
        return 1
    if args.max_files < 1:
        print("ERROR: --max-files debe ser mayor o igual a 1")
        return 1

    _log_existing_dirs(root, log_path)

    loose_files = _collect_supported_files(root, args.include_pdfs_only)
    if not loose_files:
        print("No se encontraron PDF/imagenes sueltos para analizar.")
        print(f"Log: {log_path}")
        return 0

    groups = _group_loose_files(loose_files, args.max_files)
    if args.limit > 0:
        groups = groups[: args.limit]
    ok = 0
    skipped = 0
    errors = 0

    for group in groups:
        files = list(group["files"])
        source_names = str(group["source_names"])

        if len(files) > args.max_files:
            print(f"[SKIP] {source_names}: supera --max-files={args.max_files}")
            _append_log(
                log_path,
                status="SKIP",
                old_name=source_names,
                new_name=source_names,
                detail=f"grupo supera max-files={args.max_files}",
            )
            skipped += 1
            continue

        if any(_looks_already_renamed(file_path.stem) for file_path in files):
            print(f"[ERROR] {source_names}: hay archivo(s) con nombre final aparente; se deja revision manual")
            _append_log(
                log_path,
                status="ERROR",
                old_name=source_names,
                new_name=source_names,
                detail="archivo suelto ya renombrado detectado; no se procesa automaticamente",
            )
            errors += 1
            continue

        try:
            data = _load_json_from_reader(reader_script, files)
            cab = data.get("CAB") if isinstance(data, dict) else None
            if not isinstance(cab, dict):
                raise RuntimeError("el JSON no contiene CAB")

            new_name = _build_folder_name(cab, files, data, keep_accents=args.keep_accents)
            if not new_name:
                raise RuntimeError("no se pudo construir nombre con proveedor/tipo/numero/fecha")

            proveedor = _resolve_provider_name(cab, files, keep_accents=args.keep_accents)
            numero = _build_doc_number(cab)
            if not proveedor:
                raise RuntimeError("faltó proveedor en la extracción")
            if not numero:
                raise RuntimeError("faltó punto de venta/numero/letra en la extracción")

            action, final_target = _rename_group_files(files, new_name, args.apply)
            print(f"[OK] {source_names}: {action}")
            _append_log(
                log_path,
                status="OK",
                old_name=source_names,
                new_name=final_target,
                detail=action,
            )
            ok += 1
        except Exception as exc:
            print(f"[ERROR] {source_names}: {exc}")
            _append_log(
                log_path,
                status="ERROR",
                old_name=source_names,
                new_name=source_names,
                detail=str(exc),
            )
            errors += 1

    mode = "APLICADO" if args.apply else "SIMULACION"
    print("")
    print(f"Resumen {mode}: OK={ok} SKIP={skipped} ERROR={errors}")
    print(f"Log: {log_path}")
    return 0 if errors == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
