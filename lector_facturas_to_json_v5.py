# -*- coding: utf-8 -*-
r"""
lector_facturas_to_json_v5.py

Lee 1 a 10 páginas (JPG/PNG/WEBP o PDF) y genera un .json normalizado con los
datos de la factura. La extracción puede realizarse por layout (sin IA) o
delegando al backend remoto de IA. Al finalizar OK imprime SOLO la ruta del
JSON por stdout (contrato VB6). En error sale con código != 0 y escribe el
detalle en stderr.

Requisitos:
  pip install python-dotenv
  pip install pillow          # para --tile y redimensionado de imágenes
  pip install pypdf           # para PDFs multipágina (--per-page / --auto)
  pip install pdfplumber pytesseract pymupdf  # para extracción por layout

Uso básico:
  python lector_facturas_to_json_v5.py factura.pdf --outdir E:\temp
  python lector_facturas_to_json_v5.py fac1.jpg fac2.jpg --outdir E:\temp --gui
  python lector_facturas_to_json_v5.py factura.pdf --layout-file C:\layouts\lyt_42.json
  python lector_facturas_to_json_v5.py factura.pdf --proveedor

EXE (PyInstaller):
  pyinstaller --onefile --noconsole lector_facturas_to_json_v5.py

Parámetros
----------
Archivos de entrada (posicional):
  files                   1 a 10 archivos JPG/PNG/WEBP/PDF en orden de páginas.

Entrada / salida:
  --outdir DIR            Carpeta de salida. Default: carpeta TEMP del sistema.
  --prompt-file FILE      Archivo .txt con prompt personalizado (reemplaza el
                          prompt por defecto; no aplica con --proveedor).

Estrategia de extracción:
  --layout-file FILE      Ruta al archivo JSON con el layout del proveedor.
                          Si la extracción es confiable, guarda el JSON sin
                          llamar a IA. Si falla o el resultado es insuficiente,
                          continúa con IA. Incompatible con --proveedor.
  --proveedor             Modo reducido: extrae solo codigo_proveedor / cuit /
                          nombre_proveedor. Usa un prompt corto y procesa solo
                          el primer archivo. Incompatible con --layout-proveedor.

Modelo IA:
  --model MODEL           Modelo principal. Default: gpt-4.1-mini.
  --fallback-model MODEL  Modelo de reintento si --model no alcanza o el
                          resultado parece incompleto. Default: gpt-4.1.
  --no-fallback           Desactiva el reintento automático con --fallback-model.

Backend / transporte:
  --idcliente N           Id de cliente (entero) para auditoría en el backend.
                          Se copia a IA_IDCLIENTE y a IDCLIENTE.
  --backend-url URL       Override de IA_BACKEND_URL.
  --backend-route RUTA    Override de IA_BACKEND_ROUTE.
  --client-id ID          Override de IA_CLIENT_ID.
  --client-secret SECRET  Override de IA_CLIENT_SECRET.
  --ia-task TAREA         Override de IA_TASK / opcion.

Procesamiento de páginas:
  --per-page              Procesa cada archivo/página por separado con IA y
                          luego unifica las filas. Mejora extracción en tablas
                          largas o facturas multipágina.
  --auto                  Activa per-page y ajusta --tile automáticamente según
                          cantidad de páginas (1 pág → tile 3; 2-3 → tile 4
                          per-page; 4+ → tile 5 per-page). También se activa
                          automáticamente cuando se reciben varios archivos sin
                          --per-page ni --tile explícito.
  --tile N                Divide cada imagen en N franjas horizontales
                          solapadas antes de enviarla a IA. Rango: 1 a 6.
                          Requiere Pillow. Solo afecta imágenes (no PDFs).
                          Default: 1 (sin división).

Entorno:
  --env-file FILE         Archivo .env alternativo (útil para pruebas).
  --no-local-env          No carga el .env que está junto al exe/script.

Interfaz:
  --gui                   Muestra ventana de progreso con estado, barra y log
                          (Tkinter). No altera stdout.
"""

from __future__ import annotations

import argparse
import io
import base64
import datetime as dt
import json
import os
import re
import sys
import tempfile
import threading
import time
import queue
import concurrent.futures
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from ia_backend_transport import backend_enabled, call_backend

try:
    from pypdf import PdfReader, PdfWriter
except Exception:
    PdfReader = None
    PdfWriter = None

try:
    import fitz as _fitz  # pymupdf — fallback para expansión de PDFs
except Exception:
    _fitz = None


MAX_INPUT_FILES = 10


# ----------------------------
# GUI (Tkinter) opcional
# ----------------------------
try:
    import tkinter as tk
    from tkinter import ttk
except Exception:
    tk = None
    ttk = None

try:
    from PIL import Image
except Exception:
    Image = None


class StatusUI:
    """Ventana simple: estado + barra indeterminada + tiempo + log.
    NO escribe en stdout (para no romper VB6).
    """

    def __init__(self, title="Procesando factura...", width=560, height=260):
        if tk is None or ttk is None:
            raise RuntimeError("Tkinter no está disponible en este entorno.")

        self.q: "queue.Queue[str]" = queue.Queue()
        self.t0 = time.time()

        self.root = tk.Tk()
        self.root.title(title)
        # Center window on screen
        self.root.update_idletasks()
        sw = self.root.winfo_screenwidth()
        sh = self.root.winfo_screenheight()
        x = max(0, int((sw - width) / 2))
        y = max(0, int((sh - height) / 2))
        self.root.geometry(f"{width}x{height}+{x}+{y}")
        self.root.resizable(False, False)

        self.lbl = ttk.Label(self.root, text="Iniciando...", font=("Segoe UI", 10))
        self.lbl.pack(padx=12, pady=(12, 4), anchor="w")

        self.lbl_time = ttk.Label(self.root, text="Tiempo: 00:00", font=("Segoe UI", 9))
        self.lbl_time.pack(padx=12, pady=(0, 6), anchor="w")

        self.pb = ttk.Progressbar(self.root, mode="indeterminate")
        self.pb.pack(fill="x", padx=12, pady=(0, 10))
        self.pb.start(10)

        self.txt = tk.Text(self.root, height=9, wrap="word")
        self.txt.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self.txt.configure(state="disabled")

        self.actions = ttk.Frame(self.root)
        self.actions.pack(fill="x", padx=12, pady=(0, 12))
        self.retry_btn = ttk.Button(self.actions, text="Reintentar", command=self._handle_retry, state="disabled")
        self.retry_btn.pack(side="right")
        self.close_btn = ttk.Button(self.actions, text="Cerrar", command=self.close)
        self.close_btn.pack(side="right", padx=(0, 8))

        self._closed = False
        self._finished = False
        self._retry_callback = None
        self._time_after_id = None
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self.root.after(100, self._poll)
        self._time_after_id = self.root.after(200, self._tick_time)

    def _on_close(self):
        if self._finished:
            self.close()
            return

        # Si cierran la ventana, no matamos el proceso; solo ocultamos.
        self._closed = True
        try:
            self.root.withdraw()
        except Exception:
            pass

    def _stop_timers_and_progress(self):
        if self._time_after_id is not None:
            try:
                self.root.after_cancel(self._time_after_id)
            except Exception:
                pass
            self._time_after_id = None
        try:
            self.pb.stop()
        except Exception:
            pass

    def _tick_time(self):
        if not self._closed:
            secs = int(time.time() - self.t0)
            mm = secs // 60
            ss = secs % 60
            self.lbl_time.configure(text=f"Tiempo: {mm:02d}:{ss:02d}")
            self._time_after_id = self.root.after(200, self._tick_time)

    def push(self, msg: str):
        """Seguro desde cualquier hilo."""
        try:
            self.q.put_nowait(msg)
        except Exception:
            pass

    def _poll(self):
        try:
            while True:
                msg = self.q.get_nowait()
                if msg.startswith("STATUS:"):
                    self.lbl.configure(text=msg.replace("STATUS:", "", 1).strip())
                else:
                    self._append_log(msg)
        except queue.Empty:
            pass

        if not self._closed:
            self.root.after(120, self._poll)

    def _append_log(self, s: str):
        self.txt.configure(state="normal")
        self.txt.insert("end", s + "\n")
        self.txt.see("end")
        self.txt.configure(state="disabled")

    def close(self):
        self._closed = True
        self._stop_timers_and_progress()
        try:
            self.root.destroy()
        except Exception:
            pass

    def mainloop(self):
        self.root.mainloop()

    def finish(self, status_text: str, keep_open_seconds: float = 2.5):
        """Muestra estado final unos segundos para que alcance a verse."""
        self._finished = True
        self.push(f"STATUS:{status_text}")
        self._stop_timers_and_progress()
        time.sleep(max(0.0, keep_open_seconds))
        self.close()

    def freeze(self, status_text: str) -> None:
        self._finished = True
        self._stop_timers_and_progress()
        self._closed = False
        self.push(f"STATUS:{status_text}")
        try:
            self.root.after(0, self.root.deiconify)
        except Exception:
            pass

    def set_retry_callback(self, callback) -> None:
        self._retry_callback = callback

    def _handle_retry(self):
        if callable(self._retry_callback):
            self.set_retry_enabled(False)
            self.clear_log()
            self.reset_progress("Reintentando...")
            self._retry_callback()

    def set_retry_enabled(self, enabled: bool) -> None:
        def _apply():
            if self._closed:
                return
            self.retry_btn.configure(state="normal" if enabled else "disabled")

        try:
            self.root.after(0, _apply)
        except Exception:
            pass

    def clear_log(self) -> None:
        def _clear():
            if self._closed:
                return
            self.txt.configure(state="normal")
            self.txt.delete("1.0", "end")
            self.txt.configure(state="disabled")

        try:
            self.root.after(0, _clear)
        except Exception:
            pass

    def reset_progress(self, status_text: str = "Iniciando...") -> None:
        def _reset():
            if self._closed:
                return
            self._finished = False
            self.t0 = time.time()
            self.lbl.configure(text=status_text)
            self.lbl_time.configure(text="Tiempo: 00:00")
            self._stop_timers_and_progress()
            try:
                self.pb.start(10)
            except Exception:
                pass
            self._time_after_id = self.root.after(200, self._tick_time)

        try:
            self.root.after(0, _reset)
        except Exception:
            pass


# ----------------------------
# Utilidades generales
# ----------------------------
def app_dir() -> Path:
    """Carpeta base del .py o del .exe (cuando está 'frozen')."""
    if getattr(sys, "frozen", False) and hasattr(sys, "_MEIPASS"):
        # En PyInstaller, el ejecutable real está en sys.executable
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def load_env_near_app() -> None:
    """Carga .env desde la carpeta del script/exe si existe."""
    env_path = app_dir() / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=str(env_path), override=False)
    else:
        # igual intentamos por si hay .env en cwd
        load_dotenv(override=False)


def apply_runtime_env_overrides(args: argparse.Namespace) -> None:
    if args.backend_url:
        os.environ["IA_BACKEND_URL"] = args.backend_url.strip()
    if args.backend_route:
        os.environ["IA_BACKEND_ROUTE"] = args.backend_route.strip()
    if args.client_id:
        os.environ["IA_CLIENT_ID"] = args.client_id.strip()
    if args.client_secret:
        os.environ["IA_CLIENT_SECRET"] = args.client_secret.strip()
    if args.ia_task:
        os.environ["IA_TASK"] = args.ia_task.strip()


def safe_basename(file_path: str) -> str:
    name = Path(file_path).stem
    name = re.sub(r"[^a-zA-Z0-9_\-]+", "_", name).strip("_")
    return name or "factura"

def sanitize_json_text(s: str) -> str:
    s = s.strip()
    if s.startswith("﻿"):
        s = s.lstrip("﻿")
    # remove trailing commas before } or ]
    s = re.sub(r",\s*([}\]])", r"\1", s)
    return s



def extract_first_json(text: str) -> dict:
    """Extrae el primer JSON v?lido del texto (tolerante a basura alrededor)."""
    if not text:
        raise ValueError("Respuesta vac?a del modelo.")

    # ya es JSON puro
    s = text.strip()
    if s.startswith("{") and s.endswith("}"):
        try:
            return json.loads(s)
        except json.JSONDecodeError:
            return json.loads(sanitize_json_text(s))

    # buscar bloque entre llaves (simple y efectivo)
    start = s.find("{")
    end = s.rfind("}")
    if start >= 0 and end > start:
        candidate = s[start : end + 1]
        try:
            return json.loads(candidate)
        except json.JSONDecodeError:
            return json.loads(sanitize_json_text(candidate))

    raise ValueError("No se pudo extraer JSON de la respuesta.")


def _ensure_object(x: Any) -> dict:
    return x if isinstance(x, dict) else {}


def _ensure_list(x: Any) -> list:
    return x if isinstance(x, list) else []


def _normalize_outdir_arg(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    if len(s) >= 2 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
        s = s[1:-1].strip()
    s = s.rstrip(" '\"")
    return s


def _normalize_cli_file_args(file_args: List[str]) -> List[str]:
    """Recompone rutas con espacios cuando la shell las separó en varios tokens."""
    def _clean_part(value: Any) -> str:
        return str(value).strip().strip('"').strip("'")

    normalized: List[str] = []
    i = 0
    total = len(file_args)
    while i < total:
        token = _clean_part(file_args[i])
        best_match: Optional[str] = None
        best_index = i

        candidate = token
        if candidate and Path(candidate).exists():
            best_match = candidate

        for j in range(i + 1, total):
            candidate = f"{candidate} {_clean_part(file_args[j])}".strip()
            if Path(candidate).exists():
                best_match = candidate
                best_index = j

        if best_match:
            normalized.append(best_match)
            i = best_index + 1
        else:
            normalized.append(token)
            i += 1

    return normalized


def _count_pdf_pages(file_path: str) -> int:
    if PdfReader is not None:
        try:
            return max(1, len(PdfReader(file_path).pages))
        except Exception:
            pass
    if _fitz is not None:
        try:
            doc = _fitz.open(file_path)
            n = doc.page_count
            doc.close()
            if n > 0:
                return n
        except Exception:
            pass
    return 1


def _expand_pdf_inputs(file_paths: List[str], temp_dir: str) -> tuple[List[str], List[str]]:
    """Convierte PDFs multipagina en PDFs temporales de 1 hoja para procesarlos en orden."""
    expanded: List[str] = []
    created_temp_files: List[str] = []

    for file_path in file_paths:
        ext = Path(file_path).suffix.lower()
        if ext != ".pdf":
            expanded.append(file_path)
            continue

        total_pages = _count_pdf_pages(file_path)
        if total_pages <= 1:
            expanded.append(file_path)
            continue

        stem = Path(file_path).stem

        if PdfReader is not None and PdfWriter is not None:
            # Ruta preferida: split con pypdf (genera PDFs de 1 página)
            try:
                reader = PdfReader(file_path)
                for page_index in range(total_pages):
                    writer = PdfWriter()
                    writer.add_page(reader.pages[page_index])
                    temp_page = Path(temp_dir) / f"{stem}__page_{page_index + 1:03d}.pdf"
                    with temp_page.open("wb") as fh:
                        writer.write(fh)
                    expanded.append(str(temp_page))
                    created_temp_files.append(str(temp_page))
                continue
            except Exception:
                pass  # Caer al fallback fitz

        if _fitz is not None:
            # Fallback: renderizar cada página como imagen PNG con fitz
            try:
                doc = _fitz.open(file_path)
                mat = _fitz.Matrix(2.0, 2.0)  # ~144 dpi
                for page_index in range(doc.page_count):
                    pix = doc[page_index].get_pixmap(matrix=mat)
                    temp_img = Path(temp_dir) / f"{stem}__page_{page_index + 1:03d}.png"
                    pix.save(str(temp_img))
                    expanded.append(str(temp_img))
                    created_temp_files.append(str(temp_img))
                doc.close()
                continue
            except Exception:
                pass

        raise SystemExit(
            "ERROR: Para procesar todas las hojas de un PDF multipagina necesitás instalar pypdf o pymupdf."
        )

    return expanded, created_temp_files


# ----------------------------
# Esquema esperado
# ----------------------------
CAB_KEYS = [
    "CUENTA",
    "Nombre",
    "DOMICILIO",
    "LOCALIDAD",
    "CODIGOPOSTAL",
    "IDPROVINCIA",
    "TELEFONO",
    "DOCUMENTOTIPO",
    "NUMERO_CUIT",
    "CONDICIONIVA",
    "SUCURSAL",
    "NUMERO",
    "LETRA",
    "FechaSubdiario",
    "CONCEPTO",
    "Fecha",
    "Vencimiento",
    "Vigencia",
    "FHVToCAI",
    "NROCAI",
]

ROW_KEYS = [
    "Cantidad",
    "Codigo_Articulo",
    "Descripcion",
    "UD",
    "Importe_Lista",
    "% Dto1",
    "% Dto2",
    "Importe_Neto",
    "IVA",
    "Impuestos internos",
    "Bl/Pq",
    "Moneda",
    "Total",
    "AuxNroLote",
    "AuxNroSerie",
]

TOTALES_KEYS = [
    "Neto gravado",
    "Neto no gravado",
    "Exento",
    "IVA 21%",
    "IVA 10.5%",
    "IVA 27%",
    "IVA",
    "Otros",
    "Percepcion IVA",
    "Percepcion IIBB",
    "Percepcion Ganancias",
    "Impuestos internos",
    "Ing Brutos",
    "Otros impuestos",
    "Subtotal",
    "Total",
    "Total final",
    "Moneda",
]

META_KEYS = [
    "comprobante_raw",
    "moneda_detectada",
    "observaciones",
    "totales_raw",
    "orden_columnas",
]


def normalize_schema(data: dict) -> dict:
    data = _ensure_object(data)

    cab = _ensure_object(data.get("CAB"))
    rows = _ensure_list(data.get("ROWS"))
    tot = _ensure_object(data.get("TOTALES"))
    meta = _ensure_object(data.get("meta"))

    data["CAB"] = cab
    data["ROWS"] = rows
    data["TOTALES"] = tot
    data["meta"] = meta

    data["CAB"] = {k: cab.get(k, "") for k in CAB_KEYS}

    norm_rows = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        for k in ROW_KEYS:
            r.setdefault(k, "")
        norm_rows.append(r)
    data["ROWS"] = norm_rows

    for k in TOTALES_KEYS:
        tot.setdefault(k, "")

    otros = tot.get("Otros")
    if not isinstance(otros, list):
        tot["Otros"] = [{"Etiqueta": "", "Importe_Neto": ""}]
    else:
        norm_otros = []
        for it in otros:
            if not isinstance(it, dict):
                continue
            it.setdefault("Etiqueta", "")
            it.setdefault("Importe_Neto", "")
            norm_otros.append(it)
        tot["Otros"] = norm_otros or [{"Etiqueta": "", "Importe_Neto": ""}]

    for k in META_KEYS:
        if k == "orden_columnas":
            v = meta.get(k)
            if not isinstance(v, list):
                meta[k] = []
        else:
            meta.setdefault(k, "")

    return data


def normalize_provider_only_schema(data: dict) -> dict:
    data = _ensure_object(data)
    out = {
        "codigo_proveedor": "",
        "cuit": "",
        "nombre_proveedor": "",
    }
    for key in out:
        value = data.get(key, "")
        out[key] = str(value).strip() if value is not None else ""
    return out


def infer_orden_columnas(data: dict) -> None:
    """Completa meta.orden_columnas si viene vacío.
    Intenta tomar "Detalle: ..." desde meta.comprobante_raw u observaciones.
    """
    try:
        meta = _ensure_object(data.get("meta"))
        data["meta"] = meta

        oc = meta.get("orden_columnas")
        if isinstance(oc, list) and oc:
            return

        rows = data.get("ROWS") or []
        row_keys: List[str] = []
        for r in rows:
            if isinstance(r, dict) and any(str(v).strip() for v in r.values()):
                row_keys = list(r.keys())
                break
        if not row_keys and rows and isinstance(rows[0], dict):
            row_keys = list(rows[0].keys())

        if not row_keys:
            meta["orden_columnas"] = []
            return

        key_lookup = {k.lower(): k for k in row_keys}

        def pick(*cands):
            for c in cands:
                kk = c.lower()
                if kk in key_lookup:
                    return key_lookup[kk]
            return None

        def norm(s: str) -> str:
            s = s.lower().strip()
            s = (
                s.replace("á", "a")
                .replace("é", "e")
                .replace("í", "i")
                .replace("ó", "o")
                .replace("ú", "u")
                .replace("ñ", "n")
            )
            s = re.sub(r"\s+", " ", s)
            return s

        raw = str(meta.get("comprobante_raw") or "")
        header_part = ""
        m = re.search(r"(?i)\bdetalle\s*:\s*([^\n\r]+)", raw)
        if m:
            header_part = m.group(1).strip()
        if not header_part:
            obs = str(meta.get("observaciones") or "")
            m2 = re.search(r"(?i)\bdetalle\s*:\s*([^\n\r]+)", obs)
            if m2:
                header_part = m2.group(1).strip()

        ordered: List[str] = []
        dto_seen = 0

        if header_part:
            parts = re.split(r"[,\|;]+", header_part)
            tokens = [p.strip() for p in parts if p.strip()]

            for t in tokens:
                nt = norm(t)
                k = None

                if "cant" in nt or "cantidad" in nt:
                    k = pick("Cantidad")
                elif "artic" in nt or "cod" in nt or "codigo" in nt or "producto" in nt:
                    k = pick("Codigo_Articulo")
                elif "descripcion" in nt or nt == "desc":
                    k = pick("Descripcion")
                elif nt in ("ud", "unidad", "u."):
                    k = pick("UD")
                elif "dto" in nt or "descuento" in nt or "%" in nt:
                    dto_seen += 1
                    k = pick("% Dto1" if dto_seen == 1 else "% Dto2")
                elif "lista" in nt:
                    k = pick("Importe_Lista")
                elif "neto" in nt or "precio" in nt:
                    k = pick("Importe_Neto", "Importe_Lista")
                elif "iva" in nt:
                    k = pick("IVA")
                elif "impuestos internos" in nt or "imp internos" in nt:
                    k = pick("Impuestos internos")
                elif "bl/pq" in nt or "bulto" in nt:
                    k = pick("Bl/Pq")
                elif "moneda" in nt:
                    k = pick("Moneda")
                elif "total" in nt:
                    k = pick("Total")
                elif "lote" in nt:
                    k = pick("AuxNroLote")
                elif "serie" in nt:
                    k = pick("AuxNroSerie")

                if k and k not in ordered:
                    ordered.append(k)

        if not ordered:
            common = [
                "Cantidad",
                "Codigo_Articulo",
                "Descripcion",
                "Importe_Lista",
                "% Dto1",
                "% Dto2",
                "Importe_Neto",
                "Total",
            ]
            for k in common:
                if k in row_keys and k not in ordered:
                    ordered.append(k)

        meta["orden_columnas"] = ordered if ordered else []
    except Exception:
        data.setdefault("meta", {})
        if not isinstance(data["meta"].get("orden_columnas"), list):
            data["meta"]["orden_columnas"] = []


def _is_empty_value(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return v.strip() == ""
    if isinstance(v, (list, tuple, dict)):
        return len(v) == 0
    return False

def _parse_number(raw: Any) -> Optional[float]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    # keep digits, comma, dot, minus
    s = re.sub(r"[^\d,.\-]", "", s)
    if not s:
        return None
    # decide decimal separator
    if "," in s and "." in s:
        # decide by last separator (decimal usually last)
        if s.rfind(".") > s.rfind(","):
            # dot decimal, comma thousands
            s = s.replace(",", "")
        else:
            # comma decimal, dot thousands
            s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def _extract_int(raw: Any) -> Optional[int]:
    if raw is None:
        return None
    m = re.search(r"\d+", str(raw))
    if not m:
        return None
    try:
        return int(m.group(0))
    except Exception:
        return None

def _format_number_ar(val: float, decimals: int = 3) -> str:
    s = f"{val:,.{decimals}f}"
    # python uses comma as thousands and dot as decimal -> swap
    s = s.replace(",", "X").replace(".", ",").replace("X", ".")
    return s

def adjust_importe_lista_for_bultos(data: dict) -> None:
    """Proveedor CAFES LA VIRGINIA: Importe_Lista = Total / (Cantidad * Bl/Pq) when mismatch.
    This guards against OCR picking UNIT BRUTO instead of U.NETO.
    """
    try:
        cab = _ensure_object(data.get("CAB"))
        proveedor = str(cab.get("Nombre") or "").upper()
        if "LA VIRGINIA" not in proveedor:
            return

        rows = _ensure_list(data.get("ROWS"))
        for r in rows:
            if not isinstance(r, dict):
                continue
            total = _parse_number(r.get("Total"))
            cant = _parse_number(r.get("Cantidad"))
            blpq = _extract_int(r.get("Bl/Pq")) or 0
            if not total or not cant or blpq <= 0:
                continue

            implied = total / (cant * blpq)
            current = _parse_number(r.get("Importe_Lista"))
            # overwrite if empty or far from implied (>2%)
            if current is None or abs(current - implied) / implied > 0.02:
                r["Importe_Lista"] = _format_number_ar(implied, decimals=3)
    except Exception:
        return

def _parse_expected_items(meta: dict) -> Optional[int]:
    for k in ("comprobante_raw", "observaciones", "totales_raw"):
        txt = str(meta.get(k) or "")
        m = re.search(r"(?i)cantidad\s+de\s+items\s*[:\-]?\s*(\d+)", txt)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass
    return None

def validate_totals_integrity(data: dict, tolerance: float = 0.03) -> None:
    """Validate sum(ROWS.Total) against Neto gravado or Total.
    Adds warning into meta.observaciones if mismatch exceeds tolerance.
    """
    try:
        tot = _ensure_object(data.get("TOTALES"))
        meta = _ensure_object(data.get("meta"))
        rows = _ensure_list(data.get("ROWS"))

        row_sum = 0.0
        rows_count = 0
        for r in rows:
            if not isinstance(r, dict):
                continue
            v = _parse_number(r.get("Total"))
            if v is None:
                continue
            row_sum += v
            rows_count += 1

        if rows_count == 0:
            return

        target = _parse_number(tot.get("Neto gravado"))
        if target is None:
            target = _parse_number(tot.get("Total"))
        if target is None or target == 0:
            return

        diff = abs(row_sum - target) / target
        if diff > tolerance:
            msg = (
                f"ADVERTENCIA: suma de ROWS.Total ({_format_number_ar(row_sum, 2)}) "
                f"no coincide con Neto/Total ({_format_number_ar(target, 2)}). "
                f"Desvío {diff*100:.2f}%."
            )
            obs = str(meta.get("observaciones") or "")
            meta["observaciones"] = (obs + " | " if obs else "") + msg

        exp = _parse_expected_items(meta)
        if exp is not None and rows_count < exp:
            msg = f"ADVERTENCIA: filas detectadas {rows_count} < cantidad de items {exp}."
            obs = str(meta.get("observaciones") or "")
            meta["observaciones"] = (obs + " | " if obs else "") + msg

        data["meta"] = meta
    except Exception:
        return

def needs_model_fallback(data: dict) -> tuple[bool, str]:
    """Decide si conviene reintentar con un modelo mas fuerte."""
    try:
        rows = _ensure_list(data.get("ROWS"))
        rows_count = sum(1 for r in rows if isinstance(r, dict))
        if rows_count == 0:
            return True, "Sin filas detectadas"

        meta = _ensure_object(data.get("meta"))
        exp = _parse_expected_items(meta)
        if exp is not None and rows_count < exp:
            return True, f"Filas detectadas {rows_count} < items esperados {exp}"

        obs = str(meta.get("observaciones") or "")
        if "ADVERTENCIA: suma de ROWS.Total" in obs:
            return True, "Desvio alto entre suma de filas y totales"

        return False, ""
    except Exception:
        return False, ""

def merge_data_keep_best(datas: List[dict]) -> dict:
    """Merge multiple page-level results into a single invoice.
    - CAB: keep first non-empty per key
    - ROWS: concat
    - TOTALES: prefer last non-empty per key
    - meta: keep first non-empty, except totales_raw (prefer last) and orden_columnas (first non-empty list)
    """
    if not datas:
        return {}

    out = {"CAB": {}, "ROWS": [], "TOTALES": {}, "meta": {}}

    for d in datas:
        cab = _ensure_object(d.get("CAB"))
        rows = _ensure_list(d.get("ROWS"))
        tot = _ensure_object(d.get("TOTALES"))
        meta = _ensure_object(d.get("meta"))

        # CAB: keep first non-empty
        for k, v in cab.items():
            if _is_empty_value(out["CAB"].get(k)) and not _is_empty_value(v):
                out["CAB"][k] = v
            elif k not in out["CAB"]:
                out["CAB"][k] = out["CAB"].get(k, v)

        # ROWS: concat
        out["ROWS"].extend(rows)

        # TOTALES: prefer last non-empty
        for k, v in tot.items():
            if not _is_empty_value(v):
                out["TOTALES"][k] = v
            elif k not in out["TOTALES"]:
                out["TOTALES"][k] = v

        # meta: first non-empty, except totales_raw (last), orden_columnas (first non-empty list)
        if "orden_columnas" in meta:
            oc = meta.get("orden_columnas")
            if isinstance(oc, list) and oc and not out["meta"].get("orden_columnas"):
                out["meta"]["orden_columnas"] = oc
        for k, v in meta.items():
            if k == "orden_columnas":
                continue
            if k == "totales_raw":
                if not _is_empty_value(v):
                    out["meta"][k] = v
                elif k not in out["meta"]:
                    out["meta"][k] = v
                continue
            if _is_empty_value(out["meta"].get(k)) and not _is_empty_value(v):
                out["meta"][k] = v
            elif k not in out["meta"]:
                out["meta"][k] = v

    return out


def _normalize_invoice_match_value(value: Any) -> str:
    s = str(value or "").strip().upper()
    s = re.sub(r"\s+", " ", s)
    return s


def _invoice_identity_snapshot(data: dict) -> Dict[str, str]:
    cab = _ensure_object(data.get("CAB"))
    return {
        "NUMERO_CUIT": _normalize_invoice_match_value(cab.get("NUMERO_CUIT")),
        "SUCURSAL": _normalize_invoice_match_value(cab.get("SUCURSAL")),
        "NUMERO": _normalize_invoice_match_value(cab.get("NUMERO")),
        "LETRA": _normalize_invoice_match_value(cab.get("LETRA")),
        "Fecha": _normalize_invoice_match_value(cab.get("Fecha")),
        "Nombre": _normalize_invoice_match_value(cab.get("Nombre")),
    }


def detect_mismatched_invoice_pages(page_results: List[dict], page_files: List[str]) -> tuple[List[dict], List[str]]:
    """Conserva la primera factura detectada y omite páginas con identificadores incompatibles."""
    if not page_results:
        return [], []

    accepted = [page_results[0]]
    warnings: List[str] = []
    base_snapshot = _invoice_identity_snapshot(page_results[0])

    for idx, data in enumerate(page_results[1:], start=1):
        current_snapshot = _invoice_identity_snapshot(data)
        conflicts: List[str] = []
        for key in ("NUMERO_CUIT", "SUCURSAL", "NUMERO", "LETRA", "Fecha"):
            base_value = base_snapshot.get(key) or ""
            current_value = current_snapshot.get(key) or ""
            if base_value and current_value and base_value != current_value:
                conflicts.append(f"{key}: '{base_value}' vs '{current_value}'")

        if conflicts:
            file_name = Path(page_files[idx]).name
            warnings.append(
                f"Se omite '{file_name}' por no coincidir con la primera factura ({'; '.join(conflicts)})."
            )
            continue

        accepted.append(data)

        for key, value in current_snapshot.items():
            if not base_snapshot.get(key) and value:
                base_snapshot[key] = value

    return accepted, warnings



def dedupe_rows(rows: List[dict]) -> List[dict]:
    seen = set()
    out: List[dict] = []
    for r in rows:
        if not isinstance(r, dict):
            continue
        key = (
            str(r.get("Codigo_Articulo", "")).strip(),
            str(r.get("Descripcion", "")).strip(),
            str(r.get("Cantidad", "")).strip(),
            str(r.get("Importe_Neto", "")).strip(),
            str(r.get("Total", "")).strip(),
        )
        if not any(key):
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(r)
    return out


# ----------------------------
# Prompt
# ----------------------------
DEFAULT_PROMPT = r"""

Vas a analizar 1 a 10 páginas de una factura / comprobante de compra.
Respondé **SOLO** con JSON válido (sin texto adicional).

El JSON debe tener ESTE formato fijo (NO elimines claves):

{
  "CAB": {
    "CUENTA": "",
    "Nombre": "",
    "DOMICILIO": "",
    "LOCALIDAD": "",
    "CODIGOPOSTAL": "",
    "IDPROVINCIA": "",
    "TELEFONO": "",
    "DOCUMENTOTIPO": "",
    "NUMERO_CUIT": "",
    "CONDICIONIVA": "",
    "SUCURSAL": "",
    "NUMERO": "",
    "LETRA": "",
    "FechaSubdiario": "",
    "CONCEPTO": "",
    "Fecha": "",
    "Vencimiento": "",
    "Vigencia": "",
    "FHVToCAI": "",
    "NROCAI": ""
  },
  "ROWS": [
    {
      "Codigo_Articulo": "",
      "Descripcion": "",
      "UD": "",
      "Importe_Lista": "",
      "Cantidad": "",
      "% Dto1": "",
      "% Dto2": "",
      "Importe_Neto": "",
      "Total": "",
      "AuxNroLote": "",
      "AuxNroSerie": "",
      "IVA": "",
      "Impuestos internos": "",
      "Bl/Pq": "",
      "Moneda": "",
      "Tot.Imp.Int": ""
    }
  ],
  "TOTALES": {
    "Subtotal": "",
    "Pesos brutos": "",
    "Neto gravado": "",
    "Exento": "",
    "No gravado": "",
    "Descuento general": "",
    "IVA 21": "",
    "IVA 10.5": "",
    "IVA 27": "",
    "Percepcion IVA": "",
    "Percepcion IIBB": "",
    "Percepcion Ganancias": "",
    "Impuestos internos": "",
    "Otros impuestos": "",
    "Total": "",
    "Total final": "",
    "Moneda": "",
    "Otros": [
      {"Etiqueta": "", "Importe_Neto": ""}
    ]
  },
  "meta": {
    "comprobante_raw": "",
    "moneda_detectada": "",
    "observaciones": "",
    "totales_raw": "",
    "orden_columnas": []
  }
}

REGLAS IMPORTANTES (para no confundir PROVEEDOR con CLIENTE):
- En meta.orden_columnas devolvé una LISTA con el orden real de columnas del detalle (de izquierda a derecha) según la tabla/encabezados que veas.
  * Usá los nombres EXACTOS de las claves de ROWS.
  * Ej: ["Cantidad","Codigo_Articulo","Descripcion","Importe_Neto","Total"] o ["Codigo_Articulo","Descripcion","Cantidad","Importe_Neto","% Dto1","% Dto2","Total"]
  * Si no se ven encabezados o no estás seguro, dejá [].
1) "Nombre", "DOMICILIO", "LOCALIDAD", "CODIGOPOSTAL", "IDPROVINCIA", "TELEFONO" deben corresponder **AL PROVEEDOR/EMISOR**.
   - NO uses los datos del destinatario/cliente ("Sres:", "Cliente:", etc.).

2) "CUENTA": dejar vacío "" (o si no podés, poner el CUIT del proveedor). NO usar CUIT del cliente.

3) DOCUMENTO del proveedor:
   - "DOCUMENTOTIPO" = "CUIT" (o "DNI" si realmente es DNI).
   - "NUMERO_CUIT" = CUIT del proveedor.

4) COMPROBANTE:
   - "SUCURSAL" = punto de venta (ej: "0011").
   - "NUMERO" = número siguiente (ej: "00247502").
   - "LETRA" = A/B/C si aparece.

5) FECHAS:
   - "Fecha" = fecha de emisión (dd/mm/yyyy).
   - "FechaSubdiario" = igual a "Fecha" si no hay otra.
   - "Vencimiento" = fecha de vto si aparece.

6) CAE/CAI:
   - "NROCAI" = CAE/CAI.
   - "FHVToCAI" = Vto CAE/CAI.

7) IMPORTES (ROWS y TOTALES):
   - En ROWS, en cada item incluí solo las claves que tengan valor (no repitas campos vacíos).

   - devuelve importes exactamente como se ven (raw), sin intentar normalizar

8) TOTALES (pie de página):
   - Buscá el sector donde diga: Subtotal / Neto / IVA / Percepciones / Total.
   - Si no existe algún concepto, dejalo vacío "".
   - Si hay conceptos extra (por ejemplo "Impuesto municipal", "Tasa", "Percep. varias"), cargalos en TOTALES.Otros[].
   - En meta.totales_raw poné un resumen corto de texto que veas en el pie (para auditoría/debug), sin inventar.

9) Si un dato no aparece o hay duda, dejalo vacío "". NO elimines claves.

10) IGNORAR LA COLUMNA "Precio Sug" o "Precio Sugerido"

META – ORDEN DE COLUMNAS (MUY IMPORTANTE):

- meta.orden_columnas debe contener EXCLUSIVAMENTE nombres de claves definidas en ROWS.
- NO usar los títulos reales de las columnas impresas en la factura.
- El orden debe reflejar la disposición visual de izquierda a derecha del detalle,
  pero expresado SIEMPRE con los nombres internos de ROWS.

Ejemplo:
Si la factura muestra:
  "Cant | Cod | Artículo | Precio Unit | Importe"

Y vos interpretás:
  "Cant"           ? "Cantidad"
  "Cod"            ? "Codigo_Articulo"
  "Artículo"       ? "Descripcion"
  "Precio Unit"    ? "Importe_Neto"
  "Importe"        ? "Total"
Entonces devolvé:
  meta.orden_columnas = ["Cantidad","Codigo_Articulo","Descripcion","Importe_Neto","Total"]

- Si no se ven encabezados claros o no podés inferir la equivalencia con certeza, devolvé [].

REGLAS DE PRECIOS (ROWS) — preferencia cuando el comprobante lo permita:
- "Importe_Lista": debe ser el precio unitario de lista / precio proveedor ANTES de descuentos (precio base).
- "% Dto1" y "% Dto2": descuentos porcentuales si aparecen (en columna o embebidos en la descripción).
- "Importe_Neto": debe ser el precio unitario NETO luego de aplicar descuentos sobre "Importe_Lista".
  * Si hay dos descuentos, aplicalos en cascada (ej: lista=100, dto1=10% => 90; dto2=5% => 85.50).
- "Total": debe ser "Importe_Neto" * "Cantidad" (si "Importe_Neto" es unitario neto).
- Si el documento muestra que "Importe_Neto" ya es el total por renglón (no unitario), entonces:
  * poné "Importe_Neto" como el valor que se vea (sin inventar) y calculá "Total" sólo si está explícito.
- NO inventes importes: si no se puede determinar con certeza por lo que se ve en la tabla, dejá el campo en "".

REGLA ANTI - CORTE:
- El detalle puede ser MUY largo. NO cierres el detalle antes de terminar la página.
- La línea '*** Transporte: ...' NO es fin de detalle. Es un ítem adicional y el detalle continúa debajo.
- Si el encabezado de columnas se repite, seguí leyendo los renglones.
- Si el PDF tiene varias páginas, continuá con TODAS las páginas antes de cerrar el JSON.

Respondé SOLO JSON.
"""


PROVIDER_ONLY_PROMPT = r"""
Vas a analizar 1 a 10 paginas de una factura / comprobante de compra.
Responde SOLO con JSON valido, sin texto adicional.

Objetivo: identificar el proveedor para que otro sistema lo busque en base.

Devolve exactamente este formato:
{
  "codigo_proveedor": "",
  "cuit": "",
  "nombre_proveedor": ""
}

Reglas:
- "codigo_proveedor": solo si aparece de forma explicita en el documento como codigo interno del proveedor, cuenta proveedor, nro proveedor o equivalente.
- No inventes "codigo_proveedor". Si no aparece con claridad, dejalo "".
- "cuit": CUIT del proveedor/emisor, nunca el del cliente.
- "nombre_proveedor": razon social o nombre del proveedor/emisor, nunca el cliente.
- Si hay duda entre proveedor y cliente, prioriza siempre el emisor de la factura.
- Si un dato no aparece o no es confiable, dejalo "".

Responde SOLO JSON.
"""


def read_prompt(prompt_file: Optional[str]) -> str:
    if prompt_file:
        p = Path(prompt_file)
        if p.exists():
            raw = p.read_bytes()
            if raw[:2] in (b"\xff\xfe", b"\xfe\xff"):
                return raw.decode("utf-16", errors="replace")
            if raw[:3] == b"\xef\xbb\xbf":
                return raw[3:].decode("utf-8", errors="replace")
            return raw.decode("utf-8", errors="replace")
    return DEFAULT_PROMPT


# ----------------------------
# Conversión de archivos a bloques para OpenAI
# ----------------------------
def file_to_content_block(file_path: str, max_side: Optional[int] = None, jpeg_quality: int = 85) -> Dict[str, Any]:
    ext = Path(file_path).suffix.lower()
    data = Path(file_path).read_bytes()

    if ext in (".jpg", ".jpeg", ".png", ".webp"):
        if Image is not None and max_side and max_side > 0:
            try:
                img = Image.open(io.BytesIO(data)).convert("RGB")
                w, h = img.size
                if max(w, h) > max_side:
                    img.thumbnail((max_side, max_side))
                    buf = io.BytesIO()
                    img.save(buf, format="JPEG", quality=max(50, min(int(jpeg_quality), 95)))
                    b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
                    return {"type": "input_image", "image_url": f"data:image/jpeg;base64,{b64}"}
            except Exception:
                pass

        b64 = base64.b64encode(data).decode("utf-8")
        if ext in (".jpg", ".jpeg"):
            mime = "image/jpeg"
        elif ext == ".png":
            mime = "image/png"
        else:
            mime = "image/webp"
        return {"type": "input_image", "image_url": f"data:{mime};base64,{b64}"}

    if ext == ".pdf":
        b64 = base64.b64encode(data).decode("utf-8")
        return {
            "type": "input_file",
            "filename": Path(file_path).name,
            "file_data": f"data:application/pdf;base64,{b64}",
        }

    raise ValueError(f"Tipo no soportado: {ext}. Usá JPG/PNG/WEBP o PDF.")

def file_to_content_blocks(file_path: str, tiles: int = 1, provider_only: bool = False) -> List[Dict[str, Any]]:
    ext = Path(file_path).suffix.lower()
    if tiles <= 1 or ext == ".pdf":
        if provider_only and ext in (".jpg", ".jpeg", ".png", ".webp"):
            return [file_to_content_block(file_path, max_side=1600, jpeg_quality=72)]
        return [file_to_content_block(file_path)]

    if Image is None:
        raise SystemExit("ERROR: Para --tile necesitás instalar Pillow: pip install pillow")

    if ext not in (".jpg", ".jpeg", ".png", ".webp"):
        return [file_to_content_block(file_path)]

    img = Image.open(file_path).convert("RGB")
    w, h = img.size
    tiles = max(1, min(int(tiles), 6))
    slice_h = (h + tiles - 1) // tiles
    overlap = min(60, max(20, slice_h // 10))

    blocks: List[Dict[str, Any]] = []
    for i in range(tiles):
        top = i * slice_h
        bottom = min(h, (i + 1) * slice_h)
        if i > 0:
            top = max(0, top - overlap)
        if i < tiles - 1:
            bottom = min(h, bottom + overlap)

        crop = img.crop((0, top, w, bottom))
        buf = io.BytesIO()
        crop.save(buf, format="JPEG", quality=90)
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        blocks.append({"type": "input_image", "image_url": f"data:image/jpeg;base64,{b64}"})

    return blocks


def call_backend_with_hard_timeout(
    *,
    content_blocks: List[Dict[str, Any]],
    model: str,
    max_output_tokens: int,
    text: Optional[Dict[str, Any]] = None,
    source_filename: Optional[str] = None,
    timeout_seconds: int = 300,
) -> str:
    """Aplica un timeout duro al backend para evitar procesos colgados."""

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
            raise SystemExit(f"ERROR backend timeout: superó el límite de {int(timeout_seconds)}s.") from e


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        add_help=True,
        description=f"Lector de facturas -> JSON (1 a {MAX_INPUT_FILES} páginas). Usa backend remoto o OpenAI directo segun configuracion.",
    )
    parser.add_argument("files", nargs="*", help=f"1 a {MAX_INPUT_FILES} archivos (imágenes/PDF) en orden de páginas")
    parser.add_argument(
        "--idcliente",
        type=int,
        default=None,
        help="Id de cliente para auditoria backend (IA_IDCLIENTE/IDCLIENTE).",
    )
    parser.add_argument("--outdir", default="", help="Carpeta de salida. Default: TEMP del sistema")
    parser.add_argument("--prompt-file", default="", help="Archivo .txt con prompt personalizado")
    parser.add_argument("--model", default="gpt-4.1-mini", help="Modelo a usar (default: gpt-4.1-mini)")
    parser.add_argument(
        "--fallback-model",
        default="gpt-4.1",
        help="Modelo de reintento si --model no alcanza (default: gpt-4.1).",
    )
    parser.add_argument(
        "--no-fallback",
        action="store_true",
        help="Desactiva reintento automatico con --fallback-model.",
    )
    parser.add_argument("--gui", action="store_true", help="Muestra ventana de progreso (no altera stdout)")
    parser.add_argument(
        "--per-page",
        action="store_true",
        help="Procesa cada archivo/pagina por separado y luego unifica filas (mejora extraccion en tablas largas).",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Auto-ajusta parametros (tile y per-page) segun cantidad de paginas.",
    )
    parser.add_argument(
        "--tile",
        type=int,
        default=1,
        help="Divide cada pagina en N franjas horizontales (solo imagenes). Requiere Pillow.",
    )
    parser.add_argument("--env-file", default="", help="Archivo .env alternativo para pruebas.")
    parser.add_argument("--no-local-env", action="store_true", help="No cargar .env junto al exe/script.")
    parser.add_argument("--backend-url", default="", help="Override IA_BACKEND_URL.")
    parser.add_argument("--backend-route", default="", help="Override IA_BACKEND_ROUTE.")
    parser.add_argument("--client-id", default="", help="Override IA_CLIENT_ID.")
    parser.add_argument("--client-secret", default="", help="Override IA_CLIENT_SECRET.")
    parser.add_argument("--ia-task", default="", help="Override IA_TASK/opcion.")
    parser.add_argument(
        "--proveedor",
        action="store_true",
        help="Modo reducido: extrae solo codigo/cuit/nombre del proveedor.",
    )
    parser.add_argument(
        "--layout-file",
        default="",
        dest="layout_file",
        metavar="FILE",
        help="Ruta al archivo JSON con el layout del proveedor. "
             "Si el layout extrae datos confiables los usa sin llamar a IA. "
             "Si falla o el resultado es insuficiente, usa IA normalmente.",
    )
    parser.add_argument(
        "--layout-only",
        action="store_true",
        dest="layout_only",
        help="Con --layout-file: si el layout no produce resultado confiable, "
             "termina con error en vez de caer a IA.",
    )
    parser.add_argument(
        "--all-pages",
        action="store_true",
        dest="all_pages",
        help="No descartar páginas por diferencias de identificadores entre archivos. "
             "Útil al probar comprobantes multipágina desde el configurador.",
    )
    args = parser.parse_args()
    args.files = _normalize_cli_file_args(list(args.files))

    ui = None
    if args.gui:
        try:
            ui = StatusUI()
            ui.push("STATUS:Inicializando...")
        except Exception:
            ui = None

    worker_lock = threading.Lock()

    def log(msg: str):
        if ui:
            ui.push(msg)

    def status(msg: str):
        if ui:
            ui.push(f"STATUS:{msg}")

    result = {"out_path": None, "error": None}

    def worker():
        temp_files_to_cleanup: List[str] = []
        temp_processing_dir: Optional[str] = None
        with worker_lock:
            result["out_path"] = None
            result["error"] = None
            if ui:
                ui.set_retry_enabled(False)
        try:
            status("Cargando .env / variables...")
            log("Inicio de procesamiento...")
            if not args.no_local_env:
                load_env_near_app()
            if args.env_file:
                load_dotenv(dotenv_path=args.env_file, override=True)
            apply_runtime_env_overrides(args)
            if args.idcliente is not None:
                os.environ["IA_IDCLIENTE"] = str(args.idcliente)
                os.environ["IDCLIENTE"] = str(args.idcliente)

            if not backend_enabled():
                raise SystemExit(
                    "ERROR: No hay transporte IA configurado. "
                    "Definí IA_BACKEND_URL + IA_CLIENT_ID + IA_CLIENT_SECRET o bien OPENAI_API_KEY."
                )

            if not args.files:
                raise SystemExit(f"ERROR: Debés pasar 1 a {MAX_INPUT_FILES} archivos por parámetro.")
            source_files = list(args.files)
            if args.proveedor and len(source_files) > 1:
                ignored = len(source_files) - 1
                source_files = source_files[:1]
                log(
                    f"Modo proveedor: se procesa solo el primer archivo y se omiten {ignored} archivo(s) adicional(es)."
                )
            elif len(source_files) > MAX_INPUT_FILES:
                raise SystemExit(f"ERROR: Máximo {MAX_INPUT_FILES} archivos.")
            if args.proveedor:
                # En modo proveedor priorizamos tiempo de respuesta: 1 archivo, sin tiling ni per-page.
                args.tile = 1
                args.per_page = False

            if args.tile < 1 or args.tile > 6:
                raise SystemExit("ERROR: --tile debe ser un entero entre 1 y 6.")

            status("Validando archivos...")
            for f in source_files:
                if not Path(f).exists():
                    raise SystemExit(f"ERROR: No existe el archivo: {f}")

            status("Preparando salida...")
            outdir = _normalize_outdir_arg(args.outdir) or tempfile.gettempdir()
            Path(outdir).mkdir(parents=True, exist_ok=True)
            temp_processing_dir = tempfile.mkdtemp(prefix="fact_pdf_pages_")

            status("Separando paginas PDF...")
            active_files, temp_files_to_cleanup = _expand_pdf_inputs(source_files, temp_processing_dir)

            if len(active_files) > MAX_INPUT_FILES:
                raise SystemExit(f"ERROR: Máximo {MAX_INPUT_FILES} hojas/páginas en total.")

            if not args.proveedor and len(active_files) > 1 and not args.auto and not args.per_page and args.tile == 1:
                # a 21-03-2026 Codex - si entran varias paginas, activar auto por defecto para no perder renglones
                args.auto = True
                log("Auto activado por multiples paginas.")

            # Auto-ajuste simple segun cantidad de paginas
            if args.auto:
                n = len(active_files)
                if n <= 1:
                    args.tile = 3
                    args.per_page = False
                elif n <= 3:
                    # Para pocas paginas: enviar todo junto con mas tiles.
                    # El modelo ve el contexto completo y no hay riesgo de que
                    # detect_mismatched_invoice_pages descarte paginas continuacion.
                    args.tile = 4
                    args.per_page = False
                else:
                    args.tile = 5
                    args.per_page = True
                    # Con muchas paginas de la misma factura, no descartar ninguna
                    if not args.all_pages:
                        args.all_pages = True
                        log("all-pages activado automaticamente por multiples archivos.")

            status("Cargando prompt...")
            prompt = PROVIDER_ONLY_PROMPT if args.proveedor else read_prompt(args.prompt_file.strip() or None)

            if 'json' not in prompt.lower():
                prompt = 'Responde solo con json.\n' + prompt

            status("Armando contenido...")
            log(f"Modelo: {args.model} | per-page: {args.per_page} | tile: {args.tile} | proveedor: {args.proveedor}")
            content = [{"type": "input_text", "text": prompt}]
            total_files = len(active_files)
            for i, f in enumerate(active_files, start=1):
                status(f"Adjuntando página {i}/{total_files}...")
                log(f"Archivo: {f}")
                content.extend(file_to_content_blocks(f, args.tile, provider_only=args.proveedor))

            status("Analizando con Inteligencia Artificial...")
            log("Motor IA: Activo")
            def call_model(content_blocks: List[Dict[str, Any]], model_name: str, source_file: str) -> dict:
                backend_timeout = 20 if args.proveedor else 300
                max_output_tokens = 800 if args.proveedor else 16000
                out_text = call_backend_with_hard_timeout(
                    content_blocks=content_blocks,
                    model=model_name,
                    max_output_tokens=max_output_tokens,
                    text={"format": {"type": "json_object"}},
                    source_filename=Path(source_file).name,
                    timeout_seconds=backend_timeout,
                )

                try:
                    data = extract_first_json(out_text)
                except Exception as e:
                    raw_path = Path(outdir) / f"{Path(active_files[0]).stem}_{dt.datetime.now().strftime('%Y%m%d_%H%M%S')}_raw.txt"
                    raw_path.write_text(out_text, encoding="utf-8", errors="replace")
                    raise SystemExit(f"ERROR: No se pudo parsear JSON. Se guardo la respuesta cruda en: {raw_path}") from e

                if args.proveedor:
                    data = normalize_provider_only_schema(data)
                else:
                    data = normalize_schema(data)
                    infer_orden_columnas(data)
                return data

            def run_extraction(model_name: str) -> dict:
                if args.per_page and len(active_files) > 1:
                    page_results: List[dict] = []
                    total_files = len(active_files)
                    t_pages_start = time.time()
                    for i, f in enumerate(active_files, start=1):
                        # ETA aproximado basado en promedio por p?gina procesada
                        if i > 1:
                            elapsed = time.time() - t_pages_start
                            avg = elapsed / (i - 1)
                            remaining = avg * (total_files - i + 1)
                            mm = int(remaining // 60)
                            ss = int(remaining % 60)
                            status(f"IA por pagina {i}/{total_files}... (ETA ~{mm:02d}:{ss:02d})")
                        else:
                            status(f"IA por pagina {i}/{total_files}...")
                        log(f"Archivo: {f}")
                        page_content = [{"type": "input_text", "text": prompt}]
                        page_content.extend(file_to_content_blocks(f, args.tile, provider_only=args.proveedor))
                        page_results.append(call_model(page_content, model_name, f))
                    if getattr(args, "all_pages", False):
                        accepted_results, ignored_warnings = page_results, []
                    else:
                        accepted_results, ignored_warnings = detect_mismatched_invoice_pages(page_results, active_files)
                    for warning in ignored_warnings:
                        log(warning)
                    data_model = merge_data_keep_best(accepted_results)
                    if ignored_warnings and not args.proveedor:
                        meta = _ensure_object(data_model.get("meta"))
                        data_model["meta"] = meta
                        existing_obs = str(meta.get("observaciones") or "").strip()
                        warning_text = " ".join(ignored_warnings)
                        meta["observaciones"] = f"{existing_obs} {warning_text}".strip() if existing_obs else warning_text
                else:
                    data_model = call_model(content, model_name, active_files[0])

                if not args.proveedor:
                    data_model["ROWS"] = dedupe_rows(_ensure_list(data_model.get("ROWS")))
                    adjust_importe_lista_for_bultos(data_model)
                    validate_totals_integrity(data_model, tolerance=0.03)
                return data_model

            # ------------------------------------------------------------------ #
            # Pre-proceso: extracción por layout (sin IA)
            # ------------------------------------------------------------------ #
            data = None
            layout_file = (args.layout_file or "").strip()
            if layout_file and not args.proveedor:
                status("Cargando layout desde archivo...")
                try:
                    import json as _json
                    _layout_data = _json.loads(Path(layout_file).read_text(encoding="utf-8-sig"))
                    from layout_extractor import try_layout_extraction
                    data = try_layout_extraction(
                        _layout_data,
                        active_files[:5],
                        log_fn=log,
                    )
                except ImportError:
                    log("Layout: módulo layout_extractor no disponible.")
                except (OSError, ValueError) as _layout_exc:
                    log(f"Layout: error al leer archivo ({_layout_exc!r}), se continúa con IA.")
                except Exception as _layout_exc:
                    log(f"Layout: error inesperado ({_layout_exc!r}), se continúa con IA.")

                if data is not None:
                    data = normalize_schema(data)
                    infer_orden_columnas(data)
                    adjust_importe_lista_for_bultos(data)
                    validate_totals_integrity(data, tolerance=0.03)
                    log("Layout: datos extraídos sin IA.")
                elif getattr(args, "layout_only", False):
                    raise SystemExit("ERROR: El layout no pudo procesar el archivo (zonas no definidas o archivo no legible).")
            # ------------------------------------------------------------------ #

            fallback_enabled = (not args.no_fallback) and bool(args.fallback_model) and args.fallback_model != args.model
            if data is not None:
                log("Layout OK: se omite llamada a IA.")
            else:
                log(f"Intento 1 con modelo: {args.model}")
                try:
                    data = run_extraction(args.model)
                except SystemExit as first_err:
                    if not fallback_enabled:
                        raise
                    status("Reintentando con modelo alternativo...")
                    log(f"Intento 1 fallido: {first_err}")
                    log(f"Intento 2 con modelo: {args.fallback_model}")
                    data = run_extraction(args.fallback_model)
                else:
                    if fallback_enabled:
                        should_retry, reason = needs_model_fallback(data)
                        if should_retry:
                            status("Reintentando con modelo alternativo...")
                            log(f"Fallback activado: {reason}")
                            log(f"Intento 2 con modelo: {args.fallback_model}")
                            try:
                                retry_data = run_extraction(args.fallback_model)
                            except SystemExit as retry_err:
                                log(f"Fallback fallo: {retry_err}. Se conserva resultado original.")
                            else:
                                retry_bad, _ = needs_model_fallback(retry_data)
                                if retry_bad:
                                    log("Fallback ejecutado, pero se conserva resultado original por no mejorar calidad.")
                                else:
                                    data = retry_data
                                    log("Fallback OK: se usa resultado del modelo alternativo.")

            status("Guardando JSON...")
            # Mantener exactamente el nombre original (solo cambia extension a .json)
            base = Path(active_files[0]).stem
            out_path = Path(outdir) / f"{base}.json"
            out_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

            result["out_path"] = str(out_path)
            status("Listo ✅")
            log(f"Generado: {out_path}")

        except SystemExit as e:
            result["error"] = str(e)
        except Exception as e:
            result["error"] = f"ERROR: {e!r}"
        finally:
            for temp_file in temp_files_to_cleanup:
                try:
                    Path(temp_file).unlink(missing_ok=True)
                except Exception:
                    pass
            if temp_processing_dir:
                try:
                    shutil.rmtree(temp_processing_dir, ignore_errors=True)
                except Exception:
                    pass

        if ui:
            # que se llegue a ver el “Listo” o el error
            if result["error"]:
                ui.push("STATUS:Error ❌")
                ui.push(result["error"])
                ui.freeze("Error ❌")
                ui.set_retry_enabled(True)
            else:
                ui.push("Proceso finalizado correctamente.")
                ui.finish("Listo ✅", keep_open_seconds=2.5)

    def start_worker() -> None:
        if worker_lock.locked():
            return
        if ui:
            ui.reset_progress("Inicializando...")
        t = threading.Thread(target=worker, daemon=True)
        t.start()

    # Ejecutar con GUI (thread) o directo
    if ui:
        ui.set_retry_callback(start_worker)
        start_worker()
        ui.mainloop()
    else:
        worker()

    # Contrato VB6: OK -> stdout solo ruta. Error -> stderr y exit != 0
    if result["error"]:
        try:
            print(result["error"], file=sys.stderr)
            sys.stderr.flush()
        except Exception:
            pass
        raise SystemExit(1)

    try:
        print(result["out_path"])  # SOLO la ruta — para VB6 con pipe
        sys.stdout.flush()
    except Exception:
        pass


if __name__ == "__main__":
    main()

