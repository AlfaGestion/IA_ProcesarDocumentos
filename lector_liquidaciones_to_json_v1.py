# -*- coding: utf-8 -*-
r"""
lector_liquidaciones_to_json_v1.py

- Lee documentos de múltiples páginas (JPG/PNG/WEBP o PDF).
- Llama a OpenAI y devuelve un archivo de texto con dos columnas: CONCEPTO|TOTAL.
- Modo GUI opcional (--gui): ventana simple con estado, barra, tiempo transcurrido y log.
- Importante: al finalizar OK imprime SOLO la ruta del TXT por stdout (para VB6).
  En error: sale con código != 0 y escribe el mensaje de error en stderr.

Requisitos:
  pip install openai python-dotenv pillow pypdf

Uso:
  python lector_liquidaciones_to_json_v1.py liquidacion.pdf --outdir E:\temp
  python lector_liquidaciones_to_json_v1.py img1.jpg img2.jpg --outdir E:\temp --gui

EXE (PyInstaller):
  pyinstaller --onefile --noconsole lector_liquidaciones_to_json_v1.py
"""

from __future__ import annotations

import argparse
import base64
import datetime as dt
import io
import json
import os
import re
import sys
import tempfile
import threading
import time
import queue
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional
import calendar

from dotenv import load_dotenv
from openai import OpenAI
from ia_backend_transport import backend_enabled, call_backend


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

try:
    from pypdf import PdfReader, PdfWriter
except Exception:
    PdfReader = None
    PdfWriter = None


class StatusUI:
    """Ventana simple: estado + barra indeterminada + tiempo + log.
    NO escribe en stdout (para no romper VB6).
    """

    def __init__(self, title="Procesando liquidación...", width=560, height=260):
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

        self._closed = False
        self._time_after_id = None
        self.root.protocol("WM_DELETE_WINDOW", self._on_close)

        self.root.after(100, self._poll)
        self._time_after_id = self.root.after(200, self._tick_time)

    def _on_close(self):
        # Si cierran la ventana, no matamos el proceso; solo ocultamos.
        self._closed = True
        if self._time_after_id is not None:
            try:
                self.root.after_cancel(self._time_after_id)
            except Exception:
                pass
            self._time_after_id = None
        try:
            self.root.withdraw()
        except Exception:
            pass

    def _tick_time(self):
        if not self._closed:
            secs = int(time.time() - self.t0)
            mm = secs // 60
            ss = secs % 60
            try:
                self.lbl_time.configure(text=f"Tiempo: {mm:02d}:{ss:02d}")
            except Exception:
                self._closed = True
                return
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
        try:
            self.root.destroy()
        except Exception:
            pass

    def mainloop(self):
        self.root.mainloop()


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


def safe_basename(file_path: str) -> str:
    name = Path(file_path).stem
    name = re.sub(r"[^a-zA-Z0-9_\-]+", "_", name).strip("_")
    return name or "liquidacion"


def sanitize_json_text(s: str) -> str:
    s = s.strip()
    if s.startswith("\ufeff"):
        s = s.lstrip("\ufeff")
    # remove trailing commas before } or ]
    s = re.sub(r",\s*([}\]])", r"\1", s)
    return s


def extract_first_json(text: str) -> dict:
    """Extrae el primer JSON válido del texto (tolerante a basura alrededor)."""
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


def _parse_number(raw: Any) -> float:
    if raw is None:
        return 0.0
    s = str(raw).strip()
    if not s:
        return 0.0
    # keep digits, comma, dot, minus
    s = re.sub(r"[^\d,.\-]", "", s)
    if not s:
        return 0.0
    # decide decimal separator
    if "," in s and "." in s:
        if s.rfind(".") > s.rfind(","):
            s = s.replace(",", "")
        else:
            s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return 0.0


def _round2(val: float) -> float:
    try:
        return float(f"{val:.2f}")
    except Exception:
        return 0.0

def _norm_text(s: str) -> str:
    s = s or ""
    s = unicodedata.normalize("NFD", s)
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    return s.upper()

def _ensure_keyword(concept: str, keyword: str) -> str:
    t = _norm_text(concept)
    if keyword in t:
        return concept
    return f"{keyword} - {concept}".strip()


def _classify_concept_name(concept: str) -> str:
    """Clasifica con las mismas reglas que el consumidor VB6."""
    t = _norm_text(concept)
    if "RET_GAN" in t:
        return "RET_GAN"
    if "RET_IIBB" in t:
        return "RET_IIBB"
    if "RET_IVA" in t:
        return "RET_IVA"
    if "IVA_CREDITO" in t:
        return "IVA_CREDITO"
    if "BANCO" in t:
        return "BANCO"
    if (
        "ACREDITADO" in t
        or "LIQUIDADO" in t
        or "NETO DE PAGOS" in t
        or "NETO A COBRAR" in t
        or "A DEPOSITAR" in t
    ):
        return "BANCO"
    if "ARANCEL" in t or "COMISION" in t or "CARGO" in t:
        return "GASTO"
    if "IMPUESTO" in t and "IVA" not in t and "GANANCIAS" not in t and "IIBB" not in t and "INGRESOS" not in t:
        return "GASTO"
    if ("GASTO" in t or "COMISION" in t) and "IVA" not in t and "CREDITO" not in t:
        return "GASTO"
    if "IVA" in t and ("ARANCEL" in t or "COMISION" in t or "GASTO" in t):
        return "IVA_CREDITO"
    if ("IVA" in t and "RET" in t) or ("IVA" in t and "PERCEP" in t) or "R.G. 2408" in t or "RG 2408" in t:
        return "RET_IVA"
    if (
        "INGRESOS" in t
        or "IIBB" in t
        or "SIRTAC" in t
        or "ING.BRUTOS" in t
        or "ING. BRUTOS" in t
        or "ING BRUTOS" in t
    ):
        return "RET_IIBB"
    if "GANANCIAS" in t or ("RET" in t and "GAN" in t) or "RG 830" in t:
        return "RET_GAN"
    if ("IVA" in t and "CREDITO" in t) or "CRED.FISC" in t or "CRED FISC" in t or ("IVA" in t and "CRED" in t and "FISC" in t):
        return "IVA_CREDITO"
    if "TARJETA" in t and "IVA" not in t:
        return "TARJETA"
    return "OTROS"


def _ensure_keywords_for_category(concept: str, category: str) -> str:
    if category == "TARJETA":
        return _ensure_keyword(concept, "TARJETA")
    if category == "BANCO":
        return _ensure_keyword(concept, "BANCO")
    if category == "GASTO":
        return _ensure_keyword(concept, "GASTO")
    if category == "IVA_CREDITO":
        return _ensure_keyword(concept, "IVA CREDITO")
    if category == "RET_IVA":
        return _ensure_keyword(concept, "IVA RET")
    if category == "RET_IIBB":
        return _ensure_keyword(concept, "IIBB")
    if category == "RET_GAN":
        return _ensure_keyword(concept, "GANANCIAS")
    return _ensure_keyword(concept, "OTROS")


def _canonical_label_for_category(category: str) -> str:
    mapping = {
        "TARJETA": "TARJETA",
        "BANCO": "BANCO",
        "GASTO": "GASTO",
        "IVA_CREDITO": "IVA_CREDITO",
        "RET_IVA": "RET_IVA",
        "RET_IIBB": "RET_IIBB",
        "RET_GAN": "RET_GAN",
        "OTROS": "OTROS",
    }
    return mapping.get(category, "OTROS")


def _normalize_total_for_category(total: str, category: str) -> str:
    value = _parse_number(total)
    if category == "TARJETA":
        value = abs(value)
    else:
        value = -abs(value)
    value = _round2(value)
    if abs(value) < 0.005:
        value = 0.0
    return f"{value:.2f}"

def _postprocess_output(text: str) -> str:
    lines_in = []
    for raw in text.splitlines():
        ln = raw.strip()
        if not ln:
            continue
        if ln.startswith("```") or ln.startswith("**") or ln == "---":
            continue
        lines_in.append(ln)
    if not lines_in:
        return text

    out: List[str] = []
    in_control = False
    main_lines: List[str] = []

    for ln in lines_in:
        if _norm_text(ln) == "CONTROL_TOTALES_DIARIOS":
            in_control = True
            out.extend(_apply_keywords_to_main(main_lines))
            main_lines = []
            out.append(ln)
            continue

        if not in_control:
            main_lines.append(ln)
        else:
            out.append(ln)

    if main_lines:
        out.extend(_apply_keywords_to_main(main_lines))

    return "\n".join(out) + "\n"

def _apply_keywords_to_main(lines: List[str]) -> List[str]:
    categories = ["TARJETA", "BANCO", "GASTO", "IVA_CREDITO", "RET_IVA", "RET_IIBB", "RET_GAN", "OTROS"]
    sums: Dict[str, float] = {k: 0.0 for k in categories}
    row_idx = 0
    fallback_by_position = {
        1: "TARJETA",
        2: "BANCO",
        3: "GASTO",
        4: "IVA_CREDITO",
        5: "RET_IVA",
        6: "RET_IIBB",
        7: "RET_GAN",
        8: "OTROS",
    }
    for ln in lines:
        if "|" not in ln:
            continue

        concept, total = ln.split("|", 1)
        concept = concept.strip()
        total = total.strip()
        t_concept = _norm_text(concept)
        if t_concept in ("CONCEPTO", "TIPO", "TIPOCONCEPTOIA"):
            continue
        # Evita encabezados como CONCEPTO|TOTAL
        if _norm_text(total) in ("TOTAL", "IMPORTE"):
            continue

        row_idx += 1
        cat = _classify_concept_name(concept)
        # Si el modelo devuelve "OTROS" genérico en las primeras filas,
        # recuperamos la estructura esperada original.
        if cat == "OTROS" and _norm_text(concept) in ("OTRO", "OTROS", "OTHER"):
            cat = fallback_by_position.get(row_idx, "OTROS")
        # Fallback para la línea principal cuando el modelo no incluye la palabra TARJETA.
        if row_idx == 1 and cat == "OTROS":
            cat = "TARJETA"
        sums[cat] += _parse_number(total)

    out: List[str] = []
    for cat in categories:
        concept = _canonical_label_for_category(cat)
        total = _normalize_total_for_category(str(sums[cat]), cat)
        out.append(f"{concept}|{total}")
    return out

def _write_log(log_path: Path, msg: str) -> None:
    try:
        ts = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(f"[{ts}] {msg}\n")
    except Exception:
        pass


def _extract_blocks_sequential(page_texts: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    current: Dict[str, float] = {}

    def _canon_label(ln: str) -> Optional[str]:
        t = re.sub(r"\s+", " ", ln.upper()).strip()
        if "VENTAS C/DESCUENTO CONTADO" in t:
            return "VENTAS C/DESCUENTO CONTADO"
        if "ARANCEL" in t:
            return "ARANCEL"
        if "IVA CRED.FISC.COMERCIO S/ARANC" in t:
            return "IVA CRED.FISC.COMERCIO S/ARANC 21,00%"
        if "IVA RI SERV.OPER. INT" in t:
            return "IVA RI SERV.OPER. INT."
        if "SERVICIO OPER. INTERNAC" in t or "SERV.OPER. INT" in t:
            return "SERVICIO OPER. INTERNAC."
        if "RETENCION ING.BRUTOS SIRTAC" in t:
            return "RETENCION ING.BRUTOS SIRTAC"
        if "PERCEPCION IVA R.G. 2408" in t:
            return "PERCEPCION IVA R.G. 2408 3,00 %"
        if "QR PERCEPCION IVA 3337" in t:
            return "QR PERCEPCION IVA 3337"
        if "QR RETENCION IIBB RIO NEGRO" in t:
            return "QR RETENCION IIBB RIO NEGRO"
        if "IMPORTE NETO DE PAGOS" in t:
            return "IMPORTE NETO DE PAGOS"
        return None

    for text in page_texts:
        for ln in text.splitlines():
            label = _canon_label(ln)
            if not label:
                continue
            nums = re.findall(r"([0-9][0-9\.\,]*)", ln)
            if not nums:
                continue
            amount = _parse_number(nums[-1])
            if amount <= 0:
                continue
            current[label] = amount
            if label == "IMPORTE NETO DE PAGOS":
                rows.append({"concepts": dict(current)})
                current.clear()

    return rows


def _extract_pdf_totals(files: List[str]) -> Dict[str, float]:
    """Extrae totales clave desde PDFs (cabeceras y totales diarios)."""
    totals = {
        "total_presentado": None,
        "neto_header": None,
        "bank_nacion": False,
        "bank_patagonia": False,
        "bank_name": None,
        "card_name": None,
        "period": None,
        "patagonia_desglose": [],
        "ventas_sum": 0.0,
        "arancel_sum": 0.0,
        "iva_sum": 0.0,
        "ret_iva_sum": 0.0,
        "ret_iibb_sum": 0.0,
        "ret_gan_sum": 0.0,
        "neto_sum": 0.0,
        "has_daily": False,
        "daily_rows": [],
    }

    if PdfReader is None:
        return totals

    def _first_match_val(pattern: str, text: str) -> Optional[float]:
        m = re.search(pattern, text, flags=re.IGNORECASE)
        if not m:
            return None
        return _parse_number(m.group(1))

    def _sum_matches(pattern: str, text: str) -> float:
        acc = 0.0
        for m in re.finditer(pattern, text, flags=re.IGNORECASE):
            acc += _parse_number(m.group(1))
        return acc

    def _extract_header_amounts(text: str) -> List[float]:
        # Extrae importes con formato 1.234.567,89 alrededor de los labels
        amounts: List[float] = []
        for label in ("Total presentado", "Neto de pagos"):
            idx = text.lower().find(label.lower())
            if idx >= 0:
                window = text[idx : idx + 220]
                for m in re.finditer(r"([0-9]{1,3}(?:[.\s][0-9]{3})*,[0-9]{2})", window):
                    amounts.append(_parse_number(m.group(1)))
        return [a for a in amounts if a > 0]

    def _extract_patagonia_header(text: str) -> Dict[str, float]:
        # Busca total presentado / total descuento / saldo en el encabezado Patagonia
        out = {}
        # Captura los tres montos que suelen aparecer en bloque
        m = re.search(
            r"TOTAL\s+PRESENTADO\s*\$\s*([0-9\.\,]+)\s*[\r\n ]+"
            r"TOTAL\s+DESCUENTO\s*\$\s*([0-9\.\,]+)\s*[\r\n ]+"
            r"SALDO\s*\$\s*([0-9\.\,]+)",
            text,
            flags=re.IGNORECASE,
        )
        if m:
            out["total_presentado"] = _parse_number(m.group(1))
            out["total_descuento"] = _parse_number(m.group(2))
            out["saldo"] = _parse_number(m.group(3))
            return out
        # Fallback por bloque: extraer montos en la zona cercana a "TOTAL PRESENTADO"
        idx = text.upper().find("TOTAL PRESENTADO")
        if idx >= 0:
            window = text[idx : idx + 500]
            nums = [ _parse_number(n) for n in re.findall(r"([0-9]{1,3}(?:[.\s][0-9]{3})*,[0-9]{2})", window) ]
            nums = [n for n in nums if n > 0]
            if len(nums) >= 3:
                out["total_presentado"] = nums[0]
                out["total_descuento"] = nums[1]
                out["saldo"] = nums[2]
                return out
        # Fallback por sección de domicilio: tomar 3 primeros importes antes de "FECHA DE PAGO"
        idx2 = text.upper().find("RIO NEGRO")
        if idx2 >= 0:
            window2 = text[idx2 : idx2 + 500]
            cut = window2.upper().find("FECHA DE PAGO")
            if cut > 0:
                window2 = window2[:cut]
            nums2 = [ _parse_number(n) for n in re.findall(r"([0-9]{1,3}(?:[.\s][0-9]{3})*,[0-9]{2})", window2) ]
            nums2 = [n for n in nums2 if n > 0]
            if len(nums2) >= 3:
                out["total_presentado"] = nums2[0]
                out["total_descuento"] = nums2[1]
                out["saldo"] = nums2[2]
        # Fallback por líneas: tomar los próximos 3 importes luego del bloque de títulos
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        idx = None
        for i, ln in enumerate(lines):
            if re.search(r"TOTAL\s+PRESENTADO\s*\$", ln, flags=re.IGNORECASE):
                idx = i
                break
        if idx is not None:
            nums: List[float] = []
            for ln in lines[idx : min(len(lines), idx + 12)]:
                for n in re.findall(r"([0-9]{1,3}(?:[.\s][0-9]{3})*,[0-9]{2})", ln):
                    val = _parse_number(n)
                    if val > 0:
                        nums.append(val)
            if len(nums) >= 3:
                out["total_presentado"] = nums[0]
                out["total_descuento"] = nums[1]
                out["saldo"] = nums[2]
        return out

    def _infer_card_from_filename(name: str) -> Optional[str]:
        n = name.upper()
        if "CABAL" in n:
            return "TARJETA CABAL"
        if "AMEX" in n or "AMERICAN" in n:
            return "TARJETA AMEX"
        if "MASTERCARD" in n or "MASTER" in n:
            return "TARJETA MASTERCARD"
        if "VISA" in n:
            return "TARJETA VISA"
        if "NARANJA" in n:
            return "TARJETA NARANJA"
        return None

    def _infer_card_from_text(text: str) -> Optional[str]:
        up = (text or "").upper()
        if re.search(r"\bCABAL\b", up):
            return "TARJETA CABAL"
        if re.search(r"\bAMEX\b|\bAMERICAN\s+EXPRESS\b", up):
            return "TARJETA AMEX"
        if re.search(r"\bMASTERCARD\b|\bMASTER\b", up):
            return "TARJETA MASTERCARD"
        if re.search(r"\bVISA\b", up):
            return "TARJETA VISA"
        if re.search(r"\bNARANJA\b", up):
            return "TARJETA NARANJA"
        return None

    def _is_generic_card_label(card: Optional[str]) -> bool:
        if not card:
            return False
        return bool(re.fullmatch(r"\s*TARJETA\s+DE\s+(?:DEBITO|CR[EÉ]DITO)(?:\s+.*)?\s*", card, flags=re.IGNORECASE))

    def _infer_period_from_filename(name: str) -> Optional[str]:
        m = re.search(r"(20\d{2})[-_/](\d{2})[-_/](\d{2})", name)
        if m:
            y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
            if 1 <= mo <= 12:
                last_day = calendar.monthrange(y, mo)[1]
                return f"{last_day:02d}-{mo:02d}-{y}"
        m = re.search(r"(20\d{2})(\d{2})(\d{2})", name)
        if m:
            y, mo = int(m.group(1)), int(m.group(2))
            if 1 <= mo <= 12:
                last_day = calendar.monthrange(y, mo)[1]
                return f"{last_day:02d}-{mo:02d}-{y}"
        return None

    for f in files:
        if Path(f).suffix.lower() != ".pdf":
            continue
        try:
            reader = PdfReader(f)
        except Exception:
            continue
        fname = Path(f).name
        page_texts: List[str] = []
        for i, page in enumerate(reader.pages):
            text = page.extract_text() or ""
            page_texts.append(text)
            next_head = ""
            if i + 1 < len(reader.pages):
                try:
                    next_head = (reader.pages[i + 1].extract_text() or "")[:800]
                except Exception:
                    next_head = ""

            if not totals["bank_nacion"]:
                if re.search(r"\bBCO\s+DE\s+LA\s+NACION\s+ARGENTINA\b", text, flags=re.IGNORECASE) or re.search(
                    r"\bBANCO\s+NACION\b", text, flags=re.IGNORECASE
                ):
                    totals["bank_nacion"] = True
            if not totals["bank_patagonia"]:
                if re.search(r"\bBANCO\s+PATAGONIA\b", text, flags=re.IGNORECASE):
                    totals["bank_patagonia"] = True

            if totals["bank_name"] is None:
                m = re.search(r"Entidad\s+Pagadora\s*\n([A-Z0-9 .]+)", text, flags=re.IGNORECASE)
                if m:
                    totals["bank_name"] = m.group(1).strip()
                else:
                    m = re.search(r"\bBANCO\s+DE\s+LA\s+NACION\s+ARGENTINA\b", text, flags=re.IGNORECASE)
                    if m:
                        totals["bank_name"] = "BANCO DE LA NACION ARGENTINA"
            if totals["bank_name"] is None and totals.get("bank_patagonia"):
                totals["bank_name"] = "BANCO PATAGONIA S.A."

            card_by_brand = _infer_card_from_text(text) or _infer_card_from_filename(fname)

            if totals["card_name"] is None:
                m = re.search(r"\bTARJETA\s+DE\s+(DEBITO|CR[EÉ]DITO)[^\n]{0,30}\b", text, flags=re.IGNORECASE)
                if m:
                    totals["card_name"] = re.sub(r"\s+", " ", m.group(0)).strip()
                else:
                    m = re.search(r"\bTARJETA\s+DE\s+(DEBITO|CR[EÉ]DITO)\b", text, flags=re.IGNORECASE)
                    if m:
                        totals["card_name"] = re.sub(r"\s+", " ", m.group(0)).strip()

            if card_by_brand and (totals["card_name"] is None or _is_generic_card_label(totals["card_name"])):
                totals["card_name"] = card_by_brand

            if totals["period"] is None:
                m = re.search(
                    r"\b(ENERO|FEBRERO|MARZO|ABRIL|MAYO|JUNIO|JULIO|AGOSTO|SEPTIEMBRE|SETIEMBRE|OCTUBRE|NOVIEMBRE|DICIEMBRE)\s+\d{4}\b",
                    text,
                    flags=re.IGNORECASE,
                )
                if m:
                    totals["period"] = re.sub(r"\s+", " ", m.group(0).upper()).strip()
                else:
                    m2 = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", text)
                    if m2:
                        totals["period"] = m2.group(1)

            # Encabezado: capturar importes alrededor de los labels y asignar por consistencia
            if totals["total_presentado"] is None or totals["neto_header"] is None:
                hdr_amounts = _extract_header_amounts(text)
                if hdr_amounts:
                    # Tomar los dos mayores como total/ neto (total >= neto)
                    vals = sorted(set(hdr_amounts), reverse=True)
                    if len(vals) >= 2:
                        tp = vals[0]
                        neto = vals[1]
                        totals["total_presentado"] = totals["total_presentado"] or tp
                        totals["neto_header"] = totals["neto_header"] or neto
                    elif len(vals) == 1:
                        if totals["total_presentado"] is None:
                            totals["total_presentado"] = vals[0]

            # Encabezado específico Patagonia
            if totals.get("bank_patagonia"):
                ph = _extract_patagonia_header(text)
                if ph:
                    totals["total_presentado"] = totals["total_presentado"] or ph.get("total_presentado")
                    totals["total_descuento"] = totals.get("total_descuento") or ph.get("total_descuento")
                    totals["saldo"] = totals.get("saldo") or ph.get("saldo")

            # Totales diarios
            ventas = _sum_matches(r"VENTAS\s*C[/ ]DESCUENTO\s*CONTADO\+?\s*\$?\s*([0-9\.\,]+)", text)
            arancel = _sum_matches(r"ARANCEL-?\s*\$?\s*([0-9\.\,]+)", text)
            iva = _sum_matches(r"IVA\s*CRED[^0-9]*\$?\s*([0-9\.\,]+)", text)
            ret_iibb = _sum_matches(r"RETENCION\s*ING[^0-9]*\$?\s*([0-9\.\,]+)", text)
            ret_iva = _sum_matches(r"(?:PERCEPCION|RETENCION)\s*IVA[^0-9]*\$?\s*([0-9\.\,]+)", text)
            ret_gan = _sum_matches(r"RETENCION\s*GANANCIAS[^0-9]*\$?\s*([0-9\.\,]+)", text)
            neto = _sum_matches(r"IMPORTE\s*NETO\s*DE\s*PAGOS\s*\$?\s*([0-9\.\,]+)", text)

            if any(v > 0 for v in (ventas, arancel, iva, ret_iibb, ret_iva, ret_gan, neto)):
                totals["has_daily"] = True
                totals["ventas_sum"] += ventas
                totals["arancel_sum"] += arancel
                totals["iva_sum"] += iva
                totals["ret_iibb_sum"] += ret_iibb
                totals["ret_iva_sum"] += ret_iva
                totals["ret_gan_sum"] += ret_gan
                totals["neto_sum"] += neto

            # Extraer filas diarias (bloques) solo si no es Banco Nación
            if not totals.get("bank_nacion"):
                for block in _extract_daily_blocks(text, next_head):
                    row = _parse_daily_block(block)
                    if row:
                        key = tuple(sorted((row.get("concepts") or {}).items()))
                        if "_seen_rows" not in totals:
                            totals["_seen_rows"] = set()
                        if key in totals["_seen_rows"]:
                            continue
                        totals["_seen_rows"].add(key)
                        totals["daily_rows"].append(row)

        # Para Banco Nación: reconstruir bloques en secuencia (más confiable)
        if totals.get("bank_nacion"):
            totals["daily_rows"] = _extract_blocks_sequential(page_texts)
        # Para Patagonia: extraer desglose de descuentos
        if totals.get("bank_patagonia"):
            totals["patagonia_desglose"] = _extract_patagonia_desglose(page_texts)

    totals.pop("_seen_rows", None)
    if totals["bank_name"] is None and totals.get("bank_nacion"):
        totals["bank_name"] = "BANCO DE LA NACION ARGENTINA"
    if totals.get("bank_patagonia") and totals["bank_name"] is None:
        totals["bank_name"] = "BANCO PATAGONIA S.A."
    if totals.get("card_name") is None and files:
        totals["card_name"] = _infer_card_from_filename(Path(files[0]).name)
    if totals.get("period") is None and totals.get("bank_patagonia") and files:
        totals["period"] = _infer_period_from_filename(Path(files[0]).name)
    return totals


def _extract_patagonia_desglose(page_texts: List[str]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    text = "\n".join(page_texts)
    idx = text.upper().find("DESGLOSE DE DESCUENTOS")
    if idx < 0:
        return items
    block = text[idx : idx + 2000]
    # cortar en separador si aparece
    sep = block.find("____")
    if sep > 0:
        block = block[:sep]
    pending_label = ""
    for ln in block.splitlines():
        if "$" not in ln:
            # guardar posibles etiquetas de sección para líneas con importe en la siguiente línea
            clean = re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9/%\s\.\-]", " ", ln.upper())).strip()
            clean = re.sub(r"\s{2,}", " ", clean)
            if clean and not clean.startswith("DESGLOSE"):
                pending_label = clean
            continue
        if "U$S" in ln.upper():
            continue
        nums = re.findall(r"([0-9][0-9\.\,]*)", ln)
        if not nums:
            continue
        amount = _parse_number(nums[-1])
        if amount <= 0:
            continue
        label = re.sub(r"\s+", " ", re.sub(r"[^A-Z0-9/%\s\.\-]", " ", ln.upper())).strip()
        label = re.sub(r"\b[0-9][0-9\.\,]*\b", "", label).strip()
        label = re.sub(r"\s{2,}", " ", label)
        if label in ("%", "TASA %") and pending_label:
            label = pending_label
        if label.startswith("TASA") and pending_label:
            label = f"{pending_label} - {label}"
        if label in ("%", "TASA %"):
            continue
        if label:
            items.append({"label": label, "amount": amount})
    # Si existe Base Imponible IVA, descartar Arancel Tj.Crédito (mismo importe)
    has_base = any("BASE IMPONIBLE IVA" in it["label"] for it in items)
    if has_base:
        items = [
            it
            for it in items
            if "ARANCEL TJ.C" not in it["label"]
            and "ARANCEL TJ.D" not in it["label"]
            and "CARGO POR SERVICIO" not in it["label"]
        ]
    return items


def _extract_daily_blocks(text: str, next_head: str = "") -> List[str]:
    blocks: List[str] = []
    if not text:
        return blocks
    combined = text + ("\n" + next_head if next_head else "")
    lines = [ln for ln in combined.splitlines() if ln.strip()]

    # Anclar por "IMPORTE NETO DE PAGOS" y tomar ventana de líneas anteriores
    neto_idxs = [i for i, ln in enumerate(lines) if re.search(r"IMPORTE\s*NETO\s*DE\s*PAGOS", ln, flags=re.IGNORECASE)]
    for idx in neto_idxs:
        start = max(0, idx - 12)
        end = min(len(lines), idx + 3)
        block = "\n".join(lines[start:end])
        blocks.append(block)

    # También anclar por "VENTAS..." por si hay bloques sin neto por OCR
    ventas_idxs = [i for i, ln in enumerate(lines) if re.search(r"VENTAS\s*C[/ ]DESCUENTO\s*CONTADO", ln, flags=re.IGNORECASE)]
    for idx in ventas_idxs:
        start = max(0, idx - 2)
        end = min(len(lines), idx + 10)
        block = "\n".join(lines[start:end])
        blocks.append(block)

    # Deduplicar bloques
    uniq: List[str] = []
    seen = set()
    for b in blocks:
        key = b[:200]
        if key in seen:
            continue
        seen.add(key)
        uniq.append(b)
    return uniq


def _parse_daily_block(block: str) -> Optional[Dict[str, Any]]:
    if not block:
        return None
    # extraer importes por línea, tomando el ÚLTIMO número de cada línea
    lines_all = [ln.strip() for ln in block.splitlines() if ln.strip()]
    if not lines_all:
        return None

    # limitar a las líneas del bloque de totales (entre VENTAS... e IMPORTE NETO...)
    start_idx = None
    end_idx = None
    for i, ln in enumerate(lines_all):
        if start_idx is None and re.search(r"VENTAS\s*C[/ ]DESCUENTO\s*CONTADO", ln, flags=re.IGNORECASE):
            start_idx = i
        if re.search(r"IMPORTE\s*NETO\s*DE\s*PAGOS", ln, flags=re.IGNORECASE):
            end_idx = i
            if start_idx is None:
                start_idx = 0
            break
    if start_idx is None or end_idx is None or end_idx < start_idx:
        return None
    lines = lines_all[start_idx : end_idx + 1]

    concept_map: Dict[str, float] = {}
    def _canon_label(ln: str) -> Optional[str]:
        t = re.sub(r"\s+", " ", ln.upper()).strip()
        if "VENTAS C/DESCUENTO CONTADO" in t:
            return "VENTAS C/DESCUENTO CONTADO"
        if "ARANCEL" in t:
            return "ARANCEL"
        if "IVA CRED.FISC.COMERCIO S/ARANC" in t:
            return "IVA CRED.FISC.COMERCIO S/ARANC 21,00%"
        if "IVA RI SERV.OPER. INT." in t or "IVA RI SERV.OPER. INT" in t:
            return "IVA RI SERV.OPER. INT."
        if "SERVICIO OPER. INTERNAC" in t or "SERV.OPER. INT" in t:
            return "SERVICIO OPER. INTERNAC."
        if "RETENCION ING.BRUTOS SIRTAC" in t:
            return "RETENCION ING.BRUTOS SIRTAC"
        if "PERCEPCION IVA R.G. 2408" in t:
            return "PERCEPCION IVA R.G. 2408 3,00 %"
        if "QR PERCEPCION IVA 3337" in t:
            return "QR PERCEPCION IVA 3337"
        if "QR RETENCION IIBB RIO NEGRO" in t:
            return "QR RETENCION IIBB RIO NEGRO"
        if "IMPORTE NETO DE PAGOS" in t:
            return "IMPORTE NETO DE PAGOS"
        return None

    for ln in lines:
        if not re.search(r"\d", ln):
            continue
        # Solo líneas con $ y concepto válido
        if "$" not in ln:
            continue
        label = _canon_label(ln)
        if not label:
            continue
        nums = re.findall(r"([0-9][0-9\.\,]*)", ln)
        if not nums:
            continue
        amount = _parse_number(nums[-1])
        if amount <= 0:
            continue
        concept_map[label] = amount

    if not concept_map:
        return None

    # Ya está filtrado por etiquetas canónicas

    # fecha: priorizar "F. Pres", luego "el día", luego cualquier fecha dd/mm/yyyy
    date_match = re.search(r"F\.\s*Pres\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", block, flags=re.IGNORECASE)
    if not date_match:
        date_match = re.search(r"el\s+d[ií]a\s*([0-9]{2}/[0-9]{2}/[0-9]{4})", block, flags=re.IGNORECASE)
    if not date_match:
        dates = re.findall(r"([0-9]{2}/[0-9]{2}/[0-9]{4})", block)
        date_match = None
        if dates:
            date_match = dates[-1]
    fecha = date_match.group(1) if hasattr(date_match, "group") else (date_match or "")

    return {
        "fecha": fecha,
        "concepts": concept_map,
    }


def _totals_from_daily_rows(daily_rows: List[Dict[str, Any]]) -> Dict[str, float]:
    totals: Dict[str, float] = {}
    for r in daily_rows:
        concepts = r.get("concepts") or {}
        for label, val in concepts.items():
            totals[label] = totals.get(label, 0.0) + val
    return totals


def _build_output_from_daily_columns(daily_rows: List[Dict[str, Any]], neto_header: Optional[float] = None) -> str:
    totals = _totals_from_daily_rows(daily_rows)
    if not totals:
        return ""
    preferred = [
        "VENTAS C/DESCUENTO CONTADO",
        "ARANCEL",
        "IVA CRED.FISC.COMERCIO S/ARANC 21,00%",
        "RETENCION ING.BRUTOS SIRTAC",
        "PERCEPCION IVA R.G. 2408 3,00 %",
        "RETENCION IVA",
        "RETENCION GANANCIAS",
        "IMPORTE NETO DE PAGOS",
    ]
    cols = [c for c in preferred if c in totals] + sorted(c for c in totals if c not in preferred)
    # Si el neto del encabezado existe y difiere, respetar neto y recalcular ventas para balancear
    if neto_header is not None and "IMPORTE NETO DE PAGOS" in totals:
        try:
            neto_calc = float(totals.get("IMPORTE NETO DE PAGOS", 0.0))
            if abs(neto_calc - float(neto_header)) > max(1.0, float(neto_header) * 0.002):
                totals["IMPORTE NETO DE PAGOS"] = float(neto_header)
                cargos_sum = 0.0
                for k, v in totals.items():
                    if k in ("VENTAS C/DESCUENTO CONTADO", "IMPORTE NETO DE PAGOS"):
                        continue
                    cargos_sum += float(v)
                totals["VENTAS C/DESCUENTO CONTADO"] = float(neto_header) + cargos_sum
        except Exception:
            pass

    lines: List[str] = []
    for c in cols:
        val = totals.get(c, 0.0)
        t = c.upper()
        if "VENTAS" in t:
            out = abs(val)
        else:
            out = -abs(val)
        lines.append(f"{c}|{out:.2f}")
    return "\n".join(lines) + "\n"


def _filter_daily_rows_for_bank_nacion(
    daily_rows: List[Dict[str, Any]],
    total_presentado: Optional[float],
    neto_header: Optional[float],
    log_path: Optional[Path],
) -> List[Dict[str, Any]]:
    if not daily_rows:
        return daily_rows

    def totals_for(rows: List[Dict[str, Any]]) -> Dict[str, float]:
        t = _totals_from_daily_rows(rows)
        return {
            "ventas": float(t.get("VENTAS C/DESCUENTO CONTADO", 0.0)),
            "neto": float(t.get("IMPORTE NETO DE PAGOS", 0.0)),
        }

    def close(a: float, b: float) -> bool:
        return a > 0 and abs(a - b) <= max(1.0, b * 0.002)

    base = totals_for(daily_rows)
    ok_ventas = total_presentado is None or close(base["ventas"], float(total_presentado))
    ok_neto = neto_header is None or close(base["neto"], float(neto_header))
    if ok_ventas and ok_neto:
        return daily_rows

    # Filtro: solo bloques con ventas y neto (evita bloques parciales)
    filtered = [
        r
        for r in daily_rows
        if (r.get("concepts") or {}).get("VENTAS C/DESCUENTO CONTADO", 0.0) > 0
        and (r.get("concepts") or {}).get("IMPORTE NETO DE PAGOS", 0.0) > 0
    ]
    if not filtered:
        return daily_rows

    base2 = totals_for(filtered)
    ok_ventas2 = total_presentado is None or close(base2["ventas"], float(total_presentado))
    ok_neto2 = neto_header is None or close(base2["neto"], float(neto_header))
    if log_path:
        _write_log(
            log_path,
            f"Filtro bloques parciales. Ventas {base['ventas']:.2f}->{base2['ventas']:.2f} "
            f"Neto {base['neto']:.2f}->{base2['neto']:.2f}",
        )
    return filtered if (ok_ventas2 and ok_neto2) else daily_rows


def _build_header_lines(bank: Optional[str], card: Optional[str], period: Optional[str]) -> str:
    bank_s = (bank or "").strip()
    card_s = (card or "").strip()
    period_s = (period or "").strip()

    # Convertir período "MES YYYY" a "dd/MM/YYYY" (último día del mes)
    period_date = period_s
    if period_s:
        # Si ya viene en dd/mm/yyyy, respetarlo
        if re.match(r"^\d{2}/\d{2}/\d{4}$", period_s):
            period_date = period_s.replace("/", "-")
        else:
            m_num = re.match(r"^(\d{1,2})[/-](\d{4})$", period_s)
            if m_num:
                month = int(m_num.group(1))
                year = int(m_num.group(2))
                if 1 <= month <= 12:
                    last_day = calendar.monthrange(year, month)[1]
                    period_date = f"{last_day:02d}-{month:02d}-{year}"

        m = re.match(
            r"^(ENERO|FEBRERO|MARZO|ABRIL|MAYO|JUNIO|JULIO|AGOSTO|SEPTIEMBRE|SETIEMBRE|OCTUBRE|NOVIEMBRE|DICIEMBRE)\s+(\d{4})$",
            period_s,
            flags=re.IGNORECASE,
        )
        if m:
            month_map = {
                "ENERO": 1,
                "FEBRERO": 2,
                "MARZO": 3,
                "ABRIL": 4,
                "MAYO": 5,
                "JUNIO": 6,
                "JULIO": 7,
                "AGOSTO": 8,
                "SEPTIEMBRE": 9,
                "SETIEMBRE": 9,
                "OCTUBRE": 10,
                "NOVIEMBRE": 11,
                "DICIEMBRE": 12,
            }
            month = month_map.get(m.group(1).upper())
            year = int(m.group(2))
            if month:
                last_day = calendar.monthrange(year, month)[1]
                period_date = f"{last_day:02d}-{month:02d}-{year}"

    # Concepto breve y claro, priorizando período
    bank_short = bank_s
    if re.search(r"BANCO\s+DE\s+LA\s+NACION\s+ARGENTINA", bank_s, flags=re.IGNORECASE):
        bank_short = "BANCO NACION"
    card_short = card_s.replace("TARJETA DE ", "").strip()
    if not card_short:
        card_short = card_s
    concept = f"LIQ {period_date} {card_short} {bank_short}".strip()
    concept = re.sub(r"\s+", " ", concept)
    if len(concept) > 50:
        concept = concept[:50].rstrip()

    lines = [
        bank_s,
        card_s,
        period_date,
        concept,
        "CONCEPTO|IMPORTE",
    ]
    return "\n".join(lines) + "\n"


def _write_daily_control_file(outdir: Path, source_stem: str, daily_rows: List[Dict[str, Any]], only_if_bank_nacion: bool) -> Optional[Path]:
    if not daily_rows:
        return None
    if only_if_bank_nacion is False:
        return None
    path = outdir / f"{source_stem}.xls"

    # columnas dinámicas: unión de conceptos por día
    col_set = set()
    for r in daily_rows:
        for k in (r.get("concepts") or {}).keys():
            col_set.add(k)
    # orden preferido Banco Nación
    preferred = [
        "VENTAS C/DESCUENTO CONTADO",
        "ARANCEL",
        "IVA CRED.FISC.COMERCIO S/ARANC 21,00%",
        "RETENCION ING.BRUTOS SIRTAC",
        "PERCEPCION IVA R.G. 2408 3,00 %",
        "RETENCION GANANCIAS",
        "IMPORTE NETO DE PAGOS",
    ]
    cols = [c for c in preferred if c in col_set] + sorted(c for c in col_set if c not in preferred)

    header = "LINEA\t" + "\t".join(cols) + "\tSUMA_CARGOS\tCHECK"
    lines = [header]
    totals: Dict[str, float] = {c: 0.0 for c in cols}
    total_suma_cargos = 0.0
    total_check = 0.0
    for idx, r in enumerate(daily_rows, start=1):
        row_vals = []
        concepts = r.get("concepts") or {}
        ventas = float(concepts.get("VENTAS C/DESCUENTO CONTADO", 0.0))
        neto = float(concepts.get("IMPORTE NETO DE PAGOS", 0.0))
        suma_cargos = 0.0
        for c in cols:
            val = concepts.get(c, 0.0)
            totals[c] += val
            row_vals.append(f"{val:.2f}")
            if c not in ("VENTAS C/DESCUENTO CONTADO", "IMPORTE NETO DE PAGOS"):
                suma_cargos += float(val)
        check = ventas - (neto + suma_cargos)
        total_suma_cargos += suma_cargos
        total_check += check
        lines.append(f"{idx}\t" + "\t".join(row_vals) + f"\t{suma_cargos:.2f}\t{check:.2f}")
    lines.append(
        "TOTAL\t"
        + "\t".join(f"{totals[c]:.2f}" for c in cols)
        + f"\t{total_suma_cargos:.2f}\t{total_check:.2f}"
    )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _parse_output_totals(text: str) -> Dict[str, float]:
    totals = {
        "TARJETA": 0.0,
        "BANCO": 0.0,
        "GASTO": 0.0,
        "IVA_CREDITO": 0.0,
        "RET_IVA": 0.0,
        "RET_IIBB": 0.0,
        "RET_GAN": 0.0,
        "OTROS": 0.0,
    }
    for ln in text.splitlines():
        if "|" not in ln:
            continue
        concept, total = ln.split("|", 1)
        cat = _classify_concept_name(concept.strip())
        if cat not in totals:
            cat = "OTROS"
        totals[cat] = _parse_number(total.strip())
    return totals


def _format_output_from_totals(totals: Dict[str, float]) -> str:
    order = ["TARJETA", "BANCO", "GASTO", "IVA_CREDITO", "RET_IVA", "RET_IIBB", "RET_GAN", "OTROS"]
    out_lines: List[str] = []
    for cat in order:
        val = totals.get(cat, 0.0)
        if cat == "TARJETA":
            val = abs(val)
        elif cat == "OTROS":
            val = float(f"{val:.2f}")
        else:
            val = -abs(val)
        if abs(val) < 0.005:
            val = 0.0
        out_lines.append(f"{cat}|{val:.2f}")
    return "\n".join(out_lines) + "\n"


def _apply_pdf_overrides(text: str, pdf_totals: Dict[str, float], log_path: Optional[Path]) -> str:
    totals = _parse_output_totals(text)
    changed = False

    has_daily = bool(pdf_totals.get("has_daily"))
    ventas_sum = float(pdf_totals.get("ventas_sum") or 0.0)
    arancel_sum = float(pdf_totals.get("arancel_sum") or 0.0)
    iva_sum = float(pdf_totals.get("iva_sum") or 0.0)
    ret_iva_sum = float(pdf_totals.get("ret_iva_sum") or 0.0)
    ret_iibb_sum = float(pdf_totals.get("ret_iibb_sum") or 0.0)
    ret_gan_sum = float(pdf_totals.get("ret_gan_sum") or 0.0)
    neto_sum = float(pdf_totals.get("neto_sum") or 0.0)

    total_presentado = pdf_totals.get("total_presentado")
    neto_header = pdf_totals.get("neto_header")
    bank_nacion = bool(pdf_totals.get("bank_nacion"))
    bank_patagonia = bool(pdf_totals.get("bank_patagonia"))
    header = _build_header_lines(pdf_totals.get("bank_name"), pdf_totals.get("card_name"), pdf_totals.get("period"))

    # Banco Patagonia: usar desglose de descuentos sin XLS de control
    if bank_patagonia and pdf_totals.get("patagonia_desglose"):
        lines = []
        tp = pdf_totals.get("total_presentado")
        saldo = pdf_totals.get("saldo")
        if tp is not None:
            lines.append(f"TOTAL PRESENTADO|{float(tp):.2f}")
        for it in pdf_totals["patagonia_desglose"]:
            lines.append(f"{it['label']}|{-abs(float(it['amount'])):.2f}")
        if saldo is not None:
            lines.append(f"SALDO|{-abs(float(saldo)):.2f}")
        if log_path and tp is None:
            _write_log(log_path, "WARN: No se detectó TOTAL PRESENTADO en Patagonia.")
        out_text = "\n".join(lines) + "\n"
        if log_path:
            _write_log(log_path, "Asiento Patagonia generado desde DESGLOSE DE DESCUENTOS.")
        return header + out_text

    # Si hay totales diarios bien formados, usar esos para el asiento (Banco Nación)
    if bank_nacion and pdf_totals.get("daily_rows"):
        rows = _filter_daily_rows_for_bank_nacion(
            pdf_totals["daily_rows"],
            pdf_totals.get("total_presentado"),
            pdf_totals.get("neto_header"),
            log_path,
        )
        out_text = _build_output_from_daily_columns(rows, pdf_totals.get("neto_header"))
        if out_text:
            if log_path:
                _write_log(log_path, "Asiento generado desde columnas de totales diarios (Banco Nación).")
            return header + out_text

    if has_daily and arancel_sum > 0:
        # Si el IVA viene inconsistente, recalcular como 21% del arancel.
        iva_calc = round(arancel_sum * 0.21, 2)
        if iva_sum <= 0 or abs(iva_sum - iva_calc) > 0.05:
            iva_sum = iva_calc

    # Priorizar totales diarios si existen
    if has_daily and ventas_sum > 0:
        if bank_nacion:
            # Banco Nación: la línea "VENTAS C/DESCUENTO CONTADO" es el total presentado.
            totals["TARJETA"] = ventas_sum
        else:
            totals["TARJETA"] = ventas_sum
        changed = True
    elif total_presentado is not None:
        totals["TARJETA"] = float(total_presentado)
        changed = True

    if has_daily and neto_sum > 0:
        totals["BANCO"] = neto_sum
        changed = True
    elif has_daily and ventas_sum > 0 and any(v > 0 for v in (arancel_sum, iva_sum, ret_iva_sum, ret_iibb_sum, ret_gan_sum)):
        totals["BANCO"] = ventas_sum - (arancel_sum + iva_sum + ret_iva_sum + ret_iibb_sum + ret_gan_sum)
        changed = True
    elif neto_header is not None:
        totals["BANCO"] = float(neto_header)
        changed = True

    if has_daily:
        if arancel_sum > 0:
            totals["GASTO"] = -arancel_sum
            changed = True
        if iva_sum > 0:
            totals["IVA_CREDITO"] = -iva_sum
            changed = True
        if ret_iva_sum > 0:
            totals["RET_IVA"] = -ret_iva_sum
            changed = True
        if ret_iibb_sum > 0:
            totals["RET_IIBB"] = -ret_iibb_sum
            changed = True
        if ret_gan_sum > 0:
            totals["RET_GAN"] = -ret_gan_sum
            changed = True

    if not changed:
        return header + text

    # Normalizar signos (TARJETA positiva, resto negativo) y recalcular OTROS para suma 0
    norm = {
        "TARJETA": abs(totals["TARJETA"]),
        "BANCO": -abs(totals["BANCO"]),
        "GASTO": -abs(totals["GASTO"]),
        "IVA_CREDITO": -abs(totals["IVA_CREDITO"]),
        "RET_IVA": -abs(totals["RET_IVA"]),
        "RET_IIBB": -abs(totals["RET_IIBB"]),
        "RET_GAN": -abs(totals["RET_GAN"]),
    }
    sum_except_otros = sum(norm.values())
    # Si la suma es exacta, no forzar OTROS
    if abs(sum_except_otros) < 0.01:
        norm["OTROS"] = 0.0
    else:
        norm["OTROS"] = -sum_except_otros

    if log_path:
        _write_log(log_path, f"Overrides PDF aplicados. TARJETA={norm['TARJETA']:.2f} BANCO={norm['BANCO']:.2f}")
        _write_log(log_path, f"Totales diarios: ventas={ventas_sum:.2f} arancel={arancel_sum:.2f} iva={iva_sum:.2f} ret_iva={ret_iva_sum:.2f} ret_iibb={ret_iibb_sum:.2f} ret_gan={ret_gan_sum:.2f} neto={neto_sum:.2f}")

    return header + _format_output_from_totals(norm)


def _ensure_writable_outdir(preferred: str) -> Path:
    """Devuelve un outdir escribible. Si falla, usa TEMP del sistema."""
    cand = Path(preferred or tempfile.gettempdir())
    try:
        cand.mkdir(parents=True, exist_ok=True)
        test_path = cand / f".__write_test_{os.getpid()}_{int(time.time())}.tmp"
        test_path.write_text("ok", encoding="utf-8")
        test_path.unlink(missing_ok=True)
        return cand
    except Exception:
        fallback = Path(tempfile.gettempdir())
        try:
            fallback.mkdir(parents=True, exist_ok=True)
            return fallback
        except Exception:
            return Path(".")


def _normalize_outdir_arg(raw: str) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    if len(s) >= 2 and ((s[0] == '"' and s[-1] == '"') or (s[0] == "'" and s[-1] == "'")):
        s = s[1:-1].strip()
    s = s.rstrip(" '\"")
    return s


def _ensure_writable_outdir_with_file_fallback(preferred: str, first_input_file: str) -> Path:
    """Prioriza outdir solicitado; si no se puede, usa carpeta del archivo fuente; último recurso TEMP."""
    preferred = _normalize_outdir_arg(preferred)
    # 1) Intentar outdir solicitado (si viene por parámetro)
    if preferred and preferred.strip():
        cand = Path(preferred.strip())
        try:
            cand.mkdir(parents=True, exist_ok=True)
            test_path = cand / f".__write_test_{os.getpid()}_{int(time.time())}.tmp"
            test_path.write_text("ok", encoding="utf-8")
            test_path.unlink(missing_ok=True)
            return cand
        except Exception:
            pass

    # 2) Fallback: carpeta del archivo de entrada
    try:
        src_dir = Path(first_input_file).resolve().parent
        src_dir.mkdir(parents=True, exist_ok=True)
        test_path = src_dir / f".__write_test_{os.getpid()}_{int(time.time())}.tmp"
        test_path.write_text("ok", encoding="utf-8")
        test_path.unlink(missing_ok=True)
        return src_dir
    except Exception:
        pass

    # 3) Último recurso: TEMP
    return _ensure_writable_outdir("")


def _ensure_outdir_preferred_or_fail(preferred: str) -> Path:
    cand = Path(_normalize_outdir_arg(preferred))
    if not str(cand):
        raise SystemExit("ERROR: outdir solicitado vacío.")
    try:
        cand.mkdir(parents=True, exist_ok=True)
        test_path = cand / f".__write_test_{os.getpid()}_{int(time.time())}.tmp"
        test_path.write_text("ok", encoding="utf-8")
        test_path.unlink(missing_ok=True)
        return cand
    except Exception as e:
        raise SystemExit(f"ERROR: No se puede escribir en outdir solicitado: {cand} ({e})")


# ----------------------------
# Prompt
# ----------------------------
DEFAULT_PROMPT = r"""
Vas a analizar una liquidación de tarjeta (débito o crédito) con varias páginas.

Objetivo: devolver SOLO texto con 2 columnas: CONCEPTO|TOTAL

Reglas clave:
- La primer línea es el TOTAL PRESENTADO (importe positivo).
- La línea BANCO es el NETO DE PAGOS / NETO A COBRAR / IMPORTE NETO / A ACREDITAR / LIQUIDADO / ACREDITADO.
- Todas las demás líneas son importes negativos.
- La suma de TODAS las líneas debe ser 0 (cero). Si faltan conceptos, completar con 0 o ajustar OTROS para cerrar.

Cómo identificar montos (buscar sinónimos, ignorar ubicación):
- TOTAL PRESENTADO: "Total presentado", "Total liquidación", "Total liq. tarjeta", "Importe total".
- BANCO: "Neto de pagos", "Neto a cobrar", "Importe neto", "A acreditar", "Total liquidado", "A depositar", "Acreditado".
- GASTO: "Arancel", "Comisión", "Gastos", "Cargo".
- IVA CREDITO: "IVA créd.", "IVA crédito fiscal", "IVA s/arancel".
- RET IVA: "Retención IVA", "Percepción IVA", "R.G. 2408".
- RET IIBB: "Retención IIBB", "Ingresos Brutos", "SIRTAC", "IIBB".
- RET GAN: "Retención Ganancias", "RG 830".
- OTROS: cualquier ajuste o concepto no clasificado.

Validaciones:
- Si BANCO es mayor que TARJETA y no hay explicación, revisá si están invertidos.
- Si no encontrás un concepto, poné 0.
- Asegurá que la suma total sea 0 (ajustar OTROS si hace falta).

Formato de salida OBLIGATORIO:
CONCEPTO|TOTAL
"""


def read_prompt(prompt_file: Optional[str]) -> str:
    if prompt_file:
        p = Path(prompt_file)
        if p.exists():
            return p.read_text(encoding="utf-8", errors="replace")
    return DEFAULT_PROMPT


# ----------------------------
# Conversión de archivos a bloques para OpenAI
# ----------------------------
def file_to_content_block(file_path: str) -> Dict[str, Any]:
    ext = Path(file_path).suffix.lower()
    data = Path(file_path).read_bytes()

    if ext in (".jpg", ".jpeg", ".png", ".webp"):
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


def _count_pdf_pages(file_path: str) -> int:
    if PdfReader is None:
        return 1
    try:
        return max(1, len(PdfReader(file_path).pages))
    except Exception:
        return 1


def _pdf_to_chunked_blocks(file_path: str, pages_per_chunk: int) -> List[Dict[str, Any]]:
    pages_per_chunk = int(pages_per_chunk or 0)
    if pages_per_chunk <= 0:
        return [file_to_content_block(file_path)]

    if PdfReader is None or PdfWriter is None:
        raise SystemExit("ERROR: Para dividir PDFs grandes necesitás instalar pypdf: pip install pypdf")

    reader = PdfReader(file_path)
    total = len(reader.pages)
    if total <= pages_per_chunk:
        return [file_to_content_block(file_path)]

    blocks: List[Dict[str, Any]] = []
    stem = Path(file_path).stem
    for start in range(0, total, pages_per_chunk):
        end = min(total, start + pages_per_chunk)
        writer = PdfWriter()
        for i in range(start, end):
            writer.add_page(reader.pages[i])

        buf = io.BytesIO()
        writer.write(buf)
        b64 = base64.b64encode(buf.getvalue()).decode("utf-8")
        blocks.append(
            {
                "type": "input_file",
                "filename": f"{stem}_p{start + 1:03d}-{end:03d}.pdf",
                "file_data": f"data:application/pdf;base64,{b64}",
            }
        )

    return blocks


def _is_request_too_large_error(exc: Exception) -> bool:
    """Detecta errores típicos de límite de tamaño/tokens para reintentar en chunks."""
    msg = str(exc or "").lower()
    return (
        "request too large" in msg
        or "tokens per min" in msg
        or ("rate_limit_exceeded" in msg and "requested" in msg)
    )


def file_to_content_blocks(file_path: str, tiles: int = 1, pdf_chunk_pages: int = 0) -> List[Dict[str, Any]]:
    ext = Path(file_path).suffix.lower()
    if ext == ".pdf":
        return _pdf_to_chunked_blocks(file_path, pdf_chunk_pages)

    if tiles <= 1 or ext == ".pdf":
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


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        add_help=True,
        description="Lector de liquidaciones -> TXT (multipágina). Usa OPENAI_API_KEY (env o .env junto al exe/script).",
    )
    parser.add_argument("files", nargs="*", help="Archivos de entrada (imágenes/PDF) en orden de páginas")
    parser.add_argument("--outdir", default="", help="Carpeta de salida. Default: TEMP del sistema")
    parser.add_argument("--prompt-file", default="", help="Archivo .txt con prompt personalizado")
    parser.add_argument("--model", default="gpt-4o-mini", help="Modelo a usar (default: gpt-4o-mini)")
    parser.add_argument("--gui", action="store_true", help="Muestra ventana de progreso (no altera stdout)")
    parser.add_argument(
        "--per-page",
        action="store_true",
        help="Procesa cada archivo/página por separado y luego unifica (mejora extracción en docs largos).",
    )
    parser.add_argument(
        "--auto",
        action="store_true",
        help="Auto-ajusta parámetros (tile y per-page) según cantidad de páginas.",
    )
    parser.add_argument(
        "--tile",
        type=int,
        default=1,
        help="Divide cada página en N franjas horizontales (solo imágenes). Requiere Pillow.",
    )
    parser.add_argument(
        "--pdf-chunk-pages",
        type=int,
        default=0,
        help="Divide PDFs en bloques de N páginas para documentos grandes. 0 = no dividir.",
    )
    args = parser.parse_args()

    ui = None
    if args.gui:
        try:
            ui = StatusUI()
            ui.push("STATUS:Inicializando...")
        except Exception:
            ui = None

    def log(msg: str):
        if ui:
            ui.push(msg)

    def status(msg: str):
        if ui:
            ui.push(f"STATUS:{msg}")

    result = {"out_path": None, "error": None, "log_path": None}

    def worker():
        try:
            status("Cargando .env / variables...")
            load_env_near_app()

            use_backend = backend_enabled()
            api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
            if not use_backend and not api_key:
                raise SystemExit(
                    "ERROR: No está configurada OPENAI_API_KEY ni IA_BACKEND_URL. "
                    "Definí OPENAI_API_KEY (modo local) o IA_BACKEND_URL + IA_CLIENT_ID + IA_CLIENT_SECRET (modo backend)."
                )

            if not args.files:
                raise SystemExit("ERROR: Debés pasar al menos 1 archivo por parámetro.")
            if len(args.files) > 100:
                raise SystemExit("ERROR: Máximo 100 archivos de entrada.")

            if args.tile < 1 or args.tile > 6:
                raise SystemExit("ERROR: --tile debe ser un entero entre 1 y 6.")
            if args.pdf_chunk_pages < 0:
                raise SystemExit("ERROR: --pdf-chunk-pages no puede ser negativo.")

            status("Validando archivos...")
            for f in args.files:
                if not Path(f).exists():
                    raise SystemExit(f"ERROR: No existe el archivo: {f}")

            pdf_totals = _extract_pdf_totals(args.files)

            # Auto-ajuste según páginas reales (si se puede leer el PDF).
            if args.auto:
                effective_pages = 0
                for f in args.files:
                    if Path(f).suffix.lower() == ".pdf":
                        effective_pages += _count_pdf_pages(f)
                    else:
                        effective_pages += 1

                if effective_pages <= 2:
                    args.tile = 3
                    args.per_page = False
                    if args.pdf_chunk_pages == 0:
                        args.pdf_chunk_pages = 0
                elif effective_pages <= 8:
                    args.tile = 4
                    args.per_page = True
                    if args.pdf_chunk_pages == 0:
                        args.pdf_chunk_pages = 4
                else:
                    args.tile = 5
                    args.per_page = True
                    if args.pdf_chunk_pages == 0:
                        args.pdf_chunk_pages = 5

            status("Preparando salida...")
            base = safe_basename(args.files[0])
            source_stem = Path(args.files[0]).stem
            ts = dt.datetime.now().strftime("%Y%m%d_%H%M%S")
            requested_outdir = _normalize_outdir_arg(args.outdir)
            if requested_outdir:
                outdir = _ensure_outdir_preferred_or_fail(requested_outdir)
            else:
                outdir = _ensure_writable_outdir_with_file_fallback("", args.files[0])
            src_log_name = Path(args.files[0]).with_suffix(".log").name
            log_path = Path(outdir) / src_log_name
            try:
                log_path.unlink(missing_ok=True)
            except Exception:
                pass
            result["log_path"] = str(log_path)
            _write_log(log_path, f"Inicio proceso. Archivos: {', '.join(args.files)}")

            status("Cargando prompt...")
            prompt = read_prompt(args.prompt_file.strip() or None)
            if "concepto|total" not in prompt.lower():
                prompt = "Respondé solo con texto en formato CONCEPTO|TOTAL.\n" + prompt
            _write_log(
                log_path,
                f"Modelo: {args.model} | per-page: {args.per_page} | tile: {args.tile} | pdf-chunk-pages: {args.pdf_chunk_pages}",
            )

            status("Armando contenido...")
            log(
                f"Modelo: {args.model} | per-page: {args.per_page} | tile: {args.tile} | pdf-chunk-pages: {args.pdf_chunk_pages}"
            )
            content = [{"type": "input_text", "text": prompt}]
            total_files = len(args.files)
            for i, f in enumerate(args.files, start=1):
                status(f"Adjuntando página {i}/{total_files}...")
                log(f"Archivo: {f}")
                content.extend(file_to_content_blocks(f, args.tile, args.pdf_chunk_pages))

            status("Analizando con Inteligencia Artificial...")
            log("Motor IA: Activo")
            client = None if use_backend else OpenAI(api_key=api_key)

            def call_model(content_blocks: List[Dict[str, Any]]) -> str:
                if use_backend:
                    out_text = call_backend(
                        content_blocks=content_blocks,
                        model=args.model,
                        max_output_tokens=4000,
                    )
                else:
                    resp = client.responses.create(
                        model=args.model,
                        max_output_tokens=4000,
                        input=[{"role": "user", "content": content_blocks}],
                    )

                    out_text = (getattr(resp, "output_text", None) or "").strip()
                    if not out_text:
                        try:
                            out_text = resp.output[0].content[0].text
                        except Exception:
                            parts = []
                            for item in getattr(resp, "output", []) or []:
                                for c in getattr(item, "content", []) or []:
                                    t = getattr(c, "text", None)
                                    if t:
                                        parts.append(t)
                            out_text = "\n".join(parts)

                if not out_text.strip():
                    raise SystemExit("ERROR: Respuesta vacía del modelo.")

                return out_text.strip()

            def build_units(force_pdf_page_split: bool = False) -> List[tuple[str, List[Dict[str, Any]]]]:
                units: List[tuple[str, List[Dict[str, Any]]]] = []
                for f in args.files:
                    ext = Path(f).suffix.lower()
                    if ext == ".pdf" and (force_pdf_page_split or args.per_page):
                        chunk = 1 if force_pdf_page_split else (args.pdf_chunk_pages if args.pdf_chunk_pages > 0 else 1)
                        pdf_blocks = _pdf_to_chunked_blocks(f, chunk)
                        for b in pdf_blocks:
                            units.append((f, [b]))
                    else:
                        units.append((f, file_to_content_blocks(f, args.tile, args.pdf_chunk_pages)))
                return units

            def run_units(units: List[tuple[str, List[Dict[str, Any]]]], status_label: str) -> str:
                page_results: List[str] = []
                total_units = len(units)
                t_units_start = time.time()
                for i, (src, blocks) in enumerate(units, start=1):
                    if i > 1:
                        elapsed = time.time() - t_units_start
                        avg = elapsed / (i - 1)
                        remaining = avg * (total_units - i + 1)
                        mm = int(remaining // 60)
                        ss = int(remaining % 60)
                        status(f"{status_label} {i}/{total_units}... (ETA ~{mm:02d}:{ss:02d})")
                    else:
                        status(f"{status_label} {i}/{total_units}...")
                    log(f"Unidad {i}/{total_units}: {src}")
                    unit_content = [{"type": "input_text", "text": prompt}]
                    unit_content.extend(blocks)
                    page_results.append(call_model(unit_content))
                return "\n".join([t for t in page_results if t.strip()])

            units = build_units(force_pdf_page_split=False)
            if args.per_page and len(units) > 1:
                page_results: List[str] = []
                data = run_units(units, "IA por página/bloque")
            else:
                try:
                    data = call_model(content)
                except Exception as e:
                    if not _is_request_too_large_error(e):
                        raise

                    _write_log(log_path, f"Reintento automático por tamaño/tokens: {e!r}")
                    log("Documento grande detectado. Reintentando automáticamente por páginas...")
                    status("Documento grande: reintentando por páginas...")
                    retry_units = build_units(force_pdf_page_split=True)
                    data = run_units(retry_units, "Reintento por página")

            data = _postprocess_output(str(data))
            data = _apply_pdf_overrides(data, pdf_totals, Path(result["log_path"]) if result.get("log_path") else None)

            status("Guardando TXT...")
            out_path = Path(outdir) / f"{source_stem}.txt"
            try:
                out_path.write_text(str(data).strip() + "\n", encoding="utf-8")
            except Exception:
                if requested_outdir:
                    raise SystemExit(f"ERROR: No se pudo guardar el TXT en outdir solicitado: {requested_outdir}")
                # fallback: carpeta del archivo de entrada; último recurso TEMP
                outdir = _ensure_writable_outdir_with_file_fallback("", args.files[0])
                out_path = Path(outdir) / f"{source_stem}.txt"
                out_path.write_text(str(data).strip() + "\n", encoding="utf-8")
            _write_log(log_path, f"Salida generada: {out_path}")

            control_path = _write_daily_control_file(
                outdir,
                source_stem,
                pdf_totals.get("daily_rows") or [],
                bool(pdf_totals.get("bank_nacion")),
            )
            if control_path:
                _write_log(log_path, f"Control diario generado: {control_path}")

            result["out_path"] = str(out_path)
            status("Listo")
            log(f"Generado: {out_path}")

        except SystemExit as e:
            result["error"] = str(e)
            if result.get("log_path"):
                _write_log(Path(result["log_path"]), f"ERROR: {result['error']}")
        except Exception as e:
            result["error"] = f"ERROR: {e!r}"
            if result.get("log_path"):
                _write_log(Path(result["log_path"]), f"ERROR: {result['error']}")

        if ui:
            if result["error"]:
                ui.push("STATUS:Error")
                ui.push(result["error"])
                # No cerrar automáticamente: dejar que el usuario cierre la ventana
                return
            time.sleep(0.8)
            ui.close()

    if ui:
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        ui.mainloop()
    else:
        worker()

    if result["error"]:
        print(result["error"], file=sys.stderr)
        raise SystemExit(1)

    print(result["out_path"])


if __name__ == "__main__":
    main()
