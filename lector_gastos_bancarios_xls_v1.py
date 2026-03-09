# -*- coding: utf-8 -*-
r"""
lector_gastos_bancarios_xls_v1.py

- Lee un extracto bancario en formato Excel legacy (.xls/.xlsx).
- Clasifica solo movimientos de gastos/impuestos/retenciones usando reglas JSON.
- Genera TXT con la misma estructura que el proceso actual (cabecera + CONCEPTO|IMPORTE).
- Importante: al finalizar OK imprime SOLO la ruta del TXT por stdout (para VB6).
"""

from __future__ import annotations

import argparse
import calendar
import datetime as dt
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

try:
    from dotenv import load_dotenv
except Exception:
    def load_dotenv(*args: Any, **kwargs: Any) -> bool:  # type: ignore[override]
        return False

from ia_backend_transport import backend_enabled, call_backend

try:
    import tkinter as tk
    from tkinter import ttk
except Exception:
    tk = None
    ttk = None


OUT_CATEGORIES = [
    "BANCO",
    "GASTO",
    "NETO_GRAVADO_POR_AJUSTE_IVA",
    "IDC_A_COMPUTAR",
    "IVA_CREDITO",
    "RET_IVA",
    "RET_IIBB",
    "RET_GAN",
    "OTROS",
]

DEFAULT_RULES: Dict[str, Any] = {
    "version": 1,
    "default_policy": "exclude",
    "banks": {
        "BNA": {
            "bank_name": "BANCO DE LA NACION ARGENTINA",
            "detect": {
                "sheet_name_contains": ["HOJA1 - TABLE", "HOJA1"],
                "headers_any": ["Fecha", "Comprobante", "Concepto", "Importe"],
            },
            "columns": {"date": "Fecha", "description": "Concepto", "amount": "Importe"},
            "rules": [
                {"match": r"GRAVAMEN LEY 25413|IMP\.DB/CR", "mode": "regex", "category": "IDC_A_COMPUTAR"},
                {"match": r"GRAVAMEN IBRN|IMPUESTO RIONEGRINO|ING\.? ?BRUT|SIRCREB|RET\.?IIBB", "mode": "regex", "category": "RET_IIBB"},
                {"match": r"COMISION|COMIS|^COM\b|INTERES|INTERESES|ARANCEL|GASTO BANC", "mode": "regex", "category": "GASTO"},
                {"match": r"IVA PERCEPCION|PERCEPCION IVA|RETEN IVA|RET\.IVA|RG ?2408", "mode": "regex", "category": "RET_IVA"},
                {"match": r"IVA BASE|IVA ALICUOTA|IVA ", "mode": "regex", "category": "IVA_CREDITO"},
                {"match": r"RETENCION GANAN|RET\.?GAN", "mode": "regex", "category": "RET_GAN"},
            ],
            "exclusions": [
                "TRANSFER",
                "DEBIN",
                "DEPOSITO",
                "PAGO DIRECTO",
                "TODOCARD",
                "MASTERCARD",
                "VISA",
                "CREDITO POR TRANSFERENCIA",
            ],
        },
        "BPAT": {
            "bank_name": "BANCO PATAGONIA S.A.",
            "detect": {
                "sheet_name_contains": ["VERTVERT"],
                "headers_any": ["Fecha", "Descripción", "Débito", "Crédito"],
            },
            "columns": {"date": "Fecha", "description": "Descripción", "debit": "Débito", "credit": "Crédito"},
            "rules": [
                {"match": r"GRAVAMEN LEY 25413|IMP\.DB/CR", "mode": "regex", "category": "IDC_A_COMPUTAR"},
                {"match": r"GRAVAMEN IBRN|IMPUESTO RIONEGRINO|ING\.? ?BRUT|SIRCREB|RET\.?IIBB", "mode": "regex", "category": "RET_IIBB"},
                {"match": r"COMISION|COMIS|^COM\b|INTERES|INTERESES|ARANCEL|GASTO BANC", "mode": "regex", "category": "GASTO"},
                {"match": r"IVA PERCEPCION|PERCEPCION IVA|RETEN IVA|RET\.IVA|RG ?2408", "mode": "regex", "category": "RET_IVA"},
                {"match": r"IVA BASE|IVA ALICUOTA|IVA ", "mode": "regex", "category": "IVA_CREDITO"},
                {"match": r"RETENCION GANAN|RET\.?GAN", "mode": "regex", "category": "RET_GAN"},
            ],
            "exclusions": [
                "TRANSFER",
                "PAGO CON TRANSFERENCIA",
                "PAGOS AFIP",
                "DEPOSITO",
                "TODOCARD",
                "TARJETA CREDITO",
                "DEBITO P/ACREDITAC.DE SUELDOS",
            ],
        },
    },
}


class StatusUI:
    """Ventana simple de progreso para uso interactivo (--gui)."""

    def __init__(self, title: str = "Procesando gastos bancarios...", width: int = 560, height: int = 260):
        if tk is None or ttk is None:
            raise RuntimeError("Tkinter no está disponible en este entorno.")

        self.q: "queue.Queue[str]" = queue.Queue()
        self.t0 = time.time()
        self._closed = False
        self._time_after_id = None

        self.root = tk.Tk()
        self.root.title(title)
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

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(100, self._poll)
        self._time_after_id = self.root.after(200, self._tick_time)

    def _on_close(self) -> None:
        self._closed = True
        if self._time_after_id is not None:
            try:
                self.root.after_cancel(self._time_after_id)
            except Exception:
                pass
            self._time_after_id = None
        try:
            self.root.destroy()
        except Exception:
            pass

    def _tick_time(self) -> None:
        if self._closed:
            return
        elapsed = int(time.time() - self.t0)
        mm, ss = divmod(elapsed, 60)
        try:
            self.lbl_time.config(text=f"Tiempo: {mm:02d}:{ss:02d}")
        except Exception:
            return
        self._time_after_id = self.root.after(200, self._tick_time)

    def _append(self, s: str) -> None:
        if self._closed:
            return
        if s.startswith("STATUS:"):
            try:
                self.lbl.config(text=s[len("STATUS:") :].strip())
            except Exception:
                pass
            return
        self.txt.configure(state="normal")
        self.txt.insert("end", s + "\n")
        self.txt.see("end")
        self.txt.configure(state="disabled")

    def _poll(self) -> None:
        if self._closed:
            return
        while True:
            try:
                s = self.q.get_nowait()
            except queue.Empty:
                break
            self._append(str(s))
        self.root.after(100, self._poll)

    def push(self, s: str) -> None:
        if not self._closed:
            self.q.put(str(s))

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        try:
            self.pb.stop()
        except Exception:
            pass
        try:
            self.root.destroy()
        except Exception:
            pass

    def mainloop(self) -> None:
        self.root.mainloop()


def _strip_accents(text: Any) -> str:
    s = "" if text is None else str(text)
    return "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")


def _norm_text(text: str) -> str:
    s = _strip_accents(text or "")
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s


def _parse_ar_number(value: Any) -> float:
    if isinstance(value, (int, float)):
        return float(value)
    s = ("" if value is None else str(value)).strip()
    if not s:
        return 0.0
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
    if "-" in s:
        neg = True
    s = re.sub(r"[^0-9,.\-]", "", s)
    if not s or s in {"-", ".", ","}:
        return 0.0
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s:
        s = s.replace(".", "").replace(",", ".")
    try:
        n = float(s)
    except Exception:
        return 0.0
    if neg and n > 0:
        n = -n
    return n


def _is_date_ddmmyyyy(s: str) -> bool:
    return bool(re.fullmatch(r"\d{2}/\d{2}/\d{4}", (s or "").strip()))


def _to_date_ddmmyyyy(value: Any) -> Optional[str]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        # Excel serial date (Windows): days since 1899-12-30
        v = float(value)
        if 20000 <= v <= 80000:
            base = dt.datetime(1899, 12, 30)
            d = base + dt.timedelta(days=v)
            return d.strftime("%d/%m/%Y")
        return None
    s = str(value).strip()
    if not s:
        return None
    if _is_date_ddmmyyyy(s):
        return s
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return f"{m.group(3)}/{m.group(2)}/{m.group(1)}"
    return None


def _run_powershell_excel_dump(file_path: Path, timeout_seconds: int = 90) -> Dict[str, Any]:
    ps_script = r"""
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = [System.Text.UTF8Encoding]::new($false)
$path = $env:XLS_PATH
$path = [System.IO.Path]::GetFullPath($path)
$excel = New-Object -ComObject Excel.Application
$excel.Visible = $false
$excel.DisplayAlerts = $false
try {
  $wb = $excel.Workbooks.Open($path, $null, $true)
  try {
    $ws = $wb.Worksheets.Item(1)
    $ur = $ws.UsedRange
    $rows = [int]$ur.Rows.Count
    $cols = [int]$ur.Columns.Count
    if($rows -gt 12000){ $rows = 12000 }
    if($cols -gt 12){ $cols = 12 }

    # Buscar cabecera en primeras filas para evitar recorrer basura de UsedRange.
    $headerRow = 1
    for($r=1; $r -le [Math]::Min(120, $rows); $r++){
      $rowTxt = @()
      for($c=1; $c -le [Math]::Min($cols, 8); $c++){
        $rowTxt += (($ws.Cells.Item($r,$c).Text + '')).Trim().ToUpper()
      }
      $joined = ($rowTxt -join ' | ')
      if(($joined -like '*FECHA*') -and (($joined -like '*CONCEPTO*') -or ($joined -like '*DESCRIP*'))){
        $headerRow = $r
        break
      }
    }

    $startRow = [Math]::Max(1, $headerRow - 6)
    $endCap = [Math]::Min($rows, $headerRow + 5500)
    $rng = $ws.Range($ws.Cells.Item($startRow,1), $ws.Cells.Item($endCap,$cols))
    $vals = $rng.Value2
    $data = New-Object System.Collections.Generic.List[object]
    $blankStreak = 0
    $rCount = $vals.GetLength(0)
    $cCount = $vals.GetLength(1)
    for($r=1; $r -le $rCount; $r++){
      $row = New-Object System.Collections.Generic.List[object]
      $hasData = $false
      for($c=1; $c -le $cCount; $c++){
        $v = $vals[$r,$c]
        if($null -eq $v){
          $row.Add("")
          continue
        }
        if(($v -is [double]) -or ($v -is [int]) -or ($v -is [long]) -or ($v -is [decimal])){
          $row.Add($v)
          $hasData = $true
        } else {
          $s = ([string]$v).Trim()
          if($s -ne ''){ $hasData = $true }
          $row.Add($s)
        }
      }
      $data.Add($row)
      if($hasData){
        $blankStreak = 0
      } else {
        $blankStreak++
        if($blankStreak -ge 120 -and $r -gt 100){
          break
        }
      }
    }
    $obj = [PSCustomObject]@{
      sheet_name = [string]$ws.Name
      rows = $data.Count
      cols = $cols
      data = $data
    }
    $obj | ConvertTo-Json -Depth 5 -Compress
  } finally {
    $wb.Close($false) | Out-Null
    [System.Runtime.InteropServices.Marshal]::ReleaseComObject($wb) | Out-Null
  }
} finally {
  $excel.Quit()
  [System.Runtime.InteropServices.Marshal]::ReleaseComObject($excel) | Out-Null
}
"""
    try:
        proc = subprocess.run(
            [
                "powershell",
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                ps_script,
            ],
            capture_output=True,
            text=False,
            env={**os.environ, "XLS_PATH": str(file_path)},
            timeout=max(30, int(timeout_seconds)),
            creationflags=0x08000000,
        )
    except subprocess.TimeoutExpired:
        raise SystemExit(
            f"ERROR: lectura de Excel superó el tiempo límite ({int(timeout_seconds)}s). "
            "Revisar conectividad de red/ruta o reducir tamaño del archivo."
        )
    if proc.returncode != 0:
        stderr_b = proc.stderr or b""
        try:
            stderr = stderr_b.decode("utf-8", errors="replace").strip()
        except Exception:
            stderr = stderr_b.decode("cp1252", errors="replace").strip()
        raise SystemExit(f"ERROR: No se pudo leer Excel por COM: {stderr or 'sin detalle'}")
    out_b = proc.stdout or b""
    out = ""
    for enc in ("utf-8-sig", "utf-8", "cp1252", "latin-1"):
        try:
            out = out_b.decode(enc).strip()
            break
        except Exception:
            continue
    if not out:
        raise SystemExit("ERROR: Excel devolvió salida vacía.")
    try:
        return json.loads(out)
    except Exception as e:
        raise SystemExit(f"ERROR: No se pudo parsear JSON de Excel: {e!r}") from e


def _find_header_row(data: List[List[str]], required: List[str]) -> Optional[int]:
    req = [_norm_text(x) for x in required]
    for i, row in enumerate(data):
        nr = [_norm_text(x) for x in row]
        row_join = " | ".join(nr)
        if all(any(r == c or r in c for c in nr) for r in req):
            return i
        if all(r in row_join for r in req):
            return i
    return None


def _infer_bank_from_filename(name: str) -> Optional[str]:
    n = _norm_text(name)
    if any(x in n for x in ["BPAT", "PATAGONIA"]):
        return "BPAT"
    if any(x in n for x in ["BNA", "NACION", "NAC "]):
        return "BNA"
    return None


def _infer_bank_from_sheet_text(sheet_name: str, data: List[List[str]]) -> Optional[str]:
    ns = _norm_text(sheet_name)
    if "VERTVERT" in ns:
        return "BPAT"
    if "HOJA1" in ns and "TABLE" in ns:
        return "BNA"
    joined = []
    for r in data[:80]:
        joined.append(" | ".join(_norm_text(x) for x in r[:8]))
    text = "\n".join(joined)
    if "MOVIMIENTOS DE CUENTA" in text or "DEBITO" in text and "CREDITO" in text and "DESCRIPCION" in text:
        return "BPAT"
    if "BANCO NACION" in text or "ULTIMOS MOVIMIENTOS" in text or ("COMPROBANTE" in text and "CONCEPTO" in text and "IMPORTE" in text):
        return "BNA"
    return None


def _detect_bank(rules: Dict[str, Any], sheet_name: str, data: List[List[str]], source_name: str = "") -> str:
    nsheet = _norm_text(sheet_name)
    for bank_key, spec in (rules.get("banks") or {}).items():
        detect = spec.get("detect") or {}
        names = [_norm_text(x) for x in (detect.get("sheet_name_contains") or [])]
        if names and not any(n in nsheet for n in names):
            continue
        headers_any = detect.get("headers_any") or []
        if headers_any and _find_header_row(data, headers_any) is None:
            continue
        return bank_key

    by_file = _infer_bank_from_filename(source_name or "")
    if by_file:
        return by_file
    by_text = _infer_bank_from_sheet_text(sheet_name, data)
    if by_text:
        return by_text
    raise SystemExit("ERROR: No se pudo detectar banco para el archivo.")


def _col_index(headers: List[str], target: str) -> Optional[int]:
    t = _norm_text(target)
    for i, h in enumerate(headers):
        nh = _norm_text(h)
        if nh == t or t in nh:
            return i
    return None


def _compile_rules(rules_raw: List[Dict[str, str]]) -> List[Tuple[str, Any, str]]:
    out: List[Tuple[str, Any, str]] = []
    for r in rules_raw:
        mode = (r.get("mode") or "contains").strip().lower()
        pattern = (r.get("match") or "").strip()
        category = (r.get("category") or "OTROS").strip().upper()
        if not pattern:
            continue
        if mode == "regex":
            out.append(("regex", re.compile(pattern, flags=re.IGNORECASE), category))
        else:
            out.append(("contains", _norm_text(pattern), category))
    return out


def _match_category(desc: str, compiled_rules: List[Tuple[str, Any, str]]) -> Optional[str]:
    nd = _norm_text(desc)
    if "IVA PERCEPCION" in nd or "PERCEPCION IVA" in nd:
        return "RET_IVA"
    for mode, patt, cat in compiled_rules:
        if mode == "regex":
            if patt.search(desc):
                return cat
        else:
            if patt in nd:
                return cat
    return None


def _is_excluded(desc: str, exclusions: List[str]) -> bool:
    nd = _norm_text(desc)
    for ex in exclusions:
        if _norm_text(ex) in nd:
            return True
    return False


def _infer_period_end_date(path_name: str) -> str:
    name = _norm_text(path_name)
    m = re.search(r"(?<!\d)(\d{1,2})[-_/](\d{4})(?!\d)", name)
    if m:
        month = int(m.group(1))
        year = int(m.group(2))
        if 1 <= month <= 12:
            ld = calendar.monthrange(year, month)[1]
            return f"{ld:02d}-{month:02d}-{year}"
    months = {
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
    mm = re.search(r"(ENERO|FEBRERO|MARZO|ABRIL|MAYO|JUNIO|JULIO|AGOSTO|SEPTIEMBRE|SETIEMBRE|OCTUBRE|NOVIEMBRE|DICIEMBRE)\s+(\d{4})", name)
    if mm:
        month = months[mm.group(1)]
        year = int(mm.group(2))
        ld = calendar.monthrange(year, month)[1]
        return f"{ld:02d}-{month:02d}-{year}"
    today = dt.date.today()
    ld = calendar.monthrange(today.year, today.month)[1]
    return f"{ld:02d}-{today.month:02d}-{today.year}"


def _build_header_lines(bank_name: str, period_date: str) -> str:
    bank_s = bank_name.strip()
    concept = f"GB {period_date} {bank_s}".strip()
    concept = re.sub(r"\s+", " ", concept)
    if len(concept) > 50:
        concept = concept[:50].rstrip()
    lines = [
        bank_s,
        "GASTOS BANCARIOS",
        period_date,
        concept,
        "CONCEPTO|IMPORTE",
    ]
    return "\n".join(lines) + "\n"


def _bank_concept_label(bank_name: str) -> str:
    b = _norm_text(bank_name)
    if "NACION" in b:
        return "BANCO NACION"
    if "PATAGONIA" in b:
        return "BANCO PATAGONIA"
    if b.startswith("BANCO "):
        return b
    return f"BANCO {b}".strip()


def _format_output(totals: Dict[str, float], bank_name: str) -> str:
    lines: List[str] = []
    bank_label = _bank_concept_label(bank_name)
    display_map = {
        "NETO_GRAVADO_POR_AJUSTE_IVA": "NETO GRAVADO POR AJUSTE IVA",
    }
    for cat in OUT_CATEGORIES:
        v = float(totals.get(cat, 0.0))
        if abs(v) < 0.005:
            v = 0.0
        if cat == "BANCO":
            concept = bank_label
        else:
            concept = display_map.get(cat, cat)
        lines.append(f"{concept}|{v:.2f}")
    return "\n".join(lines) + "\n"


def _write_control_concepts_file(
    outdir: Path,
    base_name: str,
    concept_totals: Dict[str, Dict[str, Any]],
    category_totals: Dict[str, float],
) -> Path:
    path = outdir / f"{base_name}_control_conceptos.xls"
    lines = ["LINEA\tCONCEPTO\tCATEGORIA\tCANTIDAD\tIMPORTE_TOTAL"]
    idx = 1
    for concept, info in sorted(concept_totals.items(), key=lambda it: abs(float(it[1].get("amount", 0.0))), reverse=True):
        cat = str(info.get("category") or "OTROS")
        cnt = int(info.get("count") or 0)
        amt = float(info.get("amount") or 0.0)
        safe_concept = concept.replace("\t", " ").replace("\r", " ").replace("\n", " ").strip()
        lines.append(f"{idx}\t{safe_concept}\t{cat}\t{cnt}\t{amt:.2f}")
        idx += 1
    lines.append("")
    lines.append("RESUMEN_POR_CATEGORIA\t\t\t\t")
    lines.append("LINEA\tCATEGORIA\tIMPORTE_TOTAL\t\t")
    j = 1
    for cat in OUT_CATEGORIES:
        lines.append(f"{j}\t{cat}\t{float(category_totals.get(cat, 0.0)):.2f}\t\t")
        j += 1
    base_21 = abs(float(category_totals.get("GASTO", 0.0)))
    iva_real = abs(float(category_totals.get("IVA_CREDITO", 0.0)))
    iva_teo = round(base_21 * 0.21, 2)
    diff = round(iva_real - iva_teo, 2)
    ajuste_base = round((iva_real / 0.21) - base_21, 2) if iva_real > 0 else 0.0
    base_ajustada = round(base_21 + ajuste_base, 2)
    iva_teo_aj = round(base_ajustada * 0.21, 2)
    diff_post = round(iva_real - iva_teo_aj, 2)
    lines.append("")
    lines.append("IVA_21_CONTROL\t\t\t\t")
    lines.append(
        "BASE_GASTO_21\tIVA_TEORICO_21\tIVA_CREDITO_REAL\tDIFERENCIA\tAJUSTE_NETO_GRAVADO\tBASE_AJUSTADA\tIVA_TEORICO_AJUSTADO\tDIF_POST_AJUSTE"
    )
    lines.append(
        f"{base_21:.2f}\t{iva_teo:.2f}\t{iva_real:.2f}\t{diff:.2f}\t{ajuste_base:.2f}\t{base_ajustada:.2f}\t{iva_teo_aj:.2f}\t{diff_post:.2f}"
    )
    if abs(diff) > 1.0:
        lines.append(f"ALERTA_IVA_21\tDIFERENCIA detectada: {diff:.2f}. Se sugiere ajuste neto gravado: {ajuste_base:.2f}\t\t\t")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _resolve_outdir(outdir_arg: str, source_file: Path) -> Tuple[Path, Optional[str]]:
    candidates: List[Path] = []
    requested: Optional[Path] = None
    if outdir_arg.strip():
        requested = Path(outdir_arg.strip())
        candidates.append(requested)
    candidates.append(source_file.parent)
    candidates.append(Path(tempfile.gettempdir()))

    seen: set[str] = set()
    last_err: Optional[Exception] = None
    for p in candidates:
        key = str(p).strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        try:
            p.mkdir(parents=True, exist_ok=True)
            test = p / ".__ia_gb_write_test.tmp"
            test.write_text("ok", encoding="utf-8")
            try:
                test.unlink(missing_ok=True)
            except Exception:
                pass
            if requested is not None and str(p).lower() != str(requested).lower():
                return p, f"OUTDIR_FALLBACK: solicitado='{requested}' usado='{p}'"
            return p, None
        except Exception as e:
            last_err = e
            continue
    raise SystemExit(f"ERROR: no se pudo usar ninguna carpeta de salida. Último error: {last_err!r}")


def _should_stage_local(path: Path) -> bool:
    s = str(path)
    if s.startswith("\\\\"):
        return True
    drive = path.drive.upper()
    return drive not in {"", "C:"}


def _stage_input_local(in_file: Path) -> Tuple[Path, Optional[Path]]:
    if not _should_stage_local(in_file):
        return in_file, None
    tmp_dir = Path(tempfile.gettempdir()) / "ia_gastos_bancarios_stage"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    staged = tmp_dir / f"{in_file.stem}_{int(time.time())}{in_file.suffix}"
    shutil.copy2(str(in_file), str(staged))
    return staged, staged


def _app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def _bundle_base_dir() -> Optional[Path]:
    p = getattr(sys, "_MEIPASS", None)
    if p:
        return Path(p)
    return None


def load_env_near_app() -> None:
    env_path = _app_base_dir() / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=str(env_path), override=False)
    else:
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


def audit_backend_usage(source_filename: str, model: str, strict: bool, timeout_seconds: int = 20) -> Optional[str]:
    if not backend_enabled():
        msg = "AUDIT: backend no configurado; se omite registro API."
        if strict:
            raise SystemExit(f"ERROR: {msg}")
        return msg
    prev_task = os.getenv("IA_TASK")
    if not (prev_task or "").strip():
        os.environ["IA_TASK"] = "GASTOS_BANCARIOS"
    try:
        _ = call_backend(
            content_blocks=[
                {"type": "input_text", "text": "Registro de auditoria de uso. Responde OK."},
                {"type": "input_text", "text": "OK"},
            ],
            model=model or "gpt-4o-mini",
            max_output_tokens=16,
            source_filename=source_filename,
            timeout_seconds=max(8, int(timeout_seconds)),
        )
        return "AUDIT: registro API OK."
    except SystemExit as e:
        if strict:
            raise
        return f"AUDIT: warning no se pudo registrar API: {str(e)}"
    except Exception as e:
        if strict:
            raise SystemExit(f"ERROR: no se pudo registrar API: {e!r}")
        return f"AUDIT: warning no se pudo registrar API: {e!r}"
    finally:
        if prev_task is None:
            os.environ.pop("IA_TASK", None)
        else:
            os.environ["IA_TASK"] = prev_task


def process_file(
    in_file: Path,
    rules: Dict[str, Any],
    outdir: Path,
    excel_timeout_seconds: int = 90,
    output_stem: Optional[str] = None,
    source_label: Optional[str] = None,
) -> Tuple[Path, Path, Path]:
    excel = _run_powershell_excel_dump(in_file, timeout_seconds=excel_timeout_seconds)
    data = excel.get("data") or []
    if not data:
        raise SystemExit("ERROR: Excel sin datos.")

    bank_key = _detect_bank(
        rules,
        str(excel.get("sheet_name") or ""),
        data,
        source_name=str(source_label or in_file.name),
    )
    bank_spec = (rules.get("banks") or {}).get(bank_key) or {}
    cols_spec = bank_spec.get("columns") or {}

    header_req = []
    for k in ("date", "description", "amount", "debit", "credit"):
        v = cols_spec.get(k)
        if v:
            header_req.append(v)
    header_row_idx = _find_header_row(data, header_req[:2] if len(header_req) >= 2 else header_req)
    if header_row_idx is None:
        raise SystemExit(f"ERROR: No se encontró cabecera para banco {bank_key}.")

    headers = data[header_row_idx]
    idx_date = _col_index(headers, cols_spec.get("date", "Fecha"))
    idx_desc = _col_index(headers, cols_spec.get("description", "Concepto"))
    idx_amount = _col_index(headers, cols_spec.get("amount", "Importe"))
    idx_debit = _col_index(headers, cols_spec.get("debit", "Debito"))
    idx_credit = _col_index(headers, cols_spec.get("credit", "Credito"))

    if idx_date is None or idx_desc is None:
        raise SystemExit(f"ERROR: Cabeceras incompletas para banco {bank_key}.")

    compiled_rules = _compile_rules(bank_spec.get("rules") or [])
    exclusions = bank_spec.get("exclusions") or []

    totals: Dict[str, float] = {k: 0.0 for k in OUT_CATEGORIES}
    unknown: Dict[str, int] = {}
    concept_totals: Dict[str, Dict[str, Any]] = {}
    matched_rows = 0
    analyzed_rows = 0

    for row in data[header_row_idx + 1 :]:
        if idx_date >= len(row) or idx_desc >= len(row):
            continue
        date_s = _to_date_ddmmyyyy(row[idx_date])
        desc = ("" if row[idx_desc] is None else str(row[idx_desc])).strip()
        if not date_s or not desc:
            continue
        analyzed_rows += 1
        if _is_excluded(desc, exclusions):
            continue

        category = _match_category(desc, compiled_rules)
        if not category:
            unknown[desc] = unknown.get(desc, 0) + 1
            continue
        if category not in totals:
            category = "OTROS"

        amount = 0.0
        if idx_amount is not None and idx_amount < len(row):
            amount = _parse_ar_number(row[idx_amount])
        else:
            debit = _parse_ar_number(row[idx_debit]) if idx_debit is not None and idx_debit < len(row) else 0.0
            credit = _parse_ar_number(row[idx_credit]) if idx_credit is not None and idx_credit < len(row) else 0.0
            amount = -abs(debit) + abs(credit)

        if abs(amount) < 0.005:
            continue
        totals[category] += amount
        key = desc.strip()
        if key not in concept_totals:
            concept_totals[key] = {"category": category, "count": 0, "amount": 0.0}
        concept_totals[key]["count"] = int(concept_totals[key]["count"]) + 1
        concept_totals[key]["amount"] = float(concept_totals[key]["amount"]) + float(amount)
        matched_rows += 1

    bank_name = bank_spec.get("bank_name") or bank_key
    # Ajuste sugerido para que IVA_CREDITO cierre con base gravada al 21%.
    base_21 = abs(float(totals.get("GASTO", 0.0)))
    iva_real = abs(float(totals.get("IVA_CREDITO", 0.0)))
    ajuste_base = round((iva_real / 0.21) - base_21, 2) if iva_real > 0 else 0.0
    if abs(ajuste_base) < 0.01:
        ajuste_base = 0.0
    totals["NETO_GRAVADO_POR_AJUSTE_IVA"] = -ajuste_base
    if ajuste_base != 0.0:
        concept_totals["NETO GRAVADO POR AJUSTE IVA"] = {
            "category": "NETO_GRAVADO_POR_AJUSTE_IVA",
            "count": 1,
            "amount": -ajuste_base,
        }

    # Contrapartida contable: BANCO balancea el asiento a cero.
    subtotal = sum(float(v) for k, v in totals.items() if k != "BANCO")
    totals["BANCO"] = -subtotal
    period_date = _infer_period_end_date(in_file.name)
    txt = _build_header_lines(bank_name, period_date) + _format_output(totals, bank_name)

    log_lines = [
        f"archivo={source_label or in_file}",
        f"banco={bank_key}",
        f"sheet={excel.get('sheet_name')}",
        f"rows_analyzed={analyzed_rows}",
        f"rows_matched={matched_rows}",
    ]
    for c in OUT_CATEGORIES:
        log_lines.append(f"total_{c}={totals.get(c, 0.0):.2f}")
    if unknown:
        log_lines.append("no_clasificados_top20:")
        for k, v in sorted(unknown.items(), key=lambda it: it[1], reverse=True)[:20]:
            log_lines.append(f"- {k} | cnt={v}")

    base_name = (output_stem or in_file.stem).strip() or in_file.stem
    candidates = [outdir, in_file.parent, Path(tempfile.gettempdir())]
    seen: set[str] = set()
    last_err: Optional[Exception] = None
    for d in candidates:
        k = str(d).strip().lower()
        if not k or k in seen:
            continue
        seen.add(k)
        try:
            d.mkdir(parents=True, exist_ok=True)
            out_path = d / f"{base_name}.txt"
            log_path = d / f"{base_name}.log"
            out_path.write_text(txt, encoding="utf-8")
            control_path = _write_control_concepts_file(d, base_name, concept_totals, totals)
            log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
            if str(d).lower() != str(outdir).lower():
                with log_path.open("a", encoding="utf-8") as f:
                    f.write(f"OUTFILE_FALLBACK: preferido='{outdir}' usado='{d}'\n")
            with log_path.open("a", encoding="utf-8") as f:
                f.write(f"control_conceptos={control_path}\n")
            return out_path, log_path, control_path
        except Exception as e:
            last_err = e
            continue
    raise SystemExit(f"ERROR: no se pudo escribir salida TXT/LOG en ninguna ruta. Último error: {last_err!r}")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Lector de gastos bancarios desde XLS/XLSX a TXT (formato compatible)."
    )
    ap.add_argument("files", nargs="*", help="Archivos de entrada (.xls/.xlsx). Se usa el primero.")
    ap.add_argument(
        "--idcliente",
        type=int,
        default=None,
        help="Id de cliente para auditoria backend (IA_IDCLIENTE/IDCLIENTE).",
    )
    ap.add_argument("--outdir", default="", help="Carpeta de salida. Default: carpeta del archivo fuente")
    ap.add_argument("--rules-file", default="", help="Ruta de reglas JSON. Default: reglas_gastos_bancarios_v1.json")
    ap.add_argument("--prompt-file", default="", help="Compatibilidad: no usado en gastos bancarios XLS")
    ap.add_argument("--model", default="gpt-4o-mini", help="Modelo para auditoría backend (default: gpt-4o-mini)")
    ap.add_argument("--gui", action="store_true", help="Compatibilidad VB6: no altera stdout")
    ap.add_argument("--per-page", action="store_true", help="Compatibilidad: no usado")
    ap.add_argument("--auto", action="store_true", help="Compatibilidad: no usado")
    ap.add_argument("--tile", type=int, default=1, help="Compatibilidad: no usado")
    ap.add_argument("--pdf-chunk-pages", type=int, default=0, help="Compatibilidad: no usado")
    ap.add_argument("--env-file", default="", help="Archivo .env alternativo para pruebas.")
    ap.add_argument("--no-local-env", action="store_true", help="No cargar .env junto al exe/script.")
    ap.add_argument("--backend-url", default="", help="Override IA_BACKEND_URL.")
    ap.add_argument("--backend-route", default="", help="Override IA_BACKEND_ROUTE.")
    ap.add_argument("--client-id", default="", help="Override IA_CLIENT_ID.")
    ap.add_argument("--client-secret", default="", help="Override IA_CLIENT_SECRET.")
    ap.add_argument("--ia-task", default="", help="Override IA_TASK/opcion.")
    ap.add_argument("--no-api-audit", action="store_true", help="Desactiva registro de auditoría API.")
    ap.add_argument("--api-audit-strict", action="store_true", help="Si falla registro API, corta con error.")
    ap.add_argument("--max-seconds", type=int, default=120, help="Tiempo máximo total de proceso (default: 120).")
    args = ap.parse_args()

    ui = None
    if args.gui:
        try:
            ui = StatusUI()
            ui.push("STATUS:Inicializando...")
        except Exception:
            ui = None

    def log(msg: str) -> None:
        if ui:
            ui.push(msg)

    def status(msg: str) -> None:
        if ui:
            ui.push(f"STATUS:{msg}")

    result: Dict[str, Optional[str]] = {"out_path": None, "error": None}

    def worker() -> None:
        staged_tmp: Optional[Path] = None
        try:
            t0 = time.time()
            max_seconds = max(30, int(args.max_seconds))
            status("Cargando configuración...")
            if not args.no_local_env:
                load_env_near_app()
            if args.env_file:
                load_dotenv(dotenv_path=args.env_file, override=True)
            apply_runtime_env_overrides(args)
            if args.idcliente is not None:
                os.environ["IA_IDCLIENTE"] = str(args.idcliente)
                os.environ["IDCLIENTE"] = str(args.idcliente)

            if not args.files:
                raise SystemExit("ERROR: Debés pasar al menos 1 archivo por parámetro.")
            in_file = Path(args.files[0])
            if not in_file.exists():
                raise SystemExit(f"ERROR: No existe el archivo: {in_file}")

            status("Cargando reglas...")
            app_default_rules_path = _app_base_dir() / "reglas_gastos_bancarios_v1.json"
            bundle_base = _bundle_base_dir()
            bundle_rules_path = (bundle_base / "reglas_gastos_bancarios_v1.json") if bundle_base else None
            rules_path = Path(args.rules_file) if args.rules_file.strip() else app_default_rules_path
            rules: Dict[str, Any]
            if not rules_path.exists() and not args.rules_file.strip() and bundle_rules_path and bundle_rules_path.exists():
                try:
                    rules = json.loads(bundle_rules_path.read_text(encoding="utf-8"))
                except Exception as e:
                    raise SystemExit(f"ERROR: Reglas JSON embebidas inválidas: {e!r}") from e
            elif not rules_path.exists():
                try:
                    rules_path.write_text(json.dumps(DEFAULT_RULES, ensure_ascii=False, indent=2), encoding="utf-8")
                    rules = DEFAULT_RULES
                except Exception:
                    rules = DEFAULT_RULES
            else:
                try:
                    rules = json.loads(rules_path.read_text(encoding="utf-8"))
                except Exception as e:
                    raise SystemExit(f"ERROR: Reglas JSON inválidas: {e!r}") from e

            status("Procesando Excel...")
            outdir, outdir_note = _resolve_outdir(args.outdir, in_file)
            try:
                staged_input, staged_tmp = _stage_input_local(in_file)
            except Exception as e:
                raise SystemExit(f"ERROR: no se pudo copiar archivo a local temporal: {e!r}") from e
            if staged_tmp is not None:
                log(f"INPUT_STAGE: origen='{in_file}' local='{staged_input}'")
            elapsed = time.time() - t0
            remaining_for_excel = max(30, min(105, int(max_seconds - elapsed - 12)))
            out_path, log_path, control_path = process_file(
                staged_input,
                rules,
                outdir,
                excel_timeout_seconds=remaining_for_excel,
                output_stem=in_file.stem,
                source_label=str(in_file),
            )
            log(f"Generado TXT: {out_path}")
            log(f"Generado control: {control_path}")
            if outdir_note:
                with log_path.open("a", encoding="utf-8") as f:
                    f.write(outdir_note + "\n")
                log(outdir_note)

            if not args.no_api_audit:
                status("Registrando uso en API...")
                elapsed = time.time() - t0
                remaining = int(max_seconds - elapsed)
                if remaining <= 5:
                    audit_msg = "AUDIT: omitido por límite de tiempo."
                else:
                    audit_msg = audit_backend_usage(
                        in_file.name,
                        args.model,
                        args.api_audit_strict,
                        timeout_seconds=min(20, remaining),
                    )
                if audit_msg:
                    with log_path.open("a", encoding="utf-8") as f:
                        f.write(audit_msg + "\n")
                    log(audit_msg)

            elapsed = time.time() - t0
            if elapsed > max_seconds:
                raise SystemExit(f"ERROR: tiempo máximo excedido ({elapsed:.0f}s > {max_seconds}s).")

            result["out_path"] = str(out_path)
            status("Listo")
        except SystemExit as e:
            result["error"] = str(e)
        except FileNotFoundError as e:
            result["error"] = (
                f"ERROR: {e!r}. Posible causa: ruta de red o unidad mapeada no disponible "
                "(ej. G: o \\\\servidor\\carpeta)."
            )
        except Exception as e:
            result["error"] = f"ERROR: {e!r}"
        finally:
            if ui:
                if result["error"]:
                    ui.push("STATUS:Error")
                    ui.push(result["error"])
                    return
                time.sleep(0.8)
                ui.close()
            if staged_tmp is not None:
                try:
                    staged_tmp.unlink(missing_ok=True)
                except Exception:
                    pass

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
