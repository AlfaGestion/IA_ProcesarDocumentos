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
import re
import subprocess
import sys
import unicodedata
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


OUT_CATEGORIES = ["GASTO", "IVA_CREDITO", "RET_IVA", "RET_IIBB", "RET_GAN", "OTROS"]


def _strip_accents(text: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", text or "") if unicodedata.category(c) != "Mn")


def _norm_text(text: str) -> str:
    s = _strip_accents(text or "")
    s = re.sub(r"\s+", " ", s).strip().upper()
    return s


def _parse_ar_number(value: str) -> float:
    s = (value or "").strip()
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


def _run_powershell_excel_dump(file_path: Path) -> Dict[str, Any]:
    ps_script = r"""
$ErrorActionPreference = 'Stop'
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
    if($rows -gt 25000){ $rows = 25000 }
    if($cols -gt 40){ $cols = 40 }
    $data = New-Object System.Collections.Generic.List[object]
    for($r=1; $r -le $rows; $r++){
      $row = New-Object System.Collections.Generic.List[string]
      for($c=1; $c -le $cols; $c++){
        $v = $ws.Cells.Item($r,$c).Text
        if($null -eq $v){ $v = '' }
        $row.Add(([string]$v).Trim())
      }
      $data.Add($row)
    }
    $obj = [PSCustomObject]@{
      sheet_name = [string]$ws.Name
      rows = $rows
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


def _detect_bank(rules: Dict[str, Any], sheet_name: str, data: List[List[str]]) -> str:
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


def _format_output(totals: Dict[str, float]) -> str:
    lines: List[str] = []
    for cat in OUT_CATEGORIES:
        v = float(totals.get(cat, 0.0))
        if abs(v) < 0.005:
            v = 0.0
        lines.append(f"{cat}|{v:.2f}")
    return "\n".join(lines) + "\n"


def _resolve_outdir(outdir_arg: str, source_file: Path) -> Path:
    if outdir_arg.strip():
        p = Path(outdir_arg.strip())
        p.mkdir(parents=True, exist_ok=True)
        return p
    return source_file.parent


def process_file(in_file: Path, rules: Dict[str, Any], outdir: Path) -> Tuple[Path, Path]:
    excel = _run_powershell_excel_dump(in_file)
    data = excel.get("data") or []
    if not data:
        raise SystemExit("ERROR: Excel sin datos.")

    bank_key = _detect_bank(rules, str(excel.get("sheet_name") or ""), data)
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
    matched_rows = 0
    analyzed_rows = 0

    for row in data[header_row_idx + 1 :]:
        if idx_date >= len(row) or idx_desc >= len(row):
            continue
        date_s = (row[idx_date] or "").strip()
        desc = (row[idx_desc] or "").strip()
        if not _is_date_ddmmyyyy(date_s) or not desc:
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
        matched_rows += 1

    bank_name = bank_spec.get("bank_name") or bank_key
    period_date = _infer_period_end_date(in_file.name)
    txt = _build_header_lines(bank_name, period_date) + _format_output(totals)

    out_path = outdir / f"{in_file.stem}.txt"
    log_path = outdir / f"{in_file.stem}.log"
    out_path.write_text(txt, encoding="utf-8")

    log_lines = [
        f"archivo={in_file}",
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
    log_path.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    return out_path, log_path


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Lector de gastos bancarios desde XLS/XLSX a TXT (formato compatible)."
    )
    ap.add_argument("file", help="Archivo de entrada .xls/.xlsx")
    ap.add_argument("--outdir", default="", help="Carpeta de salida. Default: carpeta del archivo fuente")
    ap.add_argument("--rules-file", default="", help="Ruta de reglas JSON. Default: reglas_gastos_bancarios_v1.json")
    args = ap.parse_args()

    in_file = Path(args.file)
    if not in_file.exists():
        print(f"ERROR: No existe el archivo: {in_file}", file=sys.stderr)
        raise SystemExit(1)

    rules_path = Path(args.rules_file) if args.rules_file.strip() else Path(__file__).with_name("reglas_gastos_bancarios_v1.json")
    if not rules_path.exists():
        print(f"ERROR: No existe archivo de reglas: {rules_path}", file=sys.stderr)
        raise SystemExit(1)
    try:
        rules = json.loads(rules_path.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"ERROR: Reglas JSON inválidas: {e!r}", file=sys.stderr)
        raise SystemExit(1)

    outdir = _resolve_outdir(args.outdir, in_file)
    try:
        out_path, _ = process_file(in_file, rules, outdir)
    except SystemExit:
        raise
    except Exception as e:
        print(f"ERROR: {e!r}", file=sys.stderr)
        raise SystemExit(1)

    print(str(out_path))


if __name__ == "__main__":
    main()
