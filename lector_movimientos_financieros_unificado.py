r"""
lector_movimientos_financieros_unificado.py

Esqueleto inicial del flujo unificado para:

- extractos bancarios
- liquidaciones de tarjeta
- documentos mixtos

En esta primera versión se prioriza:

- detección de tipo de fuente
- detección de institución / marca
- salida JSON unificada mínima

No reemplaza todavía a los lectores actuales.
"""

from __future__ import annotations

import argparse
import json
import os
import queue
import re
import threading
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional

import lector_gastos_bancarios_xls_v1 as bank_xls
import lector_liquidaciones_to_json_v1 as card_pdf

try:
    from pypdf import PdfReader
except Exception:
    PdfReader = None

try:
    import tkinter as tk
    from tkinter import filedialog, ttk
except Exception:
    tk = None
    ttk = None
    filedialog = None


SCHEMA_VERSION = 1
ACCOUNT_KEYS = [
    "TARJETA",
    "BANCO",
    "GASTOS_BANCARIOS",
    "ARANCEL_TARJETA",
    "COMISION_FINANCIERA",
    "NETO_GRAVADO_POR_AJUSTE_IVA",
    "IVA_CREDITO",
    "RET_IVA",
    "RET_IIBB",
    "RET_GAN",
    "RET_SUSS",
    "PERCEP_MUNICIPAL",
    "IDC_A_COMPUTAR",
    "IMPUESTOS_VARIOS",
    "AJUSTES_TARJETA",
    "OTROS",
]


class StatusUI:
    """Ventana simple de progreso para el flujo unificado."""

    def __init__(self, title: str = "Procesando movimientos financieros...", width: int = 640, height: int = 320):
        if tk is None or ttk is None:
            raise RuntimeError("Tkinter no está disponible en este entorno.")

        self.q: "queue.Queue[str]" = queue.Queue()
        self.t0 = time.time()
        self._closed = False
        self._finished = False
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

        self.txt = tk.Text(self.root, height=12, wrap="word")
        self.txt.pack(fill="both", expand=True, padx=12, pady=(0, 12))
        self.txt.configure(state="disabled")

        self.btn_close = ttk.Button(self.root, text="Cerrar", command=self.close)
        self.btn_close.pack(padx=12, pady=(0, 12), anchor="e")
        self.btn_close.pack_forget()

        self.root.protocol("WM_DELETE_WINDOW", self._on_close)
        self.root.after(100, self._poll)
        self._time_after_id = self.root.after(200, self._tick_time)

    def _on_close(self) -> None:
        if self._finished:
            self.close()
            return
        self._closed = True
        try:
            self.root.withdraw()
        except Exception:
            pass

    def _stop_timers_and_progress(self) -> None:
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

    def _tick_time(self) -> None:
        if self._closed:
            return
        elapsed = int(time.time() - self.t0)
        mm, ss = divmod(elapsed, 60)
        try:
            self.lbl.configure(text=self.lbl.cget("text"))
            self.lbl_time.configure(text=f"Tiempo: {mm:02d}:{ss:02d}")
        except Exception:
            return
        self._time_after_id = self.root.after(200, self._tick_time)

    def push(self, msg: str) -> None:
        try:
            self.q.put_nowait(str(msg))
        except Exception:
            pass

    def _append_log(self, s: str) -> None:
        self.txt.configure(state="normal")
        self.txt.insert("end", s + "\n")
        self.txt.see("end")
        self.txt.configure(state="disabled")

    def _poll(self) -> None:
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

    def finish(self, status_text: str, keep_open_seconds: float = 1.0) -> None:
        self._finished = True
        self.push(f"STATUS:{status_text}")
        self._stop_timers_and_progress()
        time.sleep(max(0.0, keep_open_seconds))
        self.close()

    def freeze(self, status_text: str) -> None:
        self._finished = True
        self._closed = False
        self.push(f"STATUS:{status_text}")
        self._stop_timers_and_progress()
        try:
            self.root.deiconify()
            self.btn_close.pack(padx=12, pady=(0, 12), anchor="e")
        except Exception:
            pass

    def close(self) -> None:
        self._closed = True
        self._stop_timers_and_progress()
        try:
            self.root.destroy()
        except Exception:
            pass

    def mainloop(self) -> None:
        self.root.mainloop()


@dataclass
class MovementItem:
    date: Optional[str] = None
    description: str = ""
    raw_amount: float = 0.0
    signed_amount: float = 0.0
    direction: str = "unknown"
    channel: str = "unknown"
    category: str = "OTROS"
    confidence: float = 0.0
    source_section: str = ""


@dataclass
class SummaryPayload:
    short_text: str = ""
    warnings: List[str] = field(default_factory=list)
    needs_review: bool = False


@dataclass
class ProposedEntryLine:
    account_key: str
    amount: float
    sign: int


@dataclass
class TracePayload:
    extractor: str = "blueprint_only"
    used_ai: bool = False
    notes: List[str] = field(default_factory=list)


@dataclass
class UnifiedDocument:
    schema_version: int
    source_file: str
    source_kind: str
    document_type: str
    institution: Optional[str]
    card_brand: Optional[str]
    period: Optional[str]
    currency: str
    items: List[MovementItem] = field(default_factory=list)
    totals: Dict[str, float] = field(default_factory=dict)
    summary: SummaryPayload = field(default_factory=SummaryPayload)
    proposed_entry: List[ProposedEntryLine] = field(default_factory=list)
    trace: TracePayload = field(default_factory=TracePayload)


def _norm_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = text.upper()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def detect_source_kind(file_path: Path) -> str:
    ext = file_path.suffix.lower()
    if ext == ".pdf":
        return "pdf"
    if ext in {".xls", ".xlsx", ".xlsm"}:
        return "spreadsheet"
    if ext in {".jpg", ".jpeg", ".png", ".webp", ".tif", ".tiff"}:
        return "image"
    return "unknown"


def read_pdf_sample(file_path: Path, max_pages: int = 3) -> str:
    if PdfReader is None:
        return ""
    try:
        reader = PdfReader(str(file_path))
    except Exception:
        return ""

    blocks: List[str] = []
    for page in reader.pages[: max(1, int(max_pages))]:
        try:
            blocks.append(page.extract_text() or "")
        except Exception:
            continue
    return "\n".join(blocks)


def detect_institution(sample_text: str, file_name: str) -> Optional[str]:
    probe = _norm_text(sample_text + "\n" + file_name)
    if "GALICIA" in probe:
        return "BANCO GALICIA"
    if "PATAGONIA" in probe:
        return "BANCO PATAGONIA S.A."
    if "NACION" in probe or "BANCO DE LA NACION ARGENTINA" in probe:
        return "BANCO DE LA NACION ARGENTINA"
    if "SANTANDER" in probe:
        return "BANCO SANTANDER"
    return None


def detect_card_brand(sample_text: str, file_name: str) -> Optional[str]:
    probe = _norm_text(sample_text + "\n" + file_name)
    if "MASTERCARD" in probe or re.search(r"\bMASTER\b", probe):
        return "TARJETA MASTERCARD"
    if "VISA" in probe:
        return "TARJETA VISA"
    if "AMEX" in probe or "AMERICAN EXPRESS" in probe:
        return "TARJETA AMEX"
    if "CABAL" in probe:
        return "TARJETA CABAL"
    return None


def detect_document_type(sample_text: str, source_kind: str, institution: Optional[str], card_brand: Optional[str]) -> str:
    probe = _norm_text(sample_text)
    has_bank_markers = any(
        marker in probe
        for marker in [
            "RESUMEN DE CUENTA",
            "ESTADO DE CUENTAS",
            "CUENTA CORRIENTE",
            "MOVIMIENTOS",
            "DEBITOS",
            "CREDITOS",
        ]
    )
    has_card_markers = any(
        marker in probe
        for marker in [
            "TOTAL PRESENTADO",
            "NETO DE PAGOS",
            "NETO PERCIBIDO",
            "LIQUIDACION",
            "DETALLE DE DESCUENTOS",
        ]
    )
    has_card_movements_inside_bank = any(
        marker in probe
        for marker in [
            "CR LIQ VISA",
            "D LIQ VISA",
            "MASTERCARD COM",
            "VENTA CON TARJETA",
            "TODOCARD",
        ]
    )
    has_monthly_settlement_layout = any(
        marker in probe
        for marker in [
            "RESUMEN MENSUAL DE LIQUIDACIONES",
            "TOTAL PRESENTADO",
            "TOTAL DESCUENTO",
            "NETO PERCIBIDO",
            "DETALLE DE DESCUENTOS",
            "DESGLOSE DE DESCUENTOS",
            "FECHA DE PAGO",
        ]
    )

    if source_kind == "spreadsheet":
        return "bank_statement"
    if has_monthly_settlement_layout:
        return "card_settlement"
    if has_bank_markers and (has_card_markers or has_card_movements_inside_bank):
        return "mixed_financial"
    if has_bank_markers and card_brand and has_card_movements_inside_bank:
        return "mixed_financial"
    if has_card_markers or (card_brand and not institution):
        return "card_settlement"
    if has_bank_markers or institution:
        return "bank_statement"
    return "unknown"


def detect_period(sample_text: str, file_name: str) -> Optional[str]:
    probe = sample_text + "\n" + file_name
    match = re.search(r"\b(\d{2}/\d{2}/\d{4})\b", probe)
    if match:
        return match.group(1)
    match = re.search(r"\b(20\d{2})[-_](\d{2})[-_](\d{2})\b", file_name)
    if match:
        y, m, d = match.groups()
        return f"{d}/{m}/{y}"
    return None


def infer_period_from_file_name(file_name: str) -> Optional[str]:
    raw = _norm_text(file_name)
    m = re.search(r"(?<!\d)(20\d{2})[_\-](\d{2})[_\-](\d{2})(?!\d)", raw)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"

    m = re.search(r"\b(\d{1,2})[-_/](\d{2})(?!\d)", raw)
    if m:
        month = int(m.group(1))
        year2 = int(m.group(2))
        if 1 <= month <= 12:
            year = 2000 + year2
            last_day = 31
            try:
                import calendar
                last_day = calendar.monthrange(year, month)[1]
            except Exception:
                pass
            return f"{last_day:02d}-{month:02d}-{year}"

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
    mm = re.search(r"\b(ENERO|FEBRERO|MARZO|ABRIL|MAYO|JUNIO|JULIO|AGOSTO|SEPTIEMBRE|SETIEMBRE|OCTUBRE|NOVIEMBRE|DICIEMBRE)\s+(20\d{2})\b", raw)
    if mm:
        month = month_map[mm.group(1)]
        year = int(mm.group(2))
        import calendar
        last_day = calendar.monthrange(year, month)[1]
        return f"{last_day:02d}-{month:02d}-{year}"
    return None


def build_totals_template() -> Dict[str, float]:
    return {key: 0.0 for key in ACCOUNT_KEYS}


def normalize_category(raw_category: str) -> str:
    norm = _norm_text(raw_category)
    mapping = {
        "GASTO": "GASTOS_BANCARIOS",
    }
    return mapping.get(norm, norm or "OTROS")


def _normalize_for_matching(value: str) -> str:
    text = _norm_text(value)
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def match_spreadsheet_category(desc: str, compiled_rules: List[Any]) -> Optional[str]:
    category = bank_xls._match_category(desc, compiled_rules)
    if category:
        return category

    relaxed = _normalize_for_matching(desc)
    transfer_like_markers = [
        "TRANSFERENCIA",
        "TRANSFER",
        "DEPOSITO",
        "DEBIN",
        "PAGO PROVEEDOR",
        "SUELDO",
        "HABERES",
    ]
    strong_tax_markers = [
        "PERCEPCION IVA",
        "IVA PERCEPCION",
        "PERC IVA",
        "PERCEP IVA",
        "RET IVA",
        "RETENCION IVA",
        "RG 2408",
        "RG2408",
        "IVA BASE",
        "IVA ALICUOTA",
        "CRED FISC IVA",
        "CREDITO FISCAL IVA",
        "BASE IMPONIBLE IVA",
        "IVA 21",
        "IVA 10 5",
        "IVA ABRIL",
        "IVA MAYO",
        "IVA JUNIO",
        "IVA JULIO",
        "IVA AGOSTO",
        "IVA SEPTIEMBRE",
        "IVA SETIEMBRE",
        "IVA OCTUBRE",
        "IVA NOVIEMBRE",
        "IVA DICIEMBRE",
        "IVA ENERO",
        "IVA FEBRERO",
        "IVA MARZO",
    ]
    looks_transfer_like = any(marker in relaxed for marker in transfer_like_markers)
    has_strong_tax_signal = any(marker in relaxed for marker in strong_tax_markers)

    if "SUSS" in relaxed or "SEG SOCIAL" in relaxed or "SEGURIDAD SOCIAL" in relaxed:
        return "RET_SUSS"
    if "MUNICIPAL" in relaxed or "TASA MUNIC" in relaxed or "MUNICP" in relaxed:
        return "PERCEP_MUNICIPAL"
    if "IVA PERCEPCION" in relaxed or "I V A PERCEPCION" in relaxed or "PERCEPCION IVA" in relaxed or "PERC IVA" in relaxed or "PERCEP IVA" in relaxed or "RET IVA" in relaxed:
        if looks_transfer_like and not has_strong_tax_signal:
            return None
        return "RET_IVA"
    if (
        "IVA BASE" in relaxed
        or "I V A BASE" in relaxed
        or "IVA ALICUOTA" in relaxed
        or "I V A ALICUOTA" in relaxed
        or re.match(r"^IVA\s+(\d+(?:[.,]\d+)?(?:\s*%)?|[A-Z]+(?:\s+\d{4})?)\b", relaxed)
        or re.match(r"^I V A\s+(\d+(?:[.,]\d+)?(?:\s*%)?|[A-Z]+(?:\s+\d{4})?)\b", relaxed)
    ):
        if looks_transfer_like and not has_strong_tax_signal:
            return None
        return "IVA_CREDITO"
    if "RET GAN" in relaxed or "RETENCION GANAN" in relaxed:
        return "RET_GAN"
    if "IIBB" in relaxed or "ING BRUT" in relaxed or "SIRCREB" in relaxed:
        return "RET_IIBB"
    if "AJUSTE TARJETA" in relaxed or "DIF TARJETA" in relaxed or "AJ LIQ TARJ" in relaxed:
        return "AJUSTES_TARJETA"
    if "COSTO FINANCIERO" in relaxed or "SERV COSTOS FINANCIEROS" in relaxed or "COBRO ANTICIPADO" in relaxed or "SERV OPER" in relaxed:
        return "COMISION_FINANCIERA"
    if "IMPUESTO" in relaxed or "TRIBUTO" in relaxed or "SELLADO" in relaxed:
        return "IMPUESTOS_VARIOS"
    if "GRAVAMEN LEY 25413" in relaxed or "IMP DB CR" in relaxed:
        return "IDC_A_COMPUTAR"
    if "ARANCEL" in relaxed:
        return "ARANCEL_TARJETA"
    if "COMISION" in relaxed or "COMIS " in f"{relaxed} " or "INTERES" in relaxed:
        return "GASTOS_BANCARIOS"
    return None


def build_summary(
    document_type: str,
    institution: Optional[str],
    card_brand: Optional[str],
    sample_text: str,
    source_kind: str,
) -> SummaryPayload:
    parts: List[str] = []
    if institution:
        parts.append(institution)
    if card_brand:
        parts.append(card_brand)
    if document_type == "mixed_financial":
        parts.append("documento mixto con movimientos bancarios y de tarjeta")
    elif document_type == "bank_statement":
        parts.append("extracto bancario")
    elif document_type == "card_settlement":
        parts.append("liquidacion de tarjeta")

    warnings: List[str] = []
    if source_kind == "pdf" and not sample_text.strip():
        warnings.append("No se pudo extraer texto de muestra; puede requerir OCR o IA visual.")
    if document_type == "unknown":
        warnings.append("No se pudo determinar con confianza el tipo de documento.")

    return SummaryPayload(
        short_text=", ".join(parts).strip(", "),
        warnings=warnings,
        needs_review=bool(warnings),
    )


def build_blueprint(file_path: Path) -> UnifiedDocument:
    source_kind = detect_source_kind(file_path)
    sample_text = read_pdf_sample(file_path) if source_kind == "pdf" else ""
    institution = detect_institution(sample_text, file_path.name)
    card_brand = detect_card_brand(sample_text, file_path.name)
    document_type = detect_document_type(sample_text, source_kind, institution, card_brand)
    period = detect_period(sample_text, file_path.name)

    trace = TracePayload(
        extractor="blueprint_only",
        used_ai=False,
        notes=[
            "Primera version: sin extraccion contable completa.",
            "Pensado para servir de contrato comun entre extractores especializados.",
        ],
    )

    proposed_entry: List[ProposedEntryLine] = []
    if document_type == "card_settlement":
        proposed_entry = [
            ProposedEntryLine(account_key="TARJETA", amount=0.0, sign=1),
            ProposedEntryLine(account_key="BANCO", amount=0.0, sign=-1),
        ]
    elif document_type in {"bank_statement", "mixed_financial"}:
        proposed_entry = [ProposedEntryLine(account_key="BANCO", amount=0.0, sign=1)]

    return UnifiedDocument(
        schema_version=SCHEMA_VERSION,
        source_file=file_path.name,
        source_kind=source_kind,
        document_type=document_type,
        institution=institution,
        card_brand=card_brand,
        period=period,
        currency="ARS",
        items=[],
        totals=build_totals_template(),
        summary=build_summary(document_type, institution, card_brand, sample_text, source_kind),
        proposed_entry=proposed_entry,
        trace=trace,
    )


def _detect_direction(amount: float) -> str:
    if amount > 0:
        return "credit"
    if amount < 0:
        return "debit"
    return "neutral"


def _build_proposed_entry_from_totals(totals: Dict[str, float]) -> List[ProposedEntryLine]:
    lines: List[ProposedEntryLine] = []
    for key in ACCOUNT_KEYS:
        amount = round(float(totals.get(key, 0.0)), 2)
        if abs(amount) < 0.005:
            continue
        lines.append(
            ProposedEntryLine(
                account_key=key,
                amount=abs(amount),
                sign=1 if amount >= 0 else -1,
            )
        )
    return lines


def apply_iva_net_adjustment(
    doc: UnifiedDocument,
    assumed_rate: float = 0.21,
    tolerance: float = 1.0,
    explicit_base: Optional[float] = None,
    base_categories: Optional[List[str]] = None,
) -> None:
    iva_credito = abs(float(doc.totals.get("IVA_CREDITO", 0.0)))
    base_categories = base_categories or ["GASTOS_BANCARIOS"]
    gasto_neto = sum(abs(float(doc.totals.get(cat, 0.0))) for cat in base_categories)
    if iva_credito <= 0 or assumed_rate <= 0:
        doc.trace.notes.append("iva_adjustment=not_applicable")
        return

    neto_teorico = round(float(explicit_base), 2) if explicit_base is not None else round(iva_credito / assumed_rate, 2)
    diferencia = round(neto_teorico - gasto_neto, 2)
    if abs(diferencia) <= tolerance:
        diferencia = 0.0

    doc.totals["NETO_GRAVADO_POR_AJUSTE_IVA"] = -diferencia

    doc.trace.notes.append(f"iva_assumed_rate={assumed_rate:.4f}")
    doc.trace.notes.append(f"iva_neto_teorico={neto_teorico:.2f}")
    doc.trace.notes.append(f"iva_gasto_base={gasto_neto:.2f}")
    doc.trace.notes.append(f"iva_neto_diff={diferencia:.2f}")

    doc.items = [item for item in doc.items if item.category != "NETO_GRAVADO_POR_AJUSTE_IVA"]
    if diferencia != 0.0:
        doc.items.append(
            MovementItem(
                date=None,
                description="NETO GRAVADO POR AJUSTE IVA",
                raw_amount=round(abs(diferencia), 2),
                signed_amount=round(-diferencia, 2),
                direction=_detect_direction(-diferencia),
                channel="adjustment",
                category="NETO_GRAVADO_POR_AJUSTE_IVA",
                confidence=1.0,
                source_section="derived_adjustment",
            )
        )
        doc.summary.warnings.append(
            f"Se infirio neto gravado adicional por IVA: base teorica {neto_teorico:.2f}, diferencia {diferencia:.2f}."
        )
        doc.summary.needs_review = True

    subtotal = sum(float(value) for key, value in doc.totals.items() if key != "BANCO")
    doc.totals["BANCO"] = round(-subtotal, 2)


def _format_amount(value: float) -> str:
    return f"{float(value):.2f}"


def _short_bank_label(bank_name: Optional[str]) -> str:
    bank_s = (bank_name or "").strip()
    norm = _norm_text(bank_s)
    if "NACION" in norm:
        return "BANCO NACION"
    if "PATAGONIA" in norm:
        return "BANCO PATAGONIA"
    if "GALICIA" in norm:
        return "BANCO GALICIA"
    return bank_s or "BANCO"


def _display_category_label(category: str, bank_name: Optional[str]) -> str:
    if category == "BANCO":
        return _short_bank_label(bank_name)
    mapping = {
        "GASTOS_BANCARIOS": "GASTOS_BANCARIOS",
        "ARANCEL_TARJETA": "ARANCEL_TARJETA",
        "NETO_GRAVADO_POR_AJUSTE_IVA": "NETO GRAVADO POR AJUSTE IVA",
        "COMISION_FINANCIERA": "COMISION FINANCIERA",
        "RET_SUSS": "RET_SUSS",
        "PERCEP_MUNICIPAL": "PERCEP_MUNICIPAL",
        "IMPUESTOS_VARIOS": "IMPUESTOS_VARIOS",
        "AJUSTES_TARJETA": "AJUSTES_TARJETA",
    }
    return mapping.get(category, category)


def _document_origin_label(document_type: str) -> str:
    mapping = {
        "bank_statement": "EXTRACTO BANCARIO",
        "card_settlement": "LIQUIDACION DE TARJETA",
        "mixed_financial": "DOCUMENTO MIXTO",
        "unknown": "NO DETERMINADO",
    }
    return mapping.get((document_type or "").strip().lower(), "NO DETERMINADO")


def build_txt_output(doc: UnifiedDocument) -> str:
    bank_s = (doc.institution or "").strip()
    card_s = (doc.card_brand or "").strip()
    period_s = (doc.period or "").strip()

    if doc.document_type == "bank_statement" and not card_s:
        concept = f"GB {period_s} {bank_s}".strip()
        concept = re.sub(r"\s+", " ", concept)
        if len(concept) > 50:
            concept = concept[:50].rstrip()
        header = "\n".join(
            [
                bank_s,
                "GASTOS BANCARIOS",
                period_s,
                concept,
                "CONCEPTO|IMPORTE",
            ]
        )
        lines = [
            f"{_display_category_label('BANCO', bank_s)}|{_format_amount(doc.totals.get('BANCO', 0.0))}",
            f"{_display_category_label('GASTOS_BANCARIOS', bank_s)}|{_format_amount(doc.totals.get('GASTOS_BANCARIOS', 0.0))}",
            f"{_display_category_label('ARANCEL_TARJETA', bank_s)}|{_format_amount(doc.totals.get('ARANCEL_TARJETA', 0.0))}",
            f"{_display_category_label('COMISION_FINANCIERA', bank_s)}|{_format_amount(doc.totals.get('COMISION_FINANCIERA', 0.0))}",
            f"{_display_category_label('NETO_GRAVADO_POR_AJUSTE_IVA', bank_s)}|{_format_amount(doc.totals.get('NETO_GRAVADO_POR_AJUSTE_IVA', 0.0))}",
            f"{_display_category_label('IDC_A_COMPUTAR', bank_s)}|{_format_amount(doc.totals.get('IDC_A_COMPUTAR', 0.0))}",
            f"{_display_category_label('IVA_CREDITO', bank_s)}|{_format_amount(doc.totals.get('IVA_CREDITO', 0.0))}",
            f"{_display_category_label('RET_IVA', bank_s)}|{_format_amount(doc.totals.get('RET_IVA', 0.0))}",
            f"{_display_category_label('RET_IIBB', bank_s)}|{_format_amount(doc.totals.get('RET_IIBB', 0.0))}",
            f"{_display_category_label('RET_GAN', bank_s)}|{_format_amount(doc.totals.get('RET_GAN', 0.0))}",
            f"{_display_category_label('RET_SUSS', bank_s)}|{_format_amount(doc.totals.get('RET_SUSS', 0.0))}",
            f"{_display_category_label('PERCEP_MUNICIPAL', bank_s)}|{_format_amount(doc.totals.get('PERCEP_MUNICIPAL', 0.0))}",
            f"{_display_category_label('IMPUESTOS_VARIOS', bank_s)}|{_format_amount(doc.totals.get('IMPUESTOS_VARIOS', 0.0))}",
            f"{_display_category_label('AJUSTES_TARJETA', bank_s)}|{_format_amount(doc.totals.get('AJUSTES_TARJETA', 0.0))}",
            f"{_display_category_label('OTROS', bank_s)}|{_format_amount(doc.totals.get('OTROS', 0.0))}",
        ]
        return header + "\n" + "\n".join(lines) + "\n"

    second_line = card_s or "MOVIMIENTOS FINANCIEROS"
    concept = f"LIQ {period_s} {card_s} {bank_s}".strip()
    concept = re.sub(r"\s+", " ", concept)
    if len(concept) > 50:
        concept = concept[:50].rstrip()
    header = "\n".join(
        [
            bank_s,
            second_line,
            period_s,
            concept,
            "CONCEPTO|IMPORTE",
        ]
    )
    ordered = [
        "TARJETA",
        "BANCO",
        "GASTOS_BANCARIOS",
        "ARANCEL_TARJETA",
        "COMISION_FINANCIERA",
        "IVA_CREDITO",
        "RET_IVA",
        "RET_IIBB",
        "RET_GAN",
        "RET_SUSS",
        "PERCEP_MUNICIPAL",
        "IDC_A_COMPUTAR",
        "IMPUESTOS_VARIOS",
        "AJUSTES_TARJETA",
        "NETO_GRAVADO_POR_AJUSTE_IVA",
        "OTROS",
    ]
    lines = [
        f"{_display_category_label(key, bank_s)}|{_format_amount(doc.totals.get(key, 0.0))}"
        for key in ordered
    ]
    return header + "\n" + "\n".join(lines) + "\n"


def build_log_output(doc: UnifiedDocument) -> str:
    lines = [
        f"source_file={doc.source_file}",
        f"source_kind={doc.source_kind}",
        f"document_type={doc.document_type}",
        f"document_origin={_document_origin_label(doc.document_type)}",
        f"institution={doc.institution or ''}",
        f"card_brand={doc.card_brand or ''}",
        f"period={doc.period or ''}",
        f"items_count={len(doc.items)}",
        f"needs_review={int(bool(doc.summary.needs_review))}",
        f"extractor={doc.trace.extractor}",
        f"used_ai={int(bool(doc.trace.used_ai))}",
    ]
    for key in ACCOUNT_KEYS:
        lines.append(f"total_{key}={_format_amount(doc.totals.get(key, 0.0))}")
    for warning in doc.summary.warnings:
        lines.append(f"warning={warning}")
    for note in doc.trace.notes:
        lines.append(f"trace_note={note}")
    return "\n".join(lines) + "\n"


def build_control_concepts_output(doc: UnifiedDocument) -> str:
    concept_totals: Dict[str, Dict[str, Any]] = {}
    for item in doc.items:
        concept = (item.description or "").replace("\t", " ").replace("\r", " ").replace("\n", " ").strip()
        if not concept:
            continue
        bucket = concept_totals.setdefault(
            concept,
            {"category": item.category, "count": 0, "amount": 0.0},
        )
        bucket["count"] = int(bucket["count"]) + 1
        bucket["amount"] = float(bucket["amount"]) + float(item.signed_amount)

    lines = [
        "DATOS_DOCUMENTO\tVALOR",
        f"ORIGEN\t{_document_origin_label(doc.document_type)}",
        f"BANCO\t{(doc.institution or '').strip()}",
        f"TARJETA\t{(doc.card_brand or '').strip()}",
        f"PERIODO\t{(doc.period or '').strip()}",
        f"TIPO_DOCUMENTO\t{(doc.document_type or '').strip()}",
        "",
        "LINEA\tCONCEPTO\tCATEGORIA\tCANTIDAD\tIMPORTE_TOTAL",
    ]
    idx = 1
    for concept, info in sorted(concept_totals.items(), key=lambda it: abs(float(it[1]["amount"])), reverse=True):
        lines.append(
            f"{idx}\t{concept}\t{info['category']}\t{int(info['count'])}\t{float(info['amount']):.2f}"
        )
        idx += 1

    lines.append("")
    lines.append("RESUMEN_POR_CATEGORIA\t\t\t\t")
    lines.append("LINEA\tCATEGORIA\tIMPORTE_TOTAL\t\t")
    j = 1
    for key in ACCOUNT_KEYS:
        lines.append(f"{j}\t{key}\t{float(doc.totals.get(key, 0.0)):.2f}\t\t")
        j += 1

    iva_credito = abs(float(doc.totals.get("IVA_CREDITO", 0.0)))
    gasto_neto = abs(float(doc.totals.get("GASTOS_BANCARIOS", 0.0))) + abs(float(doc.totals.get("ARANCEL_TARJETA", 0.0)))
    ajuste_neto = abs(float(doc.totals.get("NETO_GRAVADO_POR_AJUSTE_IVA", 0.0)))
    neto_total = round(gasto_neto + ajuste_neto, 2)
    iva_teorico = round(neto_total * 0.21, 2)
    diff = round(iva_credito - iva_teorico, 2)
    lines.append("")
    lines.append("IVA_21_CONTROL\t\t\t\t")
    lines.append("BASE_GASTOS_Y_ARANCELES\tAJUSTE_NETO\tNETO_GRAVADO_TOTAL\tIVA_CREDITO\tIVA_TEORICO_21\tDIFERENCIA")
    lines.append(f"{gasto_neto:.2f}\t{ajuste_neto:.2f}\t{neto_total:.2f}\t{iva_credito:.2f}\t{iva_teorico:.2f}\t{diff:.2f}")
    return "\n".join(lines) + "\n"


def _normalize_outdir_arg(value: str) -> str:
    raw = (value or "").strip()
    if not raw:
        return ""
    raw = raw.replace('"', "").strip()
    raw = re.sub(r"[\\/]+\s*$", "", raw)
    if raw.startswith("\\\\") and raw.count("\\") < 3:
        return raw
    return raw


def _ask_user_for_outdir(initial_dir: str = "", parent=None) -> str:
    if tk is None or filedialog is None:
        return ""
    created_root = None
    try:
        if parent is None:
            created_root = tk.Tk()
            created_root.withdraw()
            parent = created_root
        selected = filedialog.askdirectory(
            parent=parent,
            title="Elegí una carpeta de salida",
            initialdir=initial_dir if initial_dir and os.path.isdir(initial_dir) else None,
            mustexist=False,
        )
        return (selected or "").strip()
    finally:
        if created_root is not None:
            try:
                created_root.destroy()
            except Exception:
                pass


def _resolve_output_dir(preferred_outdir: str, ui: Optional[StatusUI] = None) -> Path:
    normalized = _normalize_outdir_arg(preferred_outdir)
    candidate = Path(normalized) if normalized else Path.cwd()
    try:
        candidate.mkdir(parents=True, exist_ok=True)
        return candidate
    except Exception as e:
        message = f"No se pudo usar la carpeta de salida '{candidate}': {e}"
        if ui:
            ui.push(message)
            ui.push("Seleccioná otra carpeta de salida para continuar.")
        fallback = _ask_user_for_outdir(str(candidate.parent) if candidate.parent != candidate else "", getattr(ui, "root", None))
        if fallback:
            fallback_path = Path(_normalize_outdir_arg(fallback))
            try:
                fallback_path.mkdir(parents=True, exist_ok=True)
                if ui:
                    ui.push(f"Usando nueva carpeta de salida: {fallback_path}")
                return fallback_path
            except Exception as e2:
                raise SystemExit(f"ERROR: No se pudo usar la carpeta elegida: {fallback_path} ({e2})") from e2
        raise SystemExit(message) from e


def write_output_files(doc: UnifiedDocument, outdir: Path, source_file: Path) -> Dict[str, Path]:
    outdir.mkdir(parents=True, exist_ok=True)
    base = source_file.stem
    json_path = outdir / f"{base}.unificado.json"
    txt_path = outdir / f"{base}.txt"
    log_path = outdir / f"{base}.log"
    control_path = outdir / f"{base}_control_conceptos.xls"
    json_path.write_text(json.dumps(document_to_dict(doc), ensure_ascii=False, indent=2), encoding="utf-8")
    txt_path.write_text(build_txt_output(doc), encoding="utf-8")
    log_path.write_text(build_log_output(doc), encoding="utf-8")
    control_path.write_text(build_control_concepts_output(doc), encoding="utf-8")
    return {
        "json_path": json_path,
        "txt_path": txt_path,
        "log_path": log_path,
        "control_path": control_path,
    }


def _read_pdf_pages(file_path: Path) -> List[str]:
    if PdfReader is None:
        return []
    try:
        reader = PdfReader(str(file_path))
    except Exception:
        return []
    pages: List[str] = []
    for page in reader.pages:
        try:
            pages.append(page.extract_text() or "")
        except Exception:
            pages.append("")
    return pages


def _normalize_pdf_text_for_records(text: str) -> str:
    if not text:
        return ""
    text = text.replace("\r", "\n")
    text = re.sub(r"(\d{12})(\d{1,3}(?:\.\d{3})*,\d{2})", r"\1 \2", text)
    text = re.sub(r"(\d{8})(\d{1,3}(?:\.\d{3})*,\d{2})", r"\1 \2", text)
    text = re.sub(r"(\d{6,})(\d{1,3}(?:\.\d{3})*,\d{2})", r"\1 \2", text)
    text = re.sub(r"(?<!\n)-(?=\d{1,2}/\d{2}/\d{2})", "\n", text)
    text = re.sub(r"(?<!\n)(?=\d{1,2}/\d{2}/\d{2}\b)", "\n", text)
    text = re.sub(r"(\d,\d{2})(?=\d{1,3}\.)", r"\1 ", text)
    text = re.sub(r"(\d,\d{2})(?=\d{1,2}/\d{2}/\d{2}\b)", r"\1\n", text)
    text = re.sub(r"(\d,\d{2})(?=-\d{1,2}/\d{2}/\d{2}\b)", r"\1\n", text)
    text = re.sub(r"[ \t]+", " ", text)
    return text


def _split_pdf_records(page_texts: List[str]) -> List[str]:
    records: List[str] = []
    date_pat = re.compile(r"(?<!\d)(\d{1,2}/\d{2}/\d{2})(?!\d)")
    for raw_text in page_texts:
        text = _normalize_pdf_text_for_records(raw_text)
        lines = [ln.strip() for ln in text.splitlines() if ln.strip()]
        current: Optional[str] = None
        for line in lines:
            if date_pat.match(line):
                if current:
                    records.append(current.strip())
                current = line
            elif current:
                current = f"{current} {line}".strip()
        if current:
            records.append(current.strip())
    return records


def _parse_pdf_record(record: str) -> Optional[Dict[str, Any]]:
    m = re.match(r"^(\d{1,2})/(\d{2})/(\d{2})\s+(.*)$", record.strip())
    if not m:
        return None
    day, month, year2 = m.group(1), m.group(2), m.group(3)
    body = m.group(4).strip()
    money_pat = re.compile(r"-?\d{1,3}(?:\.\d{3})*,\d{2}")
    amount_match = money_pat.search(body)
    if not amount_match:
        return None
    amount_txt = amount_match.group(0)
    desc = body[: amount_match.start()].strip(" -")
    if not desc:
        return None
    try:
        year = int(year2)
        full_year = 2000 + year if year < 100 else year
        date_full = f"{int(day):02d}/{month}/{full_year:04d}"
    except Exception:
        date_full = f"{day}/{month}/{year2}"
    return {
        "date": date_full,
        "description": re.sub(r"\s+", " ", desc).strip(),
        "amount_text": amount_txt,
        "raw_amount": bank_xls._parse_ar_number(amount_txt),
        "record_text": record,
    }


def _detect_pdf_card_category(description: str) -> Optional[str]:
    norm = _normalize_for_matching(description)
    if any(token in norm for token in ["CR LIQ VISA", "CR LIQ MASTER", "MASTERCARD COM", "VENTA CON TARJETA"]):
        return "TARJETA"
    if any(token in norm for token in ["D LIQ VISA", "DEB LIQ MASTERC", "DEB LIQ MASTER", "D LIQ MASTER"]):
        return "ARANCEL_TARJETA"
    return None


def _signed_amount_for_category(category: str, raw_amount: float) -> float:
    amount = abs(float(raw_amount))
    if category == "TARJETA":
        return amount
    return -amount


def _totals_from_card_daily_rows(daily_rows: List[Dict[str, Any]]) -> Dict[str, float]:
    totals = build_totals_template()
    for row in daily_rows:
        concepts = row.get("concepts") or {}
        totals["TARJETA"] += float(concepts.get("VENTAS C/DESCUENTO CONTADO", 0.0))
        totals["ARANCEL_TARJETA"] -= abs(float(concepts.get("ARANCEL", 0.0)))
        totals["COMISION_FINANCIERA"] -= abs(float(concepts.get("SERVICIO OPER. INTERNAC.", 0.0)))
        totals["COMISION_FINANCIERA"] -= abs(float(concepts.get("IVA RI SERV.OPER. INT.", 0.0)))
        totals["IVA_CREDITO"] -= abs(float(concepts.get("IVA CRED.FISC.COMERCIO S/ARANC 21,00%", 0.0)))
        totals["RET_IIBB"] -= abs(float(concepts.get("RETENCION ING.BRUTOS SIRTAC", 0.0)))
        totals["RET_IVA"] -= abs(float(concepts.get("PERCEPCION IVA R.G. 2408 3,00 %", 0.0)))
        totals["RET_IVA"] -= abs(float(concepts.get("QR PERCEPCION IVA 3337", 0.0)))
        totals["PERCEP_MUNICIPAL"] -= abs(float(concepts.get("QR RETENCION IIBB RIO NEGRO", 0.0)))
        totals["RET_GAN"] -= abs(float(concepts.get("RETENCION GANANCIAS", 0.0)))
        totals["BANCO"] -= abs(float(concepts.get("IMPORTE NETO DE PAGOS", 0.0)))
    return {k: round(v, 2) for k, v in totals.items()}


def _extract_generic_card_daily_rows(page_texts: List[str]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    label_map = {
        "VENTAS C/DESCUENTO CONTADO": "VENTAS C/DESCUENTO CONTADO",
        "ARANCEL": "ARANCEL",
        "IVA CRED.FISC.COMERCIO S/ARANC": "IVA CRED.FISC.COMERCIO S/ARANC 21,00%",
        "RETENCION ING.BRUTOS SIRTAC": "RETENCION ING.BRUTOS SIRTAC",
        "PERCEPCION IVA R.G. 2408": "PERCEPCION IVA R.G. 2408 3,00 %",
        "IMPORTE NETO DE PAGOS": "IMPORTE NETO DE PAGOS",
    }

    for text in page_texts:
        current: Dict[str, float] = {}
        for raw_line in text.splitlines():
            line = re.sub(r"\s+", " ", raw_line.strip())
            if not line:
                continue
            upper = line.upper()
            matched_label = None
            for token, canon in label_map.items():
                if token in upper:
                    matched_label = canon
                    break
            if not matched_label:
                continue
            nums = re.findall(r"([0-9]{1,3}(?:\.\d{3})*,\d{2})", line)
            if not nums:
                continue
            amount = bank_xls._parse_ar_number(nums[-1])
            if amount <= 0:
                continue
            current[matched_label] = amount
            if matched_label == "IMPORTE NETO DE PAGOS" and current:
                rows.append({"concepts": dict(current)})
                current.clear()
    return rows


def _extract_patagonia_monthly_day_totals(page_texts: List[str]) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    text = "\n".join(page_texts)
    block_pat = re.compile(
        r"FECHA DE PAGO.*?Arancel \$\s*([0-9\.\,]+).*?"
        r"(?:Serv\.Costos Financieros \$\s*([0-9\.\,]+).*?)?"
        r"(?:Servicio PAYWAY \$\s*([0-9\.\,]+).*?)?"
        r"(?:Deduc\.Impositivas \$\s*([0-9\.\,]+).*?)?"
        r"Total del día\s*\$?\s*([0-9\.\,]+)\s*\$?\s*([0-9\.\,]+)\s*\$?\s*([0-9\.\,]+)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for m in block_pat.finditer(text):
        rows.append(
            {
                "ventas": bank_xls._parse_ar_number(m.group(5)),
                "descuentos": bank_xls._parse_ar_number(m.group(6)),
                "neto": bank_xls._parse_ar_number(m.group(7)),
                "arancel": bank_xls._parse_ar_number(m.group(1)),
                "financiero": bank_xls._parse_ar_number(m.group(2) or "0"),
                "payway": bank_xls._parse_ar_number(m.group(3) or "0"),
                "deducciones": bank_xls._parse_ar_number(m.group(4) or "0"),
            }
        )
    return rows


def _extract_monthly_total_dia_rows(page_texts: List[str]) -> List[Dict[str, float]]:
    rows: List[Dict[str, float]] = []
    text = "\n".join(page_texts)
    block_pat = re.compile(
        r"FECHA DE PAGO\s*([0-9]{2}/[0-9]{2}(?:/[0-9]{4})?).*?"
        r"Arancel \$\s*([0-9\.\,]+).*?"
        r"(?:Serv\.Costos Financieros \$\s*([0-9\.\,]+).*?)?"
        r"(?:Servicio PAYWAY \$\s*([0-9\.\,]+).*?)?"
        r"(?:Deduc\.Impositivas \$\s*([0-9\.\,]+).*?)?"
        r"(?:Serv\.Cobro Anticipado \$\s*([0-9\.\,]+).*?)?"
        r"Total del día\s*\$?\s*([0-9\.\,]+)\s*\$?\s*([0-9\.\,]+)\s*\$?\s*([0-9\.\,]+)",
        flags=re.IGNORECASE | re.DOTALL,
    )
    for m in block_pat.finditer(text):
        rows.append(
            {
                "fecha_pago": (m.group(1) or "").strip(),
                "arancel": bank_xls._parse_ar_number(m.group(2)),
                "financiero": bank_xls._parse_ar_number(m.group(3) or "0"),
                "payway": bank_xls._parse_ar_number(m.group(4) or "0"),
                "deducciones": bank_xls._parse_ar_number(m.group(5) or "0"),
                "cobro_anticipado": bank_xls._parse_ar_number(m.group(6) or "0"),
                "ventas": bank_xls._parse_ar_number(m.group(7)),
                "descuentos": bank_xls._parse_ar_number(m.group(8)),
                "neto": bank_xls._parse_ar_number(m.group(9)),
            }
        )
    return rows


def _totals_from_monthly_total_dia_rows(day_rows: List[Dict[str, float]]) -> Dict[str, float]:
    totals = build_totals_template()
    for row in day_rows:
        totals["TARJETA"] += float(row.get("ventas", 0.0))
        totals["BANCO"] -= abs(float(row.get("neto", 0.0)))
        totals["ARANCEL_TARJETA"] -= abs(float(row.get("arancel", 0.0)))
        totals["COMISION_FINANCIERA"] -= abs(float(row.get("financiero", 0.0)))
        totals["COMISION_FINANCIERA"] -= abs(float(row.get("payway", 0.0)))
        totals["COMISION_FINANCIERA"] -= abs(float(row.get("cobro_anticipado", 0.0)))
        totals["IMPUESTOS_VARIOS"] -= abs(float(row.get("deducciones", 0.0)))
    return {k: round(v, 2) for k, v in totals.items()}


def _extract_monthly_summary_breakdown(page_texts: List[str]) -> Dict[str, float]:
    text = "\n".join(page_texts)

    def _sum_matches(pattern: str) -> float:
        total = 0.0
        for match in re.finditer(pattern, text, flags=re.IGNORECASE | re.DOTALL):
            total += bank_xls._parse_ar_number(match.group(1))
        return round(total, 2)

    base_21 = _sum_matches(r"Tasa\s*21,00\s*%\s*\$\s*([0-9\.\,]+)")
    base_105 = _sum_matches(r"Tasa\s*10,50\s*%\s*\$\s*([0-9\.\,]+)")
    iva_21 = _sum_matches(r"IVA\s*21,00\s*%\s*\$\s*([0-9\.\,]+)")
    iva_105 = _sum_matches(r"IVA\s*10,50\s*%\s*(?:Ley\s*25\.063)?\s*\$\s*([0-9\.\,]+)")

    ret_iva = 0.0
    block_match = re.search(
        r"Percep\./Retenc\.AFIP\s*-\s*DGI\s*\$?\s*([0-9\.\,]+)",
        text,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if block_match:
        ret_iva = bank_xls._parse_ar_number(block_match.group(1))
    else:
        total_regimen = re.search(
            r"Percepci[oó]n\s+IVA\s+RG\s+2408.*?Total\s+Regimen\s+\d+\s*\$?\s*([0-9\.\,]+)",
            text,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if total_regimen:
            ret_iva = bank_xls._parse_ar_number(total_regimen.group(1))

    return {
        "base_iva": round(base_21 + base_105, 2),
        "iva_credito": round(iva_21 + iva_105, 2),
        "ret_iva": round(ret_iva, 2),
    }


def _totals_from_patagonia_monthly(pdf_info: Dict[str, Any], page_texts: List[str]) -> Dict[str, float]:
    totals = build_totals_template()
    day_rows = _extract_patagonia_monthly_day_totals(page_texts)
    desglose = pdf_info.get("patagonia_desglose") or []

    for row in day_rows:
        totals["TARJETA"] += float(row.get("ventas", 0.0))
        totals["BANCO"] -= abs(float(row.get("neto", 0.0)))
        totals["ARANCEL_TARJETA"] -= abs(float(row.get("arancel", 0.0)))
        totals["COMISION_FINANCIERA"] -= abs(float(row.get("financiero", 0.0)))
        totals["COMISION_FINANCIERA"] -= abs(float(row.get("payway", 0.0)))

    for item in desglose:
        label = _normalize_for_matching(item.get("label") or "")
        amount = abs(float(item.get("amount") or 0.0))
        if amount <= 0:
            continue
        if "BASE IMPONIBLE IVA" in label or "MONTO GRAVADO" in label:
            continue
        if label.startswith("IVA") or " IVA " in f" {label} ":
            totals["IVA_CREDITO"] -= amount
        elif "RET IB" in label or "SIRTAC" in label:
            totals["RET_IIBB"] -= amount
        elif "PERCEP" in label or "AFIP" in label or "DGI" in label:
            totals["RET_IVA"] -= amount
        elif "ARANCEL" in label:
            # Ya tomado desde total del dia; evitar duplicarlo.
            continue
        elif "SERVICIO COSTOS FINANCIEROS" in label or "SERV COBRO ANTICIPADO" in label or "VENTA EN D" in label or "VENTAS EN D" in label or "CARGO POR SERVICIO" in label or "PAYWAY" in label:
            # Ya tomado desde total del dia; evitar duplicarlo.
            continue
        else:
            totals["OTROS"] -= amount

    return {k: round(v, 2) for k, v in totals.items()}


def populate_from_card_pdf(doc: UnifiedDocument, file_path: Path) -> UnifiedDocument:
    pdf_info = card_pdf._extract_pdf_totals([str(file_path)])
    daily_rows = pdf_info.get("daily_rows") or []
    page_texts = _read_pdf_pages(file_path)
    if not daily_rows:
        daily_rows = _extract_generic_card_daily_rows(page_texts)
    monthly_day_rows = _extract_monthly_total_dia_rows(page_texts)
    if not daily_rows and pdf_info.get("bank_name") == "BANCO PATAGONIA S.A." and (pdf_info.get("patagonia_desglose") or []):
        totals = _totals_from_patagonia_monthly(pdf_info, page_texts)
        doc.document_type = "card_settlement"
        doc.institution = str(pdf_info.get("bank_name") or doc.institution or "")
        if pdf_info.get("card_name") and not (doc.card_brand or "").strip():
            doc.card_brand = str(pdf_info.get("card_name"))
        doc.period = str(pdf_info.get("period") or infer_period_from_file_name(file_path.name) or doc.period or "")
        doc.items = []
        doc.totals = totals
        saldo = abs(float(pdf_info.get("saldo") or 0.0))
        if saldo > 0:
            subtotal_no_bank = sum(float(value) for key, value in doc.totals.items() if key not in {"BANCO", "AJUSTES_TARJETA"})
            authoritative_bank = -saldo
            doc.totals["AJUSTES_TARJETA"] = round(-(authoritative_bank + subtotal_no_bank), 2)
            doc.totals["BANCO"] = round(authoritative_bank, 2)
        explicit_base = None
        for it in pdf_info.get("patagonia_desglose") or []:
            label = _normalize_for_matching(it.get("label") or "")
            if "BASE IMPONIBLE IVA" in label or "MONTO GRAVADO" in label:
                explicit_base = abs(float(it.get("amount") or 0.0))
                break
        apply_iva_net_adjustment(
            doc,
            explicit_base=explicit_base,
            base_categories=["ARANCEL_TARJETA", "COMISION_FINANCIERA", "AJUSTES_TARJETA"],
        )
        doc.proposed_entry = _build_proposed_entry_from_totals(doc.totals)
        doc.trace = TracePayload(
            extractor="card_pdf_patagonia_monthly_v1",
            used_ai=False,
            notes=[
                f"patagonia_day_rows={len(_extract_patagonia_monthly_day_totals(page_texts))}",
                f"patagonia_desglose={len(pdf_info.get('patagonia_desglose') or [])}",
                f"total_presentado={pdf_info.get('total_presentado')}",
                f"saldo={pdf_info.get('saldo')}",
            ],
        )
        doc.summary.short_text = f"{doc.institution or 'Documento financiero'}, resumen mensual Patagonia"
        return doc
    if not daily_rows and monthly_day_rows:
        doc.document_type = "card_settlement"
        doc.institution = str(pdf_info.get("bank_name") or doc.institution or "")
        if pdf_info.get("card_name") and not (doc.card_brand or "").strip():
            doc.card_brand = str(pdf_info.get("card_name"))
        doc.period = str(pdf_info.get("period") or infer_period_from_file_name(file_path.name) or doc.period or "")
        doc.items = []
        doc.totals = _totals_from_monthly_total_dia_rows(monthly_day_rows)
        summary_breakdown = _extract_monthly_summary_breakdown(page_texts)
        iva_credito = abs(float(summary_breakdown.get("iva_credito", 0.0)))
        ret_iva = abs(float(summary_breakdown.get("ret_iva", 0.0)))
        if iva_credito > 0:
            doc.totals["IVA_CREDITO"] = -iva_credito
            doc.totals["IMPUESTOS_VARIOS"] = round(doc.totals.get("IMPUESTOS_VARIOS", 0.0) + iva_credito, 2)
        if ret_iva > 0:
            doc.totals["RET_IVA"] = -ret_iva
            doc.totals["IMPUESTOS_VARIOS"] = round(doc.totals.get("IMPUESTOS_VARIOS", 0.0) + ret_iva, 2)
        apply_iva_net_adjustment(
            doc,
            explicit_base=summary_breakdown.get("base_iva") or None,
            base_categories=["ARANCEL_TARJETA", "COMISION_FINANCIERA"],
        )
        doc.proposed_entry = _build_proposed_entry_from_totals(doc.totals)
        doc.trace = TracePayload(
            extractor="card_pdf_monthly_total_dia_v1",
            used_ai=False,
            notes=[
                f"monthly_day_rows={len(monthly_day_rows)}",
                f"total_presentado={pdf_info.get('total_presentado')}",
                f"neto_header={pdf_info.get('neto_header')}",
                f"monthly_base_iva={summary_breakdown.get('base_iva')}",
                f"monthly_iva_credito={summary_breakdown.get('iva_credito')}",
                f"monthly_ret_iva={summary_breakdown.get('ret_iva')}",
            ],
        )
        doc.summary.short_text = f"{doc.institution or 'Documento financiero'}, liquidacion mensual con totales diarios"
        return doc

    if not daily_rows:
        doc.summary.warnings.append("El PDF fue detectado como liquidacion, pero no se pudieron extraer bloques diarios.")
        doc.summary.needs_review = True
        doc.trace.extractor = "card_pdf_no_daily_rows"
        return doc

    totals = _totals_from_card_daily_rows(daily_rows)
    items: List[MovementItem] = []
    concept_map = {
        "VENTAS C/DESCUENTO CONTADO": ("TARJETA", "card"),
        "ARANCEL": ("ARANCEL_TARJETA", "card"),
        "IVA CRED.FISC.COMERCIO S/ARANC 21,00%": ("IVA_CREDITO", "tax"),
        "RETENCION ING.BRUTOS SIRTAC": ("RET_IIBB", "tax"),
        "PERCEPCION IVA R.G. 2408 3,00 %": ("RET_IVA", "tax"),
        "QR PERCEPCION IVA 3337": ("RET_IVA", "tax"),
        "QR RETENCION IIBB RIO NEGRO": ("PERCEP_MUNICIPAL", "tax"),
        "RETENCION GANANCIAS": ("RET_GAN", "tax"),
        "SERVICIO OPER. INTERNAC.": ("COMISION_FINANCIERA", "card"),
        "IVA RI SERV.OPER. INT.": ("COMISION_FINANCIERA", "card"),
        "IMPORTE NETO DE PAGOS": ("BANCO", "bank"),
    }

    for row in daily_rows:
        fecha = row.get("fecha") or None
        concepts = row.get("concepts") or {}
        for label, amount in concepts.items():
            mapped = concept_map.get(label)
            if not mapped:
                continue
            category, channel = mapped
            signed = float(amount)
            if category != "TARJETA":
                signed = -abs(float(amount))
            items.append(
                MovementItem(
                    date=fecha,
                    description=label,
                    raw_amount=round(abs(float(amount)), 2),
                    signed_amount=round(float(signed), 2),
                    direction=_detect_direction(signed),
                    channel=channel,
                    category=category,
                    confidence=0.99,
                    source_section="card_pdf_daily_rows",
                )
            )

    doc.document_type = "card_settlement"
    if pdf_info.get("bank_name"):
        doc.institution = str(pdf_info.get("bank_name"))
    if pdf_info.get("card_name") and not (doc.card_brand or "").strip():
        doc.card_brand = str(pdf_info.get("card_name"))
    doc.period = infer_period_from_file_name(file_path.name) or (str(pdf_info.get("period")) if pdf_info.get("period") else doc.period)
    doc.items = items
    doc.totals = totals
    apply_iva_net_adjustment(doc)
    doc.proposed_entry = _build_proposed_entry_from_totals(doc.totals)
    doc.trace = TracePayload(
        extractor="card_pdf_daily_rows_v1",
        used_ai=False,
        notes=[
            f"daily_rows={len(daily_rows)}",
            f"ventas_sum={pdf_info.get('ventas_sum')}",
            f"neto_sum={pdf_info.get('neto_sum')}",
        ],
    )
    doc.summary.short_text = f"{doc.institution or 'Documento financiero'}, liquidacion de tarjeta con {len(daily_rows)} bloques diarios"
    return doc


def populate_from_bank_pdf(doc: UnifiedDocument, file_path: Path) -> UnifiedDocument:
    page_texts = _read_pdf_pages(file_path)
    if not page_texts:
        doc.summary.warnings.append("No se pudo leer texto del PDF.")
        doc.summary.needs_review = True
        doc.trace.extractor = "bank_pdf_unreadable"
        return doc

    pdf_info = card_pdf._extract_pdf_totals([str(file_path)])
    compiled_rules = bank_xls._compile_rules((bank_xls.DEFAULT_RULES.get("banks") or {}).get("BNA", {}).get("rules") or [])
    all_rules: List[Any] = []
    for spec in (bank_xls.DEFAULT_RULES.get("banks") or {}).values():
        all_rules.extend(bank_xls._compile_rules(spec.get("rules") or []))
    compiled_rules = all_rules

    records = _split_pdf_records(page_texts)
    items: List[MovementItem] = []
    totals = build_totals_template()
    unknown: Dict[str, int] = {}

    for record in records:
        parsed = _parse_pdf_record(record)
        if not parsed:
            continue
        desc = parsed["description"]
        upper_desc = _norm_text(desc)
        if any(marker in upper_desc for marker in ["SALDO ANTERIOR", "TRANSPORTE", "SIGUIENTE", "PAGINA"]):
            continue

        raw_category = _detect_pdf_card_category(desc)
        if raw_category is None:
            raw_category = match_spreadsheet_category(desc, compiled_rules)
        if raw_category is None:
            unknown[desc] = unknown.get(desc, 0) + 1
            continue

        category = normalize_category(raw_category)
        if category not in totals:
            category = "OTROS"
        signed_amount = _signed_amount_for_category(category, parsed["raw_amount"])
        if abs(signed_amount) < 0.005:
            continue

        channel = "card" if category == "TARJETA" else "bank"
        items.append(
            MovementItem(
                date=parsed["date"],
                description=desc,
                raw_amount=round(abs(float(parsed["raw_amount"])), 2),
                signed_amount=round(float(signed_amount), 2),
                direction=_detect_direction(signed_amount),
                channel=channel,
                category=category,
                confidence=0.9 if category == "TARJETA" else 0.96,
                source_section="pdf_movements",
            )
        )
        totals[category] = round(float(totals.get(category, 0.0)) + float(signed_amount), 2)

    if pdf_info.get("bank_name") and not doc.institution:
        doc.institution = str(pdf_info.get("bank_name"))
    if pdf_info.get("card_name") and not doc.card_brand:
        doc.card_brand = str(pdf_info.get("card_name"))
    doc.period = infer_period_from_file_name(file_path.name) or bank_xls._infer_period_end_date(file_path.name)

    has_card_rows = any(item.category == "TARJETA" for item in items)
    doc.document_type = "mixed_financial" if has_card_rows else "bank_statement"
    subtotal = sum(float(value) for key, value in totals.items() if key != "BANCO")
    totals["BANCO"] = round(-subtotal, 2)

    doc.items = items
    doc.totals = totals
    doc.trace = TracePayload(
        extractor="bank_pdf_movements_v1",
        used_ai=False,
        notes=[
            f"pages={len(page_texts)}",
            f"records_detected={len(records)}",
            f"items_classified={len(items)}",
            f"records_unclassified={sum(unknown.values())}",
        ],
    )
    kind_label = "documento mixto" if doc.document_type == "mixed_financial" else "extracto bancario"
    doc.summary.short_text = f"{doc.institution or 'Documento financiero'}, {kind_label} con {len(items)} movimientos clasificados"
    if not items:
        doc.summary.warnings.append("No se pudieron clasificar movimientos contables en el PDF.")
        doc.summary.needs_review = True
    elif unknown:
        top_unknown = sorted(unknown.items(), key=lambda it: it[1], reverse=True)[:5]
        doc.summary.warnings.append(
            "Conceptos PDF no clasificados: " + "; ".join(f"{desc} ({count})" for desc, count in top_unknown)
        )
        doc.summary.needs_review = True
    apply_iva_net_adjustment(doc)
    doc.proposed_entry = _build_proposed_entry_from_totals(doc.totals)
    return doc


def populate_from_bank_spreadsheet(doc: UnifiedDocument, file_path: Path) -> UnifiedDocument:
    staged_file, cleanup_path = bank_xls._stage_input_local(file_path)
    try:
        excel = bank_xls._run_powershell_excel_dump(staged_file, timeout_seconds=90)
    finally:
        if cleanup_path is not None:
            try:
                cleanup_path.unlink(missing_ok=True)
            except Exception:
                pass

    data = excel.get("data") or []
    if not data:
        doc.summary.warnings.append("La planilla no devolvió filas utilizables.")
        doc.summary.needs_review = True
        doc.trace.extractor = "bank_spreadsheet_empty"
        return doc

    try:
        bank_key = bank_xls._detect_bank(
            bank_xls.DEFAULT_RULES,
            str(excel.get("sheet_name") or ""),
            data,
            source_name=file_path.name,
        )
    except Exception:
        doc.summary.warnings.append("La planilla no parece un extracto bancario soportado.")
        doc.summary.needs_review = True
        doc.trace.extractor = "bank_spreadsheet_unsupported"
        return doc

    bank_spec = (bank_xls.DEFAULT_RULES.get("banks") or {}).get(bank_key) or {}
    cols_spec = bank_spec.get("columns") or {}
    header_req = []
    for key in ("date", "description", "amount", "debit", "credit"):
        value = cols_spec.get(key)
        if value:
            header_req.append(value)
    header_row_idx = bank_xls._find_header_row(data, header_req[:2] if len(header_req) >= 2 else header_req)
    if header_row_idx is None:
        doc.summary.warnings.append(f"No se encontró cabecera para {bank_key}.")
        doc.summary.needs_review = True
        doc.trace.extractor = "bank_spreadsheet_header_missing"
        return doc

    headers = data[header_row_idx]
    idx_date = bank_xls._col_index(headers, cols_spec.get("date", "Fecha"))
    idx_desc = bank_xls._col_index(headers, cols_spec.get("description", "Concepto"))
    idx_amount = bank_xls._col_index(headers, cols_spec.get("amount", "Importe"))
    idx_debit = bank_xls._col_index(headers, cols_spec.get("debit", "Debito"))
    idx_credit = bank_xls._col_index(headers, cols_spec.get("credit", "Credito"))
    if idx_date is None or idx_desc is None:
        doc.summary.warnings.append(f"Cabeceras incompletas para {bank_key}.")
        doc.summary.needs_review = True
        doc.trace.extractor = "bank_spreadsheet_columns_missing"
        return doc

    compiled_rules = bank_xls._compile_rules(bank_spec.get("rules") or [])
    exclusions = bank_spec.get("exclusions") or []

    totals = build_totals_template()
    unknown: Dict[str, int] = {}
    items: List[MovementItem] = []
    analyzed_rows = 0
    matched_rows = 0

    for row in data[header_row_idx + 1 :]:
        if idx_date >= len(row) or idx_desc >= len(row):
            continue
        date_s = bank_xls._to_date_ddmmyyyy(row[idx_date])
        desc = ("" if row[idx_desc] is None else str(row[idx_desc])).strip()
        if not date_s or not desc:
            continue
        analyzed_rows += 1
        if bank_xls._is_excluded(desc, exclusions):
            continue

        raw_category = match_spreadsheet_category(desc, compiled_rules)
        if not raw_category:
            unknown[desc] = unknown.get(desc, 0) + 1
            continue
        category = normalize_category(raw_category)
        if category not in totals:
            category = "OTROS"

        amount = 0.0
        if idx_amount is not None and idx_amount < len(row):
            amount = bank_xls._parse_ar_number(row[idx_amount])
        else:
            debit = bank_xls._parse_ar_number(row[idx_debit]) if idx_debit is not None and idx_debit < len(row) else 0.0
            credit = bank_xls._parse_ar_number(row[idx_credit]) if idx_credit is not None and idx_credit < len(row) else 0.0
            amount = -abs(debit) + abs(credit)
        if abs(amount) < 0.005:
            continue

        totals[category] = float(totals.get(category, 0.0)) + float(amount)
        items.append(
            MovementItem(
                date=date_s,
                description=desc,
                raw_amount=round(abs(float(amount)), 2),
                signed_amount=round(float(amount), 2),
                direction=_detect_direction(amount),
                channel="bank",
                category=category,
                confidence=0.98,
                source_section="spreadsheet_movements",
            )
        )
        matched_rows += 1

    subtotal = sum(float(value) for key, value in totals.items() if key != "BANCO")
    totals["BANCO"] = -subtotal

    bank_name = bank_spec.get("bank_name") or bank_key
    doc.document_type = "bank_statement"
    doc.institution = bank_name
    doc.period = bank_xls._infer_period_end_date(file_path.name)
    doc.items = items
    doc.totals = totals
    doc.trace = TracePayload(
        extractor=f"bank_spreadsheet_{bank_key.lower()}_v1",
        used_ai=False,
        notes=[
            f"sheet={excel.get('sheet_name')}",
            f"rows_analyzed={analyzed_rows}",
            f"rows_matched={matched_rows}",
            f"rows_unclassified={sum(unknown.values())}",
        ],
    )
    doc.summary.short_text = f"{bank_name}, extracto bancario con {matched_rows} movimientos clasificados"
    if unknown:
        top_unknown = sorted(unknown.items(), key=lambda it: it[1], reverse=True)[:5]
        doc.summary.warnings.append(
            "Conceptos no clasificados: " + "; ".join(f"{desc} ({count})" for desc, count in top_unknown)
        )
        doc.summary.needs_review = True
    apply_iva_net_adjustment(doc)
    doc.proposed_entry = _build_proposed_entry_from_totals(doc.totals)
    return doc


def enrich_document(file_path: Path) -> UnifiedDocument:
    doc = build_blueprint(file_path)
    if doc.source_kind == "spreadsheet":
        return populate_from_bank_spreadsheet(doc, file_path)
    if doc.source_kind == "pdf" and doc.document_type == "card_settlement":
        return populate_from_card_pdf(doc, file_path)
    if doc.source_kind == "pdf" and doc.document_type in {"bank_statement", "mixed_financial"}:
        return populate_from_bank_pdf(doc, file_path)
    return doc


def document_to_dict(doc: UnifiedDocument) -> Dict[str, Any]:
    data = asdict(doc)
    data["items"] = [asdict(item) for item in doc.items]
    data["proposed_entry"] = [asdict(item) for item in doc.proposed_entry]
    data["summary"] = asdict(doc.summary)
    data["trace"] = asdict(doc.trace)
    return data


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Blueprint del nuevo flujo unificado para movimientos financieros.",
    )
    parser.add_argument("files", nargs="+", help="Archivos de entrada a inspeccionar")
    parser.add_argument("--outdir", default="", help="Carpeta para guardar JSONs. Si se omite, imprime por stdout.")
    parser.add_argument("--gui", action="store_true", help="Muestra ventana de progreso y mensajes.")
    args = parser.parse_args()

    ui: Optional[StatusUI] = None
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

    result: Dict[str, Any] = {"docs": [], "written": [], "error": None, "resolved_outdir": None}

    def worker() -> None:
        try:
            docs: List[Dict[str, Any]] = []
            written: List[Dict[str, Path]] = []
            resolved_outdir: Optional[Path] = None
            if args.outdir:
                status("Preparando carpeta de salida...")
                resolved_outdir = _resolve_output_dir(args.outdir, ui=ui)
                result["resolved_outdir"] = resolved_outdir
                log(f"Salida: {resolved_outdir}")

            for idx, raw_path in enumerate(args.files, start=1):
                file_path = Path(raw_path)
                status(f"Analizando archivo {idx}/{len(args.files)}...")
                log(f"Archivo: {file_path}")
                if not file_path.exists():
                    raise SystemExit(f"ERROR: No existe el archivo: {raw_path}")

                doc = enrich_document(file_path)
                docs.append(document_to_dict(doc))
                log(
                    f"Detectado: tipo={doc.document_type} banco={doc.institution or '-'} "
                    f"tarjeta={doc.card_brand or '-'} periodo={doc.period or '-'}"
                )
                if resolved_outdir is not None:
                    status(f"Guardando salida {idx}/{len(args.files)}...")
                    written.append(write_output_files(doc, resolved_outdir, file_path))

            result["docs"] = docs
            result["written"] = written
        except BaseException as e:
            result["error"] = str(e)

        if ui:
            if result["error"]:
                ui.push(result["error"])
                ui.freeze("Error")
            else:
                ui.finish("Listo", keep_open_seconds=1.0)

    if ui:
        t = threading.Thread(target=worker, daemon=True)
        t.start()
        ui.mainloop()
    else:
        worker()

    if result["error"]:
        raise SystemExit(result["error"])

    docs = result["docs"]
    written = result["written"]

    if args.outdir:
        if len(written) == 1:
            print(str(written[0]["txt_path"]))
        else:
            print(str(result["resolved_outdir"] or _normalize_outdir_arg(args.outdir)))
        return

    if len(docs) == 1:
        print(json.dumps(docs[0], ensure_ascii=False, indent=2))
    else:
        print(json.dumps(docs, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
