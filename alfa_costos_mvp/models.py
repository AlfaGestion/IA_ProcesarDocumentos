from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from enum import Enum
from pathlib import Path
from typing import Optional


class SourceKind(str, Enum):
    EXCEL = "excel"
    CSV = "csv"
    TXT = "txt"
    PDF = "pdf"
    IMAGE = "image"


class MatchType(str, Enum):
    PROVIDER_CODE_EXACT = "provider_code_exact"
    DESCRIPTION_FUZZY = "description_fuzzy"
    MANUAL = "manual"
    NONE = "none"


class LinkStatus(str, Enum):
    PENDING = "pending"
    CONFIRMED = "confirmed"
    REJECTED = "rejected"
    APPLIED = "applied"
    ERROR = "error"


@dataclass(slots=True)
class ImportFile:
    path: Path
    source_kind: SourceKind
    sheet_name: Optional[str] = None
    imported_at: datetime = field(default_factory=datetime.now)


@dataclass(slots=True)
class ImportedRow:
    row_number: int
    provider_code: str = ""
    description: str = ""
    cost_price: Optional[Decimal] = None
    raw_code: str = ""
    raw_description: str = ""
    raw_price: str = ""
    selected_price_column: Optional[str] = None
    source_sheet: Optional[str] = None
    raw_values: dict[str, str] = field(default_factory=dict)


@dataclass(slots=True)
class MasterArticle:
    article_id: str
    article_code: str
    description: str
    current_cost: Optional[Decimal]
    provider_code: str = ""
    provider_id: Optional[str] = None
    active: bool = True


@dataclass(slots=True)
class MatchCandidate:
    imported_row_number: int
    article: MasterArticle
    match_type: MatchType
    score: float
    provider_code_hit: bool = False
    description_score: float = 0.0
    price_support_score: float = 0.0
    notes: str = ""


@dataclass(slots=True)
class ReviewDecision:
    imported_row_number: int
    selected_article_id: Optional[str]
    status: LinkStatus
    match_type: MatchType
    operator_note: str = ""


@dataclass(slots=True)
class CostUpdateRequest:
    imported_row: ImportedRow
    article: MasterArticle
    previous_cost: Optional[Decimal]
    new_cost: Decimal
    match_type: MatchType
    source_file: str
    operator_user: str
    warning_flag: bool = False
    warning_message: str = ""


@dataclass(slots=True)
class ImportProfile:
    id: int
    provider_name: str
    provider_account: str
    price_policy: str = ""
    list_code: str = ""
    sheet_name: str = ""
    range_from: str = ""
    range_to: str = ""
    key_fields: str = ""
    notes: str = ""
    only_add: bool = False
    only_modify: bool = False


@dataclass(slots=True)
class ImportBatch:
    id: int
    profile_id: Optional[int]
    provider_name: str
    provider_account: str
    source_file: str
    source_name: str
    status: str
    user_name: str


@dataclass(slots=True)
class BatchDetailRecord:
    batch_id: int
    row_number: int
    provider_code: str
    description: str
    cost_price: Optional[Decimal]
    article_id: str = ""
    article_description: str = ""
    current_cost: Optional[Decimal] = None
    new_cost: Optional[Decimal] = None
    match_type: str = ""
    match_score: float = 0.0
    alert: str = ""
    decision: str = ""
    applied_result: str = ""
    apply_error: str = ""
    detail_id: int = 0


@dataclass(slots=True)
class HistoryRecord:
    import_batch_id: Optional[int]
    timestamp_text: str
    user_name: str
    provider_name: str
    source_file: str
    row_number: int
    article_id: str
    imported_description: str
    previous_cost: Optional[Decimal]
    new_cost: Optional[Decimal]
    match_type: str
    match_score: float
    alert_text: str = ""
