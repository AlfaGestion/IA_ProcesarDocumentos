from __future__ import annotations

from decimal import Decimal, InvalidOperation
import re
import unicodedata


def normalize_text(value: str) -> str:
    text = unicodedata.normalize("NFKD", value or "")
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = text.upper().strip()
    text = re.sub(r"\s+", " ", text)
    return text


def normalize_provider_code(value: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", normalize_text(value))


def parse_decimal(value: str) -> Decimal | None:
    raw = (value or "").strip()
    if not raw:
        return None
    cleaned = raw.replace(".", "").replace(",", ".")
    cleaned = re.sub(r"[^0-9.\-]", "", cleaned)
    try:
        return Decimal(cleaned)
    except (InvalidOperation, ValueError):
        return None

