from __future__ import annotations

from decimal import Decimal
from typing import Iterable

from rapidfuzz import fuzz

from alfa_costos_mvp.models import ImportedRow, MasterArticle, MatchCandidate, MatchType
from alfa_costos_mvp.services.normalizers import normalize_provider_code, normalize_text


class MatchEngine:
    """
    Orden obligatorio:
    1. codigo proveedor exacto
    2. descripcion similar
    3. precio como refuerzo
    """

    def build_candidates(
        self,
        imported_row: ImportedRow,
        master_articles: Iterable[MasterArticle],
    ) -> list[MatchCandidate]:
        provider_code = normalize_provider_code(imported_row.provider_code)
        description = normalize_text(imported_row.description)
        candidates: list[MatchCandidate] = []

        for article in master_articles:
            score = 0.0
            match_type = MatchType.NONE
            provider_hit = False
            description_score = 0.0
            price_support = 0.0

            if provider_code and provider_code == normalize_provider_code(article.provider_code):
                provider_hit = True
                score += 100.0
                match_type = MatchType.PROVIDER_CODE_EXACT

            if description:
                description_score = float(
                    fuzz.token_sort_ratio(description, normalize_text(article.description))
                )
                if not provider_hit and description_score >= 70:
                    score = description_score
                    match_type = MatchType.DESCRIPTION_FUZZY
                elif provider_hit:
                    score += description_score * 0.15

            if imported_row.cost_price is not None and article.current_cost is not None:
                price_support = self._price_support(imported_row.cost_price, article.current_cost)
                if match_type != MatchType.NONE:
                    score += price_support * 0.10

            if match_type != MatchType.NONE:
                candidates.append(
                    MatchCandidate(
                        imported_row_number=imported_row.row_number,
                        article=article,
                        match_type=match_type,
                        score=round(score, 2),
                        provider_code_hit=provider_hit,
                        description_score=round(description_score, 2),
                        price_support_score=round(price_support, 2),
                    )
                )

        candidates.sort(key=lambda item: item.score, reverse=True)
        return candidates[:20]

    @staticmethod
    def _price_support(imported_cost: Decimal, current_cost: Decimal) -> float:
        if current_cost == 0:
            return 0.0
        diff_pct = abs((imported_cost - current_cost) / current_cost) * 100
        if diff_pct <= 5:
            return 100.0
        if diff_pct <= 15:
            return 70.0
        if diff_pct <= 30:
            return 40.0
        return 0.0

