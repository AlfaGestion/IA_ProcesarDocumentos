from __future__ import annotations

from decimal import Decimal

from alfa_costos_mvp.config import AppConfig
from alfa_costos_mvp.models import CostUpdateRequest


class CostUpdateGuard:
    def __init__(self, config: AppConfig):
        self.config = config

    def evaluate_variation(self, previous_cost: Decimal | None, new_cost: Decimal) -> tuple[bool, str]:
        if previous_cost is None or previous_cost == 0:
            return False, ""

        variation_pct = ((new_cost - previous_cost) / previous_cost) * 100
        abs_variation = abs(float(variation_pct))
        if abs_variation >= self.config.max_variation_pct_block:
            return True, f"Variacion bloqueante: {variation_pct:.2f}%"
        if abs_variation >= self.config.max_variation_pct_warning:
            return True, f"Variacion a revisar: {variation_pct:.2f}%"
        return False, ""


class CostUpdateService:
    """
    En la implementacion real debe:
    - abrir transaccion
    - revalidar costo actual antes de escribir
    - grabar auditoria cabecera/detalle
    - aplicar update solo a filas confirmadas
    """

    def __init__(self, guard: CostUpdateGuard):
        self.guard = guard

    def prepare_request(self, request: CostUpdateRequest) -> CostUpdateRequest:
        warning_flag, warning_message = self.guard.evaluate_variation(
            request.previous_cost,
            request.new_cost,
        )
        request.warning_flag = warning_flag
        request.warning_message = warning_message
        return request

