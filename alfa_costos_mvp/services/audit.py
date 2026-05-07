from __future__ import annotations

from alfa_costos_mvp.models import CostUpdateRequest


class AuditService:
    """
    Stub de auditoria.
    Debe registrar:
    - archivo origen
    - operador
    - costo anterior / nuevo
    - metodo de match
    - score
    - fecha hora
    """

    def register_update(self, request: CostUpdateRequest) -> None:
        _ = request

