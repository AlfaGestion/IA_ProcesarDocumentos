from __future__ import annotations

from PySide6.QtWidgets import QDialog, QTableWidget, QTableWidgetItem, QVBoxLayout

from alfa_costos_mvp.models import HistoryRecord


class HistoryDialog(QDialog):
    def __init__(self, items: list[HistoryRecord], parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Historial de actualizaciones")
        self.resize(1200, 600)

        layout = QVBoxLayout(self)
        table = QTableWidget(len(items), 10, self)
        table.setHorizontalHeaderLabels(
            [
                "FechaHora",
                "Usuario",
                "Proveedor",
                "Archivo",
                "Fila",
                "Articulo",
                "Descripcion",
                "Costo ant.",
                "Costo nuevo",
                "Match",
            ]
        )
        for row_idx, item in enumerate(items):
            values = [
                item.timestamp_text,
                item.user_name,
                item.provider_name,
                item.source_file,
                str(item.row_number),
                item.article_id,
                item.imported_description,
                "" if item.previous_cost is None else f"{item.previous_cost:.2f}",
                "" if item.new_cost is None else f"{item.new_cost:.2f}",
                f"{item.match_type} ({item.match_score:.2f})" if item.match_type else "",
            ]
            for col, value in enumerate(values):
                table.setItem(row_idx, col, QTableWidgetItem(value))
        layout.addWidget(table)
