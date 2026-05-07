from __future__ import annotations

from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLineEdit,
    QMessageBox,
    QVBoxLayout,
)

from alfa_costos_mvp.config import AppConfig
from alfa_costos_mvp.services.repository import SqlServerRepository


class ConnectionDialog(QDialog):
    def __init__(self, config: AppConfig, parent=None) -> None:
        super().__init__(parent)
        self.setWindowTitle("Conexion SQL Server")
        self._result_config = config
        self._build_ui(config)

    def _build_ui(self, config: AppConfig) -> None:
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.server_edit = QLineEdit(config.sql_server)
        self.database_edit = QLineEdit(config.sql_database)
        self.user_edit = QLineEdit(config.sql_user)
        self.password_edit = QLineEdit(config.sql_password)
        self.password_edit.setEchoMode(QLineEdit.Password)
        self.driver_edit = QLineEdit(config.sql_driver)
        self.save_check = QCheckBox("Guardar esta conexion localmente")
        self.save_check.setChecked(True)

        form.addRow("Servidor", self.server_edit)
        form.addRow("Base", self.database_edit)
        form.addRow("Usuario", self.user_edit)
        form.addRow("Clave", self.password_edit)
        form.addRow("Driver ODBC", self.driver_edit)
        layout.addLayout(form)
        layout.addWidget(self.save_check)

        buttons = QDialogButtonBox(
            QDialogButtonBox.Ok | QDialogButtonBox.Cancel,
            parent=self,
        )
        buttons.accepted.connect(self._accept_with_test)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def selected_config(self) -> AppConfig:
        return self._result_config

    def should_save(self) -> bool:
        return self.save_check.isChecked()

    def _accept_with_test(self) -> None:
        config = AppConfig(
            sql_server=self.server_edit.text().strip(),
            sql_database=self.database_edit.text().strip(),
            sql_user=self.user_edit.text().strip(),
            sql_password=self.password_edit.text(),
            sql_driver=self.driver_edit.text().strip() or "ODBC Driver 18 for SQL Server",
            ia_enabled=self._result_config.ia_enabled,
            ia_task_costos=self._result_config.ia_task_costos,
            max_variation_pct_warning=self._result_config.max_variation_pct_warning,
            max_variation_pct_block=self._result_config.max_variation_pct_block,
        )
        if not config.is_sql_configured():
            QMessageBox.warning(self, "Conexion", "Completa servidor, base, usuario, clave y driver.")
            return
        try:
            SqlServerRepository(config).test_connection()
        except Exception as exc:
            QMessageBox.critical(self, "Conexion", f"No se pudo conectar a SQL Server.\n\n{exc}")
            return
        self._result_config = config
        self.accept()
