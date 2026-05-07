from __future__ import annotations

from PySide6.QtWidgets import (
    QCheckBox,
    QDialog,
    QDialogButtonBox,
    QFormLayout,
    QLineEdit,
    QMessageBox,
    QTextEdit,
    QVBoxLayout,
)

from alfa_costos_mvp.models import ImportProfile


class ProfileDialog(QDialog):
    def __init__(self, profile: ImportProfile | None = None, parent=None) -> None:
        super().__init__(parent)
        self._original = profile
        self.setWindowTitle("Perfil de proveedor")
        self.resize(520, 520)
        self._build_ui(profile)

    def _build_ui(self, profile: ImportProfile | None) -> None:
        layout = QVBoxLayout(self)
        form = QFormLayout()

        self.provider_edit = QLineEdit(profile.provider_name if profile else "")
        self.account_edit = QLineEdit(profile.provider_account if profile else "")
        self.policy_edit = QLineEdit(profile.price_policy if profile else "")
        self.list_edit = QLineEdit(profile.list_code if profile else "")
        self.sheet_edit = QLineEdit(profile.sheet_name if profile else "")
        self.range_from_edit = QLineEdit(profile.range_from if profile else "")
        self.range_to_edit = QLineEdit(profile.range_to if profile else "")
        self.key_fields_edit = QLineEdit(profile.key_fields if profile else "")
        self.notes_edit = QTextEdit(profile.notes if profile else "")
        self.only_add_check = QCheckBox("Solo altas")
        self.only_add_check.setChecked(profile.only_add if profile else False)
        self.only_modify_check = QCheckBox("Solo modificacion")
        self.only_modify_check.setChecked(profile.only_modify if profile else False)

        form.addRow("Proveedor", self.provider_edit)
        form.addRow("Cuenta proveedor", self.account_edit)
        form.addRow("Politica precios", self.policy_edit)
        form.addRow("Lista", self.list_edit)
        form.addRow("Hoja", self.sheet_edit)
        form.addRow("Rango desde", self.range_from_edit)
        form.addRow("Rango hasta", self.range_to_edit)
        form.addRow("Campos clave", self.key_fields_edit)
        form.addRow("Regla", self.only_add_check)
        form.addRow("", self.only_modify_check)
        form.addRow("Notas", self.notes_edit)
        layout.addLayout(form)

        buttons = QDialogButtonBox(QDialogButtonBox.Save | QDialogButtonBox.Cancel, self)
        buttons.accepted.connect(self._validate_and_accept)
        buttons.rejected.connect(self.reject)
        layout.addWidget(buttons)

    def profile_data(self) -> ImportProfile:
        base_id = self._original.id if self._original else 0
        return ImportProfile(
            id=base_id,
            provider_name=self.provider_edit.text().strip(),
            provider_account=self.account_edit.text().strip(),
            price_policy=self.policy_edit.text().strip(),
            list_code=self.list_edit.text().strip(),
            sheet_name=self.sheet_edit.text().strip(),
            range_from=self.range_from_edit.text().strip(),
            range_to=self.range_to_edit.text().strip(),
            key_fields=self.key_fields_edit.text().strip(),
            notes=self.notes_edit.toPlainText().strip(),
            only_add=self.only_add_check.isChecked(),
            only_modify=self.only_modify_check.isChecked(),
        )

    def _validate_and_accept(self) -> None:
        profile = self.profile_data()
        if not profile.provider_name:
            QMessageBox.warning(self, "Perfil", "El nombre del proveedor es obligatorio.")
            return
        if not profile.provider_account:
            QMessageBox.warning(self, "Perfil", "La cuenta de proveedor es obligatoria.")
            return
        if profile.only_add and profile.only_modify:
            QMessageBox.warning(
                self,
                "Perfil",
                "No conviene marcar SoloAlta y SoloModificacion al mismo tiempo.",
            )
            return
        self.accept()
