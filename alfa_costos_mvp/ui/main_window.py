from __future__ import annotations

import getpass
from pathlib import Path

from PySide6.QtCore import Qt
from PySide6.QtWidgets import (
    QComboBox,
    QDialog,
    QFileDialog,
    QFormLayout,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMainWindow,
    QMessageBox,
    QPushButton,
    QSplitter,
    QTableWidget,
    QTableWidgetItem,
    QToolBar,
    QVBoxLayout,
    QWidget,
)

from alfa_costos_mvp.config import AppConfig
from alfa_costos_mvp.models import BatchDetailRecord, ImportBatch, ImportedRow, ImportProfile
from alfa_costos_mvp.services.cost_updates import CostUpdateGuard
from alfa_costos_mvp.services.importers import StructuredFileImporter, detect_source_kind
from alfa_costos_mvp.services.matching import MatchEngine
from alfa_costos_mvp.services.repository import SqlServerRepository
from alfa_costos_mvp.ui.connection_dialog import ConnectionDialog
from alfa_costos_mvp.ui.history_dialog import HistoryDialog
from alfa_costos_mvp.ui.profile_dialog import ProfileDialog


class MainWindow(QMainWindow):
    def __init__(self, *, config: AppConfig, config_path: Path) -> None:
        super().__init__()
        self.config = config
        self.config_path = config_path
        self.repo: SqlServerRepository | None = None
        self.current_batch: ImportBatch | None = None
        self.import_profiles: list[ImportProfile] = []
        self.imported_rows: list[ImportedRow] = []
        self.match_records: list[BatchDetailRecord] = []
        self.match_candidates_by_row: dict[int, list] = {}
        self.importer = StructuredFileImporter()
        self.match_engine = MatchEngine()
        self.guard = CostUpdateGuard(config)
        self.setWindowTitle("Alfa Gestion - Actualizacion de Costos")
        self.resize(1400, 820)
        self._build_ui()
        self._ensure_connection()

    def _build_ui(self) -> None:
        toolbar = QToolBar("Principal", self)
        self.addToolBar(toolbar)

        btn_connection = QPushButton("Conexion SQL")
        btn_connection.clicked.connect(self._ensure_connection)
        toolbar.addWidget(btn_connection)

        btn_refresh_profiles = QPushButton("Cargar perfiles")
        btn_refresh_profiles.clicked.connect(self._load_profiles)
        toolbar.addWidget(btn_refresh_profiles)
        btn_new_profile = QPushButton("Nuevo perfil")
        btn_new_profile.clicked.connect(self._create_profile)
        toolbar.addWidget(btn_new_profile)
        btn_edit_profile = QPushButton("Editar perfil")
        btn_edit_profile.clicked.connect(self._edit_profile)
        toolbar.addWidget(btn_edit_profile)
        btn_delete_profile = QPushButton("Baja perfil")
        btn_delete_profile.clicked.connect(self._delete_profile)
        toolbar.addWidget(btn_delete_profile)

        toolbar.addSeparator()
        btn_import = QPushButton("Importar archivo")
        btn_import.clicked.connect(self._import_file)
        toolbar.addWidget(btn_import)

        toolbar.addSeparator()
        btn_matching = QPushButton("Procesar matching")
        btn_matching.clicked.connect(self._process_matching)
        toolbar.addWidget(btn_matching)
        btn_confirm = QPushButton("Confirmar seleccionados")
        btn_confirm.clicked.connect(self._confirm_selected_rows)
        toolbar.addWidget(btn_confirm)
        btn_discard = QPushButton("Descartar seleccionados")
        btn_discard.clicked.connect(self._discard_selected_rows)
        toolbar.addWidget(btn_discard)
        btn_apply = QPushButton("Aplicar actualizaciones")
        btn_apply.clicked.connect(self._apply_confirmed_rows)
        toolbar.addWidget(btn_apply)
        btn_history = QPushButton("Ver historial")
        btn_history.clicked.connect(self._show_history)
        toolbar.addWidget(btn_history)

        central = QWidget(self)
        root = QVBoxLayout(central)

        setup_form = QFormLayout()
        self.connection_state = QLineEdit()
        self.connection_state.setReadOnly(True)
        self.profile_combo = QComboBox()
        self.profile_combo.currentIndexChanged.connect(self._profile_changed)
        self.provider_edit = QLineEdit()
        self.provider_edit.setReadOnly(True)
        self.account_edit = QLineEdit()
        self.account_edit.setReadOnly(True)
        self.rule_flags_edit = QLineEdit()
        self.rule_flags_edit.setReadOnly(True)
        self.batch_edit = QLineEdit()
        self.batch_edit.setReadOnly(True)

        setup_form.addRow("Conexion", self.connection_state)
        setup_form.addRow("Perfil proveedor", self.profile_combo)
        setup_form.addRow("Proveedor", self.provider_edit)
        setup_form.addRow("Cuenta proveedor", self.account_edit)
        setup_form.addRow("Reglas", self.rule_flags_edit)
        setup_form.addRow("Corrida", self.batch_edit)
        root.addLayout(setup_form)

        filter_bar = QHBoxLayout()
        filter_bar.addWidget(QLabel("Archivo origen:"))
        self.file_edit = QLineEdit()
        self.file_edit.setReadOnly(True)
        filter_bar.addWidget(self.file_edit, 1)
        filter_bar.addWidget(QLabel("Estado:"))
        self.state_edit = QLineEdit("Sin iniciar")
        self.state_edit.setReadOnly(True)
        filter_bar.addWidget(self.state_edit)
        root.addLayout(filter_bar)

        splitter = QSplitter(Qt.Horizontal, self)
        splitter.addWidget(self._build_imported_panel())
        splitter.addWidget(self._build_candidates_panel())
        splitter.setSizes([700, 700])
        root.addWidget(splitter, 1)

        self.statusBar().showMessage("Listo para importar.")
        self.setCentralWidget(central)

    def _build_imported_panel(self) -> QWidget:
        widget = QWidget(self)
        layout = QVBoxLayout(widget)
        layout.addWidget(QLabel("Datos importados"))

        self.imported_table = QTableWidget(0, 8, self)
        self.imported_table.setHorizontalHeaderLabels(
            [
                "Fila",
                "Cod. proveedor",
                "Descripcion",
                "Costo",
                "Match",
                "Score",
                "Decision",
                "Alerta",
            ]
        )
        self.imported_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.imported_table.setEditTriggers(QTableWidget.NoEditTriggers)
        self.imported_table.itemSelectionChanged.connect(self._display_selected_candidates)
        layout.addWidget(self.imported_table, 1)
        return widget

    def _build_candidates_panel(self) -> QWidget:
        widget = QWidget(self)
        layout = QVBoxLayout(widget)
        layout.addWidget(QLabel("Candidatos del maestro"))

        self.candidates_table = QTableWidget(0, 7, self)
        self.candidates_table.setHorizontalHeaderLabels(
            [
                "Articulo",
                "Codigo",
                "Descripcion",
                "Costo actual",
                "Tipo match",
                "Score",
                "Notas",
            ]
        )
        self.candidates_table.setSelectionBehavior(QTableWidget.SelectRows)
        self.candidates_table.setEditTriggers(QTableWidget.NoEditTriggers)
        layout.addWidget(self.candidates_table, 1)
        return widget

    def _import_file(self) -> None:
        profile = self._selected_profile()
        if self.repo is None:
            QMessageBox.warning(self, "Conexion", "Primero conecta la aplicacion a SQL Server.")
            return
        if profile is None:
            QMessageBox.warning(self, "Perfil", "Selecciona primero un perfil de V_Ta_InterODBC.")
            return
        path, _ = QFileDialog.getOpenFileName(
            self,
            "Seleccionar archivo",
            "",
            "Archivos soportados (*.xlsx *.xls *.csv *.txt *.pdf *.jpg *.jpeg *.png *.webp *.bmp)",
        )
        if not path:
            return

        try:
            source_kind = detect_source_kind(Path(path)).value
            batch = self.repo.create_import_batch(
                profile=profile,
                source_file=path,
                user_name=getpass.getuser(),
                source_kind=source_kind,
            )
            import_rows = self.importer.read(
                import_file=self._build_import_file(path, profile.sheet_name or None)
            )
            self.imported_rows = list(import_rows)
            self.repo.insert_import_details(batch.id, self.imported_rows)
        except Exception as exc:
            QMessageBox.critical(self, "Importacion", f"No se pudo leer o registrar el archivo.\n\n{exc}")
            return

        self.current_batch = batch
        self.match_records = []
        self.match_candidates_by_row = {}
        self.file_edit.setText(path)
        self.batch_edit.setText(str(batch.id))
        self.state_edit.setText("IMPORTADA")
        self._fill_imported_table()
        self._clear_candidates_table()
        self.statusBar().showMessage(
            f"Importacion #{batch.id} creada con {len(self.imported_rows)} filas estructuradas."
        )
        QMessageBox.information(
            self,
            "Corrida creada",
            f"Se creo la importacion #{batch.id} para {batch.provider_name} con {len(self.imported_rows)} filas.",
        )

    def _ensure_connection(self) -> None:
        dialog = ConnectionDialog(self.config, self)
        if dialog.exec() != QDialog.Accepted:
            self._refresh_connection_state(connected=False)
            return

        self.config = dialog.selected_config()
        if dialog.should_save():
            self.config.to_file(self.config_path)

        self.repo = SqlServerRepository(self.config)
        self.guard = CostUpdateGuard(self.config)
        self._refresh_connection_state(connected=True)
        self._load_profiles()

    def _refresh_connection_state(self, *, connected: bool) -> None:
        if connected:
            text = f"{self.config.sql_server} / {self.config.sql_database}"
        else:
            text = "Sin conexion"
        self.connection_state.setText(text)

    def _load_profiles(self) -> None:
        if self.repo is None:
            return
        selected_id = self.profile_combo.currentData()
        try:
            profiles = self.repo.list_import_profiles()
        except Exception as exc:
            QMessageBox.critical(self, "Perfiles", f"No se pudieron cargar perfiles.\n\n{exc}")
            return

        self.import_profiles = profiles
        self.profile_combo.blockSignals(True)
        self.profile_combo.clear()
        self.profile_combo.addItem("Seleccionar perfil...", None)
        for profile in profiles:
            label = f"{profile.provider_name} [{profile.provider_account}]"
            self.profile_combo.addItem(label, profile.id)
        if selected_id is not None:
            for idx in range(self.profile_combo.count()):
                if self.profile_combo.itemData(idx) == selected_id:
                    self.profile_combo.setCurrentIndex(idx)
                    break
        self.profile_combo.blockSignals(False)
        self._profile_changed()

    def _profile_changed(self) -> None:
        profile = self._selected_profile()
        if profile is None:
            self.provider_edit.clear()
            self.account_edit.clear()
            self.rule_flags_edit.clear()
            return
        self.provider_edit.setText(profile.provider_name)
        self.account_edit.setText(profile.provider_account)
        flags: list[str] = []
        if profile.only_add:
            flags.append("SoloAlta")
        if profile.only_modify:
            flags.append("SoloModificacion")
        if profile.sheet_name:
            flags.append(f"Hoja={profile.sheet_name}")
        self.rule_flags_edit.setText(" | ".join(flags) if flags else "Sin reglas especiales")

    def _selected_profile(self) -> ImportProfile | None:
        profile_id = self.profile_combo.currentData()
        if profile_id is None:
            return None
        for profile in self.import_profiles:
            if profile.id == profile_id:
                return profile
        return None

    def _process_matching(self) -> None:
        profile = self._selected_profile()
        if self.repo is None or self.current_batch is None:
            QMessageBox.warning(self, "Matching", "Primero crea una corrida importando un archivo.")
            return
        if profile is None:
            QMessageBox.warning(self, "Matching", "No hay perfil seleccionado.")
            return
        if not self.imported_rows:
            QMessageBox.warning(self, "Matching", "No hay filas importadas para procesar.")
            return

        try:
            provider_codes = [row.provider_code for row in self.imported_rows if row.provider_code]
            articles = self.repo.find_master_articles(
                provider_account=profile.provider_account,
                provider_codes=provider_codes,
            )
        except Exception as exc:
            QMessageBox.critical(self, "Matching", f"No se pudo consultar V_MA_ARTICULOS.\n\n{exc}")
            return

        self.match_records = []
        self.match_candidates_by_row = {}
        for row in self.imported_rows:
            candidates = self.match_engine.build_candidates(row, articles)
            self.match_candidates_by_row[row.row_number] = candidates
            best = candidates[0] if candidates else None

            alert = ""
            current_cost = best.article.current_cost if best else None
            new_cost = row.cost_price
            if best and new_cost is not None:
                warning_flag, warning_message = self.guard.evaluate_variation(current_cost, new_cost)
                if warning_flag:
                    alert = warning_message

            self.match_records.append(
                BatchDetailRecord(
                    batch_id=self.current_batch.id,
                    row_number=row.row_number,
                    provider_code=row.provider_code,
                    description=row.description,
                    cost_price=row.cost_price,
                    article_id=best.article.article_id if best else "",
                    article_description=best.article.description if best else "",
                    current_cost=current_cost,
                    new_cost=new_cost,
                    match_type=best.match_type.value if best else "",
                    match_score=best.score if best else 0.0,
                    alert=alert,
                )
            )

        try:
            self.repo.update_detail_matches(self.current_batch.id, self.match_records)
        except Exception as exc:
            QMessageBox.critical(self, "Matching", f"No se pudieron guardar los resultados.\n\n{exc}")
            return

        self.state_edit.setText("EN_REVISION")
        self._fill_imported_table()
        self._display_selected_candidates()
        self.statusBar().showMessage("Matching inicial completado.")

    def _confirm_selected_rows(self) -> None:
        profile = self._selected_profile()
        if self.repo is None or self.current_batch is None:
            QMessageBox.warning(self, "Confirmacion", "Primero importa y procesa una corrida.")
            return
        if profile is None:
            QMessageBox.warning(self, "Confirmacion", "No hay perfil seleccionado.")
            return
        if profile.only_add:
            QMessageBox.information(
                self,
                "Confirmacion",
                "El perfil esta marcado como SoloAlta y este MVP no implementa altas de articulos. "
                "La corrida queda solo para revision.",
            )
            return

        selected_rows = self._selected_imported_row_indexes()
        if not selected_rows:
            QMessageBox.warning(self, "Confirmacion", "Selecciona una o mas filas importadas.")
            return

        decisions: list[BatchDetailRecord] = []
        candidate_row = self.candidates_table.currentRow()
        for imported_index in selected_rows:
            imported = self.imported_rows[imported_index]
            record = self._find_match_record(imported.row_number)
            if record is None:
                continue

            chosen = record
            candidates = self.match_candidates_by_row.get(imported.row_number, [])
            if len(selected_rows) == 1 and 0 <= candidate_row < len(candidates):
                candidate = candidates[candidate_row]
                chosen = BatchDetailRecord(
                    batch_id=record.batch_id,
                    row_number=record.row_number,
                    provider_code=record.provider_code,
                    description=record.description,
                    cost_price=record.cost_price,
                    article_id=candidate.article.article_id,
                    article_description=candidate.article.description,
                    current_cost=candidate.article.current_cost,
                    new_cost=record.new_cost,
                    match_type="manual",
                    match_score=candidate.score,
                    alert=record.alert,
                    detail_id=record.detail_id,
                )

            if not chosen.article_id:
                QMessageBox.warning(
                    self,
                    "Confirmacion",
                    f"La fila {imported.row_number} no tiene articulo vinculado. "
                    "Las altas no estan implementadas en este MVP.",
                )
                continue

            chosen.decision = "CONFIRMAR"
            decisions.append(chosen)
            self._replace_match_record(chosen)

        if not decisions:
            return
        try:
            self.repo.set_detail_decisions(
                batch_id=self.current_batch.id,
                decisions=decisions,
                user_name=getpass.getuser(),
            )
        except Exception as exc:
            QMessageBox.critical(self, "Confirmacion", f"No se pudieron guardar las decisiones.\n\n{exc}")
            return

        self.state_edit.setText("LISTA_PARA_APLICAR")
        self._fill_imported_table()
        self.statusBar().showMessage(f"Se confirmaron {len(decisions)} filas.")

    def _discard_selected_rows(self) -> None:
        if self.repo is None or self.current_batch is None:
            QMessageBox.warning(self, "Descartar", "Primero importa y procesa una corrida.")
            return
        selected_rows = self._selected_imported_row_indexes()
        if not selected_rows:
            QMessageBox.warning(self, "Descartar", "Selecciona una o mas filas importadas.")
            return

        decisions: list[BatchDetailRecord] = []
        for imported_index in selected_rows:
            imported = self.imported_rows[imported_index]
            record = self._find_match_record(imported.row_number)
            if record is None:
                record = BatchDetailRecord(
                    batch_id=self.current_batch.id,
                    row_number=imported.row_number,
                    provider_code=imported.provider_code,
                    description=imported.description,
                    cost_price=imported.cost_price,
                )
            record.decision = "DESCARTAR"
            decisions.append(record)
            self._replace_match_record(record)

        try:
            self.repo.set_detail_decisions(
                batch_id=self.current_batch.id,
                decisions=decisions,
                user_name=getpass.getuser(),
            )
        except Exception as exc:
            QMessageBox.critical(self, "Descartar", f"No se pudieron guardar los descartes.\n\n{exc}")
            return

        self._fill_imported_table()
        self.statusBar().showMessage(f"Se descartaron {len(decisions)} filas.")

    def _apply_confirmed_rows(self) -> None:
        profile = self._selected_profile()
        if self.repo is None or self.current_batch is None:
            QMessageBox.warning(self, "Aplicar", "Primero importa y confirma filas.")
            return
        if profile is None:
            QMessageBox.warning(self, "Aplicar", "No hay perfil seleccionado.")
            return
        if profile.only_add:
            QMessageBox.information(
                self,
                "Aplicar",
                "El perfil esta configurado como SoloAlta y este MVP no implementa altas automáticas.",
            )
            return

        try:
            summary = self.repo.apply_confirmed_updates(
                batch_id=self.current_batch.id,
                user_name=getpass.getuser(),
            )
        except Exception as exc:
            QMessageBox.critical(self, "Aplicar", f"No se pudieron aplicar los cambios.\n\n{exc}")
            return

        if summary["updated"] > 0 and (summary["blocked"] > 0 or summary["errors"] > 0):
            self.state_edit.setText("APLICADA_PARCIAL")
        elif summary["updated"] > 0 or summary["same"] > 0:
            self.state_edit.setText("APLICADA")
        else:
            self.state_edit.setText("ERROR")
        self.statusBar().showMessage(
            "Aplicacion finalizada: "
            f"{summary['updated']} actualizados, {summary['same']} sin cambio, "
            f"{summary['blocked']} bloqueados, {summary['errors']} errores."
        )
        QMessageBox.information(
            self,
            "Aplicacion completada",
            f"Actualizados: {summary['updated']}\n"
            f"Sin cambio: {summary['same']}\n"
            f"Bloqueados/altas: {summary['blocked']}\n"
            f"Errores: {summary['errors']}",
        )
        try:
            result_map = self.repo.load_batch_detail_results(batch_id=self.current_batch.id)
        except Exception:
            result_map = {}
        for record in self.match_records:
            result, error = result_map.get(record.row_number, ("", ""))
            record.applied_result = result
            record.apply_error = error
        self._fill_imported_table()

    def _show_history(self) -> None:
        if self.repo is None:
            QMessageBox.warning(self, "Historial", "Primero conecta la aplicacion a SQL Server.")
            return
        try:
            items = self.repo.load_recent_history(limit=100)
        except Exception as exc:
            QMessageBox.critical(self, "Historial", f"No se pudo cargar el historial.\n\n{exc}")
            return
        dialog = HistoryDialog(items, self)
        dialog.exec()

    def _create_profile(self) -> None:
        if self.repo is None:
            QMessageBox.warning(self, "Perfiles", "Primero conecta la aplicacion a SQL Server.")
            return
        dialog = ProfileDialog(parent=self)
        if dialog.exec() != QDialog.Accepted:
            return
        try:
            profile = dialog.profile_data()
            new_id = self.repo.create_import_profile(profile)
        except Exception as exc:
            QMessageBox.critical(self, "Perfiles", f"No se pudo crear el perfil.\n\n{exc}")
            return
        self._load_profiles()
        self._select_profile_id(new_id)
        self.statusBar().showMessage(f"Perfil creado: {profile.provider_name}")

    def _edit_profile(self) -> None:
        if self.repo is None:
            QMessageBox.warning(self, "Perfiles", "Primero conecta la aplicacion a SQL Server.")
            return
        profile = self._selected_profile()
        if profile is None:
            QMessageBox.warning(self, "Perfiles", "Selecciona un perfil para editar.")
            return
        dialog = ProfileDialog(profile=profile, parent=self)
        if dialog.exec() != QDialog.Accepted:
            return
        try:
            updated = dialog.profile_data()
            self.repo.update_import_profile(updated)
        except Exception as exc:
            QMessageBox.critical(self, "Perfiles", f"No se pudo actualizar el perfil.\n\n{exc}")
            return
        self._load_profiles()
        self._select_profile_id(updated.id)
        self.statusBar().showMessage(f"Perfil actualizado: {updated.provider_name}")

    def _delete_profile(self) -> None:
        if self.repo is None:
            QMessageBox.warning(self, "Perfiles", "Primero conecta la aplicacion a SQL Server.")
            return
        profile = self._selected_profile()
        if profile is None:
            QMessageBox.warning(self, "Perfiles", "Selecciona un perfil para dar de baja.")
            return
        answer = QMessageBox.question(
            self,
            "Baja de perfil",
            "Se eliminara el perfil seleccionado de V_Ta_InterODBC.\n\n"
            f"Proveedor: {profile.provider_name}\n"
            f"Cuenta: {profile.provider_account}\n\n"
            "Queres continuar?",
        )
        if answer != QMessageBox.Yes:
            return
        try:
            self.repo.delete_import_profile(profile.id)
        except Exception as exc:
            QMessageBox.critical(self, "Perfiles", f"No se pudo eliminar el perfil.\n\n{exc}")
            return
        self._load_profiles()
        self.statusBar().showMessage(f"Perfil eliminado: {profile.provider_name}")

    def _fill_imported_table(self) -> None:
        row_map = {record.row_number: record for record in self.match_records}
        self.imported_table.setRowCount(len(self.imported_rows))
        for row_index, imported in enumerate(self.imported_rows):
            record = row_map.get(imported.row_number)
            decision = "pendiente"
            alert = record.alert if record else ""
            if record:
                if record.decision == "CONFIRMAR":
                    decision = "confirmado"
                elif record.decision == "DESCARTAR":
                    decision = "descartado"
                if record.apply_error:
                    alert = f"{alert} | {record.apply_error}".strip(" |")
                elif record.applied_result:
                    alert = f"{alert} | {record.applied_result}".strip(" |")
            values = [
                str(imported.row_number),
                imported.provider_code,
                imported.description,
                "" if imported.cost_price is None else f"{imported.cost_price:.2f}",
                record.match_type if record else "sin procesar",
                f"{record.match_score:.2f}" if record and record.match_score else "0",
                decision,
                alert,
            ]
            for col, value in enumerate(values):
                self.imported_table.setItem(row_index, col, QTableWidgetItem(value))

    def _display_selected_candidates(self) -> None:
        selected = self.imported_table.selectedItems()
        if not selected:
            self._clear_candidates_table()
            return
        row_index = selected[0].row()
        if row_index >= len(self.imported_rows):
            self._clear_candidates_table()
            return
        imported_row = self.imported_rows[row_index]
        candidates = self.match_candidates_by_row.get(imported_row.row_number, [])
        self.candidates_table.setRowCount(len(candidates))
        for idx, candidate in enumerate(candidates):
            values = [
                candidate.article.article_id,
                candidate.article.article_code,
                candidate.article.description,
                "" if candidate.article.current_cost is None else f"{candidate.article.current_cost:.2f}",
                candidate.match_type.value,
                f"{candidate.score:.2f}",
                f"desc={candidate.description_score:.2f} precio={candidate.price_support_score:.2f}",
            ]
            for col, value in enumerate(values):
                self.candidates_table.setItem(idx, col, QTableWidgetItem(value))

    def _clear_candidates_table(self) -> None:
        self.candidates_table.setRowCount(0)

    def _selected_imported_row_indexes(self) -> list[int]:
        indexes = sorted({item.row() for item in self.imported_table.selectedItems()})
        return indexes

    def _find_match_record(self, row_number: int) -> BatchDetailRecord | None:
        for record in self.match_records:
            if record.row_number == row_number:
                return record
        return None

    def _replace_match_record(self, new_record: BatchDetailRecord) -> None:
        for idx, record in enumerate(self.match_records):
            if record.row_number == new_record.row_number:
                self.match_records[idx] = new_record
                return
        self.match_records.append(new_record)

    def _select_profile_id(self, profile_id: int) -> None:
        for idx in range(self.profile_combo.count()):
            if self.profile_combo.itemData(idx) == profile_id:
                self.profile_combo.setCurrentIndex(idx)
                self._profile_changed()
                return

    @staticmethod
    def _build_import_file(path: str, sheet_name: str | None):
        from alfa_costos_mvp.models import ImportFile

        file_path = Path(path)
        return ImportFile(
            path=file_path,
            source_kind=detect_source_kind(file_path),
            sheet_name=sheet_name,
        )
