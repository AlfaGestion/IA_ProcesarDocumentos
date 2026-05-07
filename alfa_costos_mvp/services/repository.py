from __future__ import annotations

from contextlib import contextmanager
import json
from pathlib import Path
from typing import Iterator
import hashlib
from decimal import Decimal

import pyodbc

from alfa_costos_mvp.config import AppConfig
from alfa_costos_mvp.models import (
    BatchDetailRecord,
    HistoryRecord,
    ImportBatch,
    ImportProfile,
    ImportedRow,
    MasterArticle,
)


class SqlServerRepository:
    """
    Repositorio base.
    Los nombres concretos de tablas y campos de articulos se dejan a validar.
    """

    def __init__(self, config: AppConfig):
        self.config = config

    def build_connection_string(self) -> str:
        return (
            f"DRIVER={{{self.config.sql_driver}}};"
            f"SERVER={self.config.sql_server};"
            f"DATABASE={self.config.sql_database};"
            f"UID={self.config.sql_user};"
            f"PWD={self.config.sql_password};"
            "TrustServerCertificate=yes;"
        )

    def test_connection(self) -> None:
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            cursor.fetchone()

    @contextmanager
    def connect(self) -> Iterator[pyodbc.Connection]:
        conn = pyodbc.connect(self.build_connection_string())
        try:
            yield conn
        finally:
            conn.close()

    def list_import_profiles(self) -> list[ImportProfile]:
        query = """
        SELECT
            Id,
            Proveedor,
            ISNULL(CuentaProveedor, ''),
            ISNULL(PoliticaPrecios, ''),
            ISNULL(LISTA, ''),
            ISNULL(Hoja, ''),
            ISNULL(RangoDesde, ''),
            ISNULL(RangoHasta, ''),
            ISNULL(CamposClave, ''),
            ISNULL(Notas, ''),
            ISNULL(SoloAlta, 0),
            ISNULL(SoloModificacion, 0)
        FROM dbo.V_Ta_InterODBC
        ORDER BY Proveedor
        """
        profiles: list[ImportProfile] = []
        with self.connect() as conn:
            cursor = conn.cursor()
            for row in cursor.execute(query).fetchall():
                profiles.append(
                    ImportProfile(
                        id=int(row[0]),
                        provider_name=str(row[1]).strip(),
                        provider_account=str(row[2]).strip(),
                        price_policy=str(row[3]).strip(),
                        list_code=str(row[4]).strip(),
                        sheet_name=str(row[5]).strip(),
                        range_from=str(row[6]).strip(),
                        range_to=str(row[7]).strip(),
                        key_fields=str(row[8]).strip(),
                        notes=str(row[9]).strip(),
                        only_add=bool(row[10]),
                        only_modify=bool(row[11]),
                    )
                )
        return profiles

    def create_import_profile(self, profile: ImportProfile) -> int:
        query = """
        INSERT INTO dbo.V_Ta_InterODBC
        (
            Proveedor,
            Odbc,
            CuentaProveedor,
            PoliticaPrecios,
            Hoja,
            LISTA,
            RangoDesde,
            RangoHasta,
            CamposClave,
            Notas,
            SoloAlta,
            SoloModificacion
        )
        OUTPUT INSERTED.Id
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        params = (
            profile.provider_name,
            "",
            profile.provider_account,
            profile.price_policy or None,
            profile.sheet_name or None,
            profile.list_code or None,
            profile.range_from or None,
            profile.range_to or None,
            profile.key_fields or None,
            profile.notes or None,
            int(profile.only_add),
            int(profile.only_modify),
        )
        with self.connect() as conn:
            cursor = conn.cursor()
            new_id = int(cursor.execute(query, params).fetchone()[0])
            conn.commit()
        return new_id

    def update_import_profile(self, profile: ImportProfile) -> None:
        query = """
        UPDATE dbo.V_Ta_InterODBC
        SET
            Proveedor = ?,
            CuentaProveedor = ?,
            PoliticaPrecios = ?,
            Hoja = ?,
            LISTA = ?,
            RangoDesde = ?,
            RangoHasta = ?,
            CamposClave = ?,
            Notas = ?,
            SoloAlta = ?,
            SoloModificacion = ?
        WHERE Id = ?
        """
        params = (
            profile.provider_name,
            profile.provider_account,
            profile.price_policy or None,
            profile.sheet_name or None,
            profile.list_code or None,
            profile.range_from or None,
            profile.range_to or None,
            profile.key_fields or None,
            profile.notes or None,
            int(profile.only_add),
            int(profile.only_modify),
            profile.id,
        )
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            conn.commit()

    def delete_import_profile(self, profile_id: int) -> None:
        query = "DELETE FROM dbo.V_Ta_InterODBC WHERE Id = ?"
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.execute(query, profile_id)
            conn.commit()

    def create_import_batch(
        self,
        *,
        profile: ImportProfile,
        source_file: str,
        user_name: str,
        source_kind: str,
    ) -> ImportBatch:
        file_path = Path(source_file)
        file_hash = self._sha256_file(file_path)
        query = """
        INSERT INTO dbo.IA_Costos_Importacion_CAB
        (
            FechaHora_InicioProceso,
            Estado,
            Usuario,
            IdInterODBC,
            Proveedor,
            CuentaProveedor,
            PoliticaPrecios,
            Lista,
            SoloAlta,
            SoloModificacion,
            ArchivoOrigen,
            ArchivoNombre,
            ArchivoHash,
            TipoArchivo,
            HojaConfigurada,
            RangoDesde,
            RangoHasta,
            NotasConfiguracion,
            PromptIAAdicional
        )
        OUTPUT INSERTED.ID
        VALUES
        (
            GETDATE(),
            'CONFIGURADA',
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?,
            ?
        )
        """
        params = (
            user_name,
            profile.id,
            profile.provider_name,
            profile.provider_account,
            profile.price_policy or None,
            profile.list_code or None,
            int(profile.only_add),
            int(profile.only_modify),
            str(file_path),
            file_path.name,
            file_hash,
            source_kind,
            profile.sheet_name or None,
            profile.range_from or None,
            profile.range_to or None,
            profile.notes or None,
            profile.notes or None,
        )
        with self.connect() as conn:
            cursor = conn.cursor()
            batch_id = int(cursor.execute(query, params).fetchone()[0])
            conn.commit()
        return ImportBatch(
            id=batch_id,
            profile_id=profile.id,
            provider_name=profile.provider_name,
            provider_account=profile.provider_account,
            source_file=str(file_path),
            source_name=file_path.name,
            status="CONFIGURADA",
            user_name=user_name,
        )

    def insert_import_details(self, batch_id: int, rows: list[ImportedRow]) -> None:
        if not rows:
            return
        query = """
        INSERT INTO dbo.IA_Costos_Importacion_DET
        (
            ID_CAB,
            FilaOrigen,
            CodigoProveedorLeido,
            DescripcionLeida,
            PrecioCostoLeido,
            JsonFilaOriginal
        )
        VALUES (?, ?, ?, ?, ?, ?)
        """
        payload = []
        for row in rows:
            payload.append(
                (
                    batch_id,
                    row.row_number,
                    row.provider_code or None,
                    row.description,
                    float(row.cost_price) if row.cost_price is not None else None,
                    json.dumps(row.raw_values, ensure_ascii=False),
                )
            )
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True
            cursor.executemany(query, payload)
            cursor.execute(
                """
                UPDATE dbo.IA_Costos_Importacion_CAB
                SET Estado = 'IMPORTADA',
                    TotalFilasLeidas = ?,
                    TotalFilasConCosto = ?
                WHERE ID = ?
                """,
                len(rows),
                len(rows),
                batch_id,
            )
            conn.commit()

    def find_master_articles(
        self,
        *,
        provider_account: str,
        provider_codes: list[str],
    ) -> list[MasterArticle]:
        query = """
        SELECT
            IDARTICULO,
            IDARTICULO,
            DESCRIPCION,
            COSTO,
            ISNULL(CodigoArtProveedor, ''),
            CUENTAPROVEEDOR,
            ISNULL(SUSPENDIDO, 0)
        FROM dbo.V_MA_ARTICULOS
        WHERE ISNULL(CUENTAPROVEEDOR, '') = ?
        """
        params: list[object] = [provider_account]

        articles: list[MasterArticle] = []
        with self.connect() as conn:
            cursor = conn.cursor()
            for row in cursor.execute(query, params).fetchall():
                articles.append(
                    MasterArticle(
                        article_id=str(row[0]).strip(),
                        article_code=str(row[1]).strip(),
                        description=str(row[2]).strip(),
                        current_cost=self._to_decimal(row[3]),
                        provider_code=str(row[4]).strip(),
                        provider_id=str(row[5]).strip() if row[5] is not None else None,
                        active=not bool(row[6]),
                    )
                )
        return articles

    def update_detail_matches(self, batch_id: int, records: list[BatchDetailRecord]) -> None:
        if not records:
            return
        query = """
        UPDATE dbo.IA_Costos_Importacion_DET
        SET
            Estado = ?,
            IdArticulo = ?,
            DescripcionArticulo = ?,
            CostoActual = ?,
            CostoNuevo = ?,
            TipoMatch = ?,
            ScoreMatch = ?,
            CoincidenciaCodigoProveedor = ?,
            ScoreDescripcion = ?,
            ScorePrecioApoyo = ?,
            AlertaVariacion = ?,
            AlertaDetalle = ?,
            VariacionPct = ?
        WHERE ID_CAB = ?
          AND FilaOrigen = ?
        """
        payload = []
        confirmed = 0
        without_match = 0
        for record in records:
            has_match = bool(record.article_id)
            if has_match:
                confirmed += 1
            else:
                without_match += 1
            payload.append(
                (
                    "MATCHEADO" if has_match else "SIN_MATCH",
                    record.article_id or None,
                    record.article_description or None,
                    float(record.current_cost) if record.current_cost is not None else None,
                    float(record.new_cost) if record.new_cost is not None else None,
                    record.match_type or None,
                    record.match_score or None,
                    1 if record.match_type == "provider_code_exact" else 0,
                    record.match_score if record.match_type == "description_fuzzy" else None,
                    None,
                    1 if bool(record.alert) else 0,
                    record.alert or None,
                    self._variation_pct(record.current_cost, record.new_cost),
                    batch_id,
                    record.row_number,
                )
            )
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True
            cursor.executemany(query, payload)
            cursor.execute(
                """
                UPDATE dbo.IA_Costos_Importacion_CAB
                SET Estado = 'EN_REVISION',
                    TotalFilasConfirmadas = ?
                WHERE ID = ?
                """,
                confirmed,
                batch_id,
            )
            conn.commit()

    def set_detail_decisions(
        self,
        *,
        batch_id: int,
        decisions: list[BatchDetailRecord],
        user_name: str,
    ) -> None:
        if not decisions:
            return
        query = """
        UPDATE dbo.IA_Costos_Importacion_DET
        SET
            Estado = ?,
            DecisionUsuario = ?,
            UsuarioRevision = ?,
            FechaHoraRevision = GETDATE(),
            ObservacionesRevision = ?,
            FueSeleccionManual = ?,
            IdArticulo = ?,
            DescripcionArticulo = ?,
            CostoActual = ?,
            CostoNuevo = ?,
            TipoMatch = ?,
            ScoreMatch = ?
        WHERE ID_CAB = ?
          AND FilaOrigen = ?
        """
        payload = []
        confirmed = 0
        for record in decisions:
            confirmed += 1 if record.decision == "CONFIRMAR" else 0
            payload.append(
                (
                    "CONFIRMADO" if record.decision == "CONFIRMAR" else "DESCARTADO",
                    record.decision,
                    user_name,
                    record.alert or None,
                    1 if record.match_type == "manual" else 0,
                    record.article_id or None,
                    record.article_description or None,
                    float(record.current_cost) if record.current_cost is not None else None,
                    float(record.new_cost) if record.new_cost is not None else None,
                    record.match_type or None,
                    record.match_score or None,
                    batch_id,
                    record.row_number,
                )
            )
        with self.connect() as conn:
            cursor = conn.cursor()
            cursor.fast_executemany = True
            cursor.executemany(query, payload)
            cursor.execute(
                """
                UPDATE dbo.IA_Costos_Importacion_CAB
                SET Estado = 'LISTA_PARA_APLICAR',
                    TotalFilasConfirmadas = (
                        SELECT COUNT(*)
                        FROM dbo.IA_Costos_Importacion_DET
                        WHERE ID_CAB = ?
                          AND Estado = 'CONFIRMADO'
                    )
                WHERE ID = ?
                """,
                batch_id,
                batch_id,
            )
            conn.commit()

    def apply_confirmed_updates(
        self,
        *,
        batch_id: int,
        user_name: str,
    ) -> dict[str, int]:
        cab_query = """
        SELECT
            ID,
            Proveedor,
            CuentaProveedor,
            ArchivoOrigen,
            ISNULL(SoloAlta, 0),
            ISNULL(SoloModificacion, 0)
        FROM dbo.IA_Costos_Importacion_CAB
        WHERE ID = ?
        """
        det_query = """
        SELECT
            d.ID,
            d.FilaOrigen,
            ISNULL(d.CodigoProveedorLeido, ''),
            ISNULL(d.DescripcionLeida, ''),
            d.PrecioCostoLeido,
            ISNULL(d.IdArticulo, ''),
            ISNULL(d.DescripcionArticulo, ''),
            d.CostoActual,
            d.CostoNuevo,
            ISNULL(d.TipoMatch, ''),
            ISNULL(d.ScoreMatch, 0),
            ISNULL(d.AlertaDetalle, '')
        FROM dbo.IA_Costos_Importacion_DET d
        WHERE d.ID_CAB = ?
          AND d.Estado = 'CONFIRMADO'
        ORDER BY d.FilaOrigen
        """
        summary = {"updated": 0, "same": 0, "blocked": 0, "errors": 0}
        with self.connect() as conn:
            cursor = conn.cursor()
            cab = cursor.execute(cab_query, batch_id).fetchone()
            if cab is None:
                raise RuntimeError("No existe la corrida indicada.")

            provider_name = str(cab[1]).strip()
            provider_account = str(cab[2]).strip()
            source_file = str(cab[3]).strip()
            only_add = bool(cab[4])
            only_modify = bool(cab[5])

            rows = cursor.execute(det_query, batch_id).fetchall()
            for row in rows:
                detail_id = int(row[0])
                row_number = int(row[1])
                provider_code = str(row[2]).strip()
                imported_description = str(row[3]).strip()
                imported_cost = self._to_decimal(row[4])
                article_id = str(row[5]).strip()
                article_description = str(row[6]).strip()
                previous_cost = self._to_decimal(row[7])
                new_cost = self._to_decimal(row[8])
                match_type = str(row[9]).strip()
                match_score = float(row[10] or 0)
                alert_text = str(row[11]).strip()

                if not article_id:
                    result = "ERROR" if only_modify else "ALTA"
                    error_text = (
                        "Perfil SoloModificacion: fila sin articulo, no se puede aplicar."
                        if only_modify
                        else "Alta de articulos no implementada en este MVP."
                    )
                    self._mark_detail_applied(
                        cursor=cursor,
                        detail_id=detail_id,
                        user_name=user_name,
                        result=result,
                        error_text=error_text,
                        applied=False,
                    )
                    if result == "ALTA":
                        summary["blocked"] += 1
                    else:
                        summary["errors"] += 1
                    continue

                if only_add:
                    self._mark_detail_applied(
                        cursor=cursor,
                        detail_id=detail_id,
                        user_name=user_name,
                        result="BLOQUEADO",
                        error_text="Perfil SoloAlta: no se permiten modificaciones de costo.",
                        applied=False,
                    )
                    summary["blocked"] += 1
                    continue

                current_row = cursor.execute(
                    """
                    SELECT COSTO
                    FROM dbo.V_MA_ARTICULOS
                    WHERE IDARTICULO = ?
                    """,
                    article_id,
                ).fetchone()
                if current_row is None:
                    self._mark_detail_applied(
                        cursor=cursor,
                        detail_id=detail_id,
                        user_name=user_name,
                        result="ERROR",
                        error_text="El articulo ya no existe en V_MA_ARTICULOS.",
                        applied=False,
                    )
                    summary["errors"] += 1
                    continue

                db_current_cost = self._to_decimal(current_row[0])
                if new_cost is None:
                    self._mark_detail_applied(
                        cursor=cursor,
                        detail_id=detail_id,
                        user_name=user_name,
                        result="ERROR",
                        error_text="La fila confirmada no tiene costo nuevo.",
                        applied=False,
                    )
                    summary["errors"] += 1
                    continue

                if db_current_cost == new_cost:
                    self._mark_detail_applied(
                        cursor=cursor,
                        detail_id=detail_id,
                        user_name=user_name,
                        result="SIN_CAMBIO",
                        error_text=None,
                        applied=True,
                    )
                    summary["same"] += 1
                    self._insert_history_row(
                        cursor=cursor,
                        batch_id=batch_id,
                        detail_id=detail_id,
                        provider_name=provider_name,
                        provider_account=provider_account,
                        source_file=source_file,
                        row_number=row_number,
                        article_id=article_id,
                        provider_code=provider_code,
                        imported_description=imported_description,
                        article_description=article_description,
                        previous_cost=db_current_cost,
                        new_cost=new_cost,
                        match_type=match_type,
                        match_score=match_score,
                        alert_text=alert_text,
                        user_name=user_name,
                    )
                    continue

                cursor.execute(
                    """
                    UPDATE dbo.V_MA_ARTICULOS
                    SET COSTO = ?,
                        FhUltimoCosto = GETDATE(),
                        Usuario = ?
                    WHERE IDARTICULO = ?
                    """,
                    float(new_cost),
                    user_name,
                    article_id,
                )
                self._mark_detail_applied(
                    cursor=cursor,
                    detail_id=detail_id,
                    user_name=user_name,
                    result="OK",
                    error_text=None,
                    applied=True,
                )
                self._insert_history_row(
                    cursor=cursor,
                    batch_id=batch_id,
                    detail_id=detail_id,
                    provider_name=provider_name,
                    provider_account=provider_account,
                    source_file=source_file,
                    row_number=row_number,
                    article_id=article_id,
                    provider_code=provider_code,
                    imported_description=imported_description,
                    article_description=article_description,
                    previous_cost=db_current_cost,
                    new_cost=new_cost,
                    match_type=match_type,
                    match_score=match_score,
                    alert_text=alert_text,
                    user_name=user_name,
                )
                summary["updated"] += 1

            cursor.execute(
                """
                UPDATE dbo.IA_Costos_Importacion_CAB
                SET
                    Estado = CASE
                        WHEN ? > 0 AND (? > 0 OR ? > 0) THEN 'APLICADA_PARCIAL'
                        WHEN ? > 0 AND ? = 0 AND ? = 0 THEN 'APLICADA'
                        ELSE 'ERROR'
                    END,
                    FechaHora_FinProceso = GETDATE(),
                    TotalActualizadas = ?,
                    TotalAltas = ?,
                    TotalSinCambios = ?,
                    TotalErrores = ?
                WHERE ID = ?
                """,
                summary["updated"],
                summary["blocked"],
                summary["errors"],
                summary["updated"],
                summary["blocked"],
                summary["errors"],
                summary["updated"],
                summary["blocked"],
                summary["same"],
                summary["errors"],
                batch_id,
            )
            conn.commit()
        return summary

    def load_recent_history(self, *, limit: int = 100) -> list[HistoryRecord]:
        query = f"""
        SELECT TOP ({int(limit)})
            ISNULL(ImportacionID, 0),
            CONVERT(nvarchar(19), FechaHora, 120),
            ISNULL(Usuario, ''),
            ISNULL(Proveedor, ''),
            ISNULL(ArchivoOrigen, ''),
            FilaOrigen,
            ISNULL(ArticuloID, ''),
            ISNULL(DescripcionImportada, ''),
            CostoAnterior,
            CostoNuevo,
            ISNULL(MatchTipo, ''),
            ISNULL(MatchScore, 0),
            ISNULL(AlertaDetalle, '')
        FROM dbo.IA_Costos_Actualizacion_Hist
        ORDER BY FechaHora DESC, ID DESC
        """
        items: list[HistoryRecord] = []
        with self.connect() as conn:
            cursor = conn.cursor()
            for row in cursor.execute(query).fetchall():
                items.append(
                    HistoryRecord(
                        import_batch_id=int(row[0]) if row[0] not in (None, 0) else None,
                        timestamp_text=str(row[1]).strip(),
                        user_name=str(row[2]).strip(),
                        provider_name=str(row[3]).strip(),
                        source_file=str(row[4]).strip(),
                        row_number=int(row[5]),
                        article_id=str(row[6]).strip(),
                        imported_description=str(row[7]).strip(),
                        previous_cost=self._to_decimal(row[8]),
                        new_cost=self._to_decimal(row[9]),
                        match_type=str(row[10]).strip(),
                        match_score=float(row[11] or 0),
                        alert_text=str(row[12]).strip(),
                    )
                )
        return items

    def load_batch_detail_results(self, *, batch_id: int) -> dict[int, tuple[str, str]]:
        query = """
        SELECT
            FilaOrigen,
            ISNULL(ResultadoAplicacion, ''),
            ISNULL(ErrorAplicacion, '')
        FROM dbo.IA_Costos_Importacion_DET
        WHERE ID_CAB = ?
        """
        items: dict[int, tuple[str, str]] = {}
        with self.connect() as conn:
            cursor = conn.cursor()
            for row in cursor.execute(query, batch_id).fetchall():
                items[int(row[0])] = (str(row[1]).strip(), str(row[2]).strip())
        return items

    def _mark_detail_applied(
        self,
        *,
        cursor: pyodbc.Cursor,
        detail_id: int,
        user_name: str,
        result: str,
        error_text: str | None,
        applied: bool,
    ) -> None:
        cursor.execute(
            """
            UPDATE dbo.IA_Costos_Importacion_DET
            SET
                Estado = CASE
                    WHEN ? = 1 THEN 'APLICADO'
                    WHEN ? = 'ERROR' THEN 'ERROR'
                    ELSE Estado
                END,
                Aplicado = ?,
                FechaHoraAplicacion = GETDATE(),
                UsuarioAplicacion = ?,
                ResultadoAplicacion = ?,
                ErrorAplicacion = ?
            WHERE ID = ?
            """,
            1 if applied else 0,
            result,
            1 if applied else 0,
            user_name,
            result,
            error_text,
            detail_id,
        )

    def _insert_history_row(
        self,
        *,
        cursor: pyodbc.Cursor,
        batch_id: int,
        detail_id: int,
        provider_name: str,
        provider_account: str,
        source_file: str,
        row_number: int,
        article_id: str,
        provider_code: str,
        imported_description: str,
        article_description: str,
        previous_cost: Decimal | None,
        new_cost: Decimal | None,
        match_type: str,
        match_score: float,
        alert_text: str,
        user_name: str,
    ) -> None:
        cursor.execute(
            """
            INSERT INTO dbo.IA_Costos_Actualizacion_Hist
            (
                ImportacionID,
                ImportacionDetID,
                Usuario,
                Proveedor,
                CuentaProveedor,
                ArchivoOrigen,
                FilaOrigen,
                ArticuloID,
                ArticuloCodigo,
                ProveedorCodigo,
                DescripcionImportada,
                DescripcionArticulo,
                CostoAnterior,
                CostoNuevo,
                VariacionPct,
                MatchTipo,
                MatchScore,
                AlertaVariacion,
                AlertaDetalle,
                Observaciones
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            batch_id,
            detail_id,
            user_name,
            provider_name,
            provider_account,
            source_file,
            row_number,
            article_id,
            article_id,
            provider_code or None,
            imported_description or None,
            article_description or None,
            float(previous_cost) if previous_cost is not None else None,
            float(new_cost) if new_cost is not None else None,
            self._variation_pct(previous_cost, new_cost),
            match_type or None,
            match_score,
            1 if bool(alert_text) else 0,
            alert_text or None,
            None,
        )

    @staticmethod
    def _to_decimal(value: object) -> Decimal | None:
        if value is None:
            return None
        return Decimal(str(value))

    @staticmethod
    def _variation_pct(previous_cost: Decimal | None, new_cost: Decimal | None) -> float | None:
        if previous_cost is None or new_cost is None or previous_cost == 0:
            return None
        return float(((new_cost - previous_cost) / previous_cost) * 100)

    @staticmethod
    def _sha256_file(path: Path) -> str:
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(65536), b""):
                digest.update(chunk)
        return digest.hexdigest()
