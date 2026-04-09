"""
visor_compras_ia.py
-------------------
Visor de comprobantes procesados por el agente IA.
Lee IA_Compras_CAB desde SQL Server, muestra grid + imagen sincronizada.
Permite filtrar, cambiar estado, configurar columnas visibles y abrir imagen aparte.

Requiere: pip install PySide6 pyodbc pdf2image pillow
          (pdf2image también necesita poppler en PATH)
"""

from __future__ import annotations

import json
import sys
import traceback
from pathlib import Path
from typing import Any, Dict, List, Optional

# ---------------------------------------------------------------------------
# Dependencias opcionales
# ---------------------------------------------------------------------------
try:
    from PySide6.QtCore import (
        Qt, QAbstractTableModel, QModelIndex, QSortFilterProxyModel,
        QThread, Signal, QSettings, QSize, QTimer,
    )
    from PySide6.QtGui import QPixmap, QImage, QColor, QFont, QIcon, QAction
    from PySide6.QtWidgets import (
        QApplication, QMainWindow, QWidget, QSplitter, QVBoxLayout,
        QHBoxLayout, QGridLayout, QLabel, QPushButton, QComboBox,
        QLineEdit, QDateEdit, QCheckBox, QTableView, QHeaderView,
        QAbstractItemView, QScrollArea, QSizePolicy, QMessageBox,
        QDialog, QDialogButtonBox, QGroupBox, QStatusBar, QFrame,
        QToolBar, QProgressBar, QMenu,
    )
    from PySide6.QtCore import QDate
except ImportError:
    print("ERROR: Instalar PySide6 → pip install PySide6")
    sys.exit(1)

try:
    import pyodbc
    PYODBC_OK = True
except ImportError:
    PYODBC_OK = False

# PDF rendering
try:
    from pdf2image import convert_from_path
    PDF2IMAGE_OK = True
except ImportError:
    PDF2IMAGE_OK = False

try:
    from PIL import Image as PILImage
    PIL_OK = True
except ImportError:
    PIL_OK = False

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
DEFAULT_SERVER   = "SERVER-ALFAVB6"
DEFAULT_DATABASE = "ALFANET"
DEFAULT_USER     = "ALFANET"
DEFAULT_PASSWORD = "ALFANET"
DEFAULT_DRIVER   = "ODBC Driver 18 for SQL Server"

APP_NAME    = "VisorComprasIA"
APP_VERSION = "1.0"
SETTINGS_FILE = Path(__file__).with_suffix(".visor.json")

# Columnas de IA_Compras_CAB con etiqueta amigable
ALL_COLUMNS: List[tuple[str, str]] = [
    ("ID",                      "ID"),
    ("Estado",                  "Estado"),
    ("FechaHora_Proceso",       "Fecha Proceso"),
    ("TipoComprobante",         "Tipo"),
    ("Letra",                   "Letra"),
    ("PuntoVenta",              "Pto.Venta"),
    ("Numero",                  "Número"),
    ("Fecha",                   "Fecha Cpte"),
    ("Proveedor_Nombre",        "Proveedor"),
    ("Proveedor_CUIT",          "CUIT"),
    ("Cuenta_Contable",         "Cuenta"),
    ("Match_Metodo",            "Match"),
    ("Total",                   "Total"),
    ("NetoGravado",             "Neto Grav."),
    ("IVA_21",                  "IVA 21%"),
    ("IVA_105",                 "IVA 10.5%"),
    ("Percepcion_IVA",          "Perc.IVA"),
    ("Percepcion_IIBB",         "Perc.IIBB"),
    ("Moneda",                  "Moneda"),
    ("CAE",                     "CAE"),
    ("Archivo_NombreRenombrado","Archivo"),
    ("Archivo_RutaOriginal",    "Carpeta"),
    ("Lector_Observaciones",    "Observaciones"),
    ("Lector_Error",            "Error"),
]

DEFAULT_VISIBLE = [
    "ID", "Estado", "FechaHora_Proceso", "TipoComprobante", "Letra",
    "PuntoVenta", "Numero", "Fecha", "Proveedor_Nombre", "Proveedor_CUIT",
    "Cuenta_Contable", "Total", "Archivo_NombreRenombrado",
]

ESTADOS = ["PENDIENTE", "APROBADO", "RECHAZADO", "SIN_PROVEEDOR", "ERROR_LECTURA", "ERROR"]

COLOR_ESTADO: Dict[str, str] = {
    "APROBADO":       "#d4edda",
    "RECHAZADO":      "#f8d7da",
    "ERROR_LECTURA":  "#fff3cd",
    "ERROR":          "#fff3cd",
    "SIN_PROVEEDOR":  "#d1ecf1",
    "PENDIENTE":      "#ffffff",
}

# ---------------------------------------------------------------------------
# Persistencia de configuración
# ---------------------------------------------------------------------------
def load_config() -> dict:
    try:
        if SETTINGS_FILE.exists():
            return json.loads(SETTINGS_FILE.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def save_config(data: dict) -> None:
    try:
        SETTINGS_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Conexión SQL
# ---------------------------------------------------------------------------
def build_conn_str(server: str, database: str, user: str, password: str, driver: str) -> str:
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={database};"
        f"UID={user};PWD={password};"
        "TrustServerCertificate=yes;"
    )

def test_connection(conn_str: str) -> Optional[str]:
    if not PYODBC_OK:
        return "pyodbc no instalado"
    try:
        with pyodbc.connect(conn_str, timeout=10):
            return None
    except Exception as e:
        return str(e)

def fetch_data(conn_str: str, filters: dict) -> List[dict]:
    """Consulta IA_Compras_CAB con filtros dinámicos."""
    col_names = [c[0] for c in ALL_COLUMNS]
    where_parts = []
    params = []

    if filters.get("estado"):
        where_parts.append("Estado = ?")
        params.append(filters["estado"])
    if filters.get("proveedor"):
        where_parts.append("Proveedor_Nombre LIKE ?")
        params.append(f"%{filters['proveedor']}%")
    if filters.get("cuit"):
        where_parts.append("Proveedor_CUIT LIKE ?")
        params.append(f"%{filters['cuit']}%")
    if filters.get("cuenta"):
        where_parts.append("Cuenta_Contable LIKE ?")
        params.append(f"%{filters['cuenta']}%")
    if filters.get("archivo"):
        where_parts.append("(Archivo_NombreOriginal LIKE ? OR Archivo_NombreRenombrado LIKE ?)")
        params.extend([f"%{filters['archivo']}%", f"%{filters['archivo']}%"])
    if filters.get("fecha_desde"):
        where_parts.append("CAST(FechaHora_Proceso AS DATE) >= ?")
        params.append(filters["fecha_desde"])
    if filters.get("fecha_hasta"):
        where_parts.append("CAST(FechaHora_Proceso AS DATE) <= ?")
        params.append(filters["fecha_hasta"])
    if filters.get("tipo"):
        where_parts.append("TipoComprobante = ?")
        params.append(filters["tipo"])
    if filters.get("numero"):
        where_parts.append("Numero LIKE ?")
        params.append(f"%{filters['numero']}%")

    where_sql = ("WHERE " + " AND ".join(where_parts)) if where_parts else ""
    sql = f"SELECT TOP 2000 {', '.join(col_names)} FROM IA_Compras_CAB {where_sql} ORDER BY ID DESC"

    with pyodbc.connect(conn_str, timeout=15) as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()

    result = []
    for row in rows:
        result.append({col_names[i]: (row[i] if row[i] is not None else "") for i in range(len(col_names))})
    return result

def update_estado(conn_str: str, ids: List[int], nuevo_estado: str) -> None:
    placeholders = ",".join("?" * len(ids))
    sql = f"UPDATE IA_Compras_CAB SET Estado = ? WHERE ID IN ({placeholders})"
    with pyodbc.connect(conn_str, timeout=15) as conn:
        cur = conn.cursor()
        cur.execute(sql, [nuevo_estado] + ids)
        conn.commit()

# ---------------------------------------------------------------------------
# Worker thread para carga de datos
# ---------------------------------------------------------------------------
class LoadWorker(QThread):
    finished = Signal(list)
    error = Signal(str)

    def __init__(self, conn_str: str, filters: dict):
        super().__init__()
        self.conn_str = conn_str
        self.filters = filters

    def run(self):
        try:
            data = fetch_data(self.conn_str, self.filters)
            self.finished.emit(data)
        except Exception as e:
            self.error.emit(str(e))

# ---------------------------------------------------------------------------
# Modelo de tabla
# ---------------------------------------------------------------------------
class ComprasModel(QAbstractTableModel):
    def __init__(self, visible_cols: List[str]):
        super().__init__()
        self._data: List[dict] = []
        self._visible = visible_cols  # lista de col_name en orden

    def set_data(self, rows: List[dict]):
        self.beginResetModel()
        self._data = rows
        self.endResetModel()

    def set_visible(self, cols: List[str]):
        self.beginResetModel()
        self._visible = cols
        self.endResetModel()

    def rowCount(self, parent=QModelIndex()):
        return len(self._data)

    def columnCount(self, parent=QModelIndex()):
        return len(self._visible)

    def headerData(self, section: int, orientation, role=Qt.DisplayRole):
        if role == Qt.DisplayRole:
            if orientation == Qt.Horizontal:
                col_name = self._visible[section]
                label = next((l for c, l in ALL_COLUMNS if c == col_name), col_name)
                return label
            return str(section + 1)
        return None

    def data(self, index: QModelIndex, role=Qt.DisplayRole):
        if not index.isValid():
            return None
        row = self._data[index.row()]
        col_name = self._visible[index.column()]
        value = row.get(col_name, "")

        if role == Qt.DisplayRole:
            if isinstance(value, float):
                return f"{value:,.2f}"
            return str(value) if value != "" else ""

        if role == Qt.BackgroundRole:
            estado = str(row.get("Estado", ""))
            color_hex = COLOR_ESTADO.get(estado, "#ffffff")
            return QColor(color_hex)

        if role == Qt.TextAlignmentRole:
            if col_name in ("Total", "NetoGravado", "IVA_21", "IVA_105",
                            "Percepcion_IVA", "Percepcion_IIBB"):
                return int(Qt.AlignRight | Qt.AlignVCenter)
            return int(Qt.AlignLeft | Qt.AlignVCenter)

        if role == Qt.UserRole:
            return row  # fila completa

        return None

    def get_row(self, index: int) -> dict:
        if 0 <= index < len(self._data):
            return self._data[index]
        return {}

    def get_ids_for_rows(self, rows: List[int]) -> List[int]:
        return [int(self._data[r]["ID"]) for r in rows if self._data[r].get("ID")]

    def update_estado_local(self, rows: List[int], nuevo_estado: str):
        for r in rows:
            self._data[r]["Estado"] = nuevo_estado
        if rows:
            top = self.index(min(rows), 0)
            bot = self.index(max(rows), self.columnCount() - 1)
            self.dataChanged.emit(top, bot)

# ---------------------------------------------------------------------------
# Panel de imagen
# ---------------------------------------------------------------------------
class ImagePanel(QScrollArea):
    def __init__(self):
        super().__init__()
        self.setWidgetResizable(True)
        self._label = QLabel("Sin imagen")
        self._label.setAlignment(Qt.AlignCenter)
        self._label.setStyleSheet("color: #888; font-size: 13px;")
        self.setWidget(self._label)
        self._current_pixmap: Optional[QPixmap] = None
        self._current_file: Optional[Path] = None

    def load_file(self, file_path: Path):
        if file_path == self._current_file:
            return
        self._current_file = file_path
        self._label.setText("Cargando...")
        self._current_pixmap = None

        if not file_path.exists():
            self._label.setText(f"Archivo no encontrado:\n{file_path}")
            return

        suffix = file_path.suffix.lower()
        if suffix == ".pdf":
            self._load_pdf(file_path)
        elif suffix in (".jpg", ".jpeg", ".png", ".bmp", ".tiff", ".tif", ".webp"):
            self._load_image(file_path)
        else:
            self._label.setText(f"Formato no soportado: {suffix}")

    def _load_image(self, path: Path):
        px = QPixmap(str(path))
        if px.isNull():
            self._label.setText(f"No se pudo cargar:\n{path.name}")
            return
        self._current_pixmap = px
        self._show_scaled()

    def _load_pdf(self, path: Path):
        if not PDF2IMAGE_OK:
            self._label.setText("pdf2image no instalado\npip install pdf2image")
            return
        try:
            images = convert_from_path(str(path), dpi=150, first_page=1, last_page=1)
            if not images:
                self._label.setText("PDF sin páginas")
                return
            img = images[0]
            # PIL → QPixmap
            img_rgb = img.convert("RGB")
            data = img_rgb.tobytes("raw", "RGB")
            qimg = QImage(data, img_rgb.width, img_rgb.height, img_rgb.width * 3, QImage.Format_RGB888)
            self._current_pixmap = QPixmap.fromImage(qimg)
            self._show_scaled()
        except Exception as e:
            self._label.setText(f"Error al renderizar PDF:\n{e}")

    def _show_scaled(self):
        if self._current_pixmap is None:
            return
        avail = self.viewport().size()
        px = self._current_pixmap.scaled(avail, Qt.KeepAspectRatio, Qt.SmoothTransformation)
        self._label.setPixmap(px)

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._show_scaled()

    def get_pixmap(self) -> Optional[QPixmap]:
        return self._current_pixmap

    def get_current_file(self) -> Optional[Path]:
        return self._current_file

    def clear(self):
        self._label.setText("Sin imagen")
        self._current_pixmap = None
        self._current_file = None

# ---------------------------------------------------------------------------
# Ventana de imagen aparte
# ---------------------------------------------------------------------------
class ImageWindow(QDialog):
    def __init__(self, pixmap: QPixmap, title: str, parent=None):
        super().__init__(parent)
        self.setWindowTitle(title)
        self.resize(900, 700)
        layout = QVBoxLayout(self)
        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        lbl = QLabel()
        lbl.setAlignment(Qt.AlignCenter)
        lbl.setPixmap(pixmap.scaled(
            QSize(860, 660), Qt.KeepAspectRatio, Qt.SmoothTransformation
        ))
        scroll.setWidget(lbl)
        layout.addWidget(scroll)

# ---------------------------------------------------------------------------
# Diálogo de configuración de columnas
# ---------------------------------------------------------------------------
class ColsDialog(QDialog):
    def __init__(self, visible: List[str], parent=None):
        super().__init__(parent)
        self.setWindowTitle("Columnas visibles")
        self.resize(350, 500)
        layout = QVBoxLayout(self)
        layout.addWidget(QLabel("Seleccioná las columnas a mostrar:"))
        self._checks: Dict[str, QCheckBox] = {}
        for col_name, label in ALL_COLUMNS:
            cb = QCheckBox(f"{label}  ({col_name})")
            cb.setChecked(col_name in visible)
            self._checks[col_name] = cb
            layout.addWidget(cb)
        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns)

    def get_visible(self) -> List[str]:
        return [c for c in [col[0] for col in ALL_COLUMNS] if self._checks[c].isChecked()]

# ---------------------------------------------------------------------------
# Diálogo de conexión
# ---------------------------------------------------------------------------
class ConnDialog(QDialog):
    def __init__(self, cfg: dict, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Configuración de conexión")
        self.resize(420, 260)
        layout = QGridLayout(self)
        fields = [
            ("Servidor:",  "server",   cfg.get("server",   DEFAULT_SERVER)),
            ("Base:",      "database", cfg.get("database", DEFAULT_DATABASE)),
            ("Usuario:",   "user",     cfg.get("user",     DEFAULT_USER)),
            ("Contraseña:","password", cfg.get("password", DEFAULT_PASSWORD)),
            ("Driver:",    "driver",   cfg.get("driver",   DEFAULT_DRIVER)),
        ]
        self._vars: Dict[str, QLineEdit] = {}
        for row, (label, key, val) in enumerate(fields):
            layout.addWidget(QLabel(label), row, 0)
            le = QLineEdit(val)
            if key == "password":
                le.setEchoMode(QLineEdit.Password)
            self._vars[key] = le
            layout.addWidget(le, row, 1)

        self._status = QLabel("")
        self._status.setStyleSheet("color: red;")
        layout.addWidget(self._status, len(fields), 0, 1, 2)

        btn_test = QPushButton("Probar conexión")
        btn_test.clicked.connect(self._test)
        layout.addWidget(btn_test, len(fields)+1, 0)

        btns = QDialogButtonBox(QDialogButtonBox.Ok | QDialogButtonBox.Cancel)
        btns.accepted.connect(self.accept)
        btns.rejected.connect(self.reject)
        layout.addWidget(btns, len(fields)+1, 1)

    def _test(self):
        cs = build_conn_str(
            self._vars["server"].text(), self._vars["database"].text(),
            self._vars["user"].text(), self._vars["password"].text(),
            self._vars["driver"].text(),
        )
        err = test_connection(cs)
        if err:
            self._status.setStyleSheet("color: red;")
            self._status.setText(f"Error: {err}")
        else:
            self._status.setStyleSheet("color: green;")
            self._status.setText("Conexión OK")

    def get_values(self) -> dict:
        return {k: v.text() for k, v in self._vars.items()}

# ---------------------------------------------------------------------------
# Ventana principal
# ---------------------------------------------------------------------------
class MainWindow(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(f"Visor Compras IA  v{APP_VERSION}")
        self.resize(1400, 750)

        self._cfg = load_config()
        self._conn_str: Optional[str] = None
        self._visible_cols: List[str] = self._cfg.get("visible_cols", DEFAULT_VISIBLE)
        self._worker: Optional[LoadWorker] = None
        self._img_debounce = QTimer()
        self._img_debounce.setSingleShot(True)
        self._img_debounce.setInterval(120)
        self._img_debounce.timeout.connect(self._load_image_for_selection)

        self._build_ui()
        self._restore_conn()

    # ------------------------------------------------------------------
    # UI
    # ------------------------------------------------------------------
    def _build_ui(self):
        # Toolbar
        tb = QToolBar("Acciones")
        tb.setMovable(False)
        self.addToolBar(tb)

        act_conn = QAction("Conexión", self)
        act_conn.setToolTip("Configurar conexión SQL Server")
        act_conn.triggered.connect(self._open_conn_dialog)
        tb.addAction(act_conn)

        act_cols = QAction("Columnas", self)
        act_cols.setToolTip("Elegir columnas visibles")
        act_cols.triggered.connect(self._open_cols_dialog)
        tb.addAction(act_cols)

        tb.addSeparator()

        act_refresh = QAction("Actualizar", self)
        act_refresh.setToolTip("Recargar datos (F5)")
        act_refresh.setShortcut("F5")
        act_refresh.triggered.connect(self._load_data)
        tb.addAction(act_refresh)

        tb.addSeparator()

        act_approve = QAction("Aprobar", self)
        act_approve.setToolTip("Marcar seleccionados como APROBADO")
        act_approve.triggered.connect(lambda: self._change_estado("APROBADO"))
        tb.addAction(act_approve)

        act_reject = QAction("Rechazar", self)
        act_reject.setToolTip("Marcar seleccionados como RECHAZADO")
        act_reject.triggered.connect(lambda: self._change_estado("RECHAZADO"))
        tb.addAction(act_reject)

        self._combo_estado_cambio = QComboBox()
        self._combo_estado_cambio.addItems(ESTADOS)
        self._combo_estado_cambio.setToolTip("Estado a asignar")
        tb.addWidget(self._combo_estado_cambio)

        act_set_estado = QAction("Cambiar estado", self)
        act_set_estado.triggered.connect(self._change_estado_combo)
        tb.addAction(act_set_estado)

        tb.addSeparator()

        act_img_win = QAction("Ver imagen aparte", self)
        act_img_win.setToolTip("Abrir imagen en ventana separada")
        act_img_win.triggered.connect(self._open_image_window)
        tb.addAction(act_img_win)

        # Central
        central = QWidget()
        self.setCentralWidget(central)
        main_layout = QVBoxLayout(central)
        main_layout.setContentsMargins(4, 4, 4, 4)
        main_layout.setSpacing(4)

        # Panel de filtros
        main_layout.addWidget(self._build_filter_panel())

        # Splitter: grid | imagen
        self._splitter = QSplitter(Qt.Horizontal)
        main_layout.addWidget(self._splitter, stretch=1)

        # Grid
        grid_widget = QWidget()
        grid_layout = QVBoxLayout(grid_widget)
        grid_layout.setContentsMargins(0, 0, 0, 0)

        self._model = ComprasModel(self._visible_cols)
        self._proxy = QSortFilterProxyModel()
        self._proxy.setSourceModel(self._model)
        self._proxy.setFilterCaseSensitivity(Qt.CaseInsensitive)

        self._table = QTableView()
        self._table.setModel(self._proxy)
        self._table.setSortingEnabled(True)
        self._table.setSelectionBehavior(QAbstractItemView.SelectRows)
        self._table.setSelectionMode(QAbstractItemView.ExtendedSelection)
        self._table.setAlternatingRowColors(False)
        self._table.horizontalHeader().setStretchLastSection(True)
        self._table.horizontalHeader().setSectionsMovable(True)
        self._table.verticalHeader().setVisible(False)
        self._table.setContextMenuPolicy(Qt.CustomContextMenu)
        self._table.customContextMenuRequested.connect(self._context_menu)
        self._table.selectionModel().currentRowChanged.connect(self._on_row_changed)
        self._table.doubleClicked.connect(lambda: self._open_image_window())

        grid_layout.addWidget(self._table)

        self._lbl_count = QLabel("0 registros")
        self._lbl_count.setStyleSheet("padding: 2px; color: #555;")
        grid_layout.addWidget(self._lbl_count)

        self._splitter.addWidget(grid_widget)

        # Panel imagen
        self._img_panel = ImagePanel()
        self._img_panel.setMinimumWidth(250)
        self._splitter.addWidget(self._img_panel)

        # Restaurar tamaño del splitter
        saved_sizes = self._cfg.get("splitter_sizes")
        if saved_sizes:
            self._splitter.setSizes(saved_sizes)
        else:
            self._splitter.setSizes([900, 450])

        # Status bar
        self._status_bar = QStatusBar()
        self.setStatusBar(self._status_bar)
        self._progress = QProgressBar()
        self._progress.setMaximumWidth(200)
        self._progress.setVisible(False)
        self._status_bar.addPermanentWidget(self._progress)
        self._status_bar.showMessage("Sin conexión. Configurá la conexión SQL Server.")

    def _build_filter_panel(self) -> QWidget:
        box = QGroupBox("Filtros")
        layout = QGridLayout(box)
        layout.setSpacing(6)

        # Fila 0
        layout.addWidget(QLabel("Estado:"), 0, 0)
        self._f_estado = QComboBox()
        self._f_estado.addItem("(todos)", "")
        for e in ESTADOS:
            self._f_estado.addItem(e, e)
        layout.addWidget(self._f_estado, 0, 1)

        layout.addWidget(QLabel("Tipo:"), 0, 2)
        self._f_tipo = QComboBox()
        self._f_tipo.addItem("(todos)", "")
        for t in ["FACTURA", "NOTA DE CREDITO", "NOTA DE DEBITO", "RECIBO", "TICKET"]:
            self._f_tipo.addItem(t, t)
        layout.addWidget(self._f_tipo, 0, 3)

        layout.addWidget(QLabel("Proveedor:"), 0, 4)
        self._f_proveedor = QLineEdit()
        self._f_proveedor.setPlaceholderText("buscar...")
        layout.addWidget(self._f_proveedor, 0, 5)

        layout.addWidget(QLabel("CUIT:"), 0, 6)
        self._f_cuit = QLineEdit()
        self._f_cuit.setPlaceholderText("CUIT...")
        layout.addWidget(self._f_cuit, 0, 7)

        # Fila 1
        layout.addWidget(QLabel("Desde:"), 1, 0)
        self._f_desde = QDateEdit()
        self._f_desde.setCalendarPopup(True)
        self._f_desde.setDate(QDate.currentDate().addMonths(-3))
        self._f_desde.setSpecialValueText("(sin filtro)")
        layout.addWidget(self._f_desde, 1, 1)

        layout.addWidget(QLabel("Hasta:"), 1, 2)
        self._f_hasta = QDateEdit()
        self._f_hasta.setCalendarPopup(True)
        self._f_hasta.setDate(QDate.currentDate())
        layout.addWidget(self._f_hasta, 1, 3)

        layout.addWidget(QLabel("Número:"), 1, 4)
        self._f_numero = QLineEdit()
        self._f_numero.setPlaceholderText("número...")
        layout.addWidget(self._f_numero, 1, 5)

        layout.addWidget(QLabel("Archivo:"), 1, 6)
        self._f_archivo = QLineEdit()
        self._f_archivo.setPlaceholderText("nombre de archivo...")
        layout.addWidget(self._f_archivo, 1, 7)

        # Botones
        btn_buscar = QPushButton("Buscar")
        btn_buscar.setDefault(True)
        btn_buscar.clicked.connect(self._load_data)
        layout.addWidget(btn_buscar, 0, 8)

        btn_limpiar = QPushButton("Limpiar")
        btn_limpiar.clicked.connect(self._clear_filters)
        layout.addWidget(btn_limpiar, 1, 8)

        return box

    # ------------------------------------------------------------------
    # Conexión
    # ------------------------------------------------------------------
    def _restore_conn(self):
        conn_cfg = self._cfg.get("connection", {})
        if conn_cfg:
            self._conn_str = build_conn_str(
                conn_cfg.get("server", DEFAULT_SERVER),
                conn_cfg.get("database", DEFAULT_DATABASE),
                conn_cfg.get("user", DEFAULT_USER),
                conn_cfg.get("password", DEFAULT_PASSWORD),
                conn_cfg.get("driver", DEFAULT_DRIVER),
            )
            self._status_bar.showMessage(
                f"Conectado a {conn_cfg.get('server')} / {conn_cfg.get('database')}"
            )

    def _open_conn_dialog(self):
        dlg = ConnDialog(self._cfg.get("connection", {}), self)
        if dlg.exec() == QDialog.Accepted:
            vals = dlg.get_values()
            self._conn_str = build_conn_str(
                vals["server"], vals["database"], vals["user"],
                vals["password"], vals["driver"],
            )
            self._cfg["connection"] = vals
            save_config(self._cfg)
            self._status_bar.showMessage(
                f"Conectado a {vals['server']} / {vals['database']}"
            )

    # ------------------------------------------------------------------
    # Columnas
    # ------------------------------------------------------------------
    def _open_cols_dialog(self):
        dlg = ColsDialog(self._visible_cols, self)
        if dlg.exec() == QDialog.Accepted:
            new_visible = dlg.get_visible()
            if not new_visible:
                QMessageBox.warning(self, "Columnas", "Seleccioná al menos una columna.")
                return
            self._visible_cols = new_visible
            self._model.set_visible(new_visible)
            self._cfg["visible_cols"] = new_visible
            save_config(self._cfg)

    # ------------------------------------------------------------------
    # Filtros
    # ------------------------------------------------------------------
    def _get_filters(self) -> dict:
        f = {}
        estado = self._f_estado.currentData()
        if estado:
            f["estado"] = estado
        tipo = self._f_tipo.currentData()
        if tipo:
            f["tipo"] = tipo
        if self._f_proveedor.text().strip():
            f["proveedor"] = self._f_proveedor.text().strip()
        if self._f_cuit.text().strip():
            f["cuit"] = self._f_cuit.text().strip()
        if self._f_numero.text().strip():
            f["numero"] = self._f_numero.text().strip()
        if self._f_archivo.text().strip():
            f["archivo"] = self._f_archivo.text().strip()
        f["fecha_desde"] = self._f_desde.date().toString("yyyy-MM-dd")
        f["fecha_hasta"] = self._f_hasta.date().toString("yyyy-MM-dd")
        return f

    def _clear_filters(self):
        self._f_estado.setCurrentIndex(0)
        self._f_tipo.setCurrentIndex(0)
        self._f_proveedor.clear()
        self._f_cuit.clear()
        self._f_numero.clear()
        self._f_archivo.clear()
        self._f_desde.setDate(QDate.currentDate().addMonths(-3))
        self._f_hasta.setDate(QDate.currentDate())

    # ------------------------------------------------------------------
    # Carga de datos
    # ------------------------------------------------------------------
    def _load_data(self):
        if not self._conn_str:
            QMessageBox.warning(self, "Sin conexión", "Configurá la conexión SQL Server primero.")
            return
        if self._worker and self._worker.isRunning():
            return

        self._progress.setVisible(True)
        self._progress.setRange(0, 0)
        self._status_bar.showMessage("Cargando datos...")

        self._worker = LoadWorker(self._conn_str, self._get_filters())
        self._worker.finished.connect(self._on_data_loaded)
        self._worker.error.connect(self._on_data_error)
        self._worker.start()

    def _on_data_loaded(self, rows: List[dict]):
        self._progress.setVisible(False)
        self._model.set_data(rows)
        self._lbl_count.setText(f"{len(rows)} registros")
        self._status_bar.showMessage(f"OK — {len(rows)} registros cargados")

    def _on_data_error(self, msg: str):
        self._progress.setVisible(False)
        self._status_bar.showMessage(f"Error: {msg}")
        QMessageBox.critical(self, "Error al cargar datos", msg)

    # ------------------------------------------------------------------
    # Navegación → imagen
    # ------------------------------------------------------------------
    def _on_row_changed(self, current: QModelIndex, previous: QModelIndex):
        self._img_debounce.start()

    def _load_image_for_selection(self):
        idx = self._table.currentIndex()
        if not idx.isValid():
            self._img_panel.clear()
            return
        src_idx = self._proxy.mapToSource(idx)
        row = self._model.get_row(src_idx.row())
        ruta = str(row.get("Archivo_RutaOriginal", "")).strip()
        nombre = str(row.get("Archivo_NombreRenombrado", "")).strip()
        if not nombre:
            nombre = str(row.get("Archivo_NombreOriginal", "")).strip()

        if not ruta or not nombre:
            self._img_panel.clear()
            return

        # nombre puede ser "A.pdf, B.pdf" (multi-página) → tomar el primero
        nombre = nombre.split(",")[0].strip()
        file_path = Path(ruta) / "PROC_AGENTE_IA" / nombre
        if not file_path.exists():
            # intentar sin subcarpeta
            file_path = Path(ruta) / nombre
        self._img_panel.load_file(file_path)

    # ------------------------------------------------------------------
    # Ver imagen aparte
    # ------------------------------------------------------------------
    def _open_image_window(self):
        px = self._img_panel.get_pixmap()
        if px is None:
            QMessageBox.information(self, "Sin imagen", "No hay imagen para mostrar.")
            return
        fp = self._img_panel.get_current_file()
        title = fp.name if fp else "Imagen"
        dlg = ImageWindow(px, title, self)
        dlg.exec()

    # ------------------------------------------------------------------
    # Cambio de estado
    # ------------------------------------------------------------------
    def _selected_source_rows(self) -> List[int]:
        sel = self._table.selectionModel().selectedRows()
        return [self._proxy.mapToSource(idx).row() for idx in sel]

    def _change_estado(self, nuevo_estado: str):
        rows = self._selected_source_rows()
        if not rows:
            QMessageBox.information(self, "Sin selección", "Seleccioná al menos un registro.")
            return
        ids = self._model.get_ids_for_rows(rows)
        resp = QMessageBox.question(
            self, "Confirmar",
            f"¿Cambiar {len(ids)} registro(s) a '{nuevo_estado}'?",
            QMessageBox.Yes | QMessageBox.No,
        )
        if resp != QMessageBox.Yes:
            return
        try:
            update_estado(self._conn_str, ids, nuevo_estado)
            self._model.update_estado_local(rows, nuevo_estado)
            self._status_bar.showMessage(
                f"{len(ids)} registro(s) actualizados a {nuevo_estado}"
            )
        except Exception as e:
            QMessageBox.critical(self, "Error", f"No se pudo actualizar:\n{e}")

    def _change_estado_combo(self):
        self._change_estado(self._combo_estado_cambio.currentText())

    # ------------------------------------------------------------------
    # Menú contextual
    # ------------------------------------------------------------------
    def _context_menu(self, pos):
        menu = QMenu(self)
        for estado in ESTADOS:
            act = QAction(f"→ {estado}", self)
            act.triggered.connect(lambda checked, e=estado: self._change_estado(e))
            menu.addAction(act)
        menu.addSeparator()
        act_img = QAction("Ver imagen aparte", self)
        act_img.triggered.connect(self._open_image_window)
        menu.addAction(act_img)
        menu.exec(self._table.viewport().mapToGlobal(pos))

    # ------------------------------------------------------------------
    # Cierre
    # ------------------------------------------------------------------
    def closeEvent(self, event):
        self._cfg["splitter_sizes"] = self._splitter.sizes()
        save_config(self._cfg)
        super().closeEvent(event)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    app = QApplication(sys.argv)
    app.setApplicationName(APP_NAME)
    app.setStyle("Fusion")
    win = MainWindow()
    win.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()
