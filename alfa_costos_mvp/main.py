from __future__ import annotations

import sys
from pathlib import Path

from PySide6.QtWidgets import QApplication

from alfa_costos_mvp.config import AppConfig, load_env_near_file
from alfa_costos_mvp.ui.main_window import MainWindow


def main() -> int:
    base_dir = Path(__file__).resolve().parent
    load_env_near_file(base_dir)
    config_path = base_dir / "app_config.json"
    config = AppConfig.from_env()
    if not config.is_sql_configured():
        config = AppConfig.from_file(config_path)

    app = QApplication(sys.argv)
    window = MainWindow(config=config, config_path=config_path)
    window.show()
    return app.exec()


if __name__ == "__main__":
    raise SystemExit(main())
