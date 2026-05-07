from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
import os

from dotenv import load_dotenv


def load_env_near_file(base_dir: Path) -> None:
    env_path = base_dir / ".env"
    if env_path.exists():
        load_dotenv(env_path, override=False)
    else:
        load_dotenv(override=False)


@dataclass(slots=True)
class AppConfig:
    sql_server: str = ""
    sql_database: str = ""
    sql_user: str = ""
    sql_password: str = ""
    sql_driver: str = "ODBC Driver 18 for SQL Server"
    ia_enabled: bool = True
    ia_task_costos: str = "COSTOS_ARTICULOS"
    max_variation_pct_warning: float = 30.0
    max_variation_pct_block: float = 70.0

    @classmethod
    def from_env(cls) -> "AppConfig":
        return cls(
            sql_server=os.getenv("ALFA_SQL_SERVER", "").strip(),
            sql_database=os.getenv("ALFA_SQL_DATABASE", "").strip(),
            sql_user=os.getenv("ALFA_SQL_USER", "").strip(),
            sql_password=os.getenv("ALFA_SQL_PASSWORD", "").strip(),
            sql_driver=os.getenv("ALFA_SQL_DRIVER", "ODBC Driver 18 for SQL Server").strip(),
            ia_enabled=os.getenv("ALFA_IA_ENABLED", "1").strip() not in {"0", "false", "False"},
            ia_task_costos=os.getenv("ALFA_IA_TASK_COSTOS", "COSTOS_ARTICULOS").strip(),
            max_variation_pct_warning=float(os.getenv("ALFA_VARIATION_WARNING_PCT", "30")),
            max_variation_pct_block=float(os.getenv("ALFA_VARIATION_BLOCK_PCT", "70")),
        )

    @classmethod
    def from_file(cls, path: Path) -> "AppConfig":
        if not path.exists():
            return cls()
        data = json.loads(path.read_text(encoding="utf-8"))
        return cls(
            sql_server=str(data.get("sql_server", "")).strip(),
            sql_database=str(data.get("sql_database", "")).strip(),
            sql_user=str(data.get("sql_user", "")).strip(),
            sql_password=str(data.get("sql_password", "")).strip(),
            sql_driver=str(data.get("sql_driver", "ODBC Driver 18 for SQL Server")).strip(),
            ia_enabled=bool(data.get("ia_enabled", True)),
            ia_task_costos=str(data.get("ia_task_costos", "COSTOS_ARTICULOS")).strip(),
            max_variation_pct_warning=float(data.get("max_variation_pct_warning", 30.0)),
            max_variation_pct_block=float(data.get("max_variation_pct_block", 70.0)),
        )

    def to_file(self, path: Path) -> None:
        payload = {
            "sql_server": self.sql_server,
            "sql_database": self.sql_database,
            "sql_user": self.sql_user,
            "sql_password": self.sql_password,
            "sql_driver": self.sql_driver,
            "ia_enabled": self.ia_enabled,
            "ia_task_costos": self.ia_task_costos,
            "max_variation_pct_warning": self.max_variation_pct_warning,
            "max_variation_pct_block": self.max_variation_pct_block,
        }
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def is_sql_configured(self) -> bool:
        return all(
            [
                self.sql_server.strip(),
                self.sql_database.strip(),
                self.sql_user.strip(),
                self.sql_password.strip(),
                self.sql_driver.strip(),
            ]
        )
