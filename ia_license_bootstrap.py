"""Resuelve IA_CLIENT_ID/IA_CLIENT_SECRET en memoria a partir de la licencia que cada instalación
de Alfa Gestión ya tiene grabada localmente -- nunca hay que repartir ni guardar un secreto de IA
por cliente en ningún .env.

Flujo:
1. Lee NW_ESTADISTICAS.dbo.TA_CONFIGURACION.VALOR donde CLAVE='LICENCIAPRINCIPAL' en el SQL Server
   local de esta instalación (mismas credenciales SQL_* que ya usa el resto del proyecto, solo
   cambia la base).
2. Manda esa licencia a POST {IA_BACKEND_URL}/v1/credentials -- AlfaCore resuelve el idcliente real
   y su KeyIa (generándolo la primera vez que hace falta) contra ALFA_CENTRAL.
3. Deja IA_CLIENT_ID/IA_CLIENT_SECRET seteados en os.environ para el resto de esta ejecución, nunca
   escritos a disco.

Cualquier paso que falle (sin pyodbc, sin base NW_ESTADISTICAS, sin fila, backend no disponible,
licencia desconocida) se degrada en silencio -- el resto del programa sigue funcionando exactamente
como si este módulo no existiera.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Optional


def _sql_conn_str_nw_estadisticas() -> str:
    user = (os.getenv("SQL_USER") or "").strip()
    pwd = (os.getenv("SQL_PASSWORD") or "").strip()
    server = (os.getenv("SQL_SERVER") or "").strip()
    driver = (os.getenv("SQL_DRIVER") or "").strip()
    if not all([user, pwd, server, driver]):
        return ""
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        "DATABASE=NW_ESTADISTICAS;"
        f"UID={user};"
        f"PWD={pwd};"
        "TrustServerCertificate=yes;"
    )


def resolve_local_licencia_principal() -> Optional[str]:
    conn_str = _sql_conn_str_nw_estadisticas()
    if not conn_str:
        return None

    try:
        import pyodbc  # type: ignore
    except Exception:
        return None

    try:
        with pyodbc.connect(conn_str, timeout=5) as conn:
            cur = conn.cursor()
            cur.execute("SELECT TOP (1) VALOR FROM dbo.TA_CONFIGURACION WHERE CLAVE = 'LICENCIAPRINCIPAL';")
            row = cur.fetchone()
            if row and row[0]:
                value = str(row[0]).strip()
                return value or None
    except Exception:
        return None

    return None


def _fetch_credentials(base_url: str, licencia: str, timeout_seconds: int = 10) -> Optional[tuple[str, str]]:
    body = json.dumps({"licenciaPrincipal": licencia}, ensure_ascii=False).encode("utf-8")
    req = urllib.request.Request(
        url=f"{base_url.rstrip('/')}/v1/credentials",
        data=body,
        method="POST",
        headers={"Content-Type": "application/json; charset=utf-8"},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception:
        return None

    if not isinstance(data, dict) or not data.get("ok"):
        return None

    idcliente = str(data.get("idcliente") or "").strip()
    key_ia = str(data.get("keyIa") or "").strip()
    if not idcliente or not key_ia:
        return None
    return idcliente, key_ia


def bootstrap_credentials_if_needed() -> None:
    # No pisa configuración explícita (.env, --client-id/--client-secret).
    if (os.getenv("IA_CLIENT_ID") or "").strip() or (os.getenv("IA_CLIENT_SECRET") or "").strip():
        return

    licencia = resolve_local_licencia_principal()
    if not licencia:
        return

    base_url = (os.getenv("IA_BACKEND_URL") or "").strip()
    if not base_url:
        # Mismo default que ia_backend_transport.DEFAULT_IA_BACKEND_URL -- se importa perezosamente
        # para evitar un ciclo de imports (ia_backend_transport importa este módulo).
        from ia_backend_transport import DEFAULT_IA_BACKEND_URL
        base_url = DEFAULT_IA_BACKEND_URL

    resolved = _fetch_credentials(base_url, licencia)
    if not resolved:
        return

    idcliente, key_ia = resolved
    os.environ["IA_CLIENT_ID"] = idcliente
    os.environ["IA_CLIENT_SECRET"] = key_ia
