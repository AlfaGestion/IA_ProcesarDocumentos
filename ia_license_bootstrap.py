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


# Motivo legible por el que no se pudieron obtener credenciales (vacio si no hubo problema).
# ia_backend_transport lo muestra en vez de seguir con un cliente de ejemplo que el servidor rechaza.
ULTIMO_MOTIVO: str = ""


def _motivo(texto: str) -> None:
    global ULTIMO_MOTIVO
    ULTIMO_MOTIVO = texto


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
        _motivo("No estan configurados los datos de conexion al SQL local (SQL_SERVER, SQL_USER, "
                "SQL_PASSWORD y SQL_DRIVER) para leer la licencia de esta instalacion.")
        return None

    try:
        import pyodbc  # type: ignore
    except Exception:
        _motivo("Falta el componente pyodbc para leer la licencia de esta instalacion.")
        return None

    server = (os.getenv("SQL_SERVER") or "").strip()
    try:
        with pyodbc.connect(conn_str, timeout=5) as conn:
            cur = conn.cursor()
            cur.execute("SELECT TOP (1) VALOR FROM dbo.TA_CONFIGURACION WHERE CLAVE = 'LICENCIAPRINCIPAL';")
            row = cur.fetchone()
            if row and row[0]:
                value = str(row[0]).strip()
                if value:
                    return value
    except Exception as e:
        _motivo(f"No se pudo leer la licencia en la base NW_ESTADISTICAS del servidor {server}: {e}")
        return None

    _motivo(f"No esta cargada la licencia (LICENCIAPRINCIPAL) en NW_ESTADISTICAS del servidor {server}.")
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
    except urllib.error.HTTPError as e:
        try:
            error = str(json.loads(e.read().decode("utf-8", errors="replace")).get("error") or "")
        except Exception:
            error = ""
        if error == "unknown_license":
            _motivo(f"La licencia {licencia} de esta instalacion no esta registrada en ALFA_CENTRAL "
                    "(ficha del cliente, campo LicenciaPrincipal). Hay que cargarla para usar la IA.")
        else:
            _motivo(f"AlfaCore ({base_url}) no entrego las credenciales de IA: HTTP {e.code} {error}".strip())
        return None
    except Exception as e:
        _motivo(f"No se pudo contactar a AlfaCore ({base_url}) para obtener las credenciales de IA: {e}")
        return None

    if not isinstance(data, dict) or not data.get("ok"):
        _motivo(f"AlfaCore ({base_url}) no entrego las credenciales de IA para la licencia {licencia}.")
        return None

    idcliente = str(data.get("idcliente") or "").strip()
    key_ia = str(data.get("keyIa") or "").strip()
    if not idcliente or not key_ia:
        _motivo(f"AlfaCore ({base_url}) devolvio credenciales incompletas para la licencia {licencia}.")
        return None
    return idcliente, key_ia


def bootstrap_credentials_if_needed() -> None:
    # No pisa configuración explícita (.env, --client-id/--client-secret).
    if (os.getenv("IA_CLIENT_ID") or "").strip() or (os.getenv("IA_CLIENT_SECRET") or "").strip():
        return

    # La licencia la puede pasar el sistema (compras) en IA_LICENCIA_PRINCIPAL: asi no hace falta
    # pyodbc, ni el .env con los datos del SQL, ni el driver ODBC para leerla de NW_ESTADISTICAS.
    licencia = (os.getenv("IA_LICENCIA_PRINCIPAL") or "").strip() or resolve_local_licencia_principal()
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
