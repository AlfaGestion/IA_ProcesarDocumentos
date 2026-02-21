from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
import time
import urllib.error
import urllib.request
from typing import Any, Dict, List, Optional


def backend_enabled() -> bool:
    return bool((os.getenv("IA_BACKEND_URL") or "").strip())


def _build_signature(secret: str, timestamp: str, nonce: str, body: str) -> str:
    msg = f"{timestamp}.{nonce}.{body}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def call_backend(
    *,
    content_blocks: List[Dict[str, Any]],
    model: str,
    max_output_tokens: int,
    text: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = 300,
) -> str:
    base_url = (os.getenv("IA_BACKEND_URL") or "").strip().rstrip("/")
    client_id = (os.getenv("IA_CLIENT_ID") or "").strip()
    client_secret = (os.getenv("IA_CLIENT_SECRET") or "").strip()
    route = (os.getenv("IA_BACKEND_ROUTE") or "/v1/process").strip()
    task = (os.getenv("IA_TASK") or "").strip()

    if not base_url:
        raise SystemExit("ERROR: Falta IA_BACKEND_URL para usar backend remoto.")
    if not client_id or not client_secret:
        raise SystemExit("ERROR: Faltan IA_CLIENT_ID / IA_CLIENT_SECRET para usar backend remoto.")
    if not route.startswith("/"):
        route = "/" + route

    payload: Dict[str, Any] = {
        "model": model,
        "max_output_tokens": int(max_output_tokens),
        "input": [{"role": "user", "content": content_blocks}],
    }
    if text is not None:
        payload["text"] = text
    if task:
        payload["task"] = task

    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    timestamp = str(int(time.time()))
    nonce = secrets.token_hex(16)
    signature = _build_signature(client_secret, timestamp, nonce, body)

    req = urllib.request.Request(
        url=f"{base_url}{route}",
        data=body.encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "X-IA-Client-Id": client_id,
            "X-IA-Timestamp": timestamp,
            "X-IA-Nonce": nonce,
            "X-IA-Signature": signature,
        },
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            resp_body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise SystemExit(f"ERROR backend HTTP {e.code}: {detail}") from e
    except Exception as e:
        raise SystemExit(f"ERROR backend no disponible: {e}") from e

    try:
        data = json.loads(resp_body)
    except Exception as e:
        raise SystemExit("ERROR backend: respuesta no es JSON válido.") from e

    if not isinstance(data, dict):
        raise SystemExit("ERROR backend: formato de respuesta inválido.")
    if data.get("ok") is False:
        raise SystemExit(f"ERROR backend: {data.get('error') or 'sin detalle'}")

    out_text = (data.get("output_text") or "").strip()
    if not out_text:
        raise SystemExit("ERROR backend: respuesta vacía del modelo.")
    return out_text

