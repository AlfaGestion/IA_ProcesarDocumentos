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


DEFAULT_IA_BACKEND_URL = "http://alfanetac.ddns.net:8805"
DEFAULT_IA_BACKEND_ROUTE = "/v1/process"
DEFAULT_IA_CLIENT_ID = "cliente_demo"
DEFAULT_IA_CLIENT_SECRET = "cambiar_por_secreto_largo"


def backend_enabled() -> bool:
    transport = _resolve_transport()
    if transport == "openai":
        return bool((os.getenv("OPENAI_API_KEY") or "").strip())
    base_url = _get_backend_base_url()
    return bool(base_url)


def _resolve_transport() -> str:
    raw = (os.getenv("IA_TRANSPORT") or "").strip().lower()
    if raw in {"backend", "openai"}:
        return raw

    if (os.getenv("IA_BACKEND_URL") or "").strip():
        return "backend"
    if (os.getenv("OPENAI_API_KEY") or "").strip():
        return "openai"
    return "backend"


def _get_backend_base_url() -> str:
    return ((os.getenv("IA_BACKEND_URL") or "").strip() or DEFAULT_IA_BACKEND_URL).rstrip("/")


def _has_openai_key() -> bool:
    return bool((os.getenv("OPENAI_API_KEY") or "").strip())


def _build_signature(secret: str, timestamp: str, nonce: str, body: str) -> str:
    msg = f"{timestamp}.{nonce}.{body}".encode("utf-8")
    return hmac.new(secret.encode("utf-8"), msg, hashlib.sha256).hexdigest()


def _infer_source_filename(content_blocks: List[Dict[str, Any]]) -> str:
    for block in content_blocks or []:
        if not isinstance(block, dict):
            continue
        name = (block.get("filename") or "").strip()
        if name:
            return name
    return ""


def call_backend(
    *,
    content_blocks: List[Dict[str, Any]],
    model: str,
    max_output_tokens: int,
    text: Optional[Dict[str, Any]] = None,
    source_filename: Optional[str] = None,
    timeout_seconds: int = 300,
) -> str:
    transport = _resolve_transport()

    if transport == "openai":
        return _call_openai_direct(
            content_blocks=content_blocks,
            model=model,
            max_output_tokens=max_output_tokens,
            text=text,
            timeout_seconds=timeout_seconds,
        )

    base_url = _get_backend_base_url()
    client_id = (os.getenv("IA_CLIENT_ID") or "").strip() or DEFAULT_IA_CLIENT_ID
    client_secret = (os.getenv("IA_CLIENT_SECRET") or "").strip() or DEFAULT_IA_CLIENT_SECRET
    route = (os.getenv("IA_BACKEND_ROUTE") or "").strip() or DEFAULT_IA_BACKEND_ROUTE
    task = (os.getenv("IA_TASK") or "").strip().upper()
    idcliente = (os.getenv("IA_IDCLIENTE") or os.getenv("IDCLIENTE") or "").strip()
    source_filename = (source_filename or "").strip() or _infer_source_filename(content_blocks)

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
        payload["opcion"] = task
    if idcliente:
        payload["idcliente"] = idcliente
    if source_filename:
        payload["source_filename"] = source_filename
        payload["archivo_nombre"] = source_filename
        payload["filename"] = source_filename
        payload["archivoNombre"] = source_filename
        payload["file_name"] = source_filename

    body = json.dumps(payload, ensure_ascii=False, separators=(",", ":"))
    timestamp = str(int(time.time()))
    nonce = secrets.token_hex(16)
    signature = _build_signature(client_secret, timestamp, nonce, body)

    headers = {
        "Content-Type": "application/json; charset=utf-8",
        "X-IA-Client-Id": client_id,
        "X-IA-Timestamp": timestamp,
        "X-IA-Nonce": nonce,
        "X-IA-Signature": signature,
    }
    if task:
        headers["X-IA-Task"] = task
        headers["X-IA-Opcion"] = task
    if idcliente:
        headers["X-IA-IdCliente"] = idcliente
    if source_filename:
        headers["X-IA-Source-Filename"] = source_filename
        headers["X-IA-Archivo-Nombre"] = source_filename

    req = urllib.request.Request(
        url=f"{base_url}{route}",
        data=body.encode("utf-8"),
        method="POST",
        headers=headers,
    )

    try:
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            resp_body = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        detail = e.read().decode("utf-8", errors="replace")
        raise SystemExit(f"ERROR backend HTTP {e.code}: {detail}") from e
    except Exception as e:
        if _has_openai_key():
            return _call_openai_direct(
                content_blocks=content_blocks,
                model=model,
                max_output_tokens=max_output_tokens,
                text=text,
                timeout_seconds=timeout_seconds,
            )
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


def _call_openai_direct(
    *,
    content_blocks: List[Dict[str, Any]],
    model: str,
    max_output_tokens: int,
    text: Optional[Dict[str, Any]] = None,
    timeout_seconds: int = 300,
) -> str:
    try:
        from openai import OpenAI
    except Exception as e:
        raise SystemExit(
            "ERROR transporte IA: el backend remoto no responde y no se pudo cargar el cliente openai. "
            "Instalá el paquete 'openai' o configurá IA_BACKEND_URL."
        ) from e

    if not _has_openai_key():
        raise SystemExit("ERROR transporte IA: falta OPENAI_API_KEY para usar OpenAI directo.")

    client = OpenAI(timeout=timeout_seconds, max_retries=0)
    try:
        response = client.responses.create(
            model=model,
            input=[{"role": "user", "content": content_blocks}],
            max_output_tokens=int(max_output_tokens),
            text=text,
        )
    except Exception as e:
        raise SystemExit(f"ERROR OpenAI directo: {e}") from e

    out_text = getattr(response, "output_text", "") or ""
    out_text = out_text.strip()
    if not out_text:
        raise SystemExit("ERROR OpenAI directo: respuesta vacía del modelo.")
    return out_text

