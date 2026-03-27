from __future__ import annotations

import hmac
import json
import secrets
from urllib.parse import urljoin, urlparse

from flask import request, session


def get_admin_csrf_token() -> str:
    token = session.get("admin_csrf_token")
    if not token:
        token = secrets.token_urlsafe(32)
        session["admin_csrf_token"] = token
    return str(token)


def rotate_admin_csrf_token() -> str:
    token = secrets.token_urlsafe(32)
    session["admin_csrf_token"] = token
    return token


def has_valid_admin_csrf_token() -> bool:
    expected = session.get("admin_csrf_token")
    provided = request.headers.get("X-CSRF-Token") or request.form.get("csrf_token")
    if not expected or not provided:
        return False
    try:
        return hmac.compare_digest(str(expected), str(provided))
    except Exception:
        return False


def is_safe_redirect_target(target: str | None) -> bool:
    if not target:
        return False
    try:
        ref = urlparse(request.host_url)
        test = urlparse(urljoin(request.host_url, target))
    except Exception:
        return False
    if test.scheme not in {"http", "https"}:
        return False
    return test.netloc == ref.netloc


def log_security_event(logger, event: str, **fields) -> None:
    payload = {"event": event, **fields}
    try:
        logger.info("security_event=%s", json.dumps(payload, sort_keys=True, default=str))
    except Exception:
        logger.info("security_event=%s payload=%r", event, payload)