"""Pure-stdlib HS256 JWT implementation."""
import base64
import hashlib
import hmac
import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional, Dict

_SECRET_PATH = Path(__file__).parent.parent / "data" / "jwt_secret.key"
_TOKEN_EXPIRY_DAYS = 7


def _load_secret() -> bytes:
    _SECRET_PATH.parent.mkdir(parents=True, exist_ok=True)
    if _SECRET_PATH.exists():
        return _SECRET_PATH.read_bytes()
    secret = os.urandom(32)
    _SECRET_PATH.write_bytes(secret)
    return secret


_SECRET = _load_secret()


def _b64url_encode(data: bytes) -> str:
    return base64.urlsafe_b64encode(data).rstrip(b"=").decode()


def _b64url_decode(s: str) -> bytes:
    padding = 4 - len(s) % 4
    if padding != 4:
        s += "=" * padding
    return base64.urlsafe_b64decode(s)


def create_token(user_id: int, username: str) -> str:
    header = _b64url_encode(json.dumps({"alg": "HS256", "typ": "JWT"}).encode())
    now = datetime.now(timezone.utc)
    exp = now + timedelta(days=_TOKEN_EXPIRY_DAYS)
    payload = {
        "sub": str(user_id),
        "username": username,
        "jti": str(uuid.uuid4()),
        "iat": int(now.timestamp()),
        "exp": int(exp.timestamp()),
    }
    payload_enc = _b64url_encode(json.dumps(payload).encode())
    signing_input = f"{header}.{payload_enc}"
    sig = hmac.new(_SECRET, signing_input.encode(), hashlib.sha256).digest()
    return f"{signing_input}.{_b64url_encode(sig)}"


def verify_token(token: str) -> Optional[Dict]:
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return None
        header, payload_enc, sig_enc = parts
        signing_input = f"{header}.{payload_enc}"
        expected_sig = hmac.new(_SECRET, signing_input.encode(), hashlib.sha256).digest()
        if not hmac.compare_digest(expected_sig, _b64url_decode(sig_enc)):
            return None
        payload = json.loads(_b64url_decode(payload_enc))
        exp = payload.get("exp", 0)
        if datetime.now(timezone.utc).timestamp() > exp:
            return None
        return payload
    except Exception:
        return None
