"""Fernet encryption for user API keys using admin key as the secret."""
import base64
import hashlib
from cryptography.fernet import Fernet

from config import API_KEY


def _get_fernet() -> Fernet:
    key_bytes = hashlib.sha256(API_KEY.encode()).digest()
    fernet_key = base64.urlsafe_b64encode(key_bytes)
    return Fernet(fernet_key)


def encrypt_api_key(plaintext: str) -> str:
    f = _get_fernet()
    return f.encrypt(plaintext.encode()).decode()


def decrypt_api_key(ciphertext: str) -> str:
    f = _get_fernet()
    return f.decrypt(ciphertext.encode()).decode()
