"""SQLite database for user management."""
import hashlib
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, Dict

DB_PATH = Path(__file__).parent.parent / "data" / "users.db"


def _connect():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with _connect() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT UNIQUE NOT NULL,
                password_hash TEXT NOT NULL,
                salt TEXT NOT NULL,
                token_budget INTEGER NOT NULL DEFAULT 1000000,
                token_used INTEGER NOT NULL DEFAULT 0,
                own_api_key_enc TEXT,
                own_base_url TEXT,
                created_at TEXT NOT NULL
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                jti TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                expires_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        """)
        conn.commit()


def _hash_password(salt_hex: str, password: str) -> str:
    return hashlib.sha256((salt_hex + password).encode()).hexdigest()


def create_user(username: str, password: str) -> Optional[Dict]:
    salt = os.urandom(16).hex()
    pw_hash = _hash_password(salt, password)
    created_at = datetime.now(timezone.utc).isoformat()
    try:
        with _connect() as conn:
            cur = conn.execute(
                "INSERT INTO users (username, password_hash, salt, created_at) VALUES (?, ?, ?, ?)",
                (username, pw_hash, salt, created_at),
            )
            conn.commit()
            return get_user_by_id(cur.lastrowid)
    except sqlite3.IntegrityError:
        return None  # username already exists


def get_user_by_username(username: str) -> Optional[Dict]:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
        return dict(row) if row else None


def get_user_by_id(user_id: int) -> Optional[Dict]:
    with _connect() as conn:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
        return dict(row) if row else None


def verify_password(user: Dict, password: str) -> bool:
    expected = _hash_password(user["salt"], password)
    return expected == user["password_hash"]


def update_token_usage(user_id: int, tokens_used: int):
    with _connect() as conn:
        conn.execute(
            "UPDATE users SET token_used = token_used + ? WHERE id = ?",
            (tokens_used, user_id),
        )
        conn.commit()


def save_user_api_key(user_id: int, encrypted_key: str, base_url: str):
    with _connect() as conn:
        conn.execute(
            "UPDATE users SET own_api_key_enc = ?, own_base_url = ? WHERE id = ?",
            (encrypted_key, base_url, user_id),
        )
        conn.commit()


def clear_user_api_key(user_id: int):
    with _connect() as conn:
        conn.execute(
            "UPDATE users SET own_api_key_enc = NULL, own_base_url = NULL WHERE id = ?",
            (user_id,),
        )
        conn.commit()


def add_session(jti: str, user_id: int, expires_at: str):
    with _connect() as conn:
        conn.execute(
            "INSERT INTO sessions (jti, user_id, expires_at) VALUES (?, ?, ?)",
            (jti, user_id, expires_at),
        )
        conn.commit()


def is_session_valid(jti: str) -> bool:
    with _connect() as conn:
        row = conn.execute(
            "SELECT expires_at FROM sessions WHERE jti = ?", (jti,)
        ).fetchone()
        if not row:
            return False
        expires = datetime.fromisoformat(row["expires_at"])
        return datetime.now(timezone.utc) < expires


def delete_session(jti: str):
    with _connect() as conn:
        conn.execute("DELETE FROM sessions WHERE jti = ?", (jti,))
        conn.commit()
