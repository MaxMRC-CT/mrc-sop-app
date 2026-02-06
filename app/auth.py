import hashlib
import os
import secrets
from dataclasses import dataclass
from typing import Optional

from fastapi import Request

from .db import get_connection, get_db, get_db


@dataclass
class User:
    id: int
    username: str
    role: str
    staff_id: Optional[int]
    must_reset_password: int
    active: int


def _hash_password(password: str, salt: str) -> str:
    dk = hashlib.pbkdf2_hmac("sha256", password.encode(), salt.encode(), 100_000)
    return dk.hex()


def create_password_hash(password: str) -> str:
    salt = secrets.token_hex(8)
    return f"{salt}${_hash_password(password, salt)}"


def verify_password(password: str, password_hash: str) -> bool:
    try:
        salt, hashed = password_hash.split("$", 1)
    except ValueError:
        return False
    return _hash_password(password, salt) == hashed


def authenticate(username: str, password: str) -> Optional[User]:
    with get_db() as conn:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT id, username, password_hash, role, staff_id, must_reset_password, active FROM users WHERE username = ?",
            (username,),
        ).fetchone()
        if not row or row["active"] != 1:
            return None
        if not verify_password(password, row["password_hash"]):
            return None
        return User(
            id=row["id"],
            username=row["username"],
            role=row["role"],
            staff_id=row["staff_id"],
            must_reset_password=row["must_reset_password"],
            active=row["active"],
        )


def get_current_user(request: Request) -> Optional[User]:
    user_id = request.session.get("user_id")
    if not user_id:
        return None
    with get_db() as conn:
        cur = conn.cursor()
        row = cur.execute(
            "SELECT id, username, role, staff_id, must_reset_password, active FROM users WHERE id = ?",
            (user_id,),
        ).fetchone()
        if not row or row["active"] != 1:
            return None
        return User(
            id=row["id"],
            username=row["username"],
            role=row["role"],
            staff_id=row["staff_id"],
            must_reset_password=row["must_reset_password"],
            active=row["active"],
        )


def require_login(request: Request) -> User:
    user = get_current_user(request)
    if not user:
        raise PermissionError("login_required")
    return user


def require_admin(request: Request) -> User:
    user = require_login(request)
    if user.role != "admin":
        raise PermissionError("admin_required")
    return user


def ensure_default_admin() -> None:
    username = os.getenv("MRC_ADMIN_USERNAME", "admin")
    password = os.getenv("MRC_ADMIN_PASSWORD", "ChangeMe123!")

    with get_db() as conn:
        cur = conn.cursor()
        exists = cur.execute("SELECT id FROM users WHERE username = ?", (username,)).fetchone()
        if not exists:
            cur.execute(
                "INSERT INTO users (username, password_hash, role) VALUES (?, ?, 'admin')",
                (username, create_password_hash(password)),
            )
            conn.commit()
