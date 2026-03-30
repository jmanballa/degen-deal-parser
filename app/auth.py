from __future__ import annotations

from datetime import datetime
from hashlib import pbkdf2_hmac
import hmac
from typing import Optional

from sqlmodel import Session, select

from .config import get_settings
from .models import User, utcnow


settings = get_settings()


def hash_password(password: str) -> str:
    digest = pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        settings.session_secret.encode("utf-8"),
        200_000,
    )
    return digest.hex()


def verify_password(password: str, password_hash: str) -> bool:
    return hmac.compare_digest(hash_password(password), password_hash)


def authenticate_user(session: Session, username: str, password: str) -> Optional[User]:
    normalized_username = (username or "").strip().lower()
    if not normalized_username or not password:
        return None

    user = session.exec(
        select(User).where(User.username == normalized_username)
    ).first()
    if not user or not user.is_active:
        return None
    if not verify_password(password, user.password_hash):
        return None
    return user


def upsert_seed_user(
    session: Session,
    *,
    username: str,
    password: str,
    display_name: str,
    role: str,
) -> None:
    normalized_username = (username or "").strip().lower()
    if not normalized_username or not password:
        return

    existing = session.exec(
        select(User).where(User.username == normalized_username)
    ).first()
    now = utcnow()

    if existing:
        changed = False
        if settings.auth_reseed_passwords and existing.password_hash != hash_password(password):
            existing.password_hash = hash_password(password)
            changed = True
        if existing.display_name != display_name:
            existing.display_name = display_name
            changed = True
        if existing.role != role:
            existing.role = role
            changed = True
        if not existing.is_active:
            existing.is_active = True
            changed = True
        if changed:
            existing.updated_at = now
            session.add(existing)
        return

    session.add(
        User(
            username=normalized_username,
            password_hash=hash_password(password),
            display_name=display_name,
            role=role,
            is_active=True,
            created_at=now,
            updated_at=now,
        )
    )


def seed_default_users(session: Session) -> None:
    upsert_seed_user(
        session,
        username=settings.admin_username,
        password=settings.admin_password,
        display_name=settings.admin_display_name,
        role="admin",
    )
    if settings.reviewer_username and settings.reviewer_password:
        upsert_seed_user(
            session,
            username=settings.reviewer_username,
            password=settings.reviewer_password,
            display_name=settings.reviewer_display_name,
            role="reviewer",
        )
    session.commit()


def role_rank(role: Optional[str]) -> int:
    return {
        "viewer": 1,
        "reviewer": 2,
        "admin": 3,
    }.get(role or "", 0)


def has_role(user: Optional[User], minimum_role: str) -> bool:
    return role_rank(user.role if user else None) >= role_rank(minimum_role)
