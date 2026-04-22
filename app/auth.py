from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
from hashlib import pbkdf2_hmac
import hmac
import json
import secrets
from typing import Optional

import bcrypt
from sqlmodel import Session, select

from .config import get_settings
from .models import (
    AuditLog,
    EmployeeProfile,
    InviteToken,
    PasswordResetToken,
    RolePermission,
    User,
    utcnow,
)


settings = get_settings()


def hash_password(password: str, salt: Optional[str] = None) -> tuple[str, str]:
    salt_used = salt if salt is not None else secrets.token_hex(16)
    digest = pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt_used.encode("utf-8"),
        200_000,
    )
    return (digest.hex(), salt_used)


def verify_password(
    password: str,
    password_hash: str,
    *,
    salt: Optional[str] = None,
) -> bool:
    if salt:
        salt_bytes = salt.encode("utf-8")
    else:
        salt_bytes = settings.session_secret.encode("utf-8")
    digest = pbkdf2_hmac(
        "sha256",
        password.encode("utf-8"),
        salt_bytes,
        200_000,
    )
    return hmac.compare_digest(digest.hex(), password_hash)


def authenticate_user(
    session: Session,
    username: str,
    password: str,
    *,
    ip_address: Optional[str] = None,
) -> Optional[User]:
    normalized_username = (username or "").strip().lower()
    if not normalized_username or not password:
        _audit_login(
            session,
            target_user_id=None,
            action="login.failed",
            details={"username": normalized_username, "reason": "missing_fields"},
            ip_address=ip_address,
        )
        return None

    user = session.exec(
        select(User).where(User.username == normalized_username)
    ).first()
    if not user:
        _audit_login(
            session,
            target_user_id=None,
            action="login.failed",
            details={"username": normalized_username, "reason": "unknown_user"},
            ip_address=ip_address,
        )
        return None
    if not user.is_active:
        _audit_login(
            session,
            target_user_id=user.id,
            action="login.failed",
            details={"username": normalized_username, "reason": "inactive"},
            ip_address=ip_address,
        )
        return None
    stored_salt = (user.password_salt or "").strip()
    if stored_salt:
        ok = verify_password(password, user.password_hash, salt=stored_salt)
    else:
        ok = verify_password(password, user.password_hash)
    if not ok:
        _audit_login(
            session,
            target_user_id=user.id,
            action="login.failed",
            details={"username": normalized_username, "reason": "bad_password"},
            ip_address=ip_address,
        )
        return None
    _audit_login(
        session,
        actor_user_id=user.id,
        target_user_id=user.id,
        action="login.succeeded",
        details={"username": normalized_username},
        ip_address=ip_address,
    )
    return user


def _audit_login(
    session: Session,
    *,
    action: str,
    details: dict,
    target_user_id: Optional[int] = None,
    actor_user_id: Optional[int] = None,
    ip_address: Optional[str] = None,
) -> None:
    session.add(
        AuditLog(
            actor_user_id=actor_user_id,
            target_user_id=target_user_id,
            action=action,
            details_json=json.dumps(details),
            ip_address=ip_address,
        )
    )
    session.commit()


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
        if settings.auth_reseed_passwords:
            stored_salt = (existing.password_salt or "").strip()
            if stored_salt:
                matches = verify_password(password, existing.password_hash, salt=stored_salt)
            else:
                matches = verify_password(password, existing.password_hash)
            if not matches:
                h, s = hash_password(password)
                existing.password_hash = h
                existing.password_salt = s
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

    pwd_hash, pwd_salt = hash_password(password)
    session.add(
        User(
            username=normalized_username,
            password_hash=pwd_hash,
            password_salt=pwd_salt,
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
    for entry in (settings.viewer_accounts or "").split(","):
        entry = entry.strip()
        if not entry or ":" not in entry:
            continue
        parts = entry.split(":", 1)
        username = parts[0].strip()
        password = parts[1].strip()
        if username and password:
            upsert_seed_user(
                session,
                username=username,
                password=password,
                display_name=username,
                role="viewer",
            )
    session.commit()


def role_rank(role: Optional[str]) -> int:
    return {
        "employee": 1,
        "viewer": 2,
        "manager": 3,
        "reviewer": 4,
        "admin": 5,
    }.get(role or "", 0)


def has_role(user: Optional[User], minimum_role: str) -> bool:
    return role_rank(user.role if user else None) >= role_rank(minimum_role)


# ---------------------------------------------------------------------------
# Password strength (Wave 3, m4)
# ---------------------------------------------------------------------------

class WeakPasswordError(ValueError):
    """Raised when a new password fails the strength policy.

    `.problems` is a list of human-readable reasons (never empty).
    """

    def __init__(self, problems: list[str]):
        super().__init__("password_weak")
        self.problems = list(problems)


SYMBOL_CHARS: frozenset[str] = frozenset(
    "!@#$%^&*()-_=+[]{}|;:',.<>/?~`\"\\"
)


def validate_password_strength(password: str) -> list[str]:
    """Return a list of policy failures. Empty list means OK.

    Policy: min 12 chars AND at least 3 of 4 character classes
    (upper / lower / digit / symbol). Whitespace is rejected outright;
    "symbol" is restricted to printable ASCII punctuation.
    """
    problems: list[str] = []
    pwd = password or ""
    if len(pwd) < 12:
        problems.append("Password must be at least 12 characters.")
    if any(c.isspace() for c in pwd):
        problems.append("Password must not contain spaces or whitespace.")
    classes = 0
    if any(c.islower() for c in pwd):
        classes += 1
    if any(c.isupper() for c in pwd):
        classes += 1
    if any(c.isdigit() for c in pwd):
        classes += 1
    if any(c in SYMBOL_CHARS for c in pwd):
        classes += 1
    if classes < 3:
        problems.append(
            "Password must include at least 3 of: lowercase, uppercase, digit, symbol."
        )
    return problems


# ---------------------------------------------------------------------------
# Employee-portal auth extensions (Wave 1)
# ---------------------------------------------------------------------------

def has_permission(
    session: Session,
    user: Optional[User],
    resource_key: str,
    *,
    cache: Optional[dict] = None,
) -> bool:
    """Check whether `user` is allowed to access `resource_key`.

    Rules:
      * Inactive / anonymous → denied.
      * Admin short-circuit: allowed unless an explicit
        `RolePermission(role='admin', resource_key=X, is_allowed=False)` row
        exists. This keeps admin accounts from accidentally self-locking via
        the matrix UI but still honors explicit admin denies.
      * Non-admin: row.is_allowed if a row exists, else default-deny.
    """
    if user is None or not user.is_active:
        return False
    if cache is not None:
        cache_key = (user.role, resource_key)
        if cache_key in cache:
            return cache[cache_key]
    row = session.exec(
        select(RolePermission).where(
            RolePermission.role == user.role,
            RolePermission.resource_key == resource_key,
        )
    ).first()
    if user.role == "admin":
        result = True if row is None else bool(row.is_allowed)
    elif row is not None:
        result = bool(row.is_allowed)
    else:
        result = False
    if cache is not None:
        cache[(user.role, resource_key)] = result
    return result


def _hash_token(raw_token: str) -> str:
    return bcrypt.hashpw(raw_token.encode("utf-8"), bcrypt.gensalt(rounds=12)).decode("utf-8")


def _verify_token(raw_token: str, token_hash: str) -> bool:
    try:
        return bcrypt.checkpw(raw_token.encode("utf-8"), token_hash.encode("utf-8"))
    except (ValueError, TypeError):
        return False


def _token_hmac_key() -> bytes:
    # Prefer a dedicated key so rotating session cookies doesn't invalidate
    # in-flight invite/reset tokens. Fall back to SESSION_SECRET so existing
    # deployments keep working without new env vars.
    key = (settings.employee_token_hmac_key or settings.session_secret or "").encode("utf-8")
    if not key:
        raise RuntimeError("Token HMAC key is empty — set EMPLOYEE_TOKEN_HMAC_KEY or SESSION_SECRET")
    return key


def _token_lookup_hmac(raw_token: str) -> bytes:
    return hmac.new(_token_hmac_key(), raw_token.encode("utf-8"), hashlib.sha256).digest()


def _find_token_row(session: Session, model, raw_token: str):
    # O(1) HMAC indexed lookup, then single bcrypt verify on the matched row.
    # NULL-HMAC legacy rows are unreachable here and expire naturally.
    now = utcnow()
    lookup = _token_lookup_hmac(raw_token)
    row = session.exec(
        select(model).where(
            model.token_lookup_hmac == lookup,
            model.used_at.is_(None),
            model.expires_at > now,
        )
    ).first()
    if row is None:
        return None
    if not _verify_token(raw_token, row.token_hash):
        return None
    return row


def generate_invite_token(
    session: Session,
    *,
    role: str,
    created_by_user_id: int,
    email_hint: Optional[str] = None,
    ttl_hours: int = 24,
) -> str:
    raw = secrets.token_urlsafe(32)
    row = InviteToken(
        token_hash=_hash_token(raw),
        token_lookup_hmac=_token_lookup_hmac(raw),
        role=role,
        created_by_user_id=created_by_user_id,
        email_hint=email_hint,
        expires_at=utcnow() + timedelta(hours=ttl_hours),
    )
    session.add(row)
    session.commit()
    return raw


def consume_invite_token(
    session: Session,
    raw_token: str,
    *,
    new_username: str,
    new_password: str,
) -> User:
    row = _find_token_row(session, InviteToken, raw_token)
    if row is None:
        raise ValueError("invite_token_invalid")
    normalized_username = (new_username or "").strip().lower()
    if not normalized_username or not new_password:
        raise ValueError("invite_username_or_password_missing")
    problems = validate_password_strength(new_password)
    if problems:
        raise WeakPasswordError(problems)
    existing = session.exec(select(User).where(User.username == normalized_username)).first()
    if existing is not None:
        raise ValueError("invite_username_taken")

    pwd_hash, pwd_salt = hash_password(new_password)
    now = utcnow()
    user = User(
        username=normalized_username,
        password_hash=pwd_hash,
        password_salt=pwd_salt,
        display_name=normalized_username,
        role=row.role or "employee",
        is_active=True,
        created_at=now,
        updated_at=now,
    )
    session.add(user)
    session.flush()  # populate user.id

    session.add(EmployeeProfile(user_id=user.id, created_at=now, updated_at=now))
    row.used_at = now
    row.used_by_user_id = user.id
    session.add(row)
    session.add(
        AuditLog(
            actor_user_id=user.id,
            target_user_id=user.id,
            action="account.invite_accepted",
            details_json=json.dumps({"email_hint": row.email_hint}),
        )
    )
    session.commit()
    session.refresh(user)
    return user


def generate_password_reset_token(
    session: Session,
    *,
    user_id: int,
    issued_by_user_id: Optional[int] = None,
    ttl_minutes: int = 60,
) -> str:
    raw = secrets.token_urlsafe(32)
    now = utcnow()
    # m9: invalidate any prior un-used reset tokens for this user so only the
    # newest link is ever live. Keeps a tidy audit trail (used_at is set).
    prior_rows = session.exec(
        select(PasswordResetToken).where(
            PasswordResetToken.user_id == user_id,
            PasswordResetToken.used_at.is_(None),
        )
    ).all()
    for prior in prior_rows:
        prior.used_at = now
        session.add(prior)
    row = PasswordResetToken(
        token_hash=_hash_token(raw),
        token_lookup_hmac=_token_lookup_hmac(raw),
        user_id=user_id,
        expires_at=now + timedelta(minutes=ttl_minutes),
        issued_by_user_id=issued_by_user_id,
    )
    session.add(row)
    session.add(
        AuditLog(
            actor_user_id=issued_by_user_id or user_id,
            target_user_id=user_id,
            action="password.reset_requested",
            details_json=json.dumps({"source": "generate"}),
        )
    )
    session.commit()
    return raw


def consume_password_reset_token(
    session: Session,
    raw_token: str,
    *,
    new_password: str,
) -> User:
    row = _find_token_row(session, PasswordResetToken, raw_token)
    if row is None:
        raise ValueError("reset_token_invalid")
    if not new_password:
        raise ValueError("reset_password_missing")
    problems = validate_password_strength(new_password)
    if problems:
        raise WeakPasswordError(problems)
    user = session.get(User, row.user_id)
    if user is None or not user.is_active:
        raise ValueError("reset_user_inactive")
    pwd_hash, pwd_salt = hash_password(new_password)
    now = utcnow()
    user.password_hash = pwd_hash
    user.password_salt = pwd_salt
    user.updated_at = now
    row.used_at = now
    session.add(user)
    session.add(row)
    session.add(
        AuditLog(
            actor_user_id=user.id,
            target_user_id=user.id,
            action="password.reset_consumed",
            details_json="{}",
        )
    )
    session.commit()
    session.refresh(user)
    return user
