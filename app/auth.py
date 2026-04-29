from __future__ import annotations

from datetime import datetime, timedelta
import hashlib
from hashlib import pbkdf2_hmac
import hmac
import json
import logging
import secrets
from typing import Optional

import bcrypt
from sqlalchemy import update
from sqlalchemy.exc import IntegrityError
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
log = logging.getLogger(__name__)

LEGACY_OPS_PERMISSION = "legacy.ops.view"
EMPLOYEE_PORTAL_ROLES = {"employee", "viewer", "manager"}
LEGACY_VIEWER_ROLES = {"admin", "reviewer"}
LEGACY_REVIEWER_ROLES = {"admin", "reviewer"}


class AuthError(RuntimeError):
    """Raised when an account cannot be authenticated safely."""


class LoginRateLimitedError(AuthError):
    """Raised when a login attempt hits a request-scoped throttle."""

    def __init__(self, response):
        super().__init__("login_rate_limited")
        self.response = response


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
    if not salt:
        raise AuthError("account requires re-migration")
    salt_bytes = salt.encode("utf-8")
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
    request=None,
    ip_address: Optional[str] = None,
) -> Optional[User]:
    normalized_username = (username or "").strip().lower()
    if normalized_username and request is not None:
        from .rate_limit import rate_limited_or_429

        if limited := rate_limited_or_429(
            request,
            key_prefix=f"team:login:user:{normalized_username}",
            max_requests=10,
            window_seconds=900,
        ):
            _audit_login(
                session,
                target_user_id=None,
                action="login.rate_limited",
                details={"username": normalized_username, "reason": "username_bucket"},
                ip_address=ip_address,
            )
            raise LoginRateLimitedError(limited)
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
    try:
        ok = verify_password(password, user.password_hash, salt=stored_salt)
    except AuthError:
        _audit_login(
            session,
            target_user_id=user.id,
            action="login.failed",
            details={"username": normalized_username, "reason": "missing_password_salt"},
            ip_address=ip_address,
        )
        return None
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
            try:
                matches = verify_password(password, existing.password_hash, salt=stored_salt)
            except AuthError:
                matches = False
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
    # Portal permissions are checked with explicit RolePermission rows.
    # Legacy ops routes use has_legacy_role() so portal managers/viewers do
    # not inherit old reviewer/viewer route gates from this compatibility rank.
    return {
        "employee": 1,
        "viewer": 2,
        "manager": 4,
        "reviewer": 4,
        "admin": 5,
    }.get(role or "", 0)


def has_role(user: Optional[User], minimum_role: str) -> bool:
    return role_rank(user.role if user else None) >= role_rank(minimum_role)


def has_legacy_role(
    session: Optional[Session],
    user: Optional[User],
    minimum_role: str,
    *,
    cache: Optional[dict] = None,
) -> bool:
    """Authorize old ops/admin pages without treating portal roles as ops roles.

    The employee portal reuses persisted role names like "viewer" and "manager".
    Legacy ops routes historically used rank checks such as minimum_role="viewer";
    this helper keeps employee-portal roles out of those pages unless a specific
    RolePermission grant exists.
    """
    if user is None or not user.is_active:
        return False
    role = (user.role or "").strip().lower()
    minimum = (minimum_role or "").strip().lower()
    if minimum == "employee":
        return has_role(user, "employee")
    if minimum == "admin":
        return role == "admin"
    if minimum == "reviewer":
        return role in LEGACY_REVIEWER_ROLES
    if minimum == "viewer":
        if role in LEGACY_VIEWER_ROLES:
            return True
        if session is None:
            return False
        return has_permission(session, user, LEGACY_OPS_PERMISSION, cache=cache)
    return has_role(user, minimum)


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
    key_text = (settings.employee_token_hmac_key or "").strip()
    if not key_text:
        raise ValueError("EMPLOYEE_TOKEN_HMAC_KEY must be set (distinct from SESSION_SECRET)")
    if key_text == (settings.session_secret or "").strip():
        raise ValueError("EMPLOYEE_TOKEN_HMAC_KEY must be distinct from SESSION_SECRET")
    key = key_text.encode("utf-8")
    if not key:
        raise ValueError("EMPLOYEE_TOKEN_HMAC_KEY must be set (distinct from SESSION_SECRET)")
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
    target_user_id: Optional[int] = None,
) -> str:
    """Issue a one-time invite link.

    If `target_user_id` is provided, the invite is bound to an existing
    (draft) User row — when consumed, it will hydrate that user/profile
    instead of creating a brand new one. This lets an admin create a
    placeholder employee (legal name + role) and schedule them BEFORE
    they've gone through onboarding.

    Also invalidates any prior outstanding invite for the same target so
    only one live link exists per draft.
    """
    now = utcnow()
    if target_user_id is not None:
        target = session.get(User, target_user_id)
        if target is None:
            raise ValueError("invite_target_user_not_found")
        if target.is_active and (target.password_hash or ""):
            raise ValueError("invite_target_already_registered")
        prior_rows = session.exec(
            select(InviteToken).where(
                InviteToken.target_user_id == target_user_id,
                InviteToken.used_at.is_(None),
            )
        ).all()
        for prior in prior_rows:
            prior.used_at = now
            session.add(prior)

    raw = secrets.token_urlsafe(32)
    row = InviteToken(
        token_hash=_hash_token(raw),
        token_lookup_hmac=_token_lookup_hmac(raw),
        role=role,
        created_by_user_id=created_by_user_id,
        email_hint=email_hint,
        expires_at=now + timedelta(hours=ttl_hours),
        target_user_id=target_user_id,
    )
    session.add(row)
    session.commit()
    return raw


def create_draft_employee(
    session: Session,
    *,
    created_by_user_id: int,
    display_name: Optional[str] = None,
    legal_name: Optional[str] = None,
    preferred_name: Optional[str] = None,
    role: str = "employee",
    hire_date: Optional["date"] = None,
    email: Optional[str] = None,
) -> User:
    """Create a pre-registered ("draft") employee.

    The employee exists as an INACTIVE User row with an empty password,
    plus an EmployeeProfile. The only required field at draft time is a
    *name Jeffrey knows them as* — whatever we call them on the floor.
    That goes into `display_name`. Legal name can be added later by the
    admin OR by the employee themselves during onboarding / profile
    editing (which is how payroll catches up to nicknames).

    The User gets a real, stable primary key — scheduling / supply /
    audit code can reference it immediately. When the employee later
    accepts their invite, the same row is hydrated (username + password
    + is_active=True + any PII they fill in themselves).
    """
    # Accept either `display_name` or `legal_name` for the required
    # "who is this" field. Prefer display_name; fall back to legal_name
    # for backward-compat with older callers and tests.
    display = (display_name or preferred_name or legal_name or "").strip()
    if not display:
        raise ValueError("draft_display_name_required")
    normalized_legal = (legal_name or "").strip()
    role_clean = (role or "employee").strip().lower() or "employee"

    from .pii import email_lookup_hash as _email_hash, encrypt_pii as _encrypt

    email_clean = (email or "").strip().lower() or None
    email_hash: Optional[str] = None
    if email_clean:
        email_hash = _email_hash(email_clean)

    now = utcnow()
    # Synthetic username so nothing collides; gets replaced when the
    # employee picks their real one during onboarding. The prefix is
    # intentionally ugly so it's obvious in admin UI / logs that this
    # isn't a real account yet.
    suffix = secrets.token_hex(6)
    placeholder_username = f"__draft_{suffix}__"

    user = User(
        username=placeholder_username,
        password_hash="",
        password_salt="",
        display_name=display,
        role=role_clean,
        is_active=False,
        created_at=now,
        updated_at=now,
    )
    session.add(user)
    session.flush()

    profile = EmployeeProfile(
        user_id=user.id,
        hire_date=hire_date,
        created_at=now,
        updated_at=now,
    )
    if normalized_legal:
        profile.legal_name_enc = _encrypt(normalized_legal)
    if email_clean:
        profile.email_ciphertext = _encrypt(email_clean)
        profile.email_lookup_hash = email_hash
    session.add(profile)
    session.add(
        AuditLog(
            actor_user_id=created_by_user_id,
            target_user_id=user.id,
            action="employee.draft_created",
            resource_key="admin.employees.edit",
            details_json=json.dumps(
                {
                    "role": role_clean,
                    "has_email": bool(email_clean),
                    "has_legal_name": bool(normalized_legal),
                }
            ),
        )
    )
    try:
        session.commit()
    except IntegrityError as exc:
        session.rollback()
        if email_hash:
            clash = session.exec(
                select(EmployeeProfile).where(
                    EmployeeProfile.email_lookup_hash == email_hash
                )
            ).first()
            if clash is not None:
                raise ValueError("draft_email_taken") from exc
        raise
    session.refresh(user)
    return user


def is_draft_user(user: Optional[User]) -> bool:
    """A draft is an inactive user with no password set — i.e. still
    waiting for an invite to be accepted."""
    if user is None:
        return False
    return (not user.is_active) and not (user.password_hash or "")


def migrate_empty_password_salts(session: Session) -> int:
    """Populate explicit password salts for legacy rows.

    Legacy portal users without a stored salt were verified with
    SESSION_SECRET. Without plaintext passwords we cannot re-hash them onto a
    fresh random salt during boot, so the migration persists the current
    legacy salt explicitly. That removes the runtime fallback and keeps those
    accounts working after future SESSION_SECRET rotations. Draft users with no
    password get a fresh random salt because no verifier has to be preserved.
    """
    migrated = 0
    now = utcnow()
    rows = session.exec(
        select(User).where(
            (User.password_salt == None) | (User.password_salt == "")  # noqa: E711
        )
    ).all()
    for user in rows:
        if user.password_hash:
            user.password_salt = settings.session_secret
        else:
            user.password_salt = secrets.token_hex(32)
        user.updated_at = now
        session.add(user)
        migrated += 1
    if migrated:
        session.commit()
        log.info("migrated %s users to explicit salts", migrated)
    return migrated


def consume_invite_token(
    session: Session,
    raw_token: str,
    *,
    new_username: str,
    new_password: str,
    email: Optional[str] = None,
    legal_name: Optional[str] = None,
    preferred_name: Optional[str] = None,
    phone: Optional[str] = None,
    address: Optional[dict] = None,
    emergency_contact_name: Optional[str] = None,
    emergency_contact_phone: Optional[str] = None,
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

    # Username uniqueness: for the draft/target flow, the target's existing
    # placeholder username (__draft_xxx__) is expected to be present and
    # must NOT trigger a "taken" error — it's the same row we're about to
    # rename.
    existing = session.exec(
        select(User).where(User.username == normalized_username)
    ).first()
    if existing is not None and existing.id != row.target_user_id:
        raise ValueError("invite_username_taken")

    # Lazy-import pii so auth.py stays importable when the portal is disabled
    # (pii module fails-closed at import time when EMPLOYEE_PORTAL_ENABLED=true).
    from .pii import email_lookup_hash as _email_hash, encrypt_pii as _encrypt

    email_clean = (email or "").strip().lower() or None
    email_hash: Optional[str] = None
    email_skipped_due_to_clash = False
    if email_clean:
        email_hash = _email_hash(email_clean)
        clash = session.exec(
            select(EmployeeProfile).where(EmployeeProfile.email_lookup_hash == email_hash)
        ).first()
        # For the draft flow, the admin may have seeded the same email at
        # draft-creation time. That's the SAME profile we're about to
        # update, not a real clash.
        if clash is not None and clash.user_id != row.target_user_id:
            # A different employee already owns this email. We used to
            # raise `invite_email_taken` here, but that blew up the whole
            # onboarding flow — a hostile UX when the real cause is
            # usually browser autofill pulling an email belonging to the
            # admin or another teammate who shares the device. Drop the
            # submitted email instead: the account still gets created,
            # the employee can fix their email from /team/profile later,
            # and we leave an audit trail for an admin to spot later.
            email_clean = None
            email_hash = None
            email_skipped_due_to_clash = True

    pwd_hash, pwd_salt = hash_password(new_password)
    now = utcnow()
    display = (preferred_name or "").strip() or normalized_username

    token_result = session.exec(
        update(InviteToken)
        .where(
            InviteToken.id == row.id,
            InviteToken.used_at.is_(None),
            InviteToken.expires_at > now,
        )
        .values(used_at=now)
        .execution_options(synchronize_session=False)
    )
    if int(token_result.rowcount or 0) != 1:
        session.rollback()
        raise ValueError("invite_token_invalid")

    # ---- Two paths: hydrate-existing (draft) vs create-new (classic) ----
    if row.target_user_id is not None:
        user = session.get(User, row.target_user_id)
        if user is None:
            raise ValueError("invite_target_user_missing")
        if user.is_active and (user.password_hash or ""):
            # Belt and suspenders — generate_invite_token already checks,
            # but make sure a second acceptance can't steal the account.
            raise ValueError("invite_target_already_registered")
        user.username = normalized_username
        user.password_hash = pwd_hash
        user.password_salt = pwd_salt
        user.display_name = display
        # Keep the role the admin chose at draft-time unless the invite
        # itself carries an override role (current issuance keeps these in
        # sync, but be defensive).
        user.role = row.role or user.role or "employee"
        user.is_active = True
        user.updated_at = now
        session.add(user)

        profile = session.get(EmployeeProfile, user.id)
        if profile is None:
            profile = EmployeeProfile(user_id=user.id, created_at=now)
            session.add(profile)
    else:
        user = User(
            username=normalized_username,
            password_hash=pwd_hash,
            password_salt=pwd_salt,
            display_name=display,
            role=row.role or "employee",
            is_active=True,
            created_at=now,
            updated_at=now,
        )
        session.add(user)
        session.flush()  # populate user.id
        profile = EmployeeProfile(user_id=user.id, created_at=now, updated_at=now)
        session.add(profile)

    # Apply PII — "only overwrite if caller provided something" so an
    # admin-entered legal_name at draft creation survives the employee
    # leaving it blank during onboarding.
    if legal_name and legal_name.strip():
        profile.legal_name_enc = _encrypt(legal_name.strip())
    if phone and phone.strip():
        profile.phone_enc = _encrypt(phone.strip())
    if emergency_contact_name and emergency_contact_name.strip():
        profile.emergency_contact_name_enc = _encrypt(emergency_contact_name.strip())
    if emergency_contact_phone and emergency_contact_phone.strip():
        profile.emergency_contact_phone_enc = _encrypt(emergency_contact_phone.strip())
    if address and isinstance(address, dict):
        cleaned = {k: str(v or "").strip() for k, v in address.items() if k in ("street", "city", "state", "zip")}
        if any(cleaned.values()):
            profile.address_enc = _encrypt(json.dumps(cleaned))
    if email_clean:
        profile.email_ciphertext = _encrypt(email_clean)
        profile.email_lookup_hash = email_hash
    profile.onboarding_completed_at = now
    profile.updated_at = now
    session.add(profile)

    row.used_at = now
    row.used_by_user_id = user.id
    session.add(row)

    captured = [
        label for label, present in (
            ("legal_name", bool(legal_name and legal_name.strip())),
            ("preferred_name", bool(preferred_name and preferred_name.strip())),
            ("email", bool(email_clean)),
            ("phone", bool(phone and phone.strip())),
            ("address", bool(address and any((address or {}).values()))),
            ("emergency_contact", bool(emergency_contact_name and emergency_contact_name.strip())),
        ) if present
    ]
    session.add(
        AuditLog(
            actor_user_id=user.id,
            target_user_id=user.id,
            action="account.invite_accepted",
            details_json=json.dumps({
                "email_hint": row.email_hint,
                "captured": captured,
                "email_skipped_due_to_clash": email_skipped_due_to_clash,
            }),
        )
    )
    if email_skipped_due_to_clash:
        session.add(
            AuditLog(
                actor_user_id=user.id,
                target_user_id=user.id,
                action="account.invite_email_dropped",
                details_json=json.dumps({
                    "reason": "address_already_on_file_for_another_employee",
                    "email_hint": row.email_hint,
                }),
            )
        )
    session.info["invite_email_skipped_due_to_clash"] = email_skipped_due_to_clash
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
    # m2 (wave 4.5): split self-serve vs admin-issued so audit trails are
    # unambiguous. "reset_requested" = user-initiated; "reset_issued" = an
    # admin (a different user) is generating a link on their behalf.
    if issued_by_user_id is not None and issued_by_user_id != user_id:
        audit_action = "password.reset_issued"
    else:
        audit_action = "password.reset_requested"
    session.add(
        AuditLog(
            actor_user_id=issued_by_user_id or user_id,
            target_user_id=user_id,
            action=audit_action,
            details_json=json.dumps({"source": "generate"}),
        )
    )
    session.commit()
    return raw


class BadCurrentPasswordError(ValueError):
    """Raised when the user-supplied current password doesn't match."""


def change_user_password(
    session: Session,
    user: User,
    *,
    current_password: str,
    new_password: str,
    ip_address: Optional[str] = None,
) -> User:
    """Rotate a logged-in user's password.

    This is the self-serve flow (Profile → Change password). The user
    MUST prove they know the current password — this isn't an admin
    reset. We also refuse if the new password fails strength rules or
    matches the current one (so the action actually improves security).
    Every outcome — success, bad current, weak new, same-as-old — is
    audited so an investigator can see the full story.
    """
    if not current_password:
        _audit_pw_change(session, user.id, "password.self_change_failed", {"reason": "missing_current"}, ip_address)
        raise BadCurrentPasswordError("current_password_required")
    if not new_password:
        _audit_pw_change(session, user.id, "password.self_change_failed", {"reason": "missing_new"}, ip_address)
        raise ValueError("new_password_required")

    if not verify_password(current_password, user.password_hash, salt=user.password_salt or None):
        _audit_pw_change(session, user.id, "password.self_change_failed", {"reason": "wrong_current"}, ip_address)
        raise BadCurrentPasswordError("current_password_wrong")

    problems = validate_password_strength(new_password)
    if problems:
        _audit_pw_change(session, user.id, "password.self_change_failed", {"reason": "weak_new"}, ip_address)
        raise WeakPasswordError(problems)

    # Refuse a no-op. Cheapest way to know: try to verify the *new*
    # password against the stored hash — if it matches, it's the same.
    if verify_password(new_password, user.password_hash, salt=user.password_salt or None):
        _audit_pw_change(session, user.id, "password.self_change_failed", {"reason": "same_as_current"}, ip_address)
        raise ValueError("new_password_same_as_current")

    pwd_hash, pwd_salt = hash_password(new_password)
    now = utcnow()
    user.password_hash = pwd_hash
    user.password_salt = pwd_salt
    user.password_changed_at = now
    user.session_invalidated_at = now
    user.updated_at = now
    session.add(user)
    _audit_pw_change(session, user.id, "password.self_change_succeeded", {}, ip_address)
    session.commit()
    session.refresh(user)
    return user


def _audit_pw_change(
    session: Session,
    user_id: int,
    action: str,
    details: dict,
    ip_address: Optional[str],
) -> None:
    session.add(
        AuditLog(
            actor_user_id=user_id,
            target_user_id=user_id,
            action=action,
            resource_key="team.password.change",
            details_json=json.dumps(details),
            ip_address=ip_address,
        )
    )


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
    user.password_changed_at = now
    user.session_invalidated_at = now
    user.updated_at = now
    session.exec(
        update(PasswordResetToken)
        .where(
            PasswordResetToken.user_id == user.id,
            PasswordResetToken.used_at.is_(None),
        )
        .values(used_at=now)
    )
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
