"""Regression tests for schedule-grid save data preservation.

These call the save handler directly with a fake Request object instead of
TestClient. The bug is in form payload semantics, and direct handler calls avoid
the TestClient hangs this sandbox has shown on schedule-admin routes.
"""
from __future__ import annotations

import asyncio
import json
import os
from datetime import date, timedelta
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from cryptography.fernet import Fernet
from sqlalchemy.pool import StaticPool
from sqlmodel import Session, create_engine, select

os.environ.setdefault("EMPLOYEE_PORTAL_ENABLED", "true")
os.environ.setdefault("EMPLOYEE_PII_KEY", Fernet.generate_key().decode("ascii"))
os.environ.setdefault("EMPLOYEE_EMAIL_HASH_SALT", "schedule-save-salt")
os.environ.setdefault("EMPLOYEE_TOKEN_HMAC_KEY", "schedule-save-hmac-" + "x" * 24)
os.environ.setdefault("SESSION_SECRET", "schedule-save-secret-" + "x" * 32)
os.environ.setdefault("ADMIN_PASSWORD", "schedule-save-admin-password")


WEEK = date(2026, 4, 27)


class _FakeRequest:
    def __init__(self, form: dict[str, str], current_user):
        self._form = form
        self.state = SimpleNamespace(current_user=current_user)
        self.client = SimpleNamespace(host="testclient")

    async def form(self):
        return self._form


@pytest.fixture()
def session():
    from app.models import SQLModel

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as s:
        yield s


def _seed_user(session: Session, user_id: int, *, role: str = "employee"):
    from app.models import User

    user = User(
        id=user_id,
        username=f"user{user_id}",
        password_hash="x",
        password_salt="x",
        display_name=f"User {user_id}",
        role=role,
        is_active=True,
        is_schedulable=True,
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def _seed_shift(session: Session, user_id: int, shift_date: date, label: str):
    from app.models import ShiftEntry, User, classify_shift_label

    if session.get(User, user_id) is None:
        _seed_user(session, user_id)
    row = ShiftEntry(
        user_id=user_id,
        shift_date=shift_date,
        label=label,
        kind=classify_shift_label(label),
        sort_order=0,
        created_by_user_id=999,
    )
    session.add(row)
    session.commit()
    session.refresh(row)
    return row


def _entries(session: Session, user_id: int, shift_date: date):
    from app.models import ShiftEntry

    return list(
        session.exec(
            select(ShiftEntry)
            .where(ShiftEntry.user_id == user_id)
            .where(ShiftEntry.shift_date == shift_date)
            .order_by(ShiftEntry.sort_order, ShiftEntry.id)
        ).all()
    )


def _save(session: Session, admin, fields: dict[str, str]):
    from app.models import STAFF_KIND_STOREFRONT
    from app.routers import team_admin_schedule as schedule

    form = {
        "week": WEEK.isoformat(),
        "staff_kind": STAFF_KIND_STOREFRONT,
        **fields,
    }
    request = _FakeRequest(form, admin)
    with patch.object(schedule, "_permission_gate", return_value=(None, admin)):
        return asyncio.run(schedule.admin_schedule_save(request, session))


def test_absent_cell_key_preserves_existing_shift(session: Session):
    from app.routers.team_admin_schedule import _build_cell_key

    admin = _seed_user(session, 999, role="admin")
    shift_date = WEEK
    _seed_shift(session, 5, shift_date, "10-6")

    response = _save(session, admin, {})

    assert response.status_code == 303
    assert _build_cell_key(5, shift_date) not in {}
    rows = _entries(session, 5, shift_date)
    assert [row.label for row in rows] == ["10-6"]


def test_empty_cell_key_without_clear_marker_preserves_shift(session: Session):
    from app.routers.team_admin_schedule import _build_cell_key

    admin = _seed_user(session, 999, role="admin")
    shift_date = WEEK
    key = _build_cell_key(5, shift_date)
    _seed_shift(session, 5, shift_date, "10-6")

    _save(session, admin, {key: "", "cleared_cells": "[]"})

    rows = _entries(session, 5, shift_date)
    assert [row.label for row in rows] == ["10-6"]


def test_empty_cell_key_with_clear_marker_deletes_shift(session: Session):
    from app.routers.team_admin_schedule import _build_cell_key

    admin = _seed_user(session, 999, role="admin")
    shift_date = WEEK
    key = _build_cell_key(5, shift_date)
    _seed_shift(session, 5, shift_date, "10-6")

    _save(
        session,
        admin,
        {key: "", "cleared_cells": json.dumps([f"5__{shift_date.isoformat()}"])},
    )

    assert _entries(session, 5, shift_date) == []


def test_nonempty_cell_key_updates_shift(session: Session):
    from app.routers.team_admin_schedule import _build_cell_key

    admin = _seed_user(session, 999, role="admin")
    shift_date = WEEK
    key = _build_cell_key(5, shift_date)
    _seed_shift(session, 5, shift_date, "10-6")

    _save(session, admin, {key: "12-8", "cleared_cells": "[]"})

    rows = _entries(session, 5, shift_date)
    assert [row.label for row in rows] == ["12-8"]


def test_full_week_save_with_one_edit_preserves_four_untouched(session: Session):
    from app.routers.team_admin_schedule import _build_cell_key

    admin = _seed_user(session, 999, role="admin")
    untouched = []
    for offset, user_id in enumerate(range(1, 6)):
        shift_date = WEEK + timedelta(days=offset)
        label = f"original-{user_id}"
        _seed_shift(session, user_id, shift_date, label)
        if user_id != 1:
            untouched.append((user_id, shift_date, label))

    edited_key = _build_cell_key(1, WEEK)
    _save(session, admin, {edited_key: "edited-one-cell", "cleared_cells": "[]"})

    assert [row.label for row in _entries(session, 1, WEEK)] == ["edited-one-cell"]
    for user_id, shift_date, label in untouched:
        rows = _entries(session, user_id, shift_date)
        assert [row.label for row in rows] == [label], (
            "untouched cells must not be deleted when one schedule cell is edited"
        )
