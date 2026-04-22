"""Permission helpers for the Degen Employee Portal.

Wave 2: matrix UI + widget registry. Pages/actions/widgets are all stored
uniformly in `RolePermission` keyed by a namespaced `resource_key`. Wave 1
seeds the defaults; this module exposes read/write helpers and an audit-logged
`set_permission` upsert used by the admin matrix page.
"""
from __future__ import annotations

import json
from typing import Dict, Iterable, List, Optional

from sqlmodel import Session, delete, select

from .models import AuditLog, DashboardWidget, RolePermission, utcnow


ROLES: tuple[str, ...] = ("employee", "viewer", "manager", "reviewer", "admin")

# Source-of-truth resource-key catalog. The DB seed denormalizes these;
# this constant is what the UI renders from. Keep in sync with
# `DEFAULT_ROLE_PERMISSIONS` in app/db.py.
RESOURCE_KEYS: List[str] = [
    # Pages
    "page.dashboard",
    "page.profile",
    "page.policies",
    "page.hours",
    "page.schedule",
    "page.supply_requests",
    "page.admin.employees",
    "page.admin.invites",
    "page.admin.permissions",
    "page.admin.supply",
    # Dashboard widgets (uniform treatment — same table as pages/actions)
    "widget.dashboard.hours_this_week",
    "widget.dashboard.estimated_pay",
    "widget.dashboard.todays_tasks",
    "widget.dashboard.upcoming_shifts",
    "widget.dashboard.supply_queue_count",
    # Actions
    "action.supply_request.submit",
    "action.supply_request.approve",
    "action.pii.reveal",
    "action.password.reset_issued",
    "action.employee.terminate",
    "action.employee.purge",
    # Admin meta (gate the permissions matrix itself)
    "admin.permissions.view",
    "admin.permissions.edit",
]


_LABEL_OVERRIDES: Dict[str, str] = {
    "page.dashboard": "Dashboard page",
    "page.profile": "Profile page",
    "page.policies": "Policies page",
    "page.hours": "Hours page",
    "page.schedule": "Schedule page",
    "page.supply_requests": "Supply requests page",
    "page.admin.employees": "Admin · Employees",
    "page.admin.invites": "Admin · Invites",
    "page.admin.permissions": "Admin · Permissions",
    "page.admin.supply": "Admin · Supply queue",
    "widget.dashboard.hours_this_week": "Hours this week",
    "widget.dashboard.estimated_pay": "Estimated pay",
    "widget.dashboard.todays_tasks": "Today's tasks",
    "widget.dashboard.upcoming_shifts": "Upcoming shifts",
    "widget.dashboard.supply_queue_count": "Supply queue count",
    "action.supply_request.submit": "Submit supply request",
    "action.supply_request.approve": "Approve supply request",
    "action.pii.reveal": "Reveal employee PII",
    "action.password.reset_issued": "Issue password reset",
    "action.employee.terminate": "Terminate employee",
    "action.employee.purge": "Purge employee (PII erase)",
    "admin.permissions.view": "View permissions matrix",
    "admin.permissions.edit": "Edit permissions matrix",
}


GROUP_ORDER: tuple[tuple[str, str], ...] = (
    ("page", "Pages"),
    ("widget", "Dashboard widgets"),
    ("action", "Actions"),
    ("admin", "Admin meta"),
)


def resource_label(key: str) -> str:
    if key in _LABEL_OVERRIDES:
        return _LABEL_OVERRIDES[key]
    tail = key.split(".", 1)[-1]
    return tail.replace("_", " ").replace(".", " · ").title()


def group_for(key: str) -> str:
    return key.split(".", 1)[0]


def grouped_resource_keys() -> List[tuple[str, str, List[str]]]:
    """Returns [(group_prefix, display_title, [keys...]), ...] in GROUP_ORDER."""
    buckets: Dict[str, List[str]] = {g: [] for g, _ in GROUP_ORDER}
    for key in RESOURCE_KEYS:
        g = group_for(key)
        buckets.setdefault(g, []).append(key)
    out: List[tuple[str, str, List[str]]] = []
    for prefix, title in GROUP_ORDER:
        keys = buckets.get(prefix, [])
        if keys:
            out.append((prefix, title, keys))
    return out


def permissions_matrix(session: Session) -> Dict[str, Dict[str, bool]]:
    """Returns {role: {resource_key: is_allowed}} for every role × seeded key."""
    rows = session.exec(select(RolePermission)).all()
    matrix: Dict[str, Dict[str, bool]] = {role: {} for role in ROLES}
    for row in rows:
        matrix.setdefault(row.role, {})[row.resource_key] = bool(row.is_allowed)
    # Ensure every (role, key) has an explicit entry for the UI (default False).
    for role in ROLES:
        row_map = matrix.setdefault(role, {})
        for key in RESOURCE_KEYS:
            row_map.setdefault(key, False)
    return matrix


def set_permission(
    session: Session,
    *,
    role: str,
    resource_key: str,
    is_allowed: bool,
    actor_user_id: Optional[int],
) -> RolePermission:
    """Upsert a single (role, resource_key) cell. Audit-logs every change."""
    if role not in ROLES:
        raise ValueError(f"unknown role: {role!r}")
    existing = session.exec(
        select(RolePermission).where(
            RolePermission.role == role,
            RolePermission.resource_key == resource_key,
        )
    ).first()
    prev: Optional[bool] = None
    if existing is None:
        row = RolePermission(
            role=role,
            resource_key=resource_key,
            is_allowed=bool(is_allowed),
            updated_at=utcnow(),
            updated_by_user_id=actor_user_id,
        )
        session.add(row)
    else:
        prev = bool(existing.is_allowed)
        existing.is_allowed = bool(is_allowed)
        existing.updated_at = utcnow()
        existing.updated_by_user_id = actor_user_id
        session.add(existing)
        row = existing

    session.add(
        AuditLog(
            actor_user_id=actor_user_id,
            action="permission.set",
            resource_key=resource_key,
            details_json=json.dumps(
                {
                    "role": role,
                    "resource_key": resource_key,
                    "is_allowed": bool(is_allowed),
                    "prev": prev,
                }
            ),
        )
    )
    session.commit()
    session.refresh(row)
    return row


def reset_to_defaults(session: Session, *, actor_user_id: Optional[int]) -> int:
    """Truncate RolePermission and re-seed from DEFAULT_ROLE_PERMISSIONS.

    Returns the number of rows seeded. Writes a single AuditLog row.
    """
    from .db import seed_employee_portal_defaults  # local import — avoid cycle

    session.exec(delete(RolePermission))
    session.commit()
    seed_employee_portal_defaults(session)
    seeded = session.exec(select(RolePermission)).all()
    session.add(
        AuditLog(
            actor_user_id=actor_user_id,
            action="permission.reset_all",
            details_json=json.dumps({"count": len(seeded)}),
        )
    )
    session.commit()
    return len(seeded)


# ---------------------------------------------------------------------------
# Dashboard widget registry
# ---------------------------------------------------------------------------

_WIDGET_REGISTRY: Dict[str, dict] = {}


def register_widget(
    widget_key: str,
    *,
    title: str,
    description: str = "",
    default_roles: Iterable[str] = (),
    order: int = 100,
) -> None:
    """Register a dashboard widget at startup. In-process only (idempotent).

    Wave 3+ will call this for each widget it ships. The DB-level registry
    (`DashboardWidget`) is populated by the Wave 1 seed; this helper is for
    runtime discovery when rendering the dashboard.
    """
    _WIDGET_REGISTRY[widget_key] = {
        "widget_key": widget_key,
        "title": title,
        "description": description,
        "default_roles": tuple(default_roles),
        "order": int(order),
    }


def all_widgets(session: Session) -> List[DashboardWidget]:
    """Return DashboardWidget rows ordered for matrix UI rendering."""
    return list(
        session.exec(
            select(DashboardWidget).order_by(
                DashboardWidget.display_order, DashboardWidget.widget_key
            )
        ).all()
    )
