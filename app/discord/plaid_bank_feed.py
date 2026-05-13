from __future__ import annotations

import base64
import hashlib
import json
from datetime import datetime, time, timezone
from typing import Any, Optional

import httpx
from cryptography.fernet import Fernet, InvalidToken
from sqlalchemy import func
from sqlmodel import Session, select

from .bank_reconciliation import (
    load_matchable_transactions,
    match_bank_rows_to_transactions,
    normalize_description_stem,
    rerun_bank_reconciliation,
)
from ..config import get_settings
from ..ledger import LEDGER_AGENT_MAX_LIMIT, LedgerFilters, run_ledger_review_agent
from ..models import (
    BankFeedAccount,
    BankFeedConnection,
    BankStatementImport,
    BankTransaction,
    utcnow,
)

PLAID_BASE_URLS = {
    "sandbox": "https://sandbox.plaid.com",
    "development": "https://development.plaid.com",
    "production": "https://production.plaid.com",
}

SYNC_WEBHOOK_CODES = {
    "SYNC_UPDATES_AVAILABLE",
    "INITIAL_UPDATE",
    "HISTORICAL_UPDATE",
    "DEFAULT_UPDATE",
    "TRANSACTIONS_REMOVED",
}


class PlaidConfigurationError(ValueError):
    pass


class PlaidAPIError(RuntimeError):
    pass


def _split_csv(value: str, *, default: list[str]) -> list[str]:
    values = [item.strip() for item in (value or "").split(",") if item.strip()]
    return values or default


def plaid_config_status() -> dict[str, Any]:
    settings = get_settings()
    missing: list[str] = []
    if not settings.plaid_enabled:
        missing.append("PLAID_ENABLED=true")
    if not (settings.plaid_client_id or "").strip():
        missing.append("PLAID_CLIENT_ID")
    if not (settings.plaid_secret or "").strip():
        missing.append("PLAID_SECRET")
    env = (settings.plaid_env or "sandbox").strip().lower()
    if env not in PLAID_BASE_URLS:
        missing.append("PLAID_ENV=sandbox|development|production")
    if not (settings.bank_feed_encryption_key or "").strip():
        missing.append("BANK_FEED_ENCRYPTION_KEY")
    return {
        "enabled": bool(settings.plaid_enabled),
        "configured": not missing or missing == ["BANK_FEED_ENCRYPTION_KEY"],
        "ready": not missing,
        "missing": missing,
        "env": env if env in PLAID_BASE_URLS else "sandbox",
        "products": _split_csv(settings.plaid_products, default=["transactions"]),
        "country_codes": _split_csv(settings.plaid_country_codes, default=["US"]),
        "webhook_url": effective_plaid_webhook_url(),
        "encryption_key_configured": bool((settings.bank_feed_encryption_key or "").strip()),
    }


def effective_plaid_webhook_url() -> str:
    settings = get_settings()
    explicit = (settings.plaid_webhook_url or "").strip()
    if explicit:
        return explicit
    base = (settings.public_base_url or "").strip().rstrip("/")
    return f"{base}/webhooks/plaid" if base else ""


def _plaid_base_url() -> str:
    settings = get_settings()
    env = (settings.plaid_env or "sandbox").strip().lower()
    if env not in PLAID_BASE_URLS:
        raise PlaidConfigurationError("PLAID_ENV must be sandbox, development, or production")
    return PLAID_BASE_URLS[env]


def _require_plaid_config() -> None:
    status = plaid_config_status()
    hard_missing = [item for item in status["missing"] if item != "BANK_FEED_ENCRYPTION_KEY"]
    if hard_missing:
        raise PlaidConfigurationError("Plaid is not configured: " + ", ".join(hard_missing))


def _fernet() -> Fernet:
    settings = get_settings()
    secret = (
        (settings.bank_feed_encryption_key or "").strip()
        or (settings.session_secret or "").strip()
        or (settings.plaid_secret or "").strip()
    )
    if not secret:
        raise PlaidConfigurationError("Set BANK_FEED_ENCRYPTION_KEY before connecting a bank feed")
    key = base64.urlsafe_b64encode(hashlib.sha256(secret.encode("utf-8")).digest())
    return Fernet(key)


def encrypt_access_token(access_token: str) -> bytes:
    return _fernet().encrypt(access_token.encode("utf-8"))


def decrypt_access_token(blob: Optional[bytes]) -> str:
    if not blob:
        raise PlaidConfigurationError("Bank feed connection has no stored access token")
    try:
        return _fernet().decrypt(bytes(blob)).decode("utf-8")
    except InvalidToken as exc:
        raise PlaidConfigurationError(
            "Stored bank feed token could not be decrypted. Check BANK_FEED_ENCRYPTION_KEY."
        ) from exc


def _plaid_payload(payload: dict[str, Any]) -> dict[str, Any]:
    settings = get_settings()
    return {
        "client_id": settings.plaid_client_id,
        "secret": settings.plaid_secret,
        **payload,
    }


def _plaid_post(path: str, payload: dict[str, Any], *, timeout: float = 30.0) -> dict[str, Any]:
    _require_plaid_config()
    url = f"{_plaid_base_url()}{path}"
    try:
        response = httpx.post(url, json=_plaid_payload(payload), timeout=timeout)
    except httpx.HTTPError as exc:
        raise PlaidAPIError(f"Plaid request failed: {exc}") from exc

    data: dict[str, Any]
    try:
        data = response.json()
    except ValueError:
        data = {}
    if response.status_code >= 400:
        message = data.get("error_message") or data.get("display_message") or response.text
        code = data.get("error_code") or response.status_code
        raise PlaidAPIError(f"Plaid {code}: {message}")
    return data


def create_plaid_link_token(*, user_id: str, user_name: str = "") -> str:
    settings = get_settings()
    webhook_url = effective_plaid_webhook_url()
    payload: dict[str, Any] = {
        "client_name": "Degen Collectibles",
        "country_codes": _split_csv(settings.plaid_country_codes, default=["US"]),
        "language": "en",
        "products": _split_csv(settings.plaid_products, default=["transactions"]),
        "user": {"client_user_id": user_id or "degen-admin"},
        "transactions": {"days_requested": 730},
    }
    if webhook_url:
        payload["webhook"] = webhook_url
    data = _plaid_post("/link/token/create", payload)
    link_token = data.get("link_token")
    if not link_token:
        raise PlaidAPIError("Plaid did not return a link_token")
    return str(link_token)


def _parse_plaid_date(value: Any) -> Optional[datetime]:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.strptime(text, "%Y-%m-%d").date()
    except ValueError:
        return None
    return datetime.combine(parsed, time(hour=12), tzinfo=timezone.utc)


def _account_type_from_plaid(account: dict[str, Any]) -> str:
    raw_type = str(account.get("type") or "").lower()
    subtype = str(account.get("subtype") or "").lower()
    if raw_type == "credit":
        return "credit_card"
    if subtype in {"checking", "savings"}:
        return subtype
    if raw_type == "depository":
        return "checking"
    return raw_type or "bank"


def _account_label(connection: BankFeedConnection, account: dict[str, Any]) -> str:
    institution = (connection.institution_name or "").strip()
    name = str(account.get("official_name") or account.get("name") or "Bank account").strip()
    mask = str(account.get("mask") or "").strip()
    parts = [part for part in (institution, name, mask) if part]
    return " ".join(parts) if parts else "Bank account"


def _ensure_import_for_account(
    session: Session,
    *,
    connection: BankFeedConnection,
    account: dict[str, Any],
) -> tuple[BankFeedAccount, BankStatementImport]:
    provider_account_id = str(account.get("account_id") or "").strip()
    if not provider_account_id:
        raise PlaidAPIError("Plaid account response was missing account_id")

    feed_account = session.exec(
        select(BankFeedAccount).where(BankFeedAccount.provider_account_id == provider_account_id)
    ).first()
    account_label = _account_label(connection, account)
    account_type = _account_type_from_plaid(account)
    balances = account.get("balances") or {}

    import_row = None
    if feed_account and feed_account.bank_import_id:
        import_row = session.get(BankStatementImport, feed_account.bank_import_id)
    if import_row is None:
        import_row = session.exec(
            select(BankStatementImport).where(
                BankStatementImport.provider == "plaid",
                BankStatementImport.provider_account_id == provider_account_id,
            )
        ).first()
    if import_row is None:
        import_row = BankStatementImport(
            label=f"{account_label} - Plaid feed",
            account_label=account_label,
            account_type=account_type,
            source_kind="plaid",
            source_name=connection.institution_name or "Plaid",
            file_hash=f"plaid:{connection.provider_item_id}:{provider_account_id}",
            provider="plaid",
            provider_item_id=connection.provider_item_id,
            provider_account_id=provider_account_id,
        )
        session.add(import_row)
        session.commit()
        session.refresh(import_row)

    if feed_account is None:
        feed_account = BankFeedAccount(
            connection_id=connection.id or 0,
            bank_import_id=import_row.id,
            provider_account_id=provider_account_id,
            account_label=account_label,
            account_type=account_type,
        )

    feed_account.connection_id = connection.id or feed_account.connection_id
    feed_account.bank_import_id = import_row.id
    feed_account.account_label = account_label
    feed_account.account_type = account_type
    feed_account.account_subtype = account.get("subtype")
    feed_account.official_name = account.get("official_name")
    feed_account.mask = account.get("mask")
    feed_account.current_balance = balances.get("current")
    feed_account.available_balance = balances.get("available")
    feed_account.iso_currency_code = balances.get("iso_currency_code")
    feed_account.is_active = True
    feed_account.updated_at = utcnow()
    session.add(feed_account)

    import_row.account_label = account_label
    import_row.account_type = account_type
    import_row.source_name = connection.institution_name or import_row.source_name
    import_row.provider = "plaid"
    import_row.provider_item_id = connection.provider_item_id
    import_row.provider_account_id = provider_account_id
    session.add(import_row)
    session.commit()
    session.refresh(feed_account)
    session.refresh(import_row)
    return feed_account, import_row


def _payload_from_plaid_transaction(
    *,
    txn: dict[str, Any],
    import_row: BankStatementImport,
    row_index: int,
) -> dict[str, Any]:
    description = str(
        txn.get("merchant_name")
        or txn.get("name")
        or txn.get("original_description")
        or "Plaid transaction"
    ).strip()
    category_parts = [str(item) for item in (txn.get("category") or []) if item]
    pfc = txn.get("personal_finance_category") or {}
    raw_type = (
        pfc.get("detailed")
        or pfc.get("primary")
        or " / ".join(category_parts)
        or txn.get("payment_channel")
        or ""
    )
    details_parts = [
        item
        for item in (
            txn.get("payment_channel"),
            pfc.get("primary"),
            " / ".join(category_parts),
        )
        if item
    ]
    plaid_amount = float(txn.get("amount") or 0.0)
    return {
        "row_index": row_index,
        "posted_at": _parse_plaid_date(txn.get("date")),
        "transaction_at": _parse_plaid_date(txn.get("authorized_date")) or _parse_plaid_date(txn.get("date")),
        "description": description,
        "description_stem": normalize_description_stem(description),
        "details": " | ".join(details_parts),
        "raw_type": str(raw_type or ""),
        "amount": round(-plaid_amount, 2),
        "balance": None,
        "check_or_slip": None,
        "account_label": import_row.account_label,
        "account_type": import_row.account_type,
        "provider_transaction_id": txn.get("transaction_id"),
        "pending": bool(txn.get("pending")),
        "pending_transaction_id": txn.get("pending_transaction_id"),
        "raw_row_json": json.dumps(txn, sort_keys=True, default=str),
    }


def _next_row_index(session: Session, import_id: int) -> int:
    max_index = session.exec(
        select(func.max(BankTransaction.row_index)).where(BankTransaction.import_id == import_id)
    ).one()
    return int(max_index or 0) + 1


def _refresh_import_totals(session: Session, import_id: int) -> None:
    import_row = session.get(BankStatementImport, import_id)
    if not import_row:
        return
    rows = [
        row
        for row in session.exec(
            select(BankTransaction).where(BankTransaction.import_id == import_id)
        ).all()
        if not row.is_removed
    ]
    dates = [row.posted_at for row in rows if row.posted_at]
    credits = round(sum(float(row.amount or 0.0) for row in rows if float(row.amount or 0.0) > 0), 2)
    debits = round(sum(float(row.amount or 0.0) for row in rows if float(row.amount or 0.0) < 0), 2)
    import_row.row_count = len(rows)
    import_row.range_start = min(dates) if dates else None
    import_row.range_end = max(dates) if dates else None
    import_row.total_credits = credits
    import_row.total_debits = debits
    import_row.net_amount = round(credits + debits, 2)
    import_row.last_sync_at = utcnow()
    import_row.last_sync_error = None
    session.add(import_row)


def _apply_payloads(session: Session, import_row: BankStatementImport, payloads: list[dict[str, Any]]) -> tuple[int, int]:
    if not payloads:
        return (0, 0)

    match_bank_rows_to_transactions(payloads, load_matchable_transactions(session, payloads))
    inserted = 0
    updated = 0
    for payload in payloads:
        provider_transaction_id = str(payload.get("provider_transaction_id") or "").strip()
        if not provider_transaction_id:
            continue
        row = session.exec(
            select(BankTransaction).where(
                BankTransaction.provider_transaction_id == provider_transaction_id
            )
        ).first()
        manual_category = bool(row and row.category_confidence == "manual")
        if row is None:
            row = BankTransaction(
                import_id=import_row.id or 0,
                row_index=int(payload["row_index"]),
                account_label=import_row.account_label,
                account_type=import_row.account_type,
                provider_transaction_id=provider_transaction_id,
            )
            inserted += 1
        else:
            updated += 1

        row.import_id = import_row.id or row.import_id
        row.account_label = import_row.account_label
        row.account_type = import_row.account_type
        row.posted_at = payload.get("posted_at")
        row.transaction_at = payload.get("transaction_at")
        row.description = str(payload.get("description") or "")
        row.description_stem = str(payload.get("description_stem") or "")
        row.details = payload.get("details") or None
        row.raw_type = payload.get("raw_type") or None
        row.amount = float(payload.get("amount") or 0.0)
        row.balance = payload.get("balance")
        row.check_or_slip = payload.get("check_or_slip") or None
        row.classification = str(payload.get("classification") or row.classification or "needs_review")
        row.confidence = str(payload.get("confidence") or row.confidence or "low")
        if not manual_category:
            row.expense_category = str(payload.get("expense_category") or row.expense_category or "uncategorized")
            row.expense_subcategory = payload.get("expense_subcategory") or None
            row.category_confidence = str(payload.get("category_confidence") or row.category_confidence or "low")
            row.category_reason = str(payload.get("category_reason") or "")
        row.match_reason = str(payload.get("match_reason") or "")
        row.matched_transaction_id = payload.get("matched_transaction_id")
        row.matched_source_message_id = payload.get("matched_source_message_id")
        row.matched_platform = payload.get("matched_platform")
        row.pending = bool(payload.get("pending"))
        row.pending_transaction_id = payload.get("pending_transaction_id")
        row.is_removed = False
        row.raw_row_json = str(payload.get("raw_row_json") or "{}")
        row.updated_at = utcnow()
        session.add(row)

        pending_id = str(payload.get("pending_transaction_id") or "").strip()
        if pending_id:
            pending_row = session.exec(
                select(BankTransaction).where(
                    BankTransaction.provider_transaction_id == pending_id
                )
            ).first()
            if pending_row and pending_row.id != row.id:
                pending_row.is_removed = True
                pending_row.review_status = "ignored"
                pending_row.updated_at = utcnow()
                session.add(pending_row)

    session.commit()
    _refresh_import_totals(session, import_row.id or 0)
    session.commit()
    return (inserted, updated)


def _ledger_agent_summary_empty() -> dict[str, Any]:
    return {
        "scanned_count": 0,
        "updated_count": 0,
        "cleared_false_matches": 0,
        "auto_reviewed": 0,
        "left_open": 0,
        "sample_actions": [],
    }


def _merge_ledger_agent_results(results: list[dict[str, Any]]) -> dict[str, Any]:
    merged = _ledger_agent_summary_empty()
    for result in results:
        for key in ("scanned_count", "updated_count", "cleared_false_matches", "auto_reviewed", "left_open"):
            merged[key] += int(result.get(key) or 0)
        merged["sample_actions"].extend(list(result.get("sample_actions") or [])[: max(10 - len(merged["sample_actions"]), 0)])
    return merged


def run_post_plaid_sync_ledger_agent(session: Session) -> dict[str, Any]:
    return run_ledger_review_agent(
        session,
        filters=LedgerFilters(status="needs_action"),
        limit=LEDGER_AGENT_MAX_LIMIT,
        applied_by="Plaid sync",
    )


def _sync_accounts_from_response(
    session: Session,
    connection: BankFeedConnection,
    accounts: list[dict[str, Any]],
) -> dict[str, BankStatementImport]:
    imports_by_account_id: dict[str, BankStatementImport] = {}
    for account in accounts:
        _, import_row = _ensure_import_for_account(session, connection=connection, account=account)
        provider_account_id = str(account.get("account_id") or "")
        if provider_account_id:
            imports_by_account_id[provider_account_id] = import_row
    return imports_by_account_id


def exchange_public_token(
    session: Session,
    *,
    public_token: str,
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    metadata = metadata or {}
    data = _plaid_post("/item/public_token/exchange", {"public_token": public_token})
    access_token = data.get("access_token")
    item_id = data.get("item_id")
    if not access_token or not item_id:
        raise PlaidAPIError("Plaid did not return access_token and item_id")

    institution = metadata.get("institution") or {}
    institution_id = institution.get("institution_id")
    institution_name = institution.get("name")

    connection = session.exec(
        select(BankFeedConnection).where(BankFeedConnection.provider_item_id == item_id)
    ).first()
    if connection is None:
        connection = BankFeedConnection(
            provider="plaid",
            provider_item_id=str(item_id),
        )
    connection.access_token_enc = encrypt_access_token(str(access_token))
    connection.institution_id = institution_id or connection.institution_id
    connection.institution_name = institution_name or connection.institution_name
    connection.status = "active"
    connection.last_sync_error = None
    connection.updated_at = utcnow()
    session.add(connection)
    session.commit()
    session.refresh(connection)

    accounts_data = _plaid_post("/accounts/get", {"access_token": str(access_token)})
    accounts = list(accounts_data.get("accounts") or [])
    _sync_accounts_from_response(session, connection, accounts)
    sync_result = sync_plaid_connection(session, connection.id or 0)
    return {
        "connection_id": connection.id,
        "institution_name": connection.institution_name,
        "accounts": len(accounts),
        "sync": sync_result,
    }


def sync_plaid_connection(session: Session, connection_id: int) -> dict[str, Any]:
    connection = session.get(BankFeedConnection, connection_id)
    if not connection:
        raise ValueError("Bank feed connection not found")
    access_token = decrypt_access_token(connection.access_token_enc)
    original_cursor = connection.cursor
    next_cursor = connection.cursor
    added_count = 0
    modified_count = 0
    removed_count = 0
    pages = 0
    imports_touched: set[int] = set()

    try:
        while True:
            payload: dict[str, Any] = {
                "access_token": access_token,
                "count": 500,
            }
            if next_cursor:
                payload["cursor"] = next_cursor
            data = _plaid_post("/transactions/sync", payload, timeout=45.0)
            accounts = list(data.get("accounts") or [])
            imports_by_account_id = _sync_accounts_from_response(session, connection, accounts)

            payloads_by_import: dict[int, list[dict[str, Any]]] = {}
            next_row_indexes: dict[int, int] = {}
            for txn in list(data.get("added") or []) + list(data.get("modified") or []):
                account_id = str(txn.get("account_id") or "")
                import_row = imports_by_account_id.get(account_id)
                if import_row is None:
                    account = {"account_id": account_id, "name": "Plaid account", "type": "bank"}
                    _, import_row = _ensure_import_for_account(session, connection=connection, account=account)
                row = session.exec(
                    select(BankTransaction).where(
                        BankTransaction.provider_transaction_id == str(txn.get("transaction_id") or "")
                    )
                ).first()
                import_id = import_row.id or 0
                if row:
                    row_index = row.row_index
                else:
                    if import_id not in next_row_indexes:
                        next_row_indexes[import_id] = _next_row_index(session, import_id)
                    row_index = next_row_indexes[import_id]
                    next_row_indexes[import_id] += 1
                payloads_by_import.setdefault(import_row.id or 0, []).append(
                    _payload_from_plaid_transaction(txn=txn, import_row=import_row, row_index=row_index)
                )
                imports_touched.add(import_row.id or 0)

            for import_id, payloads in payloads_by_import.items():
                import_row = session.get(BankStatementImport, import_id)
                if not import_row:
                    continue
                inserted, updated = _apply_payloads(session, import_row, payloads)
                added_count += inserted
                modified_count += updated

            for removed in data.get("removed") or []:
                transaction_id = str(removed.get("transaction_id") or "").strip()
                if not transaction_id:
                    continue
                row = session.exec(
                    select(BankTransaction).where(
                        BankTransaction.provider_transaction_id == transaction_id
                    )
                ).first()
                if row and not row.is_removed:
                    row.is_removed = True
                    row.review_status = "ignored"
                    row.updated_at = utcnow()
                    session.add(row)
                    imports_touched.add(row.import_id)
                    removed_count += 1
            session.commit()

            pages += 1
            next_cursor = data.get("next_cursor") or next_cursor
            if not data.get("has_more"):
                break
            if pages > 20:
                raise PlaidAPIError("Plaid sync stopped after 20 pages to avoid an infinite loop")

        now = utcnow()
        connection.cursor = next_cursor
        connection.last_sync_at = now
        connection.last_sync_error = None
        connection.updated_at = now
        session.add(connection)
        for import_id in imports_touched:
            _refresh_import_totals(session, import_id)
            import_row = session.get(BankStatementImport, import_id)
            if import_row:
                import_row.sync_cursor = next_cursor
                import_row.last_sync_at = now
                import_row.last_sync_error = None
                session.add(import_row)
        session.commit()
        ledger_agent = run_post_plaid_sync_ledger_agent(session)
        return {
            "connection_id": connection.id,
            "added": added_count,
            "modified": modified_count,
            "removed": removed_count,
            "pages": pages,
            "cursor_advanced": next_cursor != original_cursor,
            "ledger_agent": ledger_agent,
        }
    except Exception as exc:
        connection.last_sync_error = str(exc)
        connection.updated_at = utcnow()
        session.add(connection)
        session.commit()
        raise


def sync_all_plaid_connections(session: Session) -> dict[str, Any]:
    rows = list(
        session.exec(
            select(BankFeedConnection).where(
                BankFeedConnection.provider == "plaid",
                BankFeedConnection.status == "active",
            )
        ).all()
    )
    results = []
    for row in rows:
        results.append(sync_plaid_connection(session, row.id or 0))
    ledger_agent_results = [item.get("ledger_agent") for item in results if isinstance(item.get("ledger_agent"), dict)]
    return {
        "connections": len(rows),
        "results": results,
        "added": sum(int(item.get("added") or 0) for item in results),
        "modified": sum(int(item.get("modified") or 0) for item in results),
        "removed": sum(int(item.get("removed") or 0) for item in results),
        "ledger_agent": _merge_ledger_agent_results(ledger_agent_results),
    }


def list_bank_feed_connections(session: Session) -> list[dict[str, Any]]:
    rows = list(
        session.exec(
            select(BankFeedConnection).order_by(
                BankFeedConnection.created_at.desc(),
                BankFeedConnection.id.desc(),
            )
        ).all()
    )
    output: list[dict[str, Any]] = []
    for row in rows:
        accounts = list(
            session.exec(
                select(BankFeedAccount)
                .where(BankFeedAccount.connection_id == row.id)
                .order_by(BankFeedAccount.account_label)
            ).all()
        )
        output.append({"connection": row, "accounts": accounts})
    return output


def handle_plaid_webhook(session: Session, payload: dict[str, Any]) -> dict[str, Any]:
    item_id = str(payload.get("item_id") or "").strip()
    webhook_code = str(payload.get("webhook_code") or "").strip()
    if not item_id:
        return {"ok": False, "reason": "missing_item_id"}
    connection = session.exec(
        select(BankFeedConnection).where(BankFeedConnection.provider_item_id == item_id)
    ).first()
    if not connection:
        return {"ok": False, "reason": "unknown_item"}
    if webhook_code in SYNC_WEBHOOK_CODES:
        result = sync_plaid_connection(session, connection.id or 0)
        return {"ok": True, "synced": True, "result": result}
    return {"ok": True, "synced": False, "webhook_code": webhook_code}


def rerun_all_feed_imports(session: Session) -> int:
    imports = list(
        session.exec(
            select(BankStatementImport).where(BankStatementImport.source_kind == "plaid")
        ).all()
    )
    for row in imports:
        if row.id:
            rerun_bank_reconciliation(session, row.id)
    return len(imports)
