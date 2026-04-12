"""
Admin action/mutation routes: clear, recompute, backfill, channels, parser tools.

Extracted from app/main.py.
"""
from __future__ import annotations

from typing import Optional

from fastapi import APIRouter, Depends, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlmodel import Session, delete, func, select

from ..shared import *  # noqa: F401,F403 -- shared helpers, constants, state
from ..db import get_session, managed_session
from ..backfill_requests import cancel_backfill_request
from ..channels import normalize_channel_ids, upsert_watched_channel
from ..discord_ingest import get_discord_client, invalidate_available_channels_cache, list_available_discord_channels

router = APIRouter()


def _bulk_clear_all_discord_messages(session: Session) -> int:
    session.exec(delete(ParseAttempt))
    count = int(session.exec(select(func.count()).select_from(DiscordMessage)).one())
    session.exec(delete(DiscordMessage))
    session.commit()
    return count


@router.post("/admin/clear")
def clear_all_messages(request: Request):
    if denial := require_role_response(request, "admin"):
        return denial
    with managed_session() as session:
        count = _bulk_clear_all_discord_messages(session)

    return {"ok": True, "deleted": count}


@router.post("/admin/clear/form")
def clear_all_messages_form(request: Request):
    if denial := require_role_response(request, "admin"):
        return denial
    with managed_session() as session:
        count = _bulk_clear_all_discord_messages(session)

    return RedirectResponse(
        url=f"/table?success=Cleared+{count}+messages",
        status_code=303,
    )

@router.post("/admin/recompute-financials")
def admin_recompute_financials(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    updated = recompute_financial_fields(session)
    return {"ok": True, "updated": updated}

@router.post("/admin/recompute-financials/form")
def admin_recompute_financials_form(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    updated = recompute_financial_fields(session)
    return RedirectResponse(
        url=f"/table?success=Recomputed+financial+fields+for+{updated}+messages",
        status_code=303,
    )

@router.post("/admin/warm-attachment-cache")
def admin_warm_attachment_cache(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    extracted, already_cached = warm_attachment_cache(session)
    return {"ok": True, "extracted": extracted, "already_cached": already_cached}

@router.post("/admin/warm-attachment-cache/form")
def admin_warm_attachment_cache_form(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    extracted, already_cached = warm_attachment_cache(session)
    return RedirectResponse(
        url=f"/table?success=Cache+warmed:+{extracted}+extracted,+{already_cached}+already+cached",
        status_code=303,
    )

@router.post("/admin/rebuild-transactions")
def admin_rebuild_transactions(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    rebuilt = rebuild_transactions(session)
    return {"ok": True, "rebuilt": rebuilt}

@router.post("/admin/rebuild-transactions/form")
def admin_rebuild_transactions_form(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    rebuilt = rebuild_transactions(session)
    return RedirectResponse(
        url=f"/table?success=Rebuilt+{rebuilt}+normalized+transactions",
        status_code=303,
    )

@router.post("/admin/parser/reprocess-form")
def admin_parser_reprocess_form(
    request: Request,
    return_path: str = Form(default="/table"),
    force: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial

    queued = queue_auto_reprocess_candidates(
        session,
        force=bool(force),
    )
    separator = "&" if "?" in return_path else "?"
    mode_label = "manual+full" if force else "manual"
    return RedirectResponse(
        url=f"{return_path}{separator}success=Queued+{queued}+rows+for+{mode_label}+parser+reprocess",
        status_code=303,
    )

@router.post("/admin/parser/reparse-range")
def admin_parser_reparse_range(
    request: Request,
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    include_failed: Optional[str] = Form(default=None),
    include_ignored: Optional[str] = Form(default=None),
    include_reviewed: Optional[str] = Form(default=None),
    force_reviewed: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial

    start = parse_report_datetime(after)
    end = parse_report_datetime(before, end_of_day=True)
    if start is None and end is None:
        raise HTTPException(status_code=400, detail="Provide after and/or before to define a reparse range.")
    if include_reviewed and not force_reviewed:
        raise HTTPException(
            status_code=400,
            detail="Reviewed rows require force_reviewed to avoid overwriting manual review corrections.",
        )

    include_statuses = [PARSE_PARSED, PARSE_REVIEW_REQUIRED]
    if include_failed:
        include_statuses.append("failed")
    if include_ignored:
        include_statuses.append("ignored")

    run_id = safe_create_reparse_run(
        source="admin_api",
        reason="manual range reparse",
        range_after=start,
        range_before=end,
        channel_id=channel_id or None,
        include_reviewed=bool(include_reviewed),
        force_reviewed=bool(force_reviewed),
        requested_statuses=include_statuses,
    )

    result = queue_reparse_range(
        session,
        start=start,
        end=end,
        channel_id=channel_id or None,
        include_statuses=include_statuses,
        include_reviewed=bool(include_reviewed),
        reason="manual range reparse",
        reparse_run_id=run_id,
    )
    safe_finalize_reparse_run_queue(
        run_id=run_id,
        selected_count=result["matched"],
        queued_count=result["queued"],
        already_queued_count=result["already_queued"],
        skipped_reviewed_count=result["skipped_reviewed"],
        first_message_id=result["first_message_id"],
        last_message_id=result["last_message_id"],
        first_message_created_at=result["first_message_created_at"],
        last_message_created_at=result["last_message_created_at"],
    )
    return {
        "ok": True,
        "run_id": run_id,
        "queued": result["queued"],
        "matched": result["matched"],
        "channel_id": channel_id or None,
        "after": after or None,
        "before": before or None,
        "included_statuses": include_statuses,
        "include_reviewed": bool(include_reviewed),
    }

@router.post("/admin/parser/reparse-range-form")
def admin_parser_reparse_range_form(
    request: Request,
    return_path: str = Form(default="/table"),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    channel_id: Optional[str] = Form(default=None),
    include_failed: Optional[str] = Form(default=None),
    include_ignored: Optional[str] = Form(default=None),
    include_reviewed: Optional[str] = Form(default=None),
    force_reviewed: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial

    start = parse_report_datetime(after)
    end = parse_report_datetime(before, end_of_day=True)
    if start is None and end is None:
        separator = "&" if "?" in return_path else "?"
        return RedirectResponse(
            url=f"{return_path}{separator}error=Provide+after+and/or+before+to+define+a+reparse+range",
            status_code=303,
        )
    if include_reviewed and not force_reviewed:
        separator = "&" if "?" in return_path else "?"
        return RedirectResponse(
            url=(
                f"{return_path}{separator}"
                "error=Reviewed+rows+require+force_reviewed+to+avoid+overwriting+manual+corrections"
            ),
            status_code=303,
        )

    include_statuses = [PARSE_PARSED, PARSE_REVIEW_REQUIRED]
    if include_failed:
        include_statuses.append("failed")
    if include_ignored:
        include_statuses.append("ignored")

    run_id = safe_create_reparse_run(
        source="admin_form",
        reason="manual range reparse",
        range_after=start,
        range_before=end,
        channel_id=channel_id or None,
        include_reviewed=bool(include_reviewed),
        force_reviewed=bool(force_reviewed),
        requested_statuses=include_statuses,
    )

    result = queue_reparse_range(
        session,
        start=start,
        end=end,
        channel_id=channel_id or None,
        include_statuses=include_statuses,
        include_reviewed=bool(include_reviewed),
        reason="manual range reparse",
        reparse_run_id=run_id,
    )
    safe_finalize_reparse_run_queue(
        run_id=run_id,
        selected_count=result["matched"],
        queued_count=result["queued"],
        already_queued_count=result["already_queued"],
        skipped_reviewed_count=result["skipped_reviewed"],
        first_message_id=result["first_message_id"],
        last_message_id=result["last_message_id"],
        first_message_created_at=result["first_message_created_at"],
        last_message_created_at=result["last_message_created_at"],
    )
    separator = "&" if "?" in return_path else "?"
    return RedirectResponse(
        url=(
            f"{return_path}{separator}"
            f"success=Queued+{result['queued']}+rows+for+parser+range+reparse"
        ),
        status_code=303,
    )

@router.get("/admin/parser/reparse-runs", response_class=HTMLResponse)
def admin_parser_reparse_runs_page(
    request: Request,
    limit: int = Query(default=20, ge=1, le=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    rows = list_recent_reparse_runs(session, limit=limit)
    return templates.TemplateResponse(
        request,
        "reparse_runs.html",
        {
            "request": request,
            "title": "Reparse Runs",
            "current_user": getattr(request.state, "current_user", None),
            "runs": build_reparse_run_table_rows(rows),
            "limit": limit,
        },
    )

@router.get("/admin/parser/reparse-runs.json")
def admin_parser_reparse_runs_json(
    request: Request,
    limit: int = Query(default=20, ge=1, le=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        raise HTTPException(status_code=403, detail="Not authorized")
    return {
        "runs": serialize_reparse_runs(list_recent_reparse_runs(session, limit=limit)),
    }

@router.get("/admin/parser/learned-rule-log", response_class=HTMLResponse)
def admin_parser_learned_rule_log_page(
    request: Request,
    limit: int = Query(default=50, ge=1, le=100),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    return templates.TemplateResponse(
        request,
        "learned_rule_log.html",
        {
            "request": request,
            "title": "Learned Rule Log",
            "current_user": getattr(request.state, "current_user", None),
            "events": build_learned_rule_log_rows(session, limit=limit),
            "limit": limit,
        },
    )

@router.post("/admin/clear/channel/{channel_id}")
def clear_channel_messages(request: Request, channel_id: str):
    if denial := require_role_response(request, "admin"):
        return denial
    with managed_session() as session:
        rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.channel_id == channel_id)
        ).all()

        count = len(rows)
        row_ids = [row.id for row in rows if row.id is not None]
        if row_ids:
            session.exec(delete(ParseAttempt).where(ParseAttempt.message_id.in_(row_ids)))
        for row in rows:
            session.delete(row)

        session.commit()

    return {
        "ok": True,
        "channel_id": channel_id,
        "deleted": count,
    }
@router.post("/admin/clear/channel")
def clear_channel_messages_form(
    request: Request,
    channel_id: str = Form(...),
):
    if denial := require_role_response(request, "admin"):
        return denial
    with managed_session() as session:
        rows = session.exec(
            select(DiscordMessage).where(DiscordMessage.channel_id == channel_id)
        ).all()

        count = len(rows)
        channel_name = rows[0].channel_name if rows else channel_id
        row_ids = [row.id for row in rows if row.id is not None]
        if row_ids:
            session.exec(delete(ParseAttempt).where(ParseAttempt.message_id.in_(row_ids)))
        for row in rows:
            session.delete(row)

        session.commit()

    return RedirectResponse(
        url=f"/table?success=Cleared+{count}+messages+from+{channel_name}",
        status_code=303,
    )

@router.post("/admin/backfill")
async def admin_backfill(
    request: Request,
    channel_id: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    limit: Optional[int] = Form(default=None),
    oldest_first: bool = Form(default=True),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    _, _, after_dt, before_dt = validate_backfill_range(after, before)
    target_channel_ids = get_backfill_target_channel_ids(session, channel_id=channel_id)
    if not target_channel_ids:
        raise HTTPException(status_code=400, detail="No backfill-enabled watched channels are available for this request")
    persist_backfill_window_for_targets(
        session,
        channel_ids=target_channel_ids,
        after_dt=after_dt,
        before_dt=before_dt,
    )
    queued_message = queue_backfill_request(
        session,
        request,
        channel_id=channel_id,
        after_dt=after_dt,
        before_dt=before_dt,
        limit=limit,
        oldest_first=oldest_first,
    )
    trigger_backfill_claim_attempt(get_discord_client())
    return {"ok": True, "queued": True, "message": queued_message.replace("+", " ")}

@router.post("/admin/backfill/form")
async def admin_backfill_form(
    request: Request,
    channel_id: Optional[str] = Form(default=None),
    after: Optional[str] = Form(default=None),
    before: Optional[str] = Form(default=None),
    limit: Optional[int] = Form(default=None),
    oldest_first: bool = Form(default=True),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    try:
        _, _, after_dt, before_dt = validate_backfill_range(after, before)
        target_channel_ids = get_backfill_target_channel_ids(session, channel_id=channel_id)
        if not target_channel_ids:
            return RedirectResponse(
                url="/table?error=No+backfill-enabled+watched+channels+are+available+for+this+request",
                status_code=303,
            )
        persist_backfill_window_for_targets(
            session,
            channel_ids=target_channel_ids,
            after_dt=after_dt,
            before_dt=before_dt,
        )
        queued_message = queue_backfill_request(
            session,
            request,
            channel_id=channel_id,
            after_dt=after_dt,
            before_dt=before_dt,
            limit=limit,
            oldest_first=oldest_first,
        )
        trigger_backfill_claim_attempt(get_discord_client())
        return RedirectResponse(url=f"/table?success={queued_message}", status_code=303)

    except Exception as e:
        return RedirectResponse(url=f"/table?error={str(e)}", status_code=303)

@router.post("/admin/backfill/cancel")
def admin_cancel_backfill(
    request: Request,
    request_id: int = Form(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial

    ok, message = cancel_backfill_request(
        session,
        request_id,
        requested_by=current_user_label(request),
    )
    destination = "success" if ok else "error"
    encoded_message = message.replace(" ", "+")
    return RedirectResponse(url=f"/table?{destination}={encoded_message}", status_code=303)

@router.get("/admin/channels")
def admin_list_channels(request: Request, session: Session = Depends(get_session)):
    if denial := require_role_response(request, "admin"):
        return denial
    rows = get_watched_channels(session)
    return [
        {
            "id": row.id,
            "channel_id": row.channel_id,
            "channel_name": row.channel_name,
            "is_enabled": row.is_enabled,
            "backfill_enabled": row.backfill_enabled,
        }
        for row in rows
    ]

@router.post("/admin/channels/add")
async def admin_add_channel(
    request: Request,
    channel_ids: Optional[list[str]] = Form(default=None),
    manual_channel_ids: Optional[str] = Form(default=None),
    channel_name: Optional[str] = Form(default=None),
    backfill_after: Optional[str] = Form(default=None),
    backfill_before: Optional[str] = Form(default=None),
    backfill_enabled: Optional[str] = Form(default=None),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    manual_ids = []
    if manual_channel_ids:
        manual_ids = [
            piece.strip()
            for piece in manual_channel_ids.replace("\r", ",").replace("\n", ",").split(",")
        ]
    cleaned_channel_ids = normalize_channel_ids([*(channel_ids or []), *manual_ids])
    if not cleaned_channel_ids:
        return RedirectResponse(
            url="/table?error=Select+at+least+one+valid+channel",
            status_code=303,
        )

    try:
        _, _, after_dt, before_dt = validate_backfill_range(backfill_after, backfill_before)
    except HTTPException as exc:
        return RedirectResponse(url=f"/table?error={exc.detail}", status_code=303)

    should_enable_backfill = backfill_enabled is not None
    saved_channels = []
    for channel_id in cleaned_channel_ids:
        saved_channels.append(
            upsert_watched_channel(
                session,
                channel_id=channel_id,
                channel_name=channel_name if len(cleaned_channel_ids) == 1 else None,
                is_enabled=True,
                backfill_enabled=should_enable_backfill,
                backfill_after=after_dt,
                backfill_before=before_dt,
            )
        )

    if after_dt or before_dt:
        client = get_discord_client()
        if client is None or not client.is_ready():
            queued_count = 0
            for channel in saved_channels:
                queue_backfill_request(
                    session,
                    request,
                    channel_id=channel.channel_id,
                    after_dt=after_dt,
                    before_dt=before_dt,
                    limit=None,
                    oldest_first=True,
                )
                queued_count += 1
            return RedirectResponse(
                url=f"/table?success=Saved+{len(saved_channels)}+channels+and+queued+{queued_count}+backfill+request(s)+for+the+worker",
                status_code=303,
            )

        total_inserted = 0
        total_skipped = 0
        failed_channels: list[str] = []
        for channel in saved_channels:
            result = await client.backfill_channel(
                channel_id=int(channel.channel_id),
                after=after_dt,
                before=before_dt,
                oldest_first=True,
            )
            if result.get("ok"):
                total_inserted += result.get("inserted", 0)
                total_skipped += result.get("skipped", 0)
            else:
                failed_channels.append(channel.channel_id)

        if failed_channels:
            failed_text = ",".join(failed_channels)
            return RedirectResponse(
                url=f"/table?error=Saved+{len(saved_channels)}+channels+but+backfill+failed+for:+{failed_text}",
                status_code=303,
            )

        msg = (
            f"Saved+{len(saved_channels)}+channels+and+backfilled+range:"
            f"+inserted={total_inserted},+skipped={total_skipped}"
        )
        return RedirectResponse(url=f"/table?success={msg}", status_code=303)

    return RedirectResponse(
        url=f"/table?success=Saved+{len(saved_channels)}+channel(s)",
        status_code=303,
    )

@router.post("/admin/channels/toggle")
def admin_toggle_channel(
    request: Request,
    channel_id: str = Form(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    row = session.exec(
        select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
    ).first()

    if not row:
        return RedirectResponse(
            url=f"/table?error=Channel+{channel_id}+not+found",
            status_code=303,
        )

    row.is_enabled = not row.is_enabled
    row.updated_at = utcnow()
    session.add(row)
    session.commit()

    state = "enabled" if row.is_enabled else "disabled"
    return RedirectResponse(
        url=f"/table?success=Channel+{channel_id}+{state}",
        status_code=303,
    )

@router.post("/admin/channels/toggle-backfill")
def admin_toggle_channel_backfill(
    request: Request,
    channel_id: str = Form(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    row = session.exec(
        select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
    ).first()

    if not row:
        return RedirectResponse(
            url=f"/table?error=Channel+{channel_id}+not+found",
            status_code=303,
        )

    row.backfill_enabled = not row.backfill_enabled
    row.updated_at = utcnow()
    session.add(row)
    session.commit()

    state = "enabled" if row.backfill_enabled else "disabled"
    return RedirectResponse(
        url=f"/table?success=Backfill+for+channel+{channel_id}+{state}",
        status_code=303,
    )

@router.post("/admin/channels/remove")
def admin_remove_channel(
    request: Request,
    channel_id: str = Form(...),
    session: Session = Depends(get_session),
):
    if denial := require_role_response(request, "admin"):
        return denial
    row = session.exec(
        select(WatchedChannel).where(WatchedChannel.channel_id == channel_id)
    ).first()

    if not row:
        return RedirectResponse(
            url=f"/table?error=Channel+{channel_id}+not+found",
            status_code=303,
        )

    session.delete(row)
    session.commit()

    return RedirectResponse(
        url=f"/table?success=Removed+channel+{channel_id}",
        status_code=303,
    )
@router.post("/admin/channels/rescan")
def admin_rescan_channels(request: Request):
    if denial := require_role_response(request, "admin"):
        return denial
    invalidate_available_channels_cache()
    list_available_discord_channels()
    return RedirectResponse(
        url="/table?success=Channel+list+refreshed",
        status_code=303,
    )


@router.get("/admin/discord/channels")
def admin_list_discord_channels(request: Request):
    if denial := require_role_response(request, "admin"):
        return denial
    channels = list_available_discord_channels()
    return channels
