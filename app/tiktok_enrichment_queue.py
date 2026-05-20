"""Durable retry queue for TikTok webhook order enrichment.

Webhook payloads are often thin.  We acknowledge TikTok quickly, persist a local
job, and let a retryable worker fetch the full order details from the Shop API.
"""
from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta
import re

from sqlalchemy import func, or_, update
from sqlmodel import Session, select

from .models import TikTokWebhookEnrichmentJob, utcnow

ENRICH_PENDING = "pending"
ENRICH_PROCESSING = "processing"
ENRICH_SUCCEEDED = "succeeded"
ENRICH_FAILED = "failed"
ACTIVE_ENRICHMENT_STATUSES = {ENRICH_PENDING, ENRICH_PROCESSING}
DEFAULT_MAX_ATTEMPTS = 5
STALE_PROCESSING_AFTER = timedelta(minutes=10)
_SENSITIVE_ERROR_RE = re.compile(
    r"(?i)(access[_-]?token|refresh[_-]?token|api[_-]?key|app[_-]?secret|secret|password)"
    r"([\s'\"]*[:=][\s'\"]*)[^\s,'\"}]+"
)


def _normalise_order_id(order_id: str) -> str:
    return str(order_id or "").strip()


def _safe_error_text(exc: BaseException) -> str:
    """Persist retry-visible errors without storing obvious credential values."""
    text = str(exc).strip() or exc.__class__.__name__
    return _SENSITIVE_ERROR_RE.sub(r"\1\2[REDACTED]", text)[:1000]


def enqueue_tiktok_webhook_enrichment(
    session: Session,
    order_id: str,
    *,
    now: datetime | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
) -> TikTokWebhookEnrichmentJob:
    """Create or refresh the durable enrichment job for a TikTok order.

    The row is intentionally unique per order ID. Duplicate webhook delivery
    should not create duplicate work, but a later webhook for an already-finished
    order should make the order eligible for enrichment again.
    """
    normalized_order_id = _normalise_order_id(order_id)
    if not normalized_order_id:
        raise ValueError("TikTok enrichment order_id is required")

    now = now or utcnow()
    job = session.exec(
        select(TikTokWebhookEnrichmentJob).where(
            TikTokWebhookEnrichmentJob.tiktok_order_id == normalized_order_id
        )
    ).first()
    if job is None:
        job = TikTokWebhookEnrichmentJob(
            tiktok_order_id=normalized_order_id,
            status=ENRICH_PENDING,
            attempts=0,
            max_attempts=max(int(max_attempts or DEFAULT_MAX_ATTEMPTS), 1),
            next_attempt_at=now,
            last_error="",
            created_at=now,
            updated_at=now,
        )
    else:
        previous_status = job.status
        job.max_attempts = max(
            int(max_attempts or job.max_attempts or DEFAULT_MAX_ATTEMPTS),
            1,
        )
        if previous_status in {ENRICH_FAILED, ENRICH_SUCCEEDED}:
            job.status = ENRICH_PENDING
            job.next_attempt_at = now
            job.succeeded_at = None
            job.attempts = 0
            job.last_error = ""
        elif previous_status == ENRICH_PENDING:
            if job.next_attempt_at is None:
                job.next_attempt_at = now
            job.succeeded_at = None
            job.last_error = job.last_error or ""
        elif previous_status == ENRICH_PROCESSING:
            # A duplicate webhook must not make an actively claimed job
            # immediately claimable by another worker. Startup recovery handles
            # truly interrupted processing jobs.
            job.last_error = job.last_error or ""
        else:
            job.status = ENRICH_PENDING
            if job.next_attempt_at is None:
                job.next_attempt_at = now
            job.succeeded_at = None
            job.last_error = job.last_error or ""
        job.updated_at = now
    session.add(job)
    return job


def get_tiktok_webhook_enrichment_queue_counts(session: Session) -> dict[str, int]:
    def _count(status: str) -> int:
        return int(
            session.exec(
                select(func.count()).select_from(TikTokWebhookEnrichmentJob).where(
                    TikTokWebhookEnrichmentJob.status == status
                )
            ).one()
            or 0
        )

    pending = _count(ENRICH_PENDING)
    processing = _count(ENRICH_PROCESSING)
    failed = _count(ENRICH_FAILED)
    succeeded = _count(ENRICH_SUCCEEDED)
    return {
        "pending": pending,
        "processing": processing,
        "active": pending + processing,
        "failed": failed,
        "succeeded": succeeded,
    }


def _retry_delay_for_attempt(attempts: int) -> timedelta:
    # 1m, 2m, 4m, 8m, capped at 30m. Attempts is after the failure increment.
    minutes = min(2 ** max(attempts - 1, 0), 30)
    return timedelta(minutes=minutes)


def process_due_tiktok_webhook_enrichment_jobs(
    session: Session,
    *,
    now: datetime | None = None,
    enrich_fn: Callable[[str], None],
    limit: int = 10,
) -> int:
    """Process due enrichment jobs in the current DB session.

    Returns the number of jobs attempted. This function commits each claim and
    final job-state transition so multiple workers cannot all process the same
    pending row after selecting it.
    """
    now = now or utcnow()
    safe_limit = max(int(limit or 1), 1)
    jobs = session.exec(
        select(TikTokWebhookEnrichmentJob)
        .where(TikTokWebhookEnrichmentJob.status == ENRICH_PENDING)
        .where(TikTokWebhookEnrichmentJob.next_attempt_at <= now)
        .order_by(
            TikTokWebhookEnrichmentJob.next_attempt_at,
            TikTokWebhookEnrichmentJob.created_at,
        )
        .limit(safe_limit)
    ).all()

    attempted = 0
    for due_job in jobs:
        if due_job.id is None:
            continue
        claim_result = session.execute(
            update(TikTokWebhookEnrichmentJob)
            .where(TikTokWebhookEnrichmentJob.id == due_job.id)
            .where(TikTokWebhookEnrichmentJob.status == ENRICH_PENDING)
            .where(TikTokWebhookEnrichmentJob.next_attempt_at <= now)
            .execution_options(synchronize_session=False)
            .values(
                status=ENRICH_PROCESSING,
                last_attempt_at=now,
                updated_at=now,
            )
        )
        if getattr(claim_result, "rowcount", 0) != 1:
            session.rollback()
            continue
        session.commit()

        job = session.get(TikTokWebhookEnrichmentJob, due_job.id)
        if job is None:
            continue
        attempted += 1
        try:
            enrich_fn(job.tiktok_order_id)
        except Exception as exc:  # noqa: BLE001 - failures are persisted for retry/visibility
            job.attempts = int(job.attempts or 0) + 1
            job.last_error = _safe_error_text(exc)
            job.updated_at = now
            if job.attempts >= int(job.max_attempts or DEFAULT_MAX_ATTEMPTS):
                job.status = ENRICH_FAILED
                job.next_attempt_at = None
            else:
                job.status = ENRICH_PENDING
                job.next_attempt_at = now + _retry_delay_for_attempt(job.attempts)
            session.add(job)
            session.commit()
            continue

        job.status = ENRICH_SUCCEEDED
        job.last_error = ""
        job.next_attempt_at = None
        job.succeeded_at = now
        job.updated_at = now
        session.add(job)
        session.commit()
    return attempted


def requeue_interrupted_tiktok_webhook_enrichment_jobs(
    session: Session,
    *,
    now: datetime | None = None,
    stale_after: timedelta = STALE_PROCESSING_AFTER,
) -> int:
    """Reset processing jobs only after they are stale.

    A fresh process may start while another process is still enriching an order
    during a rolling restart, so startup recovery must not blindly reset all
    processing jobs.
    """
    now = now or utcnow()
    cutoff = now - stale_after
    jobs = session.exec(
        select(TikTokWebhookEnrichmentJob)
        .where(TikTokWebhookEnrichmentJob.status == ENRICH_PROCESSING)
        .where(
            or_(
                TikTokWebhookEnrichmentJob.last_attempt_at <= cutoff,
                TikTokWebhookEnrichmentJob.last_attempt_at.is_(None)
                & (TikTokWebhookEnrichmentJob.updated_at <= cutoff),
            )
        )
    ).all()
    for job in jobs:
        job.status = ENRICH_PENDING
        job.next_attempt_at = now
        job.updated_at = now
        session.add(job)
    return len(jobs)
