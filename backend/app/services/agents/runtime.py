from __future__ import annotations

from datetime import UTC, datetime, timedelta
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.domain import AgentRun

DEFAULT_CHAT_BUDGET_SECONDS = 120
DEFAULT_CHAT_BUDGET_TOKENS = 200_000
DEFAULT_BACKGROUND_BUDGET_SECONDS = 300
DEFAULT_BACKGROUND_BUDGET_TOKENS = 100_000


class BudgetExceeded(RuntimeError):
    def __init__(self, message: str = "Run budget exceeded.") -> None:
        super().__init__(message)


class RunQueue:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def claim(self, worker_id: str, lease_seconds: int) -> AgentRun | None:
        now = datetime.now(UTC)
        run = await self.session.scalar(
            select(AgentRun)
            .where(AgentRun.state == "queued")
            .order_by(AgentRun.created_at)
            .limit(1)
        )
        if run is None:
            return None
        run.state = "leased"
        run.status = "leased"
        run.lease_token = f"{worker_id}:{uuid4()}"
        run.lease_expires_at = now + timedelta(seconds=lease_seconds)
        run.timeline = [*(run.timeline or []), {"event": "leased", "worker_id": worker_id, "at": now.isoformat()}]
        await self.session.commit()
        await self.session.refresh(run)
        return run

    async def start(self, run_id: str) -> AgentRun:
        run = await self._run(run_id)
        now = datetime.now(UTC)
        run.state = "running"
        run.status = "running"
        run.started_at = run.started_at or now
        run.timeline = [*(run.timeline or []), {"event": "running", "at": now.isoformat()}]
        await self.session.commit()
        await self.session.refresh(run)
        return run

    async def complete(self, run_id: str, result: str) -> AgentRun:
        run = await self._run(run_id)
        now = datetime.now(UTC)
        run.state = "completed"
        run.status = "completed"
        run.summary = result
        run.finished_at = now
        run.duration_ms = _duration_ms(run.started_at, now)
        run.lease_token = None
        run.lease_expires_at = None
        run.timeline = [*(run.timeline or []), {"event": "completed", "at": now.isoformat()}]
        await self.session.commit()
        await self.session.refresh(run)
        return run

    async def fail(self, run_id: str, error: str) -> AgentRun:
        run = await self._run(run_id)
        now = datetime.now(UTC)
        run.state = "failed"
        run.status = "failed"
        run.summary = error
        run.error_message = error
        run.finished_at = now
        run.duration_ms = _duration_ms(run.started_at, now)
        run.lease_token = None
        run.lease_expires_at = None
        run.timeline = [*(run.timeline or []), {"event": "failed", "at": now.isoformat()}]
        await self.session.commit()
        await self.session.refresh(run)
        return run

    async def cancel(self, run_id: str) -> AgentRun:
        run = await self._run(run_id)
        now = datetime.now(UTC)
        run.state = "cancelled"
        run.status = "cancelled"
        run.summary = "Run cancelled."
        run.finished_at = now
        run.duration_ms = _duration_ms(run.started_at, now)
        run.lease_token = None
        run.lease_expires_at = None
        run.timeline = [*(run.timeline or []), {"event": "cancelled", "at": now.isoformat()}]
        await self.session.commit()
        await self.session.refresh(run)
        return run

    async def reclaim_stale_leases(self, *, now: datetime | None = None, max_retries: int = 3) -> int:
        now = now or datetime.now(UTC)
        rows = list(
            (
                await self.session.scalars(
                    select(AgentRun).where(
                        AgentRun.state == "leased",
                        AgentRun.lease_expires_at.is_not(None),
                    )
                )
            ).all()
        )
        reclaimed = 0
        for run in rows:
            lease_expires_at = run.lease_expires_at
            if lease_expires_at and lease_expires_at.tzinfo is None:
                lease_expires_at = lease_expires_at.replace(tzinfo=UTC)
            if lease_expires_at is None or lease_expires_at > now:
                continue
            run.retry_count += 1
            run.lease_token = None
            run.lease_expires_at = None
            if run.retry_count > max_retries:
                run.state = "failed"
                run.status = "failed"
                run.error_message = "Run exceeded maximum lease retries."
                run.finished_at = now
            else:
                run.state = "queued"
                run.status = "queued"
            run.timeline = [*(run.timeline or []), {"event": "lease_reclaimed", "at": now.isoformat()}]
            reclaimed += 1
        if reclaimed:
            await self.session.commit()
        return reclaimed

    async def _run(self, run_id: str) -> AgentRun:
        run = await self.session.get(AgentRun, run_id)
        if run is None:
            raise LookupError(f"AgentRun not found: {run_id}")
        return run


def apply_default_budgets(run: AgentRun, *, kind: str) -> None:
    if kind == "chat":
        if run.budget_seconds is None:
            run.budget_seconds = DEFAULT_CHAT_BUDGET_SECONDS
        if run.budget_tokens is None:
            run.budget_tokens = DEFAULT_CHAT_BUDGET_TOKENS
        return
    if run.budget_seconds is None:
        run.budget_seconds = DEFAULT_BACKGROUND_BUDGET_SECONDS
    if run.budget_tokens is None:
        run.budget_tokens = DEFAULT_BACKGROUND_BUDGET_TOKENS


async def enforce_run_budget(
    session: AsyncSession,
    run_id: str | None,
    *,
    estimated_tokens: int = 0,
    now: datetime | None = None,
) -> None:
    if not run_id:
        return
    run = await session.get(AgentRun, run_id)
    if run is None:
        return
    now = now or datetime.now(UTC)
    if run.started_at is not None:
        started_at = run.started_at.replace(tzinfo=UTC) if run.started_at.tzinfo is None else run.started_at
        if run.budget_seconds is not None and (now - started_at).total_seconds() > run.budget_seconds:
            await mark_run_timed_out(session, run, "Run time budget exceeded.", now=now)
            raise BudgetExceeded("Run time budget exceeded.")
    if run.budget_tokens is not None and estimated_tokens > run.budget_tokens:
        await mark_run_timed_out(session, run, "Run token budget exceeded.", now=now)
        raise BudgetExceeded("Run token budget exceeded.")


async def mark_run_timed_out(
    session: AsyncSession,
    run: AgentRun,
    message: str,
    *,
    now: datetime | None = None,
) -> None:
    now = now or datetime.now(UTC)
    started_at = run.started_at.replace(tzinfo=UTC) if run.started_at and run.started_at.tzinfo is None else run.started_at
    run.status = "timed_out"
    run.state = "timed_out"
    run.summary = message
    run.error_message = message
    run.finished_at = now
    run.duration_ms = int((now - started_at).total_seconds() * 1000) if started_at else 0
    run.timeline = [*(run.timeline or []), {"event": "timed_out", "at": now.isoformat(), "detail": message}]
    await session.commit()


def _duration_ms(started_at: datetime | None, finished_at: datetime) -> int:
    if started_at is None:
        return 0
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=UTC)
    return int((finished_at - started_at).total_seconds() * 1000)
