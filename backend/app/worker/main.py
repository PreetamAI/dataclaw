# Merge ~/.dataclaw/.env into os.environ BEFORE any module imports `get_settings()`.
# `app.db.session` calls it at module-load time and caches the result via lru_cache,
# so this must run before the `from app...` imports below pull that module in —
# otherwise the worker runs with the default MASTER_KEY and cannot decrypt any
# secret the API wrote with the real key. Existing process env still wins via setdefault().
from app.core.config import load_env_file  # noqa: E402

load_env_file()

import asyncio  # noqa: E402
import logging  # noqa: E402
import signal  # noqa: E402
from collections.abc import Awaitable, Callable  # noqa: E402
from datetime import UTC, datetime  # noqa: E402

from apscheduler.schedulers.asyncio import AsyncIOScheduler  # noqa: E402
from sqlalchemy import select  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession  # noqa: E402

from app.db.session import get_session  # noqa: E402
from app.models.domain import AgentRun, WorkerHeartbeat, Workspace  # noqa: E402
from app.services.agents.background_runner import run_due_background_agents  # noqa: E402
from app.services.agents.docs_agent import run_docs_agent  # noqa: E402
from app.services.agents.lineage_agent import run_lineage_agent  # noqa: E402
from app.services.agents.metadata_agent import run_metadata_agent  # noqa: E402
from app.services.evals.runner import run_batch as run_eval_batch  # noqa: E402
from app.services.knowledge_compile.service import CompileService  # noqa: E402
from app.services.settings_store import get_eval_config  # noqa: E402

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("dataclaw.worker")

AgentRunner = Callable[[AsyncSession], Awaitable[object]]


async def write_heartbeat(session: AsyncSession, *, status: str = "ok", detail: str = "") -> None:
    heartbeat = await session.scalar(
        select(WorkerHeartbeat).where(WorkerHeartbeat.worker_name == "background")
    )
    if heartbeat is None:
        heartbeat = WorkerHeartbeat(
            worker_name="background",
            last_seen_at=datetime.now(UTC),
            status=status,
            detail=detail,
        )
        session.add(heartbeat)
    else:
        heartbeat.last_seen_at = datetime.now(UTC)
        heartbeat.status = status
        heartbeat.detail = detail
    await session.commit()


async def _record_failure(session: AsyncSession, agent_name: str, exc: Exception) -> None:
    workspace = await session.scalar(select(Workspace).limit(1))
    if workspace is None:
        return
    session.add(
        AgentRun(
            workspace_id=workspace.id,
            agent_name=agent_name,
            status="error",
            summary=f"{agent_name} failed with {exc.__class__.__name__}.",
            timeline=[{"step": "run", "status": "error", "detail": exc.__class__.__name__}],
        )
    )
    await session.commit()


async def _run(name: str, runner: AgentRunner) -> None:
    async for session in get_session():
        try:
            run = await runner(session)
            logger.info("agent.completed", extra={"_agent": name, "_run_id": getattr(run, "id", None)})
        except Exception as exc:
            logger.exception("agent.failed", extra={"_agent": name})
            await _record_failure(session, name, exc)


async def metadata_job() -> None:
    await _run("metadata", run_metadata_agent)


async def lineage_job() -> None:
    await _run("lineage", run_lineage_agent)


async def docs_job() -> None:
    await _run("docs", run_docs_agent)


async def compile_job() -> None:
    async for session in get_session():
        try:
            await write_heartbeat(session, detail="compile tick")
            workspace = await session.scalar(select(Workspace).limit(1))
            if workspace:
                result = await CompileService(session).compile(workspace.id)
                logger.info("knowledge_compile.completed", extra={"_nodes": result.nodes_created + result.nodes_updated, "_edges": result.edges_created})
        except Exception as exc:
            logger.exception("knowledge_compile.failed")
            await _record_failure(session, "knowledge_compile", exc)


async def background_agents_job() -> None:
    async for session in get_session():
        try:
            await write_heartbeat(session, detail="background tick")
            results = await run_due_background_agents(session)
            logger.info("background_agents.completed", extra={"_count": len(results)})
        except Exception as exc:
            logger.exception("background_agents.failed")
            await _record_failure(session, "background_agents", exc)
            try:
                await write_heartbeat(session, status="error", detail=f"{exc.__class__.__name__}")
            except Exception:
                logger.exception("heartbeat.failed_after_background_failure")


async def heartbeat_job() -> None:
    async for session in get_session():
        try:
            await write_heartbeat(session, detail="heartbeat tick")
        except Exception:
            logger.exception("heartbeat.failed")


async def eval_batch_job() -> None:
    """Run a scheduled eval batch if the workspace opted in.

    Off by default. Configure via Settings → Evals (writes the
    ``evals:config`` AppSetting). The scheduler ticks hourly; we honour
    the user's ``schedule_interval_minutes`` by self-skipping when the
    last run was too recent — this avoids a worker-restart side effect."""
    async for session in get_session():
        try:
            await write_heartbeat(session, detail="evals tick")
            config = await get_eval_config(session)
            if not config.get("schedule_enabled"):
                return
            workspace = await session.scalar(select(Workspace).limit(1))
            if workspace is None:
                return
            status_filter = tuple(
                config.get("schedule_status_filter") or ("approved", "golden")
            )
            result = await run_eval_batch(
                session,
                workspace_id=workspace.id,
                status_filter=status_filter,
            )
            logger.info(
                "eval_batch.completed",
                extra={
                    "_batch_id": result.batch_id,
                    "_total": result.total,
                    "_passed": result.passed,
                    "_failed": result.failed,
                    "_errored": result.errored,
                    "_aborted": result.aborted,
                },
            )
        except Exception as exc:
            logger.exception("eval_batch.failed")
            await _record_failure(session, "eval_batch", exc)


def build_scheduler(*, background_interval_seconds: int = 60, heartbeat_interval_seconds: int = 30) -> AsyncIOScheduler:
    scheduler = AsyncIOScheduler(timezone="UTC")
    scheduler.add_job(metadata_job, "interval", minutes=30, id="metadata-agent")
    scheduler.add_job(lineage_job, "interval", minutes=30, id="lineage-agent")
    scheduler.add_job(docs_job, "interval", hours=6, id="docs-agent")
    scheduler.add_job(heartbeat_job, "interval", seconds=heartbeat_interval_seconds, id="worker-heartbeat")
    scheduler.add_job(background_agents_job, "interval", seconds=background_interval_seconds, id="background-agents")
    # Phase 4: scheduled evals (off by default; activate via AppSetting key
    # `evals:schedule`). Hourly interval is plenty for a manual-curated
    # eval suite — runs are kicked from the UI on demand most of the time.
    scheduler.add_job(eval_batch_job, "interval", hours=1, id="eval-batch")
    return scheduler


async def start_scheduler(
    *,
    run_initial_tick: bool = True,
    background_interval_seconds: int = 60,
    heartbeat_interval_seconds: int = 30,
) -> AsyncIOScheduler:
    scheduler = build_scheduler(
        background_interval_seconds=background_interval_seconds,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
    )
    scheduler.start()
    logger.info("DataClaw worker started with APScheduler; no Celery, no Redis.")
    if run_initial_tick:
        await background_agents_job()
    else:
        await heartbeat_job()
    return scheduler


async def main() -> None:
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for signame in ("SIGINT", "SIGTERM"):
        signum = getattr(signal, signame, None)
        if signum is not None:
            loop.add_signal_handler(signum, stop_event.set)
    scheduler = await start_scheduler(run_initial_tick=True, background_interval_seconds=60)
    await stop_event.wait()
    logger.info("DataClaw worker stopping.")
    scheduler.shutdown(wait=True)


if __name__ == "__main__":
    asyncio.run(main())
