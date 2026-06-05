# Merge ~/.dataclaw/.env into os.environ BEFORE any module imports `get_settings()`.
# `app.db.session` calls it at module-load time and caches the result via lru_cache,
# so this must run before `from app.api.deps import ...` pulls that module in.
# Existing process env (Docker -e, CI secrets) still wins via setdefault().
from app.core.config import load_env_file  # noqa: E402

load_env_file()

import asyncio  # noqa: E402
import json  # noqa: E402
import logging  # noqa: E402
import os  # noqa: E402
import uuid  # noqa: E402
from contextlib import AsyncExitStack, asynccontextmanager  # noqa: E402
from datetime import UTC, datetime, timedelta  # noqa: E402
from pathlib import Path  # noqa: E402
from time import perf_counter  # noqa: E402
from typing import Any  # noqa: E402

import httpx  # noqa: E402
from fastapi import Depends, FastAPI, HTTPException, Query, Request, Response  # noqa: E402
from fastapi.middleware.cors import CORSMiddleware  # noqa: E402
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse  # noqa: E402
from fastapi.staticfiles import StaticFiles  # noqa: E402
from pydantic import BaseModel, Field  # noqa: E402
from sqlalchemy import func, or_, select, text, update  # noqa: E402
from sqlalchemy.engine import make_url  # noqa: E402
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine  # noqa: E402

from app.api.deps import current_user, require_admin  # noqa: E402
from app.core.config import get_settings  # noqa: E402

from app.core.logging import configure_logging, run_log_drainer  # noqa: E402
from app.core.security import (
    decrypt_json,
    encrypt_json,
    password_hash,
    sign_session,
    validate_runtime_secrets,
    verify_password,
)
from app.db.base import Base
from app.db.session import get_session
from app.models.domain import (
    Agent,
    AgentMcpGrant,
    AgentRun,
    AgentToolCall,
    AgentWriteAudit,
    Alert,
    ChatMessage,
    ChatThread,
    Connector,
    Dataset,
    KnowledgeDocument,
    KnowledgeEdge,
    KnowledgeNode,
    LineageEdge,
    LogEntry,
    MonitoringConfig,
    TableAsset,
    User,
    WikiPage,
    WorkerHeartbeat,
)
from app.models.domain import (
    Workspace as WorkspaceModel,
)
from app.schemas.api import (
    AgentCreate,
    AgentGrantMatrixUpdate,
    AgentUpdate,
    ChatRequest,
    ChatResponse,
    ChatThreadCreateRequest,
    ChatThreadRenameRequest,
    ConnectorTestRequest,
    LlmProviderUpdate,
    LoginRequest,
    McpToolCallRequest,
    QueryRequest,
    WorkspaceUpdate,
)
from app.services.agents.background_runner import SYSTEM_HANDLERS, run_due_background_agents
from app.services.agents.chat import answer_question
from app.services.agents.docs_agent import run_docs_agent
from app.services.agents.freshness_agent import run_freshness_agent
from app.services.agents.lineage_agent import run_lineage_agent
from app.services.agents.metadata_agent import run_metadata_agent
from app.services.agents.runtime import apply_default_budgets
from app.services.connectors.adapters import (
    ConnectorAdapterError,
    PostgresAdapter,
    adapter_for,
    seed_sqlite_demo,
)
from app.services.connectors.catalog import CATALOG_BY_SLUG, ConnectorCategory, catalog
from app.services.demo_seed import seed_demo
from app.services.ingestion.auto_sync import auto_sync_all_connectors
from app.services.ingestion.reconciler import reconcile_wiki_disk_edits
from app.services.ingestion.service import IngestionService
from app.services.ingestion.wiki_store import WikiStore
from app.services.knowledge_compile.service import CompileService
from app.services.llm_catalog import CATALOG_BY_SLUG as LLM_CATALOG_BY_SLUG
from app.services.llm_catalog import catalog as llm_catalog
from app.services.mcp_catalog import mcp_catalog
from app.services.mcp_executor import (
    McpExecutionError,
    _sqlalchemy_url_for_datastore,
    _trino_execute,
    _trino_fetch,
    execute_mcp_tool,
)
from app.services.mcp_servers import build_mcp_app, mcp_lifespan_contexts
from app.services.observability.mocks import MOCK_EVENTS
from app.services.observability.tracing import allocate_chat_message_id, chat_trace
from app.services.settings_store import (
    get_llm_provider,
    hydrate_vector_store,
    list_llm_providers,
    resolve_openai,
    update_llm_provider,
)
from app.services.sql_safety import UnsafeSqlError, validate_read_only_sql, validate_write_sql
from app.services.sync_materializer import materialize_sync
from app.services.vector_store import ChromaUnreachableError, vector_store

settings = get_settings()
validate_runtime_secrets(settings.master_key, settings.session_secret)
configure_logging()
logger = logging.getLogger("dataclaw.api")
_login_attempts: dict[str, list[datetime]] = {}
_account_lockouts: dict[str, tuple[int, datetime | None]] = {}
_MAX_LOGIN_RATE_LIMIT_KEYS = 10000
LEGACY_MONITORING_AGENT_MAP = {
    "airflow_failure_agent": "alerting",
    "dbt_failure_agent": "alerting",
    "schema_drift_agent": "data_quality",
    "query_cost_agent": "data_quality",
}


def _mcp_stream_mounts_enabled() -> bool:
    if "PYTEST_CURRENT_TEST" in os.environ and os.getenv("ENABLE_MCP_STREAM_MOUNTS_IN_TESTS") != "1":
        return False
    return True


def _resolve_build_info() -> dict[str, str]:
    """Identify the running code so logs/health reflect what's actually executing.

    Order: explicit env (DATACLAW_GIT_SHA), git rev-parse in the install dir,
    installed package version. Resolved once at import.
    """
    import subprocess  # local import keeps module cold path small
    from importlib import metadata as _metadata

    sha = os.environ.get("DATACLAW_GIT_SHA", "").strip()
    if not sha:
        try:
            repo_root = Path(__file__).resolve().parents[2]
            result = subprocess.run(
                ["git", "-C", str(repo_root), "rev-parse", "--short=12", "HEAD"],
                capture_output=True,
                text=True,
                timeout=2,
                check=False,
            )
            if result.returncode == 0:
                sha = result.stdout.strip()
        except (OSError, subprocess.SubprocessError):
            sha = ""
    try:
        version = _metadata.version("dataclaw-platform")
    except _metadata.PackageNotFoundError:
        version = "source"
    return {"commit": sha or "unknown", "version": version}


BUILD_INFO = _resolve_build_info()


def _seed_file_backed_demo_sqlite(database_url: str) -> None:
    url = make_url(database_url)
    if not url.drivername.startswith("sqlite") or not url.database or url.database == ":memory:":
        return
    seed_sqlite_demo(Path(url.database))


def _total_memory_bytes() -> int | None:
    try:
        import psutil  # type: ignore[import-not-found]

        return int(psutil.virtual_memory().total)
    except Exception as exc:
        logger.debug("memory_probe_psutil_failed", extra={"_error": exc.__class__.__name__})
        if hasattr(os, "sysconf"):
            try:
                pages = os.sysconf("SC_PHYS_PAGES")
                page_size = os.sysconf("SC_PAGE_SIZE")
                return int(pages) * int(page_size)
            except (OSError, ValueError):
                return None
        return None


def _warn_if_low_ollama_memory() -> None:
    if settings.llm_provider.lower() != "ollama":
        return
    total = _total_memory_bytes()
    minimum = 8 * 1024**3
    if total is None or total >= minimum:
        return
    logger.warning(
        "ollama_low_memory",
        extra={
            "_memory_gb": round(total / 1024**3, 2),
            "_recommended": "16 GB is recommended for llama3.1:8b.",
        },
    )


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info(
        "startup_begin",
        extra={
            "_environment": settings.environment,
            "_demo_mode": settings.demo_mode,
            "_openai_configured": bool(settings.openai_api_key),
            "_commit": BUILD_INFO["commit"],
            "_version": BUILD_INFO["version"],
        },
    )
    _warn_if_low_ollama_memory()
    _seed_file_backed_demo_sqlite(settings.demo_database_url)
    app.state.query_engine = create_async_engine(settings.demo_database_url, pool_pre_ping=True)
    try:
        await vector_store.ping()
    except ChromaUnreachableError:
        logger.warning("chroma_unreachable_at_startup")
    if settings.test_auto_create_schema:
        logger.info("test_schema_bootstrap_begin")
        from app.db.session import engine as app_engine

        async with app_engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
        logger.info("test_schema_bootstrap_complete")
    async for session in get_session():
        await seed_demo(session)
        # Reap stale claims: any sync_state='syncing' row at boot is from a
        # process that was killed mid-sync — the task is gone, the claim isn't.
        reaped = await session.execute(
            update(Connector)
            .where(Connector.sync_state == "syncing")
            .values(
                sync_state="sync_failed",
                last_sync_error="Previous sync was interrupted by a restart.",
            )
        )
        if reaped.rowcount:
            logger.warning("stale_sync_claims_reaped", extra={"_count": reaped.rowcount})
            await session.commit()
    worker_scheduler = None
    if settings.embedded_worker and not ("PYTEST_CURRENT_TEST" in os.environ and os.getenv("ENABLE_EMBEDDED_WORKER_IN_TESTS") != "1"):
        from app.worker.main import start_scheduler

        worker_scheduler = await start_scheduler(
            run_initial_tick=False,
            background_interval_seconds=300,
            heartbeat_interval_seconds=90,
        )
        app.state.embedded_worker_scheduler = worker_scheduler
    mcp_stack = AsyncExitStack()
    if not ("PYTEST_CURRENT_TEST" in os.environ and os.getenv("ENABLE_MCP_LIFESPAN_IN_TESTS") != "1"):
        for context in mcp_lifespan_contexts():
            await mcp_stack.enter_async_context(context)
    drainer = asyncio.create_task(run_log_drainer(), name="log-drainer")
    app.state.log_drainer = drainer
    logger.info("startup_complete")
    try:
        yield
    finally:
        if worker_scheduler is not None:
            worker_scheduler.shutdown(wait=True)
        await mcp_stack.aclose()
        drainer.cancel()
        try:
            await drainer
        except asyncio.CancelledError:
            pass
        await app.state.query_engine.dispose()
        logger.info("shutdown_complete")


app = FastAPI(title=settings.app_name, version="0.1.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"],
    allow_headers=[
        "Accept",
        "Authorization",
        "Content-Type",
        "X-DataClaw-Agent-Id",
        "X-Request-Id",
    ],
)

# Phase 1 observability routers. Kept in dedicated modules so the
# observability surface can grow without bloating this file further.
from app.api.chat_traces import router as chat_traces_router  # noqa: E402
from app.api.feedback import router as feedback_router  # noqa: E402
from app.api.integrations import router as integrations_router  # noqa: E402

app.include_router(feedback_router)
app.include_router(integrations_router)
app.include_router(chat_traces_router)


@app.exception_handler(ChromaUnreachableError)
async def _chroma_unreachable_handler(_request: Request, exc: ChromaUnreachableError) -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={
            "detail": {
                "message": "Vector store unavailable.",
                "detail": "ChromaDB is not reachable.",
                "error_type": "ChromaUnreachableError",
            },
        },
    )

if _mcp_stream_mounts_enabled():
    for connector_slug in CATALOG_BY_SLUG:
        app.mount(f"/mcp/{connector_slug}/stream", build_mcp_app(connector_slug))


@app.middleware("http")
async def strip_api_v1_prefix(request: Request, call_next):
    if request.url.path.startswith("/api/v1/") or request.url.path == "/api/v1":
        new_path = request.url.path[7:] or "/"
        request.scope["path"] = new_path
        request.scope["raw_path"] = new_path.encode()
    return await call_next(request)


@app.middleware("http")
async def log_requests(request: Request, call_next):
    start = perf_counter()
    request_id = request.headers.get("x-request-id") or uuid.uuid4().hex[:12]
    request.state.request_id = request_id
    try:
        response = await call_next(request)
    except Exception:
        logger.exception(
            "request_error",
            extra={"_method": request.method, "_path": request.url.path, "_request_id": request_id},
        )
        raise
    response.headers["x-request-id"] = request_id
    duration_ms = round((perf_counter() - start) * 1000, 2)
    logger.info(
        "request_complete",
        extra={
            "_method": request.method,
            "_path": request.url.path,
            "_status_code": response.status_code,
            "_duration_ms": duration_ms,
            "_request_id": request_id,
        },
    )
    return response


def _safe_http_error(message: str, exc: Exception, status_code: int = 400) -> HTTPException:
    correlation_id = uuid.uuid4().hex[:12]
    logger.exception(message, extra={"_correlation_id": correlation_id, "_error": exc.__class__.__name__})
    # Pull a short, user-facing reason from the exception. SQL engines
    # raise messages like "no such table: foo" / "syntax error near …"
    # which the user can act on; we surface the first line of the cause
    # (or fall back to the exception itself) and cap to keep the toast
    # readable. Multi-line tracebacks stay in the log under correlation_id.
    underlying = getattr(exc, "__cause__", None) or getattr(exc, "orig", None) or exc
    raw_reason = str(underlying).splitlines()[0] if str(underlying) else ""
    reason = raw_reason[:240] if raw_reason else (
        "The operation failed. Check backend logs with the correlation_id for details."
    )
    return HTTPException(
        status_code=status_code,
        detail={
            "message": message,
            "detail": f"{message} {reason}".strip() if raw_reason else reason,
            "reason": raw_reason,
            "error_type": exc.__class__.__name__,
            "correlation_id": correlation_id,
        },
    )


def _prune_login_state(now: datetime) -> None:
    if len(_login_attempts) > _MAX_LOGIN_RATE_LIMIT_KEYS:
        stale_keys = [
            key
            for key, values in _login_attempts.items()
            if not any(now - value < timedelta(minutes=1) for value in values)
        ]
        for key in stale_keys:
            _login_attempts.pop(key, None)
    expired_lockouts = [
        email
        for email, (_count, lockout_until) in _account_lockouts.items()
        if lockout_until is not None and lockout_until <= now
    ]
    for email in expired_lockouts:
        _account_lockouts.pop(email, None)


def _check_login_rate_limit(request: Request, email: str) -> None:
    now = datetime.now(UTC)
    _prune_login_state(now)
    ip = request.client.host if request.client else "unknown"
    key = f"{ip}:{email.lower()}"
    attempts = [value for value in _login_attempts.get(key, []) if now - value < timedelta(minutes=1)]
    if len(attempts) >= 5:
        raise HTTPException(status_code=429, detail="Too many login attempts. Try again shortly.")
    _login_attempts[key] = attempts


def _record_login_failure(request: Request, email: str) -> None:
    now = datetime.now(UTC)
    ip = request.client.host if request.client else "unknown"
    key = f"{ip}:{email.lower()}"
    _login_attempts.setdefault(key, []).append(now)
    count, _ = _account_lockouts.get(email.lower(), (0, None))
    count += 1
    lockout: datetime | None = None
    if count >= 5:
        lockout = now + timedelta(seconds=min(900, 2 ** min(count - 5, 8)))
    _account_lockouts[email.lower()] = (count, lockout)


@app.get("/health")
async def health() -> dict:
    return {
        "status": "ok",
        "app": settings.app_name,
        "openai_configured": bool(settings.openai_api_key),
        "commit": BUILD_INFO["commit"],
        "version": BUILD_INFO["version"],
    }


@app.get("/healthz")
async def healthz() -> dict:
    return {"status": "ok"}


@app.get("/health/chroma")
async def health_chroma() -> dict:
    try:
        await vector_store.ping()
    except ChromaUnreachableError as exc:
        raise HTTPException(status_code=503, detail="Vector store unavailable.") from exc
    return {"status": "ok"}


@app.get("/health/worker")
async def health_worker(session: AsyncSession = Depends(get_session)) -> dict:
    heartbeat = await session.scalar(
        select(WorkerHeartbeat).where(WorkerHeartbeat.worker_name == "background")
    )
    if heartbeat is None:
        raise HTTPException(status_code=503, detail="No worker heartbeat has been recorded.")
    last_seen = heartbeat.last_seen_at
    if last_seen.tzinfo is None:
        last_seen = last_seen.replace(tzinfo=UTC)
    age_seconds = (datetime.now(UTC) - last_seen).total_seconds()
    if age_seconds > 120:
        raise HTTPException(status_code=503, detail="Worker heartbeat is stale.")
    return {
        "status": heartbeat.status,
        "last_seen_at": heartbeat.last_seen_at.isoformat(),
        "age_seconds": age_seconds,
        "detail": heartbeat.detail,
    }


@app.get("/worker/status")
async def worker_status(session: AsyncSession = Depends(get_session), user: User = Depends(current_user)) -> dict:
    heartbeat = await session.scalar(
        select(WorkerHeartbeat).where(WorkerHeartbeat.worker_name == "background")
    )
    if heartbeat is None:
        return {"status": "missing", "last_seen_at": None, "detail": "No worker heartbeat has been recorded."}
    last_seen = heartbeat.last_seen_at
    if last_seen.tzinfo is None:
        last_seen = last_seen.replace(tzinfo=UTC)
    age_seconds = (datetime.now(UTC) - last_seen).total_seconds()
    status = "ok" if age_seconds <= 120 and heartbeat.status == "ok" else "stale"
    return {
        "status": status,
        "worker_status": heartbeat.status,
        "last_seen_at": heartbeat.last_seen_at.isoformat(),
        "age_seconds": age_seconds,
        "detail": heartbeat.detail,
    }


@app.get("/auth/status")
async def auth_status():
    return {"auth_disabled": settings.auth_disabled}


async def _ensure_first_admin(session: AsyncSession) -> tuple[User, bool] | tuple[None, bool]:
    existing = await session.scalar(select(func.count()).select_from(User))
    if existing:
        return None, False
    user = User(
        email=settings.admin_email,
        password_hash=password_hash(settings.admin_password),
        is_admin=True,
    )
    session.add(user)
    await session.flush()
    return user, True


@app.post("/auth/login")
async def login(
    payload: LoginRequest,
    request: Request,
    response: Response,
    session: AsyncSession = Depends(get_session),
):
    if settings.auth_disabled:
        user = await session.scalar(select(User).where(User.email == settings.admin_email))
        if user is None:
            user = await session.scalar(select(User).order_by(User.created_at.asc()).limit(1))
        if user is None:
            raise HTTPException(
                status_code=503,
                detail="No bootstrap user. Run `dataclaw bootstrap-admin` first.",
            )
        return {"email": user.email, "is_admin": True}

    _check_login_rate_limit(request, payload.email)
    _, lockout_until = _account_lockouts.get(payload.email.lower(), (0, None))
    now = datetime.now(UTC)
    if lockout_until and lockout_until > now:
        raise HTTPException(status_code=429, detail="Account temporarily locked. Try again shortly.")
    _, bootstrap_admin_created = await _ensure_first_admin(session)
    user = await session.scalar(select(User).where(User.email == payload.email))
    if not user or not verify_password(payload.password, user.password_hash):
        _record_login_failure(request, payload.email)
        raise HTTPException(status_code=401, detail="Invalid credentials.")
    _account_lockouts.pop(payload.email.lower(), None)
    response.set_cookie(
        "dataclaw_session",
        sign_session(settings.session_secret, user.id),
        httponly=True,
        samesite="lax",
        secure=settings.cookies_secure,
    )
    await session.commit()
    return {
        "email": user.email,
        "is_admin": user.is_admin,
        "bootstrap_admin_created": bootstrap_admin_created,
    }


@app.post("/auth/logout")
async def logout(response: Response):
    response.delete_cookie("dataclaw_session")
    return {"ok": True}


@app.get("/connectors/catalog")
async def connector_catalog(user: User = Depends(current_user)):
    return [
        {
            **item.model_dump(exclude={"local_verification"}),
            "credential_schema": [field.model_dump() for field in item.credential_schema],
        }
        for item in CATALOG_BY_SLUG.values()
    ]


@app.get("/connectors")
async def connectors(
    limit: int = Query(200, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    rows = list((await session.scalars(select(Connector).order_by(Connector.category, Connector.display_name).limit(limit))).all())
    return [
        {
            "id": row.id,
            "slug": row.slug,
            "display_name": row.display_name,
            "category": row.category,
            "status": row.status,
            "credential_state": row.credential_state,
            "sync_state": row.sync_state,
            "last_synced_at": row.last_synced_at.isoformat() if row.last_synced_at else None,
            "last_sync_error": row.last_sync_error,
            "logo_key": CATALOG_BY_SLUG[row.slug].logo_key,
            "sync_summary": row.sync_summary,
        }
        for row in rows
    ]


def _connector_summary(slug: str, stored: dict) -> dict:
    item = CATALOG_BY_SLUG[slug]
    visible: dict[str, str] = {}
    secrets_set: list[str] = []
    secret_previews: dict[str, str] = {}
    for field in item.credential_schema:
        value = stored.get(field.name)
        if field.secret:
            if value:
                secrets_set.append(field.name)
                if isinstance(value, str) and len(value) >= 4:
                    secret_previews[field.name] = f"…{value[-4:]}"
        elif value is not None:
            visible[field.name] = str(value)
    return {
        "slug": slug,
        "configured": bool(stored),
        "values": visible,
        "secrets_set": secrets_set,
        "secret_previews": secret_previews,
    }


SYSTEM_AGENT_READ_CATEGORIES = {
    "alerting": {ConnectorCategory.ORCHESTRATION},
    "freshness": {ConnectorCategory.DATA_STORE},
    "data_quality": {ConnectorCategory.DATA_STORE},
    "ingestion": {ConnectorCategory.DATA_STORE, ConnectorCategory.KNOWLEDGE, ConnectorCategory.ORCHESTRATION},
    "chat": {ConnectorCategory.DATA_STORE, ConnectorCategory.KNOWLEDGE, ConnectorCategory.ORCHESTRATION},
    "docs": {ConnectorCategory.DATA_STORE, ConnectorCategory.KNOWLEDGE},
    "metadata": {ConnectorCategory.DATA_STORE, ConnectorCategory.KNOWLEDGE},
    "lineage": {ConnectorCategory.DATA_STORE},
}


async def _auto_grant_configured_connector_read_only(session: AsyncSession, connector: Connector) -> None:
    definition = CATALOG_BY_SLUG.get(connector.slug)
    if definition is None:
        return
    system_agents = list(
        (
            await session.scalars(
                select(Agent).where(Agent.workspace_id == connector.workspace_id, Agent.is_system.is_(True))
            )
        ).all()
    )
    for agent in system_agents:
        eligible = SYSTEM_AGENT_READ_CATEGORIES.get(agent.name, set())
        if definition.category not in eligible:
            continue
        grant = await session.scalar(
            select(AgentMcpGrant).where(
                AgentMcpGrant.agent_id == agent.id,
                AgentMcpGrant.connector_slug == connector.slug,
            )
        )
        if grant is None:
            grant = AgentMcpGrant(agent_id=agent.id, connector_slug=connector.slug)
            session.add(grant)
        grant.read_enabled = True
        # Writes are always approval-gated server-side, so auto-granting write
        # to the interactive chat agent is safe and removes a hidden manual
        # step that testers were tripping over for Scenario 5 (Notion writes).
        grant.write_enabled = agent.name == "chat"


@app.get("/connectors/{slug}")
async def read_connector(
    slug: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    from cryptography.fernet import InvalidToken

    if slug not in CATALOG_BY_SLUG:
        raise HTTPException(status_code=404, detail="Unknown connector.")
    connector = await session.scalar(select(Connector).where(Connector.slug == slug))
    stored: dict = {}
    if connector and connector.encrypted_credentials:
        try:
            stored = decrypt_json(get_settings().master_key, connector.encrypted_credentials)
        except InvalidToken:
            stored = {}
    return _connector_summary(slug, stored)


@app.post("/connectors/{slug}/test")
async def test_connector(
    slug: str,
    payload: ConnectorTestRequest,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    from cryptography.fernet import InvalidToken

    if slug not in CATALOG_BY_SLUG:
        raise HTTPException(status_code=404, detail="Unknown connector.")
    connector = await session.scalar(select(Connector).where(Connector.slug == slug))
    credentials = dict(payload.credentials or {})
    if connector and connector.encrypted_credentials:
        try:
            stored = decrypt_json(get_settings().master_key, connector.encrypted_credentials)
            for key, value in stored.items():
                if key not in credentials or credentials[key] in (None, ""):
                    credentials[key] = value
        except InvalidToken:
            # Old credentials encrypted with a different MASTER_KEY: ignore so
            # the user can re-enter fresh values via this Test call.
            pass
    payload.credentials = credentials
    try:
        result = await adapter_for(slug).test(credentials)
    except ConnectorAdapterError as exc:
        result = {
            "slug": slug,
            "status": "failed",
            "mode": "real",
            "message": exc.clean_message(),
            "details": {"error_type": exc.__class__.__name__},
        }
        return result
    if connector:
        was_configured = connector.credential_state == "configured"
        connector.status = result.status
        connector.last_test_message = result.message
        if result.status == "ok" and payload.persist_on_success:
            connector.credential_state = "configured"
            connector.encrypted_credentials = encrypt_json(settings.master_key, payload.credentials)
            if not was_configured:
                await _auto_grant_configured_connector_read_only(session, connector)
    await session.commit()
    return result.model_dump()


async def _run_connector_sync(slug: str, connector_id: str, credentials: dict) -> None:
    """Execute a sync on its own session. Always finalizes sync_state.

    Used by both the synchronous default path and the opt-in background path.
    Runs on a fresh session so a `PendingRollbackError` from a failed flush
    cannot prevent the terminal `sync_failed` state from being recorded.
    """
    from app.db.session import SessionLocal

    final_error: str | None = None
    try:
        result = await adapter_for(slug).sync(credentials)
        async with SessionLocal() as session:
            try:
                connector = await session.get(Connector, connector_id)
                if connector is None:
                    return
                api_key, _model, base_url, embedding_model = await resolve_openai(session)
                vector_store.ensure_embedding_model(
                    connector.workspace_id, embedding_model, api_key=api_key, base_url=base_url
                )
                await materialize_sync(session, connector, result)
                ingestion = await IngestionService(session).ingest_connector(connector, credentials)
                connector.sync_summary = {**result, "ingestion": ingestion.model_dump()}
                connector.status = result.get("mode", connector.status)
                connector.sync_state = "synced"
                connector.last_synced_at = datetime.now(UTC)
                await session.commit()
                return
            except Exception:
                await session.rollback()
                raise
    except Exception as exc:
        final_error = f"{exc.__class__.__name__}: {exc}"
        logger.exception(
            "connector_sync_failed",
            extra={"_slug": slug, "_connector_id": connector_id, "_error": exc.__class__.__name__},
        )

    try:
        async with SessionLocal() as finalize:
            await finalize.execute(
                update(Connector)
                .where(Connector.id == connector_id)
                .values(sync_state="sync_failed", last_sync_error=final_error or "Sync failed.")
            )
            await finalize.commit()
    except Exception:
        logger.exception(
            "connector_sync_finalize_failed",
            extra={"_slug": slug, "_connector_id": connector_id},
        )


@app.post("/connectors/{slug}/sync")
async def sync_connector(
    slug: str,
    response: Response,
    background: bool = False,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    from cryptography.fernet import InvalidToken

    if slug not in CATALOG_BY_SLUG:
        raise HTTPException(status_code=404, detail="Unknown connector.")
    connector = await session.scalar(select(Connector).where(Connector.slug == slug))
    if connector is None:
        raise HTTPException(status_code=404, detail="Connector is not configured.")

    credentials: dict = {}
    if connector.encrypted_credentials:
        try:
            credentials = decrypt_json(settings.master_key, connector.encrypted_credentials)
        except InvalidToken as exc:
            connector.sync_state = "sync_failed"
            connector.last_sync_error = (
                "Stored credentials were encrypted with a different MASTER_KEY. "
                "Click Configure to re-enter credentials, or restore the original MASTER_KEY."
            )
            await session.commit()
            raise HTTPException(status_code=409, detail=connector.last_sync_error) from exc

    claimed = await session.execute(
        update(Connector)
        .where(Connector.id == connector.id, Connector.sync_state != "syncing")
        .values(sync_state="syncing", last_sync_error=None)
    )
    if claimed.rowcount == 0:
        raise HTTPException(status_code=409, detail="Connector sync is already running.")
    await session.commit()

    connector_id = connector.id

    if background:
        # Opt-in async path for long-running connectors. Returns 202; clients
        # poll GET /connectors/{slug} for terminal state.
        asyncio.create_task(
            _run_connector_sync(slug, connector_id, credentials),
            name=f"connector-sync:{slug}",
        )
        response.status_code = 202
        return {
            "sync_state": "syncing",
            "slug": slug,
            "connector_id": connector_id,
            "poll_url": f"/connectors/{slug}",
        }

    # Default: run inline, return 200 with the sync summary. Matches the
    # pre-2026-06 behavior the UI and existing tests/CLIs depend on.
    await _run_connector_sync(slug, connector_id, credentials)
    await session.refresh(connector)
    if connector.sync_state == "sync_failed":
        raise _safe_http_error(
            "Sync failed.",
            RuntimeError(connector.last_sync_error or "Sync failed."),
        )
    return connector.sync_summary or {"sync_state": connector.sync_state, "slug": slug}


AGENT_RUNNERS = {
    "metadata": run_metadata_agent,
    "lineage": run_lineage_agent,
    "freshness": run_freshness_agent,
    "docs": run_docs_agent,
    **SYSTEM_HANDLERS,
}


def _agent_payload(agent: Agent, grants: list[AgentMcpGrant] | None = None) -> dict:
    payload = {
        "id": agent.id,
        "workspace_id": agent.workspace_id,
        "name": agent.name,
        "display_name": agent.display_name,
        "system_prompt": agent.system_prompt,
        "sql_query": agent.sql_query,
        "kind": agent.kind,
        "is_system": agent.is_system,
        "enabled": agent.enabled,
        "icon_key": agent.icon_key,
        "cadence_minutes": agent.cadence_minutes,
        "thresholds": agent.thresholds or {},
        "uses_llm_filter": agent.uses_llm_filter,
        "target_connector_id": agent.target_connector_id,
        "created_at": agent.created_at.isoformat(),
        "updated_at": agent.updated_at.isoformat(),
    }
    if grants is not None:
        payload["grants"] = [_grant_payload(grant) for grant in grants]
    return payload


def _grant_payload(grant: AgentMcpGrant) -> dict:
    return {
        "id": grant.id,
        "agent_id": grant.agent_id,
        "connector_slug": grant.connector_slug,
        "read_enabled": grant.read_enabled,
        "write_enabled": grant.write_enabled,
    }


async def _workspace(session: AsyncSession) -> WorkspaceModel:
    workspace = await session.scalar(select(WorkspaceModel).limit(1))
    if workspace is None:
        raise HTTPException(status_code=400, detail="Workspace has not been seeded.")
    return workspace


async def _get_agent_or_404(session: AsyncSession, agent_id: str) -> Agent:
    agent = await session.get(Agent, agent_id)
    if agent is None:
        raise HTTPException(status_code=404, detail="Agent not found.")
    return agent


@app.get("/agents")
async def list_agents(
    kind: str | None = None,
    limit: int = Query(200, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    stmt = select(Agent).order_by(Agent.is_system.desc(), Agent.name)
    if kind is not None:
        stmt = stmt.where(Agent.kind == kind)
    agents = list((await session.scalars(stmt.limit(limit))).all())
    return [_agent_payload(agent) for agent in agents]


@app.post("/agents")
async def create_agent(
    payload: AgentCreate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    workspace = await _workspace(session)
    name = payload.name.strip().lower().replace(" ", "_")
    if not name:
        raise HTTPException(status_code=400, detail="Agent name is required.")
    existing = await session.scalar(select(Agent).where(Agent.workspace_id == workspace.id, Agent.name == name))
    if existing:
        raise HTTPException(status_code=409, detail="Agent name already exists.")
    target_connector_id = payload.target_connector_id
    if payload.target_connector_slug is not None:
        target = await session.scalar(
            select(Connector).where(
                Connector.workspace_id == workspace.id,
                Connector.slug == payload.target_connector_slug,
            )
        )
        if target is None:
            raise HTTPException(status_code=404, detail="Target connector not found.")
        target_connector_id = target.id
    if target_connector_id is not None:
        target = await session.get(Connector, target_connector_id)
        if target is None or target.workspace_id != workspace.id:
            raise HTTPException(status_code=404, detail="Target connector not found.")
    grant_overrides = {grant.connector_slug: grant for grant in payload.grants}
    unknown_grants = set(grant_overrides) - set(CATALOG_BY_SLUG)
    if unknown_grants:
        raise HTTPException(status_code=400, detail=f"Unknown connector: {sorted(unknown_grants)[0]}")
    agent = Agent(
        workspace_id=workspace.id,
        name=name,
        display_name=payload.display_name or payload.name,
        system_prompt=payload.system_prompt,
        sql_query=payload.sql_query,
        kind=payload.kind,
        enabled=payload.enabled,
        icon_key=payload.icon_key,
        cadence_minutes=payload.cadence_minutes,
        thresholds=payload.thresholds,
        uses_llm_filter=payload.uses_llm_filter,
        target_connector_id=target_connector_id,
        is_system=False,
        created_by=user.id,
    )
    session.add(agent)
    await session.flush()
    for item in catalog():
        override = grant_overrides.get(item.slug)
        session.add(
            AgentMcpGrant(
                agent_id=agent.id,
                connector_slug=item.slug,
                read_enabled=override.read_enabled if override else False,
                write_enabled=override.write_enabled if override else False,
            )
        )
    await session.commit()
    return _agent_payload(agent)


@app.get("/agents/{agent_id}")
async def get_agent(agent_id: str, session: AsyncSession = Depends(get_session), user: User = Depends(current_user)):
    if agent_id == "dashboard":
        return await agents_dashboard(session, user)
    agent = await _get_agent_or_404(session, agent_id)
    grants = list((await session.scalars(select(AgentMcpGrant).where(AgentMcpGrant.agent_id == agent.id))).all())
    return _agent_payload(agent, grants)


@app.patch("/agents/{agent_id}")
async def update_agent(
    agent_id: str,
    payload: AgentUpdate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    agent = await _get_agent_or_404(session, agent_id)
    was_enabled = agent.enabled
    if payload.display_name is not None:
        agent.display_name = payload.display_name
    if payload.system_prompt is not None:
        agent.system_prompt = payload.system_prompt
    if payload.sql_query is not None:
        agent.sql_query = payload.sql_query
    if payload.kind is not None:
        agent.kind = payload.kind
    if payload.enabled is not None:
        agent.enabled = payload.enabled
        if not was_enabled and payload.enabled and agent.kind == "background":
            agent.force_run_requested_at = datetime.now(UTC)
    if payload.icon_key is not None:
        agent.icon_key = payload.icon_key
    if payload.cadence_minutes is not None:
        agent.cadence_minutes = payload.cadence_minutes
    if payload.thresholds is not None:
        agent.thresholds = payload.thresholds
    if payload.uses_llm_filter is not None:
        agent.uses_llm_filter = payload.uses_llm_filter
    if payload.target_connector_id is not None:
        agent.target_connector_id = payload.target_connector_id
    if payload.target_connector_slug is not None:
        target = await session.scalar(
            select(Connector).where(
                Connector.workspace_id == agent.workspace_id,
                Connector.slug == payload.target_connector_slug,
            )
        )
        if target is None:
            raise HTTPException(status_code=404, detail="Target connector not found.")
        agent.target_connector_id = target.id
    await session.commit()
    return _agent_payload(agent)


@app.delete("/agents/{agent_id}")
async def delete_agent(agent_id: str, session: AsyncSession = Depends(get_session), user: User = Depends(require_admin)):
    agent = await _get_agent_or_404(session, agent_id)
    if agent.is_system:
        raise HTTPException(status_code=400, detail="System agents cannot be deleted.")
    await session.delete(agent)
    await session.commit()
    return {"ok": True}


@app.get("/agents/{agent_id}/grants")
async def get_agent_grants(agent_id: str, session: AsyncSession = Depends(get_session), user: User = Depends(current_user)):
    agent = await _get_agent_or_404(session, agent_id)
    grants = list((await session.scalars(select(AgentMcpGrant).where(AgentMcpGrant.agent_id == agent.id))).all())
    return [_grant_payload(grant) for grant in grants]


@app.put("/agents/{agent_id}/grants")
async def update_agent_grants(
    agent_id: str,
    payload: AgentGrantMatrixUpdate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    agent = await _get_agent_or_404(session, agent_id)
    existing = {
        grant.connector_slug: grant
        for grant in (await session.scalars(select(AgentMcpGrant).where(AgentMcpGrant.agent_id == agent.id))).all()
    }
    for item in payload.grants:
        if item.connector_slug not in CATALOG_BY_SLUG:
            raise HTTPException(status_code=400, detail=f"Unknown connector: {item.connector_slug}")
        grant = existing.get(item.connector_slug)
        if grant is None:
            grant = AgentMcpGrant(agent_id=agent.id, connector_slug=item.connector_slug)
            session.add(grant)
        grant.read_enabled = item.read_enabled
        grant.write_enabled = item.write_enabled
    await session.commit()
    grants = list((await session.scalars(select(AgentMcpGrant).where(AgentMcpGrant.agent_id == agent.id))).all())
    return [_grant_payload(grant) for grant in grants]


@app.get("/mcp/catalog")
async def read_mcp_catalog(user: User = Depends(current_user)):
    return mcp_catalog()


@app.get("/mcp/{slug}/sse")
async def mcp_sse(slug: str, user: User = Depends(current_user)):
    if slug not in CATALOG_BY_SLUG:
        raise HTTPException(status_code=404, detail="Unknown MCP server.")
    return {"status": "ok", "transport": "streamable_http", "slug": slug}


@app.post("/mcp/{slug}/tools/{tool_name}")
async def mcp_tool_call(
    slug: str,
    tool_name: str,
    payload: McpToolCallRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    if slug not in CATALOG_BY_SLUG:
        raise HTTPException(status_code=404, detail="Unknown MCP server.")
    if "__approved" in payload.arguments:
        raise HTTPException(status_code=400, detail="__approved is reserved.")
    engine = await _resolve_query_engine(
        request,
        session,
        slug if slug in {"postgres", "mysql", "redshift", "sqlite"} else None,
    )
    owns_engine = engine is not request.app.state.query_engine
    try:
        return await execute_mcp_tool(
            session=session,
            engine=engine,
            connector_slug=slug,
            tool_name=tool_name,
            arguments=payload.arguments,
            agent_id=request.headers.get("X-DataClaw-Agent-Id"),
            user_email=user.email,
        )
    except McpExecutionError as exc:
        raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    finally:
        if owns_engine:
            await engine.dispose()


@app.post("/agents/auto_sync/run")
async def auto_sync_run(session: AsyncSession = Depends(get_session), user: User = Depends(require_admin)):
    run = await auto_sync_all_connectors(session)
    return {"id": run.id, "status": run.status, "summary": run.summary, "timeline": run.timeline}


@app.post("/agents/background/run-due")
async def background_agents_run_due(
    now: datetime | None = None,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    results = await run_due_background_agents(session, now=now)
    payload = []
    for result in results:
        if isinstance(result, AgentRun):
            payload.append(
                {
                    "id": result.id,
                    "agent_name": result.agent_name,
                    "status": result.status,
                    "summary": result.summary,
                    "timeline": result.timeline,
                }
            )
        else:
            payload.append({"result": result})
    return {"count": len(results), "results": payload}


@app.post("/agents/{name}/run")
async def agent_run(
    name: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    runner = AGENT_RUNNERS.get(name)
    if runner is None:
        raise HTTPException(status_code=404, detail=f"Unknown agent: {name}")
    run = await runner(session)
    return {"id": run.id, "status": run.status, "summary": run.summary, "timeline": run.timeline}


@app.get("/workspace")
async def workspace(session: AsyncSession = Depends(get_session), user: User = Depends(current_user)):
    workspace_row = await _workspace(session)
    datasets = list((await session.scalars(select(Dataset))).all())
    tables = list((await session.scalars(select(TableAsset))).all())
    docs = list((await session.scalars(select(KnowledgeDocument))).all())
    lineage = list((await session.scalars(select(LineageEdge))).all())
    return {
        "id": workspace_row.id,
        "name": workspace_row.name,
        "onboarding_complete": workspace_row.onboarding_complete,
        "tabs": ["IDE", "Agents"],
        "datasets": [
            {
                "id": dataset.id,
                "name": dataset.name,
                "source_type": dataset.source_type,
                "schema_name": dataset.schema_name,
                "tables": [
                    {
                        "id": table.id,
                        "name": table.name,
                        "description": table.description,
                        "business_summary": table.business_summary,
                        "row_count": table.row_count,
                        "freshness_status": table.freshness_status,
                        "tags": table.tags,
                        "columns": table.columns,
                    }
                    for table in tables
                    if table.dataset_id == dataset.id
                ],
            }
            for dataset in datasets
        ],
        "knowledge_documents": [
            {"id": doc.id, "title": doc.title, "connector": doc.connector_slug, "related_tables": doc.related_tables}
            for doc in docs
        ],
        "lineage": [
            {
                "source_table": edge.source_table,
                "target_table": edge.target_table,
                "relationship": edge.relationship,
                "evidence": edge.evidence,
            }
            for edge in lineage
        ],
    }


@app.patch("/workspace")
async def update_workspace(
    payload: WorkspaceUpdate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    workspace_row = await _workspace(session)
    if payload.onboarding_complete is not None:
        workspace_row.onboarding_complete = payload.onboarding_complete
    await session.commit()
    return {
        "id": workspace_row.id,
        "name": workspace_row.name,
        "onboarding_complete": workspace_row.onboarding_complete,
    }


def _wiki_page_payload(page: WikiPage) -> dict:
    return {
        "id": page.id,
        "workspace_id": page.workspace_id,
        "path": page.path,
        "disk_path": page.disk_path,
        "tier": page.tier,
        "source_type": page.source_type,
        "source_id": page.source_id,
        "title": page.title,
        "body": page.body,
        "frontmatter": page.frontmatter,
        "entities": page.entities,
        "content_hash": page.content_hash,
        "created_at": page.created_at.isoformat(),
        "updated_at": page.updated_at.isoformat(),
    }


@app.get("/knowledge/pages")
async def knowledge_pages(
    source_type: str | None = Query(None),
    tier: int | None = Query(None),
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    workspace = await _workspace(session)
    pages = await WikiStore().list_pages(session, workspace.id, source_type=source_type, tier=tier)
    return [_wiki_page_payload(page) for page in pages]


@app.get("/knowledge/pages/{page_path:path}")
async def knowledge_page(
    page_path: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    workspace = await _workspace(session)
    normalized = page_path if page_path.startswith("wiki/") else f"wiki/{page_path}"
    page = await WikiStore().get_page(session, workspace.id, normalized)
    if page is None:
        raise HTTPException(status_code=404, detail="Wiki page not found.")
    return _wiki_page_payload(page)


@app.get("/knowledge/search")
async def knowledge_search(
    q: str = Query(..., min_length=1),
    layer: str = Query("wiki", pattern="^(wiki|chunks|all)$"),
    limit: int = Query(10, ge=1, le=50),
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    workspace = await _workspace(session)
    results: list[dict[str, Any]] = []
    terms = [term for term in q.split() if len(term) > 2] or [q]
    if layer in {"wiki", "all"}:
        clauses = []
        for term in terms:
            pattern = f"%{term}%"
            clauses.extend([WikiPage.title.ilike(pattern), WikiPage.body.ilike(pattern), WikiPage.path.ilike(pattern)])
        pages = (
            await session.scalars(
                select(WikiPage)
                .where(
                    WikiPage.workspace_id == workspace.id,
                    or_(*clauses),
                )
                .order_by(WikiPage.tier.asc(), WikiPage.updated_at.desc())
                .limit(limit)
            )
        ).all()
        for page in pages:
            body = page.body or ""
            hit_at = body.lower().find(q.lower())
            start = max(hit_at - 120, 0) if hit_at >= 0 else 0
            snippet = body[start : start + 300]
            results.append(
                {
                    "layer": "wiki",
                    "id": page.id,
                    "title": page.title,
                    "source": page.path,
                    "source_type": page.source_type,
                    "score": None,
                    "snippet": snippet,
                }
            )
    if layer in {"chunks", "all"} and len(results) < limit:
        try:
            await hydrate_vector_store(session, workspace.id)
            chunks = await vector_store.search(workspace.id, q, top_k=limit - len(results))
        except RuntimeError:
            chunks = []
        for chunk in chunks:
            results.append(
                {
                    "layer": "chunks",
                    "id": chunk.id,
                    "title": chunk.metadata.get("title") or chunk.metadata.get("asset_name") or chunk.id,
                    "source": chunk.metadata.get("source") or chunk.metadata.get("asset_id") or chunk.metadata.get("path"),
                    "source_type": chunk.metadata.get("source_type"),
                    "score": chunk.distance,
                    "snippet": chunk.document[:300],
                    "metadata": chunk.metadata,
                }
            )
        if len(results) < limit:
            clauses = []
            for term in terms:
                pattern = f"%{term}%"
                clauses.extend(
                    [
                        KnowledgeDocument.title.ilike(pattern),
                        KnowledgeDocument.body.ilike(pattern),
                    ]
                )
            docs = (
                await session.scalars(
                    select(KnowledgeDocument)
                    .where(
                        KnowledgeDocument.workspace_id == workspace.id,
                        or_(*clauses),
                    )
                    .order_by(KnowledgeDocument.updated_at.desc())
                    .limit(limit - len(results))
                )
            ).all()
            for doc in docs:
                results.append(
                    {
                        "layer": "chunks",
                        "id": doc.id,
                        "title": doc.title,
                        "source": doc.connector_slug,
                        "source_type": doc.connector_slug,
                        "score": None,
                        "snippet": doc.body[:300],
                        "metadata": {"connector_slug": doc.connector_slug, "related_tables": doc.related_tables},
                    }
                )
    return {"query": q, "layer": layer, "results": results[:limit]}


@app.get("/knowledge/graph/nodes")
async def knowledge_graph_nodes(
    q: str = Query(..., min_length=1),
    limit: int = Query(10, ge=1, le=50),
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    workspace = await _workspace(session)
    pattern = f"%{q}%"
    nodes = (
        await session.scalars(
            select(KnowledgeNode)
            .where(
                KnowledgeNode.workspace_id == workspace.id,
                or_(
                    KnowledgeNode.canonical_name.ilike(pattern),
                    KnowledgeNode.summary.ilike(pattern),
                ),
            )
            .order_by(KnowledgeNode.updated_at.desc())
            .limit(limit)
        )
    ).all()
    return {
        "query": q,
        "nodes": [
            {
                "id": node.id,
                "type": node.type,
                "label": node.canonical_name,
                "canonical_name": node.canonical_name,
                "connector_slug": node.connector_slug,
                "source_type": node.source_type,
                "summary": node.summary,
                "aliases": node.aliases or [],
            }
            for node in nodes
        ],
    }


@app.get("/knowledge/graph/neighbors")
async def knowledge_graph_neighbors(
    node_id: str = Query(..., min_length=1),
    limit: int = Query(50, ge=1, le=200),
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    workspace = await _workspace(session)
    node = await session.scalar(
        select(KnowledgeNode).where(KnowledgeNode.workspace_id == workspace.id, KnowledgeNode.id == node_id)
    )
    if node is None:
        raise HTTPException(status_code=404, detail="Knowledge node not found.")
    edges = (
        await session.scalars(
            select(KnowledgeEdge)
            .where(
                KnowledgeEdge.workspace_id == workspace.id,
                or_(KnowledgeEdge.src_node_id == node_id, KnowledgeEdge.dst_node_id == node_id),
            )
            .limit(limit)
        )
    ).all()
    neighbor_ids = [edge.dst_node_id if edge.src_node_id == node_id else edge.src_node_id for edge in edges]
    neighbors = {
        neighbor.id: neighbor
        for neighbor in (
            await session.scalars(
                select(KnowledgeNode).where(
                    KnowledgeNode.workspace_id == workspace.id,
                    KnowledgeNode.id.in_(neighbor_ids),
                )
            )
        ).all()
    }
    return {
        "node": {"id": node.id, "type": node.type, "label": node.canonical_name},
        "neighbors": [
            {
                "id": neighbor.id,
                "type": neighbor.type,
                "label": neighbor.canonical_name,
                "connector_slug": neighbor.connector_slug,
                "source_type": neighbor.source_type,
                "edge_label": edge.relationship,
                "edge_id": edge.id,
                "direction": "out" if edge.src_node_id == node_id else "in",
                "evidence": edge.evidence,
            }
            for edge in edges
            if (neighbor := neighbors.get(edge.dst_node_id if edge.src_node_id == node_id else edge.src_node_id))
            is not None
        ],
    }


@app.post("/knowledge/compile")
async def knowledge_compile(session: AsyncSession = Depends(get_session), user: User = Depends(require_admin)):
    workspace = await _workspace(session)
    try:
        result = await CompileService(session).compile(workspace.id)
    except RuntimeError as exc:
        if str(exc) == "knowledge_compile_already_running":
            raise HTTPException(status_code=409, detail="Knowledge compile is already running.") from exc
        raise
    return result.model_dump()


@app.post("/knowledge/reconcile")
async def knowledge_reconcile(session: AsyncSession = Depends(get_session), user: User = Depends(require_admin)):
    workspace = await _workspace(session)
    changed = await reconcile_wiki_disk_edits(session, workspace.id)
    return {"pages_changed": changed}


@app.get("/knowledge/graph")
async def knowledge_graph(
    root: str | None = Query(None),
    depth: int = Query(2, ge=1, le=4),
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    workspace = await _workspace(session)
    return await CompileService(session).graph(workspace.id, root=root, depth=depth)


async def _ensure_thread(
    session: AsyncSession,
    user: User,
    thread_id: str | None,
    title_hint: str | None = None,
) -> ChatThread:
    if thread_id:
        thread = await session.scalar(
            select(ChatThread).where(ChatThread.id == thread_id, ChatThread.user_id == user.id)
        )
        if thread is None:
            raise HTTPException(status_code=404, detail="Chat thread not found.")
        return thread
    workspace = await session.scalar(select(WorkspaceModel).limit(1))
    if workspace is None:
        raise HTTPException(status_code=400, detail="Workspace has not been seeded.")
    thread = ChatThread(
        workspace_id=workspace.id,
        user_id=user.id,
        title=(title_hint or "New conversation")[:120],
    )
    session.add(thread)
    await session.flush()
    return thread


def _thread_payload(thread: ChatThread, messages: list[ChatMessage]) -> dict:
    return {
        "id": thread.id,
        "title": thread.title,
        "created_at": thread.created_at.isoformat(),
        "updated_at": thread.updated_at.isoformat(),
        "message_count": len(messages),
        "messages": [
            {
                "id": message.id,
                "role": message.role,
                "content": message.content,
                "sql": message.sql,
                "provider": message.provider,
                "llm_status": message.llm_status,
                "citations": message.citations,
                "rows": message.rows,
                "chart_spec": message.chart_spec,
                "action": message.action,
                "retrieval_trace": message.retrieval_trace,
                "trace_id": message.trace_id,
                "created_at": message.created_at.isoformat(),
            }
            for message in messages
        ],
    }


@app.get("/chat/threads")
async def list_chat_threads(
    include_archived: bool = False,
    limit: int = Query(100, ge=1, le=500),
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    stmt = select(ChatThread).where(ChatThread.user_id == user.id)
    if not include_archived:
        stmt = stmt.where(ChatThread.archived.is_(False))
    threads = list((await session.scalars(stmt.order_by(ChatThread.updated_at.desc()).limit(limit))).all())
    return [
        {
            "id": thread.id,
            "title": thread.title,
            "archived": thread.archived,
            "archived_at": thread.archived_at.isoformat() if thread.archived_at else None,
            "created_at": thread.created_at.isoformat(),
            "updated_at": thread.updated_at.isoformat(),
        }
        for thread in threads
    ]


@app.post("/chat/threads")
async def create_chat_thread(
    payload: ChatThreadCreateRequest,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    thread = await _ensure_thread(session, user, None, payload.title)
    await session.commit()
    return _thread_payload(thread, [])


@app.get("/chat/threads/{thread_id}")
async def get_chat_thread(
    thread_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    thread = await _ensure_thread(session, user, thread_id)
    messages = list(
        (
            await session.scalars(
                select(ChatMessage)
                .where(ChatMessage.thread_id == thread.id)
                .order_by(ChatMessage.created_at)
            )
        ).all()
    )
    return _thread_payload(thread, messages)


@app.patch("/chat/threads/{thread_id}")
async def rename_chat_thread(
    thread_id: str,
    payload: ChatThreadRenameRequest,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    thread = await _ensure_thread(session, user, thread_id)
    thread.title = payload.title.strip()[:120] or thread.title
    await session.commit()
    return _thread_payload(thread, [])


@app.delete("/chat/threads/{thread_id}")
async def delete_chat_thread(
    thread_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    thread = await _ensure_thread(session, user, thread_id)
    await session.delete(thread)
    await session.commit()
    return {"ok": True}


@app.post("/chat/threads/{thread_id}/close")
async def close_chat_thread(
    thread_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    thread = await _ensure_thread(session, user, thread_id)
    if not thread.archived:
        thread.archived = True
        thread.archived_at = datetime.now(UTC)
        await session.commit()
    return {
        "id": thread.id,
        "archived": thread.archived,
        "archived_at": thread.archived_at.isoformat() if thread.archived_at else None,
    }


@app.post("/chat/threads/{thread_id}/reopen")
async def reopen_chat_thread(
    thread_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    thread = await _ensure_thread(session, user, thread_id)
    if thread.archived:
        thread.archived = False
        thread.archived_at = None
        await session.commit()
    return {"id": thread.id, "archived": thread.archived}


@app.post("/ide/chat", response_model=ChatResponse)
async def ide_chat(
    payload: ChatRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    thread = await _ensure_thread(session, user, payload.thread_id, payload.question)
    if thread.archived:
        raise HTTPException(
            status_code=409,
            detail="Thread is archived. Reopen it before sending new messages.",
        )
    if "text/event-stream" in request.headers.get("accept", ""):
        return StreamingResponse(
            _stream_ide_chat(request, session, user, thread, payload),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )
    engine = await _resolve_query_engine(request, session)
    owns_engine = engine is not request.app.state.query_engine
    assistant_message_id = allocate_chat_message_id()
    async with chat_trace(
        session=session,
        chat_message_id=assistant_message_id,
        workspace_id=thread.workspace_id,
        user_id=user.id,
        thread_id=thread.id,
        question=payload.question,
    ) as trace:
        try:
            response = await answer_question(
                session,
                payload.question,
                thread.id,
                payload.model,
                tool_engine=engine,
                user_email=user.email,
                connector_slug=payload.connector_slug,
            )
        finally:
            if owns_engine:
                await engine.dispose()
        response = await _persist_chat_response(
            session,
            thread,
            payload,
            response,
            assistant_message_id=assistant_message_id,
            trace_id=trace.trace_id,
            langfuse_url=trace.langfuse_url,
        )
    return response


def _sse_frame(event: str, data: dict[str, Any]) -> str:
    return f"event: {event}\ndata: {json.dumps(data, default=str)}\n\n"


def _answer_chunks(answer: str, chunk_size: int = 24):
    words = answer.split(" ")
    if not words:
        yield ""
        return
    for index in range(0, len(words), chunk_size):
        yield " ".join(words[index : index + chunk_size]) + (" " if index + chunk_size < len(words) else "")


async def _stream_ide_chat(
    request: Request,
    session: AsyncSession,
    user: User,
    thread: ChatThread,
    payload: ChatRequest,
):
    workspace = await _workspace(session)
    run = AgentRun(
        workspace_id=workspace.id,
        agent_name="chat",
        status="running",
        state="running",
        summary=f"Streaming chat: {payload.question[:120]}",
        timeline=[{"event": "started", "thread_id": thread.id, "at": datetime.now(UTC).isoformat()}],
        started_at=datetime.now(UTC),
        idempotency_key=f"chat:{thread.id}:{uuid.uuid4()}",
    )
    apply_default_budgets(run, kind="chat")
    session.add(run)
    await session.commit()
    yield _sse_frame("thread", {"thread_id": thread.id, "thread_title": thread.title, "run_id": run.id})
    engine = await _resolve_query_engine(request, session)
    owns_engine = engine is not request.app.state.query_engine
    assistant_message_id = allocate_chat_message_id()
    try:
        async with chat_trace(
            session=session,
            chat_message_id=assistant_message_id,
            workspace_id=thread.workspace_id,
            user_id=user.id,
            thread_id=thread.id,
            question=payload.question,
        ) as trace:
            response = await answer_question(
                session,
                payload.question,
                thread.id,
                payload.model,
                tool_engine=engine,
                user_email=user.email,
                connector_slug=payload.connector_slug,
                run_id=run.id,
            )
            if response.get("tool_call") or response.get("tool_result"):
                yield _sse_frame(
                    "tool_result",
                    {
                        "tool_call": response.get("tool_call"),
                        "status": response.get("status") or response.get("llm_status"),
                        "result": response.get("tool_result"),
                    },
                )
            for chunk in _answer_chunks(str(response.get("answer") or "")):
                if await _chat_run_cancelled(session, run.id):
                    await _mark_chat_run_cancelled(session, run)
                    yield _sse_frame("cancelled", {"run_id": run.id})
                    return
                yield _sse_frame("delta", {"content": chunk})
            response = await _persist_chat_response(
                session,
                thread,
                payload,
                response,
                assistant_message_id=assistant_message_id,
                trace_id=trace.trace_id,
                langfuse_url=trace.langfuse_url,
            )
        await _mark_chat_run_completed(session, run, response.get("answer", ""))
        yield _sse_frame("done", response)
    except Exception as exc:
        await _mark_chat_run_failed(session, run, exc)
        yield _sse_frame("error", {"detail": "Chat generation failed.", "type": exc.__class__.__name__})
    finally:
        if owns_engine:
            await engine.dispose()


async def _chat_run_cancelled(session: AsyncSession, run_id: str) -> bool:
    run = await session.scalar(
        select(AgentRun)
        .where(AgentRun.id == run_id)
        .execution_options(populate_existing=True)
    )
    return run is not None and run.state == "cancelled"


async def _mark_chat_run_completed(session: AsyncSession, run: AgentRun, answer: str) -> None:
    finished_at = datetime.now(UTC)
    run.status = "completed"
    run.state = "completed"
    run.summary = answer[:500] or "Chat response completed."
    run.finished_at = finished_at
    run.duration_ms = _duration_ms(run.started_at, finished_at)
    run.timeline = [*run.timeline, {"event": "completed", "at": finished_at.isoformat()}]
    await session.commit()


async def _mark_chat_run_cancelled(session: AsyncSession, run: AgentRun) -> None:
    finished_at = datetime.now(UTC)
    run.status = "cancelled"
    run.state = "cancelled"
    run.summary = "Chat generation cancelled."
    run.finished_at = finished_at
    run.duration_ms = _duration_ms(run.started_at, finished_at)
    run.timeline = [*run.timeline, {"event": "cancelled", "at": finished_at.isoformat()}]
    await session.commit()


async def _mark_chat_run_failed(session: AsyncSession, run: AgentRun, exc: Exception) -> None:
    finished_at = datetime.now(UTC)
    run.status = "failed"
    run.state = "failed"
    run.summary = f"Chat generation failed: {exc.__class__.__name__}"
    run.error_message = exc.__class__.__name__
    run.finished_at = finished_at
    run.duration_ms = _duration_ms(run.started_at, finished_at)
    run.timeline = [*run.timeline, {"event": "failed", "at": finished_at.isoformat(), "error": exc.__class__.__name__}]
    await session.commit()


def _duration_ms(started_at: datetime | None, finished_at: datetime) -> int:
    if started_at is None:
        return 0
    if started_at.tzinfo is None:
        started_at = started_at.replace(tzinfo=UTC)
    return int((finished_at - started_at).total_seconds() * 1000)


@app.post("/chat/runs/{run_id}/cancel")
async def cancel_chat_run(
    run_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    run = await session.get(AgentRun, run_id)
    if run is None or run.agent_name != "chat":
        raise HTTPException(status_code=404, detail="Chat run not found.")
    if run.state not in {"completed", "failed", "cancelled"}:
        await _mark_chat_run_cancelled(session, run)
    return {"id": run.id, "state": run.state, "status": run.status}


async def _persist_chat_response(
    session: AsyncSession,
    thread: ChatThread,
    payload: ChatRequest,
    response: dict[str, Any],
    *,
    assistant_message_id: str | None = None,
    trace_id: str | None = None,
    langfuse_url: str | None = None,
) -> dict[str, Any]:
    citations = list(response.get("citations") or [])
    # Stash the connector+tool that produced this answer so follow-up turns
    # can cite it factually instead of guessing (was: "which connector did
    # that run against?" hallucinating PostgreSQL after a SQLite call).
    tool_call = response.get("tool_call")
    if isinstance(tool_call, dict) and tool_call.get("connector_slug"):
        citations.insert(0, {
            "type": "tool_call_provenance",
            "connector": tool_call.get("connector_slug"),
            "tool": tool_call.get("tool"),
        })
    user_message = ChatMessage(thread_id=thread.id, role="user", content=payload.question)
    assistant_kwargs: dict[str, Any] = {
        "thread_id": thread.id,
        "role": "assistant",
        "content": response.get("answer", ""),
        "sql": response.get("sql"),
        "provider": response.get("provider"),
        "llm_status": response.get("llm_status"),
        "citations": citations,
        "rows": response.get("rows") or [],
        "chart_spec": response.get("chart_spec"),
        "action": response.get("action"),
        "retrieval_trace": response.get("retrieval_trace") or {},
        "trace_id": trace_id,
    }
    if assistant_message_id is not None:
        assistant_kwargs["id"] = assistant_message_id
    assistant_message = ChatMessage(**assistant_kwargs)
    session.add_all([user_message, assistant_message])
    if thread.title in {"New conversation", ""}:
        thread.title = payload.question.strip()[:80] or thread.title
    await session.commit()
    response["thread_id"] = thread.id
    response["thread_title"] = thread.title
    response["message_id"] = assistant_message.id
    if trace_id is not None:
        response["trace_id"] = trace_id
    if langfuse_url is not None:
        response["langfuse_url"] = langfuse_url
    return response


async def _resolve_query_engine(
    request: Request,
    session: AsyncSession,
    connector_slug: str | None = None,
):
    # When no connector_slug is provided ("Auto source"), return the demo
    # engine. The MCP dispatcher gives this engine to SQLite tool calls; for
    # postgres/mysql/redshift the dispatcher builds its own per-connector engine
    # (see _sql_datastore_tool in mcp_executor.py), so the default only matters
    # for the SQLite path. Defaulting to "postgres" used to build a Postgres
    # engine that then poisoned SQLite tool calls with "relation does not
    # exist" errors.
    if connector_slug is None:
        return request.app.state.query_engine
    slug = connector_slug
    if slug not in {"postgres", "mysql", "redshift", "trino", "sqlite"}:
        raise HTTPException(status_code=400, detail=f"Connector {connector_slug} is not supported for /ide/query.")
    connector = await session.scalar(select(Connector).where(Connector.slug == slug))
    if slug == "sqlite":
        if connector and connector.sync_summary:
            path = connector.sync_summary.get("database_path")
            if path:
                return create_async_engine(f"sqlite+aiosqlite:///{path}")
        if connector_slug:
            raise HTTPException(status_code=400, detail="Connector sqlite is not configured for /ide/query.")
        return request.app.state.query_engine
    if connector and connector.encrypted_credentials:
        try:
            credentials = decrypt_json(get_settings().master_key, connector.encrypted_credentials)
        except Exception as exc:
            logger.exception("query_engine_credentials_decrypt_failed", extra={"connector_slug": slug})
            raise HTTPException(status_code=503, detail=f"Connector {slug} credentials could not be decrypted.") from exc
        try:
            if slug == "postgres":
                url = PostgresAdapter._build_url(credentials)
                if not url:
                    raise HTTPException(status_code=400, detail="Connector postgres is missing database credentials.")
            else:
                url = _sqlalchemy_url_for_datastore(slug, credentials)
            return create_async_engine(url, pool_pre_ping=True)
        except McpExecutionError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
    if connector_slug:
        raise HTTPException(status_code=400, detail=f"Connector {connector_slug} is not configured for /ide/query.")
    sqlite_connector = await session.scalar(select(Connector).where(Connector.slug == "sqlite"))
    if sqlite_connector and sqlite_connector.sync_summary:
        path = sqlite_connector.sync_summary.get("database_path")
        if path:
            return create_async_engine(f"sqlite+aiosqlite:///{path}")
    return request.app.state.query_engine


@app.post("/ide/query")
async def ide_query(
    payload: QueryRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    try:
        sql = validate_read_only_sql(payload.sql, payload.limit)
    except UnsafeSqlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if payload.connector_slug == "trino":
        connector = await session.scalar(select(Connector).where(Connector.slug == "trino"))
        if connector is None or not connector.encrypted_credentials:
            raise HTTPException(status_code=400, detail="Connector trino is not configured for /ide/query.")
        try:
            credentials = decrypt_json(get_settings().master_key, connector.encrypted_credentials)
            rows = await asyncio.to_thread(_trino_fetch, credentials, sql)
        except Exception as exc:
            raise _safe_http_error("Read-only query failed.", exc) from exc
        return {"sql": sql, "rows": rows, "read_only": True, "status": "ok"}
    engine = await _resolve_query_engine(request, session, payload.connector_slug)
    owns_engine = engine is not request.app.state.query_engine
    try:
        async with engine.connect() as conn:
            result = await conn.execute(text(sql))
            rows = [dict(row._mapping) for row in result.fetchall()]
    except Exception as exc:
        raise _safe_http_error("Read-only query failed.", exc) from exc
    finally:
        if owns_engine:
            await engine.dispose()
    return {"sql": sql, "rows": rows, "read_only": True, "status": "ok"}


@app.get("/agents/dashboard")
async def agents_dashboard(session: AsyncSession = Depends(get_session), user: User = Depends(current_user)):
    runs = list((await session.scalars(select(AgentRun).order_by(AgentRun.created_at.desc()).limit(100))).all())
    alerts = list((await session.scalars(select(Alert).order_by(Alert.created_at.desc()).limit(100))).all())
    connectors = list((await session.scalars(select(Connector))).all())
    one_hour_ago = datetime.now(UTC) - timedelta(hours=1)

    def _within_last_hour(value: datetime) -> bool:
        if value.tzinfo is None:
            value = value.replace(tzinfo=UTC)
        return value >= one_hour_ago

    recent_runs = [run for run in runs if _within_last_hour(run.created_at)]
    recent_alerts = [alert for alert in alerts if _within_last_hour(alert.created_at)]
    agent_cards = [
        {
            "name": run.agent_name,
            "status": run.status,
            "detail": run.summary,
        }
        for run in runs[:3]
    ]
    if not agent_cards:
        agent_cards = [{"name": "Metadata Agent", "status": "idle", "detail": "No runs yet."}]
    return {
        "last_hour_feed": [
            {"type": "run", "title": run.agent_name, "detail": run.summary, "status": run.status}
            for run in recent_runs[:5]
        ]
        + [
            {"type": "alert", "title": alert.title, "detail": alert.detail, "status": alert.severity}
            for alert in recent_alerts[:5]
        ],
        "agent_cards": agent_cards,
        "connectors": [
            {"slug": item.slug, "name": item.display_name, "status": item.status, "category": item.category}
            for item in connectors
        ],
        "alerts": [
            {"id": alert.id, "severity": alert.severity, "title": alert.title, "detail": alert.detail, "resolved": alert.resolved}
            for alert in alerts
        ],
        "runs": [
            {"id": run.id, "agent_name": run.agent_name, "status": run.status, "summary": run.summary, "timeline": run.timeline}
            for run in runs
        ],
    }


def _ensure_aware(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value


def _tool_call_payload(row: AgentToolCall) -> dict:
    return {
        "id": row.id,
        "run_id": row.run_id,
        "agent_name": row.agent_name,
        "tool_name": row.tool_name,
        "connector_slug": row.connector_slug,
        "args_json": row.args_json,
        "result_summary": row.result_summary,
        "result_size_bytes": row.result_size_bytes,
        "latency_ms": row.latency_ms,
        "status": row.status,
        "error_message": row.error_message,
        "called_at": _ensure_aware(row.called_at).isoformat(),
    }


def _alert_event(
    alert: Alert,
    audit: AgentWriteAudit | None = None,
    tool_calls: list[AgentToolCall] | None = None,
) -> dict:
    if alert.resolved or alert.resolved_at is not None:
        state = "resolved"
    elif alert.acknowledged_at is not None:
        state = "acknowledged"
    elif alert.requires_approval:
        state = "needs_approval"
    else:
        state = "open"
    return {
        "id": alert.id,
        "kind": "alert",
        "fingerprint": alert.fingerprint,
        "timestamp": _ensure_aware(alert.created_at).isoformat(),
        "severity": alert.severity,
        "title": alert.title,
        "detail": alert.detail,
        "state": state,
        "requires_approval": alert.requires_approval,
        "acknowledged_at": alert.acknowledged_at.isoformat() if alert.acknowledged_at else None,
        "acknowledged_by": alert.acknowledged_by,
        "resolved_at": alert.resolved_at.isoformat() if alert.resolved_at else None,
        "resolved_by": alert.resolved_by,
        "connector_slug": audit.connector_slug if audit else None,
        "logo_key": CATALOG_BY_SLUG[audit.connector_slug].logo_key if audit and audit.connector_slug in CATALOG_BY_SLUG else None,
        "tool_calls": [_tool_call_payload(row) for row in tool_calls or []],
        "actions": (
            ["approve", "acknowledge", "resolve"]
            if state == "needs_approval"
            else ["acknowledge", "resolve"]
            if state in {"open", "acknowledged"}
            else []
        ),
    }


def _run_event(run: AgentRun, agent_icon_key: str | None = None, tool_calls: list[AgentToolCall] | None = None) -> dict:
    severity = "info"
    if run.status == "failed":
        severity = "critical"
    elif run.status == "completed":
        severity = "info"
    return {
        "id": run.id,
        "kind": "agent_run",
        "timestamp": _ensure_aware(run.created_at).isoformat(),
        "severity": severity,
        "title": f"{run.agent_name} — {run.status}",
        "detail": run.summary,
        "state": run.status,
        "agent_name": run.agent_name,
        "agent_icon_key": agent_icon_key,
        "duration_ms": run.duration_ms,
        "error_message": run.error_message,
        "requires_approval": False,
        "timeline": run.timeline,
        "tool_calls": [_tool_call_payload(row) for row in tool_calls or []],
        "actions": [],
    }


def _audit_payload(row: AgentWriteAudit) -> dict:
    return {
        "id": row.id,
        "workspace_id": row.workspace_id,
        "agent_id": row.agent_id,
        "connector_slug": row.connector_slug,
        "statement_type": row.statement_type,
        "statement": row.statement,
        "target": row.target,
        "affected_rows": row.affected_rows,
        "required_approval": row.required_approval,
        "alert_id": row.alert_id,
        "executed_at": _ensure_aware(row.executed_at).isoformat(),
        "executed_by": row.executed_by,
    }


@app.get("/agents/{agent_id}/audit")
async def agent_audit(
    agent_id: str,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    await _get_agent_or_404(session, agent_id)
    rows = list(
        (
            await session.scalars(
                select(AgentWriteAudit)
                .where(AgentWriteAudit.agent_id == agent_id)
                .order_by(AgentWriteAudit.executed_at.desc())
                .limit(max(1, min(limit, 500)))
            )
        ).all()
    )
    return [_audit_payload(row) for row in rows]


@app.get("/audit")
async def audit(
    slug: str | None = None,
    from_: datetime | None = Query(None, alias="from"),
    to: datetime | None = None,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    stmt = select(AgentWriteAudit).order_by(AgentWriteAudit.executed_at.desc())
    if slug:
        stmt = stmt.where(AgentWriteAudit.connector_slug == slug)
    if from_:
        stmt = stmt.where(AgentWriteAudit.executed_at >= from_)
    if to:
        stmt = stmt.where(AgentWriteAudit.executed_at <= to)
    rows = list((await session.scalars(stmt.limit(max(1, min(limit, 500))))).all())
    return [_audit_payload(row) for row in rows]


@app.get("/observability/events")
async def observability_events(
    kind: str | None = None,
    severity: str | None = None,
    state: str | None = None,
    q: str | None = None,
    limit: int = 100,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    events: list[dict] = []
    if settings.observability_mock:
        events = [dict(event) for event in MOCK_EVENTS]
        if kind:
            events = [event for event in events if event["kind"] == kind]
        if severity:
            events = [event for event in events if event["severity"] == severity]
        if state:
            events = [event for event in events if event["state"] == state]
        if q:
            needle = q.lower()
            events = [
                event
                for event in events
                if needle in event["title"].lower() or needle in (event["detail"] or "").lower()
            ]
        events.sort(key=lambda event: event["timestamp"], reverse=True)
        return {
            "total": len(events),
            "needs_approval": sum(1 for event in events if event["state"] == "needs_approval"),
            "events": events[: max(0, limit)],
        }
    if kind in (None, "alert"):
        alerts = list((await session.scalars(select(Alert).order_by(Alert.created_at.desc()))).all())
        audits = list(
            (
                await session.scalars(
                    select(AgentWriteAudit).where(AgentWriteAudit.alert_id.is_not(None))
                )
            ).all()
        )
        audit_by_alert = {audit.alert_id: audit for audit in audits}
        alert_tool_calls = list(
            (
                await session.scalars(
                    select(AgentToolCall)
                    .where(AgentToolCall.status == "error")
                    .order_by(AgentToolCall.called_at.desc())
                    .limit(25)
                )
            ).all()
        )
        alert_calls_by_connector: dict[str, list[AgentToolCall]] = {}
        for call in alert_tool_calls:
            if call.connector_slug:
                alert_calls_by_connector.setdefault(call.connector_slug, []).append(call)
        events.extend(
            _alert_event(
                alert,
                audit_by_alert.get(alert.id),
                alert_calls_by_connector.get(audit_by_alert[alert.id].connector_slug, [])[:5]
                if alert.id in audit_by_alert
                else [],
            )
            for alert in alerts
        )
    if kind in (None, "agent_run"):
        runs = list((await session.scalars(select(AgentRun).order_by(AgentRun.created_at.desc()))).all())
        agents = list((await session.scalars(select(Agent))).all())
        icon_by_display_name = {agent.display_name: agent.icon_key for agent in agents}
        run_ids = [run.id for run in runs]
        tool_calls = (
            list(
                (
                    await session.scalars(
                        select(AgentToolCall)
                        .where(AgentToolCall.run_id.in_(run_ids))
                        .order_by(AgentToolCall.called_at.desc())
                    )
                ).all()
            )
            if run_ids
            else []
        )
        calls_by_run: dict[str, list[AgentToolCall]] = {}
        for call in tool_calls:
            if call.run_id:
                calls_by_run.setdefault(call.run_id, []).append(call)
        events.extend(_run_event(run, icon_by_display_name.get(run.agent_name), calls_by_run.get(run.id, [])[:10]) for run in runs)

    if severity:
        events = [event for event in events if event["severity"] == severity]
    if state:
        events = [event for event in events if event["state"] == state]
    if q:
        needle = q.lower()
        events = [
            event
            for event in events
            if needle in event["title"].lower() or needle in (event["detail"] or "").lower()
        ]
    events.sort(key=lambda event: event["timestamp"], reverse=True)
    return {
        "total": len(events),
        "needs_approval": sum(1 for event in events if event["state"] == "needs_approval"),
        "events": events[: max(0, limit)],
    }


@app.get("/observability/logs")
async def observability_logs(
    level: str | None = None,
    logger: str | None = None,
    q: str | None = None,
    since: str | None = None,
    limit: int = 200,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    stmt = select(LogEntry).order_by(LogEntry.timestamp.desc())
    if level:
        stmt = stmt.where(LogEntry.level == level.upper())
    if logger:
        stmt = stmt.where(LogEntry.logger_name == logger)
    if since:
        try:
            cutoff = datetime.fromisoformat(since.replace("Z", "+00:00"))
        except ValueError as exc:
            raise HTTPException(status_code=400, detail="Invalid 'since' (ISO-8601 required).") from exc
        stmt = stmt.where(LogEntry.timestamp >= cutoff)
    if q:
        needle = f"%{q.lower()}%"
        stmt = stmt.where(LogEntry.message.ilike(needle))
    stmt = stmt.limit(max(1, min(limit, 1000)))
    entries = list((await session.scalars(stmt)).all())
    return {
        "total": len(entries),
        "entries": [
            {
                "id": entry.id,
                "timestamp": _ensure_aware(entry.timestamp).isoformat(),
                "level": entry.level,
                "logger": entry.logger_name,
                "message": entry.message,
                "context": entry.context or {},
                "exception": entry.exception,
            }
            for entry in entries
        ],
    }


@app.post("/alerts/{alert_id}/acknowledge")
async def acknowledge_alert(
    alert_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    alert = await session.get(Alert, alert_id)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found.")
    if alert.acknowledged_at is None:
        alert.acknowledged_at = datetime.now(UTC)
        alert.acknowledged_by = user.email
    await session.commit()
    return _alert_event(alert)


@app.post("/alerts/{alert_id}/approve-and-execute")
async def approve_and_execute_alert(
    alert_id: str,
    request: Request,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    alert = await session.get(Alert, alert_id)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found.")
    if not alert.requires_approval:
        raise HTTPException(status_code=400, detail="Alert does not require execution approval.")
    detail = alert.detail or ""
    mcp_action: str | None = None
    mcp_agent_id: str | None = None
    mcp_arguments: dict[str, Any] = {}
    for line in detail.splitlines():
        if line.startswith("MCP-Action:"):
            mcp_action = line.split(":", 1)[1].strip() or None
        if line.startswith("Agent-ID:"):
            mcp_agent_id = line.split(":", 1)[1].strip() or None
        if line.startswith("Arguments:"):
            try:
                parsed = json.loads(line.split(":", 1)[1].strip() or "{}")
            except json.JSONDecodeError as exc:
                raise HTTPException(status_code=400, detail="Approved MCP arguments are invalid.") from exc
            if not isinstance(parsed, dict):
                raise HTTPException(status_code=400, detail="Approved MCP arguments must be an object.")
            mcp_arguments = parsed
    if mcp_action:
        if "." not in mcp_action:
            raise HTTPException(status_code=400, detail="Approved MCP action is invalid.")
        connector_slug, tool_name = mcp_action.split(".", 1)
        engine = await _resolve_query_engine(
            request,
            session,
            connector_slug if connector_slug in {"postgres", "mysql", "redshift", "sqlite"} else None,
        )
        owns_engine = engine is not request.app.state.query_engine
        try:
            result = await execute_mcp_tool(
                session=session,
                engine=engine,
                connector_slug=connector_slug,
                tool_name=tool_name,
                arguments={**mcp_arguments, "__approved": True},
                agent_id=mcp_agent_id,
                user_email=user.email,
            )
        except McpExecutionError as exc:
            raise HTTPException(status_code=exc.status_code, detail=exc.detail) from exc
        except httpx.HTTPStatusError as exc:
            vendor_status = exc.response.status_code
            vendor_detail = exc.response.text[:500] if exc.response.text else ""
            raise HTTPException(
                status_code=502,
                detail=(
                    f"{connector_slug} API returned HTTP {vendor_status}"
                    + (f": {vendor_detail}" if vendor_detail else "")
                ),
            ) from exc
        except (httpx.TimeoutException, httpx.TransportError) as exc:
            raise HTTPException(
                status_code=504,
                detail=f"{connector_slug} could not be reached.",
            ) from exc
        except Exception as exc:
            raise _safe_http_error("Approved MCP tool failed.", exc, status_code=500) from exc
        finally:
            if owns_engine:
                await engine.dispose()
        alert.acknowledged_at = datetime.now(UTC)
        alert.acknowledged_by = user.email
        alert.resolved = True
        alert.resolved_at = alert.acknowledged_at
        alert.resolved_by = user.email
        await session.commit()
        return {"status": "executed", "alert": _alert_event(alert), "result": result}
    sql = detail.split("SQL:", 1)[-1].strip()
    connector_slug = "sqlite"
    agent_id: str | None = None
    for line in detail.splitlines():
        if line.startswith("Connector:"):
            connector_slug = line.split(":", 1)[1].strip() or "sqlite"
        if line.startswith("Agent-ID:"):
            agent_id = line.split(":", 1)[1].strip() or None
    try:
        decision = validate_write_sql(sql)
    except UnsafeSqlError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    if decision.action != "requires_approval":
        raise HTTPException(status_code=400, detail="Only approval-tier SQL can be executed here.")
    if connector_slug in {"postgres", "mysql", "redshift"}:
        connector = await session.scalar(select(Connector).where(Connector.slug == connector_slug))
        if connector is None or not connector.encrypted_credentials:
            raise HTTPException(status_code=400, detail=f"Connector {connector_slug} is not configured.")
        credentials = decrypt_json(get_settings().master_key, connector.encrypted_credentials)
        engine = create_async_engine(_sqlalchemy_url_for_datastore(connector_slug, credentials), pool_pre_ping=True)
        owns_engine = True
    elif connector_slug == "sql_server":
        connector = await session.scalar(select(Connector).where(Connector.slug == connector_slug))
        if connector is None or not connector.encrypted_credentials:
            raise HTTPException(status_code=400, detail="Connector sql_server is not configured.")
        credentials = decrypt_json(get_settings().master_key, connector.encrypted_credentials)
        adapter = adapter_for("sql_server")

        def run_sql_server_write() -> int | None:
            with adapter._connect_sync(credentials) as conn:  # type: ignore[attr-defined]
                with conn.cursor() as cursor:
                    cursor.execute(decision.sql)
                    affected_rows = cursor.rowcount if cursor.rowcount is not None and cursor.rowcount >= 0 else None
                conn.commit()
                return affected_rows

        try:
            affected = await asyncio.to_thread(run_sql_server_write)
        except Exception as exc:
            raise _safe_http_error("Approved SQL failed.", exc) from exc
        engine = None
        owns_engine = False
    elif connector_slug == "trino":
        connector = await session.scalar(select(Connector).where(Connector.slug == connector_slug))
        if connector is None or not connector.encrypted_credentials:
            raise HTTPException(status_code=400, detail="Connector trino is not configured.")
        credentials = decrypt_json(get_settings().master_key, connector.encrypted_credentials)
        try:
            affected = await asyncio.to_thread(_trino_execute, credentials, decision.sql)
        except Exception as exc:
            raise _safe_http_error("Approved SQL failed.", exc) from exc
        engine = None
        owns_engine = False
    else:
        engine = await _resolve_query_engine(request, session)
        owns_engine = engine is not request.app.state.query_engine
    if connector_slug not in {"sql_server", "trino"}:
        try:
            async with engine.begin() as conn:
                result = await conn.execute(text(decision.sql))
                affected = result.rowcount if result.rowcount is not None and result.rowcount >= 0 else None
        except Exception as exc:
            raise _safe_http_error("Approved SQL failed.", exc) from exc
        finally:
            if owns_engine:
                await engine.dispose()
    alert.acknowledged_at = datetime.now(UTC)
    alert.acknowledged_by = user.email
    alert.resolved = True
    alert.resolved_at = alert.acknowledged_at
    alert.resolved_by = user.email
    workspace = await _workspace(session)
    write_agent = await session.get(Agent, agent_id) if agent_id else None
    chat_agent = write_agent or await session.scalar(select(Agent).where(Agent.name == "chat"))
    session.add(
        AgentWriteAudit(
            workspace_id=workspace.id,
            agent_id=chat_agent.id if chat_agent else None,
            connector_slug=connector_slug,
            statement_type=decision.statement_type,
            statement=decision.sql,
            target=decision.target,
            affected_rows=affected,
            required_approval=True,
            alert_id=alert.id,
            executed_at=datetime.now(UTC),
            executed_by=user.email,
        )
    )
    session.add(
        LogEntry(
            timestamp=datetime.now(UTC),
            level="info",
            logger_name="dataclaw.mcp",
            message="approved_write_sql_executed",
            context={
                "agent_id": chat_agent.id if chat_agent else None,
                "connector_slug": connector_slug,
                "statement_type": decision.statement_type,
                "target": decision.target,
                "executor": user.email,
            },
        )
    )
    logger.info("approved_write_sql_executed", extra={"_alert_id": alert.id, "_executor": user.email})
    await session.commit()
    return {"status": "executed", "alert": _alert_event(alert)}


def _provider_summary(slug: str, stored: dict) -> dict:
    definition = LLM_CATALOG_BY_SLUG[slug]
    secret_fields = {field.name for field in definition.fields if field.secret}
    visible: dict[str, str] = {}
    secrets_set: list[str] = []
    secret_previews: dict[str, str] = {}
    for field in definition.fields:
        value = stored.get(field.name)
        if field.secret:
            if value:
                secrets_set.append(field.name)
                if len(value) >= 4:
                    secret_previews[field.name] = f"…{value[-4:]}"
        elif value is not None:
            visible[field.name] = value
    return {
        "slug": slug,
        "configured": any(name in stored for name in secret_fields) if secret_fields else bool(stored),
        "values": visible,
        "secrets_set": secrets_set,
        "secret_previews": secret_previews,
    }


def _catalog_payload(item) -> dict:
    return {
        "slug": item.slug,
        "display_name": item.display_name,
        "logo_key": item.logo_key,
        "docs_url": item.docs_url,
        "description": item.description,
        "default_model": item.default_model,
        "default_embedding_model": item.default_embedding_model,
        "wired": item.wired,
        "fields": [field.model_dump() for field in item.fields],
    }


def _merged_provider_values(definition, stored: dict[str, Any], updates: dict[str, Any]) -> dict[str, Any]:
    values = dict(stored)
    for field in definition.fields:
        if updates.get(field.name) is not None:
            values[field.name] = updates[field.name]
    return values


async def _test_ollama_provider(values: dict[str, Any]) -> dict[str, str]:
    base_url = str(values.get("base_url") or "http://localhost:11434/v1").rstrip("/")
    model = str(values.get("model") or LLM_CATALOG_BY_SLUG["ollama"].default_model)
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.get(f"{base_url}/models")
            response.raise_for_status()
    except httpx.RequestError:
        return {"status": "error", "message": f"Ollama not reachable at {base_url}"}
    except httpx.HTTPStatusError as exc:
        return {"status": "error", "message": f"Ollama not reachable at {base_url} ({exc.response.status_code})"}

    payload = response.json()
    models = payload.get("data") if isinstance(payload, dict) else []
    pulled = {str(item.get("id") or item.get("name")) for item in models if isinstance(item, dict)}
    if model not in pulled:
        return {"status": "error", "message": f"Model '{model}' not pulled. Run: ollama pull {model}"}
    return {"status": "ok", "message": f"Ollama is reachable at {base_url}; model '{model}' is available."}


@app.get("/llm/catalog")
async def read_llm_catalog(user: User = Depends(current_user)):
    return [_catalog_payload(item) for item in llm_catalog()]


@app.get("/llm/providers")
async def read_llm_providers(
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    stored = await list_llm_providers(session)
    return [_provider_summary(slug, stored.get(slug, {})) for slug in LLM_CATALOG_BY_SLUG]


@app.put("/llm/providers/{slug}")
async def write_llm_provider(
    slug: str,
    payload: LlmProviderUpdate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    if slug not in LLM_CATALOG_BY_SLUG:
        raise HTTPException(status_code=404, detail=f"Unknown LLM provider: {slug}")
    await update_llm_provider(session, slug, payload.values)
    await session.commit()
    stored = await get_llm_provider(session, slug)
    return _provider_summary(slug, stored)


@app.post("/llm/providers/{slug}/test")
async def test_llm_provider(
    slug: str,
    payload: LlmProviderUpdate,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    definition = LLM_CATALOG_BY_SLUG.get(slug)
    if definition is None:
        raise HTTPException(status_code=404, detail=f"Unknown LLM provider: {slug}")
    stored = await get_llm_provider(session, slug)
    values = _merged_provider_values(definition, stored, payload.values)
    if slug == "ollama":
        return await _test_ollama_provider(values)
    missing = [
        field.label
        for field in definition.fields
        if field.required and field.secret and not values.get(field.name)
    ]
    if missing:
        return {"status": "error", "message": f"Missing required field: {missing[0]}"}
    return {"status": "ok", "message": f"{definition.display_name} settings are saved."}


@app.get("/monitoring/agents")
async def list_monitoring_agents(user: User = Depends(current_user)):
    from app.services.agents.monitoring_common import MONITORING_AGENTS
    return [
        {
            "name": agent_name,
            "display_name": meta["display_name"],
            "supported_connectors": list(meta["connectors"]),
            "default_thresholds": dict(meta.get("thresholds", {})),
        }
        for agent_name, meta in MONITORING_AGENTS.items()
    ]


@app.get("/monitoring/configs")
async def list_monitoring_configs(
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
):
    workspace = await _workspace(session)
    legacy_rows = (
        await session.scalars(
        select(MonitoringConfig).where(MonitoringConfig.workspace_id == workspace.id).order_by(MonitoringConfig.created_at)
        )
    )
    configs = [
        {
            "id": cfg.id,
            "agent_name": cfg.agent_name,
            "connector_id": cfg.connector_id,
            "enabled": cfg.enabled,
            "thresholds": cfg.thresholds or {},
            "notification_channels": cfg.notification_channels or {},
        }
        for cfg in legacy_rows.all()
    ]
    from app.services.agents.monitoring_common import MONITORING_AGENTS

    for legacy_name, unified_name in LEGACY_MONITORING_AGENT_MAP.items():
        agent = await session.scalar(
            select(Agent).where(
                Agent.workspace_id == workspace.id,
                Agent.name == unified_name,
                Agent.kind == "background",
            )
        )
        if agent is None:
            continue
        allowed = set(MONITORING_AGENTS[legacy_name]["connectors"])
        grants = list(
            (
                await session.scalars(
                    select(AgentMcpGrant).where(
                        AgentMcpGrant.agent_id == agent.id,
                        AgentMcpGrant.connector_slug.in_(allowed),
                    )
                )
            ).all()
        )
        if not grants:
            continue
        connectors = {
            connector.slug: connector
            for connector in (
                await session.scalars(
                    select(Connector).where(
                        Connector.workspace_id == workspace.id,
                        Connector.slug.in_([grant.connector_slug for grant in grants]),
                    )
                )
            ).all()
        }
        for grant in grants:
            connector = connectors.get(grant.connector_slug)
            if connector is None:
                continue
            configs.append(
                {
                    "id": f"unified:{agent.id}:{connector.id}:{legacy_name}",
                    "agent_name": legacy_name,
                    "connector_id": connector.id,
                    "enabled": grant.read_enabled,
                    "thresholds": agent.thresholds or {},
                    "notification_channels": {},
                }
            )
    return configs


class _MonitoringConfigUpsert(BaseModel):
    agent_name: str
    connector_id: str
    enabled: bool = False
    thresholds: dict[str, Any] = Field(default_factory=dict)
    notification_channels: dict[str, Any] = Field(default_factory=dict)


@app.put("/monitoring/configs")
async def upsert_monitoring_config(
    payload: _MonitoringConfigUpsert,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    workspace = await _workspace(session)
    connector = await session.get(Connector, payload.connector_id)
    if connector is None or connector.workspace_id != workspace.id:
        raise HTTPException(status_code=404, detail="Connector not found.")
    unified_name = LEGACY_MONITORING_AGENT_MAP.get(payload.agent_name)
    if unified_name is None:
        raise HTTPException(status_code=400, detail="Unknown monitoring agent.")
    agent = await session.scalar(
        select(Agent).where(
            Agent.workspace_id == workspace.id,
            Agent.name == unified_name,
            Agent.kind == "background",
        )
    )
    if agent is None:
        raise HTTPException(status_code=404, detail=f"Unified agent not found: {unified_name}")
    grant = await session.scalar(
        select(AgentMcpGrant).where(
            AgentMcpGrant.agent_id == agent.id,
            AgentMcpGrant.connector_slug == connector.slug,
        )
    )
    if grant is None:
        grant = AgentMcpGrant(agent_id=agent.id, connector_slug=connector.slug)
        session.add(grant)
    grant.read_enabled = payload.enabled
    if payload.thresholds:
        agent.thresholds = payload.thresholds
    await session.commit()
    return {
        "id": f"unified:{agent.id}:{connector.id}:{payload.agent_name}",
        "agent_name": payload.agent_name,
        "connector_id": connector.id,
        "enabled": grant.read_enabled,
        "thresholds": agent.thresholds or {},
        "notification_channels": {},
    }


@app.post("/alerts/{alert_id}/resolve")
async def resolve_alert(
    alert_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(require_admin),
):
    alert = await session.get(Alert, alert_id)
    if alert is None:
        raise HTTPException(status_code=404, detail="Alert not found.")
    alert.resolved = True
    alert.resolved_at = datetime.now(UTC)
    alert.resolved_by = user.email
    if alert.acknowledged_at is None:
        alert.acknowledged_at = alert.resolved_at
        alert.acknowledged_by = user.email
    await session.commit()
    return _alert_event(alert)


STATIC_DIR = Path(__file__).parent / "static"
if STATIC_DIR.exists() and (STATIC_DIR / "index.html").exists():
    if (STATIC_DIR / "assets").exists():
        app.mount(
            "/assets",
            StaticFiles(directory=str(STATIC_DIR / "assets")),
            name="assets",
        )

    @app.get("/", include_in_schema=False)
    async def serve_index() -> FileResponse:
        return FileResponse(STATIC_DIR / "index.html")

    @app.get("/{spa_path:path}", include_in_schema=False)
    async def serve_spa(spa_path: str) -> FileResponse:
        if spa_path.startswith(("docs", "openapi.json")):
            raise HTTPException(status_code=404)
        candidate = (STATIC_DIR / spa_path).resolve()
        try:
            candidate.relative_to(STATIC_DIR.resolve())
        except ValueError as exc:
            raise HTTPException(status_code=404) from exc
        if candidate.is_file():
            return FileResponse(candidate)
        return FileResponse(STATIC_DIR / "index.html")
