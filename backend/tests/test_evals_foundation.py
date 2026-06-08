"""Consolidated tests for the evals foundation (phase 1).

Covers all phase 1 surfaces in one reviewable file:
* tracing service + langfuse_client unit tests
* /feedback, /chat-messages/{id}/trace, /chat-messages/{id}/trace-link API
* /integrations/observability list + upsert + disable + test
* full chat -> tracing -> feedback e2e
* Langfuse SDK forwarding (mocked, no network)
"""

from __future__ import annotations


# ============================================================
# Section: tracing_unit
# ============================================================

import os
import uuid

import pytest
from sqlalchemy import select

# Make sure the test process has the env contract the app expects before any
# app module is imported. The autouse fixture in conftest.py covers
# DATACLAW_VECTOR_TEST_DOUBLE etc, but the master/session secrets must be set
# before app.core.config caches them.
os.environ.setdefault("MASTER_KEY", "test-master-key-please-change")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-please-change")


# ---------- helpers ----------


@pytest.fixture
async def db_tracing(tmp_path, monkeypatch):
    """Per-test sqlite DB with the head schema, returning a SessionLocal factory.

    Imports app.models.domain *before* the create_all so every mapped class
    is registered against Base.metadata. Reloads app.db.session so the engine
    points at the per-test sqlite file.
    """
    import importlib

    db_path = tmp_path / "phase1.sqlite"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("DEMO_DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path/'demo.sqlite'}")

    from app.core.config import get_settings
    get_settings.cache_clear()

    import app.db.session as session_module
    importlib.reload(session_module)

    # Force model registration BEFORE create_all so chat_spans/feedback exist.
    import app.models.domain  # noqa: F401
    from app.db.base import Base

    async with session_module.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield session_module.SessionLocal
    await session_module.engine.dispose()


async def _seed_chat(SessionLocal):
    from app.models.domain import ChatMessage, ChatThread, User, Workspace

    async with SessionLocal() as s:
        ws = Workspace(name="ws")
        s.add(ws)
        await s.flush()
        user = User(email=f"u-{uuid.uuid4()}@local", password_hash="x")
        s.add(user)
        await s.flush()
        thread = ChatThread(workspace_id=ws.id, user_id=user.id, title="t")
        s.add(thread)
        await s.flush()
        msg = ChatMessage(thread_id=thread.id, role="assistant", content="hello")
        s.add(msg)
        await s.commit()
        return ws.id, user.id, thread.id, msg.id


# ---------- tracing service ----------


@pytest.mark.asyncio
async def test_span_outside_chat_trace_is_noop(db_tracing) -> None:
    """`span()` must be safe to call when no chat_trace is active."""
    from app.services.observability.tracing import span as trace_span

    async with trace_span("llm", "outside") as sp:
        sp.set_output({"any": "value"})
        sp.set_usage(prompt_tokens=10)
    # No exception means pass. No spans should be persisted.
    from app.models.domain import ChatSpan

    async with db_tracing() as s:
        rows = (await s.scalars(select(ChatSpan))).all()
        assert list(rows) == []


@pytest.mark.asyncio
async def test_chat_trace_persists_root_and_nested_spans(db_tracing) -> None:
    from app.services.observability.tracing import (
        allocate_chat_message_id,
        chat_trace,
    )
    from app.services.observability.tracing import span as trace_span

    msg_id = allocate_chat_message_id()
    async with db_tracing() as s:
        async with chat_trace(
            session=s,
            chat_message_id=msg_id,
            workspace_id="ws",
            user_id="u",
            thread_id="t",
            question="how many users?",
        ) as trace:
            async with trace_span("retrieval", "brain") as r:
                r.set_output({"node_count": 1})
            async with trace_span("llm", "primary", model="gpt") as llm_span:
                llm_span.set_usage(prompt_tokens=10, completion_tokens=5, total_tokens=15)
            async with trace_span("tool", "sqlite.read_select") as t:
                t.set_output({"status": "ok"})
        # trace_id must be deterministic from the message id when langfuse is
        # off — we strip dashes from the uuid as the fallback.
        assert trace.trace_id == msg_id.replace("-", "")

    from app.models.domain import ChatSpan

    async with db_tracing() as s:
        rows = list(
            (
                await s.scalars(
                    select(ChatSpan)
                    .where(ChatSpan.chat_message_id == msg_id)
                    .order_by(ChatSpan.started_at)
                )
            ).all()
        )
    kinds = [r.kind for r in rows]
    # Ordered by started_at: root opens first, then children in the order
    # they were entered.
    assert kinds == ["root", "retrieval", "llm", "tool"], kinds
    # Children must have parent_span_id == root.id, root must have None.
    root = next(r for r in rows if r.kind == "root")
    assert root.parent_span_id is None
    for child in [r for r in rows if r.kind != "root"]:
        assert child.parent_span_id == root.id
    # LLM span captured usage.
    llm = next(r for r in rows if r.kind == "llm")
    assert llm.usage == {
        "input_tokens": 10,
        "output_tokens": 5,
        "total_tokens": 15,
    }
    assert llm.model == "gpt"


@pytest.mark.asyncio
async def test_chat_trace_records_error_on_exception(db_tracing) -> None:
    from app.services.observability.tracing import (
        allocate_chat_message_id,
        chat_trace,
    )
    from app.services.observability.tracing import span as trace_span

    msg_id = allocate_chat_message_id()
    async with db_tracing() as s:
        with pytest.raises(RuntimeError, match="boom"):
            async with chat_trace(
                session=s,
                chat_message_id=msg_id,
                workspace_id="ws",
                user_id="u",
                thread_id="t",
                question="x",
            ):
                async with trace_span("llm", "primary"):
                    raise RuntimeError("boom")

    from app.models.domain import ChatSpan

    async with db_tracing() as s:
        rows = list((await s.scalars(select(ChatSpan).where(ChatSpan.chat_message_id == msg_id))).all())
    statuses = {r.kind: r.status for r in rows}
    # Both the failing inner span AND the root must be marked error.
    assert statuses["llm"] == "error"
    assert statuses["root"] == "error"
    errors = {r.kind: r.error for r in rows}
    assert "RuntimeError" in (errors["llm"] or "")


@pytest.mark.asyncio
async def test_span_handles_misnested_stack_safely(db_tracing) -> None:
    """If an inner span raises out of order, we still pop only that span and
    keep the trace consistent."""
    from app.services.observability.tracing import (
        allocate_chat_message_id,
        chat_trace,
    )
    from app.services.observability.tracing import span as trace_span

    msg_id = allocate_chat_message_id()
    async with db_tracing() as s:
        async with chat_trace(
            session=s,
            chat_message_id=msg_id,
            workspace_id="ws",
            user_id="u",
            thread_id="t",
            question="x",
        ):
            async with trace_span("retrieval", "r"):
                pass
            try:
                async with trace_span("llm", "raises"):
                    raise ValueError("nope")
            except ValueError:
                pass
            async with trace_span("tool", "after"):
                pass

    from app.models.domain import ChatSpan

    async with db_tracing() as s:
        rows = (await s.scalars(select(ChatSpan).where(ChatSpan.chat_message_id == msg_id))).all()
        kinds = sorted([r.kind for r in rows])
    assert kinds == ["llm", "retrieval", "root", "tool"]


@pytest.mark.asyncio
async def test_safe_jsonable_truncates_large_strings(db_tracing) -> None:
    """A massive blob in span input/output must not break persistence and must
    truncate inside the local store (full payload still goes to Langfuse)."""
    from app.services.observability.tracing import (
        allocate_chat_message_id,
        chat_trace,
    )
    from app.services.observability.tracing import span as trace_span

    big = "x" * 20000
    msg_id = allocate_chat_message_id()
    async with db_tracing() as s:
        async with chat_trace(
            session=s,
            chat_message_id=msg_id,
            workspace_id="ws",
            user_id="u",
            thread_id="t",
            question="x",
        ):
            async with trace_span("tool", "huge", input={"blob": big}) as sp:
                sp.set_output({"blob": big})

    from app.models.domain import ChatSpan

    async with db_tracing() as s:
        row = (
            await s.scalar(
                select(ChatSpan).where(
                    ChatSpan.chat_message_id == msg_id,
                    ChatSpan.kind == "tool",
                )
            )
        )
    assert row is not None
    assert len(row.input["blob"]) < len(big)
    assert row.input["blob"].endswith("[truncated]")
    assert row.output["blob"].endswith("[truncated]")


# ---------- langfuse client ----------


def test_langfuse_config_rejects_disabled_or_partial() -> None:
    from app.services.observability.langfuse_client import LangfuseConfig

    assert LangfuseConfig.from_settings({}) is None
    assert LangfuseConfig.from_settings({"enabled": False, "host": "h"}) is None
    assert (
        LangfuseConfig.from_settings(
            {"enabled": True, "host": "h", "public_key": "pk"}  # missing secret_key
        )
        is None
    )
    cfg = LangfuseConfig.from_settings(
        {
            "enabled": True,
            "host": " https://cloud.langfuse.com ",
            "public_key": " pk ",
            "secret_key": " sk ",
            "project": " myproj ",
        }
    )
    assert cfg is not None
    assert cfg.host == "https://cloud.langfuse.com"
    assert cfg.project == "myproj"


def test_langfuse_resolve_sink_caches_by_config() -> None:
    from app.services.observability import langfuse_client

    langfuse_client.invalidate_cache()
    a = langfuse_client.resolve_sink(
        {"enabled": True, "host": "h", "public_key": "pk", "secret_key": "sk"}
    )
    b = langfuse_client.resolve_sink(
        {"enabled": True, "host": "h", "public_key": "pk", "secret_key": "sk"}
    )
    assert a is b
    # Different config -> different sink object.
    c = langfuse_client.resolve_sink(
        {"enabled": True, "host": "h2", "public_key": "pk", "secret_key": "sk"}
    )
    assert c is not a


def test_langfuse_sink_is_unavailable_when_sdk_missing(monkeypatch) -> None:
    """If the Langfuse SDK is not installed, the sink must report unavailable
    and helpers must return None instead of raising."""
    from app.services.observability import langfuse_client

    # Simulate the SDK import failing without uninstalling it.
    sink = langfuse_client.LangfuseSink(
        langfuse_client.LangfuseConfig(host="h", public_key="pk", secret_key="sk")
    )
    monkeypatch.setattr(sink, "_import_ok", False, raising=False)
    monkeypatch.setattr(sink, "_client", None, raising=False)
    assert sink.is_available() is False
    assert sink.create_trace_id(seed="x") is None
    assert sink.score_trace(trace_id="t", name="n", value=1.0) is None
    sink.flush()  # must not raise


# ---------- feedback service ----------


@pytest.mark.asyncio
async def test_feedback_service_happy_path(db_tracing) -> None:
    from app.models.domain import User
    from app.services.observability.feedback import FeedbackService

    _, user_id, _, msg_id = await _seed_chat(db_tracing)
    async with db_tracing() as s:
        user = await s.get(User, user_id)
        row = await FeedbackService(s).submit(
            chat_message_id=msg_id,
            sentiment="positive",
            user=user,
            comment="great",
        )
        assert row.chat_message_id == msg_id
        assert row.sentiment == "positive"
        assert row.comment == "great"
        assert row.user_id == user_id


@pytest.mark.asyncio
async def test_feedback_service_rejects_bad_sentiment(db_tracing) -> None:
    from app.services.observability.feedback import (
        FeedbackService,
        FeedbackValidationError,
    )

    _, _, _, msg_id = await _seed_chat(db_tracing)
    async with db_tracing() as s:
        with pytest.raises(FeedbackValidationError):
            await FeedbackService(s).submit(
                chat_message_id=msg_id,
                sentiment="meh",  # not in {positive, negative}
            )


@pytest.mark.asyncio
async def test_feedback_service_rejects_user_role(db_tracing) -> None:
    """Feedback on user (not assistant) messages must be rejected — feedback
    is about the *answer*, not the question."""
    from app.models.domain import ChatMessage
    from app.services.observability.feedback import (
        FeedbackService,
        FeedbackValidationError,
    )

    _, _, thread_id, _ = await _seed_chat(db_tracing)
    async with db_tracing() as s:
        user_msg = ChatMessage(thread_id=thread_id, role="user", content="q")
        s.add(user_msg)
        await s.commit()
        user_msg_id = user_msg.id

    async with db_tracing() as s:
        with pytest.raises(FeedbackValidationError):
            await FeedbackService(s).submit(
                chat_message_id=user_msg_id,
                sentiment="positive",
            )


@pytest.mark.asyncio
async def test_feedback_service_404_when_message_missing(db_tracing) -> None:
    from app.services.observability.feedback import (
        FeedbackService,
        FeedbackTargetNotFound,
    )

    async with db_tracing() as s:
        with pytest.raises(FeedbackTargetNotFound):
            await FeedbackService(s).submit(
                chat_message_id="does-not-exist",
                sentiment="positive",
            )


# ---------- settings store ----------


@pytest.mark.asyncio
async def test_observability_provider_round_trip_redacts_secret(db_tracing) -> None:
    from app.services.settings_store import (
        get_observability_provider,
        update_observability_provider,
    )

    async with db_tracing() as s:
        await update_observability_provider(
            s,
            "langfuse",
            {
                "host": "https://cloud.langfuse.com",
                "public_key": "pk-x",
                "secret_key": "sk-x",
                "enabled": True,
            },
        )
        await s.commit()
        got = await get_observability_provider(s, "langfuse")
    assert got["host"] == "https://cloud.langfuse.com"
    assert got["public_key"] == "pk-x"
    assert got["secret_key"] == "sk-x"  # the service stores plaintext; encryption is at the AppSetting row level
    assert got["enabled"] is True


@pytest.mark.asyncio
async def test_observability_provider_unknown_slug_raises(db_tracing) -> None:
    from app.services.settings_store import update_observability_provider

    async with db_tracing() as s:
        with pytest.raises(KeyError):
            await update_observability_provider(s, "wrong-slug", {"enabled": True})


@pytest.mark.asyncio
async def test_observability_provider_disable_does_not_wipe_credentials(db_tracing) -> None:
    """User must be able to toggle off without re-entering keys."""
    from app.services.settings_store import (
        get_observability_provider,
        update_observability_provider,
    )

    async with db_tracing() as s:
        await update_observability_provider(
            s,
            "langfuse",
            {"host": "h", "public_key": "pk", "secret_key": "sk", "enabled": True},
        )
        await s.commit()
        await update_observability_provider(s, "langfuse", {"enabled": False})
        await s.commit()
        got = await get_observability_provider(s, "langfuse")
    assert got["enabled"] is False
    assert got["public_key"] == "pk"
    assert got["secret_key"] == "sk"


def test_coerce_project_id_accepts_cuid_rejects_display_name():
    from app.services.observability.tracing import _coerce_project_id

    assert _coerce_project_id("cmpxnn3z407zead0eqdugizgn") == "cmpxnn3z407zead0eqdugizgn"
    assert _coerce_project_id("  cmpxnn3z407zead0eqdugizgn  ") == "cmpxnn3z407zead0eqdugizgn"
    assert _coerce_project_id("Dataclaw-langfuse") is None
    assert _coerce_project_id("") is None
    assert _coerce_project_id(None) is None
    assert _coerce_project_id("cm-too-short") is None


# ============================================================
# Section: api
# ============================================================

import os
from datetime import UTC, datetime

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="module")
async def api_client(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("phase1-api")
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path/'app.sqlite'}"
    os.environ["DEMO_DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path/'demo.sqlite'}"
    os.environ["DEMO_MODE"] = "true"
    os.environ["MASTER_KEY"] = "test-master-key-please-change"
    os.environ["SESSION_SECRET"] = "test-session-secret-please-change"
    os.environ["DATACLAW_VECTOR_TEST_DOUBLE"] = "true"
    os.environ["DATACLAW_TEST_AUTO_CREATE_SCHEMA"] = "true"
    os.environ["DATACLAW_BCRYPT_ROUNDS"] = "4"

    import importlib

    from app.core.config import get_settings
    get_settings.cache_clear()
    import app.db.session as session_module
    importlib.reload(session_module)
    from app import main as main_module
    importlib.reload(main_module)

    transport = ASGITransport(app=main_module.app)
    async with main_module.app.router.lifespan_context(main_module.app):
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            login = await ac.post(
                "/auth/login",
                json={"email": "admin@dataclaw.local", "password": "dataclaw-local-admin"},
            )
            assert login.status_code == 200
            yield ac, main_module


# ---------- helpers ----------


async def _create_chat_message(main_module) -> str:
    """Insert an assistant chat message directly via SessionLocal so we don't
    depend on /ide/chat (which would require LLM mocking)."""
    from app.models.domain import ChatMessage, ChatThread, User, Workspace
    from sqlalchemy import select

    SessionLocal = main_module.SessionLocal if hasattr(main_module, "SessionLocal") else None
    if SessionLocal is None:
        from app.db.session import SessionLocal  # type: ignore  # noqa: F811

    async with SessionLocal() as s:
        ws = await s.scalar(select(Workspace).limit(1))
        user = await s.scalar(select(User).limit(1))
        thread = ChatThread(
            workspace_id=ws.id,
            user_id=user.id,
            title="api-test thread",
        )
        s.add(thread)
        await s.flush()
        msg = ChatMessage(
            thread_id=thread.id,
            role="assistant",
            content="42",
            trace_id="trace-abc",
        )
        s.add(msg)
        await s.commit()
        return msg.id


# ---------- /feedback ----------


@pytest.mark.asyncio
async def test_feedback_happy_path(api_client) -> None:
    ac, main_module = api_client
    msg_id = await _create_chat_message(main_module)

    resp = await ac.post(
        "/feedback",
        json={"chat_message_id": msg_id, "sentiment": "positive", "comment": "good"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["sentiment"] == "positive"
    assert body["chat_message_id"] == msg_id
    assert body["langfuse_score_id"] is None  # Langfuse not configured

    fetched = await ac.get(f"/feedback/{body['id']}")
    assert fetched.status_code == 200
    assert fetched.json()["id"] == body["id"]


@pytest.mark.asyncio
async def test_feedback_rejects_invalid_sentiment(api_client) -> None:
    ac, main_module = api_client
    msg_id = await _create_chat_message(main_module)
    resp = await ac.post(
        "/feedback",
        json={"chat_message_id": msg_id, "sentiment": "neutral"},
    )
    assert resp.status_code == 400
    assert "sentiment" in resp.json()["detail"]


@pytest.mark.asyncio
async def test_feedback_404_when_message_missing(api_client) -> None:
    ac, _ = api_client
    resp = await ac.post(
        "/feedback",
        json={"chat_message_id": "ghost", "sentiment": "positive"},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_get_feedback_404(api_client) -> None:
    ac, _ = api_client
    resp = await ac.get("/feedback/does-not-exist")
    assert resp.status_code == 404


# ---------- /chat-messages/{id}/trace ----------


@pytest.mark.asyncio
async def test_get_trace_returns_empty_spans_when_none_recorded(api_client) -> None:
    ac, main_module = api_client
    msg_id = await _create_chat_message(main_module)
    resp = await ac.get(f"/chat-messages/{msg_id}/trace")
    assert resp.status_code == 200
    body = resp.json()
    assert body["chat_message_id"] == msg_id
    assert body["trace_id"] == "trace-abc"
    assert body["spans"] == []


@pytest.mark.asyncio
async def test_get_trace_returns_persisted_spans(api_client) -> None:
    ac, main_module = api_client
    msg_id = await _create_chat_message(main_module)

    # Write spans directly so we don't depend on a real chat turn.
    from app.db.session import SessionLocal
    from app.models.domain import ChatSpan
    from app.db.base import new_id

    started = datetime.now(UTC)
    async with SessionLocal() as s:
        s.add(
            ChatSpan(
                id=new_id(),
                chat_message_id=msg_id,
                kind="root",
                name="chat_turn",
                started_at=started,
                ended_at=started,
            )
        )
        s.add(
            ChatSpan(
                id=new_id(),
                chat_message_id=msg_id,
                kind="llm",
                name="primary",
                model="gpt-4o-mini",
                usage={"input_tokens": 12, "output_tokens": 4, "total_tokens": 16},
                started_at=started,
                ended_at=started,
                latency_ms=125,
            )
        )
        await s.commit()

    resp = await ac.get(f"/chat-messages/{msg_id}/trace")
    assert resp.status_code == 200
    spans = resp.json()["spans"]
    kinds = sorted([s["kind"] for s in spans])
    assert kinds == ["llm", "root"]
    llm = next(s for s in spans if s["kind"] == "llm")
    assert llm["model"] == "gpt-4o-mini"
    assert llm["usage"]["total_tokens"] == 16
    assert llm["latency_ms"] == 125


@pytest.mark.asyncio
async def test_get_trace_404_for_unknown_message(api_client) -> None:
    ac, _ = api_client
    resp = await ac.get("/chat-messages/ghost/trace")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_trace_link_returns_null_url_when_langfuse_off(api_client) -> None:
    ac, main_module = api_client
    msg_id = await _create_chat_message(main_module)
    resp = await ac.get(f"/chat-messages/{msg_id}/trace-link")
    assert resp.status_code == 200
    body = resp.json()
    assert body["trace_id"] == "trace-abc"
    assert body["langfuse_url"] is None


# ---------- /integrations/observability ----------


@pytest.mark.asyncio
async def test_integrations_list_returns_catalog_and_empty_record(api_client) -> None:
    ac, _ = api_client
    resp = await ac.get("/integrations/observability")
    assert resp.status_code == 200
    body = resp.json()
    slugs = [item["slug"] for item in body["catalog"]]
    assert "langfuse" in slugs
    lf = body["records"]["langfuse"]
    # Fresh DB: no record configured.
    assert lf["configured"] is False
    assert lf["enabled"] is False


@pytest.mark.asyncio
async def test_integrations_upsert_and_disable_lifecycle(api_client) -> None:
    ac, _ = api_client

    upsert = await ac.put(
        "/integrations/observability/langfuse",
        json={
            "values": {
                "host": "https://cloud.langfuse.com",
                "public_key": "pk-test",
                "secret_key": "sk-test",
                "enabled": True,
            }
        },
    )
    assert upsert.status_code == 200, upsert.text
    body = upsert.json()
    assert body["configured"] is True
    assert body["enabled"] is True
    # Secret value must not be echoed back; preview only.
    assert "sk-test" not in str(body["values"])
    assert "secret_key" in body["secrets_set"]
    assert body["secret_previews"]["secret_key"] != "sk-test"

    fetched = await ac.get("/integrations/observability/langfuse")
    assert fetched.status_code == 200
    assert fetched.json()["enabled"] is True

    disabled = await ac.delete("/integrations/observability/langfuse")
    assert disabled.status_code == 200
    assert disabled.json()["enabled"] is False
    # Credentials must still be set (Disable does not wipe).
    assert "secret_key" in disabled.json()["secrets_set"]


@pytest.mark.asyncio
async def test_integrations_unknown_slug_returns_404(api_client) -> None:
    ac, _ = api_client
    resp = await ac.get("/integrations/observability/grafana")
    assert resp.status_code == 404
    resp = await ac.put(
        "/integrations/observability/grafana",
        json={"values": {"enabled": True}},
    )
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_integrations_test_returns_not_configured_when_disabled(api_client) -> None:
    ac, _ = api_client
    # Ensure disabled state from the previous test.
    await ac.delete("/integrations/observability/langfuse")
    resp = await ac.post("/integrations/observability/langfuse/test")
    assert resp.status_code == 200
    assert resp.json()["status"] == "not_configured"


# ============================================================
# Section: e2e
# ============================================================

import os

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="module")
async def e2e_client(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("phase1-e2e")
    os.environ["DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path/'app.sqlite'}"
    os.environ["DEMO_DATABASE_URL"] = f"sqlite+aiosqlite:///{tmp_path/'demo.sqlite'}"
    os.environ["DEMO_MODE"] = "true"
    os.environ["MASTER_KEY"] = "test-master-key-please-change"
    os.environ["SESSION_SECRET"] = "test-session-secret-please-change"
    os.environ["DATACLAW_VECTOR_TEST_DOUBLE"] = "true"
    os.environ["DATACLAW_TEST_AUTO_CREATE_SCHEMA"] = "true"
    os.environ["DATACLAW_BCRYPT_ROUNDS"] = "4"

    import importlib

    from app.core.config import get_settings
    get_settings.cache_clear()
    import app.db.session as session_module
    importlib.reload(session_module)
    from app import main as main_module
    importlib.reload(main_module)

    transport = ASGITransport(app=main_module.app)
    async with main_module.app.router.lifespan_context(main_module.app):
        async with AsyncClient(transport=transport, base_url="http://test") as ac:
            login = await ac.post(
                "/auth/login",
                json={"email": "admin@dataclaw.local", "password": "dataclaw-local-admin"},
            )
            assert login.status_code == 200
            yield ac


@pytest.mark.asyncio
async def test_chat_persists_trace_and_supports_feedback(e2e_client) -> None:
    ac = e2e_client

    # 1) Send a chat turn.
    resp = await ac.post(
        "/ide/chat",
        json={"question": "Say exactly: phase 1 e2e check"},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["thread_id"]
    # Phase 1 contract: response carries trace_id and message_id.
    assert body["trace_id"], "ChatResponse must carry trace_id"
    assert body["message_id"], "ChatResponse must carry message_id"
    assert body["langfuse_url"] in (None, ""), "no Langfuse configured -> URL should be null"
    message_id = body["message_id"]
    trace_id = body["trace_id"]

    # 2) The chat_message row should be persisted with the same id and trace_id.
    fetched = (await ac.get(f"/chat/threads/{body['thread_id']}")).json()
    assistant = next(m for m in fetched["messages"] if m["role"] == "assistant")
    assert assistant["id"] == message_id
    # Frontend type exposes trace_id; backend column populated.
    assert assistant.get("trace_id") == trace_id

    # 3) /chat-messages/{id}/trace returns at least the root span.
    trace_resp = await ac.get(f"/chat-messages/{message_id}/trace")
    assert trace_resp.status_code == 200
    trace_body = trace_resp.json()
    assert trace_body["trace_id"] == trace_id
    kinds = {s["kind"] for s in trace_body["spans"]}
    assert "root" in kinds, f"root span missing; got {kinds}"

    # 4) Submit feedback against the assistant message.
    fb = await ac.post(
        "/feedback",
        json={"chat_message_id": message_id, "sentiment": "negative", "comment": "wrong"},
    )
    assert fb.status_code == 200, fb.text
    assert fb.json()["sentiment"] == "negative"

    # 5) Trace-link endpoint returns null URL (Langfuse off) but a valid trace_id.
    link = await ac.get(f"/chat-messages/{message_id}/trace-link")
    assert link.status_code == 200
    link_body = link.json()
    assert link_body["trace_id"] == trace_id
    assert link_body["langfuse_url"] is None


@pytest.mark.asyncio
async def test_streaming_chat_also_persists_trace(e2e_client) -> None:
    ac = e2e_client

    async with ac.stream(
        "POST",
        "/ide/chat",
        headers={"accept": "text/event-stream"},
        json={"question": "Say exactly: streaming trace check"},
    ) as response:
        assert response.status_code == 200
        body_text = "".join([chunk async for chunk in response.aiter_text()])

    # Pull out the "done" event payload.
    import json as _json

    done_frame = next(f for f in body_text.split("\n\n") if f.startswith("event: done"))
    payload = _json.loads(
        next(line for line in done_frame.splitlines() if line.startswith("data: ")).removeprefix("data: ")
    )
    assert payload["trace_id"], payload
    assert payload["message_id"], payload

    trace_resp = await ac.get(f"/chat-messages/{payload['message_id']}/trace")
    assert trace_resp.status_code == 200
    kinds = {s["kind"] for s in trace_resp.json()["spans"]}
    assert "root" in kinds


# ============================================================
# Section: langfuse
# ============================================================

import os
import sys
import types
from dataclasses import dataclass, field
from typing import Any

import pytest

os.environ.setdefault("MASTER_KEY", "test-master-key-please-change")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-please-change")


# ---------- Stub SDK (covers the v3/v4 surface our wrapper touches) ----------


@dataclass
class _StubObservation:
    """Mirrors the SDK's LangfuseSpan / LangfuseGeneration wrapper."""

    name: str
    as_type: str
    trace_id: str
    parent_observation_id: str | None
    input: Any
    metadata: dict[str, Any]
    model: str | None
    id: str
    updates: list[dict[str, Any]] = field(default_factory=list)
    ended: bool = False

    def update(self, **kwargs: Any) -> _StubObservation:
        self.updates.append(kwargs)
        return self

    def end(self, **_kwargs: Any) -> _StubObservation:
        self.ended = True
        return self


@dataclass
class _StubScore:
    name: str
    value: float
    trace_id: str | None
    comment: str | None


@dataclass
class _StubClient:
    public_key: str
    secret_key: str
    host: str | None = None
    base_url: str | None = None
    started: list[_StubObservation] = field(default_factory=list)
    scores: list[_StubScore] = field(default_factory=list)
    flushed: int = 0
    _id_counter: int = 0

    def create_trace_id(self, *, seed: str) -> str:
        # Deterministic-from-seed: real SDK uses an OTel hash; we just
        # echo a hex-packed form so the test can assert linkage.
        return f"trace-{seed}"[:32].ljust(32, "x")

    def start_observation(
        self,
        *,
        trace_context: dict[str, Any],
        name: str,
        as_type: str = "span",
        input: Any = None,
        metadata: Any = None,
        model: str | None = None,
        **_extra: Any,
    ) -> _StubObservation:
        self._id_counter += 1
        obs = _StubObservation(
            name=name,
            as_type=as_type,
            trace_id=trace_context.get("trace_id"),
            parent_observation_id=trace_context.get("parent_observation_id"),
            input=input,
            metadata=metadata or {},
            model=model,
            id=f"obs-{self._id_counter}",
        )
        self.started.append(obs)
        return obs

    def create_score(
        self,
        *,
        name: str,
        value: float,
        trace_id: str | None = None,
        comment: str | None = None,
        **_extra: Any,
    ) -> None:
        self.scores.append(_StubScore(name=name, value=value, trace_id=trace_id, comment=comment))

    def flush(self) -> None:
        self.flushed += 1


def _install_stub_sdk(monkeypatch: pytest.MonkeyPatch) -> _StubClient:
    """Inject a stub ``langfuse`` module into sys.modules and return the
    single client instance the sink will receive."""
    stub_module = types.ModuleType("langfuse")
    captured: dict[str, _StubClient] = {}

    def _Langfuse(**kwargs: Any) -> _StubClient:
        client = _StubClient(
            public_key=kwargs["public_key"],
            secret_key=kwargs["secret_key"],
            host=kwargs.get("host"),
            base_url=kwargs.get("base_url"),
        )
        captured["client"] = client
        return client

    stub_module.Langfuse = _Langfuse  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "langfuse", stub_module)

    # Also invalidate our resolver cache so the next resolve_sink() rebuilds
    # using the stubbed module.
    from app.services.observability import langfuse_client

    langfuse_client.invalidate_cache()

    # Force a fresh sink + client construction.
    sink = langfuse_client.resolve_sink(
        {
            "enabled": True,
            "host": "https://stub-langfuse.local",
            "public_key": "pk-stub",
            "secret_key": "sk-stub",
            "project": "stub-project",
        }
    )
    assert sink is not None
    assert sink.is_available() is True
    return captured["client"]


# ---------- fixtures ----------


@pytest.fixture
async def db_langfuse(tmp_path, monkeypatch):
    """Per-test sqlite + ChatSpan-table fixture."""
    import importlib

    db_path = tmp_path / "lf.sqlite"
    monkeypatch.setenv("DATABASE_URL", f"sqlite+aiosqlite:///{db_path}")
    monkeypatch.setenv("DEMO_DATABASE_URL", f"sqlite+aiosqlite:///{tmp_path/'demo.sqlite'}")

    from app.core.config import get_settings

    get_settings.cache_clear()
    import app.db.session as session_module

    importlib.reload(session_module)
    import app.models.domain  # noqa: F401
    from app.db.base import Base

    async with session_module.engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield session_module.SessionLocal
    await session_module.engine.dispose()


@pytest.fixture
async def session_with_langfuse(db_langfuse, monkeypatch):
    """Seed app_settings with a langfuse config so chat_trace picks it up."""
    from app.services.settings_store import update_observability_provider

    async with db_langfuse() as s:
        await update_observability_provider(
            s,
            "langfuse",
            {
                "enabled": True,
                "host": "https://stub-langfuse.local",
                "public_key": "pk-stub",
                "secret_key": "sk-stub",
                "project": "stub-project",
            },
        )
        await s.commit()
    return db_langfuse


# ---------- tests ----------


@pytest.mark.asyncio
async def test_chat_trace_emits_nested_spans_to_langfuse_sdk(
    session_with_langfuse, monkeypatch
) -> None:
    stub_client = _install_stub_sdk(monkeypatch)

    from app.services.observability.tracing import (
        allocate_chat_message_id,
        chat_trace,
        span as trace_span,
    )

    msg_id = allocate_chat_message_id()
    async with session_with_langfuse() as s:
        async with chat_trace(
            session=s,
            chat_message_id=msg_id,
            workspace_id="ws",
            user_id="u",
            thread_id="t",
            question="how many users?",
        ) as trace_handle:
            async with trace_span(
                "retrieval", "brain_retrieve", input={"q": "how many users?"}
            ) as rsp:
                rsp.set_output({"node_count": 3})
            async with trace_span("llm", "primary", model="gpt-4o-mini") as lsp:
                lsp.set_usage(prompt_tokens=10, completion_tokens=4, total_tokens=14)
                lsp.set_output({"content_preview": "There are 5 users."})
            async with trace_span("tool", "sqlite.read_select") as tsp:
                tsp.set_output({"status": "ok", "row_count": 1})

    # Deterministic trace_id derived from chat_message_id seed.
    assert trace_handle.trace_id == stub_client.create_trace_id(seed=msg_id)

    # 4 observations: root + retrieval + llm + tool.
    names = [obs.name for obs in stub_client.started]
    assert names == ["chat_turn", "brain_retrieve", "primary", "sqlite.read_select"]

    # The LLM span must be opened as a 'generation' (so usage/model surface
    # in Langfuse's UI). Everything else is a plain 'span'.
    as_types = {obs.name: obs.as_type for obs in stub_client.started}
    assert as_types["primary"] == "generation"
    assert as_types["chat_turn"] == "span"
    assert as_types["brain_retrieve"] == "span"
    assert as_types["sqlite.read_select"] == "span"

    # Parent-observation linking: root has no parent; every child links to
    # the root's observation id.
    by_name = {obs.name: obs for obs in stub_client.started}
    root_obs_id = by_name["chat_turn"].id
    assert by_name["chat_turn"].parent_observation_id is None
    assert by_name["brain_retrieve"].parent_observation_id == root_obs_id
    assert by_name["primary"].parent_observation_id == root_obs_id
    assert by_name["sqlite.read_select"].parent_observation_id == root_obs_id

    # Every observation gets at least one update(...) (carrying output +
    # usage where applicable) and then end(...).
    for obs in stub_client.started:
        assert obs.ended is True
        assert obs.updates, f"{obs.name} never received update()"

    # The LLM observation's update carried usage_details.
    llm_updates = by_name["primary"].updates
    last = llm_updates[-1]
    assert "usage_details" in last
    assert last["usage_details"] == {
        "input_tokens": 10,
        "output_tokens": 4,
        "total_tokens": 14,
    }

    # We always flush at trace close so manual testing is deterministic.
    assert stub_client.flushed >= 1


@pytest.mark.asyncio
async def test_chat_trace_marks_errored_span_with_error_level(
    session_with_langfuse, monkeypatch
) -> None:
    stub_client = _install_stub_sdk(monkeypatch)
    from app.services.observability.tracing import (
        allocate_chat_message_id,
        chat_trace,
        span as trace_span,
    )

    msg_id = allocate_chat_message_id()
    async with session_with_langfuse() as s:
        with pytest.raises(RuntimeError, match="boom"):
            async with chat_trace(
                session=s,
                chat_message_id=msg_id,
                workspace_id="ws",
                user_id="u",
                thread_id="t",
                question="x",
            ):
                async with trace_span("llm", "primary"):
                    raise RuntimeError("boom")

    by_name = {obs.name: obs for obs in stub_client.started}
    # Both inner span AND root must close with level=ERROR.
    for name in ("primary", "chat_turn"):
        last_update = by_name[name].updates[-1]
        assert last_update.get("level") == "ERROR"
        assert "boom" in (last_update.get("status_message") or "")


@pytest.mark.asyncio
async def test_feedback_forwards_score_to_langfuse(
    session_with_langfuse, monkeypatch
) -> None:
    """End-to-end: chat → trace → 👍 forwards a score with the trace id."""
    stub_client = _install_stub_sdk(monkeypatch)

    from app.models.domain import ChatMessage, ChatThread, User, Workspace
    from app.services.observability.feedback import FeedbackService
    from app.services.observability.tracing import (
        allocate_chat_message_id,
        chat_trace,
    )

    async with session_with_langfuse() as s:
        ws = Workspace(name="ws")
        s.add(ws)
        await s.flush()
        user = User(email="lf@local", password_hash="x")
        s.add(user)
        await s.flush()
        thread = ChatThread(workspace_id=ws.id, user_id=user.id, title="t")
        s.add(thread)
        await s.commit()
        thread_id = thread.id
        user_id = user.id

    msg_id = allocate_chat_message_id()
    async with session_with_langfuse() as s:
        async with chat_trace(
            session=s,
            chat_message_id=msg_id,
            workspace_id="ws",
            user_id=user_id,
            thread_id=thread_id,
            question="q",
        ) as trace_handle:
            pass
        # Persist the chat message with the same trace_id the trace produced.
        s.add(
            ChatMessage(
                id=msg_id,
                thread_id=thread_id,
                role="assistant",
                content="hello",
                trace_id=trace_handle.trace_id,
            )
        )
        await s.commit()

    async with session_with_langfuse() as s:
        from sqlalchemy import select

        u = await s.scalar(select(User).where(User.id == user_id))
        fb_row = await FeedbackService(s).submit(
            chat_message_id=msg_id,
            sentiment="positive",
            user=u,
        )

    assert fb_row.langfuse_score_id is not None
    assert fb_row.langfuse_score_id.startswith("lf:")

    # Exactly one score was forwarded, pointing at the right trace_id.
    assert len(stub_client.scores) == 1
    sent = stub_client.scores[0]
    assert sent.name == "user_feedback"
    assert sent.value == 1.0
    assert sent.trace_id == trace_handle.trace_id


@pytest.mark.asyncio
async def test_langfuse_failure_does_not_break_chat_trace(
    session_with_langfuse, monkeypatch
) -> None:
    """If the SDK raises inside begin_span, end_span, or flush, the local
    trace must still complete and persist. This is the load-bearing
    'resilience' contract."""
    from app.services.observability.tracing import (
        allocate_chat_message_id,
        chat_trace,
        span as trace_span,
    )

    # Build a stub that throws on EVERY call.
    class _ExplodingClient:
        public_key = "pk"
        secret_key = "sk"

        def create_trace_id(self, *, seed: str) -> str:
            raise RuntimeError("boom-trace-id")

        def start_observation(self, **_kwargs: Any) -> Any:
            raise RuntimeError("boom-start")

        def create_score(self, **_kwargs: Any) -> None:
            raise RuntimeError("boom-score")

        def flush(self) -> None:
            raise RuntimeError("boom-flush")

    stub_module = types.ModuleType("langfuse")
    stub_module.Langfuse = lambda **_kw: _ExplodingClient()  # type: ignore[attr-defined]
    monkeypatch.setitem(sys.modules, "langfuse", stub_module)

    from app.services.observability import langfuse_client
    langfuse_client.invalidate_cache()

    msg_id = allocate_chat_message_id()
    async with session_with_langfuse() as s:
        async with chat_trace(
            session=s,
            chat_message_id=msg_id,
            workspace_id="ws",
            user_id="u",
            thread_id="t",
            question="q",
        ) as trace_handle:
            async with trace_span("tool", "noop"):
                pass

    # Trace must have produced a trace_id (fallback from message_id when SDK
    # returns None) and the local spans must have persisted.
    assert trace_handle.trace_id is not None
    from sqlalchemy import select

    from app.models.domain import ChatSpan

    async with session_with_langfuse() as s:
        rows = (
            await s.scalars(
                select(ChatSpan).where(ChatSpan.chat_message_id == msg_id)
            )
        ).all()
        kinds = sorted([r.kind for r in rows])
    assert "root" in kinds and "tool" in kinds
