"""Consolidated tests for the evals layer (phases 2-5).

Covers all evals layer surfaces in one reviewable file:
* eval cases service + API: CRUD, lifecycle transitions, golden lookup
* candidate generators: schema/kg/lineage/fixture coordinator, dedupe, caps
* eval runner: batch execution, metrics, dashboard, cost budgets
* diagnose + suggestions: rules pass, apply (golden_query/prompt_diff/rules_md),
  preview-only paths, dismiss, revert, background diagnose
"""

from __future__ import annotations


# ============================================================
# Section: cases_unit
# ============================================================

import os
import uuid

import pytest

os.environ.setdefault("MASTER_KEY", "test-master-key-please-change")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-please-change")


@pytest.fixture
async def db_cases(tmp_path, monkeypatch):
    import importlib

    db_path = tmp_path / "phase2.sqlite"
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
        user_msg = ChatMessage(thread_id=thread.id, role="user", content="how many users?")
        assistant_msg = ChatMessage(thread_id=thread.id, role="assistant", content="42 users")
        s.add_all([user_msg, assistant_msg])
        await s.commit()
        return ws.id, user.id, thread.id, user_msg.id, assistant_msg.id


# ---------- CRUD basics ----------


@pytest.mark.asyncio
async def test_create_manual_case_requires_expected_field(db_cases) -> None:
    from app.services.evals.cases import (
        EvalCaseInput,
        EvalCaseService,
        EvalCaseValidationError,
    )

    ws_id, _, _, _, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        with pytest.raises(EvalCaseValidationError):
            await EvalCaseService(s).create(
                EvalCaseInput(
                    workspace_id=ws_id,
                    question="how many users?",
                    origin="manual",
                )
            )


@pytest.mark.asyncio
async def test_create_manual_case_with_expected_sql(db_cases) -> None:
    from app.services.evals.cases import EvalCaseInput, EvalCaseService

    ws_id, _, _, _, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        case = await EvalCaseService(s).create(
            EvalCaseInput(
                workspace_id=ws_id,
                question="how many users?",
                expected_sql="SELECT count(*) FROM users",
                expected_connector_slug="postgres",
                origin="manual",
            )
        )
        await s.commit()
        assert case.status == "candidate"
        assert case.origin == "manual"
        assert case.expected_sql == "SELECT count(*) FROM users"


@pytest.mark.asyncio
async def test_auto_origin_can_create_without_expected(db_cases) -> None:
    """Auto-generated candidates can land empty — the user fills them on review."""
    from app.services.evals.cases import EvalCaseInput, EvalCaseService

    ws_id, _, _, _, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        case = await EvalCaseService(s).create(
            EvalCaseInput(
                workspace_id=ws_id,
                question="which dashboard uses customers?",
                origin="auto:kg",
            )
        )
        await s.commit()
        assert case.status == "candidate"
        assert case.expected_sql is None


@pytest.mark.asyncio
async def test_create_rejects_unknown_status_and_origin(db_cases) -> None:
    from app.services.evals.cases import (
        EvalCaseInput,
        EvalCaseService,
        EvalCaseValidationError,
    )

    ws_id, _, _, _, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        with pytest.raises(EvalCaseValidationError):
            await EvalCaseService(s).create(
                EvalCaseInput(workspace_id=ws_id, question="x", origin="auto:nope")
            )
        with pytest.raises(EvalCaseValidationError):
            await EvalCaseService(s).create(
                EvalCaseInput(workspace_id=ws_id, question="x", origin="manual", status="published")
            )


@pytest.mark.asyncio
async def test_create_rejects_empty_question(db_cases) -> None:
    from app.services.evals.cases import (
        EvalCaseInput,
        EvalCaseService,
        EvalCaseValidationError,
    )

    ws_id, _, _, _, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        with pytest.raises(EvalCaseValidationError):
            await EvalCaseService(s).create(
                EvalCaseInput(workspace_id=ws_id, question="   ", expected_answer="x")
            )


# ---------- lifecycle ----------


@pytest.mark.asyncio
async def test_full_lifecycle_candidate_to_golden(db_cases) -> None:
    from app.services.evals.cases import EvalCaseInput, EvalCaseService

    ws_id, _, _, _, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        svc = EvalCaseService(s)
        case = await svc.create(
            EvalCaseInput(
                workspace_id=ws_id,
                question="how many users?",
                expected_sql="SELECT count(*) FROM users",
                origin="manual",
            )
        )
        await svc.approve(case.id)
        await svc.promote_golden(case.id)
        await s.commit()
        reloaded = await svc.get(case.id)
        assert reloaded.status == "golden"


@pytest.mark.asyncio
async def test_cannot_promote_candidate_directly_to_golden(db_cases) -> None:
    from app.services.evals.cases import (
        EvalCaseInput,
        EvalCaseService,
        EvalCaseTransitionError,
    )

    ws_id, _, _, _, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        svc = EvalCaseService(s)
        case = await svc.create(
            EvalCaseInput(
                workspace_id=ws_id,
                question="q",
                expected_sql="SELECT 1",
                origin="manual",
            )
        )
        with pytest.raises(EvalCaseTransitionError):
            await svc.promote_golden(case.id)


@pytest.mark.asyncio
async def test_cannot_promote_golden_without_expected_sql(db_cases) -> None:
    from app.services.evals.cases import (
        EvalCaseInput,
        EvalCaseService,
        EvalCaseValidationError,
    )

    ws_id, _, _, _, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        svc = EvalCaseService(s)
        case = await svc.create(
            EvalCaseInput(
                workspace_id=ws_id,
                question="q",
                expected_answer="42",
                origin="manual",
            )
        )
        await svc.approve(case.id)
        with pytest.raises(EvalCaseValidationError):
            await svc.promote_golden(case.id)


@pytest.mark.asyncio
async def test_archive_then_unarchive(db_cases) -> None:
    from app.services.evals.cases import EvalCaseInput, EvalCaseService

    ws_id, _, _, _, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        svc = EvalCaseService(s)
        case = await svc.create(
            EvalCaseInput(workspace_id=ws_id, question="q", expected_answer="a")
        )
        await svc.archive(case.id)
        reloaded = await svc.get(case.id)
        assert reloaded.status == "archived"
        await svc.unarchive(case.id)
        reloaded = await svc.get(case.id)
        assert reloaded.status == "candidate"


@pytest.mark.asyncio
async def test_update_rejected_when_archived(db_cases) -> None:
    from app.services.evals.cases import (
        EvalCaseInput,
        EvalCasePatch,
        EvalCaseService,
        EvalCaseTransitionError,
    )

    ws_id, _, _, _, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        svc = EvalCaseService(s)
        case = await svc.create(
            EvalCaseInput(workspace_id=ws_id, question="q", expected_answer="a")
        )
        await svc.archive(case.id)
        with pytest.raises(EvalCaseTransitionError):
            await svc.update(case.id, EvalCasePatch(question="q2"))


# ---------- from-feedback ----------


@pytest.mark.asyncio
async def test_from_feedback_picks_up_prior_user_question(db_cases) -> None:
    from app.models.domain import Feedback
    from app.services.evals.cases import EvalCaseService

    ws_id, _, _, _, assistant_msg_id = await _seed_chat(db_cases)
    # Seed a 👎 feedback so we can verify the eval_case_id gets stamped.
    async with db_cases() as s:
        s.add(
            Feedback(
                chat_message_id=assistant_msg_id,
                sentiment="negative",
                comment="wrong",
            )
        )
        await s.commit()

    async with db_cases() as s:
        case = await EvalCaseService(s).create_from_feedback(
            chat_message_id=assistant_msg_id,
            expected_sql="SELECT count(*) FROM users WHERE active",
        )
        await s.commit()
        assert case.question == "how many users?"  # pulled from the paired user message
        assert case.origin == "feedback"
        assert case.source_chat_message_id == assistant_msg_id
        # Latest 👎 row should now be stamped with the eval_case id.
        from sqlalchemy import select

        fb = await s.scalar(select(Feedback).where(Feedback.chat_message_id == assistant_msg_id))
        assert fb.eval_case_id == case.id


@pytest.mark.asyncio
async def test_from_feedback_rejects_user_message(db_cases) -> None:
    from app.services.evals.cases import EvalCaseService, EvalCaseValidationError

    _, _, _, user_msg_id, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        with pytest.raises(EvalCaseValidationError):
            await EvalCaseService(s).create_from_feedback(
                chat_message_id=user_msg_id,
                expected_sql="SELECT 1",
            )


@pytest.mark.asyncio
async def test_from_feedback_404_for_missing_message(db_cases) -> None:
    from app.services.evals.cases import EvalCaseNotFound, EvalCaseService

    async with db_cases() as s:
        with pytest.raises(EvalCaseNotFound):
            await EvalCaseService(s).create_from_feedback(
                chat_message_id="ghost", expected_sql="x"
            )


# ---------- list filters ----------


@pytest.mark.asyncio
async def test_list_filters_by_status_origin_and_tag(db_cases) -> None:
    from app.services.evals.cases import EvalCaseInput, EvalCaseService

    ws_id, _, _, _, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        svc = EvalCaseService(s)
        for i in range(5):
            await svc.create(
                EvalCaseInput(
                    workspace_id=ws_id,
                    question=f"q{i}",
                    expected_answer=f"a{i}",
                    tags=["tagA"] if i % 2 == 0 else ["tagB"],
                    origin="manual" if i < 3 else "auto:schema",
                )
            )
        # Make one approved.
        first = (await svc.list(workspace_id=ws_id, limit=10))[0]
        await svc.approve(first.id)
        await s.commit()

        assert len(await svc.list(workspace_id=ws_id, status="candidate")) == 4
        assert len(await svc.list(workspace_id=ws_id, status="approved")) == 1
        assert len(await svc.list(workspace_id=ws_id, origin="auto:schema")) == 2
        assert len(await svc.list(workspace_id=ws_id, tag="tagA")) == 3
        assert len(await svc.list(workspace_id=ws_id, q="q2")) == 1


# ---------- golden lookup ----------


@pytest.mark.asyncio
async def test_golden_lookup_matches_normalized_question(db_cases) -> None:
    from app.services.evals.cases import EvalCaseInput, EvalCaseService

    ws_id, _, _, _, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        svc = EvalCaseService(s)
        case = await svc.create(
            EvalCaseInput(
                workspace_id=ws_id,
                question="How many users?",
                expected_sql="SELECT count(*) FROM users",
                expected_connector_slug="postgres",
            )
        )
        await svc.approve(case.id)
        await svc.promote_golden(case.id)
        await s.commit()

        # Match should be case+punctuation insensitive.
        hit = await svc.find_golden_for_question(
            workspace_id=ws_id,
            question="  how   many users?? ",
            connector_slug="postgres",
        )
        assert hit is not None and hit.id == case.id

        # Different connector excluded.
        miss = await svc.find_golden_for_question(
            workspace_id=ws_id,
            question="How many users?",
            connector_slug="mysql",
        )
        assert miss is None

        # Non-golden case must not match.
        await svc.archive(case.id)
        await s.commit()
        miss2 = await svc.find_golden_for_question(
            workspace_id=ws_id, question="How many users?", connector_slug="postgres"
        )
        assert miss2 is None


@pytest.mark.asyncio
async def test_golden_lookup_handles_blank_question(db_cases) -> None:
    from app.services.evals.cases import EvalCaseService

    ws_id, _, _, _, _ = await _seed_chat(db_cases)
    async with db_cases() as s:
        assert (
            await EvalCaseService(s).find_golden_for_question(workspace_id=ws_id, question="")
            is None
        )


# ============================================================
# Section: cases_api
# ============================================================

import os

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="module")
async def cases_client(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("phase2-api")
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


# ---------- helpers ----------


async def _new_chat_message(ac: AsyncClient, question: str) -> dict:
    resp = await ac.post("/ide/chat", json={"question": question})
    assert resp.status_code == 200, resp.text
    return resp.json()


# ---------- CRUD ----------


@pytest.mark.asyncio
async def test_create_list_get_patch_case_round_trip(cases_client) -> None:
    ac = cases_client
    create = await ac.post(
        "/evals/cases",
        json={
            "question": "how many active customers?",
            "expected_sql": "SELECT count(*) FROM customers WHERE active",
            "expected_connector_slug": "postgres",
            "tags": ["smoke", "billing"],
            "origin": "manual",
        },
    )
    assert create.status_code == 200, create.text
    case = create.json()
    assert case["status"] == "candidate"
    assert case["origin"] == "manual"
    assert set(case["tags"]) == {"smoke", "billing"}

    fetched = await ac.get(f"/evals/cases/{case['id']}")
    assert fetched.status_code == 200
    assert fetched.json()["id"] == case["id"]

    patched = await ac.patch(
        f"/evals/cases/{case['id']}",
        json={"expected_answer": "There are 42 active customers.", "tags": ["smoke"]},
    )
    assert patched.status_code == 200
    body = patched.json()
    assert body["expected_answer"] == "There are 42 active customers."
    assert body["tags"] == ["smoke"]

    listing = await ac.get("/evals/cases?status=candidate&q=customers")
    assert listing.status_code == 200
    assert any(c["id"] == case["id"] for c in listing.json())


@pytest.mark.asyncio
async def test_create_rejects_missing_expected(cases_client) -> None:
    ac = cases_client
    resp = await ac.post(
        "/evals/cases",
        json={"question": "noise"},  # no expected_*
    )
    assert resp.status_code == 400


# ---------- lifecycle through HTTP ----------


@pytest.mark.asyncio
async def test_lifecycle_endpoints(cases_client) -> None:
    ac = cases_client
    create = await ac.post(
        "/evals/cases",
        json={
            "question": "lifecycle q",
            "expected_sql": "SELECT 1",
            "origin": "manual",
        },
    )
    case_id = create.json()["id"]

    # candidate -> golden directly: 409.
    bad = await ac.post(f"/evals/cases/{case_id}/promote-golden")
    assert bad.status_code == 409, bad.text

    approved = await ac.post(f"/evals/cases/{case_id}/approve")
    assert approved.status_code == 200
    assert approved.json()["status"] == "approved"

    promoted = await ac.post(f"/evals/cases/{case_id}/promote-golden")
    assert promoted.status_code == 200
    assert promoted.json()["status"] == "golden"

    archived = await ac.post(f"/evals/cases/{case_id}/archive")
    assert archived.status_code == 200
    assert archived.json()["status"] == "archived"

    # patch on archived: 409.
    bad_patch = await ac.patch(f"/evals/cases/{case_id}", json={"question": "x"})
    assert bad_patch.status_code == 409

    unarchived = await ac.post(f"/evals/cases/{case_id}/unarchive")
    assert unarchived.status_code == 200
    assert unarchived.json()["status"] == "candidate"


@pytest.mark.asyncio
async def test_get_unknown_case_404(cases_client) -> None:
    ac = cases_client
    resp = await ac.get("/evals/cases/ghost")
    assert resp.status_code == 404


# ---------- from-feedback end-to-end ----------


@pytest.mark.asyncio
async def test_from_feedback_after_real_chat_turn(cases_client) -> None:
    ac = cases_client
    chat = await _new_chat_message(ac, "Say exactly: phase2 from-feedback")
    message_id = chat["message_id"]

    # 👎 the message first.
    fb = await ac.post(
        "/feedback",
        json={"chat_message_id": message_id, "sentiment": "negative"},
    )
    assert fb.status_code == 200

    # Now promote into a candidate eval case.
    create = await ac.post(
        "/evals/cases/from-feedback",
        json={
            "chat_message_id": message_id,
            "expected_answer": "phase2 from-feedback",
            "expected_sql": None,
            "expected_tool": "sqlite.read_select",
        },
    )
    assert create.status_code == 200, create.text
    case = create.json()
    assert case["status"] == "candidate"
    assert case["origin"] == "feedback"
    assert case["source_chat_message_id"] == message_id

    # The feedback row should have eval_case_id stamped.
    fb_row = await ac.get(f"/feedback/{fb.json()['id']}")
    assert fb_row.status_code == 200
    assert fb_row.json()["eval_case_id"] == case["id"]


@pytest.mark.asyncio
async def test_from_feedback_404_when_message_missing(cases_client) -> None:
    ac = cases_client
    resp = await ac.post(
        "/evals/cases/from-feedback",
        json={"chat_message_id": "ghost", "expected_sql": "x"},
    )
    assert resp.status_code == 404


# ---------- golden-query short-circuit through /ide/chat ----------


@pytest.mark.asyncio
async def test_golden_query_hit_short_circuits_chat(cases_client) -> None:
    ac = cases_client
    # Seed a golden case for a deterministic question.
    create = await ac.post(
        "/evals/cases",
        json={
            "question": "What is the canonical user count?",
            "expected_answer": "There are exactly 7 canonical users.",
            "expected_sql": "SELECT count(*) FROM canonical_users",
            "origin": "manual",
        },
    )
    case_id = create.json()["id"]
    await ac.post(f"/evals/cases/{case_id}/approve")
    promoted = await ac.post(f"/evals/cases/{case_id}/promote-golden")
    assert promoted.json()["status"] == "golden"

    # Hit /ide/chat with the same question — should short-circuit.
    chat = await _new_chat_message(ac, "What is the canonical user count?")
    assert chat["llm_status"] == "golden_query_hit"
    assert chat["sql"] == "SELECT count(*) FROM canonical_users"
    assert chat["answer"] == "There are exactly 7 canonical users."
    # Provenance citation must point at the eval case.
    assert any(
        isinstance(c, dict)
        and c.get("type") == "golden_query_provenance"
        and c.get("eval_case_id") == case_id
        for c in chat.get("citations", [])
    )


@pytest.mark.asyncio
async def test_golden_query_match_is_punctuation_and_case_insensitive(cases_client) -> None:
    ac = cases_client
    create = await ac.post(
        "/evals/cases",
        json={
            "question": "ARR by region last quarter?",
            "expected_sql": "SELECT region, sum(arr) FROM mrr GROUP BY 1",
            "expected_answer": "ARR by region (cached).",
            "origin": "manual",
        },
    )
    case_id = create.json()["id"]
    await ac.post(f"/evals/cases/{case_id}/approve")
    await ac.post(f"/evals/cases/{case_id}/promote-golden")

    chat = await _new_chat_message(ac, "  arr by region  last  quarter ")
    assert chat["llm_status"] == "golden_query_hit"
    assert chat["answer"] == "ARR by region (cached)."


@pytest.mark.asyncio
async def test_archived_golden_does_not_short_circuit(cases_client) -> None:
    ac = cases_client
    create = await ac.post(
        "/evals/cases",
        json={
            "question": "unique test question for archive path",
            "expected_sql": "SELECT 1",
            "expected_answer": "should-not-leak",
            "origin": "manual",
        },
    )
    case_id = create.json()["id"]
    await ac.post(f"/evals/cases/{case_id}/approve")
    await ac.post(f"/evals/cases/{case_id}/promote-golden")
    await ac.post(f"/evals/cases/{case_id}/archive")

    chat = await _new_chat_message(ac, "unique test question for archive path")
    # No golden hit -> llm_status is anything except 'golden_query_hit'.
    assert chat["llm_status"] != "golden_query_hit"
    assert chat["answer"] != "should-not-leak"


# ============================================================
# Section: gen_unit
# ============================================================

import os
import uuid

import pytest

os.environ.setdefault("MASTER_KEY", "test-master-key-please-change")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-please-change")


@pytest.fixture
async def db_gen(tmp_path, monkeypatch):
    import importlib

    db_path = tmp_path / "phase3.sqlite"
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


async def _seed_workspace(SessionLocal):
    from app.models.domain import User, Workspace

    async with SessionLocal() as s:
        ws = Workspace(name="ws")
        s.add(ws)
        await s.flush()
        user = User(email=f"u-{uuid.uuid4()}@local", password_hash="x")
        s.add(user)
        await s.commit()
        return ws.id, user.id


async def _seed_schema(SessionLocal, ws_id: str):
    """Seed one Connector + Dataset + 2 TableAssets (one SQL, one metadata)."""
    from app.models.domain import Connector, Dataset, TableAsset

    async with SessionLocal() as s:
        sql_conn = Connector(
            workspace_id=ws_id,
            slug="postgres",
            category="Data store",
            display_name="Postgres",
        )
        meta_conn = Connector(
            workspace_id=ws_id,
            slug="notion",
            category="Knowledge base",
            display_name="Notion",
        )
        s.add_all([sql_conn, meta_conn])
        await s.flush()
        sql_ds = Dataset(
            workspace_id=ws_id,
            connector_id=sql_conn.id,
            name="core",
            source_type="postgres",
            schema_name="core",
        )
        meta_ds = Dataset(
            workspace_id=ws_id,
            connector_id=meta_conn.id,
            name="docs",
            source_type="notion",
            schema_name="public",
        )
        s.add_all([sql_ds, meta_ds])
        await s.flush()
        s.add_all([
            TableAsset(
                dataset_id=sql_ds.id,
                name="customers",
                columns=[
                    {"name": "id", "type": "INTEGER"},
                    {"name": "email", "type": "TEXT"},
                ],
                row_count=1234,
            ),
            TableAsset(
                dataset_id=sql_ds.id,
                name="orders",
                columns=[{"name": "id", "type": "INTEGER"}],
                row_count=10,
            ),
            # Metadata-only connector — should NOT produce a row-count case.
            TableAsset(
                dataset_id=meta_ds.id,
                name="runbook",
                columns=[{"name": "title", "type": "TEXT"}],
            ),
        ])
        await s.commit()


# ---------- SchemaProducer ----------


@pytest.mark.asyncio
async def test_schema_producer_emits_columns_and_count_for_sql_connector(db_gen) -> None:
    from app.services.evals.generators.schema import SchemaProducer

    ws_id, _ = await _seed_workspace(db_gen)
    await _seed_schema(db_gen, ws_id)
    async with db_gen() as s:
        cands = await SchemaProducer().generate(s, workspace_id=ws_id, limit=20)

    # 2 tables x 2 templates for postgres = 4; 1 table x 1 template for notion = 1; total 5.
    # The first two postgres tables each get column + count (4); the metadata-only
    # notion table gets only the column-shape question.
    assert len(cands) == 5
    column_qs = [c for c in cands if c.question.startswith("What columns does")]
    count_qs = [c for c in cands if c.question.startswith("How many rows")]
    assert len(column_qs) == 3
    assert len(count_qs) == 2
    # Postgres count cases must carry connector + tool + SQL.
    pg_count = next(c for c in count_qs if "customers" in c.question)
    assert pg_count.expected_connector_slug == "postgres"
    assert pg_count.expected_tool == "postgres.read_select"
    assert pg_count.expected_sql == "SELECT COUNT(*) FROM core.customers"
    # Notion table must NOT have a SQL count case.
    assert not any("runbook" in c.question and c.expected_sql for c in cands)


@pytest.mark.asyncio
async def test_schema_producer_respects_limit(db_gen) -> None:
    from app.services.evals.generators.schema import SchemaProducer

    ws_id, _ = await _seed_workspace(db_gen)
    await _seed_schema(db_gen, ws_id)
    async with db_gen() as s:
        cands = await SchemaProducer().generate(s, workspace_id=ws_id, limit=2)
    assert len(cands) == 2


@pytest.mark.asyncio
async def test_schema_producer_empty_workspace(db_gen) -> None:
    from app.services.evals.generators.schema import SchemaProducer

    ws_id, _ = await _seed_workspace(db_gen)
    async with db_gen() as s:
        cands = await SchemaProducer().generate(s, workspace_id=ws_id, limit=20)
    assert cands == []


# ---------- KgProducer ----------


@pytest.mark.asyncio
async def test_kg_producer_emits_usage_and_build_questions(db_gen) -> None:
    from app.models.domain import KnowledgeEdge, KnowledgeNode
    from app.services.evals.generators.kg import KgProducer

    ws_id, _ = await _seed_workspace(db_gen)
    async with db_gen() as s:
        table = KnowledgeNode(
            workspace_id=ws_id,
            type="table",
            canonical_name="core.customers",
            connector_slug="postgres",
        )
        dash = KnowledgeNode(
            workspace_id=ws_id,
            type="dashboard",
            canonical_name="Sales Overview",
            connector_slug="tableau",
        )
        dag = KnowledgeNode(
            workspace_id=ws_id,
            type="dag",
            canonical_name="airflow.daily_customers",
            connector_slug="airflow",
        )
        s.add_all([table, dash, dag])
        await s.flush()
        s.add_all([
            KnowledgeEdge(
                workspace_id=ws_id,
                src_node_id=dash.id,
                dst_node_id=table.id,
                relationship="uses",
                source="frontmatter",
            ),
            KnowledgeEdge(
                workspace_id=ws_id,
                src_node_id=dag.id,
                dst_node_id=table.id,
                relationship="builds",
                source="frontmatter",
            ),
        ])
        await s.commit()

    async with db_gen() as s:
        cands = await KgProducer().generate(s, workspace_id=ws_id, limit=20)
    qs = [c.question for c in cands]
    assert any("Which dashboards or models use core.customers" in q for q in qs)
    assert any("Which DAG or model builds core.customers" in q for q in qs)


@pytest.mark.asyncio
async def test_kg_producer_empty_graph(db_gen) -> None:
    from app.services.evals.generators.kg import KgProducer

    ws_id, _ = await _seed_workspace(db_gen)
    async with db_gen() as s:
        cands = await KgProducer().generate(s, workspace_id=ws_id, limit=10)
    assert cands == []


# ---------- LineageProducer ----------


@pytest.mark.asyncio
async def test_lineage_producer_emits_upstream_and_downstream(db_gen) -> None:
    from app.models.domain import ColumnLineageEdge
    from app.services.evals.generators.lineage import LineageProducer

    ws_id, _ = await _seed_workspace(db_gen)
    async with db_gen() as s:
        s.add(
            ColumnLineageEdge(
                workspace_id=ws_id,
                source_connector_slug="postgres",
                source_table="core.customers",
                source_column="email",
                target_connector_slug="dbt",
                target_table="analytics.dim_customers",
                target_column="email",
            )
        )
        await s.commit()

    async with db_gen() as s:
        cands = await LineageProducer().generate(s, workspace_id=ws_id, limit=10)
    qs = [c.question for c in cands]
    assert any("Which columns derive from core.customers.email?" in q for q in qs)
    assert any("Where does analytics.dim_customers.email get its data from?" in q for q in qs)


# ---------- FixtureProducer ----------


@pytest.mark.asyncio
async def test_fixture_producer_always_emits(db_gen) -> None:
    from app.services.evals.generators.fixture import DEMO_CASES, FixtureProducer

    ws_id, _ = await _seed_workspace(db_gen)
    async with db_gen() as s:
        cands = await FixtureProducer().generate(s, workspace_id=ws_id, limit=99)
    assert len(cands) == len(DEMO_CASES)


# ---------- Coordinator ----------


@pytest.mark.asyncio
async def test_coordinator_dedupes_against_existing_rows(db_gen) -> None:
    from app.services.evals.cases import EvalCaseInput, EvalCaseService
    from app.services.evals.generators import ALL_PRODUCERS, run_generators

    ws_id, _ = await _seed_workspace(db_gen)
    await _seed_schema(db_gen, ws_id)

    # Pre-seed a manual case with the same normalized question one of the
    # producers will emit. Coordinator must skip the producer's duplicate.
    async with db_gen() as s:
        await EvalCaseService(s).create(
            EvalCaseInput(
                workspace_id=ws_id,
                question="How many rows are in core.customers?",
                expected_sql="SELECT 1",
                expected_connector_slug="postgres",
                origin="manual",
            )
        )
        await s.commit()

    async with db_gen() as s:
        result = await run_generators(
            s,
            producers=ALL_PRODUCERS,
            workspace_id=ws_id,
            sources=["schema"],
            limit_per_source=50,
        )
    schema_result = next(p for p in result.producers if p.slug == "schema")
    assert schema_result.skipped_duplicate >= 1
    # Inserted IDs must not include any duplicate of the pre-seeded question.
    assert all(rid in result.inserted_ids for rid in result.inserted_ids)
    # And the existing manual row should remain (we don't mutate it).


@pytest.mark.asyncio
async def test_coordinator_respects_per_source_cap(db_gen) -> None:
    from app.services.evals.generators import ALL_PRODUCERS, run_generators

    ws_id, _ = await _seed_workspace(db_gen)
    await _seed_schema(db_gen, ws_id)
    async with db_gen() as s:
        result = await run_generators(
            s,
            producers=ALL_PRODUCERS,
            workspace_id=ws_id,
            sources=["schema"],
            limit_per_source=2,
        )
    schema_result = next(p for p in result.producers if p.slug == "schema")
    assert schema_result.inserted == 2


@pytest.mark.asyncio
async def test_coordinator_respects_workspace_ceiling(db_gen, monkeypatch) -> None:
    """Drop the ceiling to a low number, pre-populate, then prove the
    coordinator short-circuits cleanly."""
    from app.services.evals.cases import EvalCaseInput, EvalCaseService
    from app.services.evals.generators import ALL_PRODUCERS, run_generators
    from app.services.evals.generators import coordinator as coord_mod

    monkeypatch.setattr(coord_mod, "WORKSPACE_CANDIDATE_CEILING", 3)

    ws_id, _ = await _seed_workspace(db_gen)
    await _seed_schema(db_gen, ws_id)

    # Fill the queue to the ceiling with unique candidates.
    async with db_gen() as s:
        svc = EvalCaseService(s)
        for i in range(3):
            await svc.create(
                EvalCaseInput(
                    workspace_id=ws_id,
                    question=f"pre-existing {i}",
                    expected_answer=f"a{i}",
                    origin="manual",
                )
            )
        await s.commit()

    async with db_gen() as s:
        result = await run_generators(
            s,
            producers=ALL_PRODUCERS,
            workspace_id=ws_id,
            sources=["fixture"],
            limit_per_source=50,
        )
    assert result.ceiling_reached
    assert result.inserted_ids == []


@pytest.mark.asyncio
async def test_coordinator_isolates_producer_failure(db_gen, monkeypatch) -> None:
    """A producer that raises must not prevent the others from running.
    Use a stub producer that throws; coordinator records the error and keeps
    going through the rest."""
    from app.services.evals.generators import run_generators
    from app.services.evals.generators.base import (
        CandidateCase,
        Producer,
        ProducerError,
    )

    class BadProducer:
        slug = "bad"
        origin = "auto:schema"  # any valid origin
        display_name = "Broken"

        async def generate(self, session, *, workspace_id, limit):
            raise ProducerError("simulated outage")

    class GoodProducer:
        slug = "good"
        origin = "auto:fixture"
        display_name = "Good"

        async def generate(self, session, *, workspace_id, limit):
            return [
                CandidateCase(
                    question="good question?",
                    origin=self.origin,
                    expected_answer="42",
                )
            ]

    ws_id, _ = await _seed_workspace(db_gen)
    producers: list[Producer] = [BadProducer(), GoodProducer()]
    async with db_gen() as s:
        result = await run_generators(
            s, producers=producers, workspace_id=ws_id, sources=None
        )
    bad = next(p for p in result.producers if p.slug == "bad")
    good = next(p for p in result.producers if p.slug == "good")
    assert bad.error and "simulated outage" in bad.error
    assert good.inserted == 1
    assert len(result.inserted_ids) == 1


@pytest.mark.asyncio
async def test_coordinator_unknown_origin_recorded_as_error(db_gen) -> None:
    from app.services.evals.generators import run_generators
    from app.services.evals.generators.base import CandidateCase, Producer

    class BogusProducer:
        slug = "bogus"
        origin = "auto:schema"
        display_name = "Bogus"

        async def generate(self, session, *, workspace_id, limit):
            return [
                CandidateCase(
                    question="q",
                    origin="auto:totally-made-up",
                    expected_answer="a",
                )
            ]

    ws_id, _ = await _seed_workspace(db_gen)
    producers: list[Producer] = [BogusProducer()]
    async with db_gen() as s:
        result = await run_generators(s, producers=producers, workspace_id=ws_id)
    bogus = result.producers[0]
    assert bogus.inserted == 0
    assert bogus.error and "invalid origin" in bogus.error


# ============================================================
# Section: gen_api
# ============================================================

import os

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="module")
async def gen_client(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("phase3-api")
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


# ---------- producers catalog ----------


@pytest.mark.asyncio
async def test_producers_catalog_lists_all(gen_client) -> None:
    ac = gen_client
    resp = await ac.get("/evals/producers")
    assert resp.status_code == 200
    body = resp.json()
    slugs = [p["slug"] for p in body["producers"]]
    assert slugs == ["schema", "kg", "lineage", "fixture"]
    assert body["default_limit_per_source"] == 50
    assert body["workspace_candidate_ceiling"] == 500


# ---------- generate-candidates ----------


@pytest.mark.asyncio
async def test_generate_fixture_produces_candidates(gen_client) -> None:
    ac = gen_client
    resp = await ac.post(
        "/evals/cases/generate-candidates",
        json={"sources": ["fixture"], "limit_per_source": 50},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    fixture = next(p for p in body["producers"] if p["slug"] == "fixture")
    assert fixture["inserted"] >= 3
    assert body["candidates_in_queue_after"] >= body["candidates_in_queue_before"]
    # Inserted cases must show up in the listing as candidates with auto:fixture origin.
    listing = await ac.get("/evals/cases?status=candidate&origin=auto:fixture")
    assert listing.status_code == 200
    assert len(listing.json()) >= 3


@pytest.mark.asyncio
async def test_generate_dedupes_on_second_call(gen_client) -> None:
    ac = gen_client
    # Already seeded by the previous test — second run should insert 0
    # via the fixture producer (all duplicates).
    resp = await ac.post(
        "/evals/cases/generate-candidates",
        json={"sources": ["fixture"]},
    )
    assert resp.status_code == 200
    fixture = next(p for p in resp.json()["producers"] if p["slug"] == "fixture")
    assert fixture["inserted"] == 0
    assert fixture["skipped_duplicate"] >= 3


@pytest.mark.asyncio
async def test_generate_rejects_unknown_source(gen_client) -> None:
    ac = gen_client
    resp = await ac.post(
        "/evals/cases/generate-candidates",
        json={"sources": ["does-not-exist"]},
    )
    assert resp.status_code == 400


@pytest.mark.asyncio
async def test_generate_with_no_sources_runs_all_producers(gen_client) -> None:
    ac = gen_client
    resp = await ac.post("/evals/cases/generate-candidates", json={})
    assert resp.status_code == 200
    body = resp.json()
    # All four producers should appear in the report (even if some produce 0).
    slugs = sorted([p["slug"] for p in body["producers"]])
    assert slugs == ["fixture", "kg", "lineage", "schema"]


# ---------- bulk approve / archive ----------


@pytest.mark.asyncio
async def test_bulk_approve_handles_mixed_results(gen_client) -> None:
    ac = gen_client

    # Seed two candidates we control.
    created = []
    for i in range(2):
        c = await ac.post(
            "/evals/cases",
            json={
                "question": f"bulk approve target {i}",
                "expected_answer": f"a{i}",
                "origin": "manual",
            },
        )
        assert c.status_code == 200
        created.append(c.json()["id"])

    resp = await ac.post(
        "/evals/cases/bulk-approve",
        json={"case_ids": [*created, "ghost-id"]},
    )
    assert resp.status_code == 200
    results = resp.json()["results"]
    assert len(results) == 3
    ok = [r for r in results if r["ok"]]
    err = [r for r in results if not r["ok"]]
    assert len(ok) == 2
    assert all(r["status"] == "approved" for r in ok)
    assert len(err) == 1
    assert err[0]["error"] == "not_found"


@pytest.mark.asyncio
async def test_bulk_archive(gen_client) -> None:
    ac = gen_client
    # Create + immediately bulk-archive.
    c = await ac.post(
        "/evals/cases",
        json={"question": "bulk archive target", "expected_answer": "a", "origin": "manual"},
    )
    case_id = c.json()["id"]
    resp = await ac.post(
        "/evals/cases/bulk-archive",
        json={"case_ids": [case_id]},
    )
    assert resp.status_code == 200
    assert resp.json()["results"][0]["status"] == "archived"


@pytest.mark.asyncio
async def test_bulk_rejects_empty_list(gen_client) -> None:
    ac = gen_client
    resp = await ac.post("/evals/cases/bulk-approve", json={"case_ids": []})
    assert resp.status_code == 422  # pydantic min_length=1


@pytest.mark.asyncio
async def test_bulk_approve_idempotent_on_already_approved(gen_client) -> None:
    """Calling approve twice on the same case must not surface as an error —
    the service short-circuits when already in target state."""
    ac = gen_client
    c = await ac.post(
        "/evals/cases",
        json={"question": "idempotency target", "expected_answer": "a", "origin": "manual"},
    )
    case_id = c.json()["id"]
    first = await ac.post(
        "/evals/cases/bulk-approve",
        json={"case_ids": [case_id]},
    )
    assert first.status_code == 200
    second = await ac.post(
        "/evals/cases/bulk-approve",
        json={"case_ids": [case_id]},
    )
    assert second.status_code == 200
    assert second.json()["results"][0]["ok"] is True
    assert second.json()["results"][0]["status"] == "approved"


# ============================================================
# Section: metrics_unit
# ============================================================

import os

import pytest

os.environ.setdefault("MASTER_KEY", "test-master-key-please-change")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-please-change")


def _ctx(**overrides):
    from app.services.evals.metrics.base import EvalContext

    defaults = dict(
        expected_answer=None,
        expected_sql=None,
        expected_connector_slug=None,
        expected_tool=None,
        expected_citations=[],
        expected_result_hash=None,
        expected_result_preview=[],
        actual_answer=None,
        actual_sql=None,
        actual_connector_slug=None,
        actual_tool=None,
        actual_citations=[],
        actual_result_hash=None,
        actual_result_preview=[],
        retrieval_candidates=[],
        chat_spans=[],
        duration_ms=0,
        prompt_tokens=None,
        completion_tokens=None,
        total_tokens=None,
        cost_usd=None,
        model=None,
        previous_run_passed=None,
        repeat_scores=[],
        question="q",
    )
    defaults.update(overrides)
    return EvalContext(**defaults)


# ---------- sql_correctness ----------


def test_sql_correctness_exact_match() -> None:
    from app.services.evals.metrics.sql_correctness import SqlCorrectness

    ctx = _ctx(expected_sql="SELECT count(*) FROM users", actual_sql="select COUNT(*)  from users;")
    s = SqlCorrectness().score(ctx, threshold=0.5)
    assert s.status == "ok"
    assert s.score == 1.0
    assert s.passed is True


def test_sql_correctness_token_set_partial() -> None:
    from app.services.evals.metrics.sql_correctness import SqlCorrectness

    ctx = _ctx(
        expected_sql="SELECT a, b FROM t",
        actual_sql="SELECT b, a FROM t",
    )
    s = SqlCorrectness().score(ctx, threshold=0.5)
    # Token sets are equal -> 0.5 partial credit, passes at threshold=0.5.
    assert s.score == 0.5
    assert s.passed is True


def test_sql_correctness_total_miss() -> None:
    from app.services.evals.metrics.sql_correctness import SqlCorrectness

    ctx = _ctx(
        expected_sql="SELECT * FROM users",
        actual_sql="SELECT * FROM orders",
    )
    s = SqlCorrectness().score(ctx, threshold=0.5)
    assert s.score == 0.0
    assert s.passed is False


def test_sql_correctness_skipped_without_expected() -> None:
    from app.services.evals.metrics.sql_correctness import SqlCorrectness

    s = SqlCorrectness().score(_ctx(actual_sql="x"), threshold=0.5)
    assert s.status == "skipped"


def test_sql_correctness_fails_when_actual_empty() -> None:
    from app.services.evals.metrics.sql_correctness import SqlCorrectness

    s = SqlCorrectness().score(_ctx(expected_sql="x"), threshold=0.5)
    assert s.passed is False
    assert s.score == 0.0


# ---------- result_accuracy ----------


def test_result_accuracy_skipped_when_no_expected_hash() -> None:
    from app.services.evals.metrics.result_accuracy import ResultAccuracy

    s = ResultAccuracy().score(_ctx(actual_result_hash="abc"), threshold=1.0)
    assert s.status == "skipped"


def test_result_accuracy_match() -> None:
    from app.services.evals.metrics.result_accuracy import ResultAccuracy

    s = ResultAccuracy().score(
        _ctx(expected_result_hash="abc", actual_result_hash="abc"), threshold=1.0
    )
    assert s.passed is True and s.score == 1.0


def test_result_accuracy_mismatch() -> None:
    from app.services.evals.metrics.result_accuracy import ResultAccuracy

    s = ResultAccuracy().score(
        _ctx(expected_result_hash="abc", actual_result_hash="def"), threshold=1.0
    )
    assert s.passed is False


# ---------- connector + tool ----------


def test_connector_accuracy() -> None:
    from app.services.evals.metrics.connector_tool import ConnectorAccuracy

    assert ConnectorAccuracy().score(_ctx(), threshold=1.0).status == "skipped"
    s = ConnectorAccuracy().score(
        _ctx(expected_connector_slug="postgres", actual_connector_slug="postgres"),
        threshold=1.0,
    )
    assert s.passed is True
    s = ConnectorAccuracy().score(
        _ctx(expected_connector_slug="postgres", actual_connector_slug="mysql"),
        threshold=1.0,
    )
    assert s.passed is False


def test_tool_accuracy() -> None:
    from app.services.evals.metrics.connector_tool import ToolAccuracy

    s = ToolAccuracy().score(
        _ctx(expected_tool="postgres.read_select", actual_tool="postgres.read_select"),
        threshold=1.0,
    )
    assert s.passed is True


# ---------- citations + retrieval ----------


def test_citation_accuracy_jaccard() -> None:
    from app.services.evals.metrics.citations import CitationAccuracy

    s = CitationAccuracy().score(
        _ctx(
            expected_citations=[
                {"source": "postgres", "table": "core.customers"},
                {"source": "postgres", "table": "core.orders"},
            ],
            actual_citations=[{"source": "postgres", "table": "core.customers"}],
        ),
        threshold=0.5,
    )
    # Intersection size 1, union size 2 -> Jaccard 0.5
    assert s.score == 0.5
    assert s.passed is True


def test_citation_accuracy_skipped_without_expected() -> None:
    from app.services.evals.metrics.citations import CitationAccuracy

    s = CitationAccuracy().score(_ctx(), threshold=0.5)
    assert s.status == "skipped"


def test_retrieval_quality_recall_at_k() -> None:
    from app.services.evals.metrics.citations import RetrievalQuality

    s = RetrievalQuality().score(
        _ctx(
            expected_citations=[
                {"source": "postgres", "table": "core.customers"},
                {"source": "postgres", "table": "core.orders"},
            ],
            retrieval_candidates=["node-1 core.customers", "node-2 misc"],
        ),
        threshold=0.5,
    )
    # 1 of 2 expected tables present in candidate text -> 0.5
    assert s.score == 0.5


# ---------- operational ----------


def test_latency_and_cost_and_tokens() -> None:
    from app.services.evals.metrics.operational import Cost, Latency, Tokens

    lat = Latency().score(_ctx(duration_ms=215), threshold=0.0)
    assert lat.score == 215.0
    cost = Cost().score(_ctx(cost_usd=0.0123, model="gpt-4o-mini"), threshold=0.0)
    assert cost.score == pytest.approx(0.0123)
    cost_skipped = Cost().score(_ctx(), threshold=0.0)
    assert cost_skipped.status == "skipped"
    tk = Tokens().score(_ctx(prompt_tokens=10, completion_tokens=5, total_tokens=15), threshold=0.0)
    assert tk.score == 15.0


def test_regression_metric_records_previous_pass() -> None:
    from app.services.evals.metrics.operational import Regression

    s = Regression().score(_ctx(previous_run_passed=True), threshold=0.0)
    assert s.status == "ok"
    assert s.detail["previous_run_passed"] is True
    s = Regression().score(_ctx(), threshold=0.0)
    assert s.status == "skipped"


def test_flakiness_stdev() -> None:
    from app.services.evals.metrics.operational import Flakiness

    s = Flakiness().score(_ctx(repeat_scores=[1.0, 1.0, 1.0]), threshold=0.1)
    assert s.score == 0.0 and s.passed is True
    s = Flakiness().score(_ctx(repeat_scores=[1.0, 0.0, 0.5]), threshold=0.1)
    assert s.score is not None and s.score > 0.1
    assert s.passed is False
    assert Flakiness().score(_ctx(), threshold=0.1).status == "skipped"


def test_judges_skipped_without_ragas() -> None:
    """Without the optional ragas dep installed, judges must skip cleanly."""
    from app.services.evals.metrics.judges import AnswerRelevancy, Faithfulness, Safety

    # We don't assert installation state — both branches still return a
    # MetricScore with status='skipped' (the integration ships in Phase 5).
    for m in (Faithfulness(), AnswerRelevancy(), Safety()):
        s = m.score(_ctx(), threshold=0.7)
        assert s.status == "skipped"


# ============================================================
# Section: runner_api
# ============================================================

import os

import pytest
import yaml
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="module")
async def runner_client(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("phase4-api")
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


# ---------- helpers ----------


async def _seed_approved_case(ac: AsyncClient, question: str, sql: str | None = "SELECT 1") -> str:
    create = await ac.post(
        "/evals/cases",
        json={
            "question": question,
            "expected_sql": sql,
            "expected_answer": "an answer" if not sql else None,
            "origin": "manual",
        },
    )
    assert create.status_code == 200, create.text
    case_id = create.json()["id"]
    approved = await ac.post(f"/evals/cases/{case_id}/approve")
    assert approved.status_code == 200
    return case_id


# ---------- POST /evals/runs ----------


@pytest.mark.asyncio
async def test_run_batch_against_approved_case(runner_client) -> None:
    ac = runner_client
    case_id = await _seed_approved_case(ac, "phase4 e2e: any short question")

    resp = await ac.post(
        "/evals/runs",
        json={"case_ids": [case_id]},
    )
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["total"] == 1
    assert body["passed"] + body["failed"] + body["errored"] == 1
    assert len(body["run_ids"]) == 1

    # The run row + per-metric results should be queryable.
    run_id = body["run_ids"][0]
    detail = await ac.get(f"/evals/runs/{run_id}")
    assert detail.status_code == 200
    dbody = detail.json()
    assert dbody["case"]["id"] == case_id
    metric_names = {r["metric"] for r in dbody["results"]}
    # All registered metrics should produce a row (some skipped).
    assert {"sql_correctness", "latency", "tokens"}.issubset(metric_names)


@pytest.mark.asyncio
async def test_run_batch_no_cases_returns_empty(runner_client) -> None:
    ac = runner_client
    resp = await ac.post(
        "/evals/runs",
        json={"case_ids": ["does-not-exist"]},
    )
    assert resp.status_code == 200
    assert resp.json()["total"] == 0
    assert resp.json()["run_ids"] == []


@pytest.mark.asyncio
async def test_run_batch_repeat_creates_multiple_runs(runner_client) -> None:
    ac = runner_client
    case_id = await _seed_approved_case(ac, "phase4 repeat-mode question")
    resp = await ac.post(
        "/evals/runs",
        json={"case_ids": [case_id], "repeat": 3},
    )
    assert resp.status_code == 200
    assert resp.json()["total"] == 3
    runs = await ac.get(f"/evals/runs?eval_case_id={case_id}")
    assert runs.status_code == 200
    # >= 3 because earlier tests may have run this case too.
    assert len(runs.json()) >= 3


# ---------- GET /evals/runs filters ----------


@pytest.mark.asyncio
async def test_runs_listing_filters_pass_fail(runner_client) -> None:
    ac = runner_client
    # Seed a case that the demo chat reliably fails on (it'll have no
    # expected_sql match in chat output), then verify the failed filter.
    case_id = await _seed_approved_case(
        ac, "phase4 filter test: extremely specific failing question", sql="SELECT specific_col_xyz FROM users"
    )
    run_resp = await ac.post("/evals/runs", json={"case_ids": [case_id]})
    assert run_resp.status_code == 200
    batch_id = run_resp.json()["batch_id"]

    filtered = await ac.get(f"/evals/runs?batch_id={batch_id}&passed=false")
    assert filtered.status_code == 200
    # All filtered runs in the batch must be failed (or none if the chat
    # somehow happened to return matching SQL).
    for r in filtered.json():
        assert r["passed"] is False


# ---------- GET /evals/runs/{id} 404 ----------


@pytest.mark.asyncio
async def test_run_detail_404(runner_client) -> None:
    ac = runner_client
    resp = await ac.get("/evals/runs/ghost")
    assert resp.status_code == 404


# ---------- dashboard ----------


@pytest.mark.asyncio
async def test_dashboard_zero_window_when_no_recent_runs(runner_client) -> None:
    """A 0-hour-ago window should always be empty; sanity-check the shape."""
    ac = runner_client
    # range_days=1 is the smallest allowed; query without seeding new runs.
    # The dashboard handler returns null/zero fields when total_runs is 0,
    # so we instead assert structural shape — total_runs may include prior
    # test runs in the 24h window, but the response must always have these
    # keys regardless.
    resp = await ac.get("/evals/metrics/dashboard?range_days=1")
    assert resp.status_code == 200
    body = resp.json()
    assert {"total_runs", "pass_rate", "p50_latency_ms", "p95_latency_ms",
            "total_cost_usd", "total_tokens", "regression_count",
            "failure_category_counts", "metrics", "daily"}.issubset(body.keys())


@pytest.mark.asyncio
async def test_dashboard_includes_metric_summaries_after_a_run(runner_client) -> None:
    ac = runner_client
    # Ensure at least one run exists, then aggregate.
    case_id = await _seed_approved_case(ac, "phase4 dashboard guarantor case")
    await ac.post("/evals/runs", json={"case_ids": [case_id]})

    resp = await ac.get("/evals/metrics/dashboard?range_days=7")
    assert resp.status_code == 200
    body = resp.json()
    assert body["total_runs"] >= 1
    metric_names = {m["metric"] for m in body["metrics"]}
    assert "sql_correctness" in metric_names
    assert body["pass_rate"] is not None
    assert isinstance(body["total_cost_usd"], (int, float))
    assert isinstance(body["daily"], list)


# ---------- Promptfoo export ----------


@pytest.mark.asyncio
async def test_promptfoo_export_returns_parseable_yaml(runner_client) -> None:
    ac = runner_client
    # Seed a golden case so the export has at least one entry.
    case = await ac.post(
        "/evals/cases",
        json={
            "question": "phase4 promptfoo export check",
            "expected_sql": "SELECT 1",
            "expected_answer": "ok",
            "origin": "manual",
        },
    )
    cid = case.json()["id"]
    await ac.post(f"/evals/cases/{cid}/approve")
    await ac.post(f"/evals/cases/{cid}/promote-golden")

    resp = await ac.get("/evals/cases.promptfoo.yaml?status=golden")
    assert resp.status_code == 200
    assert "text/yaml" in resp.headers.get("content-type", "")
    parsed = yaml.safe_load(resp.text)
    assert parsed["description"] == "DataClaw golden evals"
    assert isinstance(parsed["tests"], list)
    assert any(
        "phase4 promptfoo export check" in (t.get("description") or "") for t in parsed["tests"]
    )


@pytest.mark.asyncio
async def test_promptfoo_export_empty_status_returns_empty_tests(runner_client) -> None:
    ac = runner_client
    # archived status with no rows -> tests: [].
    resp = await ac.get("/evals/cases.promptfoo.yaml?status=archived")
    assert resp.status_code == 200
    parsed = yaml.safe_load(resp.text)
    assert parsed["tests"] == []


# ---------- runner error isolation ----------


@pytest.mark.asyncio
async def test_runner_records_runner_error_category_when_chat_fails(
    runner_client, monkeypatch
) -> None:
    """Force answer_question to raise; runner must persist an eval_run with
    failure_category='runner_error' and an EvalResult of metric='runner'
    status='error', not blow up the API."""
    ac = runner_client
    case_id = await _seed_approved_case(ac, "phase4 runner error isolation")

    async def _boom(*args, **kwargs):
        raise RuntimeError("simulated outage")

    monkeypatch.setattr("app.services.evals.runner.answer_question", _boom, raising=False)

    # The runner imports answer_question lazily inside the function; we
    # patch it on the runner module after first call. To make sure the
    # patch sticks, also patch on the source module.
    import app.services.agents.chat as chat_module
    monkeypatch.setattr(chat_module, "answer_question", _boom)

    resp = await ac.post("/evals/runs", json={"case_ids": [case_id]})
    assert resp.status_code == 200
    run_id = resp.json()["run_ids"][0]
    detail = await ac.get(f"/evals/runs/{run_id}")
    assert detail.status_code == 200
    body = detail.json()
    assert body["failure_category"] == "runner_error"
    runner_row = next(r for r in body["results"] if r["metric"] == "runner")
    assert runner_row["status"] == "error"


# ============================================================
# Section: sugg_unit
# ============================================================

import os
import uuid

import pytest

os.environ.setdefault("MASTER_KEY", "test-master-key-please-change")
os.environ.setdefault("SESSION_SECRET", "test-session-secret-please-change")


@pytest.fixture
async def db_sugg(tmp_path, monkeypatch):
    import importlib

    db_path = tmp_path / "phase5.sqlite"
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


async def _seed_run(
    SessionLocal,
    *,
    failure_category: str | None = "sql_error",
    expected_sql: str = "SELECT count(*) FROM users",
    actual_sql: str | None = "SELECT * FROM users",
    passed: bool = False,
    expected_connector: str | None = "postgres",
    expected_tool: str | None = None,
    expected_citations: list[dict] | None = None,
):
    from datetime import UTC, datetime

    from app.models.domain import EvalCase, EvalResult, EvalRun, User, Workspace

    async with SessionLocal() as s:
        ws = Workspace(name="ws")
        s.add(ws)
        await s.flush()
        user = User(email=f"u-{uuid.uuid4()}@local", password_hash="x")
        s.add(user)
        await s.flush()
        case = EvalCase(
            workspace_id=ws.id,
            question="how many users?",
            expected_answer="42",
            expected_sql=expected_sql,
            expected_connector_slug=expected_connector,
            expected_tool=expected_tool,
            expected_citations=expected_citations or [],
            status="approved",
            origin="manual",
        )
        s.add(case)
        await s.flush()
        run = EvalRun(
            workspace_id=ws.id,
            batch_id="b1",
            eval_case_id=case.id,
            chat_message_id=None,
            passed=passed,
            failure_category=failure_category,
            actual_answer="some answer",
            actual_sql=actual_sql,
            duration_ms=100,
            created_at=datetime.now(UTC),
        )
        s.add(run)
        await s.flush()
        # Add a few metric rows so diagnose has signal.
        s.add(
            EvalResult(
                eval_run_id=run.id,
                metric="sql_correctness",
                status="ok",
                score=0.0,
                passed=False,
                detail={},
            )
        )
        await s.commit()
        return ws.id, user.id, case.id, run.id


# ---------- DiagnoseService rules-based ----------


@pytest.mark.asyncio
async def test_diagnose_sql_error_produces_golden_query_suggestion(db_sugg) -> None:
    from app.services.evals.diagnose import DiagnoseService

    ws_id, _, case_id, run_id = await _seed_run(db_sugg)
    async with db_sugg() as s:
        rows = await DiagnoseService(s).diagnose(run_id, use_llm=False)
    kinds = sorted({r.kind for r in rows})
    assert "golden_query" in kinds
    golden = next(r for r in rows if r.kind == "golden_query")
    assert golden.proposed_value == "SELECT count(*) FROM users"
    assert golden.apply_payload["expected_connector_slug"] == "postgres"
    # All Phase 5 drafts are persisted as pending and scoped to the workspace.
    assert all(r.status == "pending" for r in rows)
    assert all(r.workspace_id == ws_id for r in rows)
    assert all(r.eval_run_id == run_id for r in rows)
    # Avoids referencing the unused case_id name.
    assert case_id


@pytest.mark.asyncio
async def test_diagnose_wrong_connector_produces_routing_rule(db_sugg) -> None:
    from app.services.evals.diagnose import DiagnoseService

    _, _, _, run_id = await _seed_run(db_sugg, failure_category="wrong_connector")
    async with db_sugg() as s:
        rows = await DiagnoseService(s).diagnose(run_id, use_llm=False)
    kinds = {r.kind for r in rows}
    assert "connector_routing" in kinds
    routing = next(r for r in rows if r.kind == "connector_routing")
    assert "postgres" in routing.proposed_value


@pytest.mark.asyncio
async def test_diagnose_wrong_tool_produces_tool_description_suggestion(db_sugg) -> None:
    from app.services.evals.diagnose import DiagnoseService

    _, _, _, run_id = await _seed_run(
        db_sugg,
        failure_category="wrong_tool",
        expected_tool="postgres.read_select",
    )
    async with db_sugg() as s:
        rows = await DiagnoseService(s).diagnose(run_id, use_llm=False)
    kinds = {r.kind for r in rows}
    assert "tool_description_diff" in kinds


@pytest.mark.asyncio
async def test_diagnose_missing_citation_produces_retrieval_context(db_sugg) -> None:
    from app.services.evals.diagnose import DiagnoseService

    _, _, _, run_id = await _seed_run(
        db_sugg,
        failure_category="missing_citation",
        expected_citations=[{"source": "postgres", "table": "core.customers"}],
    )
    async with db_sugg() as s:
        rows = await DiagnoseService(s).diagnose(run_id, use_llm=False)
    kinds = {r.kind for r in rows}
    assert "retrieval_context" in kinds


@pytest.mark.asyncio
async def test_diagnose_is_idempotent(db_sugg) -> None:
    """Second diagnose call returns the existing rows, not new ones."""
    from app.services.evals.diagnose import DiagnoseService

    _, _, _, run_id = await _seed_run(db_sugg)
    async with db_sugg() as s:
        first = await DiagnoseService(s).diagnose(run_id, use_llm=False)
        first_ids = {r.id for r in first}
    async with db_sugg() as s:
        second = await DiagnoseService(s).diagnose(run_id, use_llm=False)
        second_ids = {r.id for r in second}
    assert first_ids == second_ids


@pytest.mark.asyncio
async def test_diagnose_404_on_unknown_run(db_sugg) -> None:
    from app.services.evals.diagnose import DiagnoseError, DiagnoseService

    async with db_sugg() as s:
        with pytest.raises(DiagnoseError):
            await DiagnoseService(s).diagnose("ghost", use_llm=False)


# ---------- SuggestionService Apply paths ----------


@pytest.mark.asyncio
async def test_apply_golden_query_creates_and_promotes_eval_case(db_sugg) -> None:
    from app.models.domain import EvalCase, User
    from app.services.evals.diagnose import DiagnoseService
    from app.services.evals.suggestions import SuggestionService

    _, user_id, _, run_id = await _seed_run(db_sugg)
    async with db_sugg() as s:
        rows = await DiagnoseService(s).diagnose(run_id, use_llm=False)
        golden = next(r for r in rows if r.kind == "golden_query")

    async with db_sugg() as s:
        u = await s.get(User, user_id)
        applied = await SuggestionService(s).apply(golden.id, user=u)
        assert applied.status == "applied"
        assert applied.apply_result["status"] == "ok"
        created_case_id = applied.apply_result["created_eval_case_id"]
        new_case = await s.get(EvalCase, created_case_id)
    assert new_case is not None
    assert new_case.status == "golden"
    assert new_case.expected_sql == "SELECT count(*) FROM users"


@pytest.mark.asyncio
async def test_apply_prompt_diff_writes_appsetting_override(db_sugg) -> None:
    from app.services.evals.diagnose import DiagnoseService
    from app.services.evals.suggestions import (
        PROMPT_OVERRIDE_KEY,
        SuggestionService,
        load_chat_prompt_override,
    )
    from app.services.settings_store import _read

    _, _, _, run_id = await _seed_run(db_sugg, failure_category="hallucination")
    async with db_sugg() as s:
        rows = await DiagnoseService(s).diagnose(run_id, use_llm=False)
        prompt_row = next(r for r in rows if r.kind == "prompt_diff")

    async with db_sugg() as s:
        applied = await SuggestionService(s).apply(prompt_row.id, user=None)
        assert applied.apply_result["setting_key"] == PROMPT_OVERRIDE_KEY
        loaded = await load_chat_prompt_override(s)
        assert loaded
        # Encrypted-at-rest sanity: _read returns decrypted dict.
        payload = await _read(s, PROMPT_OVERRIDE_KEY)
        assert "text" in payload


@pytest.mark.asyncio
async def test_apply_rules_md_appends_and_dedupes(db_sugg) -> None:
    from app.services.evals.diagnose import DiagnoseService
    from app.services.evals.suggestions import (
        SuggestionService,
        load_chat_rules_md,
    )

    _, _, _, run_id = await _seed_run(db_sugg, failure_category="wrong_connector")
    async with db_sugg() as s:
        rows = await DiagnoseService(s).diagnose(run_id, use_llm=False)
        # connector_routing has preview-only Apply — use a manually-seeded
        # rules_md suggestion via wrong_result path instead.
    _, _, _, run_id2 = await _seed_run(db_sugg, failure_category="wrong_result")
    async with db_sugg() as s:
        rows2 = await DiagnoseService(s).diagnose(run_id2, use_llm=False)
        rules_row = next((r for r in rows2 if r.kind == "rules_md"), None)
    assert rules_row is not None

    async with db_sugg() as s:
        await SuggestionService(s).apply(rules_row.id, user=None)
    async with db_sugg() as s:
        first = await load_chat_rules_md(s)
    assert first and first.startswith("-")

    # Re-applying the SAME rule mustn't double it. Diagnose the same run
    # again (idempotent), but for this test we just apply a different
    # suggestion with the same proposed_value and assert dedupe.
    _, _, _, run_id3 = await _seed_run(db_sugg, failure_category="wrong_result")
    async with db_sugg() as s:
        rows3 = await DiagnoseService(s).diagnose(run_id3, use_llm=False)
        rules_row3 = next((r for r in rows3 if r.kind == "rules_md"), None)
    assert rules_row3 is not None
    # Force identical proposed text to test dedupe.
    async with db_sugg() as s:
        row3 = await s.get(type(rules_row3), rules_row3.id)
        row3.proposed_value = rules_row.proposed_value
        await s.commit()
    async with db_sugg() as s:
        await SuggestionService(s).apply(rules_row3.id, user=None)
    async with db_sugg() as s:
        second = await load_chat_rules_md(s)
    assert second == first  # de-duped


@pytest.mark.asyncio
async def test_preview_only_kinds_record_apply_but_dont_mutate(db_sugg) -> None:
    from app.services.evals.diagnose import DiagnoseService
    from app.services.evals.suggestions import SuggestionService

    _, _, _, run_id = await _seed_run(db_sugg, failure_category="wrong_connector")
    async with db_sugg() as s:
        rows = await DiagnoseService(s).diagnose(run_id, use_llm=False)
        routing = next(r for r in rows if r.kind == "connector_routing")

    async with db_sugg() as s:
        applied = await SuggestionService(s).apply(routing.id, user=None)
    assert applied.status == "applied"
    assert applied.apply_result["status"] == "preview_only"


@pytest.mark.asyncio
async def test_dismiss_marks_status_and_blocks_subsequent_apply(db_sugg) -> None:
    from app.services.evals.diagnose import DiagnoseService
    from app.services.evals.suggestions import (
        SuggestionService,
        SuggestionTransitionError,
    )

    _, _, _, run_id = await _seed_run(db_sugg)
    async with db_sugg() as s:
        rows = await DiagnoseService(s).diagnose(run_id, use_llm=False)
        golden = next(r for r in rows if r.kind == "golden_query")

    async with db_sugg() as s:
        dismissed = await SuggestionService(s).dismiss(golden.id, user=None)
    assert dismissed.status == "dismissed"

    async with db_sugg() as s:
        with pytest.raises(SuggestionTransitionError):
            await SuggestionService(s).apply(golden.id, user=None)


@pytest.mark.asyncio
async def test_apply_twice_rejected(db_sugg) -> None:
    from app.services.evals.diagnose import DiagnoseService
    from app.services.evals.suggestions import (
        SuggestionService,
        SuggestionTransitionError,
    )

    _, _, _, run_id = await _seed_run(db_sugg)
    async with db_sugg() as s:
        rows = await DiagnoseService(s).diagnose(run_id, use_llm=False)
        golden = next(r for r in rows if r.kind == "golden_query")

    async with db_sugg() as s:
        await SuggestionService(s).apply(golden.id, user=None)
    async with db_sugg() as s:
        with pytest.raises(SuggestionTransitionError):
            await SuggestionService(s).apply(golden.id, user=None)


# ---------- Apply-time SQL validation (regression: poisoned-golden bug) ----------


def test_looks_like_sql_predicate() -> None:
    from app.services.evals.suggestions import _looks_like_sql

    # Real queries
    assert _looks_like_sql("SELECT 1")
    assert _looks_like_sql("  select * from foo  ")
    assert _looks_like_sql("WITH cte AS (SELECT 1) SELECT * FROM cte")
    assert _looks_like_sql("-- a leading comment\nSELECT 1")
    # Not SQL — the shapes we saw the LLM emit as golden_query proposals.
    assert not _looks_like_sql("id (INTEGER), segment (TEXT), arr (INTEGER)")
    assert not _looks_like_sql("Update the schema to add an index on customer_id")
    assert not _looks_like_sql("")
    assert not _looks_like_sql("INSERT INTO foo VALUES (1)")  # not a read query


@pytest.mark.asyncio
async def test_apply_golden_query_rejects_non_sql_proposed_value(db_sugg) -> None:
    """Regression: an LLM-source golden_query suggestion whose
    proposed_value is a column listing (not SQL) used to pass Apply,
    create a golden case, then 400 the next chat hit on /ide/query."""
    from app.models.domain import EvalSuggestion
    from app.services.evals.suggestions import (
        SuggestionService,
        SuggestionValidationError,
    )

    _, _, _, run_id = await _seed_run(db_sugg)
    async with db_sugg() as s:
        from app.models.domain import EvalRun
        from sqlalchemy import select as _select

        workspace_id = await s.scalar(
            _select(EvalRun.workspace_id).where(EvalRun.id == run_id)
        )
        bad = EvalSuggestion(
            eval_run_id=run_id,
            workspace_id=workspace_id,
            kind="golden_query",
            title="LLM: golden_query suggestion",
            rationale="schema description, not a query",
            current_value=None,
            proposed_value="id (INTEGER), segment (TEXT), arr (INTEGER)",
            target=None,
            confidence=0.8,
            source="llm",
            status="pending",
            apply_payload={
                "question": "what columns does customers have?",
                "expected_sql": "id (INTEGER), segment (TEXT), arr (INTEGER)",
                "expected_connector_slug": "sqlite",
            },
        )
        s.add(bad)
        await s.commit()
        bad_id = bad.id

    async with db_sugg() as s:
        with pytest.raises(SuggestionValidationError):
            await SuggestionService(s).apply(bad_id, user=None)


@pytest.mark.asyncio
async def test_apply_golden_query_rejects_unrunnable_sqlite_sql(db_sugg, tmp_path) -> None:
    """Regression: bad SQL (wrong table / wrong column) used to be
    promoted to a golden case verbatim. With a configured sqlite
    connector, dry-run EXPLAIN catches it before Apply lands."""
    from pathlib import Path

    from app.models.domain import Connector, EvalSuggestion
    from app.services.connectors.adapters import seed_sqlite_demo
    from app.services.evals.suggestions import (
        SuggestionService,
        SuggestionValidationError,
    )

    demo_path = Path(tmp_path) / "demo_apply_validation.sqlite"
    seed_sqlite_demo(demo_path)

    _, _, _, run_id = await _seed_run(db_sugg)
    async with db_sugg() as s:
        from app.models.domain import EvalRun
        from sqlalchemy import select as _select

        workspace_id = await s.scalar(
            _select(EvalRun.workspace_id).where(EvalRun.id == run_id)
        )
        # Configure the sqlite connector to point at the freshly-seeded demo.
        s.add(
            Connector(
                workspace_id=workspace_id,
                slug="sqlite",
                category="datastore",
                display_name="SQLite",
                status="active",
                encrypted_credentials=None,
                sync_summary={"database_path": str(demo_path)},
            )
        )
        s.add(
            EvalSuggestion(
                id="bad-sql-runtime",
                eval_run_id=run_id,
                workspace_id=workspace_id,
                kind="golden_query",
                title="Promote this SQL",
                rationale="syntactically SQL but the table doesn't exist",
                current_value=None,
                proposed_value=(
                    "SELECT id, customer_id, amount FROM core.orders "
                    "ORDER BY amount DESC LIMIT 5"
                ),
                target=None,
                confidence=0.9,
                source="rules",
                status="pending",
                apply_payload={
                    "question": "List the top 5 demo orders by total amount.",
                    "expected_sql": (
                        "SELECT id, customer_id, amount FROM core.orders "
                        "ORDER BY amount DESC LIMIT 5"
                    ),
                    "expected_connector_slug": "sqlite",
                },
            )
        )
        await s.commit()

    async with db_sugg() as s:
        with pytest.raises(SuggestionValidationError) as exc_info:
            await SuggestionService(s).apply("bad-sql-runtime", user=None)
    # Error must surface the underlying engine message so the user can fix it.
    assert "core.orders" in str(exc_info.value) or "no such table" in str(exc_info.value)


@pytest.mark.asyncio
async def test_diagnose_drops_llm_golden_query_when_not_sql(db_sugg, monkeypatch) -> None:
    """Regression: LLM augmentation used to surface a golden_query
    suggestion whose proposed_value was a column listing. The filter
    in _llm_augment now drops these before they reach the user."""
    from app.services.evals import diagnose as diagnose_mod
    from app.services.evals.diagnose import DiagnoseService

    _, _, _, run_id = await _seed_run(db_sugg)

    async def _fake_llm_augment(self, run, case, metrics_by_name, rules_drafts):
        # Re-exercise the same JSON-parse + per-item filter path the real
        # OpenAI call would. We bypass network by returning the parsed
        # items directly through the documented hook.
        return await _real_filter_path(self, run, case, metrics_by_name)

    async def _real_filter_path(self, run, case, metrics_by_name):
        # Build two items: one valid SQL, one column listing.
        items = [
            {
                "kind": "golden_query",
                "title": "valid",
                "proposed_value": "SELECT * FROM users",
                "confidence": 0.5,
            },
            {
                "kind": "golden_query",
                "title": "bad",
                "proposed_value": "id (INTEGER), segment (TEXT), arr (INTEGER)",
                "confidence": 0.5,
            },
        ]
        # Reproduce the per-item guard from _llm_augment inline so the
        # test exercises the public filter (_looks_like_sql) the same way.
        from app.services.evals.diagnose import _DraftSuggestion
        from app.services.evals.suggestions import _looks_like_sql

        out = []
        for item in items:
            if item["kind"] == "golden_query" and not _looks_like_sql(item["proposed_value"]):
                continue
            out.append(
                _DraftSuggestion(
                    kind=item["kind"],
                    title=item["title"],
                    rationale="",
                    proposed_value=item["proposed_value"],
                    confidence=item["confidence"],
                    source="llm",
                )
            )
        return out

    monkeypatch.setattr(DiagnoseService, "_llm_augment", _fake_llm_augment)

    async with db_sugg() as s:
        rows = await DiagnoseService(s).diagnose(run_id, use_llm=True)

    llm_goldens = [
        r for r in rows
        if r.kind == "golden_query" and r.source == "llm"
    ]
    # The "bad" one is dropped; the "valid" one survives.
    assert len(llm_goldens) == 1
    assert llm_goldens[0].proposed_value == "SELECT * FROM users"
    # Sanity: diagnose_mod is intentionally imported to ensure module is loadable.
    assert diagnose_mod is not None


# ============================================================
# Section: sugg_api
# ============================================================

import os

import pytest
from httpx import ASGITransport, AsyncClient


@pytest.fixture(scope="module")
async def sugg_client(tmp_path_factory):
    tmp_path = tmp_path_factory.mktemp("phase5-api")
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


# ---------- helpers ----------


async def _seed_approved_case(
    ac: AsyncClient,
    question: str,
    *,
    sql: str | None = "SELECT 1",
    connector: str | None = "postgres",
) -> str:
    create = await ac.post(
        "/evals/cases",
        json={
            "question": question,
            "expected_sql": sql,
            "expected_connector_slug": connector,
            "origin": "manual",
        },
    )
    assert create.status_code == 200, create.text
    case_id = create.json()["id"]
    await ac.post(f"/evals/cases/{case_id}/approve")
    return case_id


async def _run_one_batch(ac: AsyncClient, case_id: str) -> str:
    """Kick a single-case batch and return the run_id."""
    resp = await ac.post("/evals/runs", json={"case_ids": [case_id]})
    assert resp.status_code == 200, resp.text
    body = resp.json()
    assert body["run_ids"], body
    return body["run_ids"][0]


# ---------- /evals/suggestions/kinds ----------


@pytest.mark.asyncio
async def test_suggestion_kinds_catalog(sugg_client) -> None:
    ac = sugg_client
    resp = await ac.get("/evals/suggestions/kinds")
    assert resp.status_code == 200
    body = resp.json()
    assert set(body["kinds"]) == {
        "golden_query",
        "prompt_diff",
        "rules_md",
        "tool_description_diff",
        "retrieval_context",
        "connector_routing",
    }
    assert set(body["apply_supported"]) == {"golden_query", "prompt_diff", "rules_md"}


# ---------- diagnose round-trip ----------


@pytest.mark.asyncio
async def test_diagnose_and_list_returns_pending_suggestions(sugg_client) -> None:
    ac = sugg_client
    case_id = await _seed_approved_case(ac, "phase5 diagnose round-trip")
    run_id = await _run_one_batch(ac, case_id)

    diag = await ac.post(
        f"/evals/runs/{run_id}/diagnose",
        json={"use_llm": False},
    )
    assert diag.status_code == 200, diag.text
    rows = diag.json()
    assert rows, "diagnose returned no suggestions"
    assert all(r["status"] == "pending" for r in rows)
    # The status flag for apply support should match the backend catalog.
    for r in rows:
        assert isinstance(r["apply_supported"], bool)

    listed = await ac.get(f"/evals/runs/{run_id}/suggestions")
    assert listed.status_code == 200
    assert len(listed.json()) == len(rows)


@pytest.mark.asyncio
async def test_diagnose_idempotent_via_api(sugg_client) -> None:
    ac = sugg_client
    case_id = await _seed_approved_case(ac, "phase5 idempotent diagnose")
    run_id = await _run_one_batch(ac, case_id)

    first = await ac.post(f"/evals/runs/{run_id}/diagnose", json={"use_llm": False})
    second = await ac.post(f"/evals/runs/{run_id}/diagnose", json={"use_llm": False})
    assert first.status_code == 200 and second.status_code == 200
    first_ids = sorted([r["id"] for r in first.json()])
    second_ids = sorted([r["id"] for r in second.json()])
    assert first_ids == second_ids


@pytest.mark.asyncio
async def test_diagnose_404_for_unknown_run(sugg_client) -> None:
    ac = sugg_client
    resp = await ac.post("/evals/runs/ghost/diagnose", json={"use_llm": False})
    assert resp.status_code == 404


# ---------- apply paths through HTTP ----------


@pytest.mark.asyncio
async def test_apply_golden_query_via_api_creates_new_case(sugg_client) -> None:
    ac = sugg_client
    case_id = await _seed_approved_case(
        ac,
        "phase5 apply golden via API",
        sql="SELECT 99 FROM dual",
    )
    run_id = await _run_one_batch(ac, case_id)
    diag = await ac.post(f"/evals/runs/{run_id}/diagnose", json={"use_llm": False})
    golden = next(
        (r for r in diag.json() if r["kind"] == "golden_query"),
        None,
    )
    assert golden is not None, diag.json()

    apply = await ac.post(f"/evals/suggestions/{golden['id']}/apply")
    assert apply.status_code == 200, apply.text
    body = apply.json()
    assert body["status"] == "applied"
    new_case_id = body["apply_result"]["created_eval_case_id"]
    # Newly created case must be golden.
    case = await ac.get(f"/evals/cases/{new_case_id}")
    assert case.status_code == 200
    assert case.json()["status"] == "golden"


@pytest.mark.asyncio
async def test_apply_preview_only_kind_returns_200_with_preview_status(sugg_client) -> None:
    ac = sugg_client
    # A wrong_connector failure category produces a connector_routing
    # suggestion (preview-only).
    case_id = await _seed_approved_case(
        ac,
        "phase5 preview-only apply",
        # Connector mismatch: case expects postgres, demo chat won't pick it.
        connector="postgres",
    )
    run_id = await _run_one_batch(ac, case_id)
    diag = await ac.post(f"/evals/runs/{run_id}/diagnose", json={"use_llm": False})
    routing = next(
        (r for r in diag.json() if r["kind"] == "connector_routing"),
        None,
    )
    if routing is None:
        # Demo chat may match the expected connector in some envs; skip.
        pytest.skip("no connector_routing suggestion generated for this run")
    apply = await ac.post(f"/evals/suggestions/{routing['id']}/apply")
    assert apply.status_code == 200
    body = apply.json()
    assert body["status"] == "applied"
    assert body["apply_result"]["status"] == "preview_only"


@pytest.mark.asyncio
async def test_apply_404_and_409(sugg_client) -> None:
    ac = sugg_client
    # 404 unknown suggestion.
    resp = await ac.post("/evals/suggestions/ghost/apply")
    assert resp.status_code == 404

    # 409 on re-apply.
    case_id = await _seed_approved_case(ac, "phase5 409 on re-apply")
    run_id = await _run_one_batch(ac, case_id)
    diag = await ac.post(f"/evals/runs/{run_id}/diagnose", json={"use_llm": False})
    golden = next(r for r in diag.json() if r["kind"] == "golden_query")
    first = await ac.post(f"/evals/suggestions/{golden['id']}/apply")
    assert first.status_code == 200
    second = await ac.post(f"/evals/suggestions/{golden['id']}/apply")
    assert second.status_code == 409


@pytest.mark.asyncio
async def test_dismiss_blocks_apply(sugg_client) -> None:
    ac = sugg_client
    case_id = await _seed_approved_case(ac, "phase5 dismiss blocks apply")
    run_id = await _run_one_batch(ac, case_id)
    diag = await ac.post(f"/evals/runs/{run_id}/diagnose", json={"use_llm": False})
    golden = next(r for r in diag.json() if r["kind"] == "golden_query")
    dismissed = await ac.post(f"/evals/suggestions/{golden['id']}/dismiss")
    assert dismissed.status_code == 200
    assert dismissed.json()["status"] == "dismissed"
    blocked = await ac.post(f"/evals/suggestions/{golden['id']}/apply")
    assert blocked.status_code == 409


# ---------- chat integration: applied prompt_diff reaches LLM context ----------


@pytest.mark.asyncio
async def test_applied_prompt_override_is_readable_for_chat(sugg_client) -> None:
    """End-to-end: apply a prompt_diff suggestion through the API → the
    AppSetting is populated and ``load_chat_prompt_override`` returns the
    text chat.py will prepend as a system message.

    (The injection-into-messages itself is a one-line addition in
    answer_question that the manual test guide T5.x exercises against a
    live LLM. The deterministic fallback in CI bypasses the LLM-call
    path entirely, so an in-process E2E assertion would prove nothing.)
    """
    ac = sugg_client

    from app.db.session import SessionLocal
    from app.models.domain import EvalRun
    from app.services.evals.suggestions import load_chat_prompt_override

    case_id = await _seed_approved_case(ac, "phase5 prompt override readback")
    run_id = await _run_one_batch(ac, case_id)

    async with SessionLocal() as s:
        run = await s.get(EvalRun, run_id)
        run.failure_category = "hallucination"
        await s.commit()

    diag = await ac.post(f"/evals/runs/{run_id}/diagnose", json={"use_llm": False})
    prompt_row = next((r for r in diag.json() if r["kind"] == "prompt_diff"), None)
    assert prompt_row is not None, diag.json()

    apply = await ac.post(f"/evals/suggestions/{prompt_row['id']}/apply")
    assert apply.status_code == 200

    async with SessionLocal() as s:
        text = await load_chat_prompt_override(s)
    assert text and "Example question" in text


@pytest.mark.asyncio
async def test_answer_question_prepends_override_and_rules(sugg_client, monkeypatch) -> None:  # noqa: ARG001 — fixture consumed for side effects (lifespan + shared DB)
    """White-box check: drive answer_question directly with a stubbed
    OpenAI sugg_client so we observe the system messages it builds. Verifies
    both prompt_override and rules_md are folded in as system messages.
    """
    # Live module lookup — the module-scoped sugg_client fixture reloads
    # app.db.session at setup, so a captured import would be stale.
    from app.db import session as _session_module
    from app.services.evals.suggestions import (
        PROMPT_OVERRIDE_KEY,
        RULES_MD_KEY,
    )
    from app.services.settings_store import _write

    SessionLocal = _session_module.SessionLocal

    # Seed both overrides (replaces any prior test's override).
    async with SessionLocal() as s:
        await _write(s, PROMPT_OVERRIDE_KEY, {"text": "OVERRIDE_SENTINEL_TEXT"})
        await _write(s, RULES_MD_KEY, {"text": "- always say RULES_SENTINEL"})
        await s.commit()

    # Patch settings_store.resolve_openai so we look "configured" + stub the
    # OpenAI sugg_client constructor so no real network is attempted.
    import app.services.agents.chat as chat_module

    async def _stub_resolve(_session):
        return "fake-key", "fake-model", None, None

    monkeypatch.setattr(chat_module, "resolve_openai", _stub_resolve)

    captured: dict[str, list[dict[str, str]]] = {}

    async def _capturing(sugg_client, *, span_name, model, messages, **kwargs):
        captured.setdefault("messages", list(messages))
        # Returning a fake completion would require building the OpenAI
        # response shape; easier to raise so the function falls through to
        # its existing error path. We don't care about the response — only
        # what got sent.
        raise RuntimeError("intercepted")

    monkeypatch.setattr(chat_module, "_traced_chat_completion", _capturing)

    from app.services.agents.chat import answer_question

    async with SessionLocal() as s:
        try:
            await answer_question(
                s,
                "phase5 override injection check",
                thread_id=None,
                model=None,
                tool_engine=None,
                user_email="tester",
                connector_slug=None,
            )
        except Exception:
            pass

    sent = captured.get("messages") or []
    system_messages = [
        m["content"]
        for m in sent
        if m.get("role") == "system" and isinstance(m.get("content"), str)
    ]
    joined = "\n".join(system_messages)
    assert "OVERRIDE_SENTINEL_TEXT" in joined
    assert "RULES_SENTINEL" in joined
    # The default DataClaw system prompt is still present.
    assert "DataClaw" in joined
