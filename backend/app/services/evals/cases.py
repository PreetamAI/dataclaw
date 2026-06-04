"""Eval-case domain service.

Responsibilities (Phase 2):
* CRUD on ``eval_cases``.
* Create-from-feedback: turn a 👎 + correction into a candidate eval case
  with provenance pointing back at the source chat message.
* Lifecycle transitions: candidate → approved → golden → archived. Only
  forward-from-candidate-or-approved-only paths are allowed; promoting
  straight from candidate to golden is explicitly disallowed (the user
  must approve first).
* Golden-query lookup used by the chat agent before generating fresh SQL.

Out of scope here:
* Result-set hashing on promote-golden (Phase 4 needs a connector engine
  to execute the expected SQL; for now we leave the field NULL and
  the eval-runner fills it on first run).
* Vector similarity index over questions (Phase 3 wiring; until then
  the lookup uses normalized exact + ILIKE-style match).
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from typing import Any

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.domain import ChatMessage, EvalCase, Feedback, User

logger = logging.getLogger(__name__)


# ---------- public enums ----------

CASE_STATUSES = ("candidate", "approved", "golden", "archived")
CASE_ORIGINS = (
    "manual",
    "feedback",
    "auto:schema",
    "auto:kg",
    "auto:lineage",
    "auto:dbt",
    "auto:airflow",
    "auto:dagster",
    "auto:fixture",
)


# ---------- errors ----------


class EvalCaseValidationError(ValueError):
    """Surface as 400."""


class EvalCaseNotFound(LookupError):
    """Surface as 404."""


class EvalCaseTransitionError(ValueError):
    """Disallowed status transition (surface as 409)."""


# ---------- DTOs ----------


@dataclass
class EvalCaseInput:
    workspace_id: str
    question: str
    expected_answer: str | None = None
    expected_sql: str | None = None
    expected_connector_slug: str | None = None
    expected_tool: str | None = None
    expected_citations: list[dict] | None = None
    tags: list[str] | None = None
    origin: str = "manual"
    source_chat_message_id: str | None = None
    status: str = "candidate"


@dataclass
class EvalCasePatch:
    question: str | None = None
    expected_answer: str | None = None
    expected_sql: str | None = None
    expected_connector_slug: str | None = None
    expected_tool: str | None = None
    expected_citations: list[dict] | None = None
    tags: list[str] | None = None


# ---------- service ----------


class EvalCaseService:
    def __init__(self, session: AsyncSession):
        self._session = session

    # --- create ---

    async def create(
        self,
        payload: EvalCaseInput,
        *,
        created_by: User | None = None,
    ) -> EvalCase:
        _validate_status(payload.status)
        _validate_origin(payload.origin)
        question = (payload.question or "").strip()
        if not question:
            raise EvalCaseValidationError("question is required")
        # Auto-generated candidates can land without expected_* (the user
        # fills them in during review). Manual + feedback origins must
        # contain at least one expected_* field; otherwise the case has
        # nothing to grade against.
        if payload.origin in {"manual", "feedback"} and not any(
            (
                payload.expected_answer,
                payload.expected_sql,
                payload.expected_tool,
            )
        ):
            raise EvalCaseValidationError(
                "manual / feedback eval cases need at least one of "
                "expected_answer, expected_sql, expected_tool"
            )
        case = EvalCase(
            workspace_id=payload.workspace_id,
            question=question,
            expected_answer=_clean(payload.expected_answer),
            expected_sql=_clean(payload.expected_sql),
            expected_connector_slug=_clean(payload.expected_connector_slug),
            expected_tool=_clean(payload.expected_tool),
            expected_citations=payload.expected_citations or [],
            tags=payload.tags or [],
            status=payload.status,
            origin=payload.origin,
            source_chat_message_id=payload.source_chat_message_id,
            created_by=created_by.id if created_by else None,
        )
        self._session.add(case)
        await self._session.flush()
        return case

    async def create_from_feedback(
        self,
        *,
        chat_message_id: str,
        question: str | None = None,
        expected_answer: str | None = None,
        expected_sql: str | None = None,
        expected_connector_slug: str | None = None,
        expected_tool: str | None = None,
        expected_citations: list[dict] | None = None,
        tags: list[str] | None = None,
        created_by: User | None = None,
    ) -> EvalCase:
        """Promote a 👎 feedback to a candidate eval case carrying the
        user's correction. Also stamps ``feedback.eval_case_id`` so the
        chat UI can show that a correction has already been captured."""
        message = await self._session.get(ChatMessage, chat_message_id)
        if message is None:
            raise EvalCaseNotFound(chat_message_id)
        if message.role != "assistant":
            raise EvalCaseValidationError(
                "eval cases derive from assistant answers, not user questions"
            )
        # Resolve the question: prefer the explicit override, else look up the
        # paired user message in the same thread.
        if not question:
            question = await self._previous_user_question(message)
        if not question:
            raise EvalCaseValidationError(
                "could not determine the original question; pass `question` explicitly"
            )
        # Workspace via thread → workspace_id (single hop).
        from app.models.domain import ChatThread

        thread = await self._session.get(ChatThread, message.thread_id)
        workspace_id = thread.workspace_id if thread else None
        if workspace_id is None:
            raise EvalCaseValidationError("source chat message has no workspace")

        case = await self.create(
            EvalCaseInput(
                workspace_id=workspace_id,
                question=question,
                expected_answer=expected_answer,
                expected_sql=expected_sql,
                expected_connector_slug=expected_connector_slug,
                expected_tool=expected_tool,
                expected_citations=expected_citations,
                tags=tags,
                origin="feedback",
                source_chat_message_id=chat_message_id,
                status="candidate",
            ),
            created_by=created_by,
        )
        # Stamp the most recent negative feedback row for this message with
        # the eval_case id so the UI can highlight "correction captured".
        latest_neg = await self._session.scalar(
            select(Feedback)
            .where(
                Feedback.chat_message_id == chat_message_id,
                Feedback.sentiment == "negative",
            )
            .order_by(Feedback.created_at.desc())
            .limit(1)
        )
        if latest_neg is not None:
            latest_neg.eval_case_id = case.id
        return case

    # --- read ---

    async def get(self, case_id: str) -> EvalCase:
        case = await self._session.get(EvalCase, case_id)
        if case is None:
            raise EvalCaseNotFound(case_id)
        return case

    async def list(
        self,
        *,
        workspace_id: str,
        status: str | None = None,
        origin: str | None = None,
        tag: str | None = None,
        q: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[EvalCase]:
        stmt = select(EvalCase).where(EvalCase.workspace_id == workspace_id)
        if status:
            _validate_status(status)
            stmt = stmt.where(EvalCase.status == status)
        if origin:
            _validate_origin(origin)
            stmt = stmt.where(EvalCase.origin == origin)
        if q:
            term = f"%{q.lower()}%"
            stmt = stmt.where(
                or_(
                    EvalCase.question.ilike(term),
                    EvalCase.expected_answer.ilike(term),
                    EvalCase.expected_sql.ilike(term),
                )
            )
        stmt = stmt.order_by(EvalCase.updated_at.desc()).offset(offset).limit(limit)
        rows = list((await self._session.scalars(stmt)).all())
        if tag:
            rows = [row for row in rows if tag in (row.tags or [])]
        return rows

    # --- update ---

    async def update(self, case_id: str, patch: EvalCasePatch) -> EvalCase:
        case = await self.get(case_id)
        if case.status == "archived":
            raise EvalCaseTransitionError("archived cases cannot be edited; unarchive first")
        if patch.question is not None:
            value = patch.question.strip()
            if not value:
                raise EvalCaseValidationError("question cannot be empty")
            case.question = value
        if patch.expected_answer is not None:
            case.expected_answer = _clean(patch.expected_answer)
        if patch.expected_sql is not None:
            case.expected_sql = _clean(patch.expected_sql)
        if patch.expected_connector_slug is not None:
            case.expected_connector_slug = _clean(patch.expected_connector_slug)
        if patch.expected_tool is not None:
            case.expected_tool = _clean(patch.expected_tool)
        if patch.expected_citations is not None:
            case.expected_citations = patch.expected_citations
        if patch.tags is not None:
            case.tags = patch.tags
        await self._session.flush()
        return case

    # --- lifecycle ---

    async def approve(self, case_id: str) -> EvalCase:
        case = await self.get(case_id)
        if case.status == "approved":
            return case
        if case.status != "candidate":
            raise EvalCaseTransitionError(
                f"cannot approve a case in status '{case.status}'"
            )
        case.status = "approved"
        await self._session.flush()
        return case

    async def promote_golden(self, case_id: str) -> EvalCase:
        """Promote an approved case to golden. Only approved cases qualify —
        candidates must be reviewed/approved first. ``expected_sql`` is
        required to be golden (we look up by SQL substitution at chat time)."""
        case = await self.get(case_id)
        if case.status == "golden":
            return case
        if case.status != "approved":
            raise EvalCaseTransitionError(
                "only approved cases can be promoted to golden; approve it first"
            )
        if not case.expected_sql:
            raise EvalCaseValidationError(
                "golden promotion requires expected_sql to be set"
            )
        case.status = "golden"
        await self._session.flush()
        return case

    async def archive(self, case_id: str) -> EvalCase:
        case = await self.get(case_id)
        if case.status == "archived":
            return case
        case.status = "archived"
        await self._session.flush()
        return case

    async def unarchive(self, case_id: str) -> EvalCase:
        case = await self.get(case_id)
        if case.status != "archived":
            raise EvalCaseTransitionError("only archived cases can be unarchived")
        case.status = "candidate"
        await self._session.flush()
        return case

    # --- golden lookup (used by chat.py before generating fresh SQL) ---

    async def find_golden_for_question(
        self,
        *,
        workspace_id: str,
        question: str,
        connector_slug: str | None = None,
    ) -> EvalCase | None:
        """Return a golden case whose normalized question matches the given
        one. Phase 2 uses normalize+exact-match (case-insensitive, whitespace-
        collapsed, punctuation-stripped). Phase 3 swaps in vector similarity
        once we wire the Chroma collection."""
        normalized = _normalize(question)
        if not normalized:
            return None
        stmt = (
            select(EvalCase)
            .where(
                EvalCase.workspace_id == workspace_id,
                EvalCase.status == "golden",
            )
            .order_by(EvalCase.updated_at.desc())
        )
        if connector_slug:
            stmt = stmt.where(
                or_(
                    EvalCase.expected_connector_slug == connector_slug,
                    EvalCase.expected_connector_slug.is_(None),
                )
            )
        rows = list((await self._session.scalars(stmt)).all())
        for row in rows:
            if _normalize(row.question) == normalized:
                return row
        return None

    # --- internal ---

    async def _previous_user_question(self, assistant_msg: ChatMessage) -> str | None:
        prior_user = await self._session.scalar(
            select(ChatMessage)
            .where(
                ChatMessage.thread_id == assistant_msg.thread_id,
                ChatMessage.role == "user",
                ChatMessage.created_at <= assistant_msg.created_at,
            )
            .order_by(ChatMessage.created_at.desc())
            .limit(1)
        )
        return prior_user.content if prior_user else None


# ---------- helpers ----------


def _validate_status(status: str) -> None:
    if status not in CASE_STATUSES:
        raise EvalCaseValidationError(
            f"status must be one of {CASE_STATUSES}, got {status!r}"
        )


def _validate_origin(origin: str) -> None:
    if origin not in CASE_ORIGINS:
        raise EvalCaseValidationError(
            f"origin must be one of {CASE_ORIGINS}, got {origin!r}"
        )


def _clean(value: str | None) -> str | None:
    if value is None:
        return None
    stripped = value.strip()
    return stripped or None


_PUNCT_RE = re.compile(r"[^a-z0-9\s]")
_WS_RE = re.compile(r"\s+")


def _normalize(value: str | None) -> str:
    if not value:
        return ""
    lowered = value.lower()
    cleaned = _PUNCT_RE.sub(" ", lowered)
    return _WS_RE.sub(" ", cleaned).strip()


# Export so api/evals.py can use the helper for span metadata too.
normalize_question = _normalize


def _safe_jsonable(value: Any) -> Any:  # pragma: no cover - kept for symmetry
    return value
