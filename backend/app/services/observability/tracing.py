"""Chat tracing service.

Public surface
--------------
* ``chat_trace(...)``      - async ctx manager: opens a root trace for a chat
                             turn. Yields a ``ChatTrace`` handle whose
                             ``trace_id`` can be persisted on ChatMessage.
* ``span(kind, name, ...)``- async ctx manager: opens a child span under the
                             current trace. Safe to call when no trace is
                             active (becomes a no-op).
* ``record_usage(...)``    - attach token/cost metrics to the active LLM span.

Design
------
* Always-on local sink writes to ``chat_spans``. Langfuse is an additional
  forwarding sink, resolved per call via :mod:`langfuse_client` (cached).
* State is held in a ContextVar so spans nest correctly across awaits without
  threading state through every internal function signature.
* All sink calls are best-effort. Span exit catches and logs any sink error;
  the wrapped business logic always sees its own exceptions, never the
  sink's.
* DB writes use a *dedicated* AsyncSession per trace so we don't entangle
  span persistence with the request session's transaction lifecycle. If the
  request rolls back, spans are still recorded.
"""

from __future__ import annotations

import contextvars
import logging
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any
from uuid import uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from app.db.base import new_id
from app.models.domain import ChatSpan
from app.services.observability.langfuse_client import (
    LangfuseSink,
    _SpanHandle,
    resolve_sink,
)
from app.services.settings_store import get_observability_provider

logger = logging.getLogger(__name__)


# ---------- public dataclasses ----------


@dataclass
class _SpanRecord:
    id: str
    parent_id: str | None
    kind: str
    name: str
    started_at: datetime
    ended_at: datetime | None = None
    status: str = "ok"
    error: str | None = None
    input: dict[str, Any] = field(default_factory=dict)
    output: dict[str, Any] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)
    usage: dict[str, Any] = field(default_factory=dict)
    model: str | None = None
    # Live Langfuse span handle opened at span-start and closed at span-end.
    # Carries the real SDK wrapper plus a stable observation_id used by
    # child spans for parent_observation_id linking.
    langfuse_handle: _SpanHandle | None = None


@dataclass
class _TraceState:
    trace_id: str
    chat_message_id: str
    workspace_id: str | None
    user_id: str | None
    thread_id: str | None
    langfuse: LangfuseSink | None
    root_span_id: str
    span_stack: list[_SpanRecord] = field(default_factory=list)
    completed: list[_SpanRecord] = field(default_factory=list)


# ContextVar scoped to the current async task chain.
_current: contextvars.ContextVar[_TraceState | None] = contextvars.ContextVar(
    "dataclaw_chat_trace", default=None
)


# Langfuse project IDs are CUIDs: 'cm' + 22-24 lowercase alphanumerics.
# A pasted display name (e.g. "Dataclaw-langfuse") makes /project/<name>/
# URLs 404 on Cloud, so we only emit a link when the field looks like an id.
_LANGFUSE_PROJECT_ID_RE = re.compile(r"^cm[0-9a-z]{20,}$")


def _coerce_project_id(value: str | None) -> str | None:
    if not value:
        return None
    candidate = value.strip()
    if _LANGFUSE_PROJECT_ID_RE.match(candidate):
        return candidate
    return None


class ChatTrace:
    """Handle returned by ``chat_trace()``. Exposes the deterministic
    trace_id and Langfuse URL (if any) for cross-linking on the wire."""

    def __init__(self, state: _TraceState):
        self._state = state

    @property
    def trace_id(self) -> str:
        return self._state.trace_id

    @property
    def chat_message_id(self) -> str:
        return self._state.chat_message_id

    @property
    def langfuse_url(self) -> str | None:
        ls = self._state.langfuse
        if ls is None:
            return None
        host = ls.config.host.rstrip("/")
        project_id = _coerce_project_id(ls.config.project)
        if project_id is None:
            return None
        return f"{host}/project/{project_id}/traces/{self._state.trace_id}"


# ---------- resolution helpers ----------


async def _resolve_langfuse(session: AsyncSession) -> LangfuseSink | None:
    try:
        payload = await get_observability_provider(session, "langfuse")
    except Exception:
        logger.debug("langfuse_settings_read_failed", exc_info=True)
        return None
    sink = resolve_sink(payload)
    if sink is None or not sink.is_available():
        return None
    return sink


# ---------- root trace ----------


@asynccontextmanager
async def chat_trace(
    *,
    session: AsyncSession,
    chat_message_id: str,
    workspace_id: str | None = None,
    user_id: str | None = None,
    thread_id: str | None = None,
    question: str | None = None,
) -> AsyncIterator[ChatTrace]:
    """Open a root chat trace. ``chat_message_id`` should be the pre-allocated
    UUID we will later assign to the assistant ChatMessage row — this keeps
    the trace_id deterministic across the response/feedback round trip."""
    langfuse = await _resolve_langfuse(session)

    trace_id = langfuse.create_trace_id(seed=chat_message_id) if langfuse else None
    if not trace_id:
        # Fall back to the message id itself for the local sink. We hex-pack so
        # the column width is comfortable even if uuid prefixes are added later.
        trace_id = chat_message_id.replace("-", "")

    root_id = new_id()
    started_at = datetime.now(UTC)
    root = _SpanRecord(
        id=root_id,
        parent_id=None,
        kind="root",
        name="chat_turn",
        started_at=started_at,
        input={"question": question} if question is not None else {},
        metadata={
            "workspace_id": workspace_id,
            "user_id": user_id,
            "thread_id": thread_id,
        },
    )
    state = _TraceState(
        trace_id=trace_id,
        chat_message_id=chat_message_id,
        workspace_id=workspace_id,
        user_id=user_id,
        thread_id=thread_id,
        langfuse=langfuse,
        root_span_id=root_id,
        span_stack=[root],
    )
    token = _current.set(state)
    handle = ChatTrace(state)
    # Open the live Langfuse root span at trace start so child spans get a
    # real parent_observation_id and the trace shows correct wall-clock
    # timing in the Langfuse UI.
    if langfuse is not None:
        root.langfuse_handle = langfuse.begin_span(
            trace_id=trace_id,
            parent_observation_id=None,
            as_type="span",
            name="chat_turn",
            input=root.input,
            metadata=root.metadata,
            model=None,
        )
    try:
        yield handle
    except Exception as exc:
        root.status = "error"
        root.error = f"{exc.__class__.__name__}: {exc}"
        raise
    finally:
        root.ended_at = datetime.now(UTC)
        # Close the live Langfuse root span FIRST so the SDK has correct
        # wall-clock timing for the parent before any flush.
        if langfuse is not None and root.langfuse_handle is not None:
            try:
                langfuse.end_span(
                    root.langfuse_handle,
                    output={"status": root.status},
                    usage=None,
                    status=root.status,
                    error=root.error,
                )
            except Exception:
                logger.debug("langfuse_root_end_failed", exc_info=True)
        state.completed.append(root)
        state.span_stack.clear()
        _current.reset(token)
        # Persist spans to the local sink. Use a fresh session so a failing
        # request transaction doesn't lose the trace.
        try:
            await _flush_spans(state)
        except Exception:
            logger.exception("chat_trace_flush_failed", extra={"trace_id": trace_id})
        # Flush the Langfuse SDK's background queue so the trace lands
        # promptly (the SDK auto-flushes on shutdown too, but flushing
        # here makes the manual test guide deterministic).
        if langfuse is not None:
            try:
                langfuse.flush()
            except Exception:
                logger.debug("langfuse_flush_failed", exc_info=True)


# ---------- child spans ----------


@asynccontextmanager
async def span(
    kind: str,
    name: str,
    *,
    input: Any = None,
    metadata: dict[str, Any] | None = None,
    model: str | None = None,
) -> AsyncIterator[SpanHandle]:
    """Open a child span. If no chat_trace is active (e.g. called from a
    background path that isn't traced yet), this is a no-op handle."""
    state = _current.get()
    if state is None:
        yield _NoopSpan()
        return
    parent = state.span_stack[-1] if state.span_stack else None
    record = _SpanRecord(
        id=new_id(),
        parent_id=parent.id if parent else None,
        kind=kind,
        name=name,
        started_at=datetime.now(UTC),
        input=_safe_jsonable(input),
        metadata=metadata or {},
        model=model,
    )
    state.span_stack.append(record)
    handle = SpanHandle(record)
    # Open the live Langfuse span NOW so its start_time reflects wall clock.
    # Parent linkage comes from whichever span is currently on top (which is
    # the one we just pushed onto — so look up the prior frame).
    if state.langfuse is not None:
        parent_obs_id = None
        # The parent is the second-from-top after we pushed our own record.
        if len(state.span_stack) >= 2:
            parent_record = state.span_stack[-2]
            parent_obs_id = (
                parent_record.langfuse_handle.observation_id
                if parent_record.langfuse_handle
                else None
            )
        record.langfuse_handle = state.langfuse.begin_span(
            trace_id=state.trace_id,
            parent_observation_id=parent_obs_id,
            as_type="generation" if kind == "llm" else "span",
            name=name,
            input=record.input,
            metadata=record.metadata,
            model=model,
        )
    try:
        yield handle
    except Exception as exc:
        record.status = "error"
        record.error = f"{exc.__class__.__name__}: {exc}"
        raise
    finally:
        record.ended_at = datetime.now(UTC)
        # Detach from the stack only if still on top (defensive against
        # mis-nested usage from caller exceptions).
        if state.span_stack and state.span_stack[-1] is record:
            state.span_stack.pop()
        state.completed.append(record)
        # Close the live Langfuse span with output/usage captured during the
        # body. Safe to call even when the SDK is absent — the no-op handle
        # makes this a cheap noop.
        if state.langfuse is not None and record.langfuse_handle is not None:
            try:
                state.langfuse.end_span(
                    record.langfuse_handle,
                    output=record.output,
                    usage=record.usage or None,
                    status=record.status,
                    error=record.error,
                )
            except Exception:
                logger.debug("langfuse_span_end_failed", exc_info=True)


class SpanHandle:
    """Mutable handle the caller uses to attach output/usage to the span."""

    def __init__(self, record: _SpanRecord):
        self._record = record

    def set_output(self, output: Any) -> None:
        self._record.output = _safe_jsonable(output)

    def set_metadata(self, **kwargs: Any) -> None:
        self._record.metadata.update(kwargs)

    def set_usage(
        self,
        *,
        prompt_tokens: int | None = None,
        completion_tokens: int | None = None,
        total_tokens: int | None = None,
        cost_usd: float | None = None,
    ) -> None:
        usage: dict[str, Any] = {}
        if prompt_tokens is not None:
            usage["input_tokens"] = prompt_tokens
        if completion_tokens is not None:
            usage["output_tokens"] = completion_tokens
        if total_tokens is not None:
            usage["total_tokens"] = total_tokens
        if cost_usd is not None:
            usage["cost_usd"] = cost_usd
        if usage:
            self._record.usage.update(usage)

    def set_model(self, model: str | None) -> None:
        if model:
            self._record.model = model


class _NoopSpan(SpanHandle):
    def __init__(self):
        pass

    def set_output(self, output: Any) -> None:
        return

    def set_metadata(self, **kwargs: Any) -> None:
        return

    def set_usage(self, **kwargs: Any) -> None:
        return

    def set_model(self, model: str | None) -> None:
        return


# ---------- module-level helpers ----------


def current_trace_id() -> str | None:
    state = _current.get()
    return state.trace_id if state else None


def current_chat_message_id() -> str | None:
    state = _current.get()
    return state.chat_message_id if state else None


def current_langfuse_url() -> str | None:
    state = _current.get()
    if state is None or state.langfuse is None:
        return None
    return ChatTrace(state).langfuse_url


# ---------- internal: persistence ----------


async def _flush_spans(state: _TraceState) -> None:
    if not state.completed:
        return
    # Look up SessionLocal lazily so test reloads of app.db.session are
    # honoured (a captured module-level import would freeze the old engine).
    from app.db import session as _session_module

    async with _session_module.SessionLocal() as bg_session:
        for record in state.completed:
            bg_session.add(
                ChatSpan(
                    id=record.id,
                    chat_message_id=state.chat_message_id,
                    parent_span_id=record.parent_id,
                    kind=record.kind,
                    name=record.name,
                    status=record.status,
                    error=record.error,
                    input=record.input,
                    output=record.output,
                    span_metadata=record.metadata,
                    usage=record.usage,
                    model=record.model,
                    latency_ms=_latency_ms(record.started_at, record.ended_at),
                    started_at=record.started_at,
                    ended_at=record.ended_at or record.started_at,
                )
            )
        await bg_session.commit()


# ---------- internal: small utils ----------


def _latency_ms(start: datetime, end: datetime | None) -> int:
    if end is None:
        return 0
    return max(0, int((end - start).total_seconds() * 1000))


def _safe_jsonable(value: Any) -> dict[str, Any]:
    """Coerce arbitrary input/output to a JSON-safe dict so we never crash
    span persistence on an exotic type. Truncate strings to keep row size
    bounded — full payloads belong in Langfuse, not the local DB."""
    MAX_STR = 8000
    if value is None:
        return {}
    if isinstance(value, dict):
        return {str(k): _shrink(v, MAX_STR) for k, v in value.items()}
    return {"value": _shrink(value, MAX_STR)}


def _shrink(value: Any, max_str: int) -> Any:
    if isinstance(value, str):
        return value if len(value) <= max_str else value[:max_str] + "...[truncated]"
    if isinstance(value, list | tuple):
        return [_shrink(v, max_str) for v in list(value)[:200]]
    if isinstance(value, dict):
        return {str(k): _shrink(v, max_str) for k, v in value.items()}
    if isinstance(value, int | float | bool):
        return value
    return str(value)[:max_str]


# Backwards-compatible token used to seed a synthetic chat_message_id when the
# caller is about to allocate the message row (e.g. /ide/chat creates one upfront
# so the trace_id remains stable through the response cycle).
def allocate_chat_message_id() -> str:
    return str(uuid4())
