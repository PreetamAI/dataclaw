"""Chat trace API.

GET /chat-messages/{id}/trace      - return the local span tree for a chat turn
GET /chat-messages/{id}/trace-link - return a Langfuse UI URL if configured

The local span tree is always present (since Phase 1 always writes spans
to the local sink). The Langfuse link is populated only when the workspace
has enabled the Langfuse integration.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import current_user
from app.db.session import get_session
from app.models.domain import ChatMessage, ChatSpan, Feedback, User
from app.services.observability.langfuse_client import resolve_sink
from app.services.settings_store import get_observability_provider

router = APIRouter(prefix="/chat-messages", tags=["chat-traces"])


class SpanView(BaseModel):
    id: str
    parent_span_id: str | None
    kind: str
    name: str
    status: str
    error: str | None
    input: dict = Field(default_factory=dict)
    output: dict = Field(default_factory=dict)
    metadata: dict = Field(default_factory=dict)
    usage: dict = Field(default_factory=dict)
    model: str | None
    latency_ms: int
    started_at: datetime
    ended_at: datetime


class TraceView(BaseModel):
    chat_message_id: str
    trace_id: str | None
    spans: list[SpanView]


class TraceLink(BaseModel):
    chat_message_id: str
    trace_id: str | None
    langfuse_url: str | None


class MessageFeedbackView(BaseModel):
    chat_message_id: str
    sentiment: str | None  # "positive" | "negative" | None when no feedback yet
    feedback_id: str | None
    comment: str | None
    eval_case_id: str | None


@router.get("/{message_id}/trace", response_model=TraceView)
async def get_message_trace(
    message_id: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> TraceView:
    message = await session.get(ChatMessage, message_id)
    if message is None:
        raise HTTPException(status_code=404, detail="Chat message not found.")
    rows = list(
        (
            await session.scalars(
                select(ChatSpan)
                .where(ChatSpan.chat_message_id == message_id)
                .order_by(ChatSpan.started_at.asc())
            )
        ).all()
    )
    return TraceView(
        chat_message_id=message_id,
        trace_id=message.trace_id,
        spans=[
            SpanView(
                id=row.id,
                parent_span_id=row.parent_span_id,
                kind=row.kind,
                name=row.name,
                status=row.status,
                error=row.error,
                input=row.input or {},
                output=row.output or {},
                metadata=row.span_metadata or {},
                usage=row.usage or {},
                model=row.model,
                latency_ms=row.latency_ms,
                started_at=row.started_at,
                ended_at=row.ended_at,
            )
            for row in rows
        ],
    )


@router.get("/{message_id}/trace-link", response_model=TraceLink)
async def get_message_trace_link(
    message_id: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> TraceLink:
    message = await session.get(ChatMessage, message_id)
    if message is None:
        raise HTTPException(status_code=404, detail="Chat message not found.")
    if not message.trace_id:
        return TraceLink(chat_message_id=message_id, trace_id=None, langfuse_url=None)
    payload = await get_observability_provider(session, "langfuse")
    sink = resolve_sink(payload)
    url: str | None = None
    if sink is not None:
        host = sink.config.host.rstrip("/")
        project = sink.config.project
        url = (
            f"{host}/project/{project}/traces/{message.trace_id}"
            if project
            else f"{host}/traces/{message.trace_id}"
        )
    return TraceLink(chat_message_id=message_id, trace_id=message.trace_id, langfuse_url=url)


@router.get("/{message_id}/feedback", response_model=MessageFeedbackView)
async def get_message_feedback(
    message_id: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> MessageFeedbackView:
    """Latest feedback for the given chat message, or null sentiment if none.
    Used by the FeedbackBar UI to restore the 👍/👎 selection after refresh
    or when switching between chat sessions."""
    message = await session.get(ChatMessage, message_id)
    if message is None:
        raise HTTPException(status_code=404, detail="Chat message not found.")
    row = await session.scalar(
        select(Feedback)
        .where(Feedback.chat_message_id == message_id)
        .order_by(Feedback.created_at.desc())
        .limit(1)
    )
    if row is None:
        return MessageFeedbackView(
            chat_message_id=message_id,
            sentiment=None,
            feedback_id=None,
            comment=None,
            eval_case_id=None,
        )
    return MessageFeedbackView(
        chat_message_id=message_id,
        sentiment=row.sentiment,
        feedback_id=row.id,
        comment=row.comment,
        eval_case_id=row.eval_case_id,
    )
