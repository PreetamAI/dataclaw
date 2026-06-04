"""Feedback domain service.

Records 👍 / 👎 on assistant chat messages. The feedback row is the
canonical artifact (always persisted); forwarding to Langfuse is a
best-effort follow-up if the workspace has configured it.

Phase 1 scope: capture only. Phase 2 wires the optional correction payload
into ``eval_cases`` via ``services/evals/cases.py``.
"""

from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from app.models.domain import ChatMessage, Feedback, User
from app.services.observability.langfuse_client import resolve_sink
from app.services.settings_store import get_observability_provider

logger = logging.getLogger(__name__)


VALID_SENTIMENTS = frozenset({"positive", "negative"})


class FeedbackValidationError(ValueError):
    """Raised when an incoming feedback payload is malformed. Surface as 400."""


class FeedbackTargetNotFound(LookupError):
    """Raised when the targeted chat_message does not exist. Surface as 404."""


class FeedbackService:
    def __init__(self, session: AsyncSession):
        self._session = session

    async def submit(
        self,
        *,
        chat_message_id: str,
        sentiment: str,
        user: User | None = None,
        comment: str | None = None,
    ) -> Feedback:
        if sentiment not in VALID_SENTIMENTS:
            raise FeedbackValidationError(
                f"sentiment must be one of {sorted(VALID_SENTIMENTS)}"
            )
        message = await self._session.get(ChatMessage, chat_message_id)
        if message is None:
            raise FeedbackTargetNotFound(chat_message_id)
        if message.role != "assistant":
            # Feedback only makes sense on assistant turns; user/system messages
            # are inputs to the system, not outputs of it.
            raise FeedbackValidationError(
                "feedback can only be attached to assistant messages"
            )

        row = Feedback(
            chat_message_id=chat_message_id,
            user_id=user.id if user else None,
            sentiment=sentiment,
            comment=(comment or None),
        )
        self._session.add(row)
        await self._session.flush()

        # Forward to Langfuse if configured and the message has a trace_id.
        # Done synchronously inside the request — the Langfuse SDK queues
        # internally so this returns quickly.
        if message.trace_id:
            score_id = await self._forward_score(
                trace_id=message.trace_id,
                sentiment=sentiment,
                comment=comment,
            )
            if score_id:
                row.langfuse_score_id = score_id

        await self._session.commit()
        await self._session.refresh(row)
        return row

    async def _forward_score(
        self,
        *,
        trace_id: str,
        sentiment: str,
        comment: str | None,
    ) -> str | None:
        try:
            payload = await get_observability_provider(self._session, "langfuse")
        except Exception:
            logger.debug("feedback_settings_read_failed", exc_info=True)
            return None
        sink = resolve_sink(payload)
        if sink is None or not sink.is_available():
            return None
        # Langfuse scores are floats; we use the convention value=1 for 👍
        # and value=0 for 👎 under the well-known "user_feedback" name. Eval
        # metrics later use distinct score names so they don't collide.
        value = 1.0 if sentiment == "positive" else 0.0
        return sink.score_trace(
            trace_id=trace_id,
            name="user_feedback",
            value=value,
            comment=comment,
        )
