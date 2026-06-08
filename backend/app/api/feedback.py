"""Feedback API.

POST /feedback        - submit 👍 / 👎 on an assistant message
GET  /feedback/{id}   - look up a stored feedback row (debug / Phase-2 wiring)
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import current_user
from app.db.session import get_session
from app.models.domain import Feedback, User
from app.services.observability.feedback import (
    FeedbackService,
    FeedbackTargetNotFound,
    FeedbackValidationError,
)

router = APIRouter(prefix="/feedback", tags=["feedback"])


class FeedbackCreateRequest(BaseModel):
    chat_message_id: str
    sentiment: str  # "positive" | "negative"
    comment: str | None = None


class FeedbackRecord(BaseModel):
    id: str
    chat_message_id: str
    sentiment: str
    comment: str | None
    user_id: str | None
    langfuse_score_id: str | None
    eval_case_id: str | None

    @classmethod
    def from_row(cls, row: Feedback) -> FeedbackRecord:
        return cls(
            id=row.id,
            chat_message_id=row.chat_message_id,
            sentiment=row.sentiment,
            comment=row.comment,
            user_id=row.user_id,
            langfuse_score_id=row.langfuse_score_id,
            eval_case_id=row.eval_case_id,
        )


@router.post("", response_model=FeedbackRecord)
async def submit_feedback(
    payload: FeedbackCreateRequest,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
) -> FeedbackRecord:
    service = FeedbackService(session)
    try:
        row = await service.submit(
            chat_message_id=payload.chat_message_id,
            sentiment=payload.sentiment,
            user=user,
            comment=payload.comment,
        )
    except FeedbackValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except FeedbackTargetNotFound as exc:
        raise HTTPException(status_code=404, detail=f"Chat message {exc} not found.") from exc
    return FeedbackRecord.from_row(row)


@router.get("/{feedback_id}", response_model=FeedbackRecord)
async def get_feedback(
    feedback_id: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> FeedbackRecord:
    row = await session.get(Feedback, feedback_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Feedback not found.")
    return FeedbackRecord.from_row(row)
