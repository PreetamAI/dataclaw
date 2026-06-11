"""ChatHistoryProducer — turn past good chat turns into regression eval cases.

Source of signal: assistant ChatMessage rows that
  * sit in a *user* thread (not the hidden eval/scheduled threads — those
    are by definition self-referential),
  * carry an actual SQL body (so there's something deterministic to assert
    against in the eval runner),
  * have at least one positive feedback row AND zero negative feedback rows
    (a single 👎 disqualifies the turn — we only mint regression cases from
    answers a user has explicitly endorsed),
  * have an immediately-preceding user message in the same thread that
    provides the question.

Each surviving turn becomes one CandidateCase whose expected_* fields are
populated from the historical assistant message + the user's original
question. The eval runner re-asks the same question; metrics compare the
new SQL / connector / citations / row count back to the captured baseline.

Why this matters: the schema/kg/lineage producers cover the breadth of
*workspace state* but they don't know what *users actually ask*. A
production regression where the chat agent suddenly stops using the right
connector for "give me last week's revenue" won't be caught by any
template-based producer — but it will be caught by a chat-history case
that captured the original good answer.
"""

from __future__ import annotations

from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.domain import ChatMessage, ChatThread, Feedback
from app.services.evals.generators.base import CandidateCase


# Skip turns longer than this when reconstructing the question; the chat
# normalize step caps at ~512 chars anyway and a multi-paragraph "question"
# is almost always a paste, not a real ask.
MAX_QUESTION_CHARS = 500


class ChatHistoryProducer:
    slug = "chat_history"
    origin = "auto:chat_history"
    display_name = "Chat history"

    async def generate(
        self,
        session: AsyncSession,
        *,
        workspace_id: str,
        limit: int,
    ) -> list[CandidateCase]:
        # Pull all user-thread messages in chronological order, keyed by thread.
        thread_ids = list(
            (
                await session.scalars(
                    select(ChatThread.id).where(
                        ChatThread.workspace_id == workspace_id,
                        ChatThread.kind == "user",
                        ChatThread.archived.is_(False),
                    )
                )
            ).all()
        )
        if not thread_ids:
            return []

        messages = list(
            (
                await session.scalars(
                    select(ChatMessage)
                    .where(ChatMessage.thread_id.in_(thread_ids))
                    .order_by(ChatMessage.thread_id, ChatMessage.created_at)
                )
            ).all()
        )
        if not messages:
            return []

        # Group user-question -> assistant-answer pairs.
        by_thread: dict[str, list[ChatMessage]] = defaultdict(list)
        for m in messages:
            by_thread[m.thread_id].append(m)

        # Collect assistant message ids so we can load feedback in one trip.
        assistant_ids = [
            m.id
            for thread_msgs in by_thread.values()
            for m in thread_msgs
            if m.role == "assistant" and m.sql
        ]
        if not assistant_ids:
            return []

        feedback_rows = list(
            (
                await session.scalars(
                    select(Feedback).where(Feedback.chat_message_id.in_(assistant_ids))
                )
            ).all()
        )
        pos: dict[str, int] = defaultdict(int)
        neg: dict[str, int] = defaultdict(int)
        for fb in feedback_rows:
            if fb.sentiment == "positive":
                pos[fb.chat_message_id] += 1
            elif fb.sentiment == "negative":
                neg[fb.chat_message_id] += 1

        candidates: list[CandidateCase] = []

        # Newest endorsed turns first so the most-relevant questions win
        # under the per-producer cap.
        assistant_msgs = sorted(
            (m for m in messages if m.role == "assistant" and m.sql),
            key=lambda m: m.created_at,
            reverse=True,
        )
        for assistant in assistant_msgs:
            if len(candidates) >= limit:
                break
            if pos.get(assistant.id, 0) == 0:
                continue  # no thumbs-up = not endorsed
            if neg.get(assistant.id, 0) > 0:
                continue  # a single thumbs-down disqualifies

            question = _preceding_user_question(by_thread[assistant.thread_id], assistant)
            if not question:
                continue
            if len(question) > MAX_QUESTION_CHARS:
                continue

            connector_slug = _connector_from_citations(assistant.citations) or None
            tool = _tool_from_connector(connector_slug)
            expected_answer = assistant.content.strip() or None
            # Strip the rows preview down to a tiny sample — the runner uses
            # the expected_result_preview for accuracy comparisons; capturing
            # the full row set would bloat the eval_cases table for no gain.
            preview = list((assistant.rows or [])[:5])

            candidates.append(
                CandidateCase(
                    question=question,
                    origin=self.origin,
                    expected_answer=expected_answer,
                    expected_sql=assistant.sql,
                    expected_connector_slug=connector_slug,
                    expected_tool=tool,
                    expected_citations=list(assistant.citations or []),
                    tags=_derive_tags(assistant, preview),
                    confidence=0.95,
                )
            )

        return candidates


# ---------- helpers ----------


def _preceding_user_question(
    thread_msgs: list[ChatMessage], assistant: ChatMessage
) -> str | None:
    """Find the nearest preceding user message in the same thread."""
    last_user: str | None = None
    for m in thread_msgs:
        if m.id == assistant.id:
            return last_user
        if m.role == "user" and m.content and m.content.strip():
            last_user = m.content.strip()
    return None


def _connector_from_citations(citations: list[dict] | None) -> str | None:
    """Pull the first connector slug we can find in the citation list."""
    for c in citations or []:
        if not isinstance(c, dict):
            continue
        slug = c.get("source") or c.get("connector_slug")
        if isinstance(slug, str) and slug:
            return slug
    return None


def _tool_from_connector(connector_slug: str | None) -> str | None:
    """Best-effort mapping. The runner does not require this — it's a hint
    for the connector_accuracy metric. If we don't know, leave it blank."""
    if not connector_slug:
        return None
    # SQL connectors expose read_select; everything else is opaque enough
    # that guessing the tool name will give a false signal.
    sql_conns = {
        "postgres",
        "mysql",
        "redshift",
        "sql_server",
        "databricks",
        "bigquery",
        "snowflake",
        "trino",
        "sqlite",
    }
    if connector_slug in sql_conns:
        return f"{connector_slug}.read_select"
    return None


def _derive_tags(assistant: ChatMessage, preview: list[dict]) -> list[str]:
    tags = ["chat_history", "endorsed"]
    if preview:
        tags.append("with_rows")
    sql = (assistant.sql or "").lstrip().lower()
    if sql.startswith("select count("):
        tags.append("count")
    elif " join " in f" {sql} ":
        tags.append("join")
    elif " group by " in f" {sql} ":
        tags.append("aggregate")
    return tags
