"""Coordinator: runs producers, deduplicates, applies caps, persists.

The dedupe key is (workspace_id, normalized_question) — same normalization
the chat-side golden lookup uses, so an auto-generated candidate that
matches an existing case (any status) is rejected. That keeps the candidate
queue clean as users approve and promote cases over time.

Caps (locked decisions from the plan, not overridable in v1):
* limit_per_source: default 50
* workspace candidate ceiling: 500 active (status=candidate) rows. New
  generation runs are short-circuited when the workspace already has 500
  candidates pending review — the user has to clear the queue first.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.domain import EvalCase, User
from app.services.evals.cases import (
    CASE_ORIGINS,
    EvalCaseInput,
    EvalCaseService,
    normalize_question,
)
from app.services.evals.generators.base import Producer, ProducerError

logger = logging.getLogger(__name__)


DEFAULT_LIMIT_PER_SOURCE = 50
WORKSPACE_CANDIDATE_CEILING = 500


@dataclass
class ProducerResult:
    slug: str
    display_name: str
    produced: int  # what the producer returned
    inserted: int  # what we actually persisted (post dedupe + cap)
    skipped_duplicate: int = 0
    skipped_cap: int = 0
    error: str | None = None


@dataclass
class GenerationResult:
    """Aggregate outcome of a generate-candidates invocation."""

    workspace_id: str
    inserted_ids: list[str] = field(default_factory=list)
    producers: list[ProducerResult] = field(default_factory=list)
    candidate_ceiling: int = WORKSPACE_CANDIDATE_CEILING
    candidates_in_queue_before: int = 0
    candidates_in_queue_after: int = 0
    ceiling_reached: bool = False


async def run_generators(
    session: AsyncSession,
    *,
    producers: list[Producer],
    workspace_id: str,
    limit_per_source: int = DEFAULT_LIMIT_PER_SOURCE,
    created_by: User | None = None,
    sources: list[str] | None = None,
) -> GenerationResult:
    """Run each producer in sequence, persist via EvalCaseService, return a
    detailed audit so the UI can show "X inserted, Y duplicate, Z capped".

    `sources` filters which producer slugs run; ``None`` runs all.
    """
    selected = (
        [p for p in producers if p.slug in set(sources)] if sources else list(producers)
    )

    result = GenerationResult(workspace_id=workspace_id)
    if not selected:
        return result

    # Pre-load existing normalized questions for cheap dedupe. Includes
    # archived cases — we don't want to keep regenerating something a user
    # explicitly archived.
    existing_rows = list(
        (
            await session.scalars(
                select(EvalCase.question).where(EvalCase.workspace_id == workspace_id)
            )
        ).all()
    )
    seen_normalized: set[str] = {normalize_question(q) for q in existing_rows}

    candidates_before = await session.scalar(
        select(func.count(EvalCase.id)).where(
            EvalCase.workspace_id == workspace_id,
            EvalCase.status == "candidate",
        )
    ) or 0
    result.candidates_in_queue_before = int(candidates_before)
    remaining_capacity = max(0, WORKSPACE_CANDIDATE_CEILING - int(candidates_before))
    if remaining_capacity == 0:
        result.ceiling_reached = True
        result.candidates_in_queue_after = int(candidates_before)
        return result

    service = EvalCaseService(session)

    for producer in selected:
        per_result = ProducerResult(
            slug=producer.slug,
            display_name=producer.display_name,
            produced=0,
            inserted=0,
        )
        try:
            produced = await producer.generate(
                session,
                workspace_id=workspace_id,
                limit=limit_per_source,
            )
        except ProducerError as exc:
            per_result.error = f"{exc.__class__.__name__}: {exc}"
            logger.warning(
                "eval_producer_error",
                extra={"_producer": producer.slug, "_error": per_result.error},
            )
            result.producers.append(per_result)
            continue
        except Exception as exc:  # last-resort: one producer must not kill the others
            per_result.error = f"unexpected:{exc.__class__.__name__}"
            logger.exception("eval_producer_unexpected", extra={"_producer": producer.slug})
            result.producers.append(per_result)
            continue

        per_result.produced = len(produced)

        for candidate in produced:
            if per_result.inserted >= limit_per_source:
                break
            if remaining_capacity <= 0:
                per_result.skipped_cap += 1
                continue
            normalized = normalize_question(candidate.question)
            if not normalized:
                per_result.skipped_duplicate += 1  # treat blank as dedupe
                continue
            if normalized in seen_normalized:
                per_result.skipped_duplicate += 1
                continue
            if candidate.origin not in CASE_ORIGINS:
                # Producer set an unexpected origin — refuse instead of
                # silently demoting; surfaces as a unit-test failure.
                per_result.error = (
                    f"producer returned invalid origin {candidate.origin!r}"
                )
                break
            try:
                row = await service.create(
                    EvalCaseInput(
                        workspace_id=workspace_id,
                        question=candidate.question,
                        expected_answer=candidate.expected_answer,
                        expected_sql=candidate.expected_sql,
                        expected_connector_slug=candidate.expected_connector_slug,
                        expected_tool=candidate.expected_tool,
                        expected_citations=candidate.expected_citations or [],
                        tags=list(candidate.tags or []),
                        origin=candidate.origin,
                        status="candidate",
                    ),
                    created_by=created_by,
                )
            except Exception as exc:  # individual create failure shouldn't kill the batch
                per_result.error = f"create_failed:{exc.__class__.__name__}"
                logger.exception(
                    "eval_candidate_persist_failed",
                    extra={"_producer": producer.slug, "_question": candidate.question[:120]},
                )
                continue
            result.inserted_ids.append(row.id)
            per_result.inserted += 1
            remaining_capacity -= 1
            seen_normalized.add(normalized)

        result.producers.append(per_result)
        if remaining_capacity <= 0:
            result.ceiling_reached = True
            break

    if result.inserted_ids:
        await session.commit()

    after = await session.scalar(
        select(func.count(EvalCase.id)).where(
            EvalCase.workspace_id == workspace_id,
            EvalCase.status == "candidate",
        )
    ) or 0
    result.candidates_in_queue_after = int(after)
    return result
