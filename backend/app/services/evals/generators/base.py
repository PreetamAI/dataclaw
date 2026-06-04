"""Producer protocol + CandidateCase dataclass.

A producer is a small, focused unit that turns one *kind* of workspace state
(schemas, knowledge graph, lineage, demo fixtures) into candidate eval cases.
They're stateless, pure-read, and isolated — any producer can fail without
taking the others down (see ProducerError and the coordinator's per-producer
try/except).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession


@dataclass
class CandidateCase:
    """One auto-generated eval case before it lands in the DB.

    Mirrors EvalCaseInput's payload-relevant fields. The coordinator stamps
    workspace_id + origin and persists via EvalCaseService.create — we keep
    this struct distinct from EvalCaseInput so producers don't have to know
    about persistence concerns.
    """

    question: str
    origin: str  # e.g. "auto:schema", "auto:kg", "auto:lineage", "auto:fixture"
    expected_answer: str | None = None
    expected_sql: str | None = None
    expected_connector_slug: str | None = None
    expected_tool: str | None = None
    expected_citations: list[dict] = field(default_factory=list)
    tags: list[str] = field(default_factory=list)
    # Producer-supplied confidence (0.0–1.0). Surfaced in the review UI so
    # the user can triage. Not currently persisted — kept as a hint.
    confidence: float = 1.0


class ProducerError(Exception):
    """Raised by a producer when it can't generate (missing data, schema
    drift, etc). The coordinator catches and logs; other producers keep
    running."""


class Producer(Protocol):
    """All producers implement this. Keep ``generate`` pure-read and
    fast — heavy work belongs in the worker (Phase 4)."""

    slug: str           # e.g. "schema", "kg", "lineage", "fixture"
    origin: str         # e.g. "auto:schema"
    display_name: str   # e.g. "Schemas"

    async def generate(
        self,
        session: AsyncSession,
        *,
        workspace_id: str,
        limit: int,
    ) -> list[CandidateCase]:
        ...
