"""Metric plug-in protocol + supporting dataclasses.

A Metric reads one ``EvalContext`` and returns one ``MetricScore``. Metrics
are intentionally simple and pure-compute — the runner is in charge of
persistence, ordering, and failure category derivation.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol


@dataclass
class EvalContext:
    """Everything a metric needs to score a single run.

    The runner builds one of these per case-execution and hands it to every
    metric. None of the comparators mutate it; this keeps metrics safely
    parallelizable later (Phase 5 may run them concurrently).
    """

    # Expected (from the EvalCase row)
    expected_answer: str | None
    expected_sql: str | None
    expected_connector_slug: str | None
    expected_tool: str | None
    expected_citations: list[dict]
    expected_result_hash: str | None
    expected_result_preview: list[dict]
    # Actual (from this run)
    actual_answer: str | None
    actual_sql: str | None
    actual_connector_slug: str | None
    actual_tool: str | None
    actual_citations: list[dict]
    actual_result_hash: str | None
    actual_result_preview: list[dict]
    # Trace-derived
    retrieval_candidates: list[str] = field(default_factory=list)
    chat_spans: list[dict] = field(default_factory=list)
    duration_ms: int = 0
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    total_tokens: int | None = None
    cost_usd: float | None = None
    model: str | None = None
    # Cross-run signals
    previous_run_passed: bool | None = None  # for regression
    repeat_scores: list[float] = field(default_factory=list)  # for flakiness
    # Identity
    question: str = ""
    # Per-workspace judge LLM config used by Ragas-backed judge metrics
    # (faithfulness / answer_relevancy / safety). Shape:
    #   {"api_key": str, "model": str, "base_url": str | None,
    #    "embedding_model": str | None}
    # None means "no judge available" — judge metrics will skip cleanly.
    judge_config: dict[str, Any] | None = None


@dataclass
class MetricScore:
    metric: str
    # status: ok | skipped | error
    status: str = "ok"
    score: float | None = None    # 0.0-1.0 where applicable
    passed: bool | None = None    # None if metric is informational only
    detail: dict[str, Any] = field(default_factory=dict)


class Metric(Protocol):
    name: str
    # When True, this metric contributes to the run's overall pass/fail.
    # When False, it's informational (latency, cost, etc.).
    gates_pass: bool

    def score(self, ctx: EvalContext, *, threshold: float) -> MetricScore:
        ...
