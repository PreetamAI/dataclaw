"""Citation accuracy + retrieval quality.

* citation_accuracy: Jaccard overlap of (source, table) tuples between
  expected and actual citations.
* retrieval_quality: recall@k of expected citations within the
  retrieval-span's candidate_ids list (chat_spans payload).
"""

from __future__ import annotations

from app.services.evals.metrics.base import EvalContext, MetricScore


def _citation_keys(citations: list[dict]) -> set[tuple[str | None, str | None]]:
    keys: set[tuple[str | None, str | None]] = set()
    for c in citations or []:
        if not isinstance(c, dict):
            continue
        # The chat path emits various citation shapes (tool_call_provenance,
        # golden_query_provenance, plain {title, connector}). We only score
        # those that name a (source/connector, table/title) pair.
        source = c.get("source") or c.get("connector")
        table = c.get("table") or c.get("title")
        if source and table:
            keys.add((str(source), str(table)))
    return keys


class CitationAccuracy:
    name = "citation_accuracy"
    gates_pass = True

    def score(self, ctx: EvalContext, *, threshold: float) -> MetricScore:
        expected = _citation_keys(ctx.expected_citations)
        if not expected:
            return MetricScore(metric=self.name, status="skipped",
                               detail={"reason": "no expected citations"})
        actual = _citation_keys(ctx.actual_citations)
        union = expected | actual
        inter = expected & actual
        score = len(inter) / len(union) if union else 0.0
        return MetricScore(
            metric=self.name,
            status="ok",
            score=score,
            passed=score >= threshold,
            detail={
                "expected": [list(k) for k in sorted(expected)],
                "actual": [list(k) for k in sorted(actual)],
                "intersection_size": len(inter),
                "union_size": len(union),
            },
        )


class RetrievalQuality:
    name = "retrieval_quality"
    gates_pass = True

    def score(self, ctx: EvalContext, *, threshold: float) -> MetricScore:
        expected_keys = _citation_keys(ctx.expected_citations)
        if not expected_keys:
            return MetricScore(metric=self.name, status="skipped",
                               detail={"reason": "no expected citations"})
        if not ctx.retrieval_candidates:
            return MetricScore(metric=self.name, status="ok", score=0.0,
                               passed=False,
                               detail={"reason": "no retrieval candidates in trace"})
        # We don't have the candidate-id → (source, table) map at this
        # boundary (Phase 1's retrieval span recorded the question + node
        # counts but not the canonical keys). Until that lands, do a
        # substring-recall against the candidate strings.
        candidate_text = " ".join(str(c) for c in ctx.retrieval_candidates).lower()
        hits = sum(
            1
            for source, table in expected_keys
            if (table or "").lower() in candidate_text
        )
        score = hits / len(expected_keys)
        return MetricScore(
            metric=self.name,
            status="ok",
            score=score,
            passed=score >= threshold,
            detail={
                "hits": hits,
                "expected_count": len(expected_keys),
                "candidates_inspected": len(ctx.retrieval_candidates),
            },
        )
