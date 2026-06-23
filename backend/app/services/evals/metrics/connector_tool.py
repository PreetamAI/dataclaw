"""Connector + tool accuracy.

Two related metrics kept in one module because they share the comparison
pattern (exact-match on a slug-shaped string).
"""

from __future__ import annotations

from app.services.evals.metrics.base import EvalContext, MetricScore


class ConnectorAccuracy:
    name = "connector_accuracy"
    gates_pass = True

    def score(self, ctx: EvalContext, *, threshold: float) -> MetricScore:
        if not ctx.expected_connector_slug:
            return MetricScore(metric=self.name, status="skipped",
                               detail={"reason": "no expected_connector_slug"})
        # No connector tool was invoked: the chat answered from the knowledge
        # base (e.g. schema/lineage questions). That is not a *wrong* connector
        # — gate on answer correctness instead, so skip rather than fail.
        if not ctx.actual_connector_slug:
            return MetricScore(metric=self.name, status="skipped",
                               detail={"reason": "no connector tool invoked (answered from knowledge base)",
                                       "expected": ctx.expected_connector_slug})
        passed = ctx.actual_connector_slug == ctx.expected_connector_slug
        return MetricScore(
            metric=self.name,
            status="ok",
            score=1.0 if passed else 0.0,
            passed=passed,
            detail={
                "expected": ctx.expected_connector_slug,
                "actual": ctx.actual_connector_slug,
            },
        )


class ToolAccuracy:
    name = "tool_accuracy"
    gates_pass = True

    def score(self, ctx: EvalContext, *, threshold: float) -> MetricScore:
        if not ctx.expected_tool:
            return MetricScore(metric=self.name, status="skipped",
                               detail={"reason": "no expected_tool"})
        if not ctx.actual_tool:
            return MetricScore(metric=self.name, status="skipped",
                               detail={"reason": "no tool invoked (answered from knowledge base)",
                                       "expected": ctx.expected_tool})
        passed = ctx.actual_tool == ctx.expected_tool
        return MetricScore(
            metric=self.name,
            status="ok",
            score=1.0 if passed else 0.0,
            passed=passed,
            detail={
                "expected": ctx.expected_tool,
                "actual": ctx.actual_tool,
            },
        )
