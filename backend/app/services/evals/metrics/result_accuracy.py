"""Result-accuracy via row-hash comparison.

Compares ``actual_result_hash`` against ``expected_result_hash`` (cached
on promote-golden per the locked decisions). If either is missing, this
metric is skipped — the runner records ``status=skipped`` instead of
falsely-passing or falsely-failing.
"""

from __future__ import annotations

from app.services.evals.metrics.base import EvalContext, MetricScore

NAME = "result_accuracy"
GATES_PASS = True
DEFAULT_THRESHOLD = 1.0  # binary metric


class ResultAccuracy:
    name = NAME
    gates_pass = GATES_PASS

    def score(self, ctx: EvalContext, *, threshold: float) -> MetricScore:
        if not ctx.expected_result_hash:
            return MetricScore(
                metric=self.name,
                status="skipped",
                detail={"reason": "no cached expected_result_hash (promote a golden case to populate)"},
            )
        if not ctx.actual_result_hash:
            return MetricScore(
                metric=self.name,
                status="ok",
                score=0.0,
                passed=False,
                detail={"reason": "actual produced no result set"},
            )
        passed = ctx.actual_result_hash == ctx.expected_result_hash
        return MetricScore(
            metric=self.name,
            status="ok",
            score=1.0 if passed else 0.0,
            passed=passed,
            detail={
                "expected_hash": ctx.expected_result_hash,
                "actual_hash": ctx.actual_result_hash,
            },
        )
