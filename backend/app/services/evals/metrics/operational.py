"""Operational, non-judging metrics: latency, cost, tokens, regression,
flakiness. None of these gate pass/fail by default; the dashboard surfaces
them as trend signals."""

from __future__ import annotations

import statistics

from app.services.evals.metrics.base import EvalContext, MetricScore


class Latency:
    name = "latency"
    gates_pass = False

    def score(self, ctx: EvalContext, *, threshold: float) -> MetricScore:
        return MetricScore(
            metric=self.name,
            status="ok",
            score=float(ctx.duration_ms),
            passed=None,
            detail={"duration_ms": ctx.duration_ms},
        )


class Cost:
    name = "cost"
    gates_pass = False

    def score(self, ctx: EvalContext, *, threshold: float) -> MetricScore:
        if ctx.cost_usd is None:
            return MetricScore(metric=self.name, status="skipped",
                               detail={"reason": "no cost_usd on trace"})
        return MetricScore(
            metric=self.name,
            status="ok",
            score=float(ctx.cost_usd),
            passed=None,
            detail={"cost_usd": ctx.cost_usd, "model": ctx.model},
        )


class Tokens:
    name = "tokens"
    gates_pass = False

    def score(self, ctx: EvalContext, *, threshold: float) -> MetricScore:
        if ctx.total_tokens is None and ctx.prompt_tokens is None and ctx.completion_tokens is None:
            return MetricScore(metric=self.name, status="skipped",
                               detail={"reason": "no token counts on trace"})
        return MetricScore(
            metric=self.name,
            status="ok",
            score=float(ctx.total_tokens or 0),
            passed=None,
            detail={
                "prompt_tokens": ctx.prompt_tokens,
                "completion_tokens": ctx.completion_tokens,
                "total_tokens": ctx.total_tokens,
                "model": ctx.model,
            },
        )


class Regression:
    """Flags a transition: previously-passing case now failing."""

    name = "regression"
    gates_pass = False

    def score(self, ctx: EvalContext, *, threshold: float) -> MetricScore:
        if ctx.previous_run_passed is None:
            return MetricScore(metric=self.name, status="skipped",
                               detail={"reason": "no prior run for case"})
        # We can't know the *current* pass yet at metric time — the runner
        # aggregates gating metrics afterwards. So we record the prior
        # state here and let the runner stamp `passed` after aggregation.
        return MetricScore(
            metric=self.name,
            status="ok",
            score=None,
            passed=None,
            detail={"previous_run_passed": ctx.previous_run_passed},
        )


class Flakiness:
    """Variance over N repeats of the same case in the same batch.

    Only emitted when mode='repeat:N' on the run; otherwise skipped.
    """

    name = "flakiness"
    gates_pass = False

    def score(self, ctx: EvalContext, *, threshold: float) -> MetricScore:
        if not ctx.repeat_scores or len(ctx.repeat_scores) < 2:
            return MetricScore(metric=self.name, status="skipped",
                               detail={"reason": "single-run mode"})
        stdev = statistics.pstdev(ctx.repeat_scores)
        return MetricScore(
            metric=self.name,
            status="ok",
            score=stdev,
            passed=stdev <= threshold,
            detail={
                "samples": ctx.repeat_scores,
                "stdev": stdev,
            },
        )
