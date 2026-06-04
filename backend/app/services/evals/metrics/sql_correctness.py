"""SQL correctness via lightweight normalization.

Strategy (no extra deps required in this phase):
* Strip comments, collapse whitespace, normalize identifiers/quotes/case.
* Compare normalized forms; identical → 1.0.
* If unequal but tokens are a permutation (different SELECT ordering),
  give partial credit 0.5.
* sqlglot-based AST equivalence is the natural Phase-5 upgrade — keep this
  module surface-stable so swapping the impl is a one-file change.
"""

from __future__ import annotations

import re

from app.services.evals.metrics.base import EvalContext, MetricScore

NAME = "sql_correctness"
GATES_PASS = True
DEFAULT_THRESHOLD = 0.5


_COMMENT_BLOCK_RE = re.compile(r"/\*.*?\*/", re.DOTALL)
_COMMENT_LINE_RE = re.compile(r"--[^\n]*")
_QUOTE_RE = re.compile(r'"|`')
_WHITESPACE_RE = re.compile(r"\s+")
_TRAILING_SEMI_RE = re.compile(r";\s*$")


def _normalize(sql: str) -> str:
    s = _COMMENT_BLOCK_RE.sub(" ", sql)
    s = _COMMENT_LINE_RE.sub(" ", s)
    s = _QUOTE_RE.sub("", s)
    s = _TRAILING_SEMI_RE.sub("", s)
    s = _WHITESPACE_RE.sub(" ", s).strip().lower()
    return s


def _tokens(sql: str) -> list[str]:
    # Cheap tokenizer: split on whitespace + punctuation boundaries.
    return [t for t in re.split(r"[\s,()=<>]+", _normalize(sql)) if t]


class SqlCorrectness:
    name = NAME
    gates_pass = GATES_PASS

    def score(self, ctx: EvalContext, *, threshold: float) -> MetricScore:
        if not ctx.expected_sql:
            return MetricScore(metric=self.name, status="skipped",
                               detail={"reason": "no expected_sql"})
        if not ctx.actual_sql:
            return MetricScore(metric=self.name, status="ok", score=0.0,
                               passed=False,
                               detail={"reason": "actual produced no SQL"})
        en = _normalize(ctx.expected_sql)
        an = _normalize(ctx.actual_sql)
        if en == an:
            return MetricScore(metric=self.name, status="ok", score=1.0,
                               passed=True,
                               detail={"match": "exact_after_normalize"})
        if sorted(_tokens(en)) == sorted(_tokens(an)):
            return MetricScore(metric=self.name, status="ok", score=0.5,
                               passed=0.5 >= threshold,
                               detail={"match": "token_set"})
        return MetricScore(
            metric=self.name,
            status="ok",
            score=0.0,
            passed=False,
            detail={
                "match": "differ",
                "expected_normalized": en,
                "actual_normalized": an,
            },
        )
