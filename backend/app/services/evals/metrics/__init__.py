"""Metric plug-in registry + default per-metric pass thresholds.

A workspace can override any threshold via the eval_metric_thresholds table
(see app/models/domain.py:EvalMetricThreshold); these defaults are the
fallback when no row exists for a given (workspace, metric).
"""

from app.services.evals.metrics.base import EvalContext, Metric, MetricScore
from app.services.evals.metrics.citations import CitationAccuracy, RetrievalQuality
from app.services.evals.metrics.connector_tool import ConnectorAccuracy, ToolAccuracy
from app.services.evals.metrics.judges import AnswerRelevancy, Faithfulness, Safety
from app.services.evals.metrics.operational import (
    Cost,
    Flakiness,
    Latency,
    Regression,
    Tokens,
)
from app.services.evals.metrics.result_accuracy import ResultAccuracy
from app.services.evals.metrics.sql_correctness import SqlCorrectness


# Order is the order metric rows appear in the run-detail UI.
ALL_METRICS: list[Metric] = [
    SqlCorrectness(),
    ResultAccuracy(),
    ConnectorAccuracy(),
    ToolAccuracy(),
    CitationAccuracy(),
    RetrievalQuality(),
    Faithfulness(),
    AnswerRelevancy(),
    Safety(),
    Latency(),
    Cost(),
    Tokens(),
    Regression(),
    Flakiness(),
]


DEFAULT_THRESHOLDS: dict[str, float] = {
    "sql_correctness": 0.5,
    "result_accuracy": 1.0,
    "connector_accuracy": 1.0,
    "tool_accuracy": 1.0,
    "citation_accuracy": 0.5,
    "retrieval_quality": 0.5,
    "faithfulness": 0.7,
    "answer_relevancy": 0.7,
    "safety": 1.0,
    "latency": 0.0,        # informational
    "cost": 0.0,           # informational
    "tokens": 0.0,         # informational
    "regression": 0.0,     # informational
    "flakiness": 0.1,      # stdev cap (lower is better)
}


__all__ = [
    "ALL_METRICS",
    "DEFAULT_THRESHOLDS",
    "EvalContext",
    "Metric",
    "MetricScore",
]
