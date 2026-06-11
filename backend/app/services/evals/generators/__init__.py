"""Eval-case auto-generators.

Each producer turns connected-source state (schemas, knowledge graph, column
lineage, demo fixtures) into a list of CandidateCase records. The coordinator
in :mod:`app.services.evals.generators.coordinator` runs all producers,
deduplicates, applies caps, and persists via EvalCaseService — producers
themselves are pure: they don't touch the DB beyond reads, never insert.
"""

from app.services.evals.generators.base import CandidateCase, Producer, ProducerError
from app.services.evals.generators.chat_history import ChatHistoryProducer
from app.services.evals.generators.coordinator import GenerationResult, run_generators
from app.services.evals.generators.fixture import FixtureProducer
from app.services.evals.generators.kg import KgProducer
from app.services.evals.generators.lineage import LineageProducer
from app.services.evals.generators.schema import SchemaProducer

# Order is the user-visible order on the Evals page candidate-review view.
# chat_history is listed last because it depends on accumulated usage —
# most workspaces won't have endorsed turns on day one.
ALL_PRODUCERS: list[Producer] = [
    SchemaProducer(),
    KgProducer(),
    LineageProducer(),
    FixtureProducer(),
    ChatHistoryProducer(),
]

__all__ = [
    "ALL_PRODUCERS",
    "CandidateCase",
    "ChatHistoryProducer",
    "FixtureProducer",
    "GenerationResult",
    "KgProducer",
    "LineageProducer",
    "Producer",
    "ProducerError",
    "SchemaProducer",
    "run_generators",
]
