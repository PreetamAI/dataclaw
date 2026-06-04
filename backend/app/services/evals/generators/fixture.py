"""FixtureProducer — canned candidates against the demo SQLite source.

Always-present source. These cases are valuable as smoke tests because the
data is shipped with the repo, so eval runs are deterministic regardless of
which external connectors a workspace has wired up.
"""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession

from app.services.evals.generators.base import CandidateCase

# A small, deterministic set tied to the seeded demo schema in
# app/services/demo_seed.py. Keeping them inline (rather than loading from a
# yaml file) so the producer is a single, reviewable unit.
# The actual demo SQLite schema (see connectors/adapters.py seeder):
#   customers(customer_id TEXT PK, segment TEXT, arr REAL)
#   products (product_id  TEXT PK, family TEXT, gross_margin REAL)
#   orders   (order_id    TEXT PK, customer_id, product_id, net_revenue REAL,
#             ordered_at TEXT)
# Citations keep the qualified ``core.<table>`` form because the
# connector_accuracy metric and staleness invalidator both accept either
# the bare or qualified spelling (see evals/staleness.py). SQL bodies use
# the bare table names so they execute against SQLite, which has no
# schema concept.
DEMO_CASES: list[CandidateCase] = [
    CandidateCase(
        question="How many customers are in the demo dataset?",
        origin="auto:fixture",
        expected_sql="SELECT COUNT(*) FROM customers",
        expected_connector_slug="sqlite",
        expected_tool="sqlite.read_select",
        expected_citations=[
            {"source": "sqlite", "table": "core.customers"},
        ],
        tags=["fixture", "demo", "count"],
        confidence=1.0,
    ),
    CandidateCase(
        question="List the top 5 demo orders by total amount.",
        origin="auto:fixture",
        expected_sql=(
            "SELECT order_id, customer_id, net_revenue FROM orders "
            "ORDER BY net_revenue DESC LIMIT 5"
        ),
        expected_connector_slug="sqlite",
        expected_tool="sqlite.read_select",
        expected_citations=[
            {"source": "sqlite", "table": "core.orders"},
        ],
        tags=["fixture", "demo", "ranking"],
        confidence=1.0,
    ),
    CandidateCase(
        question="What columns does core.customers have?",
        origin="auto:fixture",
        expected_answer="customer_id (TEXT), segment (TEXT), arr (REAL)",
        expected_connector_slug="sqlite",
        expected_citations=[
            {"source": "sqlite", "table": "core.customers"},
        ],
        tags=["fixture", "demo", "schema"],
        confidence=1.0,
    ),
]


class FixtureProducer:
    slug = "fixture"
    origin = "auto:fixture"
    display_name = "Demo fixtures"

    async def generate(
        self,
        session: AsyncSession,  # unused; kept for protocol compatibility
        *,
        workspace_id: str,  # unused
        limit: int,
    ) -> list[CandidateCase]:
        return DEMO_CASES[:limit]
