"""SchemaProducer — turn table assets into eval candidates.

Cheapest, highest-signal producer: every synced TableAsset gives us 2
candidate cases out of the box. These are deterministic and don't need any
LLM call to generate, which makes them ideal smoke tests.
"""

from __future__ import annotations

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.domain import Connector, Dataset, TableAsset
from app.services.evals.generators.base import CandidateCase


class SchemaProducer:
    slug = "schema"
    origin = "auto:schema"
    display_name = "Schemas"

    async def generate(
        self,
        session: AsyncSession,
        *,
        workspace_id: str,
        limit: int,
    ) -> list[CandidateCase]:
        # Pull tables + dataset + connector slug in a single join. Order by
        # most-recently-synced so the most relevant tables produce first when
        # we hit `limit`.
        stmt = (
            select(TableAsset, Dataset, Connector)
            .join(Dataset, TableAsset.dataset_id == Dataset.id)
            .join(Connector, Dataset.connector_id == Connector.id, isouter=True)
            .where(Dataset.workspace_id == workspace_id)
            .order_by(TableAsset.updated_at.desc())
        )
        rows = list((await session.execute(stmt)).all())
        candidates: list[CandidateCase] = []
        for table, dataset, connector in rows:
            if len(candidates) >= limit:
                break
            connector_slug = connector.slug if connector else None
            qualified = _qualify(dataset.schema_name, table.name)
            cite = [
                {
                    "source": connector_slug or dataset.source_type,
                    "table": qualified,
                    "columns": [c.get("name") for c in (table.columns or []) if isinstance(c, dict)],
                }
            ]
            # 1) Schema-shape question
            columns_text = _describe_columns(table.columns or [])
            if columns_text:
                candidates.append(
                    CandidateCase(
                        question=f"What columns does {qualified} have?",
                        origin=self.origin,
                        expected_answer=columns_text,
                        expected_connector_slug=connector_slug,
                        expected_citations=cite,
                        tags=["schema", "columns"],
                        confidence=1.0,
                    )
                )
                if len(candidates) >= limit:
                    break
            # 2) Row-count question (only if the connector likely supports SQL —
            #    metadata-only connectors don't). The list mirrors the
            #    DATA_STORE_CONNECTOR_SLUGS set in agents/chat.py; keeping the
            #    knowledge inline avoids a circular import.
            if connector_slug in _SQL_CONNECTORS:
                candidates.append(
                    CandidateCase(
                        question=f"How many rows are in {qualified}?",
                        origin=self.origin,
                        expected_sql=f"SELECT COUNT(*) FROM {qualified}",
                        expected_connector_slug=connector_slug,
                        expected_tool=f"{connector_slug}.read_select",
                        expected_citations=cite,
                        tags=["schema", "count"],
                        # row_count on TableAsset is stale by definition; if
                        # we have a non-zero one it's at least a hint.
                        expected_answer=(
                            f"Approximately {table.row_count:,} rows "
                            f"(as of last sync)."
                            if table.row_count
                            else None
                        ),
                        confidence=0.9,
                    )
                )
        return candidates


_SQL_CONNECTORS = frozenset(
    {
        "postgres",
        "mysql",
        "redshift",
        "sql_server",
        "databricks",
        "bigquery",
        "snowflake",
        "trino",
        "sqlite",
    }
)


def _qualify(schema_name: str | None, table_name: str) -> str:
    """Build a `schema.table` reference, skipping the schema if absent or
    if the schema is already embedded in the name (idempotent)."""
    if not schema_name or schema_name in {"", "public"}:
        return table_name if "." in table_name else table_name
    if table_name.startswith(f"{schema_name}."):
        return table_name
    return f"{schema_name}.{table_name}"


def _describe_columns(columns: list[dict]) -> str:
    parts: list[str] = []
    for col in columns:
        if not isinstance(col, dict):
            continue
        name = col.get("name")
        if not name:
            continue
        ctype = col.get("type") or ""
        if ctype:
            parts.append(f"{name} ({ctype})")
        else:
            parts.append(str(name))
    if not parts:
        return ""
    return ", ".join(parts)
