"""LineageProducer — turn column-level lineage edges into eval candidates.

Templates:
* "Which columns derive from {source_table}.{source_column}?"  (downstream)
* "Where does {target_table}.{target_column} get its data from?" (upstream)

Uses ColumnLineageEdge rows produced by the knowledge compile step.
"""

from __future__ import annotations

from collections import defaultdict

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.domain import ColumnLineageEdge
from app.services.evals.generators.base import CandidateCase


class LineageProducer:
    slug = "lineage"
    origin = "auto:lineage"
    display_name = "Column lineage"

    async def generate(
        self,
        session: AsyncSession,
        *,
        workspace_id: str,
        limit: int,
    ) -> list[CandidateCase]:
        edges = list(
            (
                await session.scalars(
                    select(ColumnLineageEdge).where(
                        ColumnLineageEdge.workspace_id == workspace_id
                    )
                )
            ).all()
        )
        if not edges:
            return []

        # Group edges by both source-column (for downstream questions) and
        # target-column (for upstream questions).
        downstream: dict[tuple[str, str, str], list[ColumnLineageEdge]] = defaultdict(list)
        upstream: dict[tuple[str, str, str], list[ColumnLineageEdge]] = defaultdict(list)
        for e in edges:
            downstream[(e.source_connector_slug, e.source_table, e.source_column)].append(e)
            upstream[(e.target_connector_slug, e.target_table, e.target_column)].append(e)

        candidates: list[CandidateCase] = []

        # Downstream: "which columns derive from X.Y?"
        for (src_conn, src_table, src_col), group in downstream.items():
            if len(candidates) >= limit:
                break
            targets = sorted({f"{e.target_table}.{e.target_column}" for e in group})
            if not targets:
                continue
            candidates.append(
                CandidateCase(
                    question=(
                        f"Which columns derive from {src_table}.{src_col}?"
                    ),
                    origin=self.origin,
                    expected_answer=", ".join(targets),
                    expected_connector_slug=src_conn,
                    expected_citations=[
                        {"source": src_conn, "table": src_table, "columns": [src_col]},
                    ]
                    + [
                        {"source": e.target_connector_slug, "table": e.target_table,
                         "columns": [e.target_column]}
                        for e in group
                    ],
                    tags=["lineage", "downstream"],
                    confidence=0.9,
                )
            )

        # Upstream: "where does X.Y get its data from?"
        for (tgt_conn, tgt_table, tgt_col), group in upstream.items():
            if len(candidates) >= limit:
                break
            sources = sorted({f"{e.source_table}.{e.source_column}" for e in group})
            if not sources:
                continue
            candidates.append(
                CandidateCase(
                    question=(
                        f"Where does {tgt_table}.{tgt_col} get its data from?"
                    ),
                    origin=self.origin,
                    expected_answer=", ".join(sources),
                    expected_connector_slug=tgt_conn,
                    expected_citations=[
                        {"source": tgt_conn, "table": tgt_table, "columns": [tgt_col]},
                    ]
                    + [
                        {"source": e.source_connector_slug, "table": e.source_table,
                         "columns": [e.source_column]}
                        for e in group
                    ],
                    tags=["lineage", "upstream"],
                    confidence=0.9,
                )
            )
        return candidates[:limit]
