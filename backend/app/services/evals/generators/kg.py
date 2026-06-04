"""KgProducer — walk the knowledge graph for relationship questions.

Templates:
* "Which dashboards / dbt models / docs use {table}?"       (incoming edges)
* "What upstream tables feed {model_or_dag}?"                (outgoing edges)
* "Which DAG builds {table}?"                                 (specialised
                                                               incoming-edge
                                                               variant for
                                                               dag/dbt nodes)

The graph is built by knowledge_compile/. Producers stay read-only and don't
care how the edges got there — if the compiler hasn't run, KG candidates
just come back empty (no error).
"""

from __future__ import annotations

from collections import defaultdict
from collections.abc import Iterable

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.domain import KnowledgeEdge, KnowledgeNode
from app.services.evals.generators.base import CandidateCase


# Relationship buckets — we map raw edge `relationship` values to coarse
# semantic intents the templates care about. Anything not in the buckets
# is ignored (a producer must not invent meaning from unknown edges).
INCOMING_USE = frozenset({"uses", "reads_from", "queries", "depends_on", "references"})
INCOMING_BUILDS = frozenset({"builds", "produces", "writes_to", "materializes"})
OUTGOING_FEEDS = frozenset({"feeds", "writes_to", "produces", "materializes"})
OUTGOING_DERIVES = frozenset({"derives_from", "uses", "depends_on", "reads_from"})

# Node types we treat as the "thing that uses" (left side of "X uses table T").
USER_NODE_TYPES = frozenset({"dashboard", "metric", "dbt_model", "dag", "doc"})


class KgProducer:
    slug = "kg"
    origin = "auto:kg"
    display_name = "Knowledge graph"

    async def generate(
        self,
        session: AsyncSession,
        *,
        workspace_id: str,
        limit: int,
    ) -> list[CandidateCase]:
        # Load nodes + edges scoped to the workspace; build node lookups.
        node_rows = list(
            (
                await session.scalars(
                    select(KnowledgeNode).where(KnowledgeNode.workspace_id == workspace_id)
                )
            ).all()
        )
        if not node_rows:
            return []
        by_id = {n.id: n for n in node_rows}
        edge_rows = list(
            (
                await session.scalars(
                    select(KnowledgeEdge).where(KnowledgeEdge.workspace_id == workspace_id)
                )
            ).all()
        )
        if not edge_rows:
            return []

        # Index edges by dst (incoming) and src (outgoing) for fast lookup.
        incoming: dict[str, list[KnowledgeEdge]] = defaultdict(list)
        outgoing: dict[str, list[KnowledgeEdge]] = defaultdict(list)
        for e in edge_rows:
            incoming[e.dst_node_id].append(e)
            outgoing[e.src_node_id].append(e)

        candidates: list[CandidateCase] = []

        # --- "which X uses table T?" + "which DAG builds table T?" -------
        table_nodes = [n for n in node_rows if n.type == "table"]
        for table in table_nodes:
            if len(candidates) >= limit:
                break
            edges = incoming.get(table.id, [])

            uses = _filter_by_kind(edges, INCOMING_USE, by_id, USER_NODE_TYPES)
            if uses:
                summary = _name_list([by_id[e.src_node_id] for e in uses])
                candidates.append(
                    CandidateCase(
                        question=f"Which dashboards or models use {table.canonical_name}?",
                        origin=self.origin,
                        expected_answer=summary,
                        expected_connector_slug=table.connector_slug,
                        expected_citations=_node_citations([table, *(by_id[e.src_node_id] for e in uses)]),
                        tags=["kg", "usage"],
                        confidence=0.85,
                    )
                )
                if len(candidates) >= limit:
                    break

            # Build/materialize side: usually dbt/airflow nodes.
            builds = _filter_by_kind(edges, INCOMING_BUILDS, by_id, {"dag", "dbt_model"})
            if builds:
                summary = _name_list([by_id[e.src_node_id] for e in builds])
                candidates.append(
                    CandidateCase(
                        question=f"Which DAG or model builds {table.canonical_name}?",
                        origin=self.origin,
                        expected_answer=summary,
                        expected_connector_slug=table.connector_slug,
                        expected_citations=_node_citations([table, *(by_id[e.src_node_id] for e in builds)]),
                        tags=["kg", "build"],
                        confidence=0.85,
                    )
                )

        # --- "what upstream tables feed model M?" ------------------------
        model_nodes = [n for n in node_rows if n.type in {"dbt_model", "dag", "metric"}]
        for model in model_nodes:
            if len(candidates) >= limit:
                break
            out_edges = outgoing.get(model.id, [])
            ups = _filter_by_kind(out_edges, OUTGOING_FEEDS, by_id, {"table", "column"}, reverse=True)
            # OUTGOING_FEEDS values are model->table (uncommon); also try
            # incoming edges to the model whose relationship is in OUTGOING_DERIVES
            # (table->model), which is the typical lineage direction.
            in_edges = incoming.get(model.id, [])
            derives = _filter_by_kind(in_edges, OUTGOING_DERIVES, by_id, {"table"})
            sources = [by_id[e.dst_node_id] for e in ups] + [by_id[e.src_node_id] for e in derives]
            sources = _dedupe_nodes(sources)
            if sources:
                summary = _name_list(sources)
                candidates.append(
                    CandidateCase(
                        question=f"What upstream tables feed {model.canonical_name}?",
                        origin=self.origin,
                        expected_answer=summary,
                        expected_connector_slug=model.connector_slug,
                        expected_citations=_node_citations([model, *sources]),
                        tags=["kg", "lineage"],
                        confidence=0.8,
                    )
                )

        return candidates[:limit]


# ---------- helpers ----------


def _filter_by_kind(
    edges: list[KnowledgeEdge],
    relationships: Iterable[str],
    by_id: dict[str, KnowledgeNode],
    counterpart_node_types: Iterable[str],
    *,
    reverse: bool = False,
) -> list[KnowledgeEdge]:
    """Filter edges by relationship name AND by the type of the *other* node
    in the edge (src or dst depending on direction)."""
    rels = set(relationships)
    types = set(counterpart_node_types)
    out: list[KnowledgeEdge] = []
    for e in edges:
        if e.relationship not in rels:
            continue
        other_id = e.src_node_id if not reverse else e.dst_node_id
        node = by_id.get(other_id)
        if node is None or node.type not in types:
            continue
        out.append(e)
    return out


def _name_list(nodes: list[KnowledgeNode]) -> str:
    seen: set[str] = set()
    parts: list[str] = []
    for n in nodes:
        if n.canonical_name in seen:
            continue
        seen.add(n.canonical_name)
        label = f"{n.canonical_name} ({n.type})"
        parts.append(label)
    return ", ".join(parts)


def _node_citations(nodes: list[KnowledgeNode]) -> list[dict]:
    out: list[dict] = []
    seen: set[tuple[str, str]] = set()
    for n in nodes:
        key = (n.connector_slug, n.canonical_name)
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "source": n.connector_slug,
                "table": n.canonical_name,
                "type": n.type,
            }
        )
    return out


def _dedupe_nodes(nodes: list[KnowledgeNode]) -> list[KnowledgeNode]:
    seen: set[str] = set()
    out: list[KnowledgeNode] = []
    for n in nodes:
        if n.id in seen:
            continue
        seen.add(n.id)
        out.append(n)
    return out
