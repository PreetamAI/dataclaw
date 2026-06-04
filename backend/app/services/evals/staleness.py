"""Schema-drift invalidation for golden eval cases.

When a connector sync detects a column change, golden eval cases that
reference the affected table can no longer be trusted as canonical —
their cached ``expected_result_hash`` may be wrong, their ``expected_sql``
may reference a column that no longer exists, and chat will keep
short-circuiting to a stale answer.

This service finds those golden cases, demotes them back to ``approved``
(so chat stops short-circuiting), and tags them ``stale_after_schema_drift``
so the user can spot and re-promote them after manual review.

We do NOT auto-archive — that would silently drop user-curated evals.
Demotion + tag keeps the suite intact while making the staleness visible.
"""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass

from sqlalchemy import or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.domain import EvalCase, TableAsset

logger = logging.getLogger(__name__)


STALE_TAG = "stale_after_schema_drift"


@dataclass
class InvalidationResult:
    workspace_id: str
    affected_table: str
    demoted_case_ids: list[str]


def column_signature(columns: list[dict] | None) -> str:
    """Order-invariant hash of column (name, type) pairs. Used to detect
    schema drift: if the new signature differs from the old, the schema
    changed in a way that may break cached eval results."""
    if not columns:
        return "empty"
    norm = sorted(
        (str(c.get("name") or "").lower(), str(c.get("type") or "").lower())
        for c in columns
        if isinstance(c, dict)
    )
    payload = json.dumps(norm, sort_keys=True)
    return hashlib.sha256(payload.encode()).hexdigest()


def _table_key_matches(case_citations: list[dict] | None, qualified_name: str) -> bool:
    """A golden case is "affected" if any of its expected_citations names
    the same table. Match is case-insensitive on the table field; we accept
    either bare ``customers`` or qualified ``core.customers``."""
    if not case_citations:
        return False
    needle = qualified_name.lower()
    needle_bare = needle.rsplit(".", 1)[-1]
    for c in case_citations:
        if not isinstance(c, dict):
            continue
        t = (c.get("table") or c.get("title") or "")
        if not isinstance(t, str) or not t:
            continue
        t = t.lower()
        if t == needle or t == needle_bare or t.rsplit(".", 1)[-1] == needle_bare:
            return True
    return False


async def invalidate_golden_cases_for_table(
    session: AsyncSession,
    *,
    workspace_id: str,
    qualified_table_name: str,
) -> InvalidationResult:
    """Demote every golden EvalCase in the workspace whose
    expected_citations reference ``qualified_table_name``.

    Idempotent: a case already demoted (no longer golden) is skipped. A
    case that has the stale tag stays at the same demoted-and-tagged
    state if invoked a second time.
    """
    stmt = select(EvalCase).where(
        EvalCase.workspace_id == workspace_id,
        EvalCase.status == "golden",
    )
    rows = list((await session.scalars(stmt)).all())
    demoted: list[str] = []
    for case in rows:
        if not _table_key_matches(case.expected_citations, qualified_table_name):
            continue
        case.status = "approved"
        tags = list(case.tags or [])
        if STALE_TAG not in tags:
            tags.append(STALE_TAG)
            case.tags = tags
        # Clear the cached result hash so the next promote-to-golden has
        # to re-execute the SQL against the new schema.
        case.expected_result_hash = None
        case.expected_result_preview = []
        demoted.append(case.id)
    if demoted:
        await session.flush()
        logger.info(
            "eval_golden_demoted_on_schema_drift",
            extra={
                "_workspace": workspace_id,
                "_table": qualified_table_name,
                "_count": len(demoted),
            },
        )
    return InvalidationResult(
        workspace_id=workspace_id,
        affected_table=qualified_table_name,
        demoted_case_ids=demoted,
    )


async def detect_and_invalidate_for_table_asset(
    session: AsyncSession,
    *,
    table_asset: TableAsset,
    prior_columns: list[dict] | None,
) -> InvalidationResult | None:
    """Compare a TableAsset's current columns against the prior snapshot
    and run invalidation if they differ. Returns None when no drift.

    Caller (typically the sync path or a CLI) provides ``prior_columns``;
    we compute the signature here so callers don't have to know about the
    hash algorithm.
    """
    new_sig = column_signature(list(table_asset.columns or []))
    old_sig = column_signature(prior_columns)
    if new_sig == old_sig:
        return None
    # Resolve workspace via Dataset → workspace_id.
    from app.models.domain import Dataset

    dataset = await session.get(Dataset, table_asset.dataset_id)
    if dataset is None:
        return None
    qualified = table_asset.name
    if dataset.schema_name and dataset.schema_name not in {"", "public"}:
        if not qualified.startswith(f"{dataset.schema_name}."):
            qualified = f"{dataset.schema_name}.{qualified}"
    return await invalidate_golden_cases_for_table(
        session,
        workspace_id=dataset.workspace_id,
        qualified_table_name=qualified,
    )


async def list_stale_golden_cases(
    session: AsyncSession,
    *,
    workspace_id: str,
) -> list[EvalCase]:
    """Return cases that were demoted by schema drift (carry the tag
    ``STALE_TAG``) so the UI can surface a "needs review" badge."""
    stmt = select(EvalCase).where(
        EvalCase.workspace_id == workspace_id,
        or_(EvalCase.status == "approved", EvalCase.status == "candidate"),
    )
    rows = list((await session.scalars(stmt)).all())
    return [r for r in rows if STALE_TAG in (r.tags or [])]
