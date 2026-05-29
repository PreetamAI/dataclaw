from __future__ import annotations

import asyncio
import logging
from datetime import UTC, datetime

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.security import decrypt_json
from app.models.domain import AgentRun, Connector, Workspace
from app.services.connectors.adapters import adapter_for
from app.services.connectors.catalog import CATALOG_BY_SLUG, ConnectorCategory
from app.services.ingestion.service import IngestionService
from app.services.sync_materializer import materialize_sync

logger = logging.getLogger("dataclaw.auto_sync")
CONNECTOR_SYNC_TIMEOUT_SECONDS = 60.0


async def auto_sync_all_connectors(session: AsyncSession) -> AgentRun:
    settings = get_settings()
    workspace = await session.scalar(select(Workspace).limit(1))
    if workspace is None:
        raise RuntimeError("Workspace has not been seeded.")
    workspace_id = workspace.id

    # Snapshot only the primitive identifiers we need. ORM instances loaded
    # here would be expired/detached after the first rollback inside the loop
    # (rollback expires regardless of expire_on_commit=False), and accessing
    # any attribute on them then raises DetachedInstanceError.
    connector_keys: list[tuple[str, str]] = [
        (row.id, row.slug)
        for row in (
            await session.scalars(
                select(Connector).where(Connector.credential_state == "configured")
            )
        ).all()
    ]
    timeline: list[dict] = []
    status = "completed"
    for connector_id, connector_slug in connector_keys:
        connector_started_at = datetime.now(UTC)
        try:
            claimed = await session.execute(
                update(Connector)
                .where(Connector.id == connector_id, Connector.sync_state != "syncing")
                .values(sync_state="syncing", last_sync_error=None)
            )
            if claimed.rowcount == 0:
                timeline.append({"slug": connector_slug, "status": "skipped", "detail": "sync already running"})
                continue
            await session.commit()
            connector = await session.get(Connector, connector_id)
            if connector is None:
                timeline.append({"slug": connector_slug, "status": "skipped", "detail": "connector deleted mid-run"})
                continue
            credentials = {}
            if connector.encrypted_credentials:
                credentials = decrypt_json(settings.master_key, connector.encrypted_credentials)
            result, count, ingestion = await asyncio.wait_for(
                _sync_one_connector(session, connector, credentials),
                timeout=CONNECTOR_SYNC_TIMEOUT_SECONDS,
            )
            connector.sync_summary = result
            connector.sync_state = "synced"
            connector.last_synced_at = datetime.now(UTC)
            await session.commit()
            timeline.append(
                {
                    "slug": connector_slug,
                    "status": "ok",
                    "objects_synced": count,
                    "wiki_pages": ingestion.pages_written,
                    "raw_chunks": ingestion.raw_chunks_written,
                    "duration_ms": int((datetime.now(UTC) - connector_started_at).total_seconds() * 1000),
                }
            )
        except TimeoutError:
            status = "completed_with_errors"
            await session.rollback()
            await session.execute(
                update(Connector)
                .where(Connector.id == connector_id)
                .values(
                    sync_state="sync_failed",
                    last_sync_error=f"TimeoutError: exceeded {CONNECTOR_SYNC_TIMEOUT_SECONDS:.0f}s",
                )
            )
            await session.commit()
            logger.warning("auto_sync_timeout", extra={"_connector_slug": connector_slug})
            timeline.append({"slug": connector_slug, "status": "failed", "error": "TimeoutError"})
        except Exception as exc:
            status = "completed_with_errors"
            await session.rollback()
            await session.execute(
                update(Connector)
                .where(Connector.id == connector_id)
                .values(sync_state="sync_failed", last_sync_error=f"{exc.__class__.__name__}: {exc}")
            )
            await session.commit()
            logger.exception("auto_sync_failed", extra={"_connector_slug": connector_slug})
            timeline.append({"slug": connector_slug, "status": "failed", "error": exc.__class__.__name__})
    run = AgentRun(
        workspace_id=workspace_id,
        agent_name="auto_sync",
        status=status,
        summary=f"Auto-synced {sum(1 for item in timeline if item['status'] == 'ok')} of {len(timeline)} configured connectors.",
        timeline=timeline,
    )
    session.add(run)
    await session.commit()
    return run


async def _sync_one_connector(session: AsyncSession, connector: Connector, credentials: dict) -> tuple[dict, int, object]:
    result = await adapter_for(connector.slug).sync(credentials)
    definition = CATALOG_BY_SLUG.get(connector.slug)
    if definition and definition.category == ConnectorCategory.KNOWLEDGE:
        count = 0
    else:
        count = await materialize_sync(session, connector, result)
    ingestion = await IngestionService(session).ingest_connector(connector, credentials)
    return result, count, ingestion
