"""Integrations API.

Generic surface for optional, external-service integrations the platform
can talk to. Phase 1 ships only Langfuse (observability); the catalog
pattern leaves room to add more providers without changing the route shape.

GET    /integrations/observability                  - list catalog + current values
GET    /integrations/observability/{slug}           - get current config (secrets redacted)
PUT    /integrations/observability/{slug}           - upsert config
DELETE /integrations/observability/{slug}           - disable + clear (sets enabled=False)
POST   /integrations/observability/{slug}/test      - probe the integration
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import require_admin
from app.db.session import get_session
from app.models.domain import User
from app.services.observability.langfuse_client import invalidate_cache, resolve_sink
from app.services.settings_store import (
    OBSERVABILITY_CATALOG,
    get_observability_provider,
    list_observability_providers,
    update_observability_provider,
)

router = APIRouter(prefix="/integrations", tags=["integrations"])


# ---------- response shapes ----------


class ObservabilityField(BaseModel):
    name: str
    label: str
    secret: bool
    required: bool
    placeholder: str


class ObservabilityCatalogItem(BaseModel):
    slug: str
    display_name: str
    docs_url: str
    description: str
    fields: list[ObservabilityField]


class ObservabilityRecord(BaseModel):
    slug: str
    configured: bool
    enabled: bool
    values: dict[str, str] = Field(default_factory=dict)
    secrets_set: list[str] = Field(default_factory=list)
    secret_previews: dict[str, str] = Field(default_factory=dict)


class ObservabilityListResponse(BaseModel):
    catalog: list[ObservabilityCatalogItem]
    records: dict[str, ObservabilityRecord]


class ObservabilityUpdateRequest(BaseModel):
    values: dict[str, Any] = Field(default_factory=dict)


class ObservabilityTestResponse(BaseModel):
    status: str  # "ok" | "error" | "not_configured" | "sdk_missing"
    message: str


# ---------- helpers ----------


def _catalog_item(slug: str) -> ObservabilityCatalogItem:
    definition = OBSERVABILITY_CATALOG[slug]
    return ObservabilityCatalogItem(
        slug=definition["slug"],
        display_name=definition["display_name"],
        docs_url=definition["docs_url"],
        description=definition["description"],
        fields=[ObservabilityField(**field) for field in definition["fields"]],
    )


def _project_record(slug: str, raw: dict[str, Any]) -> ObservabilityRecord:
    definition = OBSERVABILITY_CATALOG[slug]
    secret_names = {field["name"] for field in definition["fields"] if field["secret"]}
    values = {k: v for k, v in raw.items() if isinstance(v, str) and k not in secret_names and k != "enabled"}
    secrets_set = sorted([k for k in raw.keys() if k in secret_names and raw.get(k)])
    secret_previews = {k: _preview(str(raw[k])) for k in secrets_set}
    return ObservabilityRecord(
        slug=slug,
        configured=bool(raw),
        enabled=bool(raw.get("enabled")),
        values=values,
        secrets_set=secrets_set,
        secret_previews=secret_previews,
    )


def _preview(value: str) -> str:
    if len(value) <= 6:
        return "*" * len(value)
    return f"{value[:3]}...{value[-3:]}"


# ---------- routes ----------


@router.get("/observability", response_model=ObservabilityListResponse)
async def list_observability(
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(require_admin),
) -> ObservabilityListResponse:
    raws = await list_observability_providers(session)
    return ObservabilityListResponse(
        catalog=[_catalog_item(slug) for slug in OBSERVABILITY_CATALOG],
        records={slug: _project_record(slug, raws.get(slug, {})) for slug in OBSERVABILITY_CATALOG},
    )


@router.get("/observability/{slug}", response_model=ObservabilityRecord)
async def get_observability(
    slug: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(require_admin),
) -> ObservabilityRecord:
    if slug not in OBSERVABILITY_CATALOG:
        raise HTTPException(status_code=404, detail=f"Unknown observability provider '{slug}'.")
    raw = await get_observability_provider(session, slug)
    return _project_record(slug, raw)


@router.put("/observability/{slug}", response_model=ObservabilityRecord)
async def upsert_observability(
    slug: str,
    payload: ObservabilityUpdateRequest,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(require_admin),
) -> ObservabilityRecord:
    if slug not in OBSERVABILITY_CATALOG:
        raise HTTPException(status_code=404, detail=f"Unknown observability provider '{slug}'.")
    try:
        await update_observability_provider(session, slug, payload.values)
    except KeyError as exc:
        raise HTTPException(status_code=400, detail=f"Unknown provider {exc}.") from exc
    await session.commit()
    invalidate_cache()
    raw = await get_observability_provider(session, slug)
    return _project_record(slug, raw)


@router.delete("/observability/{slug}", response_model=ObservabilityRecord)
async def disable_observability(
    slug: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(require_admin),
) -> ObservabilityRecord:
    if slug not in OBSERVABILITY_CATALOG:
        raise HTTPException(status_code=404, detail=f"Unknown observability provider '{slug}'.")
    await update_observability_provider(session, slug, {"enabled": False})
    await session.commit()
    invalidate_cache()
    raw = await get_observability_provider(session, slug)
    return _project_record(slug, raw)


@router.post("/observability/{slug}/test", response_model=ObservabilityTestResponse)
async def test_observability(
    slug: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(require_admin),
) -> ObservabilityTestResponse:
    if slug not in OBSERVABILITY_CATALOG:
        raise HTTPException(status_code=404, detail=f"Unknown observability provider '{slug}'.")
    raw = await get_observability_provider(session, slug)
    if not raw:
        return ObservabilityTestResponse(status="not_configured", message="No configuration saved.")
    if not raw.get("enabled"):
        return ObservabilityTestResponse(
            status="not_configured",
            message="Configuration exists but integration is disabled.",
        )
    sink = resolve_sink(raw)
    if sink is None:
        return ObservabilityTestResponse(
            status="error",
            message="Configuration is incomplete or invalid.",
        )
    if not sink.is_available():
        return ObservabilityTestResponse(
            status="sdk_missing",
            message="Langfuse SDK not installed. Install with: pip install dataclaw[evals]",
        )
    probe = sink.create_trace_id(seed="dataclaw-test")
    if probe is None:
        return ObservabilityTestResponse(
            status="error",
            message="Could not initialise the integration client.",
        )
    return ObservabilityTestResponse(status="ok", message="Integration reachable.")
