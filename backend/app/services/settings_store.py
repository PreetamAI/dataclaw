from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.security import decrypt_json, encrypt_json
from app.models.domain import AppSetting
from app.services.llm_catalog import CATALOG_BY_SLUG


def _llm_key(slug: str) -> str:
    return f"llm:{slug}"


def _observability_key(slug: str) -> str:
    return f"observability:{slug}"


# Catalog of optional observability integrations. Keeping this inline (rather
# than a separate registry module) until we add more than one provider — the
# field shape mirrors LlmCatalogItem so the frontend can reuse ConfigureModal.
OBSERVABILITY_CATALOG: dict[str, dict[str, Any]] = {
    "langfuse": {
        "slug": "langfuse",
        "display_name": "Langfuse",
        "docs_url": "https://langfuse.com/docs/tracing",
        "description": (
            "Optional secondary trace sink. Chat traces are always written to the "
            "local span store; enabling Langfuse forwards them to a Langfuse "
            "Cloud or self-hosted project for richer trace exploration."
        ),
        "fields": [
            {
                "name": "host",
                "label": "Host",
                "secret": False,
                "required": True,
                "placeholder": "https://cloud.langfuse.com",
            },
            {
                "name": "public_key",
                "label": "Public key",
                "secret": False,
                "required": True,
                "placeholder": "pk-lf-...",
            },
            {
                "name": "secret_key",
                "label": "Secret key",
                "secret": True,
                "required": True,
                "placeholder": "sk-lf-...",
            },
            {
                "name": "project",
                "label": "Project ID",
                "secret": False,
                "required": False,
                "placeholder": "cm... (Settings → General → Debug → project.id in Langfuse)",
            },
        ],
    },
}


async def _read(session: AsyncSession, key: str) -> dict[str, Any]:
    row = await session.get(AppSetting, key)
    if row is None:
        return {}
    return decrypt_json(get_settings().master_key, row.encrypted_value)


async def _write(session: AsyncSession, key: str, payload: dict[str, Any]) -> AppSetting:
    encrypted = encrypt_json(get_settings().master_key, payload)
    row = await session.get(AppSetting, key)
    if row is None:
        row = AppSetting(key=key, encrypted_value=encrypted)
        session.add(row)
    else:
        row.encrypted_value = encrypted
    await session.flush()
    return row


async def get_llm_provider(session: AsyncSession, slug: str) -> dict[str, Any]:
    return await _read(session, _llm_key(slug))


async def list_llm_providers(session: AsyncSession) -> dict[str, dict[str, Any]]:
    return {
        slug: await _read(session, _llm_key(slug))
        for slug in CATALOG_BY_SLUG
    }


async def update_llm_provider(
    session: AsyncSession,
    slug: str,
    values: dict[str, Any],
) -> AppSetting:
    if slug not in CATALOG_BY_SLUG:
        raise KeyError(slug)
    current = await _read(session, _llm_key(slug))
    for field in CATALOG_BY_SLUG[slug].fields:
        if field.name not in values:
            continue
        incoming = values[field.name]
        if incoming is None:
            current.pop(field.name, None)
            continue
        stripped = str(incoming).strip()
        if stripped:
            current[field.name] = stripped
        else:
            current.pop(field.name, None)
    return await _write(session, _llm_key(slug), current)


def active_llm_provider_slug() -> str:
    settings = get_settings()
    slug = settings.llm_provider if settings.llm_provider in CATALOG_BY_SLUG else "openai"
    if not CATALOG_BY_SLUG[slug].wired:
        return "openai"
    return slug


async def resolve_openai(session: AsyncSession) -> tuple[str | None, str | None, str | None, str | None]:
    settings = get_settings()
    slug = active_llm_provider_slug()
    stored = await get_llm_provider(session, slug)
    definition = CATALOG_BY_SLUG[slug]

    if slug == "ollama":
        api_key = stored.get("api_key") or "ollama-local"
        model = stored.get("model") or definition.default_model
        base_url = stored.get("base_url") or "http://localhost:11434/v1"
        embedding_model = stored.get("embedding_model") or definition.default_embedding_model
        return api_key, model, base_url, embedding_model

    api_key = stored.get("api_key")
    model = (stored.get("model") or settings.openai_model) if api_key else None
    embedding_model = stored.get("embedding_model") or definition.default_embedding_model
    return api_key, model, stored.get("base_url"), embedding_model


async def get_observability_provider(session: AsyncSession, slug: str) -> dict[str, Any]:
    return await _read(session, _observability_key(slug))


async def list_observability_providers(session: AsyncSession) -> dict[str, dict[str, Any]]:
    return {slug: await _read(session, _observability_key(slug)) for slug in OBSERVABILITY_CATALOG}


async def update_observability_provider(
    session: AsyncSession,
    slug: str,
    values: dict[str, Any],
) -> AppSetting:
    if slug not in OBSERVABILITY_CATALOG:
        raise KeyError(slug)
    current = await _read(session, _observability_key(slug))
    field_names = {field["name"] for field in OBSERVABILITY_CATALOG[slug]["fields"]}
    field_names.add("enabled")  # toggle without re-entering credentials
    for name in field_names:
        if name not in values:
            continue
        incoming = values[name]
        if incoming is None or incoming is False:
            if name == "enabled":
                current["enabled"] = False
            else:
                current.pop(name, None)
            continue
        if isinstance(incoming, bool):
            current[name] = incoming
            continue
        stripped = str(incoming).strip()
        if stripped:
            current[name] = stripped
        else:
            current.pop(name, None)
    return await _write(session, _observability_key(slug), current)


async def hydrate_vector_store(session: AsyncSession, workspace_id: str) -> None:
    api_key, _model, base_url, embedding_model = await resolve_openai(session)
    from app.services.vector_store import vector_store

    vector_store.ensure_embedding_model(workspace_id, embedding_model, api_key=api_key, base_url=base_url)
