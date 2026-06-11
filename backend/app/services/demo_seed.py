from sqlalchemy import select
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.config import get_settings
from app.core.security import encrypt_json, password_hash
from app.models.domain import Agent, AgentMcpGrant, Connector, User, Workspace
from app.services.connectors.catalog import catalog
from app.services.settings_store import update_llm_provider

BUILTIN_AGENTS = [
    {
        "name": "chat",
        "display_name": "Chat",
        "kind": "on_demand",
        "icon_key": "bot",
        "prompt": "Answer questions, use granted tools, and explain actions clearly.",
    },
    {
        "name": "docs",
        "display_name": "Docs",
        "kind": "background",
        "icon_key": "file-text",
        "prompt": "Ground data documentation in schemas and knowledge sources.",
        "cadence_minutes": 60,
    },
    {
        "name": "compile-agent",
        "display_name": "Compile Agent",
        "kind": "background",
        "icon_key": "brain",
        "prompt": "Rebuild the internal DataClaw knowledge graph from wiki pages when content changes.",
        "cadence_minutes": 2,
    },
    {
        "name": "compiling",
        "display_name": "Compiling",
        "kind": "on_demand",
        "icon_key": "brain",
        "prompt": "Rebuild the internal DataClaw knowledge graph from wiki pages.",
    },
    {
        "name": "alerting",
        "display_name": "Alerting",
        "kind": "background",
        "icon_key": "activity",
        "prompt": "Detect failed orchestration runs and suppress noisy borderline signals.",
        "cadence_minutes": 5,
        "uses_llm_filter": True,
    },
    {
        "name": "data_quality",
        "display_name": "Data Quality",
        "kind": "background",
        "icon_key": "shield-check",
        "prompt": "Detect schema drift and unusually expensive queries.",
        "cadence_minutes": 30,
        "thresholds": {"duration_ms": 5000, "rows_returned": 10000},
    },
    {
        "name": "freshness",
        "display_name": "Freshness",
        "kind": "background",
        "icon_key": "activity",
        "prompt": "Monitor stale or empty synced tables.",
        "cadence_minutes": 10,
    },
    {
        "name": "ingestion",
        "display_name": "Ingestion",
        "kind": "background",
        "icon_key": "database",
        "prompt": "Refresh configured connectors, wiki pages, and Chroma embeddings.",
        "cadence_minutes": 360,
    },
    {
        "name": "reconciliation",
        "display_name": "Reconciliation",
        "kind": "background",
        "icon_key": "git-branch",
        "prompt": "Reconcile disk-edited wiki files back into DataClaw.",
        "cadence_minutes": 60,
    },
    {
        "name": "metadata",
        "display_name": "Metadata",
        "kind": "background",
        "icon_key": "sparkles",
        "prompt": "Document schemas and improve metadata quality.",
        "cadence_minutes": 30,
        "hidden": True,
    },
    {
        "name": "lineage",
        "display_name": "Lineage",
        "kind": "background",
        "icon_key": "git-branch",
        "prompt": "Infer and maintain lineage relationships.",
        "cadence_minutes": 30,
        "hidden": True,
    },
]


async def _ensure_builtin_agents(session: AsyncSession, workspace: Workspace, user: User) -> None:
    definitions = catalog()
    configured_slugs = {
        connector.slug
        for connector in (
            await session.scalars(
                select(Connector).where(
                    Connector.workspace_id == workspace.id,
                    Connector.credential_state == "configured",
                )
            )
        ).all()
    }
    for definition_row in BUILTIN_AGENTS:
        name = definition_row["name"]
        agent = await session.scalar(select(Agent).where(Agent.workspace_id == workspace.id, Agent.name == name))
        if agent is None:
            agent = Agent(
                workspace_id=workspace.id,
                name=name,
                display_name=definition_row["display_name"],
                system_prompt=definition_row["prompt"],
                kind=definition_row["kind"],
                is_system=True,
                enabled=True,
                icon_key=definition_row["icon_key"],
                cadence_minutes=definition_row.get("cadence_minutes"),
                thresholds=definition_row.get("thresholds", {}),
                uses_llm_filter=definition_row.get("uses_llm_filter", False),
                created_by=user.id,
            )
            session.add(agent)
            await session.flush()
        else:
            agent.kind = definition_row["kind"]
            agent.cadence_minutes = definition_row.get("cadence_minutes")
            if agent.thresholds is None:
                agent.thresholds = definition_row.get("thresholds", {})
            agent.uses_llm_filter = bool(definition_row.get("uses_llm_filter", agent.uses_llm_filter))

        grants = (await session.scalars(select(AgentMcpGrant).where(AgentMcpGrant.agent_id == agent.id))).all()
        grant_by_slug = {grant.connector_slug: grant for grant in grants}
        if name == "chat":
            for grant in grant_by_slug.values():
                if grant.connector_slug != "openai" and grant.connector_slug not in configured_slugs:
                    grant.read_enabled = False
                    grant.write_enabled = False
                elif grant.connector_slug in configured_slugs and not grant.write_enabled:
                    # Writes are always approval-gated, so auto-granting write
                    # to the chat agent on configured connectors is safe and
                    # removes a hidden manual step for testers.
                    grant.write_enabled = True
        for definition in definitions:
            if definition.slug in grant_by_slug:
                continue
            configured = definition.slug == "openai" or definition.slug in configured_slugs
            chat_write = name == "chat" and configured and definition.slug != "openai"
            session.add(
                AgentMcpGrant(
                    agent_id=agent.id,
                    connector_slug=definition.slug,
                    read_enabled=name == "chat" and configured,
                    write_enabled=chat_write,
                )
            )


async def _ensure_admin_user(session: AsyncSession) -> User:
    settings = get_settings()
    user = await session.scalar(select(User).where(User.email == settings.admin_email))
    if user is not None:
        return user
    user = User(
        email=settings.admin_email,
        password_hash=password_hash(settings.admin_password),
        is_admin=True,
    )
    session.add(user)
    await session.flush()
    return user


async def _ensure_workspace(session: AsyncSession) -> Workspace:
    workspace = await session.scalar(select(Workspace).limit(1))
    if workspace is not None:
        return workspace
    workspace = Workspace(name="DataClaw", onboarding_complete=False)
    session.add(workspace)
    await session.flush()
    return workspace


async def _ensure_connectors(session: AsyncSession, workspace: Workspace) -> None:
    settings = get_settings()
    existing_slugs = {
        row
        for row in (
            await session.scalars(
                select(Connector.slug).where(Connector.workspace_id == workspace.id)
            )
        ).all()
    }
    for definition in catalog():
        if definition.slug in existing_slugs:
            continue
        status = definition.local_verification.value
        if definition.slug == "openai" and not settings.openai_api_key:
            status = "not_configured"
        session.add(
            Connector(
                workspace_id=workspace.id,
                slug=definition.slug,
                category=definition.category.value,
                display_name=definition.display_name,
                status=status,
                credential_state="not_configured",
                sync_summary={"behavior": definition.sync_behavior},
            )
        )
    await session.flush()
    await _ensure_sqlite_demo_connector(session, workspace, settings)


async def _ensure_sqlite_demo_connector(session: AsyncSession, workspace: Workspace, settings) -> None:
    connector = await session.scalar(
        select(Connector).where(Connector.workspace_id == workspace.id, Connector.slug == "sqlite")
    )
    if connector is None or connector.encrypted_credentials:
        return
    url = make_url(settings.demo_database_url)
    # SQLAlchemy's make_url turns "sqlite:///:memory:" into database="/:memory:"
    # (the leading "/" is the path separator). Strip it before comparing so
    # the in-memory sentinel is honoured regardless of how it was written.
    database = (url.database or "").lstrip("/")
    if not url.drivername.startswith("sqlite") or not database or database == ":memory:":
        return
    connector.encrypted_credentials = encrypt_json(
        settings.master_key,
        {"database_path": url.database},
    )
    connector.credential_state = "configured"
    connector.status = "ok"


async def seed_demo(session: AsyncSession) -> None:
    """Idempotent local-install seed.

    Always ensures a workspace, the admin user, the connector catalog
    rows, the built-in agents, and (when OPENAI_API_KEY is set) the
    OpenAI provider exist. Safe to run on every startup.
    """
    settings = get_settings()
    workspace = await _ensure_workspace(session)
    user = await _ensure_admin_user(session)
    await _ensure_connectors(session, workspace)
    if settings.openai_api_key:
        await update_llm_provider(
            session,
            "openai",
            {
                "api_key": settings.openai_api_key,
                "model": settings.openai_model,
                "embedding_model": "text-embedding-3-small",
            },
        )
    await _ensure_builtin_agents(session, workspace, user)
    await session.commit()
