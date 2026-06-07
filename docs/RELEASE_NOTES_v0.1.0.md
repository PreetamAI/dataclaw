# DataClaw v0.1.0 Release Notes

First public release. The whole product runs from one `pipx install`.

## Install

```bash
pipx install dataclaw-platform
dataclaw init
dataclaw start
```

Browser opens at `http://127.0.0.1:8000`. The SQLite demo connector is pre-configured with `customers / orders / products` tables.

For multi-container deployments:

```bash
git clone https://github.com/svangapally81/dataclaw.git
cd dataclaw
cp .env.example .env

# Generate real secrets (placeholders in .env.example are rejected at startup)
python3 -c "from cryptography.fernet import Fernet; print('MASTER_KEY=' + Fernet.generate_key().decode())" >> .env
python3 -c "import secrets; print('SESSION_SECRET=' + secrets.token_urlsafe(48))" >> .env
# Open .env and remove the original placeholder MASTER_KEY/SESSION_SECRET lines

docker compose up -d
```

## What's in v0.1.0

- **Bundled UI + API + worker + embedded Chroma** running in one process for local installs
- **21 connectors** across data stores, knowledge bases, ETL/orchestration, and LLM providers
- **Per-connector MCP grants** with explicit read/write scoping per agent
- **Destructive-write approval flow** with `agent_write_audit` history
- **11 built-in agents** (chat, docs, compile, alerting, data-quality, freshness, ingestion, reconciliation, metadata, lineage) plus a custom-agent builder
- **Knowledge graph compile** from wiki frontmatter + `[[wiki-links]]` + warehouse metadata
- **OpenAI and Ollama** LLM providers via OpenAI-compatible plumbing

## Connector stability

| Tier | Count | Connectors |
|------|------:|------------|
| 🟢 Stable | 15 | `postgres`, `mysql`, `sql_server`, `trino`, `bigquery`, `snowflake`, `databricks`, `sqlite`, `notion`, `confluence`, `github`, `dbt`, `prefect`, `dagster`, `airbyte` |
| 🔵 Stable (read-only) | 1 | `fivetran` (write tools approval-gated, not executed live) |
| 🟡 Beta | 1 | `airflow` (sandbox infrastructure flakiness) |
| 🔴 Known issue | 1 | `redshift` (psycopg3 driver bug - opt in with `EXPERIMENTAL_ENABLE_redshift=true`) |
| 🚫 Unsupported | 2 | `quip` (vendor discontinued), `google_docs` (OAuth flow not wired) |

Stable tiers are backed by live MCP tool coverage runs against real backends. Every connector's read and write tools were exercised against either a Docker fixture container (databases, orchestrators) or a real SaaS sandbox account (Notion, GitHub, Confluence, BigQuery, Snowflake, Databricks, Fivetran).

Promotion rule going forward: a connector graduates from `beta` to `stable` when its coverage shard passes 5 consecutive nightly runs of the `acme-rig` workflow.

## Known limitations

- **App database**: SQLite only. Local single-node deployments only.
- **Worker**: single APScheduler instance. Running multiple workers duplicates scheduled syncs.
- **Chat agent multi-store routing**: when a question requires tools across 4+ connectors at once, the LLM occasionally picks unexpected tools. Direct MCP tool access via `/mcp/{slug}/tools/{tool}` is the deterministic path. Chat improvements are tracked in the roadmap.
- **OpenAI cost**: the chat agent uses real OpenAI calls. Configure Ollama in Settings → LLM Providers for fully local, zero-cost operation.

## Where to start

- [`docs/CONNECTOR_MATRIX.md`](./CONNECTOR_MATRIX.md) - per-connector tool reference
- [`docs/FLOW_BRIEF.md`](./FLOW_BRIEF.md) - architecture in one page
- [`docs/TESTER_GUIDE.md`](./TESTER_GUIDE.md) - manual validation guide for contributors
- [`docs/UI_TEST_SCRIPT.md`](./UI_TEST_SCRIPT.md) - copy-paste chat prompts to exercise every flow
- [`README.md`](../README.md) - full quickstart, Ollama setup, and Docker Compose paths
