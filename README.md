# DataClaw

[![License: Apache 2.0](https://img.shields.io/badge/license-Apache_2.0-blue.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://www.python.org/)
[![Node 22+](https://img.shields.io/badge/node-22%2B-green.svg)](https://nodejs.org/)
[![Local LLM via Ollama](https://img.shields.io/badge/local%20LLM-Ollama-purple.svg)](#run-it-fully-local-with-ollama)

DataClaw is a self-hosted, AI-native data platform. Connect your warehouses, pipelines, and docs behind explicit MCP grants. A team of agents - chat, docs, alerting, freshness, data quality, ingestion, and your own custom agents - work over a knowledge graph compiled from your sources. Bring any LLM (OpenAI, Anthropic) or run fully local with Ollama.

```
┌─────────────────┐    ┌──────────────┐    ┌──────────────────────────┐
│  React frontend │───►│ FastAPI API  │───►│  Connectors              │
│  (Vite + RTK)   │    │  + agents    │    │  Postgres, MySQL, SQLite │
└─────────────────┘    │  + chat      │    │  Snowflake, BigQuery,    │
                       │  + MCP grants│    │  Redshift, Databricks,   │
                       │  + Chroma    │    │  SQL Server, Airflow,    │
                       │  + Postgres  │    │  dbt, Prefect, Dagster,  │
                       │  worker loop │    │  Airbyte, Fivetran,      │
                       └──────┬───────┘    │  Notion, Google Docs,    │
                              │            │  Quip, Confluence, GitHub│
                              ▼            └──────────────────────────┘
                       ┌──────────────────┐
                       │  LLM provider    │
                       │  OpenAI / Ollama │
                       └──────────────────┘
```

## Highlights

- **21 connectors** across data stores, knowledge bases, ETL/orchestration, and LLM providers - all behind explicit per-agent MCP read/write grants where tools exist.
- **Unified agent model**: every agent (on-demand chat/docs/compiling/custom + background alerting/freshness/data-quality/ingestion/reconciler/custom) shares the same shape - name, system prompt, MCP grants, enabled toggle, cadence (background only).
- **Custom agents in 30 seconds** - create your own background or on-demand agent in the UI: pick a prompt or SQL + connectors + grants + cadence.
- **Local LLM via Ollama** - same UX, your hardware, zero per-token cost. Default models: `llama3.1:8b` chat + `nomic-embed-text` embeddings. Swap to OpenAI or any OpenAI-compatible endpoint with one config change.
- **Knowledge graph** compiled from your wiki + warehouse metadata + DAG state + lineage. Agents cite back to it instead of hallucinating.
- **Write safety + audit** - destructive SQL creates approval-required alerts; every executed write logged in `agent_write_audit`.
- **Evals + golden queries** - measure answer quality, capture canonical SQL, and turn 👍'd chat answers into regression cases. Deterministic metrics work out of the box; optional LLM-judge metrics via the `[evals]` extra.
- **Self-hosted, open-source** - runs on your laptop, your VPS, or your VPC. No data leaves your network.

## 🎥 Demo

<div align="center">
  <a href="https://youtu.be/YCmWXT2Zgio">
    <img src="https://img.youtube.com/vi/YCmWXT2Zgio/maxresdefault.jpg" alt="DataClaw Demo" width="900">
  </a>
  <br/>
  <a href="https://youtu.be/YCmWXT2Zgio"><strong>▶ Watch the DataClaw Demo</strong></a>
</div>

## Quickstart

### Prerequisites

- **Python 3.12+** (3.13 also fine)
- **`pipx`** for the recommended install: `brew install pipx` on macOS, `python3 -m pip install --user pipx` elsewhere
- **Docker Desktop** if you choose the Docker Compose path
- **Node 22+** + **`uv`** only if you're developing from source

### Recommended: `pipx`

```bash
pipx install dataclaw-platform
dataclaw init
dataclaw start
```

`dataclaw init` generates a fresh `~/.dataclaw/.env` with a unique Fernet `MASTER_KEY` and session secret. It starts the bundled UI, FastAPI backend, embedded APScheduler worker, SQLite app database, demo SQLite database, and persistent Chroma under `~/.dataclaw/`. Auth is disabled by default so the UI opens straight to the workspace. For hosted deployments, flip `DATACLAW_AUTH_DISABLED=false` and configure `ADMIN_EMAIL` / `ADMIN_PASSWORD`.

> Note: the PyPI package is named `dataclaw-platform`; the CLI command is `dataclaw`.

### Other install paths

**Docker Compose** - multi-container deployment with a dedicated Chroma service and separate worker:

```bash
git clone https://github.com/svangapally81/dataclaw.git
cd dataclaw
cp .env.example .env

# Generate real secrets (placeholders in .env.example are rejected at startup)
python3 -c "from cryptography.fernet import Fernet; print('MASTER_KEY=' + Fernet.generate_key().decode())" >> .env
python3 -c "import secrets; print('SESSION_SECRET=' + secrets.token_urlsafe(48))" >> .env
# Then open .env and remove the two original placeholder MASTER_KEY/SESSION_SECRET lines

docker compose up -d
```

UI at `http://localhost:8000`. Stop with `docker compose down`.

**From source** (contributors only):

```bash
git clone https://github.com/svangapally81/dataclaw.git
cd dataclaw
make install         # backend (uv sync) + frontend (npm ci)
make dev             # starts backend + frontend (Vite :5173) + worker + ChromaDB
```

Use `make dev` when iterating on the React UI (hot reload). For a release-style local run from source, `make quickstart` is the equivalent of the pipx path.

### First-run checklist

1. **Settings → LLM provider** - pick Ollama (no cloud key needed) or OpenAI/Anthropic. See [Ollama setup](#run-it-fully-local-with-ollama) below.
2. **Connectors** - configure at least one data source. Postgres / MySQL / SQLite need just host + creds. SaaS connectors need a vendor token.
3. **Knowledge → Brain** - the background compile agent rebuilds the graph automatically after connector/wiki changes.
4. **Editor** - ask anything: *"how many customers do we have?"*, *"which DAG produces ad_spend_daily?"*, *"summarize last week's alerts."*
5. **Agents → Background** - toggle on the alerting agent for your ETL connectors. Failed DAGs / dbt tests now surface in Observability.

## Run it fully local with Ollama

DataClaw speaks the OpenAI API. Point it at a local Ollama daemon and the whole stack - chat, summarizer, embeddings, alerting LLM filter - runs on your laptop. Zero cloud key. Zero data leaves your network.

```bash
# 1. Install Ollama
brew install ollama && brew services start ollama   # macOS
# or: https://ollama.com/download

# 2. Pull the two models DataClaw uses
ollama pull llama3.1:8b           # chat model (~5 GB, ~16 GB RAM)
ollama pull nomic-embed-text      # embeddings (~274 MB)

# 3. Verify
curl http://localhost:11434/api/tags     # should list both models
```

Then in DataClaw:

1. **Settings → LLM provider → Ollama (local)**
2. Base URL `http://localhost:11434/v1` · Model `llama3.1:8b` · Embedding `nomic-embed-text`
3. **Save** → **Test connection**

To pin Ollama as the active provider for background agents and ingestion, also set `DATACLAW_LLM_PROVIDER=ollama` in `.env`.

| Tier | Chat model | Disk | RAM | When to pick |
|---|---|---|---|---|
| Lightweight | `llama3.2:3b` / `qwen2.5:3b` | ~2 GB | 8 GB | Base M1 / 8 GB laptops, simple aggregations only |
| **Standard** | `llama3.1:8b` (default) | ~5 GB | **16 GB** | Most users - reliable tool calling, solid SQL |
| High quality | `qwen2.5:14b` / `qwen2.5-coder:14b` | ~9 GB | 32 GB | M1/M2 Pro/Max - cloud-quality SQL (CTEs, windows) |

Full LLM-provider guide at [getdataclaw.xyz/docs/llm-providers](https://getdataclaw.xyz/docs/llm-providers).

## Connectors

The connector catalog includes 21 slugs. The source of truth is `backend/app/services/connectors/catalog.py`; the generated reference table lives in `docs/CONNECTOR_MATRIX.md`. The Connectors tab in the UI surfaces the same catalog.

Stability tiers in v0.1.0 (all backed by 5/5 live MCP tool coverage runs except where noted):

| Tier | Count | Connectors |
|---|---|---|
| 🟢 Stable | 15 | `postgres`, `mysql`, `sql_server`, `trino`, `bigquery`, `snowflake`, `databricks`, `sqlite`, `notion`, `confluence`, `github`, `dbt`, `prefect`, `dagster`, `airbyte` |
| 🔵 Stable (read-only) | 1 | `fivetran` (write tools approval-gated, not executed live) |
| 🟡 Beta | 1 | `airflow` (4/5 coverage runs - one sandbox infra failure) |
| 🔴 Known issue | 1 | `redshift` (psycopg3 driver bug - opt in with `EXPERIMENTAL_ENABLE_redshift=true`) |
| 🚫 Unsupported | 2 | `quip` (Salesforce discontinued the API), `google_docs` (OAuth flow not wired) |

See `docs/CONNECTOR_MATRIX.md` for per-connector tool coverage and `docs/RELEASE_NOTES_v0.1.0.md` for the stability evidence behind each tier.

## Agents

Two kinds, both behind explicit MCP grants:

### On-demand (LLM-driven, triggered by user or system)

| Agent | Default behavior |
|---|---|
| `chat-agent` | Q&A over the wiki + graph + connectors. The agent every user touches first. |
| `docs-agent` | Generates table/DAG documentation on demand. |
| `compiling-agent` | Rebuilds the knowledge graph from wiki pages. |
| `+ Custom` | Free-form: pick a prompt + connectors + grants. |

### Background (scheduled, cadence-driven)

| Agent | Cadence | Locked to | What it does |
|---|---|---|---|
| `alerting-agent` | 5m | ETL connectors | Polls for failed runs/tests. Rule-driven → optional LLM filter → creates `Alert`. |
| `data-quality-agent` | 30m | Data stores | Schema drift + query cost monitoring. |
| `freshness-agent` | 10m | Data stores | Flags stale tables against SLA. |
| `ingestion-agent` | 6h | All `fetch_content` connectors | Pulls new content, summarizes, embeds in Chroma. |
| `reconciler-agent` | 1h | Internal | Wiki disk → SQLite reconciler. |
| `+ Custom` | User picks | User picks | User-supplied SQL/prompt + threshold + cadence. |

Manage everything in the **Agents** tab. The API exposes `GET /agents?kind=…`, `POST /agents`, `PATCH /agents/{id}`, `GET/PUT /agents/{id}/grants`.

## Evals

Measure whether the agent is actually answering correctly, and let good answers compound.

- **Golden queries** - mark a question's SQL as canonical. The chat agent prefers it over generating fresh SQL. Lifecycle: `candidate → approved → golden` (promotion to `golden` is always an explicit human step).
- **Eval cases from your workspace** - producers auto-generate candidates from schemas, the knowledge graph, lineage, demo fixtures, and **endorsed chat history** (a 👍'd answer with SQL and no 👎 becomes a regression case).
- **Metrics** - deterministic graders (SQL correctness, result accuracy, citations, connector/tool routing, cost/latency) run on the base install. Optional **LLM-judge** metrics (faithfulness, answer relevancy, safety) run when the `[evals]` extra is installed and any LLM provider is configured - including local Ollama.
- **Suggestions** - a failing run can propose a golden query, a prompt tweak, or a workspace rule; applying one is human-reviewed.

Install the optional extra for LLM-judge metrics and the optional Langfuse trace sink:

```bash
pipx install 'dataclaw-platform[evals]'
```

The `[evals]` extra is **not** required to use evals - the deterministic metrics work without it. It is also **not** a Langfuse account: Langfuse is an optional, off-by-default trace sink you configure only if you want to forward agent traces to Langfuse (cloud or self-hosted). See `docs/RELEASE_NOTES_v0.2.0.md` and [getdataclaw.xyz/docs/evals](https://getdataclaw.xyz/docs/evals).

Manage everything in the **Evals** tab. The API is mounted under `/evals` (cases, runs, suggestions, config).

## MCP, approval, and audit

Every connector slug has a mounted MCP endpoint under `/mcp/{slug}` plus a REST-compatible tool endpoint at `/mcp/{slug}/tools/{tool_name}`. Tool names are scoped by prefix:

- `read_*` requires the agent's read grant for that connector.
- `write_*` requires the write grant.

Allowed write SQL executes immediately. Destructive SQL (`DROP TABLE`, `TRUNCATE`, unbounded `DELETE`/`UPDATE`) creates an approval-required alert and does not run until `POST /alerts/{id}/approve-and-execute`. Every executed write is logged to operational logs and `agent_write_audit`.

## Verification

```bash
# Static checks + unit tests
make lint
make test
make build

# Live integration suite (real Docker: Postgres, MySQL, Airflow, dbt, Prefect, Dagster, Airbyte)
make test-integration

# End-to-end with real OpenAI (requires OPENAI_API_KEY in .env)
RUN_OPENAI_E2E=1 .venv/bin/python -m pytest backend/tests/integration/test_e2e_v01.py -q

# End-to-end with local Ollama
DATACLAW_LLM_PROVIDER=ollama .venv/bin/python -m pytest backend/tests/integration/test_e2e_v01.py -q
```

Browser smoke tests:

```bash
cd frontend
npx playwright test tests/e2e/agents-observability.spec.ts
```

## Make targets

```bash
make help              # list everything
make install           # backend (uv) + frontend (npm) deps
make dev               # backend + frontend in foreground
make test              # unit/contract tests
make integration-up    # Docker compose for live connector services
make test-integration  # integration-up → adapter tests → tear down
make integration-e2e   # integration-up → chat-agent E2E → tear down
make lint              # ruff + tsc
make build             # production frontend bundle
make clean             # caches, dist, demo sqlite files
```

## Production checklist

- Set `MASTER_KEY` and `SESSION_SECRET` to long random values per environment.
- Remove `DATACLAW_AUTH_DISABLED=true` and require real admin login.
- Pick an LLM provider in `Settings → LLM provider`. For zero-cost local, run Ollama and set `DATACLAW_LLM_PROVIDER=ollama`.
- Configure connector credentials per environment.
- Front the API with TLS and restrict `cors_origins` to your frontend domains.

## Repository layout

```
dataclaw/
├── backend/                       # FastAPI + agents + connectors + worker
│   ├── app/
│   │   ├── api/                   # auth deps
│   │   ├── core/                  # config, security (Fernet), structured logging
│   │   ├── db/                    # SQLAlchemy + alembic
│   │   ├── models/                # User, Workspace, Connector, Agent, Alert, ChatThread, …
│   │   ├── services/
│   │   │   ├── agents/            # chat, docs, metadata, lineage, freshness,
│   │   │   │                      # background_runner, alert_llm_filter
│   │   │   ├── connectors/        # 21-slug catalog + adapters
│   │   │   ├── ingestion/         # service.py + summarizer.py
│   │   │   ├── knowledge_compile/ # wiki → graph
│   │   │   ├── llm_catalog.py     # OpenAI + Ollama provider definitions
│   │   │   ├── mcp_*.py           # MCP catalog / executor / servers
│   │   │   ├── settings_store.py  # resolve_openai (handles base_url for Ollama)
│   │   │   └── sync_materializer.py
│   │   ├── worker/                # single-loop background dispatcher
│   │   └── main.py
│   ├── alembic/versions/          # 0001–0023 (0012 = unified agents, 0020–0023 = evals)
│   └── tests/                     # unit + contract + integration
├── frontend/                      # React + RTK Query
│   └── src/
│       ├── components/            # Sidebar, Workspace, IDE, Agents (tabbed),
│       │                          # BackgroundAgents, AgentCard, CustomAgentModal,
│       │                          # ConfigureModal, Connectors, …
│       ├── lib/                   # catalog, errors
│       ├── services/api.ts        # RTK Query client
│       └── styles/app.css
├── docs/                          # release notes, connector matrix, contributor guides, screenshots
├── logos/                         # canonical brand assets
├── tests/integration/             # docker-compose + seed scripts for the acme-rig test stack
└── Makefile
```

## Contributing & security

- See `CONTRIBUTING.md` for development guidance.
- See `SECURITY.md` for the security model and vulnerability reporting.
- Released under Apache-2.0. See `LICENSE` and `NOTICE`.
