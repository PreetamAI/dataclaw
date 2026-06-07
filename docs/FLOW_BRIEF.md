# DataClaw - Flow Brief

One page. What the product does, how it's wired, and where to look when something breaks.

## What it is

DataClaw is an **agentic layer over the modern data stack**. It connects to warehouses (Snowflake, BigQuery, Databricks, Redshift), operational DBs (Postgres, MySQL, SQL Server), orchestrators (Airflow, Prefect, Dagster, dbt, Fivetran, Airbyte), and knowledge sources (Notion, Confluence, GitHub) - then lets LLM-driven agents safely query and act on that data, with every write gated by operator approval and tracked in an audit log.

## The three loops

```
┌─ INGESTION (offline) ──────────────┐  ┌─ CHAT / AGENTS (online) ──────────┐  ┌─ APPROVALS (operator) ──┐
│ Connector.sync()                   │  │ User question or schedule         │  │ Alert appears in        │
│  └─> adapter pulls schema + docs   │  │  └─> chat orchestrator            │  │ Gateway → Observability │
│      └─> chunker splits text       │  │      ├─> retrieval (Chroma)       │  │  └─> Approve / Reject   │
│          └─> embedder (OpenAI or   │  │      ├─> LLM with tool calling    │  │      └─> execute MCP    │
│              local SentenceXfmr)   │  │      │   └─> MCP tool dispatch    │  │          tool + audit   │
│              └─> Chroma + Postgres │  │      │       ├─> read tools run   │  │                         │
│                  (vector + meta)   │  │      │       └─> write tools     │  │                         │
│                                    │  │      │           pause → alert    │  │                         │
│                                    │  │      └─> answer + citations       │  │                         │
└────────────────────────────────────┘  └───────────────────────────────────┘  └─────────────────────────┘
```

## Module map (where each piece lives)

| Concern | File |
|---|---|
| HTTP routes (auth, connectors, chat, alerts, agents) | `backend/app/main.py` |
| Connector catalog (slug → display name + credential schema + stability) | `backend/app/services/connectors/catalog.py` |
| Connector adapters (sync, test, fetch_content per slug) | `backend/app/services/connectors/adapters.py` |
| MCP tool catalog (read_/write_ tool list per connector) | `backend/app/services/mcp_catalog.py` |
| MCP tool dispatcher + execution + per-vendor handlers | `backend/app/services/mcp_executor.py` |
| Chat orchestrator (LLM tool loop, deterministic fallbacks, retrieval) | `backend/app/services/agents/chat.py` |
| Background agent runner (cron-style scheduled agents) | `backend/app/services/agents/background_runner.py` |
| Vector store (Chroma + embedding selection) | `backend/app/services/vector_store.py` |
| Ingestion (chunker, summarizer, wiki store) | `backend/app/services/ingestion/` |
| Knowledge graph compile (lineage + node materialization) | `backend/app/services/knowledge_compile/` |
| LLM provider credential store | `backend/app/services/settings_store.py` |
| Sidebar + top-level pages | `frontend/src/components/Sidebar.tsx`, `Workspace.tsx` |
| Connector configure modal | `frontend/src/components/ConfigureModal.tsx` |
| Chat editor | `frontend/src/components/IDE.tsx` |
| Approval + alerts | `frontend/src/components/Gateway.tsx` |
| Brain / wiki view | `frontend/src/components/Knowledge.tsx`, `KnowledgeGraph.tsx` |

## The write-approval contract

Every `write_*` MCP tool path looks like this:

```
chat LLM proposes notion.write_append_to_page(page_id=…, body=…)
        │
        ▼
execute_mcp_tool()
        │
        ├─ resolve_granted_agent() - verifies agent has write_enabled on connector
        │
        ├─ _preflight_write_arguments() - NEW: validates resource IDs exist
        │   (e.g. Notion page_id; if 404, raises early with self-correction hint)
        │
        └─ if __approved flag absent:
            _pending_mcp_approval() - creates Alert(severity=critical, requires_approval=True)
                with detail containing MCP-Action / Agent-ID / Arguments
        else:
            _execute_mcp_tool_inner() - actually runs the vendor API call,
                records AgentWriteAudit row
```

Operator clicks **Approve** in Gateway → `POST /alerts/{id}/approve-and-execute` parses the alert detail and re-enters `execute_mcp_tool` with `__approved=True`. The route now wraps the call so vendor errors return HTTP 502, network errors return 504, and any other exception returns a logged 500 with a correlation id.

## Feature inventory (what you can actually do today)

| Capability | UI tab | Backed by |
|---|---|---|
| Connect data warehouse / DB / docs / orchestrator | Connectors | `services/connectors/adapters.py` per slug |
| See raw schema / pages / runs from a connector | Connectors → row → details | `Connector.last_sync_*` columns |
| Compile knowledge graph (entities + lineage + summaries) | Knowledge Base → Brain → Compile | `services/knowledge_compile/service.py` |
| Browse wiki pages generated from synced docs | Knowledge Base → Brain → Pages | `services/ingestion/wiki_store.py` |
| Inspect knowledge graph visually | Knowledge Base → Brain → Graph | `KnowledgeGraph.tsx` + `/knowledge/graph` |
| Ask chat questions with RAG citations | Editor → Chat | `services/agents/chat.py` |
| Generate charts in chat | Editor → Chat (auto when "trend"/"by month" detected) | `_should_generate_chart` |
| Run a write tool with operator approval | Editor → Chat → approve in Gateway | `_pending_mcp_approval` → `approve-and-execute` |
| Schedule background agents (alerts, freshness, quality, docs, ingestion, lineage, metadata) | Agents → Background | `services/agents/background_runner.py` |
| Run on-demand agents | Agents → On-demand | same runner, no cadence |
| See approval queue, alerts, recent runs | Gateway → Observability | `/alerts`, `/agent-runs`, `/events` |
| Per-agent MCP read/write grants | Agents → row → Configure | `AgentMcpGrant` model |
| Configure LLM provider (OpenAI / Ollama / Anthropic / etc) | Settings → LLM Provider | `services/settings_store.py` |

## Auto-grant rules (so you don't get bitten)

- **System agents** (`alerting`, `freshness`, `data_quality`, `ingestion`, `chat`, `docs`, `metadata`, `lineage`) are seeded automatically.
- When a connector is configured (Test → green → Persist), `_auto_grant_configured_connector_read_only` enables **read** on every system agent whose category list includes that connector's category.
- **Chat agent additionally gets `write_enabled=True`** on every category it can read. Writes still require approval, so this is safe - and it removes the "why doesn't my chat agent have write?" trip every tester hit.
- Custom agents need grants set manually in the Agent Configure modal.

## Where things crash and what the symptom looks like

| Symptom | Likely cause | Where to look |
|---|---|---|
| `Internal Server Error` (21 bytes) on Approve | Vendor 4xx/5xx during approved exec | `main.py::approve_and_execute_alert` - now translates to 502/504 |
| Chat answers with no embeddings | Chroma unreachable or no embedding key | `vector_store.py::_get_embedding_function` - fallback now logs `embedding_fallback_local` |
| Connector "Sync failed" with `InvalidToken` | Encrypted creds were written with a different `MASTER_KEY` | `~/.dataclaw/.env` `MASTER_KEY` rotated; re-enter credentials |
| Notion write fails after approval | `page_id` hallucinated by LLM | Now caught pre-approval by `_preflight_write_arguments` |
| dbt connector "Sync failed" against fixture | `base_url` was hidden in the UI | Now exposed as optional field; set to `http://localhost:18090/api/v2/accounts/1234` |
| Background agents never fire | Worker not running | `dataclaw doctor` shows worker status; `EMBEDDED_WORKER=true` runs it in-process |
| Airflow connector shows DAGs but no failed runs | No runs were triggered after `integration-up` | `make integration-seed` now triggers `dataclaw_e2e_failure` and waits for terminal state |

## Sairam-side blockers (one liner each)

1. **Redshift**: open security group ingress for tester IPs on `default-workgroup.203358432634.us-east-1:5439` (or share VPC peering).
2. **GitHub PAT mismatch**: rotate PAT to own `svangapally81/dataclaw`, OR update `api_tokens.txt` + `docs/TESTER_GUIDE.md` to point at `ShandilyaPeddi/dataclaw-ci`.
