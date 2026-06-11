# Handoff — Real connectors + chat provenance (paste into new chat)

> Context for a fresh session. Branch `phase-2-evals-layer`. Backend runs on
> `127.0.0.1:8000` (uvicorn `--reload`, log at `/tmp/dataclaw-dev.log`). Admin
> session cookie at `/tmp/dc-cookies.txt`. Three real connectors are wired:
> **PostgreSQL**, **Notion**, **Airflow** (Snowflake/etc. stay demo).

## What was just done (this session)

Five fixes landed in the backend, all verified end-to-end against real data.
None are committed yet — `git status` shows them as unstaged edits.

| # | Fix | File(s) | Result |
|---|-----|---------|--------|
| 1 | **Provenance pills** for MCP multi-tool turns. The persist path built `tool_call_provenance` citations from both `tool_call` (singular) and `tool_calls` (plural), but never wrote them back into the live `response` dict — so the streaming `done` frame and `/ide/chat` JSON showed no pills. Added `response["citations"] = citations`. | `backend/app/main.py` (`_persist_chat_response`, ~1921-1947) | ✅ pills `via postgres · read_query_select`, `via notion · read_search_pages`, `via airflow · read_list_dags` now present in every turn |
| 2 | **Postgres schema resolution.** Added a `default_schema` credential field + `_engine_kwargs_for_datastore` that sets libpq `options=-c search_path=<schemas>` on the async engine, and `_default_schema_for_datastore` advertises the first schema to tool callers. Unqualified `FROM churn_events` now resolves to `raw.churn_events`. | `backend/app/services/mcp_executor.py`, `connectors/catalog.py`, `connectors/adapters.py` | ✅ unqualified SQL resolves across `raw,core,public` |
| 3 | **SQLite `:memory:` detection.** `make_url(...).database` parses as `/:memory:`; the guard now handles both forms so sqlite reports `not_configured` instead of `REAL`. | `backend/app/services/demo_seed.py` | ✅ connectors page shows sqlite `not_configured` |
| 4 | **DBAPIError misclassification (reliability).** `ProgrammingError` is a subclass of `DBAPIError`, so a bad-SQL error (e.g. `relation core.churn_events does not exist`) was reported as *"PostgreSQL could not be reached or rejected credentials"* (503) — telling the model to give up. Now: `OperationalError`/`InterfaceError` → 503 (real connectivity); other `DBAPIError` → 400 with the underlying DB message, so the agent self-corrects on the next tool round. | `backend/app/services/agents/chat.py` (`_mcp_error_response`, imports) | ✅ model now retries and fixes its own SQL |
| 5 | **Column context (reliability).** Brain-node schema entries carried only name/summary (no columns), so the LLM hallucinated column names (`event_date`, `occurred_at`). Now columns from `TableAsset` are indexed by bare table name and attached to the matching brain node. Also tightened the system prompt: prefer **unqualified** table names (search_path resolves them); never default a table to the `core` schema. | `backend/app/services/agents/chat.py` (schema_context build ~3585, system prompt ~3800) | ✅ model uses real columns (`event_at`, `event_type`); 4/4 runs complete cleanly |

## Verified state

- Canonical 3-part question now returns `llm_status: mcp_tool_completed` on every
  run, executes real SQL against Postgres, and shows all three provenance pills.
- Connectors: postgres `synced/real`, notion `synced/real`, airflow `synced/real`,
  sqlite `not_configured`.
- Brain compiled: 503 nodes / 1306 edges.

## ⚠️ Open issue to discuss next (NOT one of the 3 fixes)

**The model over-filters churn.** The Notion "Churn definition" page says:
*"Churn is counted when a paying customer cancels, downgrades to free, or has no
successful order for 30 days."* In Postgres `churn_events.event_type` holds exactly
those three values: `cancellation`, `downgrade`, `inactive_30d`. There is **no**
`event_type = 'churn'` value.

The agent frequently writes `WHERE event_type = 'churn'` → returns **0**, which is
wrong. The correct last-7-days churn count (no type filter, since every row is a
churn event per the definition) is **25 distinct customers**. Latest events span
2026-06-07 … 06-09, so data is fresh.

Root cause: the model has the column name but not the enum *values*, and doesn't
map the Notion prose definition → the `event_type` enum. Options to weigh next:
- (a) Surface distinct values for low-cardinality text columns in schema context.
- (b) Add a column-level note/synonym ("event_type ∈ {cancellation,downgrade,inactive_30d}; all are churn") to the brain.
- (c) Strengthen the prompt to derive filter values from cited definition docs.

## Questions to run in the UI (for screenshots)

1. **Canonical investor question** (the headline):
   > How many customers churned in the last 7 days according to Postgres, which Notion page documents the churn definition, and which Airflow DAG owns the churn calculation?
   - Expect: an answer naming `acme_churn_calc` DAG + the "Churn definition" Notion
     page, **three provenance pills** under the reply, and a count.
   - ⚠️ Count may read "0" until the open issue above is fixed — correct is **25**.
2. **Direct SQL sanity:**
   > Run a SQL query against Postgres to count distinct customer_id in churn_events where event_at is within the last 7 days.
   - Expect: ~25, single `postgres · read_query_select` pill.
3. **Provenance self-attribution (follow-up turn):** after Q1, ask
   > Which connector and tool did you use for the churn count?
   - Expect: it cites `postgres.read_query_select` from the marker.
4. **SQLite cosmetic check:** Connectors page → confirm SQLite shows
   **Not configured** (not REAL).

### Screenshots to capture
- Chat reply to Q1 showing the **three provenance pills** + answer.
- Langfuse/local trace for Q1 (postgres/notion/airflow spans).
- Connectors page (postgres/notion/airflow SYNCED, sqlite Not configured).
- Knowledge base → Brain (compiled graph, table counts).
- Evals page (search bar pinned, collapsed generate panel, empty-state right pane).

## How to re-run quickly (curl)

```bash
cd "/Users/preetam/Desktop/Cognilayer LLP/dataclaw"
curl -s -b /tmp/dc-cookies.txt -X POST http://localhost:8000/ide/chat \
  -H 'content-type: application/json' \
  -d '{"question":"How many customers churned in the last 7 days according to Postgres, which Notion page documents the churn definition, and which Airflow DAG owns the churn calculation?"}'
```

## Not done on purpose
- **No commit** — edits are unstaged, waiting on your go-ahead.
- Open churn-semantics issue left for the next chat per your call.
