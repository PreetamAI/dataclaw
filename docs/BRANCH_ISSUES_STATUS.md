# Branch issues & status — `chore/svangapally81-rename`

Snapshot: 2026-05-28. Tracks every issue surfaced on this branch (testing + demo recording) and what's been done.

## ✅ Fixed and LIVE in the running app

All six fixes below are now deployed: `pipx uninstall` + `pipx install` from `backend/` re-ran cleanly, installed files match the working tree, daemon restarted (`dataclaw stop` → `dataclaw start`, PID 2917), `/health` returns 200.

- **Chat 30s timeout → "request timeout" error.**
  - Fix: `DEFAULT_CHAT_BUDGET_SECONDS` 30→120, `DEFAULT_CHAT_BUDGET_TOKENS` 50k→200k in [runtime.py](backend/app/services/agents/runtime.py).

- **Redshift "codec not available: UNICODE" — first SQL call always failed.**
  - Fix: alias `UNICODE` → `utf-8` in `psycopg._encodings` at adapter import. [adapters.py](backend/app/services/connectors/adapters.py).

- **Redshift SQLAlchemy dialect probed params Redshift doesn't expose.**
  - Fix: rewrote `RedshiftAdapter` to use raw psycopg in a thread; bypasses dialect introspection. Catalog promoted KNOWN_ISSUE → BETA. [adapters.py](backend/app/services/connectors/adapters.py), [catalog.py](backend/app/services/connectors/catalog.py).

- **Airflow sandbox first-boot 5xx / connection error flake.**
  - Fix: adapter-level retry on `test()` — 2 attempts, 1s + 3s backoff. Catalog caveat removed. [adapters.py](backend/app/services/connectors/adapters.py).

- **Redshift sync `InsufficientPrivilege: permission denied for schema pg_auto_copy`.**
  - Fix: expanded schema-exclude list (`pg_catalog`, `information_schema`, `pg_internal`, `pg_auto_copy`, `pg_automv`, `catalog_history`) + `not like 'pg\_%'` filter; per-table `count(*)` wrapped in `try/except psycopg.errors.InsufficientPrivilege` so one locked-down table doesn't fail the whole sync. [adapters.py](backend/app/services/connectors/adapters.py).

- **Chat misroutes `customers` query to Postgres → "connectivity issue".**
  - Fix: added explicit connector-routing rule to the chat system prompt — pick the tool whose connector slug owns the referenced table in `schemas` / `retrieval_trace`; never assume a table exists on a connector that didn't surface in the trace. [chat.py](backend/app/services/agents/chat.py).

## ✅ Production-readiness pass (2026-05-29) — in working tree, not yet deployed

Seven reliability gaps surfaced during a live multi-connector reconfigure session (Notion, GitHub, Confluence, Fivetran, BigQuery, Databricks, Redshift, Snowflake). All seven are fixed in source; the running daemon is still on the prior wheel until `pipx install` + restart.

- **Stuck `sync_state='syncing'` rows after mid-transaction crash.** A `database is locked` during a flush left the session in `PendingRollback`; the `except` branch then tried `commit()` on that session and produced a 500 while the row stayed `syncing` forever. Subsequent `/sync` calls returned 409 "already running" until I `UPDATE`'d the row by hand.
  - Fix: extracted `_run_connector_sync()` that always rolls back on flush failure and writes the terminal state from a *fresh* session. Added a startup reaper that flips any `syncing` row to `sync_failed` with a "previous sync was interrupted by a restart" message. [main.py](backend/app/main.py).

- **`auto_sync_all_connectors` failing silently with `DetachedInstanceError`.** Background scheduler was throwing on `connector.id` because the loop carried ORM instances across a `session.rollback()` — rollback expires all instances regardless of `expire_on_commit=False`.
  - Fix: snapshot `(id, slug)` tuples upfront, refetch each connector inside its iteration. [auto_sync.py:28-37](backend/app/services/ingestion/auto_sync.py#L28-L37).

- **SQLite write contention under concurrent syncs.** Five parallel `/sync` calls produced three 500s with `database is locked`. WAL + 30s `busy_timeout` were already set in [session.py:34-39](backend/app/db/session.py#L34-L39) — verified — but the failure mode was actually the rollback-discipline bug above (caught by the `_run_connector_sync` fix), not the absence of WAL.

- **Dev source vs. running code drift.** Local working-tree fixes (e.g. Redshift `escape '\'` → `escape '!'`) sat unshipped while the live daemon ran the pipx-installed v0.2.0 wheel. Anyone debugging from logs read source that wasn't executing.
  - Fix: `BUILD_INFO` resolves the running git SHA + package version at import. Surfaced in the `startup_begin` log line and the `/health` response — `curl /health` now shows `"commit": "<sha>"` so you can tell what's actually running. [main.py](backend/app/main.py).

- **/sync was synchronous and blocked the HTTP request for 40+ seconds.** Standard clients timed out at 60s before the sync finished server-side.
  - Fix: `/sync` returns **202 Accepted** immediately with a `poll_url`; the work runs as an `asyncio.create_task` on its own session. Added `?wait=true` query param for callers (tests, CLIs, scripts) that need synchronous semantics — that path returns 200 with the summary. [main.py](backend/app/main.py).

- **No structured retries for transient driver errors.** Only Airflow had a hand-rolled retry. Network blips, rate-limit responses, and connection-reset errors propagated immediately on the first attempt.
  - Fix: `_RetryingAdapter` wraps every `adapter_for()` result. Retries `AdapterReachabilityError`, `AdapterRateLimitError`, `httpx.TimeoutException`, `httpx.TransportError`, `ConnectionError`, `TimeoutError` with exponential backoff. `test()` retries 3× (0.5s / 1.5s / 4.5s); `sync()` retries 2× (5s / 15s) to avoid amplifying load. Permanent errors (auth, missing creds) skip retry. Each retry logs `slug + op + attempt + next_delay_s`. Adapter-specific methods like `ConfluenceAdapter.fetch_content` pass through via `__getattr__` so existing callers stay unaware of the wrapper. [adapters.py](backend/app/services/connectors/adapters.py).

- **Secrets pasted through chat / dropped into `/tmp` scripts.** Live tokens for 8 connectors landed in chat history while configuring them; the helper script that held them was on disk briefly during the run.
  - Fix: `dataclaw secrets import <file> [--dry-run]` CLI reads a `{"slug": {credential_fields}}` JSON file, runs `adapter_for(slug).test(creds)` for each entry, and on success encrypts + persists into the connector row through the same code path as `/connectors/{slug}/test?persist_on_success=true`. Secrets never leave the local file. [cli.py](backend/app/cli.py).

**Verification:** 286 backend tests pass (`uv run python -m pytest tests/ --ignore=tests/integration`). Two pre-existing test-isolation failures in `test_connectors_real.py::test_snowflake_adapter_*_private_key_without_password` were confirmed to exist on a clean `HEAD` without these changes — `git stash` + re-run reproduced the same failures — so they're separately tracked, not regressions from this pass.

**Deploy:** `dataclaw stop && pipx install -e backend --force && dataclaw start`. After restart, `curl http://127.0.0.1:8000/health` should show `"commit": "<HEAD-sha>"` confirming the new code is live.

## 🟡 Pre-recording config (UI flips, not code)

- **Approval modal interrupts demo flow.**
  - Action: Agents → On-demand → Chat → turn OFF approval-required for *reads* before recording. Keep ON for writes.

- **Optional safety net:** disable the Postgres connector for the demo run — the routing fix now picks SQLite automatically, but disabling Postgres removes any chance of the LLM second-guessing.

## 📋 Other (non-blocking, log for later)

- **Demo redo for Sairam.** Script lives at [docs/DEMO_RECORDING_SCRIPT.md](docs/DEMO_RECORDING_SCRIPT.md).
- **DEMO_SQLITE_PATH mismatch.** `adapters.py` defaults to `/tmp/dataclaw_demo.sqlite`; live demo sqlite is `~/.dataclaw/demo.sqlite`. Working because the SQLite connector is explicitly configured; default is dead code — clean up later.

## Recording readiness checklist (pre-take 2)

1. ✅ All six code fixes deployed via `pipx install` and daemon restarted.
2. Turn OFF approval-required for read queries (Agents → On-demand → Chat).
3. (Optional) Disable Postgres connector to harden routing.
4. Warm up LLM with one throwaway chat ("hi" works).
5. Verify `sqlite3 ~/.dataclaw/demo.sqlite "select count(*) from customers"` → `4`.
6. Pre-test the two demo questions (below) end-to-end before hitting record.

---

## 🎬 Video question guide

Use these in order. Each one is rehearsed to land cleanly with the fixes above.

### Question 1 — natural language → SQL on a real table

> **"how many customers do I have?"**

- Expected: agent picks the SQLite connector, generates `select count(*) from customers`, runs it, returns **4**.
- What to point at on screen: the SQL block (proves it's not a hallucination), the row count, the retrieval trace chip `customers (sqlite)`.
- Why this question: it's the smallest possible round-trip — retrieval → tool routing → SQL exec → answer. If this works, the whole loop works.

### Question 2 — follow-up to show context + grounding

> **"which connector did that run against?"**

- Expected: agent answers "sqlite" referencing the prior tool call.
- What to point at: shows the chat has memory of the previous tool result, not just the question text.

### Question 3 (optional, only if time) — slightly richer aggregation

> **"how many orders does each customer have?"**

- Expected: `select customer_id, count(*) from orders group by customer_id` (or with a join), returns 1 row per customer.
- What to point at: agent picked the right join key from the schema context, didn't invent columns.

### Backup if anything stalls

If the chat hangs or errors mid-recording, switch to:

> **"list the tables in my demo database"**

- Hits `read_list_tables` on SQLite — cheaper call, faster response, still demonstrates tool use.

### Things NOT to ask on camera

- Anything with the word "delete" / "drop" / "update" — will trip the write approval modal even with reads-approval off.
- Questions about Redshift tables — sync was just fixed but hasn't been re-tested with your live cluster yet; do that off-camera first.
- "What's your system prompt?" / meta questions — the model may respond inconsistently.
