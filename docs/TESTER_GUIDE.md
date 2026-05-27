# DataClaw v0.1.0 - Tester Guide

Manual testing before public release. Two stages. **Don't start stage 2 until stage 1 is green.**

Repo: `https://github.com/svangapally81/dataclaw` (branch: `main`)

You'll need: your own OpenAI API key, plus credentials for the SaaS connectors you want to try.

**Credentials**: the project owner will send a separate, secure bundle of test sandbox tokens for Notion, GitHub, Confluence, BigQuery, Snowflake, Databricks, Redshift, and Fivetran. Use those tokens in the UI when configuring each SaaS connector - see the [Credentials reference](#credentials-reference) section below for which token maps to which UI field. For local DB / ETL connectors, the bundled Docker fixtures need no external credentials.

---

## Stage 1 - Developer path

Goal: prove every documented connector and chat flow actually works when used through the UI.

### Run the product + spin up Docker fixtures

You'll need: **Docker Desktop** running (the local container fixtures require it), `uv` for Python deps (`brew install uv`), Node 22+, and Python 3.12+.

```bash
git clone https://github.com/svangapally81/dataclaw.git
cd dataclaw
make quickstart                 # installs deps + bundles UI + dataclaw init + dataclaw start
```

UI opens at `http://127.0.0.1:8000`.

In a **second terminal**, boot the Docker fixture stack and run the seed script:

```bash
make integration-up             # boots ~10 container services
make integration-seed           # populates them with Acme-shaped test data (idempotent)
```

`integration-up` runs `docker compose up -d --wait` on `tests/integration/docker-compose.yml`. `integration-seed` runs `tests/integration/seed/run.py` - it inserts `customers / orders / products` rows into Postgres + MySQL + SQL Server, sets up Trino's memory catalog, loads the BigQuery emulator dataset, and triggers the fixture API services to load their JSON fixtures. Re-running is safe - it's a no-op if data already exists.

What's already seeded for you after `make integration-seed`:

| Connector | Where | What's in it |
|---|---|---|
| **SQLite** | bundled with the app | demo `customers / orders / products` tables (visible immediately) |
| **Postgres** | container `:55432` | `core` schema with `customers`, `orders`, `products` |
| **MySQL** | container `:53306` | `dataclaw_integration` DB with same shape |
| **SQL Server** | container `:11433` | `dataclaw_integration` DB |
| **Trino** | container `:18080` | memory catalog with sample table |
| **BigQuery emulator** | container `:19050` | `dataclaw-integration` project, `core` dataset |
| **Airflow** | container `:18080` | 3 DAGs incl. one with a recent failure |
| **dbt fixture API** | container | mock dbt Cloud-style API with seeded models |
| **Prefect** | container `:18082` | seeded flow + run history |
| **Dagster** | container `:18083` | seeded assets + materializations |
| **Airbyte** | container `:18081` | seeded connection + job history |

### Connect tools from the UI

In **Settings → LLM Providers**, paste your OpenAI key, hit **Test**.

In the **Connectors** tab, decide per-connector whether to use the **Docker fixture** (zero setup) or **real SaaS API** (your account):

| Connector | Use fixture (recommended for fast test) | Use real SaaS (recommended if you have an account) |
|---|---|---|
| Postgres, MySQL, SQL Server, Trino | ✅ default - use the running container | only if you want your own warehouse |
| Airflow, Prefect, Dagster, dbt, Airbyte | ✅ default - use the running container | only if you have Cloud-tier access |
| BigQuery | ✅ emulator works for read paths | ✅ for full coverage incl. write paths |
| **Notion, GitHub, Confluence** | ❌ no local fixture - must use real SaaS | ✅ **required** - use your own integration token / PAT |
| Snowflake, Databricks, Redshift | ❌ no local fixture - must use real SaaS | ✅ **required** - use your own account |
| Fivetran | ❌ no local fixture | ✅ **required** - use your API key |

**Aim for at least**: 2 data stores + 2 ETL tools + 2 knowledge sources connected.

For each connector:
- Enter credentials (real or fixture endpoint per the table above)
- Click **Test** → expect green
- Click **Sync** → expect tables/pages/runs discovered

**If "Sync" doesn't auto-seed real data into the graph**, file that as a bug - it's the whole point of the connector.

### Try a few chat scenarios

Open Editor, ask:

1. **Single-source question**: "What tables do I have in [warehouse-name]?" → real schema returned with citations
2. **Document question**: "Summarize my Notion docs" → real content from your pages
3. **ETL question**: "Show me the latest failed run in [tool]" → identifies a real run
4. **Cross-source question**: "How does [thing in Notion] relate to [table in warehouse]?" → agent uses both, links them
5. **Write+approval flow**: "Append today's deployment notes to my [Notion/Confluence] runbook" → returns pending approval → approve in UI → write executes → audit log shows it

### Stage 1 pass criteria

- [ ] Every connector you tried: Test green, Sync returns real data
- [ ] Chat answers reference real data from your accounts (not made-up)
- [ ] Write approval flow completes end-to-end with audit trail
- [ ] `dataclaw doctor` is fully green
- [ ] No errors in `~/.dataclaw/dataclaw.log` during normal use
- [ ] UI feels usable - no broken forms, confusing buttons, or dead clicks

**If anything fails → stop, file blockers, do not proceed to Stage 2.**

When done with Stage 1:
```bash
make integration-down           # stop + remove the fixture containers
```

---

## Stage 2 - Installation path

Goal: prove the three documented install paths work for a brand-new user following docs verbatim.

Use a **fresh machine or fresh user account** if you can, so you're not relying on dev tools already installed. For this stage you're testing the *install experience* - use just the **bundled SQLite demo** + **your own SaaS creds** (Notion, OpenAI). Don't bother with Docker fixtures here; those are dev tooling, not what end users do on first run.

For each path below, follow the docs literally - `README.md` + [getdataclaw.xyz/docs/quickstart](https://getdataclaw.xyz/docs/quickstart). **If the docs are missing a step you needed, that's a bug.**

### Path A - pipx (CLI install)

```bash
pipx install dataclaw-platform==0.1.0
# Or from a locally-built wheel: pipx install backend/dist/dataclaw_platform-0.1.0-py3-none-any.whl
dataclaw init
dataclaw start
```

Repeat **scenarios 1, 2, 5** from Stage 1 with the same connectors.

### Path B - Docker Compose

```bash
git clone https://github.com/svangapally81/dataclaw.git
cd dataclaw
cp .env.example .env
# Edit .env per docs (MASTER_KEY, SESSION_SECRET, etc.)
docker compose up -d
```

Open `http://localhost:8000`. Repeat **scenarios 1, 2, 5**.

### Path C - From source

```bash
git clone https://github.com/svangapally81/dataclaw.git
cd dataclaw
make quickstart
```

Repeat **scenarios 1, 2, 5**.

### Stage 2 pass criteria

- [ ] All 3 paths install + boot without errors **strictly following the docs**
- [ ] All 3 land at a working UI
- [ ] Same 3 scenarios work the same across all 3 paths
- [ ] Stop + restart preserves data
- [ ] **You did not need to figure out any undocumented step** - if you did, the docs have a gap

---

## Bug-reporting template

```markdown
**Stage**: 1 or 2
**Scenario/Step**: (which scenario number, or which install step)
**Install path** (Stage 2 only): pipx / docker / source
**Expected**: ...
**Actual**: ...
**Steps to reproduce**: 1. ... 2. ...
**Logs**: relevant log tail
**Severity**:
  - blocker - cannot proceed, install fails, data loss
  - annoying - works but UX bad / confusing / unexpected
  - cosmetic - typo, alignment, polish
**Docs gap** (Stage 2): was there a step you needed that wasn't in the docs?
```

Anything **blocker** holds the v0.1.0 release. Annoying + cosmetic go to v0.1.1 backlog.


---

## Credentials reference

The project owner will send you a separate, secure credential bundle. Each item below maps a credential type to the UI field where you paste it. **Treat these tokens as secrets** - don't commit them, don't paste them in screenshots, don't share them outside this engagement.

| Connector | UI: where to configure | Fields you paste into |
|---|---|---|
| **OpenAI** | Settings → LLM Providers → OpenAI | API key, Model (use `gpt-4.1-mini`) |
| **Notion** | Connectors → Notion → Configure | Integration token (`ntn_...`) |
| **GitHub** | Connectors → GitHub → Configure | PAT (`github_pat_...`), Repositories (the test repo from the bundle) |
| **Confluence** | Connectors → Confluence → Configure | Site URL, Email, API token |
| **BigQuery** | Connectors → BigQuery → Configure | Service Account JSON (paste the whole JSON), Project ID |
| **Snowflake** | Connectors → Snowflake → Configure | Account, Warehouse, Database, Schema, User, Private key (RSA) |
| **Databricks** | Connectors → Databricks → Configure | Workspace URL, SQL warehouse HTTP path, Token (`dapi...`) |
| **Redshift** | Connectors → Redshift → Configure | Cluster endpoint, Port (5439), Database (`dev`), User, Password |
| **Fivetran** | Connectors → Fivetran → Configure | API key, API secret |

For container-based connectors (Postgres, MySQL, SQL Server, Trino, Airflow, dbt fixture, Prefect, Dagster, Airbyte), use the localhost endpoints listed in the seeding table above. No external token is needed - they run on your machine.

### Reporting credential issues

If a token doesn't work (auth failure, repo not found, dataset doesn't exist, etc.), file a **blocker** bug - the project owner will rotate or fix the sandbox account. Don't waste time debugging vendor-side credential setup; ping back.

---
