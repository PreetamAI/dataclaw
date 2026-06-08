# Acme Investor Demo — End-to-End Evals Loop

Canonical, reproducible walkthrough of the **full evals loop** against the
**Acme seeded fixtures** on **Postgres + Notion + Airflow**. Two operators
running this should land on the same state — same eval case, same golden
SQL, same suggestion outcomes.

Two ways to run it:

1. **Automated:** `make acme-evals-loop` runs the
   [`test_evals_loop_postgres_notion_airflow`](../tests/integration/acme/e2e/test_evals_loop.py)
   pytest integration test. Fast, deterministic, asserts every step.
2. **Manual UI walkthrough:** follow the [Steps](#steps) section in the
   running app at `http://localhost:5173`. Same state, browser version.

For the connector-agnostic phase-by-phase Evals + Langfuse smoke matrix
(synthetic data, no Acme), see [`EVALS_TESTING_GUIDE.md`](./EVALS_TESTING_GUIDE.md).

---

## Prerequisites

### Env

Add to `.env` (or export in the shell you'll use):

```bash
OPENAI_API_KEY=sk-...                       # required (chat + eval-runner)
NOTION_INTEGRATION_TOKEN=secret_...         # required (Notion seed + live read)
NOTION_TEST_PARENT_PAGE_ID=<page_id>        # required (parent for seeded Acme pages)
DEMO_MODE=true
```

Postgres + Airflow run as docker-compose services seeded by `seed_acme.py`
— no external credentials required for those two.

### Docker

The Docker daemon must be running.

```bash
docker info >/dev/null     # smoke-test the daemon
make integration-up        # boots postgres, airflow, etc. from tests/integration/docker-compose.yml
```

### Seed the Acme fixtures

```bash
make acme-seed BOOT_CONTAINERS=1
```

Writes:

| Connector | What gets seeded |
|-----------|------------------|
| **Postgres** (container) | `raw.customers` + churn-event rows so `SELECT … WHERE status = 'churned'` returns a non-empty result. |
| **Notion** (SaaS) | Acme onboarding doc, churn-definition page, on-call runbook, Postgres↔BigQuery pipeline page — under `NOTION_TEST_PARENT_PAGE_ID`. |
| **Airflow** (container) | `acme_churn_calc` and `acme_etl_daily` DAGs (see `tests/integration/airflow/dags/`), with at least one failed-then-recovered run. |

The exact ids land in `tests/integration/acme/seed/acme_ids.json` and are
read by the e2e suite via `tests/integration/acme/common.py`.

---

## Steps

The numbered steps below mirror the automated test exactly. Every assertion in the test
maps to a row in this table.

| # | Step | What the app does | How to verify (UI / DB / API) |
|---|------|-------------------|-------------------------------|
| 1 | **Connect & sync** Postgres, Notion, Airflow. | `POST /connectors/{slug}/test` then `POST /connectors/{slug}/sync` for each. Knowledge graph + lineage refresh on success. | Settings → Integrations: all three show **status: ok** with green pill. |
| 2 | **Chat turn 1**: ask `"Why did churn spike last week and which DAG owns the calculation?"` | Real LLM call. Agent picks tools across all three connectors. | Assistant message renders. View trace: see `retrieval / golden_lookup` with `hit: false`, plus `tool` spans for `postgres.read_query_select`, a Notion read, and `airflow.read_*`. |
| 3 | **Feedback (👎 + comment)**: "we want the canonical churn-count SQL pinned". | `POST /feedback {sentiment: "negative", comment, chat_message_id}`. | DB: `feedback` row with `sentiment='negative'`, `eval_case_id IS NULL` (not promoted yet). |
| 4 | **Create eval case from feedback** with `expected_sql` + `expected_answer` pinned to the canonical churn-count query. | `POST /evals/cases/from-feedback`. Stamps the latest negative feedback row's `eval_case_id`. | Evals page → Candidate tab: new row, origin `feedback`, status `candidate`. |
| 5 | **Approve → promote-golden**. | Two POST calls, status walks `candidate → approved → golden`. | Evals page → Golden tab: row visible with gold pill. DB: `eval_cases.status='golden'`. |
| 6 | **Chat turn 2**: re-ask the same question. | Chat agent's pre-LLM `_lookup_golden` matches the normalized question → returns the golden SQL + answer without invoking the LLM. | Response shows `llm_status: "golden_query_hit"`. Trace: `retrieval / golden_lookup` with `hit: true` + `tool / golden_query_hit`. Cites the eval case via `golden_query_provenance`. |
| 7 | **Run evals batch** against the golden case (`status_filter=["golden"]`). | `POST /evals/runs` — runner replays the question via the chat agent, records an EvalRun + EvalResults across the 14 metrics. | Evals → Runs tab: new run; metrics dashboard updates. DB: `eval_runs` + `eval_results` rows. |
| 8 | **Diagnose the run** with rules-only (`use_llm=false`) — deterministic and free. | `POST /evals/runs/{run_id}/diagnose`. Rules-based diagnose inspects the failure category (or absence of one) and emits zero or more `EvalSuggestion` rows. | Run-detail page: suggestion cards under the run, each typed `golden_query` / `prompt_diff` / `rules_md`. |
| 9 | **Apply the first actionable suggestion**. | `POST /evals/suggestions/{id}/apply`. Side effect depends on `kind`: `golden_query` creates+promotes a new EvalCase, `prompt_diff` rewrites the prompt-extension setting, `rules_md` appends a line to the chat rules. | Suggestion card flips to **Applied** with revert option. Whatever the suggestion mutated is now visible (golden list / Settings → Prompt / Settings → Chat rules). |

A golden-vs-golden run will often produce **zero** suggestions — that's a valid steady
state for the demo, and the automated test treats it as a pass (the `apply` step is
skipped). When a suggestion does exist, the test drives it through Apply so we cover
the full loop.

---

## Run it

### Automated (recommended)

```bash
# After seed completes:
RUN_ACME_E2E=1 OPENAI_API_KEY=$OPENAI_API_KEY make acme-evals-loop
```

Single test, ~30s on a warm box. Result lands in `evals-loop.json` for the
acme-report aggregator. The test is also wired into `make acme-full
REQUIRE_LIVE=1`, so it runs as part of the release-gate sweep.

### Manual (UI)

1. `make backend && make frontend` (two terminals).
2. Sign in as `admin@dataclaw.local` / `dataclaw-local-admin`.
3. Walk the 9 steps in the table above. Each row's right-hand cell is the assertion you should be able to confirm in the UI or trace viewer.

---

## What "the same state" means

Two operators running this should converge on the same persisted artefacts:

- One `eval_cases` row, `status='golden'`, `origin='feedback'`,
  `expected_sql` byte-equal to the canonical `SELECT COUNT(*) FROM raw.customers …` pinned in the test.
- One `feedback` row stamped with that case's id.
- At least one `eval_runs` row referencing the golden case + a Langfuse
  trace id (when Langfuse is configured).
- A `chat_messages` row from chat turn 2 with `llm_status='golden_query_hit'`.

If you see drift from these, that's the regression. The canonical SQL +
answer live inline at the top of [`test_evals_loop.py`](../tests/integration/acme/e2e/test_evals_loop.py)
— if Sairam pins a different query, that's the one line to update.

---

## When the live run isn't possible

If Docker can't run, Notion creds aren't available, or you only want the
loop-logic verification, the unit-level coverage in
[`backend/tests/test_evals_layer.py`](../backend/tests/test_evals_layer.py)
(`test_feedback_to_golden_short_circuits_chat`, `test_eval_case_workspace_isolation`,
and the lifecycle suite around them) exercises the same code paths against
sqlite + seeded synthetic data. Those run inside `make verify` and in CI on
every PR.
