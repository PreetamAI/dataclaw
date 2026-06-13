# DataClaw v0.2.0 Release Notes

**Evals & the self-improving loop.** DataClaw can now measure its own answer quality, capture canonical queries, and turn user feedback into regression tests — so quality compounds the longer it runs in your stack.

## Install

The core evals layer ships in the base install — no extra needed:

```bash
pipx install dataclaw-platform        # upgrade an existing install: pipx upgrade dataclaw-platform
dataclaw init
dataclaw start
```

For the optional **LLM-judge metrics** (faithfulness, answer relevancy, safety) and the optional **Langfuse trace sink**, install the extra:

```bash
pipx install 'dataclaw-platform[evals]'
```

Already on Docker Compose? Pull the new image and `docker compose up -d` — no config changes required.

## What's new

### Eval cases & golden queries

Curate `question → expected SQL / answer` pairs with a `candidate → approved → golden` lifecycle. A **golden** case short-circuits the chat agent to the canonical SQL instead of regenerating it — faster and stable. Promotion to `golden` is always an explicit human step; an applied suggestion lands at `approved`, never auto-activates.

### Eval cases from your workspace

Producers auto-generate candidate cases from the data you already have:

- **schema** — one case per real table (system tables skipped)
- **knowledge graph** / **lineage** — cases grounded in compiled entities and relationships
- **fixtures** — the bundled demo questions
- **chat history** — an assistant answer that carries SQL, has at least one 👍, and zero 👎 becomes a regression case. This is what catches "the agent used to answer *give me last week's revenue* correctly and now doesn't."

### Metrics

| Tier | Metrics | Needs |
|---|---|---|
| Deterministic (base install) | SQL correctness, result accuracy, citations, connector/tool routing, cost/latency | nothing extra |
| LLM-judge (optional) | faithfulness, answer relevancy, safety | the `[evals]` extra + any configured LLM provider (OpenAI, Anthropic, or local Ollama) |

If the extra isn't installed or no provider is set, judge metrics return `skipped` with a clear reason — nothing breaks.

> **Result accuracy is informational, not gating.** An exact result-set hash is only reliable against frozen fixture data; against a live warehouse the rows legitimately change between runs, so a mismatch doesn't mean a regression. It reports a score but does not fail a run. Gating on result accuracy returns with side-by-side execution (run the golden SQL and the agent SQL in the same moment, compare) in a later release.

### Suggestions

A failing run can propose a fix — a golden query, a prompt tweak, or a workspace rule. Applying a suggestion is human-reviewed; nothing is applied silently.

### Feedback & schema-drift handling

- **👍/👎 on chat answers** is captured per message and feeds the chat-history producer.
- When a connector sync changes a table's columns, golden cases referencing it are **demoted and tagged** for review — never silently deleted.

## Do I need a Langfuse account? No.

Langfuse is an **optional, off-by-default** trace sink. DataClaw's own database is always the system of record for traces. You only configure Langfuse (in Settings, per workspace) if you specifically want to inspect agent traces in Langfuse's UI — and you can self-host Langfuse so nothing leaves your network. Without keys, or without the SDK installed, it's a clean no-op. Evals — including LLM judges — do **not** require it.

## Known limitations / deferred

- The evals API is workspace-authenticated; full per-workspace isolation (IDOR hardening) and a default cost ceiling for scheduled LLM-judge batches land with the multi-user / VPC release. On a single-user local install these don't apply.
- Scheduled eval batches currently run on a fixed interval; honoring a custom `schedule_interval_minutes` is a follow-up.

## Upgrade notes

- Four new migrations (`0020`–`0023`) run automatically on start.
- No breaking API or schema changes to existing surfaces.
- New optional dependency group `[evals]`; the base install is unchanged in size.
