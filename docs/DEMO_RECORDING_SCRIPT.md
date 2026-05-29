# DataClaw Demo Recording Script (v2)

Re-record addressing Sairam's feedback:
- Open one connector (Snowflake), show config, then close it.
- After Agents/Logs walkthrough, go to Chat and ask a natural-language question that executes a real SQL query (e.g. "how many customers do I have?").

Target length: 4-6 minutes. Resolution: 1920x1080. No background noise / mic check before take.

---

## Pre-flight (off-camera, do BEFORE hitting record)

1. `dataclaw start` is running, UI is open at `http://localhost:8000`.
2. LLM provider is configured (Ollama llama3.1:8b or OpenAI key set) - verify by sending one chat prompt in advance so the model is warm; this avoids the request-timeout we saw last take.
3. Snowflake connector credentials ready in clipboard (or already saved, just to open/close).
4. At least one connector with a `customers` table connected and compiled - confirm a known answer for "how many customers" (e.g. demo SQLite has `customers` table).
5. Approval-required toggle: turn OFF for read-only queries so the chat answer flows end-to-end without an approval prompt mid-recording. (Write/destructive queries should still require approval - just don't trip it during the demo.)
6. Browser zoom at 110%. Close DevTools, extra tabs, notifications (Slack/Pumble in DND).
7. Sample question rehearsed: "how many customers do I have?"

---

## Scene-by-scene script

### Scene 1 - Intro (0:00 - 0:20)
**On screen:** DataClaw home/workspace.

> "Hi, this is DataClaw - a self-hosted AI-native data platform. I'll show three things: connectors, the agents and their logs, and the chat agent answering a question by running real SQL."

### Scene 2 - Connectors: open & close one (0:20 - 1:10)
**Action:** Click **Connectors** in left nav. Hover the grid so all 21 are visible.

> "DataClaw ships with twenty-one connectors - warehouses, ETL tools, wikis, LLM providers. Let me open one."

**Action:** Click **Snowflake**. Configuration drawer/modal opens.

> "Each connector takes its native credentials - here, account, warehouse, role, user, and an auth method. Test connection runs a live ping."

**Action:** (Optional) Click **Test connection** if it's quick (<3s). Then click **Cancel** / **Close** without saving.

> "I'll close this without changes - you can see we've already got Postgres and the demo SQLite connected on the list."

### Scene 3 - Agents & Logs (1:10 - 2:30)
**Action:** Click **Agents** in left nav.

> "Every agent shares the same shape - a system prompt, MCP grants for which connectors it can read or write, an enable toggle, and a cadence for background agents."

**Action:** Show the agents list - chat, docs, alerting, freshness, data-quality, ingestion. Open one background agent (e.g. **Alerting**).

> "This is the alerting agent. It runs on a schedule, watches DAG failures and dbt test results, and writes to the observability feed."

**Action:** Click **Logs** / **Activity** tab.

> "Every run shows up here - inputs, the LLM trace, tools it called, and outputs. Audit-friendly."

**Action:** Scroll one log entry briefly. Close the drawer.

### Scene 4 - Chat: NL question -> SQL (2:30 - 4:30)
**Action:** Click **Editor** / **Chat** in left nav.

> "Now the chat agent. It has read grants on our connected databases, so it can write SQL, run it, and bring back grounded answers."

**Action:** Type slowly so viewers can read: `how many customers do I have?`

**Action:** Hit Enter. Let the agent think.

**On screen:** Agent response appears with the generated SQL (e.g. `SELECT COUNT(*) FROM customers`) and the result.

> "The agent picked the customers table from the knowledge graph, generated this SELECT COUNT, executed it against the connector, and returned the number. The SQL is visible right there - no black box."

**Action:** (Optional) Ask a one-line follow-up to show context: `which connector did that run against?`

### Scene 5 - Wrap (4:30 - 5:00)
**Action:** Cursor on the workspace home.

> "That's the loop - connect a source, agents compile a graph, and you can chat against it with real SQL on the wire. Everything self-hosted, your data stays in your network. Thanks for watching."

---

## Things to AVOID in this take (caused issues last time)

- Don't fire the chat query before the LLM has been warmed up - cold first call timed out.
- Don't demo a write/destructive query that triggers the approval modal mid-flow.
- Don't open Settings or anything that exposes API keys / `.env` values on screen.
- Don't switch desktops mid-record (causes the resolution flicker we saw).

## Post-record

- Trim dead air at start and end.
- Export 1080p MP4, <50 MB if possible (Pumble inline preview).
- Drop into the Sairam DM with one line: "redo per your notes - Snowflake open/close + chat -> SQL at 2:30".
