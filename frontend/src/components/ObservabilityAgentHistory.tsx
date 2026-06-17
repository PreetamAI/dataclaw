import { Bot, CircleCheck, Clock, XCircle } from "lucide-react";

import { AGENT_ICONS } from "../lib/agent-icons";
import type { ObservabilityEvent } from "../types";

type Props = {
  events: ObservabilityEvent[];
  filter?: string;
};

const FAILED_STATES = new Set(["failed", "timed_out", "cancelled", "error"]);

export function ObservabilityAgentHistory({ events, filter = "" }: Props) {
  const runsByAgent = new Map<string, ObservabilityEvent[]>();
  for (const event of events) {
    if (event.kind !== "agent_run") continue;
    const name = event.agent_name || event.title.split(" — ")[0] || "Agent";
    const runs = runsByAgent.get(name) ?? [];
    runs.push(event);
    runsByAgent.set(name, runs);
  }
  const query = filter.trim().toLowerCase();
  const groups = [...runsByAgent.entries()]
    .filter(([agent]) => !query || agent.toLowerCase().includes(query))
    .map(([agent, runs]) => ({ agent, runs: runs.slice(0, 20) }));

  if (runsByAgent.size === 0) {
    return <p className="connector-empty">No agent runs have been recorded yet.</p>;
  }
  if (groups.length === 0) {
    return <p className="connector-empty">No agents match “{filter.trim()}”.</p>;
  }

  return (
    <div className="agent-history-grid">
      {groups.map(({ agent, runs }) => (
        <AgentRunTable agent={agent} key={agent} runs={runs} />
      ))}
    </div>
  );
}

function AgentRunTable({ agent, runs }: { agent: string; runs: ObservabilityEvent[] }) {
  const latest = runs[0];
  const AgentIcon = AGENT_ICONS[(latest?.agent_icon_key || "bot") as keyof typeof AGENT_ICONS] ?? Bot;
  const failures = runs.filter((run) => FAILED_STATES.has(run.state)).length;
  const successes = runs.length - failures;
  const durations = runs.map((run) => run.duration_ms ?? 0).filter((ms) => ms > 0);
  const avgDuration = durations.length
    ? durations.reduce((sum, ms) => sum + ms, 0) / durations.length
    : 0;
  const sparkline = runs
    .filter((run) => (run.duration_ms ?? 0) > 0)
    .slice(0, 14)
    .map((run) => ({ id: run.id, duration: run.duration_ms ?? 0, failed: FAILED_STATES.has(run.state) }));
  const maxDuration = Math.max(1, ...sparkline.map((point) => point.duration));

  return (
    <article className="agent-history-card">
      <header>
        <span className="event-glyph"><AgentIcon size={14} /></span>
        <div>
          <strong>{agent}</strong>
          <em>
            {runs.length} recent {runs.length === 1 ? "run" : "runs"}
            {latest ? ` · ${relativeTime(latest.timestamp)}` : ""}
          </em>
        </div>
      </header>
      <div className="agent-card-stats">
        <span className="stat ok"><CircleCheck size={12} /> {successes}</span>
        {failures > 0 ? <span className="stat bad"><XCircle size={12} /> {failures}</span> : null}
        {avgDuration > 0 ? <span className="stat"><Clock size={12} /> {formatDuration(avgDuration)} avg</span> : null}
      </div>
      {sparkline.length > 0 ? (
        <div className="agent-sparkline" aria-label="Run duration sparkline">
          {sparkline.map((point) => (
            <span
              className={point.failed ? "failed" : ""}
              key={point.id}
              style={{ height: `${Math.max(10, (point.duration / maxDuration) * 100)}%` }}
            />
          ))}
        </div>
      ) : null}
      <div className="agent-runs-scroll">
        <table>
          <thead>
            <tr>
              <th>Status</th>
              <th>When</th>
              <th>Duration</th>
              <th>Tools</th>
            </tr>
          </thead>
          <tbody>
            {runs.map((run) => (
              <tr key={run.id}>
                <td><StatusBadge state={run.state} /></td>
                <td>{new Date(run.timestamp).toLocaleString()}</td>
                <td>{formatDuration(run.duration_ms)}</td>
                <td>{run.tool_calls?.length ?? 0}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      {runs.some((run) => run.error_message) ? (
        <p className="agent-history-error"><XCircle size={13} /> {runs.find((run) => run.error_message)?.error_message}</p>
      ) : null}
    </article>
  );
}

function StatusBadge({ state }: { state: string }) {
  return (
    <span className={`run-status ${state}`}>
      <Clock size={11} /> {state.replaceAll("_", " ")}
    </span>
  );
}

function relativeTime(iso: string): string {
  const diffSec = Math.max(0, Math.round((Date.now() - new Date(iso).getTime()) / 1000));
  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffSec < 3600) return `${Math.round(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.round(diffSec / 3600)}h ago`;
  return new Date(iso).toLocaleDateString();
}

function formatDuration(durationMs?: number | null): string {
  if (!durationMs) return "n/a";
  if (durationMs < 1000) return `${Math.round(durationMs)}ms`;
  return `${(durationMs / 1000).toFixed(1)}s`;
}
