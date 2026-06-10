import {
  AlertTriangle,
  Bot,
  CheckCircle2,
  CircleAlert,
  CircleCheck,
  CircleDot,
  Clock,
  Search,
  X,
  XCircle,
} from "lucide-react";
import { useMemo, useRef, useState } from "react";

import { AGENT_ICONS } from "../lib/agent-icons";
import { logoDefinition } from "../lib/logos";
import { JsonDetails } from "./JsonDetails";
import {
  useAcknowledgeAlertMutation,
  useApproveAlertMutation,
  useObservabilityEventsQuery,
  useResolveAlertMutation,
  useWorkerStatusQuery,
} from "../services/api";
import type { AgentToolCallEvent, ObservabilityEvent } from "../types";
import { ObservabilityAgentHistory } from "./ObservabilityAgentHistory";

const STATE_FILTERS: { key: string; label: string }[] = [
  { key: "", label: "All" },
  { key: "needs_approval", label: "Needs approval" },
  { key: "acknowledged", label: "Acknowledged" },
  { key: "resolved", label: "Resolved" },
  { key: "completed", label: "Agent runs" },
];

const SEVERITY_FILTERS: { key: string; label: string }[] = [
  { key: "", label: "Any severity" },
  { key: "critical", label: "Critical" },
  { key: "warning", label: "Warning" },
  { key: "info", label: "Info" },
];

function severityIcon(severity: string) {
  if (severity === "critical") return <XCircle size={14} />;
  if (severity === "warning") return <AlertTriangle size={14} />;
  return <CircleDot size={14} />;
}

function stateIcon(state: string) {
  if (state === "resolved" || state === "completed") return <CircleCheck size={13} />;
  if (state === "acknowledged") return <Clock size={13} />;
  if (state === "needs_approval") return <CircleAlert size={13} />;
  return <CircleDot size={13} />;
}

function relativeTime(iso: string): string {
  const then = new Date(iso).getTime();
  const now = Date.now();
  const diffSec = Math.max(0, Math.round((now - then) / 1000));
  if (diffSec < 60) return `${diffSec}s ago`;
  if (diffSec < 3600) return `${Math.round(diffSec / 60)}m ago`;
  if (diffSec < 86400) return `${Math.round(diffSec / 3600)}h ago`;
  return new Date(iso).toLocaleString();
}

function LogoMark({ logoKey, label }: { logoKey?: string | null; label: string }) {
  const logo = logoDefinition(logoKey ?? undefined);
  if (!logo) return <Bot size={14} />;
  if (logo.kind === "asset") return <img alt="" src={logo.src} />;
  return (
    <svg aria-label={label} role="img" viewBox="0 0 24 24">
      <path d={logo.icon.path} fill={`#${logo.icon.hex}`} />
    </svg>
  );
}

export function Gateway() {
  const [state, setState] = useState("");
  const [severity, setSeverity] = useState("");
  const [search, setSearch] = useState("");
  const [selectedEvent, setSelectedEvent] = useState<ObservabilityEvent | null>(null);

  const eventsQuery = useObservabilityEventsQuery(
    { state, severity, q: search.trim(), limit: 200 },
    { pollingInterval: 10_000 },
  );
  const workerQuery = useWorkerStatusQuery(undefined, { pollingInterval: 10_000 });
  const [acknowledgeAlert, ackState] = useAcknowledgeAlertMutation();
  const [approveAlert, approveState] = useApproveAlertMutation();
  const [resolveAlert, resolveState] = useResolveAlertMutation();

  const data = eventsQuery.data;
  const events = data?.events ?? [];

  const PAGE_SIZE = 20;
  const [page, setPage] = useState(0);
  const filterKey = `${state}|${severity}|${search}`;
  const lastFilterKey = useRef(filterKey);
  // Derive page during render instead of resetting via useEffect: when the
  // filter key changes we clamp page to 0 immediately, avoiding a render
  // with stale pagination.
  let effectivePage = page;
  if (lastFilterKey.current !== filterKey) {
    lastFilterKey.current = filterKey;
    effectivePage = 0;
    if (page !== 0) setPage(0);
  }
  const totalPages = Math.max(1, Math.ceil(events.length / PAGE_SIZE));
  const visibleEvents = events.slice(effectivePage * PAGE_SIZE, effectivePage * PAGE_SIZE + PAGE_SIZE);

  const summary = useMemo(() => {
    const buckets = { needs_approval: 0, acknowledged: 0, resolved: 0, agent_runs: 0, critical: 0 };
    for (const event of events) {
      if (event.kind === "agent_run") buckets.agent_runs += 1;
      if (event.state === "needs_approval") buckets.needs_approval += 1;
      if (event.state === "acknowledged") buckets.acknowledged += 1;
      if (event.state === "resolved") buckets.resolved += 1;
      if (event.severity === "critical") buckets.critical += 1;
    }
    return buckets;
  }, [events]);
  const recentFailures = useMemo(() => {
    const cutoff = Date.now() - 60 * 60 * 1000;
    return events.filter((event) => event.kind === "agent_run" && event.state === "failed" && new Date(event.timestamp).getTime() >= cutoff);
  }, [events]);
  const workerLoading = workerQuery.isLoading || workerQuery.isFetching;
  const workerStatus = workerQuery.data?.status ?? "missing";
  const bannerState = workerLoading ? "loading" : workerStatus === "ok" ? (recentFailures.length > 0 ? "warning" : "ok") : "critical";
  const bannerText = workerLoading
    ? "Checking worker status"
    : workerStatus === "ok"
    ? recentFailures.length > 0
      ? `${recentFailures.length} agent ${recentFailures.length === 1 ? "failed" : "failures"} in the last hour`
      : "All agents running"
    : "Worker offline";

  return (
    <section className="gateway-view">
      <section className={`worker-banner ${bannerState}`}>
        <div>
          <strong>{bannerText}</strong>
          <span>
            {workerQuery.data?.last_seen_at
              ? `Last heartbeat ${relativeTime(workerQuery.data.last_seen_at)}`
              : workerLoading ? "Loading latest heartbeat." : "No worker heartbeat has been recorded."}
          </span>
        </div>
        <em>{workerStatus}</em>
      </section>
      <section className="observability-summary">
        <article className={summary.needs_approval > 0 ? "alert" : ""}>
          <strong>{summary.needs_approval}</strong>
          <span>Needs approval</span>
        </article>
        <article>
          <strong>{summary.critical}</strong>
          <span>Critical</span>
        </article>
        <article>
          <strong>{summary.acknowledged}</strong>
          <span>Acknowledged</span>
        </article>
        <article>
          <strong>{summary.agent_runs}</strong>
          <span>Agent runs</span>
        </article>
      </section>

      <div className="observability-grid">
        <section className="gateway-panel events-panel">
          <header>
            <div>
              <h2>Events</h2>
              <p>Approval queue, alerts, and agent runs across the workspace.</p>
            </div>
            <label className="integration-search">
              <Search size={15} />
              <input
                aria-label="Search events"
                placeholder="Search title or detail"
                value={search}
                onChange={(event) => setSearch(event.target.value)}
              />
            </label>
          </header>

          <div className="filter-strip">
            {STATE_FILTERS.map((filter) => (
              <button
                aria-pressed={state === filter.key}
                className={state === filter.key ? "active" : ""}
                key={filter.key || "all"}
                onClick={() => setState(filter.key)}
                type="button"
              >
                {filter.label}
              </button>
            ))}
            <span className="filter-divider" />
            {SEVERITY_FILTERS.map((filter) => (
              <button
                aria-pressed={severity === filter.key}
                className={severity === filter.key ? "active" : ""}
                key={filter.key || "any"}
                onClick={() => setSeverity(filter.key)}
                type="button"
              >
                {filter.label}
              </button>
            ))}
          </div>

          {eventsQuery.isLoading ? (
            <p className="connector-empty">Loading events…</p>
          ) : events.length === 0 ? (
            <p className="connector-empty">
              No events match this filter. {state !== "" || severity !== "" || search ? "Try clearing filters." : "Configure a connector and run an agent to start the feed."}
            </p>
          ) : (
            <>
              <ul className="event-list">
                {visibleEvents.map((event) => (
                  <EventRow
                    key={event.id}
                    event={event}
                    onOpen={() => setSelectedEvent(event)}
                  />
                ))}
              </ul>
              {totalPages > 1 ? (
                <div className="pagination">
                  <button type="button" onClick={() => setPage((p) => Math.max(0, p - 1))} disabled={effectivePage === 0}>
                    Previous
                  </button>
                  <span>
                    Page {effectivePage + 1} of {totalPages} · {events.length} total
                  </span>
                  <button type="button" onClick={() => setPage((p) => Math.min(totalPages - 1, p + 1))} disabled={effectivePage >= totalPages - 1}>
                    Next
                  </button>
                </div>
              ) : null}
            </>
          )}
        </section>
        <ObservabilityAgentHistory events={events} />
      </div>
      <EventDrawer
        event={selectedEvent}
        onClose={() => setSelectedEvent(null)}
        onAcknowledge={() => selectedEvent && acknowledgeAlert(selectedEvent.id)}
        onApprove={() => selectedEvent && approveAlert(selectedEvent.id)}
        onResolve={() => selectedEvent && resolveAlert(selectedEvent.id)}
        ackBusy={ackState.isLoading}
        approveBusy={approveState.isLoading}
        resolveBusy={resolveState.isLoading}
      />
    </section>
  );
}

function EventRow({
  event,
  onOpen,
}: {
  event: ObservabilityEvent;
  onOpen: () => void;
}) {
  const isAlert = event.kind === "alert";
  const AgentIcon = AGENT_ICONS[(event.agent_icon_key || "bot") as keyof typeof AGENT_ICONS] ?? Bot;

  return (
    <li className={`event-row severity-${event.severity} state-${event.state}`}>
      <button className="event-head" onClick={onOpen} type="button">
        <span className="event-glyph">
          {isAlert ? (
            event.logo_key ? <LogoMark label={event.connector_slug ?? event.title} logoKey={event.logo_key} /> : severityIcon(event.severity)
          ) : (
            <AgentIcon size={14} />
          )}
        </span>
        <div className="event-meta">
          <strong>{event.title}</strong>
          <em>{event.detail}</em>
        </div>
        <span className="event-state">
          {stateIcon(event.state)} {event.state.replaceAll("_", " ")}
        </span>
        <span className="event-time">{relativeTime(event.timestamp)}</span>
      </button>
    </li>
  );
}

function EventDrawer({
  event,
  onClose,
  onAcknowledge,
  onApprove,
  onResolve,
  ackBusy,
  approveBusy,
  resolveBusy,
}: {
  event: ObservabilityEvent | null;
  onClose: () => void;
  onAcknowledge: () => void;
  onApprove: () => void;
  onResolve: () => void;
  ackBusy: boolean;
  approveBusy: boolean;
  resolveBusy: boolean;
}) {
  if (!event) return null;
  const isAlert = event.kind === "alert";
  const canAcknowledge = isAlert && event.actions.includes("acknowledge") && !event.acknowledged_at;
  const canApprove = isAlert && event.actions.includes("approve") && event.requires_approval && event.state === "needs_approval";
  const canResolve = isAlert && event.actions.includes("resolve");
  const resolveLabel = event.requires_approval && event.state === "needs_approval" ? "Reject" : "Resolve";
  const toolCalls = event.tool_calls ?? [];

  return (
    <aside className="event-drawer" aria-label="Event detail">
      <header className="event-drawer-head">
        <div>
          <p className="eyebrow">{event.kind.replaceAll("_", " ")}</p>
          <h2>{event.title}</h2>
        </div>
        <button aria-label="Close event" className="icon-button" onClick={onClose} type="button">
          <X size={16} />
        </button>
      </header>
      <div className="event-drawer-body">
        <p className="event-drawer-detail">{event.detail}</p>
        <dl className="event-detail-grid">
          <div><dt>Severity</dt><dd>{event.severity}</dd></div>
          <div><dt>State</dt><dd>{event.state.replaceAll("_", " ")}</dd></div>
          <div><dt>Timestamp</dt><dd>{new Date(event.timestamp).toLocaleString()}</dd></div>
          {event.connector_slug ? <div><dt>Connector</dt><dd>{event.connector_slug}</dd></div> : null}
          {event.agent_name ? <div><dt>Agent</dt><dd>{event.agent_name}</dd></div> : null}
        </dl>
        {(canApprove || canAcknowledge || canResolve) ? (
          <div className="event-actions">
            {canApprove ? <button className="primary" disabled={approveBusy} onClick={onApprove} type="button"><CheckCircle2 size={13} /> Approve</button> : null}
            {canAcknowledge ? <button className="ghost" disabled={ackBusy} onClick={onAcknowledge} type="button"><CheckCircle2 size={13} /> Acknowledge</button> : null}
            {canResolve ? <button className="primary" disabled={resolveBusy} onClick={onResolve} type="button">{resolveLabel}</button> : null}
          </div>
        ) : null}
        {Array.isArray(event.timeline) && event.timeline.length > 0 ? (
          <section className="event-drawer-section">
            <h3>Timeline</h3>
            <JsonDetails label={`Show timeline (${event.timeline.length} entries)`} value={event.timeline} />
          </section>
        ) : null}
        <section className="event-drawer-section">
          <h3>Tool calls</h3>
          {toolCalls.length === 0 ? (
            <p className="connector-empty compact">No tool calls recorded for this event.</p>
          ) : (
            <ul className="tool-call-list">
              {toolCalls.map((call) => (
                <li key={call.id} className={`tool-call ${call.status}`}>
                  <strong>{call.connector_slug ? `${call.connector_slug}.` : ""}{call.tool_name}</strong>
                  <span>{call.status} · {call.latency_ms}ms · {new Date(call.called_at).toLocaleString()}</span>
                  {call.error_message ? <em>{call.error_message}</em> : null}
                  {renderToolCallSummary(call)}
                </li>
              ))}
            </ul>
          )}
        </section>
      </div>
    </aside>
  );
}

function renderToolCallSummary(call: AgentToolCallEvent) {
  const slug = (call.connector_slug || "").toLowerCase();
  const args = call.args_json || {};
  if (slug === "airflow") {
    const dagId = pickString(args, "dag_id");
    const runId = pickString(args, "run_id", "dag_run_id");
    const state = pickString(args, "state");
    const fields: Array<[string, string]> = [];
    if (dagId) fields.push(["DAG", dagId]);
    if (runId) fields.push(["Run", runId]);
    if (state) fields.push(["State", state]);
    if (fields.length > 0) {
      return (
        <dl className="tool-call-airflow">
          {fields.map(([label, value]) => (
            <div key={label}><dt>{label}</dt><dd>{value}</dd></div>
          ))}
          {call.result_summary ? <div><dt>Result</dt><dd>{call.result_summary}</dd></div> : null}
        </dl>
      );
    }
  }
  if (call.result_summary) {
    return <code className="tool-call-summary">{call.result_summary}</code>;
  }
  return null;
}

function pickString(args: Record<string, unknown>, ...keys: string[]): string | null {
  for (const key of keys) {
    const value = args[key];
    if (typeof value === "string" && value.trim()) return value;
  }
  return null;
}
