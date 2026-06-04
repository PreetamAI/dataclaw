import { useEffect, useMemo, useState } from "react";

import {
  useApplySuggestionMutation,
  useApproveEvalCaseMutation,
  useArchiveEvalCaseMutation,
  useBulkApproveEvalCasesMutation,
  useBulkArchiveEvalCasesMutation,
  useDiagnoseRunMutation,
  useDiagnoseStatusQuery,
  useDismissSuggestionMutation,
  useRevertSuggestionMutation,
  useEvalCasesQuery,
  useEvalDashboardQuery,
  useEvalProducersQuery,
  useEvalRunQuery,
  useEvalRunsQuery,
  useGenerateEvalCandidatesMutation,
  usePatchEvalCaseMutation,
  usePromoteGoldenEvalCaseMutation,
  useRunEvalBatchMutation,
  useRunSuggestionsQuery,
  useUnarchiveEvalCaseMutation,
} from "../services/api";
import type {
  EvalCase,
  EvalCaseStatus,
  EvalRun,
  EvalSuggestion,
  GenerateCandidatesResponse,
} from "../types";

type EvalsSection = "cases" | "runs" | "dashboard";

const STATUS_TABS: { key: EvalCaseStatus | "all"; label: string; pill: string }[] = [
  { key: "candidate", label: "Candidates", pill: "tab-candidate" },
  { key: "approved", label: "Approved", pill: "tab-approved" },
  { key: "golden", label: "Golden", pill: "tab-golden" },
  { key: "archived", label: "Archived", pill: "tab-archived" },
  { key: "all", label: "All", pill: "tab-all" },
];

export function Evals() {
  const [section, setSection] = useState<EvalsSection>("cases");
  return (
    <section className="gateway-view">
      <section className="gateway-panel">
        <header>
          <div>
            <h2>Evals</h2>
            <p>
              Manual + feedback-derived eval cases, scheduled / on-demand
              eval runs, and a dashboard summarising quality over time.
            </p>
          </div>
        </header>
        <div className="category-tabs evals-section-tabs" role="tablist">
          {(["cases", "runs", "dashboard"] as EvalsSection[]).map((s) => (
            <button
              key={s}
              aria-pressed={section === s}
              className={section === s ? "active" : ""}
              onClick={() => setSection(s)}
              type="button"
            >
              {s === "cases" ? "Cases" : s === "runs" ? "Runs" : "Dashboard"}
            </button>
          ))}
          <a
            className="settings-link evals-export-link"
            href="/evals/cases.promptfoo.yaml?status=golden"
          >
            Export golden cases → promptfoo.yaml ↗
          </a>
        </div>
        {section === "cases" ? <EvalsCases /> : null}
        {section === "runs" ? <EvalsRuns /> : null}
        {section === "dashboard" ? <EvalsDashboard /> : null}
      </section>
    </section>
  );
}


function EvalsCases() {
  const [tab, setTab] = useState<EvalCaseStatus | "all">("candidate");
  const [search, setSearch] = useState("");
  const { data: cases, isLoading } = useEvalCasesQuery(
    tab === "all" ? { q: search || undefined } : { status: tab, q: search || undefined },
  );

  const [activeId, setActiveId] = useState<string | null>(null);
  const active = useMemo(
    () => cases?.find((c) => c.id === activeId) ?? null,
    [cases, activeId],
  );

  const [checked, setChecked] = useState<Set<string>>(new Set());
  // Reset selection on tab/search change so users don't accidentally bulk-act
  // on rows they can no longer see.
  const visibleIds = useMemo(() => new Set((cases ?? []).map((c) => c.id)), [cases]);
  const checkedVisible = useMemo(
    () => Array.from(checked).filter((id) => visibleIds.has(id)),
    [checked, visibleIds],
  );

  const toggleChecked = (id: string) =>
    setChecked((prev) => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id);
      else next.add(id);
      return next;
    });
  const clearChecked = () => setChecked(new Set());

  const [bulkApprove, bulkApproveState] = useBulkApproveEvalCasesMutation();
  const [bulkArchive, bulkArchiveState] = useBulkArchiveEvalCasesMutation();
  const [bulkMessage, setBulkMessage] = useState<string | null>(null);

  const runBulk = async (
    fn: typeof bulkApprove,
    verb: string,
  ) => {
    if (checkedVisible.length === 0) return;
    setBulkMessage(null);
    const resp = await fn(checkedVisible).unwrap().catch(() => null);
    if (!resp) {
      setBulkMessage(`${verb} failed.`);
      return;
    }
    const okCount = resp.results.filter((r) => r.ok).length;
    const errCount = resp.results.length - okCount;
    setBulkMessage(
      errCount === 0
        ? `${verb} ${okCount} case${okCount === 1 ? "" : "s"}.`
        : `${verb} ${okCount} case${okCount === 1 ? "" : "s"}, ${errCount} skipped.`,
    );
    clearChecked();
  };

  const [runBatch, runBatchState] = useRunEvalBatchMutation();
  const onRunSelected = async () => {
    if (checkedVisible.length === 0) return;
    setBulkMessage(null);
    const resp = await runBatch({ case_ids: checkedVisible }).unwrap().catch(() => null);
    if (!resp) {
      setBulkMessage("Run failed to kick off.");
      return;
    }
    const abortSuffix = resp.aborted
      ? ` — ABORTED (${resp.abort_reason ?? "budget exceeded"})`
      : "";
    setBulkMessage(
      `Ran batch ${resp.batch_id.slice(0, 8)}…: ${resp.passed} passed, ${resp.failed} failed${
        resp.errored ? `, ${resp.errored} errored` : ""
      }${abortSuffix}.`,
    );
    clearChecked();
  };

  return (
    <>
      <div className="evals-cases-toolbar">
        <label className="integration-search">
          <input
            aria-label="Search eval cases"
            placeholder="Search question, SQL, answer…"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
          />
        </label>
      </div>

      <GenerateCandidatesPanel
        onComplete={(resp) => {
          setTab("candidate");
          clearChecked();
          setBulkMessage(
            resp.inserted_ids.length === 0
              ? "No new candidates (all duplicates or ceiling reached)."
              : `Inserted ${resp.inserted_ids.length} new candidate${
                  resp.inserted_ids.length === 1 ? "" : "s"
                }.`,
          );
        }}
      />

      <div className="category-tabs" role="tablist">
        {STATUS_TABS.map((t) => (
          <button
            key={t.key}
            aria-pressed={tab === t.key}
            className={tab === t.key ? "active" : ""}
            onClick={() => {
              setTab(t.key);
              setActiveId(null);
              clearChecked();
              setBulkMessage(null);
            }}
            type="button"
          >
            {t.label}
          </button>
        ))}
      </div>

      <div className="evals-bulkbar">
        <span>
          {checkedVisible.length === 0
            ? "Tick rows to bulk-act"
            : `${checkedVisible.length} selected`}
        </span>
        {tab === "candidate" || tab === "all" ? (
          <button
            type="button"
            onClick={() => runBulk(bulkApprove, "Approved")}
            disabled={checkedVisible.length === 0 || bulkApproveState.isLoading}
          >
            Approve selected
          </button>
        ) : null}
        <button
          type="button"
          onClick={onRunSelected}
          disabled={checkedVisible.length === 0 || runBatchState.isLoading}
          title="Execute a one-off batch against the selected cases"
        >
          Run selected
        </button>
        <button
          type="button"
          className="settings-link"
          onClick={() => runBulk(bulkArchive, "Archived")}
          disabled={checkedVisible.length === 0 || bulkArchiveState.isLoading}
        >
          Archive selected
        </button>
        {bulkMessage ? (
          <span className="settings-status-message">{bulkMessage}</span>
        ) : null}
      </div>

      <div className="evals-layout">
        <div className="evals-list">
          {isLoading ? (
            <div className="settings-empty">Loading…</div>
          ) : !cases || cases.length === 0 ? (
            <div className="settings-empty">
              No eval cases in this view. Send 👎 feedback on a chat answer
              or use “Generate candidates” above.
            </div>
          ) : (
            cases.map((c) => (
              <EvalCaseRow
                key={c.id}
                evalCase={c}
                selected={c.id === activeId}
                onSelect={() => setActiveId(c.id)}
                checked={checked.has(c.id)}
                onToggleCheck={() => toggleChecked(c.id)}
                showCheckbox={true}
              />
            ))
          )}
        </div>
        <div className="evals-detail">
          {active ? <EvalCaseDetail evalCase={active} /> : (
            <div className="settings-empty">
              Select a case to view, edit, and transition it.
            </div>
          )}
        </div>
      </div>
    </>
  );
}

function GenerateCandidatesPanel({
  onComplete,
}: {
  onComplete: (resp: GenerateCandidatesResponse) => void;
}) {
  const { data: catalog } = useEvalProducersQuery();
  const [generate, generateState] = useGenerateEvalCandidatesMutation();
  const [picked, setPicked] = useState<Set<string>>(new Set());
  const [limit, setLimit] = useState<number>(50);
  const [report, setReport] = useState<GenerateCandidatesResponse | null>(null);

  if (!catalog) {
    return <div className="evals-generate-panel">Loading producers…</div>;
  }

  const togglePicked = (slug: string) =>
    setPicked((prev) => {
      const next = new Set(prev);
      if (next.has(slug)) next.delete(slug);
      else next.add(slug);
      return next;
    });

  const onGenerate = async () => {
    setReport(null);
    const sources = picked.size > 0 ? Array.from(picked) : null;
    const resp = await generate({
      sources,
      limit_per_source: Math.max(1, Math.min(limit, catalog.default_limit_per_source)),
    })
      .unwrap()
      .catch(() => null);
    if (!resp) return;
    setReport(resp);
    onComplete(resp);
  };

  return (
    <div className="evals-generate-panel">
      <div className="evals-generate-controls">
        <strong>Generate candidates from:</strong>
        {catalog.producers.map((p) => (
          <label key={p.slug} className="evals-generate-source">
            <input
              type="checkbox"
              checked={picked.has(p.slug)}
              onChange={() => togglePicked(p.slug)}
            />
            {p.display_name}
          </label>
        ))}
        <label className="evals-generate-limit">
          <span>Per-source cap</span>
          <input
            type="number"
            min={1}
            max={catalog.default_limit_per_source}
            value={limit}
            onChange={(e) => setLimit(Number(e.target.value) || 1)}
          />
        </label>
        <button
          type="button"
          onClick={onGenerate}
          disabled={generateState.isLoading}
        >
          {picked.size === 0 ? "Generate (all sources)" : `Generate (${picked.size})`}
        </button>
        <span className="settings-status-message">
          Workspace ceiling: {catalog.workspace_candidate_ceiling} candidate rows.
        </span>
      </div>

      {report ? (
        <div className="evals-generate-report">
          <span>
            Queue: {report.candidates_in_queue_before} → {report.candidates_in_queue_after}
            {report.ceiling_reached ? " (ceiling reached)" : ""}
          </span>
          <ul>
            {report.producers.map((pr) => (
              <li key={pr.slug}>
                <strong>{pr.display_name}</strong>: produced {pr.produced}, inserted {pr.inserted}
                {pr.skipped_duplicate > 0 ? `, ${pr.skipped_duplicate} duplicate` : ""}
                {pr.skipped_cap > 0 ? `, ${pr.skipped_cap} skipped (cap)` : ""}
                {pr.error ? ` — error: ${pr.error}` : ""}
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  );
}

function EvalCaseRow({
  evalCase,
  selected,
  onSelect,
  checked,
  onToggleCheck,
  showCheckbox,
}: {
  evalCase: EvalCase;
  selected: boolean;
  onSelect: () => void;
  checked: boolean;
  onToggleCheck: () => void;
  showCheckbox: boolean;
}) {
  return (
    <div className={`evals-list-row ${selected ? "evals-list-row-selected" : ""}`}>
      {showCheckbox ? (
        <input
          type="checkbox"
          aria-label="Select for bulk action"
          checked={checked}
          onChange={onToggleCheck}
          className="evals-list-row-check"
          onClick={(e) => e.stopPropagation()}
        />
      ) : null}
      <button
        type="button"
        className="evals-list-row-body"
        onClick={onSelect}
      >
        <div className="evals-list-row-head">
          <span className={`evals-status-pill evals-status-${evalCase.status}`}>
            {evalCase.status}
          </span>
          <span className="evals-list-row-origin">{evalCase.origin}</span>
        </div>
        <div className="evals-list-row-question">{evalCase.question}</div>
        <div className="evals-list-row-meta">
          {evalCase.expected_connector_slug ? (
            <span>{evalCase.expected_connector_slug}</span>
          ) : null}
          {evalCase.expected_tool ? <span>{evalCase.expected_tool}</span> : null}
          {evalCase.tags.length > 0 ? <span>tags: {evalCase.tags.join(", ")}</span> : null}
        </div>
      </button>
    </div>
  );
}

function EvalCaseDetail({ evalCase }: { evalCase: EvalCase }) {
  const [question, setQuestion] = useState(evalCase.question);
  const [expectedAnswer, setExpectedAnswer] = useState(evalCase.expected_answer ?? "");
  const [expectedSql, setExpectedSql] = useState(evalCase.expected_sql ?? "");
  const [expectedConnector, setExpectedConnector] = useState(
    evalCase.expected_connector_slug ?? "",
  );
  const [expectedTool, setExpectedTool] = useState(evalCase.expected_tool ?? "");
  const [tags, setTags] = useState(evalCase.tags.join(", "));

  const [patch, patchState] = usePatchEvalCaseMutation();
  const [approve, approveState] = useApproveEvalCaseMutation();
  const [promote, promoteState] = usePromoteGoldenEvalCaseMutation();
  const [archive, archiveState] = useArchiveEvalCaseMutation();
  const [unarchive, unarchiveState] = useUnarchiveEvalCaseMutation();
  const [transitionError, setTransitionError] = useState<string | null>(null);

  const onSave = async () => {
    setTransitionError(null);
    await patch({
      id: evalCase.id,
      body: {
        question,
        expected_answer: expectedAnswer || null,
        expected_sql: expectedSql || null,
        expected_connector_slug: expectedConnector || null,
        expected_tool: expectedTool || null,
        tags: tags
          ? tags.split(",").map((t) => t.trim()).filter(Boolean)
          : [],
      },
    });
  };

  const wrapTransition = async (fn: () => Promise<unknown>) => {
    setTransitionError(null);
    try {
      await fn();
    } catch (err: unknown) {
      const detail =
        err && typeof err === "object" && "data" in err
          ? (err as { data?: { detail?: string } }).data?.detail
          : undefined;
      setTransitionError(detail || "Transition failed.");
    }
  };

  return (
    <div className="evals-detail-card">
      <header className="evals-detail-head">
        <span className={`evals-status-pill evals-status-${evalCase.status}`}>
          {evalCase.status}
        </span>
        <span className="evals-detail-origin">{evalCase.origin}</span>
      </header>

      <label className="settings-field">
        <span>Question</span>
        <input
          type="text"
          value={question}
          onChange={(e) => setQuestion(e.target.value)}
        />
      </label>

      <label className="settings-field">
        <span>Expected answer</span>
        <textarea
          rows={3}
          value={expectedAnswer}
          onChange={(e) => setExpectedAnswer(e.target.value)}
        />
      </label>

      <label className="settings-field">
        <span>Expected SQL {evalCase.status === "approved" ? "(required for golden)" : ""}</span>
        <textarea
          className="eval-correction-sql"
          rows={4}
          value={expectedSql}
          onChange={(e) => setExpectedSql(e.target.value)}
        />
      </label>

      <div className="eval-correction-row">
        <label className="settings-field">
          <span>Expected connector</span>
          <input
            type="text"
            value={expectedConnector}
            onChange={(e) => setExpectedConnector(e.target.value)}
          />
        </label>
        <label className="settings-field">
          <span>Expected tool</span>
          <input
            type="text"
            value={expectedTool}
            onChange={(e) => setExpectedTool(e.target.value)}
          />
        </label>
      </div>

      <label className="settings-field">
        <span>Tags (comma-separated)</span>
        <input type="text" value={tags} onChange={(e) => setTags(e.target.value)} />
      </label>

      <footer className="evals-detail-actions">
        <button type="button" onClick={onSave} disabled={patchState.isLoading}>
          Save edits
        </button>

        {evalCase.status === "candidate" ? (
          <button
            type="button"
            onClick={() => wrapTransition(() => approve(evalCase.id).unwrap())}
            disabled={approveState.isLoading}
          >
            Approve →
          </button>
        ) : null}

        {evalCase.status === "approved" ? (
          <button
            type="button"
            onClick={() => wrapTransition(() => promote(evalCase.id).unwrap())}
            disabled={promoteState.isLoading || !evalCase.expected_sql}
            title={!evalCase.expected_sql ? "Set expected_sql to promote to golden" : ""}
          >
            Promote to golden ★
          </button>
        ) : null}

        {evalCase.status !== "archived" ? (
          <button
            type="button"
            className="settings-link"
            onClick={() => wrapTransition(() => archive(evalCase.id).unwrap())}
            disabled={archiveState.isLoading}
          >
            Archive
          </button>
        ) : (
          <button
            type="button"
            className="settings-link"
            onClick={() => wrapTransition(() => unarchive(evalCase.id).unwrap())}
            disabled={unarchiveState.isLoading}
          >
            Unarchive
          </button>
        )}

        {transitionError ? (
          <span className="feedback-status feedback-status-error">{transitionError}</span>
        ) : null}
      </footer>
    </div>
  );
}


// ---------- Runs section ----------


function EvalsRuns() {
  const [activeRunId, setActiveRunId] = useState<string | null>(null);
  const [batchFilter, setBatchFilter] = useState<string>("");
  const [passedFilter, setPassedFilter] = useState<"all" | "passed" | "failed">("all");

  const { data: runs, isLoading } = useEvalRunsQuery({
    batch_id: batchFilter || undefined,
    passed: passedFilter === "all" ? undefined : passedFilter === "passed",
    limit: 200,
  });

  return (
    <>
      <div className="evals-runs-toolbar">
        <label className="settings-field">
          <span>Filter by batch_id</span>
          <input
            type="text"
            placeholder="(any)"
            value={batchFilter}
            onChange={(e) => setBatchFilter(e.target.value)}
          />
        </label>
        <label className="settings-field">
          <span>Pass / fail</span>
          <select
            value={passedFilter}
            onChange={(e) => setPassedFilter(e.target.value as typeof passedFilter)}
          >
            <option value="all">All</option>
            <option value="passed">Passed</option>
            <option value="failed">Failed</option>
          </select>
        </label>
      </div>

      <div className="evals-layout">
        <div className="evals-list">
          {isLoading ? (
            <div className="settings-empty">Loading runs…</div>
          ) : !runs || runs.length === 0 ? (
            <div className="settings-empty">
              No eval runs yet. From Cases, tick a few and click “Run selected”.
            </div>
          ) : (
            runs.map((run) => (
              <EvalRunRow
                key={run.id}
                run={run}
                selected={activeRunId === run.id}
                onSelect={() => setActiveRunId(run.id)}
              />
            ))
          )}
        </div>
        <div className="evals-detail">
          {activeRunId ? (
            <EvalRunDetailPanel runId={activeRunId} />
          ) : (
            <div className="settings-empty">
              Select a run to inspect expected vs actual, per-metric scores,
              and the trace.
            </div>
          )}
        </div>
      </div>
    </>
  );
}

function EvalRunRow({
  run,
  selected,
  onSelect,
}: {
  run: EvalRun;
  selected: boolean;
  onSelect: () => void;
}) {
  return (
    <button
      type="button"
      className={`evals-list-row-body ${selected ? "evals-run-row-selected" : ""}`}
      onClick={onSelect}
    >
      <div className="evals-list-row-head">
        <span
          className={`evals-status-pill ${
            run.passed ? "evals-status-passed" : "evals-status-failed"
          }`}
        >
          {run.error ? "errored" : run.passed ? "passed" : "failed"}
        </span>
        {run.failure_category ? (
          <span className="evals-list-row-origin">{run.failure_category}</span>
        ) : null}
      </div>
      <div className="evals-list-row-question">
        {run.actual_answer?.slice(0, 140) || (run.error ? `error: ${run.error}` : "(no answer)")}
      </div>
      <div className="evals-list-row-meta">
        <span>batch {run.batch_id.slice(0, 8)}</span>
        <span>{run.duration_ms} ms</span>
        {run.model ? <span>{run.model}</span> : null}
        {run.cost_usd !== null ? <span>${run.cost_usd.toFixed(4)}</span> : null}
        <span>{new Date(run.created_at).toLocaleString()}</span>
      </div>
    </button>
  );
}

function EvalRunDetailPanel({ runId }: { runId: string }) {
  const { data: detail, isLoading } = useEvalRunQuery(runId);
  if (isLoading || !detail) {
    return <div className="settings-empty">Loading run…</div>;
  }
  return (
    <div className="evals-detail-card">
      <header className="evals-detail-head">
        <span
          className={`evals-status-pill ${
            detail.passed ? "evals-status-passed" : "evals-status-failed"
          }`}
        >
          {detail.error ? "errored" : detail.passed ? "passed" : "failed"}
        </span>
        {detail.failure_category ? (
          <span className="evals-detail-origin">{detail.failure_category}</span>
        ) : null}
        <span className="evals-detail-origin">
          batch {detail.batch_id.slice(0, 8)} · {new Date(detail.created_at).toLocaleString()}
        </span>
      </header>

      <div className="evals-runs-question">
        <strong>Question</strong>
        <p>{detail.case.question}</p>
      </div>

      <div className="evals-runs-comparison">
        <ComparisonCard label="Expected" value={detail.case.expected_answer} sql={detail.case.expected_sql} />
        <ComparisonCard label="Actual" value={detail.actual_answer} sql={detail.actual_sql} />
      </div>

      <div className="evals-runs-metrics">
        <h4>Metrics</h4>
        <table>
          <thead>
            <tr>
              <th>Metric</th>
              <th>Status</th>
              <th>Score</th>
              <th>Passed</th>
              <th>Detail</th>
            </tr>
          </thead>
          <tbody>
            {detail.results.map((r) => (
              <tr key={r.metric}>
                <td>{r.metric}</td>
                <td>{r.status}</td>
                <td>{r.score === null ? "—" : Number(r.score).toFixed(3)}</td>
                <td>{r.passed === null ? "—" : r.passed ? "yes" : "no"}</td>
                <td>
                  <code>{JSON.stringify(r.detail).slice(0, 160)}</code>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="evals-runs-trace">
        <strong>Trace</strong>
        {detail.chat_message_id ? (
          <a
            className="settings-link"
            href={`/chat-messages/${detail.chat_message_id}/trace`}
            target="_blank"
            rel="noreferrer"
          >
            Open span tree →
          </a>
        ) : (
          <span className="feedback-trace-note">
            No chat_message_id (runner-side error).
          </span>
        )}
      </div>

      <RunSuggestionsPanel runId={runId} />
    </div>
  );
}


function RunSuggestionsPanel({ runId }: { runId: string }) {
  const { data: suggestions, isLoading } = useRunSuggestionsQuery(runId);
  const [diagnose, diagnoseState] = useDiagnoseRunMutation();
  const [diagnoseError, setDiagnoseError] = useState<string | null>(null);
  const [bgPolling, setBgPolling] = useState<boolean>(false);
  // Poll while a background diagnose is in flight; stop on completed/error.
  const { data: status } = useDiagnoseStatusQuery(runId, {
    skip: !bgPolling,
    pollingInterval: 2000,
  });
  // Stop polling + invalidate suggestions cache once the worker reports
  // a terminal state.
  useEffect(() => {
    if (!bgPolling || !status) return;
    if (status.status !== "running") {
      setBgPolling(false);
    }
  }, [bgPolling, status]);

  const onDiagnose = async () => {
    setDiagnoseError(null);
    const result = await diagnose({ runId, body: { use_llm: true } });
    if ("error" in result) {
      setDiagnoseError("Diagnose failed. Check server logs.");
    }
  };

  const onDiagnoseBackground = async () => {
    setDiagnoseError(null);
    setBgPolling(true);
    const result = await diagnose({
      runId,
      body: { use_llm: true, background: true },
    });
    if ("error" in result) {
      setDiagnoseError("Diagnose failed to start. Check server logs.");
      setBgPolling(false);
    }
  };

  const hasSuggestions = (suggestions?.length ?? 0) > 0;

  return (
    <div className="evals-suggestions">
      <header className="evals-suggestions-head">
        <strong>Improvement suggestions</strong>
        <button
          type="button"
          onClick={onDiagnose}
          disabled={diagnoseState.isLoading || bgPolling}
        >
          {hasSuggestions ? "Re-diagnose" : "Diagnose this run"}
        </button>
        <button
          type="button"
          className="settings-link"
          onClick={onDiagnoseBackground}
          disabled={diagnoseState.isLoading || bgPolling}
          title="Kicks diagnose in the background. The page won't block on the LLM round-trip."
        >
          {bgPolling
            ? `Working… (${status?.status ?? "running"})`
            : "Run in background"}
        </button>
        {diagnoseError ? (
          <span className="feedback-status feedback-status-error">
            {diagnoseError}
          </span>
        ) : null}
      </header>
      {isLoading ? (
        <div className="feedback-trace-note">Loading…</div>
      ) : !hasSuggestions ? (
        <div className="feedback-trace-note">
          No suggestions yet. Click <em>Diagnose this run</em> to generate
          rules-based + LLM-augmented suggestions across prompt, golden
          query, rules, connector routing, tool descriptions, and retrieval
          context. None are applied automatically.
        </div>
      ) : (
        <ul className="evals-suggestions-list">
          {suggestions!.map((s) => (
            <SuggestionRow key={s.id} suggestion={s} />
          ))}
        </ul>
      )}
    </div>
  );
}

function SuggestionRow({ suggestion }: { suggestion: EvalSuggestion }) {
  const [apply, applyState] = useApplySuggestionMutation();
  const [dismiss, dismissState] = useDismissSuggestionMutation();
  const [revert, revertState] = useRevertSuggestionMutation();
  const [error, setError] = useState<string | null>(null);

  const onApply = async () => {
    setError(null);
    const result = await apply(suggestion.id);
    if ("error" in result) {
      const data =
        (result.error as { data?: { detail?: string } }).data?.detail ||
        "Apply failed.";
      setError(String(data));
    }
  };
  const onDismiss = async () => {
    setError(null);
    await dismiss(suggestion.id);
  };
  const onRevert = async () => {
    setError(null);
    const result = await revert(suggestion.id);
    if ("error" in result) {
      const data =
        (result.error as { data?: { detail?: string } }).data?.detail ||
        "Revert failed.";
      setError(String(data));
    }
  };

  const revertSupported =
    suggestion.status === "applied" &&
    (suggestion.apply_result.revert_supported === true ||
      suggestion.apply_result.status === "preview_only");
  const statusClass =
    suggestion.status === "applied"
      ? "evals-status-passed"
      : suggestion.status === "dismissed"
        ? "evals-status-archived"
        : suggestion.status === "reverted"
          ? "evals-status-archived"
          : "evals-status-candidate";

  return (
    <li className="evals-suggestion-row">
      <div className="evals-suggestion-head">
        <span className={`evals-status-pill ${statusClass}`}>{suggestion.status}</span>
        <span className="evals-suggestion-kind">{suggestion.kind}</span>
        <span className="evals-list-row-origin">{suggestion.source}</span>
        <span className="evals-list-row-origin">
          confidence {(suggestion.confidence * 100).toFixed(0)}%
        </span>
        {!suggestion.apply_supported ? (
          <span className="evals-suggestion-badge">preview only</span>
        ) : null}
      </div>
      <h5 className="evals-suggestion-title">{suggestion.title}</h5>
      <p className="evals-suggestion-rationale">{suggestion.rationale}</p>
      {suggestion.current_value ? (
        <details className="evals-suggestion-diff">
          <summary>Current → Proposed</summary>
          <div className="evals-suggestion-diff-grid">
            <pre className="evals-suggestion-diff-from">
              {suggestion.current_value}
            </pre>
            <pre className="evals-suggestion-diff-to">
              {suggestion.proposed_value}
            </pre>
          </div>
        </details>
      ) : (
        <pre className="evals-suggestion-proposed">{suggestion.proposed_value}</pre>
      )}
      {suggestion.status === "applied" && suggestion.apply_result.status === "preview_only" ? (
        <span className="feedback-trace-note">
          {String(suggestion.apply_result.message ?? "Preview only — Apply path not yet wired.")}
        </span>
      ) : null}
      {suggestion.status === "applied" &&
      typeof suggestion.apply_result.created_eval_case_id === "string" ? (
        <span className="feedback-status feedback-status-ok">
          Created golden case {String(suggestion.apply_result.created_eval_case_id).slice(0, 8)}…
        </span>
      ) : null}
      {suggestion.status === "pending" ? (
        <div className="evals-suggestion-actions">
          <button
            type="button"
            onClick={onApply}
            disabled={applyState.isLoading}
          >
            Apply
          </button>
          <button
            type="button"
            className="settings-link"
            onClick={onDismiss}
            disabled={dismissState.isLoading}
          >
            Dismiss
          </button>
          {error ? (
            <span className="feedback-status feedback-status-error">{error}</span>
          ) : null}
        </div>
      ) : revertSupported ? (
        <div className="evals-suggestion-actions">
          <button
            type="button"
            className="settings-link"
            onClick={onRevert}
            disabled={revertState.isLoading}
            title="Undo this apply (archives the created case / restores prior prompt / removes the rule line)"
          >
            Revert
          </button>
          {error ? (
            <span className="feedback-status feedback-status-error">{error}</span>
          ) : null}
        </div>
      ) : null}
    </li>
  );
}

function ComparisonCard({
  label,
  value,
  sql,
}: {
  label: string;
  value: string | null;
  sql: string | null;
}) {
  return (
    <div className="evals-runs-compare-card">
      <h5>{label}</h5>
      <p>{value || <em>(none)</em>}</p>
      {sql ? <pre className="evals-runs-compare-sql">{sql}</pre> : null}
    </div>
  );
}


// ---------- Dashboard section ----------


function EvalsDashboard() {
  const [rangeDays, setRangeDays] = useState<number>(7);
  const { data: dash, isLoading } = useEvalDashboardQuery(rangeDays);

  return (
    <>
      <div className="evals-dashboard-toolbar">
        <label className="settings-field">
          <span>Range</span>
          <select
            value={rangeDays}
            onChange={(e) => setRangeDays(Number(e.target.value))}
          >
            <option value={1}>Last 24h</option>
            <option value={7}>Last 7 days</option>
            <option value={30}>Last 30 days</option>
            <option value={90}>Last 90 days</option>
          </select>
        </label>
      </div>

      {isLoading || !dash ? (
        <div className="settings-empty">Loading dashboard…</div>
      ) : dash.total_runs === 0 ? (
        <div className="settings-empty">
          No eval runs in this window. Kick a batch from the Cases tab to
          populate the dashboard.
        </div>
      ) : (
        <>
          <div className="evals-dashboard-tiles">
            <DashTile label="Total runs" value={String(dash.total_runs)} />
            <DashTile
              label="Pass rate"
              value={
                dash.pass_rate === null
                  ? "—"
                  : `${(dash.pass_rate * 100).toFixed(1)}%`
              }
            />
            <DashTile
              label="Latency p50"
              value={dash.p50_latency_ms ? `${dash.p50_latency_ms.toFixed(0)} ms` : "—"}
            />
            <DashTile
              label="Latency p95"
              value={dash.p95_latency_ms ? `${dash.p95_latency_ms.toFixed(0)} ms` : "—"}
            />
            <DashTile label="Total cost" value={`$${dash.total_cost_usd.toFixed(4)}`} />
            <DashTile label="Total tokens" value={dash.total_tokens.toLocaleString()} />
            <DashTile label="Regressions" value={String(dash.regression_count)} />
          </div>

          {Object.keys(dash.failure_category_counts).length > 0 ? (
            <section className="evals-dashboard-section">
              <h4>Failure categories</h4>
              <ul className="evals-dashboard-fc">
                {Object.entries(dash.failure_category_counts).map(([cat, n]) => (
                  <li key={cat}>
                    <span className="evals-status-pill evals-status-failed">{cat}</span>
                    {n}
                  </li>
                ))}
              </ul>
            </section>
          ) : null}

          <section className="evals-dashboard-section">
            <h4>Metrics ({dash.range_days}d window)</h4>
            <table className="evals-dashboard-table">
              <thead>
                <tr>
                  <th>Metric</th>
                  <th>Mean</th>
                  <th>Pass rate</th>
                  <th>Sample size</th>
                </tr>
              </thead>
              <tbody>
                {dash.metrics.map((m) => (
                  <tr key={m.metric}>
                    <td>{m.metric}</td>
                    <td>{m.mean === null ? "—" : m.mean.toFixed(3)}</td>
                    <td>
                      {m.pass_rate === null ? "—" : `${(m.pass_rate * 100).toFixed(1)}%`}
                    </td>
                    <td>{m.sample_size}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>

          <section className="evals-dashboard-section">
            <h4>Daily</h4>
            <table className="evals-dashboard-table">
              <thead>
                <tr>
                  <th>Date</th>
                  <th>Runs</th>
                  <th>Passed</th>
                  <th>Failed</th>
                  <th>Avg latency</th>
                  <th>Avg cost</th>
                </tr>
              </thead>
              <tbody>
                {dash.daily.map((d) => (
                  <tr key={d.date}>
                    <td>{d.date}</td>
                    <td>{d.runs}</td>
                    <td>{d.passed}</td>
                    <td>{d.failed}</td>
                    <td>{d.avg_duration_ms ? `${d.avg_duration_ms.toFixed(0)} ms` : "—"}</td>
                    <td>{d.avg_cost_usd !== null ? `$${d.avg_cost_usd.toFixed(4)}` : "—"}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </section>
        </>
      )}
    </>
  );
}

function DashTile({ label, value }: { label: string; value: string }) {
  return (
    <div className="evals-dashboard-tile">
      <strong>{value}</strong>
      <span>{label}</span>
    </div>
  );
}
