import { useEffect, useState } from "react";

import {
  useEvalConfigQuery,
  useUpdateEvalConfigMutation,
} from "../services/api";

const STATUS_OPTIONS = ["candidate", "approved", "golden"] as const;

export function EvalConfigCard() {
  const { data: config, isLoading } = useEvalConfigQuery();
  const [update, updateState] = useUpdateEvalConfigMutation();

  const [scheduleEnabled, setScheduleEnabled] = useState(false);
  const [intervalMinutes, setIntervalMinutes] = useState<number>(60);
  const [statusFilter, setStatusFilter] = useState<Set<string>>(
    new Set(["approved", "golden"]),
  );
  const [budget, setBudget] = useState<number>(0);
  const [judgesEnabled, setJudgesEnabled] = useState<boolean>(true);
  const [maxConcurrency, setMaxConcurrency] = useState<number>(4);
  const [message, setMessage] = useState<string | null>(null);

  // Hydrate the form from server state once it lands.
  useEffect(() => {
    if (!config) return;
    setScheduleEnabled(config.schedule_enabled);
    setIntervalMinutes(config.schedule_interval_minutes);
    setStatusFilter(new Set(config.schedule_status_filter));
    setBudget(config.batch_max_cost_usd);
    setJudgesEnabled(config.judges_enabled);
    setMaxConcurrency(config.batch_max_concurrency);
  }, [config]);

  const onSave = async () => {
    setMessage(null);
    const result = await update({
      schedule_enabled: scheduleEnabled,
      schedule_interval_minutes: intervalMinutes,
      schedule_status_filter: Array.from(statusFilter),
      batch_max_cost_usd: budget,
      judges_enabled: judgesEnabled,
      batch_max_concurrency: maxConcurrency,
    });
    if ("error" in result) {
      setMessage("Save failed.");
    } else {
      setMessage("Saved.");
    }
  };

  const toggleStatus = (status: string) =>
    setStatusFilter((prev) => {
      const next = new Set(prev);
      if (next.has(status)) next.delete(status);
      else next.add(status);
      return next;
    });

  if (isLoading) {
    return <div className="settings-empty">Loading eval config…</div>;
  }

  return (
    <article className="settings-card">
      <header className="settings-card-header">
        <div>
          <h3>Evals</h3>
          <p>
            Schedule batches, cap per-batch cost, and toggle Ragas-backed
            LLM-judge metrics (faithfulness, answer relevancy, safety).
          </p>
        </div>
        <span
          className={`settings-status ${
            scheduleEnabled ? "settings-status-on" : "settings-status-off"
          }`}
        >
          {scheduleEnabled ? "Schedule on" : "Manual only"}
        </span>
      </header>

      <div className="settings-fields">
        <label className="settings-field settings-field-checkbox">
          <input
            type="checkbox"
            checked={scheduleEnabled}
            onChange={(e) => setScheduleEnabled(e.target.checked)}
          />
          <span>Run scheduled batches in the background worker</span>
        </label>

        <label className="settings-field">
          <span>Schedule interval (minutes — clamped 5..1440)</span>
          <input
            type="number"
            min={5}
            max={24 * 60}
            value={intervalMinutes}
            onChange={(e) => setIntervalMinutes(Number(e.target.value) || 60)}
          />
        </label>

        <div className="settings-field">
          <span>Schedule status filter (which case statuses run)</span>
          <div className="eval-correction-citations">
            {STATUS_OPTIONS.map((s) => (
              <label key={s} className="eval-correction-citation">
                <input
                  type="checkbox"
                  checked={statusFilter.has(s)}
                  onChange={() => toggleStatus(s)}
                />
                {s}
              </label>
            ))}
          </div>
        </div>

        <label className="settings-field">
          <span>Per-batch cost ceiling (USD — 0 means no limit)</span>
          <input
            type="number"
            step="0.01"
            min={0}
            value={budget}
            onChange={(e) => setBudget(Number(e.target.value) || 0)}
          />
        </label>

        <label className="settings-field">
          <span>
            Batch concurrency (1 = serial; clamped 1..16). Higher = faster
            batches but the cost cap becomes a soft cap with up to
            (concurrency−1) cases of overshoot.
          </span>
          <input
            type="number"
            min={1}
            max={16}
            value={maxConcurrency}
            onChange={(e) => setMaxConcurrency(Number(e.target.value) || 1)}
          />
        </label>

        <label className="settings-field settings-field-checkbox">
          <input
            type="checkbox"
            checked={judgesEnabled}
            onChange={(e) => setJudgesEnabled(e.target.checked)}
          />
          <span>
            Enable Ragas LLM-judge metrics (faithfulness, answer relevancy,
            safety) — requires the [evals] extra installed and a configured
            LLM provider.
          </span>
        </label>
      </div>

      <footer className="settings-card-footer">
        <button type="button" onClick={onSave} disabled={updateState.isLoading}>
          Save
        </button>
        {message ? <span className="settings-status-message">{message}</span> : null}
      </footer>
    </article>
  );
}
