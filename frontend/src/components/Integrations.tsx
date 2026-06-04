import { useEffect, useMemo, useState } from "react";

import {
  useDisableObservabilityProviderMutation,
  useObservabilityProvidersQuery,
  useTestObservabilityProviderMutation,
  useUpsertObservabilityProviderMutation,
} from "../services/api";
import type {
  ObservabilityCatalogItem,
  ObservabilityRecord,
} from "../types";

export function Integrations({ search }: { search: string }) {
  const { data, isLoading, error } = useObservabilityProvidersQuery();

  if (isLoading) {
    return <div className="settings-empty">Loading integrations…</div>;
  }
  if (error || !data) {
    return (
      <div className="settings-empty">
        Could not load integrations. Make sure you are signed in as an admin.
      </div>
    );
  }

  const term = search.trim().toLowerCase();
  const items = data.catalog.filter((item) =>
    !term
      ? true
      : item.display_name.toLowerCase().includes(term) ||
        item.description.toLowerCase().includes(term),
  );

  if (items.length === 0) {
    return <div className="settings-empty">No integrations match your search.</div>;
  }

  return (
    <div className="settings-grid">
      {items.map((item) => (
        <ObservabilityCard
          key={item.slug}
          item={item}
          record={data.records[item.slug]}
        />
      ))}
    </div>
  );
}

function ObservabilityCard({
  item,
  record,
}: {
  item: ObservabilityCatalogItem;
  record: ObservabilityRecord | undefined;
}) {
  const [upsert, upsertState] = useUpsertObservabilityProviderMutation();
  const [disable, disableState] = useDisableObservabilityProviderMutation();
  const [runTest, testState] = useTestObservabilityProviderMutation();

  const initialValues = useMemo(() => buildInitialValues(item, record), [item, record]);
  const [values, setValues] = useState<Record<string, string>>(initialValues);
  const [enabled, setEnabled] = useState<boolean>(Boolean(record?.enabled));
  const [testResult, setTestResult] = useState<string | null>(null);

  // Re-sync local form when the upstream record changes (e.g. after disable).
  useEffect(() => {
    setValues(initialValues);
    setEnabled(Boolean(record?.enabled));
  }, [initialValues, record?.enabled]);

  const onSave = async () => {
    setTestResult(null);
    const payloadValues: Record<string, string | boolean | null> = { enabled };
    for (const field of item.fields) {
      const current = values[field.name] ?? "";
      if (field.secret && !current) {
        // Leave existing encrypted secret untouched.
        continue;
      }
      payloadValues[field.name] = current.trim() || null;
    }
    await upsert({ slug: item.slug, body: { values: payloadValues } });
  };

  const onDisable = async () => {
    setTestResult(null);
    await disable(item.slug);
  };

  const onTest = async () => {
    setTestResult(null);
    const result = await runTest(item.slug).unwrap().catch(() => null);
    if (result) {
      setTestResult(`${result.status}: ${result.message}`);
    } else {
      setTestResult("Test failed.");
    }
  };

  const statusLabel = !record?.configured
    ? "Not configured"
    : record.enabled
      ? "Enabled"
      : "Disabled";

  return (
    <article className="settings-card">
      <header className="settings-card-header">
        <div>
          <h3>{item.display_name}</h3>
          <p>{item.description}</p>
        </div>
        <span
          className={`settings-status ${
            record?.enabled ? "settings-status-on" : "settings-status-off"
          }`}
        >
          {statusLabel}
        </span>
      </header>

      <div className="settings-fields">
        {item.fields.map((field) => (
          <label key={field.name} className="settings-field">
            <span>
              {field.label}
              {field.required ? " *" : ""}
              {field.secret && record?.secrets_set.includes(field.name) ? (
                <em className="settings-secret-hint">
                  {" "}
                  (set: {record.secret_previews[field.name]})
                </em>
              ) : null}
            </span>
            <input
              type={field.secret ? "password" : "text"}
              placeholder={
                field.secret && record?.secrets_set.includes(field.name)
                  ? "Leave blank to keep current value"
                  : field.placeholder
              }
              value={values[field.name] ?? ""}
              onChange={(event) =>
                setValues((prev) => ({ ...prev, [field.name]: event.target.value }))
              }
            />
          </label>
        ))}

        <label className="settings-field settings-field-checkbox">
          <input
            type="checkbox"
            checked={enabled}
            onChange={(event) => setEnabled(event.target.checked)}
          />
          <span>Forward chat traces to {item.display_name}</span>
        </label>
      </div>

      <footer className="settings-card-footer">
        <button type="button" onClick={onSave} disabled={upsertState.isLoading}>
          Save
        </button>
        <button
          type="button"
          onClick={onTest}
          disabled={testState.isLoading || !record?.configured}
        >
          Test
        </button>
        <button
          type="button"
          className="settings-link"
          onClick={onDisable}
          disabled={disableState.isLoading || !record?.enabled}
        >
          Disable
        </button>
        <a href={item.docs_url} target="_blank" rel="noreferrer" className="settings-link">
          Docs ↗
        </a>
        {testResult ? <span className="settings-status-message">{testResult}</span> : null}
      </footer>
    </article>
  );
}

function buildInitialValues(
  item: ObservabilityCatalogItem,
  record: ObservabilityRecord | undefined,
): Record<string, string> {
  const base: Record<string, string> = {};
  for (const field of item.fields) {
    if (field.secret) {
      base[field.name] = "";
      continue;
    }
    base[field.name] = record?.values[field.name] ?? "";
  }
  return base;
}
