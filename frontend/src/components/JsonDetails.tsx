import { useMemo } from "react";

export function JsonDetails({
  label,
  value,
  open = false,
}: {
  label: string;
  value: unknown;
  open?: boolean;
}) {
  const pretty = useMemo(() => {
    try {
      return JSON.stringify(value, null, 2);
    } catch {
      return String(value);
    }
  }, [value]);
  return (
    <details className="json-details" open={open}>
      <summary>{label}</summary>
      <pre>{pretty}</pre>
    </details>
  );
}
