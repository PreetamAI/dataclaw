import { AlertTriangle, ExternalLink, X } from "lucide-react";

import { JsonDetails } from "./JsonDetails";
import type { ChatResponse } from "../types";

type Props = {
  response: ChatResponse | null;
  onClose: () => void;
  onOpenObservability?: () => void;
};

export function WriteToolPreview({ response, onClose, onOpenObservability }: Props) {
  if (!response || response.status !== "pending_approval") return null;
  const toolCall = response.tool_call ?? {};
  const result = response.tool_result ?? {};
  const connector = String(toolCall.connector_slug ?? "connector");
  const tool = String(toolCall.tool ?? "write tool");

  return (
    <aside className="write-preview" aria-label="Write tool preview">
      <header className="write-preview-head">
        <div>
          <p className="eyebrow">Pending approval</p>
          <h2>{connector}.{tool}</h2>
        </div>
        <button aria-label="Close preview" className="icon-button" onClick={onClose} type="button">
          <X size={16} />
        </button>
      </header>
      <div className="write-preview-body">
        <div className="write-preview-warning">
          <AlertTriangle size={16} />
          <span>This write has not executed yet. Review and approve it in Observability.</span>
        </div>
        <dl className="write-preview-grid">
          <div><dt>Connector</dt><dd>{connector}</dd></div>
          <div><dt>Tool</dt><dd>{tool}</dd></div>
          {response.alert_id ? <div><dt>Alert</dt><dd>{response.alert_id}</dd></div> : null}
          <div><dt>Status</dt><dd>{response.status}</dd></div>
        </dl>
        <section>
          <h3>Payload</h3>
          <JsonDetails label="Show raw payload" value={result} />
        </section>
        <div className="write-preview-actions">
          <button className="primary" onClick={onOpenObservability} type="button">
            <ExternalLink size={13} /> Open Observability
          </button>
        </div>
      </div>
    </aside>
  );
}
