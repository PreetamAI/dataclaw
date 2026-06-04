import { useState } from "react";

import { Connectors } from "./Connectors";
import { Evals } from "./Evals";
import { Gateway } from "./Gateway";
import { IDE } from "./IDE";
import { Agents } from "./Agents";
import { BackgroundAgents } from "./BackgroundAgents";
import { Knowledge } from "./Knowledge";
import { Sessions } from "./Sessions";
import { Settings } from "./Settings";
import { Sidebar } from "./Sidebar";
import { FirstRun } from "./Onboarding/FirstRun";
import { useWorkerStatusQuery } from "../services/api";
import type {
  Connector,
  Dashboard,
  TabName,
  TableAsset,
  Workspace as WorkspaceType,
} from "../types";

type WorkspaceProps = {
  connectors: Connector[];
  workspace?: WorkspaceType;
  dashboard?: Dashboard;
  selectedTable?: TableAsset;
  tab: TabName;
  setTab: (value: TabName) => void;
  activeThreadId: string | null;
  setActiveThreadId: (value: string | null) => void;
  setSelectedTableId: (value: string) => void;
  refresh: () => Promise<void>;
  error: string;
  setError: (value: string) => void;
  notice?: string;
};

const BREADCRUMBS: Record<TabName, [string, string]> = {
  Editor: ["Editor", "Chat"],
  Connectors: ["Knowledge base", "Connectors"],
  Knowledge: ["Knowledge base", "Brain"],
  Settings: ["Settings", "LLM provider"],
  Monitoring: ["Agents", "Monitoring"],
  Gateway: ["Gateway", "Observability"],
  Agents: ["Agents", "Configuration"],
  Evals: ["Quality", "Evals"],
};

export function Workspace(props: WorkspaceProps) {
  const [collapsed, setCollapsed] = useState(false);
  const workerQuery = useWorkerStatusQuery(undefined, { pollingInterval: 15000 });
  const previewMode = props.tab === "Knowledge" && new URLSearchParams(window.location.search).get("preview") === "1";
  const datasets = props.workspace?.datasets ?? [];
  const docs = props.workspace?.knowledge_documents ?? [];
  const hasKnowledgeBase = datasets.length > 0 || docs.length > 0;
  const alertBadge = props.dashboard?.alerts?.filter((a) => !a.resolved).length ?? 0;
  const workerStatus = workerQuery.data?.status ?? "missing";
  const [section, leaf] = BREADCRUMBS[props.tab];

  if (previewMode) {
    return (
      <main className="workspace preview-workspace">
        <section className="workspace-pane">
          <div className="pane-body">
            <Knowledge />
          </div>
        </section>
      </main>
    );
  }

  return (
    <main className="workspace">
      <Sidebar
        tab={props.tab}
        setTab={props.setTab}
        collapsed={collapsed}
        setCollapsed={setCollapsed}
        alertBadge={alertBadge}
        workerStatus={workerStatus}
      />

      {props.tab === "Editor" ? (
        <Sessions
          activeThreadId={props.activeThreadId}
          setActiveThreadId={props.setActiveThreadId}
        />
      ) : null}

      <section className="workspace-pane">
        <header className="pane-head">
          <nav className="breadcrumb" aria-label="Breadcrumb">
            <span className="breadcrumb-root">DataClaw</span>
            <span className="breadcrumb-sep">›</span>
            <span className="breadcrumb-section">{section}</span>
            <span className="breadcrumb-sep">›</span>
            <strong className="breadcrumb-leaf">{leaf}</strong>
          </nav>
          <div className="pane-head-end">
            <span className="topbar-avatar" aria-hidden="true">S</span>
          </div>
        </header>

        {props.notice ? <p className="notice pane-error">{props.notice}</p> : null}
        {props.error ? <p className="error pane-error">{props.error}</p> : null}

        <div className="pane-body">
          <FirstRun
            connectors={props.connectors}
            workspace={props.workspace}
            setTab={props.setTab}
            refresh={props.refresh}
          />
          {props.tab === "Editor" ? (
            <IDE
              activeThreadId={props.activeThreadId}
              setActiveThreadId={props.setActiveThreadId}
              hasKnowledgeBase={hasKnowledgeBase}
              onError={props.setError}
              setTab={props.setTab}
            />
          ) : props.tab === "Gateway" ? (
            <Gateway />
          ) : props.tab === "Settings" ? (
            <Settings />
          ) : props.tab === "Monitoring" ? (
            <BackgroundAgents />
          ) : props.tab === "Agents" ? (
            <Agents />
          ) : props.tab === "Knowledge" ? (
            <Knowledge />
          ) : props.tab === "Evals" ? (
            <Evals />
          ) : (
            <Connectors
              connectors={props.connectors}
              dashboard={props.dashboard}
              refresh={props.refresh}
            />
          )}
        </div>
      </section>
    </main>
  );
}
