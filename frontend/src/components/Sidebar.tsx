import {
  Activity,
  Brain,
  CheckCircle2,
  ChevronDown,
  ChevronRight,
  ChevronsLeft,
  ChevronsRight,
  Clock,
  ExternalLink,
  FileCog,
  type LucideIcon,
  MessageSquare,
  Plug,
  ShieldCheck,
} from "lucide-react";
import { useState } from "react";

import type { TabName } from "../types";

type NavItem = {
  key: TabName;
  label: string;
  icon: LucideIcon;
};

type Section = {
  label: string;
  items: NavItem[];
};

const SECTIONS: Section[] = [
  {
    label: "Editor",
    items: [{ key: "Editor", label: "Chat", icon: MessageSquare }],
  },
  {
    label: "Knowledge base",
    items: [
      { key: "Connectors", label: "Connectors", icon: Plug },
      { key: "Knowledge", label: "Brain", icon: Brain },
    ],
  },
  {
    label: "Settings",
    items: [{ key: "Settings", label: "LLM provider", icon: FileCog }],
  },
  {
    label: "Agents",
    items: [
      { key: "Agents", label: "On-demand", icon: ShieldCheck },
      { key: "Monitoring", label: "Background", icon: Clock },
    ],
  },
  {
    label: "Gateway",
    items: [{ key: "Gateway", label: "Observability", icon: Activity }],
  },
  {
    label: "Quality",
    items: [{ key: "Evals", label: "Evals", icon: CheckCircle2 }],
  },
];

type SidebarProps = {
  tab: TabName;
  setTab: (tab: TabName) => void;
  collapsed: boolean;
  setCollapsed: (value: boolean) => void;
  alertBadge?: number;
  workerStatus?: string;
};

export function Sidebar({
  tab,
  setTab,
  collapsed,
  setCollapsed,
  alertBadge = 0,
  workerStatus = "missing",
}: SidebarProps) {
  const [openSections, setOpenSections] = useState<Set<string>>(
    new Set(SECTIONS.map((s) => s.label)),
  );

  function toggleSection(label: string) {
    setOpenSections((prev) => {
      const next = new Set(prev);
      if (next.has(label)) next.delete(label);
      else next.add(label);
      return next;
    });
  }

  return (
    <aside className={`sidebar ${collapsed ? "collapsed" : ""}`}>
      <header className="sidebar-head">
        <div className="sidebar-brand">
          <span className="sidebar-mark">
            <img alt="" src="/brand/dataclaw-mark-dark.png" />
          </span>
          {!collapsed ? (
            <div>
              <em>CONTROL</em>
              <strong>DataClaw</strong>
            </div>
          ) : null}
        </div>
        <button
          aria-label={collapsed ? "Expand sidebar" : "Collapse sidebar"}
          className="sidebar-toggle"
          onClick={() => setCollapsed(!collapsed)}
          type="button"
        >
          {collapsed ? <ChevronsRight size={14} /> : <ChevronsLeft size={14} />}
        </button>
      </header>

      <div className="sidebar-scroll">
        {SECTIONS.map((section) => {
          const open = openSections.has(section.label);
          return (
            <div className="sidebar-section" key={section.label}>
              {!collapsed ? (
                <button
                  className="sidebar-section-head"
                  onClick={() => toggleSection(section.label)}
                  type="button"
                >
                  <span>{section.label.toUpperCase()}</span>
                  {open ? <ChevronDown size={12} /> : <ChevronRight size={12} />}
                </button>
              ) : null}
              {open || collapsed
                ? section.items.map((item) => {
                    const Icon = item.icon;
                    const showBadge = item.key === "Gateway" && alertBadge > 0;
                    const showWorkerDot = item.key === "Monitoring";
                    return (
                      <button
                        aria-pressed={tab === item.key}
                        className={`sidebar-item ${tab === item.key ? "active" : ""}`}
                        key={item.key}
                        onClick={() => setTab(item.key)}
                        title={item.label}
                        type="button"
                      >
                        <Icon size={15} />
                        {!collapsed ? <span>{item.label}</span> : null}
                        {showBadge && !collapsed ? (
                          <span className="sidebar-badge">{alertBadge}</span>
                        ) : null}
                        {showWorkerDot ? (
                          <span className={`sidebar-worker-dot ${workerStatus}`} aria-label={`Worker ${workerStatus}`} />
                        ) : null}
                      </button>
                    );
                  })
                : null}
            </div>
          );
        })}
      </div>

      <footer className="sidebar-footer">
        <a
          className="sidebar-doclink"
          href="https://github.com"
          rel="noreferrer noopener"
          target="_blank"
        >
          <ExternalLink size={13} />
          {!collapsed ? <span>Docs</span> : null}
        </a>
        {!collapsed ? (
          <div className="sidebar-version">
            <em>VERSION</em>
            <strong>v0.2.0</strong>
            <span className={`sidebar-status ${workerStatus}`} aria-label={`Worker ${workerStatus}`} />
          </div>
        ) : null}
      </footer>
    </aside>
  );
}
