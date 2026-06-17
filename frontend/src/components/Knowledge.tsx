import { ChevronRight, ExternalLink, GitBranch, Library, RefreshCw, Search } from "lucide-react";
import { useMemo, useState } from "react";

import { errorMessage } from "../lib/errors";
import { extractHeadings } from "../lib/toc";
import { useCompileKnowledgeMutation, useKnowledgePagesQuery } from "../services/api";
import type { KnowledgeNode, WikiPage } from "../types";
import { KnowledgeGraph } from "./KnowledgeGraph";
import { WikiPageView } from "./WikiPageView";

type Mode = "Pages" | "Graph";

const OPEN_GROUPS_KEY = "brain.openGroups";

const SOURCE_DOT: Record<string, string> = {
  airflow: "#ea580c",
  notion: "#16a34a",
  postgres: "#2563eb",
  sqlite: "#0ea5e9",
};

const OVERVIEW_PREVIEW = 6;

function groupPages(pages: WikiPage[]) {
  return pages.reduce<Record<string, WikiPage[]>>((acc, page) => {
    acc[page.source_type] = [...(acc[page.source_type] ?? []), page];
    return acc;
  }, {});
}

function loadOpenGroups(): Record<string, boolean> {
  try {
    return JSON.parse(localStorage.getItem(OPEN_GROUPS_KEY) ?? "{}");
  } catch {
    return {};
  }
}

export function Knowledge() {
  const [mode, setMode] = useState<Mode>("Pages");
  const [query, setQuery] = useState("");
  const [selectedPath, setSelectedPath] = useState(() => new URLSearchParams(window.location.search).get("path") ?? "");
  const [graphRoot, setGraphRoot] = useState("");
  const [toast, setToast] = useState("");
  const [openGroups, setOpenGroups] = useState<Record<string, boolean>>(loadOpenGroups);
  const [expandedCards, setExpandedCards] = useState<Record<string, boolean>>({});
  const [activeSlug, setActiveSlug] = useState("");
  const { data: pages = [], isFetching, refetch } = useKnowledgePagesQuery({ tier: 1 });
  const [compile, compileState] = useCompileKnowledgeMutation();

  const filtered = useMemo(() => {
    const needle = query.toLowerCase();
    if (!needle) return pages;
    return pages.filter((page) => {
      return (
        page.title.toLowerCase().includes(needle) ||
        page.path.toLowerCase().includes(needle) ||
        page.entities.some((entity) => entity.toLowerCase().includes(needle))
      );
    });
  }, [pages, query]);

  const selectedInFiltered = selectedPath ? filtered.find((page) => page.path === selectedPath) : undefined;
  const selected = selectedPath ? selectedInFiltered : undefined;
  const selectedFilteredOut = Boolean(selectedPath && pages.some((page) => page.path === selectedPath) && !selectedInFiltered);
  const grouped = groupPages(filtered);
  const groupEntries = Object.entries(grouped);
  const searching = query.trim().length > 0;
  const headings = selected ? extractHeadings(selected.body) : [];

  function persistOpenGroups(next: Record<string, boolean>) {
    setOpenGroups(next);
    try {
      localStorage.setItem(OPEN_GROUPS_KEY, JSON.stringify(next));
    } catch {
      /* storage unavailable — keep in-memory state */
    }
  }

  function toggleGroup(source: string) {
    persistOpenGroups({ ...openGroups, [source]: !(openGroups[source] ?? false) });
  }

  function setAllGroups(open: boolean) {
    persistOpenGroups(Object.fromEntries(groupEntries.map(([source]) => [source, open])));
  }

  function openPage(path: string) {
    setSelectedPath(path);
    setActiveSlug("");
  }

  function scrollToHeading(slug: string) {
    setActiveSlug(slug);
    document.getElementById(slug)?.scrollIntoView({ behavior: "smooth", block: "start" });
  }

  async function runCompile() {
    try {
      const result = await compile().unwrap();
      setToast(`${result.nodes_created + result.nodes_updated} nodes, ${result.edges_created} edges`);
      await refetch();
    } catch (err) {
      setToast(`Compile failed — ${errorMessage(err)}`);
    }
  }

  function openPreview() {
    if (!selected) return;
    const url = `/?tab=Knowledge&path=${encodeURIComponent(selected.path)}&preview=1`;
    window.open(url, "_blank", "noopener,noreferrer");
  }

  function handleNodeClick(node: KnowledgeNode) {
    const page = pages.find((item) => item.id === node.primary_wiki_page_id);
    if (page) openPage(page.path);
  }

  return (
    <section className="gateway-view">
      <section className="gateway-panel">
        <header>
          <div>
            <h2>Brain</h2>
            <p>Wiki summaries and the compiled knowledge graph from your connected sources.</p>
          </div>
          <div className="knowledge-actions">
            {toast ? <span className="knowledge-toast">{toast}</span> : null}
            <button
              className="primary"
              disabled={compileState.isLoading}
              onClick={runCompile}
              type="button"
            >
              <RefreshCw size={14} className={compileState.isLoading ? "spin" : ""} />
              Compile knowledge
            </button>
          </div>
        </header>

        <div className="category-tabs" role="tablist">
          <button
            aria-pressed={mode === "Pages"}
            className={mode === "Pages" ? "active" : ""}
            onClick={() => setMode("Pages")}
            type="button"
          >
            <Library size={14} /> Pages
          </button>
          <button
            aria-pressed={mode === "Graph"}
            className={mode === "Graph" ? "active" : ""}
            onClick={() => setMode("Graph")}
            type="button"
          >
            <GitBranch size={14} /> Graph
          </button>
        </div>

        {mode === "Pages" ? (
          <div className="knowledge-pages">
            <aside className="wiki-tree">
              <label className="search-box">
                <Search size={14} />
                <input
                  placeholder="Search pages or entities"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                />
              </label>
              <div className="tree-tools">
                <button onClick={() => setAllGroups(true)} type="button">Expand all</button>
                <button onClick={() => setAllGroups(false)} type="button">Collapse all</button>
              </div>
              {isFetching ? <span className="tree-status">Refreshing pages</span> : null}
              {selectedFilteredOut ? (
                <span className="tree-status">Selected page no longer matches filter.</span>
              ) : null}
              {groupEntries.length === 0 ? (
                <div className="wiki-empty compact">
                  <strong>No pages</strong>
                  <span>Sync a connector to generate wiki pages.</span>
                </div>
              ) : (
                groupEntries.map(([source, sourcePages]) => {
                  const open = searching || (openGroups[source] ?? false);
                  return (
                    <div className={`tree-group${open ? " open" : ""}`} key={source}>
                      <button className="tree-group-head" onClick={() => toggleGroup(source)} type="button">
                        <ChevronRight className="tree-chevron" size={14} />
                        <span className="tree-group-name">{source}</span>
                        <span className="tree-count">{sourcePages.length}</span>
                      </button>
                      {open ? (
                        <div className="tree-group-body">
                          {sourcePages.map((page) => (
                            <button
                              className={selected?.path === page.path ? "active" : ""}
                              key={page.path}
                              onClick={() => openPage(page.path)}
                              type="button"
                            >
                              {page.title}
                            </button>
                          ))}
                        </div>
                      ) : null}
                    </div>
                  );
                })
              )}
            </aside>
            <section className="wiki-reader">
              {selected ? (
                <>
                  <div className="wiki-reader-actions">
                    <button className="ghost wiki-back" onClick={() => setSelectedPath("")} type="button">
                      <ChevronRight size={14} className="flip" /> Overview
                    </button>
                    <button className="ghost" onClick={openPreview} type="button">
                      <ExternalLink size={14} /> Open preview in new window
                    </button>
                  </div>
                  <div className={headings.length > 0 ? "wiki-reader-grid" : ""}>
                    <WikiPageView page={selected} onLinkClick={openPage} />
                    {headings.length > 0 ? (
                      <nav className="wiki-toc" aria-label="On this page">
                        <span className="wiki-toc-label">On this page</span>
                        {headings.map((heading) => (
                          <button
                            className={`wiki-toc-link level-${heading.level}${activeSlug === heading.slug ? " active" : ""}`}
                            key={heading.slug}
                            onClick={() => scrollToHeading(heading.slug)}
                            type="button"
                          >
                            {heading.text}
                          </button>
                        ))}
                      </nav>
                    ) : null}
                  </div>
                </>
              ) : (
                <div className="wiki-overview">
                  <div className="wiki-overview-head">
                    <span className="wiki-overview-eyebrow">Knowledge base — pick a page to read</span>
                    <h2>Overview</h2>
                  </div>
                  {groupEntries.length === 0 ? (
                    <div className="wiki-empty">
                      <strong>No pages</strong>
                      <span>Sync a connector to generate wiki pages.</span>
                    </div>
                  ) : (
                    <div className="wiki-overview-cards">
                      {groupEntries.map(([source, sourcePages]) => {
                        const cardOpen = expandedCards[source] ?? false;
                        const visible = cardOpen ? sourcePages : sourcePages.slice(0, OVERVIEW_PREVIEW);
                        return (
                          <div className="wiki-overview-card" key={source}>
                            <div className="wiki-overview-card-head">
                              <b>{source}/</b>
                              <span className="tree-count">{sourcePages.length}</span>
                            </div>
                            <ul className={cardOpen ? "scroll" : ""}>
                              {visible.map((page) => (
                                <li key={page.path}>
                                  <button onClick={() => openPage(page.path)} type="button">
                                    <span className="wiki-dot" style={{ background: SOURCE_DOT[source] ?? "var(--muted)" }} />
                                    <span className="wiki-overview-title">{page.title}</span>
                                  </button>
                                </li>
                              ))}
                            </ul>
                            {sourcePages.length > OVERVIEW_PREVIEW ? (
                              <button
                                className="wiki-overview-more"
                                onClick={() => setExpandedCards((prev) => ({ ...prev, [source]: !cardOpen }))}
                                type="button"
                              >
                                {cardOpen ? "Show less" : `+${sourcePages.length - OVERVIEW_PREVIEW} more…`}
                              </button>
                            ) : null}
                          </div>
                        );
                      })}
                    </div>
                  )}
                </div>
              )}
            </section>
          </div>
        ) : (
          <div className="knowledge-graph-pane">
            <KnowledgeGraph root={graphRoot || undefined} depth={2} onNodeClick={handleNodeClick} />
          </div>
        )}
      </section>
    </section>
  );
}
