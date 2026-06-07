import { ChevronDown, Database, Send, Sparkles, StopCircle } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { useDispatch } from "react-redux";

import { errorMessage } from "../lib/errors";
import { ChatChart } from "./ChatChart";
import { CitationDrawer } from "./CitationDrawer";
import { RetrievalTrace } from "./RetrievalTrace";
import { WriteToolPreview } from "./WriteToolPreview";
import {
  API,
  dataclawApi,
  useChatThreadQuery,
  useConnectorsQuery,
  useLlmCatalogQuery,
  useLlmProvidersQuery,
  useQueryMutation,
} from "../services/api";
import type { AppDispatch } from "../store";
import type { ChatCitation, ChatMessage, ChatResponse, QueryRow, TabName } from "../types";

const SUGGESTIONS = [
  "Show me daily revenue for the last week and chart it",
  "Create an Airflow job that refreshes dim_customers every 2 hours",
  "What is our on-call rotation policy for freshness breaches?",
  "What does MRR mean in our company?",
  "Which pipelines failed this week?",
];

type IdeProps = {
  activeThreadId: string | null;
  setActiveThreadId: (value: string | null) => void;
  hasKnowledgeBase: boolean;
  onError: (message: string) => void;
  setTab?: (tab: import("../types").TabName) => void;
};

type LiveRows = Record<string, QueryRow[]>;

export function IDE({ activeThreadId, setActiveThreadId, hasKnowledgeBase, onError, setTab }: IdeProps) {
  const [draft, setDraft] = useState("");
  const [pending, setPending] = useState<string | null>(null);
  const [liveRows, setLiveRows] = useState<LiveRows>({});
  const [model, setModel] = useState<string>("");
  const [modelOpen, setModelOpen] = useState(false);
  const [connectorSlug, setConnectorSlug] = useState<string>("");
  const [connectorOpen, setConnectorOpen] = useState(false);
  const [activeCitation, setActiveCitation] = useState<ChatCitation | null>(null);
  const [writePreview, setWritePreview] = useState<ChatResponse | null>(null);
  const [streaming, setStreaming] = useState(false);
  const [streamedAnswer, setStreamedAnswer] = useState("");
  const scrollRef = useRef<HTMLDivElement>(null);
  const modelRef = useRef<HTMLDivElement>(null);
  const connectorRef = useRef<HTMLDivElement>(null);
  const abortRef = useRef<AbortController | null>(null);
  const activeRunIdRef = useRef<string | null>(null);
  const dispatch = useDispatch<AppDispatch>();

  const threadQuery = useChatThreadQuery(activeThreadId ?? "", { skip: !activeThreadId });
  const connectorsQuery = useConnectorsQuery();
  const { data: providers } = useLlmProvidersQuery();
  const { data: catalog } = useLlmCatalogQuery();
  const [runQuery] = useQueryMutation();

  const { models: MODELS, defaultModel: DEFAULT_MODEL, providerLabel, providerReady } = useMemo(() => {
    const activeProvider = providers?.find((p) => p.configured);
    if (!activeProvider || !catalog) {
      return { models: [] as string[], defaultModel: "", providerLabel: "", providerReady: false };
    }
    const catalogItem = catalog.find((c) => c.slug === activeProvider.slug);
    if (!catalogItem) {
      return { models: [] as string[], defaultModel: "", providerLabel: "", providerReady: false };
    }
    const modelField = catalogItem.fields.find((f) => f.name === "model");
    const options = modelField?.options ?? [];
    const list = options.length > 0 ? options : [catalogItem.default_model];
    const saved = activeProvider.values.model;
    return {
      models: list,
      defaultModel: saved && list.includes(saved) ? saved : (catalogItem.default_model || list[0]),
      providerLabel: catalogItem.display_name,
      providerReady: true,
    };
  }, [providers, catalog]);

  useEffect(() => {
    if (!providerReady) {
      if (model !== "") setModel("");
      return;
    }
    if (!model || !MODELS.includes(model)) {
      setModel(DEFAULT_MODEL);
    }
  }, [DEFAULT_MODEL, MODELS, model, providerReady]);

  const availableConnectors = useMemo(
    () =>
      (connectorsQuery.data ?? []).filter(
        (connector) =>
          ["data_store", "knowledge_base", "etl_orchestration"].includes(connector.category) &&
          connector.credential_state === "configured",
      ),
    [connectorsQuery.data],
  );
  const selectedConnector = availableConnectors.find((connector) => connector.slug === connectorSlug);

  useEffect(() => {
    if (connectorSlug && !selectedConnector) {
      setConnectorSlug("");
    }
  }, [connectorSlug, selectedConnector]);

  const messages: ChatMessage[] = threadQuery.data?.messages ?? [];

  useEffect(() => {
    if (scrollRef.current) {
      scrollRef.current.scrollTop = scrollRef.current.scrollHeight;
    }
  }, [messages.length, pending]);

  useEffect(() => {
    function onClick(event: MouseEvent) {
      if (modelRef.current && !modelRef.current.contains(event.target as Node)) {
        setModelOpen(false);
      }
      if (connectorRef.current && !connectorRef.current.contains(event.target as Node)) {
        setConnectorOpen(false);
      }
    }
    document.addEventListener("mousedown", onClick);
    return () => document.removeEventListener("mousedown", onClick);
  }, []);

  async function handleAsk(question: string) {
    if (!question.trim()) return;
    onError("");
    setPending(question);
    setStreamedAnswer("");
    setStreaming(true);
    setDraft("");
    const abortController = new AbortController();
    abortRef.current = abortController;
    activeRunIdRef.current = null;
    try {
      const response = await streamChat({
        question,
        thread_id: activeThreadId ?? undefined,
        model,
        connector_slug: connectorSlug || undefined,
        onDelta: (chunk) => setStreamedAnswer((value) => value + chunk),
        onRun: (runId) => {
          activeRunIdRef.current = runId;
        },
        signal: abortController.signal,
      });
      setActiveThreadId(response.thread_id);
      if (response.status === "pending_approval") {
        setWritePreview(response);
      }
      dispatch(dataclawApi.util.invalidateTags(["ChatThreads", { type: "ChatThreads", id: response.thread_id }]));
      if (response.sql) {
        try {
          const queryResult = await runQuery({ sql: response.sql, limit: 100, connector_slug: connectorSlug || undefined }).unwrap();
          setLiveRows((prev) => ({ ...prev, [response.thread_id]: queryResult.rows }));
        } catch (err) {
          onError(errorMessage(err));
        }
      }
    } catch (err) {
      if (!(err instanceof DOMException && err.name === "AbortError")) {
        onError(errorMessage(err));
      }
    } finally {
      setPending(null);
      setStreaming(false);
      setStreamedAnswer("");
      abortRef.current = null;
      activeRunIdRef.current = null;
    }
  }

  async function handleStop() {
    const runId = activeRunIdRef.current;
    abortRef.current?.abort();
    if (runId) {
      await fetch(`${API}/chat/runs/${runId}/cancel`, {
        method: "POST",
        credentials: "include",
        headers: { "content-type": "application/json" },
      }).catch(() => undefined);
    }
  }

  const showWelcome = !activeThreadId && !pending && messages.length === 0;
  const liveResultRows = activeThreadId ? liveRows[activeThreadId] ?? [] : [];
  const liveResultColumns = useMemo(
    () => (liveResultRows.length > 0 ? Object.keys(liveResultRows[0]) : []),
    [liveResultRows],
  );

  return (
    <section className="editor">
      <header className="editor-header">
        <div className="editor-header-title">
          <Sparkles size={14} />
          <span>{threadQuery.data?.title ?? "New chat"}</span>
        </div>
      </header>

      <div className="editor-stream" ref={scrollRef}>
        {showWelcome ? (
          <div className="editor-welcome">
            <div className="editor-welcome-mark">
              <Sparkles size={26} />
            </div>
            <h1>Ask DataClaw anything about your stack</h1>
            <p>
              {hasKnowledgeBase
                ? "Grounded in your schema, lineage, and docs — not hallucinated. Try one of these, or type your own."
                : "Configure a connector in the Connectors tab to start grounding answers in your schema, lineage, and docs."}
            </p>
            <div className="editor-suggestions">
              {SUGGESTIONS.map((suggestion) => (
                <button key={suggestion} onClick={() => setDraft(suggestion)} type="button">
                  {suggestion}
                </button>
              ))}
            </div>
          </div>
        ) : null}

        {messages.map((message) => (
          <ChatBubble
            key={message.id}
            message={message}
            liveRows={liveResultRows}
            liveColumns={liveResultColumns}
            onCitationClick={setActiveCitation}
            setTab={setTab}
          />
        ))}

        {pending ? (
          <>
            <div className="bubble user">
              <p>{pending}</p>
            </div>
            <div className="bubble assistant pending">
              {streamedAnswer ? <p>{streamedAnswer}</p> : <span>Thinking…</span>}
            </div>
          </>
        ) : null}
      </div>

      <form
        className="editor-composer"
        onSubmit={(event) => {
          event.preventDefault();
          handleAsk(draft);
        }}
      >
        <textarea
          aria-label="Ask about your data"
          disabled={streaming}
          placeholder="Ask about your data..."
          value={draft}
          onChange={(event) => setDraft(event.target.value)}
          onKeyDown={(event) => {
            if (event.key === "Enter" && !event.shiftKey) {
              event.preventDefault();
              handleAsk(draft);
            }
          }}
          rows={1}
        />
        <div className="editor-composer-foot">
          <div className="editor-model" ref={modelRef}>
            <button
              aria-expanded={modelOpen}
              className={providerReady ? "editor-model-pill" : "editor-model-pill warn"}
              onClick={() => setModelOpen((value) => !value)}
              type="button"
            >
              <span className="editor-model-dot" aria-hidden="true" />
              <span>{providerReady ? model : "No LLM provider"}</span>
              <ChevronDown size={12} />
            </button>
            {modelOpen ? (
              <ul className="editor-model-menu" role="listbox">
                {providerReady ? (
                  <>
                    <li className="editor-model-menu-header" aria-hidden="true">
                      {providerLabel}
                    </li>
                    {MODELS.map((candidate) => (
                      <li key={candidate}>
                        <button
                          aria-selected={candidate === model}
                          className={candidate === model ? "active" : ""}
                          onClick={() => {
                            setModel(candidate);
                            setModelOpen(false);
                          }}
                          role="option"
                          type="button"
                        >
                          {candidate}
                        </button>
                      </li>
                    ))}
                  </>
                ) : (
                  <>
                    <li className="editor-model-menu-header" aria-hidden="true">
                      Not configured
                    </li>
                    <li>
                      <button
                        className="editor-model-cta"
                        onClick={() => {
                          setModelOpen(false);
                          setTab?.("Settings");
                        }}
                        type="button"
                      >
                        Configure your LLM provider →
                      </button>
                    </li>
                  </>
                )}
              </ul>
            ) : null}
          </div>
          <div className="editor-model" ref={connectorRef}>
            <button
              aria-expanded={connectorOpen}
              className="editor-model-pill"
              onClick={() => setConnectorOpen((value) => !value)}
              type="button"
            >
              <Database size={12} />
              <span>{selectedConnector?.display_name ?? "Auto source"}</span>
              <ChevronDown size={12} />
            </button>
            {connectorOpen ? (
              <ul className="editor-model-menu" role="listbox">
                <li>
                  <button
                    aria-selected={!connectorSlug}
                    className={!connectorSlug ? "active" : ""}
                    onClick={() => {
                      setConnectorSlug("");
                      setConnectorOpen(false);
                    }}
                    role="option"
                    type="button"
                  >
                    Auto source
                  </button>
                </li>
                {availableConnectors.length > 0 ? (
                  availableConnectors.map((connector) => (
                    <li key={connector.slug}>
                      <button
                        aria-selected={connector.slug === connectorSlug}
                        className={connector.slug === connectorSlug ? "active" : ""}
                        onClick={() => {
                          setConnectorSlug(connector.slug);
                          setConnectorOpen(false);
                        }}
                        role="option"
                        type="button"
                      >
                        {connector.display_name}
                      </button>
                    </li>
                  ))
                ) : (
                  <li className="editor-model-menu-header" aria-hidden="true">
                    No connectors configured
                  </li>
                )}
              </ul>
            ) : null}
          </div>
          <button
            aria-label={providerReady ? "Send" : "Configure an LLM provider first"}
            title={providerReady ? undefined : "Configure an LLM provider in Settings first"}
            className="editor-send"
            disabled={streaming || !draft.trim() || !providerReady}
            type="submit"
          >
            <Send size={14} />
          </button>
          {streaming ? (
            <button aria-label="Stop generating" className="editor-stop" onClick={handleStop} type="button">
              <StopCircle size={14} />
            </button>
          ) : null}
        </div>
      </form>
      <CitationDrawer citation={activeCitation} onClose={() => setActiveCitation(null)} />
      <WriteToolPreview
        response={writePreview}
        onClose={() => setWritePreview(null)}
        onOpenObservability={() => {
          setWritePreview(null);
          setTab?.("Gateway");
        }}
      />
    </section>
  );
}

async function streamChat({
  question,
  thread_id,
  model,
  connector_slug,
  onDelta,
  onRun,
  signal,
}: {
  question: string;
  thread_id?: string;
  model?: string;
  connector_slug?: string | null;
  onDelta: (chunk: string) => void;
  onRun?: (runId: string) => void;
  signal?: AbortSignal;
}): Promise<ChatResponse> {
  const response = await fetch(`${API}/ide/chat`, {
    method: "POST",
    credentials: "include",
    headers: {
      "content-type": "application/json",
      accept: "text/event-stream",
    },
    body: JSON.stringify({ question, thread_id, model, connector_slug }),
    signal,
  });
  if (!response.ok || !response.body) {
    throw new Error(`Chat request failed with status ${response.status}`);
  }

  const reader = response.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";
  let finalResponse: ChatResponse | null = null;

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buffer += decoder.decode(value, { stream: true });
    const frames = buffer.split("\n\n");
    buffer = frames.pop() ?? "";
    for (const frame of frames) {
      const event = parseSseFrame(frame);
      if (!event) continue;
      if (event.event === "delta") {
        onDelta(String(event.data.content ?? ""));
      } else if (event.event === "thread" && event.data.run_id) {
        onRun?.(String(event.data.run_id));
      } else if (event.event === "done") {
        finalResponse = event.data as ChatResponse;
      } else if (event.event === "cancelled") {
        throw new DOMException("Chat generation cancelled", "AbortError");
      } else if (event.event === "error") {
        throw new Error(String(event.data.detail ?? "Chat stream failed"));
      }
    }
  }

  if (!finalResponse) {
    throw new Error("Chat stream ended before completion");
  }
  return finalResponse;
}

function parseSseFrame(frame: string): { event: string; data: Record<string, unknown> } | null {
  const eventLine = frame.split("\n").find((line) => line.startsWith("event: "));
  const dataLine = frame.split("\n").find((line) => line.startsWith("data: "));
  if (!eventLine || !dataLine) return null;
  try {
    return {
      event: eventLine.slice("event: ".length),
      data: JSON.parse(dataLine.slice("data: ".length)) as Record<string, unknown>,
    };
  } catch {
    return {
      event: "error",
      data: { detail: "Chat stream returned malformed data." },
    };
  }
}

function ChatBubble({
  message,
  liveRows,
  liveColumns,
  onCitationClick,
  setTab,
}: {
  message: ChatMessage;
  liveRows: QueryRow[];
  liveColumns: string[];
  onCitationClick: (citation: ChatCitation) => void;
  setTab?: (tab: TabName) => void;
}) {
  if (message.role === "user") {
    return (
      <div className="bubble user">
        <p>{message.content}</p>
      </div>
    );
  }
  const visibleCitations = message.citations.filter((citation) => citation.type !== "tool_call_provenance");
  return (
    <div className="bubble assistant">
      {message.provider ? (
        <span className="bubble-meta">
          {message.provider} · {message.llm_status}
        </span>
      ) : null}
      <p>{message.content}</p>
      {message.sql ? (
        <div className="editor-sql">
          <span>SQL</span>
          <code>{message.sql}</code>
        </div>
      ) : null}
      {message.chart_spec ? <ChatChart spec={message.chart_spec} /> : null}
      {message.action && setTab ? (
        <button className="chat-action-button" onClick={() => setTab(message.action!.tab)} type="button">
          {message.action.label}
        </button>
      ) : null}
      <RetrievalTrace trace={message.retrieval_trace} />
      {liveColumns.length > 0 ? (
        <div className="editor-results">
          <table>
            <thead>
              <tr>
                {liveColumns.map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {liveRows.map((row, index) => {
                const rowKey = liveColumns
                  .slice(0, 3)
                  .map((column) => String(row[column] ?? ""))
                  .join("␟") || `row-${index}`;
                return (
                  <tr key={rowKey}>
                    {liveColumns.map((column) => (
                      <td key={column}>{String(row[column])}</td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : null}
      {visibleCitations.length > 0 ? (
        <div className="bubble-citations">
          {visibleCitations.map((citation, index) => (
            citation.path ? (
              <button key={`${citation.path}-${index}`} onClick={() => onCitationClick(citation)} type="button">
                {citation.title} <em>({citation.connector})</em>
              </button>
            ) : (
              <span key={`${citation.title}-${index}`}>{citation.title} <em>({citation.connector})</em></span>
            )
          ))}
        </div>
      ) : null}
    </div>
  );
}
