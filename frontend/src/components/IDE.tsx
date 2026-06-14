import { CheckCircle2, ChevronDown, Database, FileText, Send, Sparkles, Star, StopCircle, Wrench, XCircle } from "lucide-react";
import { useEffect, useMemo, useRef, useState } from "react";
import { useDispatch } from "react-redux";

import { errorMessage } from "../lib/errors";
import { ChatChart } from "./ChatChart";
import { CitationDrawer } from "./CitationDrawer";
import { FeedbackBar } from "./FeedbackBar";
import { JsonDetails } from "./JsonDetails";
import { RetrievalTrace } from "./RetrievalTrace";
import { SqlBlock } from "./SqlBlock";
import {
  API,
  dataclawApi,
  useChatThreadQuery,
  useConnectorsQuery,
  useLlmCatalogQuery,
  useLlmProvidersQuery,
  useApproveAlertMutation,
  useQueryMutation,
  useResolveAlertMutation,
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
type ApprovedMessage = {
  content: string;
  status: string;
  llm_status: string;
  tool_result?: Record<string, unknown> | null;
};

export function IDE({ activeThreadId, setActiveThreadId, hasKnowledgeBase, onError, setTab }: IdeProps) {
  const [draft, setDraft] = useState("");
  const [pending, setPending] = useState<string | null>(null);
  const [liveRows, setLiveRows] = useState<LiveRows>({});
  const [approvedMessages, setApprovedMessages] = useState<Record<string, ApprovedMessage>>({});
  const [model, setModel] = useState<string>("");
  const [modelOpen, setModelOpen] = useState(false);
  const [connectorSlug, setConnectorSlug] = useState<string>("");
  const [connectorOpen, setConnectorOpen] = useState(false);
  const [activeCitation, setActiveCitation] = useState<ChatCitation | null>(null);
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
  const [approveAlert, approveState] = useApproveAlertMutation();
  const [resolveAlert, resolveState] = useResolveAlertMutation();

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
  // Latest assistant turn drives the end-of-chat feedback bar. Scoping
  // feedback to one specific message (the most recent answer) lets the
  // existing 👎-correction → eval_case pipeline keep working — we just
  // moved the UI surface from per-bubble to thread-end.
  const lastAssistantMessage = useMemo(() => {
    for (let i = messages.length - 1; i >= 0; i -= 1) {
      if (messages[i].role === "assistant") return messages[i];
    }
    return null;
  }, [messages]);

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
      dispatch(dataclawApi.util.invalidateTags(["ChatThreads", { type: "ChatThreads", id: response.thread_id }]));
      if (response.sql && response.message_id) {
        try {
          const sqlConnector =
            (response.tool_call?.connector_slug as string | undefined) ??
            (response.tool_calls?.find((tc) => (tc as { tool?: string })?.tool === "read_query_select")
              ?.connector_slug as string | undefined) ??
            connectorSlug;
          const queryResult = await runQuery({ sql: response.sql, limit: 100, connector_slug: sqlConnector || undefined }).unwrap();
          setLiveRows((prev) => ({ ...prev, [response.message_id!]: queryResult.rows }));
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
  const latestAssistantMessageId = useMemo(
    () => [...messages].reverse().find((message) => message.role === "assistant")?.id,
    [messages],
  );

  async function handleApproveMessage(message: ChatMessage) {
    if (!message.alert_id) return;
    try {
      const result = await approveAlert(message.alert_id).unwrap();
      const toolResult = result.result;
      const dag = toolResult?.dag as Record<string, unknown> | undefined;
      const dagId = typeof dag?.dag_id === "string" ? dag.dag_id : undefined;
      setApprovedMessages((prev) => ({
        ...prev,
        [message.id]: {
          content: dagId ? `Approved and created Airflow DAG ${dagId}.` : "Approved and executed the requested write.",
          status: "executed",
          llm_status: "mcp_tool_completed",
          tool_result: toolResult ?? null,
        },
      }));
      dispatch(dataclawApi.util.invalidateTags(["Observability", "ChatThreads", "Workspace", "Dashboard"]));
    } catch (err) {
      onError(errorMessage(err));
    }
  }

  async function handleRejectMessage(message: ChatMessage) {
    if (!message.alert_id) return;
    try {
      await resolveAlert(message.alert_id).unwrap();
      setApprovedMessages((prev) => ({
        ...prev,
        [message.id]: {
          content: "Rejected this write request. No changes were executed.",
          status: "rejected",
          llm_status: "approval_rejected",
          tool_result: message.tool_result ?? null,
        },
      }));
      dispatch(dataclawApi.util.invalidateTags(["Observability", "ChatThreads"]));
    } catch (err) {
      onError(errorMessage(err));
    }
  }

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
            liveRows={message.id === latestAssistantMessageId ? liveRows[message.id] ?? [] : []}
            onCitationClick={setActiveCitation}
            setTab={setTab}
            approvedMessage={approvedMessages[message.id]}
            approving={approveState.isLoading}
            rejecting={resolveState.isLoading}
            onApprove={() => handleApproveMessage(message)}
            onReject={() => handleRejectMessage(message)}
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

        {/* Thread-level feedback: shown once at the end of the chat,
            scoped to the most recent assistant turn. Per Sairam's
            preference — keeps the conversation clean and gives one
            clear "rate the last answer" surface. */}
        {!pending && lastAssistantMessage ? (
          <div className="chat-end-feedback">
            <span className="chat-end-feedback-label">How was the last answer?</span>
            <FeedbackBar
              key={lastAssistantMessage.id}
              message={lastAssistantMessage}
            />
          </div>
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
  onCitationClick,
  setTab,
  approvedMessage,
  approving,
  rejecting,
  onApprove,
  onReject,
}: {
  message: ChatMessage;
  liveRows: QueryRow[];
  onCitationClick: (citation: ChatCitation) => void;
  setTab?: (tab: TabName) => void;
  approvedMessage?: ApprovedMessage;
  approving?: boolean;
  rejecting?: boolean;
  onApprove?: () => void;
  onReject?: () => void;
}) {
  if (message.role === "user") {
    return (
      <div className="bubble user">
        <p>{message.content}</p>
      </div>
    );
  }
  const effectiveContent = approvedMessage?.content ?? message.content;
  const effectiveStatus = approvedMessage?.status ?? message.status;
  const effectiveLlmStatus = approvedMessage?.llm_status ?? message.llm_status;
  const effectiveToolResult = approvedMessage?.tool_result ?? message.tool_result;
  const messageRows = liveRows.length > 0 ? liveRows : message.rows ?? [];
  const messageColumns = messageRows.length > 0 ? Object.keys(messageRows[0]) : [];
  const isPendingApproval = effectiveStatus === "pending_approval" || effectiveLlmStatus === "pending_approval";
  const toolCall = message.tool_call ?? {};
  const connector = String(toolCall.connector_slug ?? "connector");
  const tool = String(toolCall.tool ?? "write tool");
  const provenanceCitations = message.citations.filter((c) => c.type === "tool_call_provenance");
  const sourceCitations = message.citations.filter((c) => c.type !== "tool_call_provenance");
  return (
    <div className="bubble assistant">
      {message.provider ? (
        <span className="bubble-meta">
          {message.provider} · {effectiveLlmStatus}
        </span>
      ) : null}
      <p>{effectiveContent}</p>
      {isPendingApproval && message.alert_id ? (
        <div className="chat-approval">
          <div>
            <strong>Approve this write?</strong>
            <span>{connector}.{tool}</span>
          </div>
          <div className="chat-approval-actions">
            <button className="primary" disabled={approving || rejecting} onClick={onApprove} type="button">
              <CheckCircle2 size={13} /> Approve
            </button>
            <button className="ghost danger" disabled={approving || rejecting} onClick={onReject} type="button">
              <XCircle size={13} /> Reject
            </button>
          </div>
          {effectiveToolResult ? <JsonDetails label="Advanced payload" value={effectiveToolResult} /> : null}
        </div>
      ) : null}
      {message.sql ? <SqlBlock sql={message.sql} /> : null}
      {message.chart_spec ? <ChatChart spec={message.chart_spec} /> : null}
      {message.action && setTab ? (
        <button className="chat-action-button" onClick={() => setTab(message.action!.tab)} type="button">
          {message.action.label}
        </button>
      ) : null}
      <RetrievalTrace trace={message.retrieval_trace} />
      {messageColumns.length > 0 ? (
        <div className="editor-results">
          <table>
            <thead>
              <tr>
                {messageColumns.map((column) => (
                  <th key={column}>{column}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {messageRows.map((row, index) => {
                const rowKey = messageColumns
                  .slice(0, 3)
                  .map((column) => String(row[column] ?? ""))
                  .join("␟") || `row-${index}`;
                return (
                  <tr key={rowKey}>
                    {messageColumns.map((column) => (
                      <td key={column}>{String(row[column])}</td>
                    ))}
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>
      ) : null}
      {provenanceCitations.length > 0 ? (
        <div className="bubble-provenance">
          {provenanceCitations.map((citation, index) => (
            <span
              key={`prov-${citation.connector}-${citation.tool}-${index}`}
              className="provenance-pill"
            >
              <Wrench size={11} aria-hidden /> via{" "}
              <strong>{citation.connector}</strong>
              {citation.tool ? <> · <code>{citation.tool}</code></> : null}
            </span>
          ))}
        </div>
      ) : null}
      {sourceCitations.length > 0 ? (
        <div className="bubble-citations">
          {sourceCitations.map((citation, index) => {
            const icon = citationIcon(citation);
            const key = `${citation.path ?? citation.title}-${index}`;
            const body = (
              <>
                {icon}
                <span className="citation-title">{citation.title}</span>
                <em>({citation.connector})</em>
              </>
            );
            return citation.path ? (
              <button
                key={key}
                onClick={() => onCitationClick(citation)}
                type="button"
                data-citation-type={citation.type ?? "source"}
              >
                {body}
              </button>
            ) : (
              <span
                key={key}
                data-citation-type={citation.type ?? "source"}
              >
                {body}
              </span>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}

function citationIcon(citation: ChatCitation) {
  if (citation.type === "golden_query_provenance") return <Star size={12} aria-hidden />;
  if (citation.connector?.toLowerCase() === "postgres" || citation.connector?.toLowerCase() === "mysql") {
    return <Database size={12} aria-hidden />;
  }
  return <FileText size={12} aria-hidden />;
}
