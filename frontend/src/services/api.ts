import { createApi, fetchBaseQuery } from "@reduxjs/toolkit/query/react";

import type {
  ChatRequest,
  ChatResponse,
  ChatThread,
  ChatThreadSummary,
  ChatTraceLink,
  ChatTraceView,
  Connector,
  BatchSummary,
  BulkActionResponse,
  DashboardResponse,
  EvalCase,
  EvalCaseCreateRequest,
  EvalCaseFromFeedbackRequest,
  EvalCaseListQuery,
  EvalCasePatchRequest,
  DiagnoseRequest,
  DiagnoseStatusResponse,
  EvalConfig,
  EvalConfigUpdateRequest,
  EvalRun,
  EvalRunDetail,
  EvalRunListQuery,
  EvalSuggestion,
  EvalSuggestionCatalog,
  GenerateCandidatesRequest,
  GenerateCandidatesResponse,
  ProducerCatalog,
  RunBatchRequest,
  ConnectorCatalogItem,
  ConnectorRecord,
  ConnectorTestResponse,
  Dashboard,
  Agent,
  AgentSummary,
  FeedbackCreateRequest,
  FeedbackRecord,
  MessageFeedback,
  GrantMatrix,
  CompileResult,
  LlmCatalogItem,
  LlmProviderRecord,
  LlmProviderTestResponse,
  LlmProviderUpdate,
  LoginRequest,
  LoginResponse,
  McpCatalogItem,
  KnowledgeGraphResponse,
  WikiPage,
  MetadataRunResponse,
  MonitoringAgentDescriptor,
  ObservabilityEvent,
  ObservabilityFeed,
  ObservabilityListResponse,
  ObservabilityRecord,
  ObservabilityTestResponse,
  ObservabilityUpdateRequest,
  QueryRequest,
  QueryResponse,
  SyncResponse,
  WorkerStatus,
  Workspace,
} from "../types";

type ObservabilityQuery = {
  kind?: string;
  severity?: string;
  state?: string;
  q?: string;
  limit?: number;
};

type ConnectorTestRequest = {
  slug: string;
  credentials: Record<string, string>;
  persist_on_success?: boolean;
};

export const API = import.meta.env.VITE_API_URL ?? "/api/v1";

export const dataclawApi = createApi({
  reducerPath: "dataclawApi",
  baseQuery: fetchBaseQuery({
    baseUrl: API,
    credentials: "include",
    prepareHeaders: (headers) => {
      headers.set("content-type", "application/json");
      return headers;
    },
  }),
  tagTypes: ["Auth", "Connectors", "Workspace", "Dashboard", "ChatThreads", "Observability", "LlmProviders", "Agents", "AgentGrants", "McpCatalog", "Knowledge", "Monitoring", "Worker", "Feedback", "ObservabilityProviders", "ChatTraces", "EvalCases", "EvalRuns", "EvalDashboard", "EvalSuggestions"],
  endpoints: (builder) => ({
    login: builder.mutation<LoginResponse, LoginRequest>({
      query: (body) => ({ url: "/auth/login", method: "POST", body }),
      invalidatesTags: ["Auth", "Connectors", "Workspace", "Dashboard"],
    }),
    logout: builder.mutation<{ ok: boolean }, void>({
      query: () => ({ url: "/auth/logout", method: "POST" }),
      invalidatesTags: ["Auth", "Connectors", "Workspace", "Dashboard"],
    }),
    connectors: builder.query<Connector[], void>({
      query: () => "/connectors",
      providesTags: ["Connectors"],
    }),
    connectorCatalog: builder.query<ConnectorCatalogItem[], void>({
      query: () => "/connectors/catalog",
      providesTags: ["Connectors"],
    }),
    connectorRecord: builder.query<ConnectorRecord, string>({
      query: (slug) => `/connectors/${slug}`,
      providesTags: (_result, _err, slug) => [{ type: "Connectors", id: slug }],
    }),
    syncConnector: builder.mutation<SyncResponse, string>({
      query: (slug) => ({ url: `/connectors/${slug}/sync`, method: "POST" }),
      invalidatesTags: ["Connectors", "Workspace", "Dashboard"],
    }),
    testConnector: builder.mutation<ConnectorTestResponse, ConnectorTestRequest>({
      query: ({ slug, credentials, persist_on_success = true }) => ({
        url: `/connectors/${slug}/test`,
        method: "POST",
        body: { credentials, persist_on_success },
      }),
      invalidatesTags: ["Connectors"],
    }),
    workspace: builder.query<Workspace, void>({
      query: () => "/workspace",
      providesTags: ["Workspace"],
    }),
    updateWorkspace: builder.mutation<Workspace, Partial<Pick<Workspace, "onboarding_complete">>>({
      query: (body) => ({ url: "/workspace", method: "PATCH", body }),
      invalidatesTags: ["Workspace"],
    }),
    workerStatus: builder.query<WorkerStatus, void>({
      query: () => "/worker/status",
      providesTags: ["Worker"],
    }),
    dashboard: builder.query<Dashboard, void>({
      query: () => "/agents/dashboard",
      providesTags: ["Dashboard"],
    }),
    runMetadataAgent: builder.mutation<MetadataRunResponse, { force: boolean }>({
      query: (body) => ({ url: "/agents/metadata/run", method: "POST", body }),
      invalidatesTags: ["Workspace", "Dashboard"],
    }),
    runAgent: builder.mutation<MetadataRunResponse, string>({
      query: (name) => ({ url: `/agents/${name}/run`, method: "POST" }),
      invalidatesTags: ["Workspace", "Dashboard", "Observability"],
    }),
    observabilityEvents: builder.query<ObservabilityFeed, ObservabilityQuery | void>({
      query: (params) => {
        const search = new URLSearchParams();
        if (params) {
          for (const [key, value] of Object.entries(params)) {
            if (value !== undefined && value !== null && value !== "") {
              search.set(key, String(value));
            }
          }
        }
        const qs = search.toString();
        return qs ? `/observability/events?${qs}` : "/observability/events";
      },
      providesTags: ["Observability"],
    }),
    acknowledgeAlert: builder.mutation<ObservabilityEvent, string>({
      query: (id) => ({ url: `/alerts/${id}/acknowledge`, method: "POST" }),
      invalidatesTags: ["Observability"],
    }),
    approveAlert: builder.mutation<{ status: string; alert: ObservabilityEvent }, string>({
      query: (id) => ({ url: `/alerts/${id}/approve-and-execute`, method: "POST" }),
      invalidatesTags: ["Observability", "Workspace", "Dashboard"],
    }),
    resolveAlert: builder.mutation<ObservabilityEvent, string>({
      query: (id) => ({ url: `/alerts/${id}/resolve`, method: "POST" }),
      invalidatesTags: ["Observability"],
    }),
    chat: builder.mutation<ChatResponse, ChatRequest>({
      query: (body) => ({ url: "/ide/chat", method: "POST", body }),
      invalidatesTags: ["ChatThreads"],
    }),
    query: builder.mutation<QueryResponse, QueryRequest>({
      query: (body) => ({ url: "/ide/query", method: "POST", body }),
    }),
    chatThreads: builder.query<ChatThreadSummary[], void>({
      query: () => "/chat/threads",
      providesTags: ["ChatThreads"],
    }),
    chatThread: builder.query<ChatThread, string>({
      query: (id) => `/chat/threads/${id}`,
      providesTags: (_result, _err, id) => [{ type: "ChatThreads", id }],
    }),
    createChatThread: builder.mutation<ChatThread, { title?: string }>({
      query: (body) => ({ url: "/chat/threads", method: "POST", body }),
      invalidatesTags: ["ChatThreads"],
    }),
    deleteChatThread: builder.mutation<{ ok: boolean }, string>({
      query: (id) => ({ url: `/chat/threads/${id}`, method: "DELETE" }),
      invalidatesTags: ["ChatThreads"],
    }),
    renameChatThread: builder.mutation<ChatThread, { id: string; title: string }>({
      query: ({ id, title }) => ({ url: `/chat/threads/${id}`, method: "PATCH", body: { title } }),
      invalidatesTags: ["ChatThreads"],
    }),
    llmCatalog: builder.query<LlmCatalogItem[], void>({
      query: () => "/llm/catalog",
      providesTags: ["LlmProviders"],
    }),
    llmProviders: builder.query<LlmProviderRecord[], void>({
      query: () => "/llm/providers",
      providesTags: ["LlmProviders"],
    }),
    updateLlmProvider: builder.mutation<LlmProviderRecord, { slug: string } & LlmProviderUpdate>({
      query: ({ slug, values }) => ({
        url: `/llm/providers/${slug}`,
        method: "PUT",
        body: { values },
      }),
      invalidatesTags: ["LlmProviders"],
    }),
    testLlmProvider: builder.mutation<LlmProviderTestResponse, { slug: string } & LlmProviderUpdate>({
      query: ({ slug, values }) => ({
        url: `/llm/providers/${slug}/test`,
        method: "POST",
        body: { values },
      }),
    }),
    agents: builder.query<AgentSummary[], { kind?: string } | void>({
      query: (params) => {
        const kind = params?.kind;
        return kind ? `/agents?kind=${encodeURIComponent(kind)}` : "/agents";
      },
      providesTags: ["Agents"],
    }),
    agent: builder.query<Agent, string>({
      query: (id) => `/agents/${id}`,
      providesTags: (_result, _err, id) => [{ type: "Agents", id }, "AgentGrants"],
    }),
    createAgent: builder.mutation<AgentSummary, Partial<AgentSummary> & { name: string; grants?: GrantMatrix["grants"] }>({
      query: (body) => ({ url: "/agents", method: "POST", body }),
      invalidatesTags: ["Agents"],
    }),
    updateAgent: builder.mutation<AgentSummary, { id: string; patch: Partial<AgentSummary> }>({
      query: ({ id, patch }) => ({ url: `/agents/${id}`, method: "PATCH", body: patch }),
      invalidatesTags: (_result, _err, { id }) => [{ type: "Agents", id }, "Agents"],
    }),
    deleteAgent: builder.mutation<{ ok: boolean }, string>({
      query: (id) => ({ url: `/agents/${id}`, method: "DELETE" }),
      invalidatesTags: ["Agents"],
    }),
    agentGrants: builder.query<Agent["grants"], string>({
      query: (id) => `/agents/${id}/grants`,
      providesTags: ["AgentGrants"],
    }),
    updateAgentGrants: builder.mutation<Agent["grants"], { id: string } & GrantMatrix>({
      query: ({ id, grants }) => ({ url: `/agents/${id}/grants`, method: "PUT", body: { grants } }),
      invalidatesTags: ["AgentGrants"],
    }),
    mcpCatalog: builder.query<McpCatalogItem[], void>({
      query: () => "/mcp/catalog",
      providesTags: ["McpCatalog"],
    }),
    knowledgePages: builder.query<WikiPage[], { source_type?: string; tier?: number } | void>({
      query: (params) => {
        const search = new URLSearchParams();
        if (params?.source_type) search.set("source_type", params.source_type);
        if (params?.tier) search.set("tier", String(params.tier));
        const qs = search.toString();
        return qs ? `/knowledge/pages?${qs}` : "/knowledge/pages";
      },
      providesTags: ["Knowledge"],
    }),
    knowledgePage: builder.query<WikiPage, string>({
      query: (path) => `/knowledge/pages/${path}`,
      providesTags: (_result, _err, path) => [{ type: "Knowledge", id: path }],
    }),
    compileKnowledge: builder.mutation<CompileResult, void>({
      query: () => ({ url: "/knowledge/compile", method: "POST" }),
      invalidatesTags: ["Knowledge"],
    }),
    knowledgeGraph: builder.query<KnowledgeGraphResponse, { root?: string; depth?: number } | void>({
      query: (params) => {
        const search = new URLSearchParams();
        if (params?.root) search.set("root", params.root);
        if (params?.depth) search.set("depth", String(params.depth));
        const qs = search.toString();
        return qs ? `/knowledge/graph?${qs}` : "/knowledge/graph";
      },
      providesTags: ["Knowledge"],
    }),
    monitoringAgents: builder.query<MonitoringAgentDescriptor[], void>({
      query: () => "/monitoring/agents",
      providesTags: ["Monitoring"],
    }),
    submitFeedback: builder.mutation<FeedbackRecord, FeedbackCreateRequest>({
      query: (body) => ({ url: "/feedback", method: "POST", body }),
      invalidatesTags: (_result, _err, arg) => [
        { type: "Feedback", id: arg.chat_message_id },
      ],
    }),
    messageFeedback: builder.query<MessageFeedback, string>({
      query: (messageId) => `/chat-messages/${messageId}/feedback`,
      providesTags: (_result, _err, id) => [{ type: "Feedback", id }],
    }),
    chatTrace: builder.query<ChatTraceView, string>({
      query: (messageId) => `/chat-messages/${messageId}/trace`,
      providesTags: (_result, _err, id) => [{ type: "ChatTraces", id }],
    }),
    chatTraceLink: builder.query<ChatTraceLink, string>({
      query: (messageId) => `/chat-messages/${messageId}/trace-link`,
      providesTags: (_result, _err, id) => [{ type: "ChatTraces", id }],
    }),
    observabilityProviders: builder.query<ObservabilityListResponse, void>({
      query: () => "/integrations/observability",
      providesTags: ["ObservabilityProviders"],
    }),
    upsertObservabilityProvider: builder.mutation<
      ObservabilityRecord,
      { slug: string; body: ObservabilityUpdateRequest }
    >({
      query: ({ slug, body }) => ({
        url: `/integrations/observability/${slug}`,
        method: "PUT",
        body,
      }),
      invalidatesTags: ["ObservabilityProviders"],
    }),
    disableObservabilityProvider: builder.mutation<ObservabilityRecord, string>({
      query: (slug) => ({
        url: `/integrations/observability/${slug}`,
        method: "DELETE",
      }),
      invalidatesTags: ["ObservabilityProviders"],
    }),
    testObservabilityProvider: builder.mutation<ObservabilityTestResponse, string>({
      query: (slug) => ({
        url: `/integrations/observability/${slug}/test`,
        method: "POST",
      }),
    }),
    evalCases: builder.query<EvalCase[], EvalCaseListQuery | void>({
      query: (params) => {
        const search = new URLSearchParams();
        if (params) {
          for (const [key, value] of Object.entries(params)) {
            if (value !== undefined && value !== null && value !== "") {
              search.set(key, String(value));
            }
          }
        }
        const qs = search.toString();
        return qs ? `/evals/cases?${qs}` : "/evals/cases";
      },
      providesTags: (result) => [
        ...(result ? result.map((c) => ({ type: "EvalCases" as const, id: c.id })) : []),
        { type: "EvalCases", id: "LIST" },
      ],
    }),
    evalCase: builder.query<EvalCase, string>({
      query: (id) => `/evals/cases/${id}`,
      providesTags: (_r, _e, id) => [{ type: "EvalCases", id }],
    }),
    createEvalCase: builder.mutation<EvalCase, EvalCaseCreateRequest>({
      query: (body) => ({ url: "/evals/cases", method: "POST", body }),
      invalidatesTags: [{ type: "EvalCases", id: "LIST" }, "Feedback"],
    }),
    createEvalCaseFromFeedback: builder.mutation<EvalCase, EvalCaseFromFeedbackRequest>({
      query: (body) => ({ url: "/evals/cases/from-feedback", method: "POST", body }),
      invalidatesTags: [{ type: "EvalCases", id: "LIST" }, "Feedback"],
    }),
    patchEvalCase: builder.mutation<EvalCase, { id: string; body: EvalCasePatchRequest }>({
      query: ({ id, body }) => ({ url: `/evals/cases/${id}`, method: "PATCH", body }),
      invalidatesTags: (_r, _e, arg) => [
        { type: "EvalCases", id: arg.id },
        { type: "EvalCases", id: "LIST" },
      ],
    }),
    approveEvalCase: builder.mutation<EvalCase, string>({
      query: (id) => ({ url: `/evals/cases/${id}/approve`, method: "POST" }),
      invalidatesTags: (_r, _e, id) => [
        { type: "EvalCases", id },
        { type: "EvalCases", id: "LIST" },
      ],
    }),
    promoteGoldenEvalCase: builder.mutation<EvalCase, string>({
      query: (id) => ({ url: `/evals/cases/${id}/promote-golden`, method: "POST" }),
      invalidatesTags: (_r, _e, id) => [
        { type: "EvalCases", id },
        { type: "EvalCases", id: "LIST" },
      ],
    }),
    archiveEvalCase: builder.mutation<EvalCase, string>({
      query: (id) => ({ url: `/evals/cases/${id}/archive`, method: "POST" }),
      invalidatesTags: (_r, _e, id) => [
        { type: "EvalCases", id },
        { type: "EvalCases", id: "LIST" },
      ],
    }),
    unarchiveEvalCase: builder.mutation<EvalCase, string>({
      query: (id) => ({ url: `/evals/cases/${id}/unarchive`, method: "POST" }),
      invalidatesTags: (_r, _e, id) => [
        { type: "EvalCases", id },
        { type: "EvalCases", id: "LIST" },
      ],
    }),
    evalProducers: builder.query<ProducerCatalog, void>({
      query: () => "/evals/producers",
    }),
    generateEvalCandidates: builder.mutation<
      GenerateCandidatesResponse,
      GenerateCandidatesRequest
    >({
      query: (body) => ({
        url: "/evals/cases/generate-candidates",
        method: "POST",
        body,
      }),
      invalidatesTags: [{ type: "EvalCases", id: "LIST" }],
    }),
    bulkApproveEvalCases: builder.mutation<BulkActionResponse, string[]>({
      query: (case_ids) => ({
        url: "/evals/cases/bulk-approve",
        method: "POST",
        body: { case_ids },
      }),
      invalidatesTags: [{ type: "EvalCases", id: "LIST" }],
    }),
    bulkArchiveEvalCases: builder.mutation<BulkActionResponse, string[]>({
      query: (case_ids) => ({
        url: "/evals/cases/bulk-archive",
        method: "POST",
        body: { case_ids },
      }),
      invalidatesTags: [{ type: "EvalCases", id: "LIST" }],
    }),
    runEvalBatch: builder.mutation<BatchSummary, RunBatchRequest>({
      query: (body) => ({ url: "/evals/runs", method: "POST", body }),
      invalidatesTags: [
        { type: "EvalRuns", id: "LIST" },
        { type: "EvalDashboard", id: "DASH" },
      ],
    }),
    evalRuns: builder.query<EvalRun[], EvalRunListQuery | void>({
      query: (params) => {
        const search = new URLSearchParams();
        if (params) {
          for (const [key, value] of Object.entries(params)) {
            if (value !== undefined && value !== null && value !== "") {
              search.set(key, String(value));
            }
          }
        }
        const qs = search.toString();
        return qs ? `/evals/runs?${qs}` : "/evals/runs";
      },
      providesTags: (result) => [
        ...(result ? result.map((r) => ({ type: "EvalRuns" as const, id: r.id })) : []),
        { type: "EvalRuns", id: "LIST" },
      ],
    }),
    evalRun: builder.query<EvalRunDetail, string>({
      query: (id) => `/evals/runs/${id}`,
      providesTags: (_r, _e, id) => [{ type: "EvalRuns", id }],
    }),
    evalDashboard: builder.query<DashboardResponse, number | void>({
      query: (rangeDays) =>
        rangeDays
          ? `/evals/metrics/dashboard?range_days=${rangeDays}`
          : "/evals/metrics/dashboard",
      providesTags: [{ type: "EvalDashboard", id: "DASH" }],
    }),
    suggestionKinds: builder.query<EvalSuggestionCatalog, void>({
      query: () => "/evals/suggestions/kinds",
    }),
    runSuggestions: builder.query<EvalSuggestion[], string>({
      query: (runId) => `/evals/runs/${runId}/suggestions`,
      providesTags: (_r, _e, id) => [{ type: "EvalSuggestions", id }],
    }),
    diagnoseRun: builder.mutation<
      EvalSuggestion[],
      { runId: string; body?: DiagnoseRequest }
    >({
      query: ({ runId, body }) => ({
        url: `/evals/runs/${runId}/diagnose`,
        method: "POST",
        body: body ?? {},
      }),
      invalidatesTags: (_r, _e, arg) => [{ type: "EvalSuggestions", id: arg.runId }],
    }),
    applySuggestion: builder.mutation<EvalSuggestion, string>({
      query: (id) => ({
        url: `/evals/suggestions/${id}/apply`,
        method: "POST",
      }),
      invalidatesTags: (result) =>
        result
          ? [
              { type: "EvalSuggestions", id: result.eval_run_id },
              { type: "EvalCases", id: "LIST" },
            ]
          : [{ type: "EvalCases", id: "LIST" }],
    }),
    dismissSuggestion: builder.mutation<EvalSuggestion, string>({
      query: (id) => ({
        url: `/evals/suggestions/${id}/dismiss`,
        method: "POST",
      }),
      invalidatesTags: (result) =>
        result ? [{ type: "EvalSuggestions", id: result.eval_run_id }] : [],
    }),
    revertSuggestion: builder.mutation<EvalSuggestion, string>({
      query: (id) => ({
        url: `/evals/suggestions/${id}/revert`,
        method: "POST",
      }),
      invalidatesTags: (result) =>
        result
          ? [
              { type: "EvalSuggestions", id: result.eval_run_id },
              { type: "EvalCases", id: "LIST" },
            ]
          : [{ type: "EvalCases", id: "LIST" }],
    }),
    evalConfig: builder.query<EvalConfig, void>({
      query: () => "/evals/config",
      providesTags: ["EvalDashboard"],
    }),
    updateEvalConfig: builder.mutation<EvalConfig, EvalConfigUpdateRequest>({
      query: (body) => ({ url: "/evals/config", method: "PUT", body }),
      invalidatesTags: ["EvalDashboard"],
    }),
    diagnoseStatus: builder.query<DiagnoseStatusResponse, string>({
      query: (runId) => `/evals/runs/${runId}/diagnose/status`,
    }),
  }),
});

export const {
  useLoginMutation,
  useLogoutMutation,
  useLazyConnectorsQuery,
  useConnectorsQuery,
  useConnectorCatalogQuery,
  useLazyWorkspaceQuery,
  useWorkspaceQuery,
  useUpdateWorkspaceMutation,
  useWorkerStatusQuery,
  useLazyDashboardQuery,
  useDashboardQuery,
  useSyncConnectorMutation,
  useTestConnectorMutation,
  useConnectorRecordQuery,
  useRunMetadataAgentMutation,
  useRunAgentMutation,
  useChatMutation,
  useQueryMutation,
  useChatThreadsQuery,
  useLazyChatThreadsQuery,
  useChatThreadQuery,
  useLazyChatThreadQuery,
  useCreateChatThreadMutation,
  useDeleteChatThreadMutation,
  useRenameChatThreadMutation,
  useObservabilityEventsQuery,
  useAcknowledgeAlertMutation,
  useApproveAlertMutation,
  useResolveAlertMutation,
  useLlmCatalogQuery,
  useLlmProvidersQuery,
  useTestLlmProviderMutation,
  useUpdateLlmProviderMutation,
  useAgentsQuery,
  useAgentQuery,
  useCreateAgentMutation,
  useUpdateAgentMutation,
  useDeleteAgentMutation,
  useAgentGrantsQuery,
  useUpdateAgentGrantsMutation,
  useMcpCatalogQuery,
  useKnowledgePagesQuery,
  useKnowledgePageQuery,
  useCompileKnowledgeMutation,
  useKnowledgeGraphQuery,
  useLazyKnowledgeGraphQuery,
  useMonitoringAgentsQuery,
  useSubmitFeedbackMutation,
  useMessageFeedbackQuery,
  useChatTraceQuery,
  useChatTraceLinkQuery,
  useObservabilityProvidersQuery,
  useUpsertObservabilityProviderMutation,
  useDisableObservabilityProviderMutation,
  useTestObservabilityProviderMutation,
  useEvalCasesQuery,
  useEvalCaseQuery,
  useCreateEvalCaseMutation,
  useCreateEvalCaseFromFeedbackMutation,
  usePatchEvalCaseMutation,
  useApproveEvalCaseMutation,
  usePromoteGoldenEvalCaseMutation,
  useArchiveEvalCaseMutation,
  useUnarchiveEvalCaseMutation,
  useEvalProducersQuery,
  useGenerateEvalCandidatesMutation,
  useBulkApproveEvalCasesMutation,
  useBulkArchiveEvalCasesMutation,
  useRunEvalBatchMutation,
  useEvalRunsQuery,
  useEvalRunQuery,
  useEvalDashboardQuery,
  useSuggestionKindsQuery,
  useRunSuggestionsQuery,
  useDiagnoseRunMutation,
  useApplySuggestionMutation,
  useDismissSuggestionMutation,
  useRevertSuggestionMutation,
  useEvalConfigQuery,
  useUpdateEvalConfigMutation,
  useDiagnoseStatusQuery,
} = dataclawApi;
