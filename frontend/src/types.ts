export type TabName = "Editor" | "Connectors" | "Knowledge" | "Settings" | "Gateway" | "Agents" | "Monitoring" | "Evals";

export type LlmField = {
  name: string;
  label: string;
  secret: boolean;
  required: boolean;
  placeholder: string;
  options?: string[];
};

export type LlmCatalogItem = {
  slug: string;
  display_name: string;
  logo_key: string;
  docs_url: string;
  description: string;
  default_model: string;
  default_embedding_model?: string | null;
  wired: boolean;
  fields: LlmField[];
};

export type LlmProviderRecord = {
  slug: string;
  configured: boolean;
  values: Record<string, string>;
  secrets_set: string[];
  secret_previews: Record<string, string>;
};

export type LlmProviderUpdate = {
  values: Record<string, string | null>;
};

export type LlmProviderTestResponse = {
  status: "ok" | "error" | string;
  message: string;
};

export type AgentToolCallEvent = {
  id: string;
  run_id?: string | null;
  agent_name: string;
  tool_name: string;
  connector_slug?: string | null;
  args_json: Record<string, unknown>;
  result_summary: string;
  result_size_bytes: number;
  latency_ms: number;
  status: "ok" | "error" | string;
  error_message?: string | null;
  called_at: string;
};

export type ObservabilityEvent = {
  id: string;
  kind: "alert" | "agent_run";
  timestamp: string;
  severity: "info" | "warning" | "critical" | string;
  title: string;
  detail: string;
  state: "open" | "needs_approval" | "acknowledged" | "resolved" | "completed" | "failed" | string;
  requires_approval: boolean;
  acknowledged_at?: string | null;
  acknowledged_by?: string | null;
  resolved_at?: string | null;
  resolved_by?: string | null;
  connector_slug?: string | null;
  logo_key?: string | null;
  agent_name?: string | null;
  agent_icon_key?: string | null;
  duration_ms?: number | null;
  error_message?: string | null;
  timeline?: unknown[];
  tool_calls?: AgentToolCallEvent[];
  actions: string[];
};

export type ObservabilityFeed = {
  total: number;
  needs_approval: number;
  events: ObservabilityEvent[];
};

export type CredentialField = {
  name: string;
  label: string;
  secret: boolean;
  required: boolean;
  placeholder: string;
};

export type Connector = {
  id?: string;
  slug: string;
  display_name: string;
  category: string;
  status: string;
  credential_state?: string;
  sync_state?: "never_synced" | "syncing" | "synced" | "sync_failed";
  last_synced_at?: string | null;
  last_sync_error?: string | null;
  logo_key?: string;
  sync_summary?: Record<string, unknown>;
};

export type ConnectorRecord = {
  slug: string;
  configured: boolean;
  values: Record<string, string>;
  secrets_set: string[];
  secret_previews: Record<string, string>;
};

export type ConnectorStability =
  | "stable"
  | "stable_read_only"
  | "beta"
  | "known_issue"
  | "unsupported";

export type ConnectorCatalogItem = {
  slug: string;
  display_name: string;
  category: string;
  credential_schema: CredentialField[];
  logo_key: string;
  docs_url: string;
  sync_behavior: string;
  production_notes: string;
  recommended: boolean;
  stability: ConnectorStability;
  known_issues: string[];
  stability_notes: string;
};

export type ConnectorTestResponse = {
  slug: string;
  status: "ok" | "failed" | "credential_required" | "mock_tested" | "not_configured";
  mode: string;
  message: string;
  details?: Record<string, unknown>;
};

export type ColumnAsset = {
  name: string;
  type: string;
  description: string;
};

export type TableAsset = {
  id: string;
  name: string;
  description: string;
  business_summary: string;
  row_count: number;
  freshness_status: string;
  tags: string[];
  columns: ColumnAsset[];
};

export type Dataset = {
  id: string;
  name: string;
  source_type: string;
  schema_name: string;
  tables: TableAsset[];
};

export type KnowledgeDocument = {
  id: string;
  title: string;
  connector: string;
  related_tables: string[];
};

export type LineageEdge = {
  source_table: string;
  target_table: string;
  relationship: string;
  evidence: string;
};

export type WikiPage = {
  id: string;
  workspace_id: string;
  path: string;
  disk_path: string;
  tier: number;
  source_type: string;
  source_id: string;
  title: string;
  body: string;
  frontmatter: Record<string, unknown>;
  entities: string[];
  content_hash: string;
  created_at: string;
  updated_at: string;
};

export type KnowledgeNode = {
  id: string;
  type: "table" | "column" | "dag" | "dbt_model" | "doc" | "metric" | "owner" | "dataset" | string;
  canonical_name: string;
  aliases: string[];
  primary_wiki_page_id?: string | null;
};

export type KnowledgeEdge = {
  id: string;
  src_node_id: string;
  dst_node_id: string;
  relationship: string;
  evidence: string;
  confidence: number;
  source: "frontmatter" | "wiki_link" | "fk_match" | "llm_inference" | string;
};

export type KnowledgeGraphResponse = {
  nodes: KnowledgeNode[];
  edges: KnowledgeEdge[];
};

export type CompileResult = {
  nodes_created: number;
  nodes_updated: number;
  edges_created: number;
  runtime_ms: number;
};

export type Workspace = {
  id: string;
  name: string;
  onboarding_complete: boolean;
  tabs: TabName[];
  datasets: Dataset[];
  knowledge_documents: KnowledgeDocument[];
  lineage: LineageEdge[];
};

export type WorkerStatus = {
  status: "ok" | "stale" | "missing" | string;
  worker_status?: string;
  last_seen_at?: string | null;
  age_seconds?: number;
  detail?: string;
};

export type AgentCard = {
  name: string;
  status: string;
  detail: string;
};

export type FeedItem = {
  type: "run" | "alert";
  title: string;
  detail: string;
  status: string;
};

export type AlertItem = {
  id: string;
  severity: string;
  title: string;
  detail: string;
  resolved: boolean;
};

export type AgentRun = {
  id: string;
  agent_name: string;
  status: string;
  summary: string;
  timeline: unknown[];
};

export type DashboardConnector = {
  slug: string;
  name: string;
  status: string;
  category: string;
};

export type Dashboard = {
  last_hour_feed: FeedItem[];
  agent_cards: AgentCard[];
  connectors?: DashboardConnector[];
  alerts?: AlertItem[];
  runs?: AgentRun[];
};

export type LoginRequest = {
  email: string;
  password: string;
};

export type LoginResponse = {
  email: string;
  is_admin: boolean;
  bootstrap_admin_created?: boolean;
};

export type ChatRequest = {
  question: string;
  thread_id?: string;
  model?: string;
  connector_slug?: string | null;
};

export type ChatCitation = {
  title: string;
  connector: string;
  type?: string;
  tool?: string;
  path?: string;
};

export type ChatAction = {
  label: string;
  tab: TabName;
  connector_slug?: string | null;
  agent_name?: string | null;
};

export type ChatResponse = {
  answer: string;
  sql?: string;
  provider: string;
  llm_status: string;
  status?: string | null;
  alert_id?: string | null;
  action?: ChatAction | null;
  tool_call?: Record<string, unknown> | null;
  tool_result?: Record<string, unknown> | null;
  tool_calls?: Record<string, unknown>[] | null;
  tool_results?: Record<string, unknown>[] | null;
  thread_id: string;
  thread_title: string;
  citations?: ChatCitation[];
  metadata_sources?: unknown[];
  chart_spec?: Record<string, unknown> | null;
  retrieval_trace?: Record<string, unknown>;
  trace_id?: string | null;
  langfuse_url?: string | null;
  message_id?: string | null;
};

export type ChatMessage = {
  id: string;
  role: "user" | "assistant" | "system";
  content: string;
  sql?: string | null;
  provider?: string | null;
  llm_status?: string | null;
  citations: ChatCitation[];
  rows: QueryRow[];
  chart_spec?: Record<string, unknown> | null;
  action?: ChatAction | null;
  retrieval_trace?: Record<string, unknown>;
  trace_id?: string | null;
  created_at: string;
};

export type FeedbackSentiment = "positive" | "negative";

export type FeedbackRecord = {
  id: string;
  chat_message_id: string;
  sentiment: FeedbackSentiment;
  comment: string | null;
  user_id: string | null;
  langfuse_score_id: string | null;
  eval_case_id: string | null;
};

export type FeedbackCreateRequest = {
  chat_message_id: string;
  sentiment: FeedbackSentiment;
  comment?: string | null;
};

export type ObservabilityField = {
  name: string;
  label: string;
  secret: boolean;
  required: boolean;
  placeholder: string;
};

export type ObservabilityCatalogItem = {
  slug: string;
  display_name: string;
  docs_url: string;
  description: string;
  fields: ObservabilityField[];
};

export type ObservabilityRecord = {
  slug: string;
  configured: boolean;
  enabled: boolean;
  values: Record<string, string>;
  secrets_set: string[];
  secret_previews: Record<string, string>;
};

export type ObservabilityListResponse = {
  catalog: ObservabilityCatalogItem[];
  records: Record<string, ObservabilityRecord>;
};

export type ObservabilityUpdateRequest = {
  values: Record<string, string | boolean | null>;
};

export type ObservabilityTestResponse = {
  status: "ok" | "error" | "not_configured" | "sdk_missing" | string;
  message: string;
};

export type ChatSpanRecord = {
  id: string;
  parent_span_id: string | null;
  kind: "root" | "retrieval" | "llm" | "tool" | "sql" | string;
  name: string;
  status: "ok" | "error" | string;
  error: string | null;
  input: Record<string, unknown>;
  output: Record<string, unknown>;
  metadata: Record<string, unknown>;
  usage: Record<string, unknown>;
  model: string | null;
  latency_ms: number;
  started_at: string;
  ended_at: string;
};

export type ChatTraceView = {
  chat_message_id: string;
  trace_id: string | null;
  spans: ChatSpanRecord[];
};

export type ChatTraceLink = {
  chat_message_id: string;
  trace_id: string | null;
  langfuse_url: string | null;
};

export type EvalCaseStatus = "candidate" | "approved" | "golden" | "archived";

export type EvalCaseOrigin =
  | "manual"
  | "feedback"
  | "auto:schema"
  | "auto:kg"
  | "auto:lineage"
  | "auto:dbt"
  | "auto:airflow"
  | "auto:dagster"
  | "auto:fixture";

export type EvalCaseCitation = {
  source?: string | null;
  table?: string | null;
  columns?: string[] | null;
  type?: string | null;
  connector?: string | null;
  tool?: string | null;
  title?: string | null;
  path?: string | null;
};

export type EvalCase = {
  id: string;
  workspace_id: string;
  question: string;
  expected_answer: string | null;
  expected_sql: string | null;
  expected_connector_slug: string | null;
  expected_tool: string | null;
  expected_citations: EvalCaseCitation[];
  expected_result_hash: string | null;
  expected_result_preview: Record<string, unknown>[];
  tags: string[];
  status: EvalCaseStatus;
  origin: EvalCaseOrigin;
  source_chat_message_id: string | null;
  created_by: string | null;
  created_at: string;
  updated_at: string;
};

export type EvalCaseCreateRequest = {
  question: string;
  expected_answer?: string | null;
  expected_sql?: string | null;
  expected_connector_slug?: string | null;
  expected_tool?: string | null;
  expected_citations?: EvalCaseCitation[] | null;
  tags?: string[] | null;
  origin?: EvalCaseOrigin;
  status?: EvalCaseStatus;
};

export type EvalCaseFromFeedbackRequest = {
  chat_message_id: string;
  question?: string | null;
  expected_answer?: string | null;
  expected_sql?: string | null;
  expected_connector_slug?: string | null;
  expected_tool?: string | null;
  expected_citations?: EvalCaseCitation[] | null;
  tags?: string[] | null;
};

export type EvalCasePatchRequest = {
  question?: string | null;
  expected_answer?: string | null;
  expected_sql?: string | null;
  expected_connector_slug?: string | null;
  expected_tool?: string | null;
  expected_citations?: EvalCaseCitation[] | null;
  tags?: string[] | null;
};

export type EvalCaseListQuery = {
  status?: EvalCaseStatus;
  origin?: EvalCaseOrigin;
  tag?: string;
  q?: string;
  limit?: number;
  offset?: number;
};

export type ProducerCatalogItem = {
  slug: string;
  display_name: string;
  origin: string;
};

export type ProducerCatalog = {
  producers: ProducerCatalogItem[];
  default_limit_per_source: number;
  workspace_candidate_ceiling: number;
};

export type GenerateCandidatesRequest = {
  sources?: string[] | null;
  limit_per_source?: number;
};

export type ProducerResult = {
  slug: string;
  display_name: string;
  produced: number;
  inserted: number;
  skipped_duplicate: number;
  skipped_cap: number;
  error: string | null;
};

export type GenerateCandidatesResponse = {
  workspace_id: string;
  inserted_ids: string[];
  producers: ProducerResult[];
  candidate_ceiling: number;
  candidates_in_queue_before: number;
  candidates_in_queue_after: number;
  ceiling_reached: boolean;
};

export type BulkActionResult = {
  id: string;
  ok: boolean;
  status: string | null;
  error: string | null;
};

export type BulkActionResponse = {
  results: BulkActionResult[];
};

export type EvalRun = {
  id: string;
  workspace_id: string;
  batch_id: string;
  eval_case_id: string;
  chat_message_id: string | null;
  passed: boolean;
  failure_category: string | null;
  actual_answer: string | null;
  actual_sql: string | null;
  actual_citations: Record<string, unknown>[];
  actual_result_preview: Record<string, unknown>[];
  actual_result_hash: string | null;
  langfuse_trace_id: string | null;
  duration_ms: number;
  prompt_tokens: number | null;
  completion_tokens: number | null;
  cost_usd: number | null;
  model: string | null;
  error: string | null;
  created_at: string;
};

export type EvalResult = {
  metric: string;
  status: "ok" | "skipped" | "error" | string;
  score: number | null;
  passed: boolean | null;
  detail: Record<string, unknown>;
};

export type EvalRunDetail = EvalRun & {
  case: EvalCase;
  results: EvalResult[];
};

export type EvalRunListQuery = {
  batch_id?: string;
  eval_case_id?: string;
  passed?: boolean;
  since_hours?: number;
  limit?: number;
  offset?: number;
};

export type RunBatchRequest = {
  case_ids?: string[] | null;
  status_filter?: string[] | null;
  repeat?: number;
};

export type BatchSummary = {
  batch_id: string;
  workspace_id: string;
  total: number;
  passed: number;
  failed: number;
  errored: number;
  run_ids: string[];
  aborted: boolean;
  abort_reason: string | null;
  cost_usd: number;
};

export type EvalConfig = {
  schedule_enabled: boolean;
  schedule_interval_minutes: number;
  schedule_status_filter: string[];
  batch_max_cost_usd: number;
  judges_enabled: boolean;
  batch_max_concurrency: number;
};

export type EvalConfigUpdateRequest = Partial<EvalConfig>;

export type DashboardMetricSummary = {
  metric: string;
  mean: number | null;
  pass_rate: number | null;
  sample_size: number;
};

export type DashboardBucket = {
  date: string;
  runs: number;
  passed: number;
  failed: number;
  avg_duration_ms: number | null;
  avg_cost_usd: number | null;
};

export type EvalSuggestionKind =
  | "golden_query"
  | "prompt_diff"
  | "rules_md"
  | "tool_description_diff"
  | "retrieval_context"
  | "connector_routing";

export type EvalSuggestionStatus = "pending" | "applied" | "dismissed" | "reverted";

export type EvalSuggestion = {
  id: string;
  eval_run_id: string;
  workspace_id: string;
  kind: EvalSuggestionKind;
  title: string;
  rationale: string;
  current_value: string | null;
  proposed_value: string;
  target: string | null;
  confidence: number;
  source: "rules" | "llm" | string;
  status: EvalSuggestionStatus;
  apply_payload: Record<string, unknown>;
  apply_result: Record<string, unknown>;
  applied_at: string | null;
  applied_by: string | null;
  created_at: string;
  updated_at: string;
  apply_supported: boolean;
};

export type EvalSuggestionCatalog = {
  kinds: EvalSuggestionKind[];
  apply_supported: EvalSuggestionKind[];
};

export type DiagnoseRequest = {
  use_llm?: boolean;
  background?: boolean;
};

export type DiagnoseStatusResponse = {
  run_id: string;
  status: "idle" | "running" | "completed" | string; // "error: ..." too
};

export type DashboardResponse = {
  workspace_id: string;
  range_days: number;
  total_runs: number;
  pass_rate: number | null;
  p50_latency_ms: number | null;
  p95_latency_ms: number | null;
  total_cost_usd: number;
  total_tokens: number;
  regression_count: number;
  failure_category_counts: Record<string, number>;
  metrics: DashboardMetricSummary[];
  daily: DashboardBucket[];
};

export type ChatThreadSummary = {
  id: string;
  title: string;
  created_at: string;
  updated_at: string;
};

export type ChatThread = ChatThreadSummary & {
  message_count: number;
  messages: ChatMessage[];
};

export type QueryRequest = {
  sql: string;
  limit: number;
  connector_slug?: string | null;
};

export type QueryRow = Record<string, string | number | boolean | null>;

export type QueryResponse = {
  sql: string;
  rows: QueryRow[];
  read_only: boolean;
  status: string;
};

export type SyncResponse = {
  mode?: string;
  objects_synced?: number;
  [key: string]: unknown;
};

export type MetadataRunResponse = {
  id: string;
  status: string;
  summary: string;
  timeline?: unknown[];
};

export type AgentSummary = {
  id: string;
  workspace_id: string;
  name: string;
  display_name: string;
  system_prompt: string;
  sql_query?: string | null;
  kind: "background" | "on_demand" | string;
  is_system: boolean;
  enabled: boolean;
  icon_key: string;
  cadence_minutes?: number | null;
  thresholds?: Record<string, number | string | boolean>;
  uses_llm_filter?: boolean;
  target_connector_id?: string | null;
  target_connector_slug?: string | null;
  created_at: string;
  updated_at: string;
};

export type Agent = AgentSummary & {
  grants: GrantScope[];
};

export type GrantScope = {
  id: string;
  agent_id: string;
  connector_slug: string;
  read_enabled: boolean;
  write_enabled: boolean;
};

export type GrantMatrix = {
  grants: Array<Pick<GrantScope, "connector_slug" | "read_enabled" | "write_enabled">>;
};

export type McpTool = {
  name: string;
  scope: "read" | "write";
};

export type McpCatalogItem = {
  slug: string;
  display_name: string;
  logo_key: string;
  read_tools: McpTool[];
  write_tools: McpTool[];
};

export type MonitoringAgentDescriptor = {
  name: string;
  display_name: string;
  supported_connectors: string[];
  default_thresholds: Record<string, number>;
};
