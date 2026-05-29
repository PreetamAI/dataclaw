from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, Field


class ConnectorCategory(StrEnum):
    KNOWLEDGE = "knowledge_base"
    DATA_STORE = "data_store"
    ORCHESTRATION = "etl_orchestration"
    LLM = "llm_provider"


class VerificationMode(StrEnum):
    REAL = "real"
    DEMO = "demo"
    CREDENTIAL_REQUIRED = "credential_required"
    NOT_CONFIGURED = "not_configured"


class Stability(StrEnum):
    """Connector readiness tier.

    stable           - read + write exercised live, audit/approval verified
    stable_read_only - reads production-ready; writes pending adapter work
    beta             - usable but expect rough edges (esp. live-write paths)
    known_issue      - disabled by default; opt-in via EXPERIMENTAL_ENABLE_<slug>
    unsupported      - adapter present for legacy reasons; service dead/unreachable
    """

    STABLE = "stable"
    STABLE_READ_ONLY = "stable_read_only"
    BETA = "beta"
    KNOWN_ISSUE = "known_issue"
    UNSUPPORTED = "unsupported"


class CredentialField(BaseModel):
    name: str
    label: str
    secret: bool = True
    required: bool = True
    placeholder: str = ""


class ConnectorDefinition(BaseModel):
    slug: str
    display_name: str
    category: ConnectorCategory
    logo_key: str
    docs_url: str
    credential_schema: list[CredentialField]
    local_verification: VerificationMode
    sync_behavior: str
    production_notes: str
    recommended: bool = False
    stability: Stability = Stability.BETA
    known_issues: list[str] = Field(default_factory=list)
    stability_notes: str = ""


def token_field(name: str, label: str = "API token") -> CredentialField:
    return CredentialField(name=name, label=label, placeholder="••••••••••")


def host_field(name: str = "host", label: str = "Host") -> CredentialField:
    return CredentialField(name=name, label=label, secret=False, placeholder="host.company.com")


def catalog() -> list[ConnectorDefinition]:
    return [
        ConnectorDefinition(
            slug="sqlite",
            display_name="SQLite",
            category=ConnectorCategory.DATA_STORE,
            logo_key="sqlite",
            docs_url="https://www.sqlite.org/docs.html",
            credential_schema=[
                CredentialField(
                    name="database_path",
                    label="Database path",
                    secret=False,
                    required=False,
                    placeholder="/data/dataclaw_demo.sqlite (leave empty for built-in demo)",
                ),
            ],
            local_verification=VerificationMode.REAL,
            sync_behavior="Reads sqlite_master, introspects tables and columns, and counts rows for each table.",
            production_notes="Bundled demo database is seeded with customers, orders, and products at startup.",
            recommended=True,
        ),
        ConnectorDefinition(
            slug="notion",
            display_name="Notion",
            category=ConnectorCategory.KNOWLEDGE,
            logo_key="notion",
            docs_url="https://developers.notion.com/",
            credential_schema=[token_field("integration_token"), CredentialField(name="database_ids", label="Database IDs", secret=False, required=False)],
            local_verification=VerificationMode.CREDENTIAL_REQUIRED,
            sync_behavior="Sync pages, databases, titles, rich text, and table mentions into Chroma-backed knowledge snippets.",
            production_notes="Requires a Notion integration token and shared pages/databases.",
            recommended=True,
        ),
        ConnectorDefinition(
            slug="google_docs",
            display_name="Google Docs / Drive",
            category=ConnectorCategory.KNOWLEDGE,
            logo_key="google-drive",
            docs_url="https://developers.google.com/drive/api",
            credential_schema=[token_field("service_account_json", "Service account JSON")],
            local_verification=VerificationMode.CREDENTIAL_REQUIRED,
            sync_behavior="Sync document titles, body text, folder metadata, and document-table references.",
            production_notes="Use service account or OAuth app credentials in production.",
            recommended=True,
        ),
        ConnectorDefinition(
            slug="quip",
            display_name="Quip",
            category=ConnectorCategory.KNOWLEDGE,
            logo_key="quip",
            docs_url="https://quip.com/dev/automation/documentation",
            credential_schema=[token_field("access_token")],
            local_verification=VerificationMode.CREDENTIAL_REQUIRED,
            sync_behavior="Sync threads, spreadsheets, document sections, and links into knowledge context.",
            production_notes="Requires Quip API access for the workspace.",
        ),
        ConnectorDefinition(
            slug="github",
            display_name="GitHub",
            category=ConnectorCategory.KNOWLEDGE,
            logo_key="github",
            docs_url="https://docs.github.com/en/rest",
            credential_schema=[token_field("token"), CredentialField(name="repositories", label="Repositories", secret=False)],
            local_verification=VerificationMode.CREDENTIAL_REQUIRED,
            sync_behavior="Sync markdown docs, dbt projects, SQL files, issues, and pull request metadata.",
            production_notes="Fine-grained tokens should be scoped to repository contents and metadata.",
            recommended=True,
        ),
        ConnectorDefinition(
            slug="confluence",
            display_name="Confluence",
            category=ConnectorCategory.KNOWLEDGE,
            logo_key="confluence",
            docs_url="https://developer.atlassian.com/cloud/confluence/rest/",
            credential_schema=[host_field("site_url", "Site URL"), token_field("api_token"), CredentialField(name="email", label="Email", secret=False)],
            local_verification=VerificationMode.CREDENTIAL_REQUIRED,
            sync_behavior="Sync spaces, pages, labels, and links to data assets.",
            production_notes="Cloud API token required.",
        ),
        ConnectorDefinition(
            slug="postgres",
            display_name="PostgreSQL",
            category=ConnectorCategory.DATA_STORE,
            logo_key="postgresql",
            docs_url="https://www.postgresql.org/docs/",
            credential_schema=[CredentialField(name="database_url", label="Database URL", secret=True, required=False, placeholder="postgresql+psycopg://user:pass@host:5432/db"), host_field(), CredentialField(name="port", label="Port", secret=False, placeholder="5432"), CredentialField(name="database", label="Database", secret=False), CredentialField(name="user", label="User", secret=False), token_field("password", "Password")],
            local_verification=VerificationMode.DEMO,
            sync_behavior="Real local test connection, schema introspection, row-count sampling, and read-only query execution.",
            production_notes="Uses SQLAlchemy's async psycopg driver with read-only query enforcement.",
            recommended=True,
        ),
        ConnectorDefinition(
            slug="snowflake",
            display_name="Snowflake",
            category=ConnectorCategory.DATA_STORE,
            logo_key="snowflake",
            docs_url="https://docs.snowflake.com/en/developer-guide/python-connector/python-connector",
            credential_schema=[
                CredentialField(name="account", label="Account", secret=False),
                CredentialField(name="warehouse", label="Warehouse", secret=False),
                CredentialField(name="database", label="Database", secret=False),
                CredentialField(name="schema", label="Schema", secret=False),
                CredentialField(name="user", label="User", secret=False),
                CredentialField(name="password", label="Password", secret=True, required=False),
                CredentialField(name="private_key", label="Private key", secret=True, required=False),
                CredentialField(name="private_key_passphrase", label="Private key passphrase", secret=True, required=False),
            ],
            local_verification=VerificationMode.CREDENTIAL_REQUIRED,
            sync_behavior="Validate warehouse access, sync schemas/tables/columns, and sample metadata.",
            production_notes="Install Snowflake driver in deployments that enable this connector.",
            recommended=True,
        ),
        ConnectorDefinition(
            slug="redshift",
            display_name="Amazon Redshift",
            category=ConnectorCategory.DATA_STORE,
            logo_key="amazon-redshift",
            docs_url="https://docs.aws.amazon.com/redshift/",
            credential_schema=[host_field("cluster_endpoint", "Cluster endpoint"), CredentialField(name="port", label="Port", secret=False, required=False, placeholder="5439"), CredentialField(name="database", label="Database", secret=False), CredentialField(name="user", label="User", secret=False), token_field("password", "Password")],
            local_verification=VerificationMode.CREDENTIAL_REQUIRED,
            sync_behavior="Validate cluster access and sync schemas, tables, columns, and freshness hints.",
            production_notes="Use IAM auth or Secrets Manager in hardened deployments.",
        ),
        ConnectorDefinition(
            slug="sql_server",
            display_name="SQL Server",
            category=ConnectorCategory.DATA_STORE,
            logo_key="microsoft-sql-server",
            docs_url="https://learn.microsoft.com/en-us/sql/connect/",
            credential_schema=[host_field(), CredentialField(name="port", label="Port", secret=False, required=False, placeholder="1433"), CredentialField(name="database", label="Database", secret=False), CredentialField(name="user", label="User", secret=False), token_field("password", "Password")],
            local_verification=VerificationMode.CREDENTIAL_REQUIRED,
            sync_behavior="Validate connection and sync INFORMATION_SCHEMA metadata.",
            production_notes="Requires ODBC driver availability in production image.",
        ),
        ConnectorDefinition(
            slug="databricks",
            display_name="Databricks",
            category=ConnectorCategory.DATA_STORE,
            logo_key="databricks",
            docs_url="https://docs.databricks.com/api/",
            credential_schema=[host_field("workspace_url", "Workspace URL"), CredentialField(name="http_path", label="SQL warehouse HTTP path", secret=False), token_field("token")],
            local_verification=VerificationMode.CREDENTIAL_REQUIRED,
            sync_behavior="Validate SQL warehouse, sync Unity Catalog assets, jobs, lineage, and freshness.",
            production_notes="Requires Databricks SQL connector in production image.",
            recommended=True,
        ),
        ConnectorDefinition(
            slug="bigquery",
            display_name="BigQuery",
            category=ConnectorCategory.DATA_STORE,
            logo_key="google-bigquery",
            docs_url="https://cloud.google.com/bigquery/docs/reference/libraries",
            credential_schema=[
                token_field("service_account_json", "Service account JSON"),
                CredentialField(name="project_id", label="Project ID", secret=False),
                CredentialField(name="emulator_host", label="Emulator host", secret=False, required=False, placeholder="http://127.0.0.1:19050"),
            ],
            local_verification=VerificationMode.CREDENTIAL_REQUIRED,
            sync_behavior="Validate project/dataset access and sync table, column, and job metadata.",
            production_notes="Requires Google Cloud BigQuery client in production image.",
        ),
        ConnectorDefinition(
            slug="mysql",
            display_name="MySQL",
            category=ConnectorCategory.DATA_STORE,
            logo_key="mysql",
            docs_url="https://dev.mysql.com/doc/",
            credential_schema=[host_field(), CredentialField(name="port", label="Port", secret=False, placeholder="3306"), CredentialField(name="database", label="Database", secret=False), CredentialField(name="user", label="User", secret=False), token_field("password", "Password")],
            local_verification=VerificationMode.CREDENTIAL_REQUIRED,
            sync_behavior="Validate connection and sync INFORMATION_SCHEMA metadata.",
            production_notes="Requires MySQL async driver in production image.",
        ),
        ConnectorDefinition(
            slug="trino",
            display_name="Trino",
            category=ConnectorCategory.DATA_STORE,
            logo_key="trino",
            docs_url="https://trino.io/docs/current/client/python.html",
            credential_schema=[
                host_field(),
                CredentialField(name="port", label="Port", secret=False, required=False, placeholder="8080"),
                CredentialField(name="catalog", label="Catalog", secret=False),
                CredentialField(name="schema", label="Schema", secret=False),
                CredentialField(name="user", label="User", secret=False),
                CredentialField(name="password", label="Password", secret=True, required=False),
            ],
            local_verification=VerificationMode.CREDENTIAL_REQUIRED,
            sync_behavior="Validate connection and sync Trino catalog, schema, table, and column metadata.",
            production_notes="Uses the Trino Python DBAPI client; configure auth mode per deployment.",
        ),
        ConnectorDefinition(
            slug="airflow",
            display_name="Airflow",
            category=ConnectorCategory.ORCHESTRATION,
            logo_key="apache-airflow",
            docs_url="https://airflow.apache.org/docs/apache-airflow/stable/stable-rest-api-ref.html",
            credential_schema=[host_field("base_url", "Base URL"), CredentialField(name="username", label="Username", secret=False), token_field("password", "Password")],
            local_verification=VerificationMode.REAL,
            sync_behavior="Sync DAGs, tasks, run history, failed pipelines, owners, and schedule freshness.",
            production_notes="Airflow API availability varies by auth configuration.",
            recommended=True,
        ),
        ConnectorDefinition(
            slug="dbt",
            display_name="dbt",
            category=ConnectorCategory.ORCHESTRATION,
            logo_key="dbt",
            docs_url="https://docs.getdbt.com/dbt-cloud/api-v2",
            credential_schema=[
                token_field("api_token"),
                CredentialField(
                    name="account_id",
                    label="Account ID",
                    secret=False,
                    placeholder="12345",
                ),
                CredentialField(
                    name="base_url",
                    label="Base URL (optional)",
                    secret=False,
                    required=False,
                    placeholder="https://cloud.getdbt.com/api/v2/accounts/12345 - leave blank for dbt Cloud, set for fixture or self-hosted",
                ),
            ],
            local_verification=VerificationMode.REAL,
            sync_behavior="Sync models, sources, exposures, docs, lineage graph, tests, and run results.",
            production_notes="Supports dbt Cloud API metadata and GitHub project sync. Override base_url to target a fixture or self-hosted endpoint.",
            recommended=True,
        ),
        ConnectorDefinition(
            slug="fivetran",
            display_name="Fivetran",
            category=ConnectorCategory.ORCHESTRATION,
            logo_key="fivetran",
            docs_url="https://fivetran.com/docs/rest-api",
            credential_schema=[CredentialField(name="api_key", label="API key", secret=False), token_field("api_secret", "API secret")],
            local_verification=VerificationMode.REAL,
            sync_behavior="Sync connectors, schemas, sync state, failures, and freshness breaches.",
            production_notes="Uses Fivetran REST API basic auth.",
        ),
        ConnectorDefinition(
            slug="dagster",
            display_name="Dagster",
            category=ConnectorCategory.ORCHESTRATION,
            logo_key="dagster",
            docs_url="https://docs.dagster.io/",
            credential_schema=[host_field("graphql_url", "GraphQL URL"), token_field("token", "Token")],
            local_verification=VerificationMode.REAL,
            sync_behavior="Sync assets, materializations, partitions, checks, runs, and failures.",
            production_notes="GraphQL endpoint is required.",
        ),
        ConnectorDefinition(
            slug="prefect",
            display_name="Prefect",
            category=ConnectorCategory.ORCHESTRATION,
            logo_key="prefect",
            docs_url="https://docs.prefect.io/latest/api-ref/rest-api/",
            credential_schema=[host_field("api_url", "API URL"), token_field("api_key")],
            local_verification=VerificationMode.REAL,
            sync_behavior="Sync flows, deployments, runs, failures, owners, and freshness hints.",
            production_notes="Prefect Cloud and self-hosted API URLs are both supported by config.",
        ),
        ConnectorDefinition(
            slug="airbyte",
            display_name="Airbyte",
            category=ConnectorCategory.ORCHESTRATION,
            logo_key="airbyte",
            docs_url="https://reference.airbyte.com/",
            credential_schema=[host_field("api_url", "API URL"), token_field("api_key", "API key")],
            local_verification=VerificationMode.REAL,
            sync_behavior="Sync sources, destinations, connection state, schemas, and job failures.",
            production_notes="Supports Airbyte API-compatible deployments.",
        ),
        ConnectorDefinition(
            slug="openai",
            display_name="OpenAI",
            category=ConnectorCategory.LLM,
            logo_key="openai",
            docs_url="https://platform.openai.com/docs/",
            credential_schema=[token_field("api_key", "API key"), CredentialField(name="model", label="Model", secret=False, required=False, placeholder="gpt-4.1-mini")],
            local_verification=VerificationMode.REAL,
            sync_behavior="Validate API key and model availability, then use raw chat completions with function calling.",
            production_notes="Reads OPENAI_API_KEY from .env for local testing.",
            recommended=True,
        ),
    ]


# Stability tiers per connector - applied after catalog construction so each
# connector definition above stays focused on credentials/behavior. Update
# this map (and Stability enum) when promoting a connector or recording a
# regression. Mirrors docs-site/src/pages/connector-matrix.md.
_STABILITY: dict[str, tuple[Stability, list[str], str]] = {
    # 🟢 stable - read + write exercised live with audit/approval verified
    "postgres":   (Stability.STABLE, [], "DDL (CREATE TABLE) and DML (INSERT) executed end-to-end via chat; approval gate + AgentWriteAudit confirmed."),
    "mysql":      (Stability.STABLE, [], "Integration-tested via docker-compose mysql:8.0."),
    "sql_server": (Stability.STABLE, [], "Integration-tested via docker-compose mssql:2022."),
    "trino":      (Stability.STABLE, [], "Integration-tested with memory catalog; read/write/SELECT/INSERT exercised."),
    "bigquery":   (Stability.STABLE, [], "BigQuery emulator read/write E2E passed with approval gate, row-count verification, and cleanup."),
    "sqlite":     (Stability.STABLE, [], "Built-in demo seeded at startup; read/write fully covered."),
    "snowflake":  (Stability.STABLE, [], "Live Snowflake read/write E2E passed with explicit warehouse context, approval gate, approved execution, verification, and cleanup."),
    "databricks": (Stability.STABLE, [], "Live Databricks warehouse reads and approval-gated SQL execution passed against the configured SQL warehouse."),
    "notion":     (Stability.STABLE, [], "Acme coverage shard passed 5/5 runs with read_ and write_ tool fixtures exercised against seeded Notion data."),
    "confluence": (Stability.STABLE, [], "Acme coverage shard passed 5/5 runs with read_ and write_ tool fixtures exercised against seeded Confluence data."),
    "github":     (Stability.STABLE, [], "Acme coverage shard passed 5/5 runs with read_ and write_ tool fixtures exercised against seeded GitHub data."),

    # 🔵 stable read-only - reads production-ready; writes pending adapter work
    "fivetran":   (Stability.STABLE_READ_ONLY, [
                       "write_trigger_sync is approval-gated but not executed by design for the read-only tier",
                   ], "Live Fivetran list connectors and sync-history reads passed; trigger-sync returned pending approval and was not executed."),

    # 🟡 beta - adapter wired; fixture-backed E2E only; live SaaS E2E pending
    "airflow":    (Stability.BETA, [], "Fixture-backed DAG reads/source/logs plus trigger/pause/create writes passed with approval gates; first-boot flake mitigated by adapter-level retry (2 attempts, 1s + 3s backoff)."),
    "redshift":   (Stability.BETA, [], "psycopg3 UNICODE codec aliased at adapter import time; live SELECT verified against Redshift Serverless. Live read/write E2E pending."),
    "dbt":        (Stability.STABLE, [], "Acme coverage shard passed 5/5 runs with read_ and write_ tool fixtures exercised against seeded dbt data."),
    "prefect":    (Stability.STABLE, [], "Acme coverage shard passed 5/5 runs with read_ and write_ tool fixtures exercised against seeded Prefect data."),
    "dagster":    (Stability.STABLE, [], "Acme coverage shard passed 5/5 runs with read_ and write_ tool fixtures exercised against seeded Dagster data."),
    "airbyte":    (Stability.STABLE, [], "Acme coverage shard passed 5/5 runs with read_ and write_ tool fixtures exercised against seeded Airbyte data."),

    # 🔴 known issue - disabled by default; opt-in via EXPERIMENTAL_ENABLE_<slug>
    # (empty - Redshift was promoted to BETA after the UNICODE codec alias landed)

    # 🚫 unsupported - adapter exists for legacy reasons; service dead/unreachable
    "quip":       (Stability.UNSUPPORTED, [
                       "Quip was discontinued by Salesforce in 2024 - no new integration tokens can be created",
                   ], "Adapter retained for back-compat with existing Quip configs only."),
    "google_docs": (Stability.UNSUPPORTED, [
                       "OAuth consent flow not yet wired - service-account-only auth has limited document access",
                   ], "Service account auth works for small subsets; full Drive coverage needs the OAuth flow."),

    # llm_provider entries are not connectors in the same sense; mark stable to suppress noise
    "openai":     (Stability.STABLE, [], "Provider configuration tested end-to-end."),
    "ollama":     (Stability.BETA, [
                       "Requires ≥16 GB free RAM for llama3.1:8b; not exercised by automated tests yet",
                   ], "Adapter wired; tested manually in prior runs."),
}


def _apply_stability(items: list[ConnectorDefinition]) -> list[ConnectorDefinition]:
    for item in items:
        stab = _STABILITY.get(item.slug)
        if stab is None:
            continue
        item.stability, item.known_issues, item.stability_notes = stab
    return items


CATALOG_BY_SLUG = {item.slug: item for item in _apply_stability(catalog())}


class TestResult(BaseModel):
    slug: str
    status: Literal["ok", "failed", "credential_required", "not_configured"]
    mode: VerificationMode
    message: str
    details: dict[str, Any] = Field(default_factory=dict)
