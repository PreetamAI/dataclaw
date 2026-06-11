from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

EnvRequirement = str | tuple[str, ...]

SAAS_ENV: dict[str, list[EnvRequirement]] = {
    "notion": [("NOTION_INTEGRATION_TOKEN", "NOTION_TOKEN"), "NOTION_TEST_PARENT_PAGE_ID"],
    "github": [("GITHUB_TEST_TOKEN", "GH_TEST_TOKEN"), ("GITHUB_TEST_REPO", "GH_TEST_REPO")],
    "confluence": [
        "CONFLUENCE_SITE_URL",
        "CONFLUENCE_EMAIL",
        "CONFLUENCE_SPACE_KEY",
        ("CONFLUENCE_API_TOKEN", "CONFLUENCE_API_BASIC_AUTH_TOKEN", "CONFLUENCE_API_OAUTH_TOKEN"),
    ],
    "bigquery": ["BIGQUERY_SERVICE_ACCOUNT_JSON", "BIGQUERY_PROJECT_ID"],
    "snowflake": ["SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", ("SNOWFLAKE_PASSWORD", "SNOWFLAKE_PRIVATE_KEY")],
    "databricks": [("DATABRICKS_WORKSPACE_URL", "DATABRICKS_HOST"), "DATABRICKS_HTTP_PATH", "DATABRICKS_TOKEN"],
    "redshift": [("REDSHIFT_CLUSTER_ENDPOINT", "REDSHIFT_ENDPOINT"), "REDSHIFT_USER", "REDSHIFT_PASSWORD"],
    "fivetran": ["FIVETRAN_API_KEY", "FIVETRAN_API_SECRET"],
}

DEFAULT_SNOWFLAKE_WAREHOUSE = "COMPUTE_WH"

ACME_DOCS = {
    "Customers data model": (
        "Acme Co customers are segmented into enterprise, growth, and self-serve tiers. "
        "The canonical customer identifier starts in Postgres raw.customers and becomes "
        "cust_id in BigQuery acme_analytics.dim_customers."
    ),
    "Churn definition": (
        "Churn is counted when a paying customer cancels, downgrades to free, or has no "
        "successful order for 30 days. Last week's spike is owned by the acme_churn_calc DAG "
        "and is reflected in Snowflake ACME.MARTS.CHURN_EVENTS."
    ),
    "On-call runbook": (
        "For customer-impacting data incidents, check Airflow acme_churn_calc, Prefect "
        "acme_revenue_recalc, Snowflake ACME.MARTS.REVENUE_DAILY, and the Confluence "
        "Postgres to BigQuery pipeline page before paging finance engineering."
    ),
    "Finance duplicate-payment runbook": (
        "Finance Engineering owns duplicate-payment review for enterprise customers. "
        "For Northstar Retail Group, start with core.customers.email, inspect core.orders "
        "and core.payments for multiple succeeded charges on one order, then check "
        "core.refunds before Support issues a credit. The refund_alerts and "
        "payments_reconciliation Airflow DAGs provide operational context."
    ),
    "Order status definitions": (
        "stuck_in_3ds means the payment is blocked in the 3-D Secure authentication flow "
        "and the customer has not completed the issuer challenge. It is not successful "
        "revenue until core.payments.status becomes succeeded."
    ),
}

ACME_DBT_FILES = {
    "dbt_project.yml": "name: dataclaw_acme\nversion: '1.0'\nprofile: acme\nmodel-paths: ['models']\n",
    "profiles.yml": "acme:\n  target: dev\n  outputs:\n    dev:\n      type: bigquery\n      method: service-account\n      project: dataclaw-bq\n      dataset: acme_analytics\n",
    "models/staging/stg_customers.sql": "select customer_id, segment, created_at from {{ source('raw', 'customers') }}\n",
    "models/marts/dim_customers.sql": "select customer_id as cust_id, segment, created_at from {{ ref('stg_customers') }}\n",
    "models/marts/fct_orders.sql": "select order_id, customer_id as cust_id, arr_usd from {{ source('raw', 'orders') }}\n",
}


def env_first(*names: str) -> str | None:
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return None


def github_test_repo() -> str | None:
    explicit = env_first("GITHUB_TEST_REPO", "GH_TEST_REPO")
    if explicit:
        return explicit
    owner = os.getenv("GITHUB_REPOSITORY_OWNER")
    repo_name = os.getenv("ACME_GITHUB_REPO_NAME")
    if owner and repo_name:
        return f"{owner}/{repo_name}"
    return None


def normalize_snowflake_account(value: str) -> str:
    account = value.strip()
    if not account:
        return account
    parsed = urlparse(account if "://" in account else f"https://{account}")
    host = (parsed.hostname or account).strip().rstrip("/")
    for suffix in (".privatelink.snowflakecomputing.com", ".snowflakecomputing.com"):
        if host.endswith(suffix):
            host = host[: -len(suffix)]
            break
    return host


def parse_redshift_endpoint(value: str, default_port: int = 5439) -> tuple[str, int, str | None]:
    raw = value.strip()
    parsed = urlparse(raw if "://" in raw else f"redshift://{raw}")
    host = parsed.hostname or raw.split(":", 1)[0]
    database = parsed.path.lstrip("/") or None
    return host, parsed.port or default_port, database


def redshift_cluster_identifier(endpoint: str) -> str | None:
    host = parse_redshift_endpoint(endpoint)[0]
    first_label = host.split(".", 1)[0].strip()
    return first_label or None


def missing_env(names: list[EnvRequirement]) -> list[str]:
    missing: list[str] = []
    for requirement in names:
        if isinstance(requirement, tuple):
            if not env_first(*requirement):
                missing.append(" or ".join(requirement))
            continue
        if not os.getenv(requirement):
            missing.append(requirement)
    return missing


def skipped(reason: str) -> dict[str, Any]:
    return {"status": "skipped", "reason": reason}


def sdk_missing(name: str, exc: ImportError) -> dict[str, Any]:
    return skipped(f"{name} is not installed: {exc.name or exc.__class__.__name__}")


def record_action(actions: list[dict[str, str]], service: str, entity: str, action: str) -> None:
    actions.append({"service": service, "entity": entity, "action": action})
    print(f"{service}: {action} {entity}")
