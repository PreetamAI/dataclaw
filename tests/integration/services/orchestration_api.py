from __future__ import annotations

import base64
import os
from datetime import UTC, datetime
from typing import Any

from fastapi import FastAPI, Request

SERVICE = os.getenv("CONNECTOR_SERVICE", "generic")
AIRFLOW_RUN_ID = "manual__acme_coverage"
AIRFLOW_RUNS: dict[str, list[dict]] = {
    "acme_etl_daily": [
        {
            "dag_id": "acme_etl_daily",
            "dag_run_id": AIRFLOW_RUN_ID,
            "state": "success",
            "conf": {"acme": True},
            "logical_date": datetime.now(UTC).isoformat(),
        }
    ],
    "acme_churn_calc": [
        {
            "dag_id": "acme_churn_calc",
            "dag_run_id": AIRFLOW_RUN_ID,
            "state": "failed",
            "conf": {"acme": True},
            "logical_date": datetime.now(UTC).isoformat(),
        }
    ],
}
AIRFLOW_DAGS: dict[str, dict] = {
    "acme_etl_daily": {
        "dag_id": "acme_etl_daily",
        "is_paused": False,
        "owners": ["data"],
        "schedule_interval": {"value": "0 2 * * *"},
        "tags": ["acme", "postgres", "bigquery"],
        "source": (
            'with DAG(dag_id="acme_etl_daily") as dag:\n'
            '    extract = BashOperator(task_id="extract", bash_command="echo extract raw.customers raw.orders")\n'
            '    load = BashOperator(task_id="load_bq", bash_command="echo load bq_raw")\n'
            "    extract >> load\n"
        ),
        "tasks": [
            {"task_id": "extract", "downstream_task_ids": ["load_bq"]},
            {"task_id": "load_bq", "downstream_task_ids": []},
        ],
    },
    "acme_churn_calc": {
        "dag_id": "acme_churn_calc",
        "is_paused": False,
        "owners": ["data"],
        "schedule_interval": {"value": "0 4 * * *"},
        "tags": ["acme", "churn", "failed-fixture"],
        "source": (
            'with DAG(dag_id="acme_churn_calc") as dag:\n'
            '    score = BashOperator(task_id="score_churn", bash_command="echo score churn && exit 1")\n'
        ),
        "tasks": [{"task_id": "score_churn", "downstream_task_ids": []}],
    },
    "daily_orders_refresh": {
        "dag_id": "daily_orders_refresh",
        "is_paused": False,
        "owners": ["data"],
        "schedule_interval": {"value": "@daily"},
        "tags": ["revenue", "ingest"],
        "source": (
            "from airflow import DAG\n"
            "def load_orders():\n"
            "    sql = \"insert into orders select * from staging_orders\"\n"
            "    return sql\n"
        ),
    },
    "hourly_dim_customers": {
        "dag_id": "hourly_dim_customers",
        "is_paused": False,
        "owners": ["data"],
        "schedule_interval": {"value": "@hourly"},
        "tags": ["dimensions"],
    },
    "weekly_finance_close": {
        "dag_id": "weekly_finance_close",
        "is_paused": True,
        "owners": ["finance"],
        "schedule_interval": {"value": "0 0 * * MON"},
        "tags": ["finance"],
    },
}
AIRFLOW_VARIABLES: dict[str, dict] = {
    "acme_coverage_marker": {"key": "acme_coverage_marker", "value": "seeded", "description": "Acme coverage marker"}
}
AIRFLOW_POOLS: dict[str, dict] = {
    "default_pool": {"name": "default_pool", "slots": 128, "description": "Default Airflow pool"}
}
DBT_RUNS: dict[int, list[dict]] = {}
AIRBYTE_WORKSPACE = {"workspaceId": "airbyte-workspace", "name": "Local Airbyte Acme fixture"}
AIRBYTE_SOURCES: dict[str, dict] = {
    "acme-postgres-source": {
        "sourceId": "acme-postgres-source",
        "name": "raw_postgres",
        "sourceName": "Postgres",
        "workspaceId": AIRBYTE_WORKSPACE["workspaceId"],
    }
}
AIRBYTE_DESTINATIONS: dict[str, dict] = {
    "acme-bq-destination": {
        "destinationId": "acme-bq-destination",
        "name": "bq_raw",
        "destinationName": "BigQuery",
        "workspaceId": AIRBYTE_WORKSPACE["workspaceId"],
    }
}
AIRBYTE_JOBS: list[dict] = [
    {
        "id": 1,
        "connectionId": "orders-to-warehouse",
        "jobType": "sync",
        "status": "succeeded",
        "createdAt": datetime.now(UTC).isoformat(),
        "logs": ["raw_postgres -> bq_raw sync completed for Acme orders"],
    }
]
AIRBYTE_CONNECTIONS: dict[str, dict] = {
    "orders-to-warehouse": {
        "connectionId": "orders-to-warehouse",
        "name": "raw_postgres -> bq_raw",
        "sourceId": "acme-postgres-source",
        "destinationId": "acme-bq-destination",
        "status": "active",
        "scheduleType": "basic",
        "destination": "bigquery",
        "syncCatalog": {
            "streams": [
                {"stream": {"name": "customers", "namespace": "raw_postgres"}, "config": {"selected": True}},
                {"stream": {"name": "orders", "namespace": "raw_postgres"}, "config": {"selected": True}},
            ]
        },
    }
}
PREFECT_ACME_FLOW = {"id": "flow-acme-revenue-recalc", "name": "acme_revenue_recalc"}
PREFECT_LEGACY_FLOW = {"id": "flow-orders", "name": "orders refresh"}
PREFECT_FLOWS = [PREFECT_ACME_FLOW, PREFECT_LEGACY_FLOW]
PREFECT_ACME_DEPLOYMENT = {
    "id": "deployment-acme-revenue-recalc",
    "name": "acme_revenue_recalc/default",
    "flow_id": PREFECT_ACME_FLOW["id"],
    "entrypoint": "flows.acme_revenue_recalc:flow",
}
PREFECT_LEGACY_DEPLOYMENT = {
    "id": "deployment-daily-ingestion",
    "name": "daily_ingestion",
    "flow_id": PREFECT_LEGACY_FLOW["id"],
    "entrypoint": "flows.daily_ingestion:flow",
}
PREFECT_RUNS: list[dict] = [
    {
        "id": "prefect-run-acme-revenue-recalc",
        "deployment_id": PREFECT_ACME_DEPLOYMENT["id"],
        "name": "acme_revenue_recalc scheduled run",
        "flow_id": PREFECT_ACME_FLOW["id"],
        "state": {"type": "COMPLETED", "name": "Completed"},
        "parameters": {"dataset": "snowflake_churn_events"},
        "created": datetime.now(UTC).isoformat(),
    }
]
PREFECT_DEPLOYMENTS: list[dict] = [PREFECT_ACME_DEPLOYMENT.copy(), PREFECT_LEGACY_DEPLOYMENT.copy()]
DAGSTER_JOB_NAME = "acme_assets"
DAGSTER_SENSOR_NAME = "acme_asset_sensor"
DAGSTER_SCHEDULE_NAME = "acme_assets_daily"
DAGSTER_PARTITION = "2026-05-20"
DAGSTER_ASSETS: list[dict] = [
    {
        "id": "asset-customers",
        "key": {"path": ["customers"]},
        "assetKey": {"path": ["customers"]},
        "partitionKeys": [DAGSTER_PARTITION],
        "assetMaterializations": [
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "runId": "dagster-run-acme-assets",
                "metadataEntries": [{"label": "rows", "description": "1000 Acme customer rows materialized"}],
            }
        ],
        "assetChecksOrError": {
            "__typename": "AssetChecks",
            "checks": [{"name": "customers_not_null", "executionForLatestMaterialization": {"status": "SUCCEEDED"}}],
        },
    },
    {
        "id": "asset-orders",
        "key": {"path": ["orders"]},
        "assetKey": {"path": ["orders"]},
        "partitionKeys": [DAGSTER_PARTITION],
        "assetMaterializations": [
            {
                "timestamp": datetime.now(UTC).isoformat(),
                "runId": "dagster-run-acme-assets",
                "metadataEntries": [{"label": "rows", "description": "5000 Acme order rows materialized"}],
            }
        ],
        "assetChecksOrError": {
            "__typename": "AssetChecks",
            "checks": [{"name": "orders_have_customer", "executionForLatestMaterialization": {"status": "SUCCEEDED"}}],
        },
    },
]
DAGSTER_REPOSITORIES: list[dict] = [
    {
        "name": "dataclaw",
        "pipelines": [{"name": DAGSTER_JOB_NAME, "description": "Materializes Acme customers and orders assets.", "modes": [{"name": "default"}]}],
        "sensors": [{"name": DAGSTER_SENSOR_NAME, "sensorState": {"status": "RUNNING"}}],
        "schedules": [{"name": DAGSTER_SCHEDULE_NAME, "scheduleState": {"status": "RUNNING"}}],
    }
]
DAGSTER_RUNS: list[dict] = [
    {
        "runId": "dagster-run-acme-assets",
        "status": "SUCCESS",
        "pipelineName": DAGSTER_JOB_NAME,
        "stepKeysToExecute": ["customers", "orders"],
        "startTime": datetime.now(UTC).timestamp(),
        "endTime": datetime.now(UTC).timestamp(),
        "createdAt": datetime.now(UTC).isoformat(),
    }
]
DAGSTER_EVENTS: list[dict] = [
    {"__typename": "MaterializationEvent", "message": "Materialized Acme customers asset", "timestamp": datetime.now(UTC).isoformat(), "stepKey": "customers", "level": "INFO"},
    {"__typename": "MaterializationEvent", "message": "Materialized Acme orders asset", "timestamp": datetime.now(UTC).isoformat(), "stepKey": "orders", "level": "INFO"},
]
DATABRICKS_STATEMENTS: dict[str, dict] = {}
DATABRICKS_CLUSTER = {"cluster_id": "cluster-acme-analytics", "cluster_name": "acme-analytics"}
DATABRICKS_JOB = {"job_id": 42, "settings": {"name": "acme_events_refresh"}}
DATABRICKS_WAREHOUSE = {"id": "warehouse-acme-sql", "name": "acme-sql-warehouse", "state": "RUNNING"}
DATABRICKS_NOTEBOOK = {
    "path": "/Shared/dataclaw/acme/events_refresh",
    "content": base64.b64encode(b"select * from acme.silver.events").decode(),
    "language": "SQL",
}
DATABRICKS_JOB_RUNS: list[dict] = [
    {
        "run_id": 5000,
        "job_id": DATABRICKS_JOB["job_id"],
        "state": {"life_cycle_state": "TERMINATED", "result_state": "SUCCESS"},
        "created_at": datetime.now(UTC).isoformat(),
        "logs": "Refreshed acme.silver.events with 1000 event rows.",
    }
]
NOTION_FIXTURES: dict[str, dict] = {
    "page-data-glossary": {
        "title": "Data Glossary",
        "body": "The [[orders]] table captures customer purchases. It joins [[customers]] on customer_id and powers [[ltv]].",
    },
    "page-ownership-runbook": {
        "title": "Ownership Runbook",
        "body": (
            "Analytics owns [[orders]], [[customers]], and the [[daily_orders_refresh]] pipeline. "
            "Finance Engineering owns [[refund_alerts]], [[payments_reconciliation]], and the Duplicate "
            "Payment Investigation runbook. Customer escalations from enterprise accounts, including "
            "Northstar Retail Group, route to finance-eng@dataclaw.com before Support issues credits."
        ),
    },
    "page-refund-alerts-sop": {
        "title": "Refund Alerts SOP",
        "body": (
            "Refund Alerts SOP. The refund_alerts Airflow DAG monitors core.refunds every 15 minutes. "
            "If a customer reports a double charge, first confirm core.customers.email, then join "
            "core.orders, core.payments, and core.refunds. A duplicate charge is any order with more "
            "than one succeeded payment for the same order_id. For enterprise accounts such as "
            "Northstar Retail Group, verify the Stripe charge ids, check whether a prior refund exists, "
            "and confirm refund reason plus issued_at timestamp before creating a new refund. Owner: "
            "finance-eng@dataclaw.com. Escalate unresolved duplicate-payment incidents to "
            "#finance-eng-oncall."
        ),
    },
    "page-duplicate-payment-runbook": {
        "title": "Duplicate Payment Investigation Runbook",
        "body": (
            "Duplicate Payment Investigation Runbook. Start with core.customers.email, then inspect "
            "core.orders and core.payments for multiple succeeded charges on one order. Known demo "
            "case: Priya Shah at Northstar Retail Group reported a duplicate enterprise subscription "
            "charge after a payment retry. Finance should review the affected order, the original "
            "Stripe charge, the retry charge, and any existing core.refunds row before issuing a credit."
        ),
    },
    "page-order-status-definitions": {
        "title": "Order Status Definitions",
        "body": (
            "Order status definitions. stuck_in_3ds means the payment is blocked in the 3-D Secure "
            "authentication flow and the customer has not completed the issuer challenge. It is not "
            "a successful payment and should be treated as an abandoned or failed authentication until "
            "core.payments.status becomes succeeded."
        ),
    },
    "page-metrics-handbook": {
        "title": "Metrics Handbook",
        "body": "LTV means customer lifetime value. MRR is monthly recurring revenue. ARR is annual recurring revenue.",
    },
    "page-data-quality-policies": {
        "title": "Data Quality Policies",
        "body": "Freshness checks watch [[orders]] and [[fct_orders]]. Failed dbt tests page analytics on call.",
    },
}
NOTION_PAGES: list[dict] = []
NOTION_APPENDS: dict[str, list[dict]] = {}
GITHUB_FILES: dict[str, dict] = {}
GITHUB_PULLS: list[dict] = []
CONFLUENCE_PAGES: list[dict] = []

app = FastAPI(title=f"DataClaw {SERVICE} integration fixture")


@app.get("/health")
async def generic_health() -> dict:
    return {"status": "healthy", "service": SERVICE}


@app.get("/objects")
async def generic_objects() -> dict:
    return {"objects": [{"id": f"{SERVICE}-object-1", "name": f"{SERVICE} object"}]}


@app.get("/api/v1/health")
async def airflow_health() -> dict:
    return {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}}


@app.get("/api/v1/dags")
async def airflow_dags() -> dict:
    dags = list(AIRFLOW_DAGS.values())
    return {"dags": dags, "total_entries": len(dags)}


@app.post("/api/v1/dags")
async def airflow_create_dag(request: Request) -> dict:
    payload = await request.json()
    dag_id = payload.get("dag_id") or "dataclaw_generated_dag"
    dag = {
        "dag_id": dag_id,
        "is_paused": bool(payload.get("is_paused", False)),
        "owners": payload.get("owners") or ["data"],
        "schedule_interval": {"value": payload.get("schedule_interval") or payload.get("schedule") or "@daily"},
        "tags": payload.get("tags") or ["generated"],
        "source": payload.get("source") or "",
    }
    AIRFLOW_DAGS[dag_id] = dag
    return dag


@app.patch("/api/v1/dags/{dag_id}")
async def airflow_patch_dag(dag_id: str, request: Request) -> dict:
    payload = await request.json()
    dag = AIRFLOW_DAGS.setdefault(
        dag_id,
        {"dag_id": dag_id, "is_paused": False, "owners": ["data"], "schedule_interval": {"value": "@daily"}, "tags": []},
    )
    if "is_paused" in payload:
        dag["is_paused"] = bool(payload["is_paused"])
    return dag


@app.get("/api/v1/dags/{dag_id}/source")
async def airflow_dag_source(dag_id: str) -> dict:
    dag = AIRFLOW_DAGS.get(dag_id, {})
    return {"dag_id": dag_id, "source": dag.get("source") or f"dag_id = '{dag_id}'"}


@app.get("/api/v1/dags/{dag_id}/details")
async def airflow_dag_details(dag_id: str) -> dict:
    dag = AIRFLOW_DAGS.get(dag_id, {"dag_id": dag_id, "tasks": []})
    return {"dag_id": dag_id, "tasks": dag.get("tasks", []), "file_token": dag_id, "fileloc": f"/opt/airflow/dags/{dag_id}.py"}


@app.post("/api/v1/dags/{dag_id}/dagRuns")
async def airflow_trigger_dag(dag_id: str, request: Request) -> dict:
    payload = await request.json()
    run = {
        "dag_id": dag_id,
        "dag_run_id": f"manual__{len(AIRFLOW_RUNS.get(dag_id, [])) + 1}",
        "state": "success",
        "conf": payload.get("conf") or {},
        "logical_date": datetime.now(UTC).isoformat(),
    }
    AIRFLOW_RUNS.setdefault(dag_id, []).append(run)
    return run


@app.get("/api/v1/dags/{dag_id}/dagRuns")
async def airflow_dag_runs(dag_id: str) -> dict:
    runs = AIRFLOW_RUNS.get(dag_id, [])
    return {"dag_runs": list(reversed(runs)), "total_entries": len(runs)}


@app.get("/api/v1/dags/{dag_id}/dagRuns/{run_id}")
async def airflow_dag_run(dag_id: str, run_id: str) -> dict:
    for run in AIRFLOW_RUNS.get(dag_id, []):
        if run["dag_run_id"] == run_id:
            return run
    return {"dag_id": dag_id, "dag_run_id": run_id, "state": "not_found"}


@app.get("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
async def airflow_task_instances(dag_id: str, run_id: str) -> dict:
    state = next((run.get("state") for run in AIRFLOW_RUNS.get(dag_id, []) if run["dag_run_id"] == run_id), "success")
    tasks = AIRFLOW_DAGS.get(dag_id, {}).get("tasks") or [{"task_id": "extract"}]
    task_instances = [
        {
            "dag_id": dag_id,
            "dag_run_id": run_id,
            "task_id": task["task_id"],
            "state": "failed" if state == "failed" and task["task_id"] == "score_churn" else "success",
            "try_number": 1,
        }
        for task in tasks
    ]
    return {"task_instances": task_instances, "total_entries": len(task_instances)}


@app.get("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{try_number}")
async def airflow_task_logs(dag_id: str, run_id: str, task_id: str, try_number: int) -> str:
    return f"{dag_id}/{run_id}/{task_id} try {try_number}: Acme task log"


@app.get("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries")
async def airflow_xcom_entries(dag_id: str, run_id: str, task_id: str) -> dict:
    return {"xcom_entries": [{"dag_id": dag_id, "dag_run_id": run_id, "task_id": task_id, "key": "return_value", "value": "ok"}]}


@app.get("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/xcomEntries/{key}")
async def airflow_xcom_entry(dag_id: str, run_id: str, task_id: str, key: str) -> dict:
    return {"dag_id": dag_id, "dag_run_id": run_id, "task_id": task_id, "key": key, "value": "ok"}


@app.get("/api/v1/pools")
async def airflow_pools() -> dict:
    pools = list(AIRFLOW_POOLS.values())
    return {"pools": pools, "total_entries": len(pools)}


@app.get("/api/v1/pools/{name}")
async def airflow_get_pool(name: str) -> dict:
    return AIRFLOW_POOLS.get(name, {"name": name, "slots": 1, "description": ""})


@app.patch("/api/v1/pools/{name}")
async def airflow_patch_pool(name: str, request: Request) -> dict:
    payload = await request.json()
    pool = AIRFLOW_POOLS.setdefault(name, {"name": name, "slots": 1, "description": ""})
    pool.update({key: value for key, value in payload.items() if value is not None})
    pool["name"] = name
    return pool


@app.post("/api/v1/pools")
async def airflow_create_pool(request: Request) -> dict:
    payload = await request.json()
    name = payload.get("name") or "default_pool"
    AIRFLOW_POOLS[name] = {"name": name, "slots": payload.get("slots") or 1, "description": payload.get("description") or ""}
    return AIRFLOW_POOLS[name]


@app.get("/api/v1/variables")
async def airflow_variables() -> dict:
    variables = list(AIRFLOW_VARIABLES.values())
    return {"variables": variables, "total_entries": len(variables)}


@app.get("/api/v1/variables/{key}")
async def airflow_get_variable(key: str) -> dict:
    return AIRFLOW_VARIABLES.get(key, {"key": key, "value": ""})


@app.patch("/api/v1/variables/{key}")
async def airflow_patch_variable(key: str, request: Request) -> dict:
    payload = await request.json()
    variable = AIRFLOW_VARIABLES.setdefault(key, {"key": key, "value": ""})
    variable.update({field: value for field, value in payload.items() if value is not None})
    variable["key"] = key
    return variable


@app.post("/api/v1/variables")
async def airflow_create_variable(request: Request) -> dict:
    payload = await request.json()
    key = payload.get("key") or "acme_coverage_marker"
    AIRFLOW_VARIABLES[key] = {"key": key, "value": payload.get("value") or "", "description": payload.get("description")}
    return AIRFLOW_VARIABLES[key]


@app.get("/api/v1/importErrors")
async def airflow_import_errors() -> dict:
    return {"import_errors": [], "total_entries": 0}


@app.post("/api/v1/dags/{dag_id}/clearTaskInstances")
async def airflow_clear_task_instances(dag_id: str, request: Request) -> dict:
    payload = await request.json()
    return {"dag_id": dag_id, "cleared": payload.get("task_ids") or [], "dry_run": payload.get("dry_run", False)}


@app.patch("/api/v1/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}")
async def airflow_patch_task_instance(dag_id: str, run_id: str, task_id: str, request: Request) -> dict:
    payload = await request.json()
    return {"dag_id": dag_id, "dag_run_id": run_id, "task_id": task_id, "state": payload.get("new_state") or "success"}


@app.delete("/api/v1/dags/{dag_id}")
async def airflow_delete_dag(dag_id: str) -> dict:
    dag = AIRFLOW_DAGS.pop(dag_id, {"dag_id": dag_id})
    return {"dag_id": dag_id, "deleted": True, "dag": dag}


@app.get("/api/v2/accounts/{account_id}/runs/")
async def dbt_runs(account_id: int) -> dict:
    triggered_runs = DBT_RUNS.get(account_id, [])
    return {
        "data": triggered_runs
        + [
            {
                "id": 1,
                "trigger_id": 7,
                "account_id": account_id,
                "status_humanized": "Success",
                "job_id": 100,
                "is_complete": True,
            },
            {
                "id": 2,
                "trigger_id": 8,
                "account_id": account_id,
                "status_humanized": "Error",
                "job_id": 101,
                "is_complete": True,
            },
        ]
    }


@app.get("/api/v2/accounts/{account_id}/runs/{run_id}/")
async def dbt_run(account_id: int, run_id: int) -> dict:
    for run in DBT_RUNS.get(account_id, []):
        if run["id"] == run_id:
            return {"data": run}
    return {
        "data": {
            "id": run_id,
            "account_id": account_id,
            "status_humanized": "Not Found",
            "is_complete": True,
        }
    }


@app.get("/api/v2/accounts/{account_id}/projects/")
async def dbt_projects(account_id: int) -> dict:
    return {
        "data": [
            {"id": 1, "account_id": account_id, "name": "dataclaw-acme-dbt"},
            {"id": 2, "account_id": account_id, "name": "core_warehouse"},
            {"id": 3, "account_id": account_id, "name": "marketing"},
        ]
    }


@app.get("/api/v2/accounts/{account_id}/lineage/")
async def dbt_lineage(account_id: int) -> dict:
    return {
        "data": {
            "account_id": account_id,
            "nodes": [
                {"unique_id": "model.dataclaw.stg_customers", "name": "stg_customers"},
                {"unique_id": "model.dataclaw.dim_customers", "name": "dim_customers"},
                {"unique_id": "model.dataclaw.fct_orders", "name": "fct_orders"},
            ],
            "edges": [
                {"source": "model.dataclaw.stg_customers", "target": "model.dataclaw.dim_customers"},
                {"source": "source.dataclaw.raw.orders", "target": "model.dataclaw.fct_orders"},
                {"source": "model.dataclaw.dim_customers", "target": "model.dataclaw.fct_orders"},
            ],
        }
    }


@app.get("/api/v2/accounts/{account_id}/projects/{project_id}/lineage/")
async def dbt_project_lineage(account_id: int, project_id: int) -> dict:
    lineage = await dbt_lineage(account_id)
    lineage["data"]["project_id"] = project_id
    return lineage


@app.post("/api/v2/accounts/{account_id}/jobs/{job_id}/run/")
async def dbt_trigger_run(account_id: int, job_id: int, request: Request) -> dict:
    payload = await request.json()
    run = {
        "id": 1000 + len(DBT_RUNS.get(account_id, [])),
        "trigger_id": 9000 + len(DBT_RUNS.get(account_id, [])),
        "account_id": account_id,
        "job_id": job_id,
        "status_humanized": "Queued",
        "is_complete": False,
        "cause": payload.get("cause") or "Triggered by DataClaw",
        "git_branch": payload.get("git_branch"),
        "schema_override": payload.get("schema_override"),
        "created_at": datetime.now(UTC).isoformat(),
    }
    DBT_RUNS.setdefault(account_id, []).append(run)
    return {"data": run}


def dbt_manifest_fixture() -> dict:
    return {
        "nodes": {
            "model.dataclaw.stg_customers": {
                "unique_id": "model.dataclaw.stg_customers",
                "resource_type": "model",
                "name": "stg_customers",
                "raw_code": "select customer_id, segment, created_at from source('raw', 'customers')",
                "depends_on": {"nodes": ["source.dataclaw.raw.customers"]},
                "columns": {
                    "customer_id": {"name": "customer_id", "data_type": "text"},
                    "segment": {"name": "segment", "data_type": "text"},
                },
            },
            "model.dataclaw.dim_customers": {
                "unique_id": "model.dataclaw.dim_customers",
                "resource_type": "model",
                "name": "dim_customers",
                "description": "Customer dimension with Acme schema drift from customer_id to cust_id.",
                "raw_code": "select customer_id as cust_id, segment, created_at from {{ ref('stg_customers') }}",
                "depends_on": {"nodes": ["model.dataclaw.stg_customers"]},
                "columns": {
                    "cust_id": {"name": "cust_id", "data_type": "text"},
                    "segment": {"name": "segment", "data_type": "text"},
                    "created_at": {"name": "created_at", "data_type": "timestamp"},
                },
            },
            "model.dataclaw.fct_orders": {
                "unique_id": "model.dataclaw.fct_orders",
                "resource_type": "model",
                "name": "fct_orders",
                "description": "Order fact table used for ARR reporting.",
                "raw_code": "select order_id, customer_id as cust_id, arr_usd from source('raw', 'orders')",
                "depends_on": {"nodes": ["source.dataclaw.raw.orders", "model.dataclaw.dim_customers"]},
                "columns": {
                    "order_id": {"name": "order_id", "data_type": "text"},
                    "cust_id": {"name": "cust_id", "data_type": "text"},
                    "arr_usd": {"name": "arr_usd", "data_type": "numeric"},
                },
            },
            "model.dataclaw.fct_revenue_daily": {
                "unique_id": "model.dataclaw.fct_revenue_daily",
                "resource_type": "model",
                "name": "fct_revenue_daily",
                "raw_code": "select date_trunc('day', ordered_at) as ordered_day, sum(arr_usd) as revenue from {{ ref('fct_orders') }} group by 1",
                "depends_on": {"nodes": ["model.dataclaw.fct_orders"]},
                "columns": {
                    "ordered_day": {"name": "ordered_day", "data_type": "date"},
                    "revenue": {"name": "revenue", "data_type": "numeric"},
                },
            },
            "test.dataclaw.fct_orders_not_null_order_id": {
                "unique_id": "test.dataclaw.fct_orders_not_null_order_id",
                "resource_type": "test",
                "name": "not_null_fct_orders_order_id",
                "depends_on": {"nodes": ["model.dataclaw.fct_orders"]},
            },
        },
        "sources": {
            "source.dataclaw.raw.customers": {
                "unique_id": "source.dataclaw.raw.customers",
                "resource_type": "source",
                "name": "customers",
                "source_name": "raw",
            },
            "source.dataclaw.raw.orders": {
                "unique_id": "source.dataclaw.raw.orders",
                "resource_type": "source",
                "name": "orders",
                "source_name": "raw",
            },
        },
        "exposures": {
            "exposure.dataclaw.arr_dashboard": {
                "unique_id": "exposure.dataclaw.arr_dashboard",
                "resource_type": "exposure",
                "name": "arr_dashboard",
                "depends_on": {"nodes": ["model.dataclaw.fct_orders"]},
            }
        },
        "parent_map": {
            "model.dataclaw.dim_customers": ["model.dataclaw.stg_customers"],
            "model.dataclaw.fct_orders": ["source.dataclaw.raw.orders", "model.dataclaw.dim_customers"],
        },
        "child_map": {
            "model.dataclaw.stg_customers": ["model.dataclaw.dim_customers"],
            "model.dataclaw.dim_customers": ["model.dataclaw.fct_orders"],
            "model.dataclaw.fct_orders": ["model.dataclaw.fct_revenue_daily", "exposure.dataclaw.arr_dashboard"],
        },
    }


def dbt_run_results_fixture() -> dict:
    return {
        "metadata": {"generated_at": datetime.now(UTC).isoformat()},
        "results": [
            {
                "unique_id": "model.dataclaw.dim_customers",
                "status": "success",
                "execution_time": 1.2,
            },
            {
                "unique_id": "test.dataclaw.fct_orders_not_null_order_id",
                "status": "fail",
                "failures": 3,
                "message": "fct_orders has 3 null order_id values in the Acme fixture.",
            },
        ],
    }


def dbt_sources_fixture() -> dict:
    return {
        "metadata": {"generated_at": datetime.now(UTC).isoformat()},
        "results": [
            {
                "unique_id": "source.dataclaw.raw.orders",
                "status": "pass",
                "max_loaded_at": datetime.now(UTC).isoformat(),
            }
        ],
    }


def dbt_catalog_fixture() -> dict:
    return {
        "nodes": {
            "model.dataclaw.dim_customers": {
                "unique_id": "model.dataclaw.dim_customers",
                "columns": {
                    "cust_id": {"type": "text", "comment": "Renamed customer identifier."},
                    "segment": {"type": "text"},
                },
            }
        },
        "sources": {},
    }


def dbt_artifact_fixture(path: str) -> dict:
    if path == "manifest.json":
        return dbt_manifest_fixture()
    if path == "run_results.json":
        return dbt_run_results_fixture()
    if path == "sources.json":
        return dbt_sources_fixture()
    if path == "catalog.json":
        return dbt_catalog_fixture()
    return {"path": path, "content": ""}


@app.get("/api/v2/accounts/{account_id}/manifest.json")
async def dbt_manifest(account_id: int) -> dict:
    return dbt_manifest_fixture()


@app.get("/api/v2/accounts/{account_id}/artifacts/manifest.json")
async def dbt_manifest_artifact(account_id: int) -> dict:
    return dbt_manifest_fixture()


@app.get("/api/v2/accounts/{account_id}/artifacts/{artifact_path:path}")
async def dbt_account_artifact(account_id: int, artifact_path: str) -> dict:
    return dbt_artifact_fixture(artifact_path)


@app.get("/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/")
async def dbt_run_artifacts(account_id: int, run_id: int) -> dict:
    return {"data": ["manifest.json", "run_results.json", "sources.json", "catalog.json"]}


@app.get("/api/v2/accounts/{account_id}/runs/{run_id}/artifacts/{artifact_path:path}")
async def dbt_run_artifact(account_id: int, run_id: int, artifact_path: str) -> dict:
    return dbt_artifact_fixture(artifact_path)


@app.get("/v1/users/me")
async def notion_me() -> dict:
    return {"id": "fake-user", "name": "DataClaw Integration"}


@app.post("/v1/search")
async def notion_search(request: Request) -> dict:
    extra = [
        {
            "id": page["id"],
            "object": "page",
            "properties": {"title": {"title": [{"plain_text": page["title"]}]}},
        }
        for page in NOTION_PAGES
    ]
    return {
        "results": extra
        + [
            {
                "id": page_id,
                "object": "page",
                "parent": {"page_id": "integration-root"},
                "last_edited_time": "2026-05-08T00:00:00Z",
                "properties": {"title": {"title": [{"plain_text": page["title"]}]}},
            }
            for page_id, page in NOTION_FIXTURES.items()
        ]
    }


@app.post("/v1/pages")
async def notion_create_page(request: Request) -> dict:
    payload = await request.json()
    title = "Untitled"
    title_nodes = (
        payload.get("properties", {})
        .get("title", {})
        .get("title", [])
    )
    if title_nodes:
        title = title_nodes[0].get("text", {}).get("content") or title_nodes[0].get("plain_text") or title
    page = {
        "id": f"page-created-{len(NOTION_PAGES) + 1}",
        "object": "page",
        "title": title,
        "payload": payload,
    }
    NOTION_PAGES.append(page)
    return page


@app.get("/v1/pages/created")
async def notion_created_pages() -> dict:
    return {"results": NOTION_PAGES}


@app.get("/v1/pages/{page_id}")
async def notion_get_page(page_id: str) -> dict:
    for page in NOTION_PAGES:
        if page["id"] == page_id:
            return page | {"appended": NOTION_APPENDS.get(page_id, [])}
    fixture = NOTION_FIXTURES.get(page_id, {"title": "Data Glossary", "body": "LTV joins customers to orders."})
    return {"id": page_id, "object": "page", **fixture, "appended": NOTION_APPENDS.get(page_id, [])}


@app.get("/v1/blocks/{block_id}/children")
async def notion_block_children(block_id: str) -> dict:
    fixture = NOTION_FIXTURES.get(block_id)
    if not fixture:
        return {"object": "list", "results": [], "has_more": False}
    return {
        "object": "list",
        "has_more": False,
        "results": [
            {
                "id": f"{block_id}-body",
                "type": "paragraph",
                "paragraph": {"rich_text": [{"plain_text": fixture["body"]}]},
            }
        ],
    }


@app.patch("/v1/blocks/{block_id}/children")
async def notion_append_children(block_id: str, request: Request) -> dict:
    payload = await request.json()
    children = payload.get("children") or []
    NOTION_APPENDS.setdefault(block_id, []).extend(children)
    return {"object": "list", "results": children, "block_id": block_id}


@app.get("/repos/{owner}/{repo}")
async def github_repo(owner: str, repo: str) -> dict:
    return {
        "full_name": f"{owner}/{repo}",
        "default_branch": "main",
        "description": "DataClaw integration fixture repo.",
        "language": "Python",
    }


@app.get("/user")
async def github_user() -> dict:
    return {"login": "dataclaw-bot", "name": "DataClaw Bot"}


def _github_file(path: str, content: str) -> dict:
    return {
        "path": path,
        "type": "file",
        "encoding": "base64",
        "content": base64.b64encode(content.encode()).decode(),
    }


@app.get("/repos/{owner}/{repo}/contents")
async def github_get_root_content(owner: str, repo: str) -> list[dict]:
    return await github_get_content(owner, repo, "")


@app.put("/repos/{owner}/{repo}/contents/{path:path}")
async def github_put_content(owner: str, repo: str, path: str, request: Request) -> dict:
    payload = await request.json()
    key = f"{owner}/{repo}/{path}"
    GITHUB_FILES[key] = {"owner": owner, "repo": repo, "path": path, "payload": payload}
    return {
        "content": {"name": path.split("/")[-1], "path": path, "sha": f"sha-{len(GITHUB_FILES)}"},
        "commit": {"message": payload.get("message", "")},
    }


@app.get("/repos/{owner}/{repo}/contents/{path:path}")
async def github_get_content(owner: str, repo: str, path: str) -> Any:
    if path in {"", "/"}:
        return [
            _github_file("README.md", "# Warehouse\n\nThis repo documents [[orders]], [[customers]], and revenue models."),
            _github_file("dbt_project.yml", "name: dataclaw_fixture\nmodels:\n  dataclaw_fixture:\n    +materialized: table\n"),
            {"path": "models", "type": "dir"},
        ]
    if path == "models":
        return [
            _github_file("models/orders.sql", "select order_id, customer_id, net_revenue from orders"),
        ]
    return GITHUB_FILES.get(f"{owner}/{repo}/{path}", {"path": path, "missing": True})


@app.get("/search/code")
async def github_search_code(q: str = "", per_page: int = 30) -> dict:
    files = [
        _github_file("models/orders.sql", "select order_id, customer_id, net_revenue from orders"),
        _github_file("models/marts/fct_orders.sql", "select order_id, customer_id, net_revenue from {{ ref('stg_orders') }}"),
        _github_file(".github/workflows/ci.yml", "name: ci\non: [push]\njobs: {lint: {runs-on: ubuntu-latest, steps: []}}\n"),
    ]
    needle = q.lower()
    items = [item for item in files if needle in item["path"].lower() or needle in base64.b64decode(item["content"]).decode().lower()]
    return {"total_count": len(items), "items": items[:per_page]}


@app.get("/repos/{owner}/{repo}/actions/workflows")
async def github_workflows(owner: str, repo: str, per_page: int = 30) -> dict:
    return {
        "total_count": 1,
        "workflows": [
            {
                "id": 1,
                "name": "ci",
                "path": ".github/workflows/ci.yml",
                "state": "active",
            }
        ][:per_page],
    }


@app.post("/repos/{owner}/{repo}/pulls")
async def github_create_pull(owner: str, repo: str, request: Request) -> dict:
    payload = await request.json()
    pull = {
        "id": len(GITHUB_PULLS) + 1,
        "number": len(GITHUB_PULLS) + 1,
        "state": "open",
        "title": payload.get("title"),
        "head": {"ref": payload.get("head")},
        "base": {"ref": payload.get("base")},
        "body": payload.get("body"),
        "html_url": f"https://example.test/{owner}/{repo}/pull/{len(GITHUB_PULLS) + 1}",
    }
    GITHUB_PULLS.append(pull)
    return pull


@app.get("/v1/threads/recent")
async def quip_threads() -> dict:
    return {
        "threads": [
            {"id": "thread-revenue-glossary", "title": "Revenue glossary"},
            {"id": "thread-pipeline-runbook", "title": "Pipeline runbook"},
        ]
    }


@app.get("/1/users/current")
async def quip_user() -> dict:
    return {"id": "fake-user", "name": "DataClaw"}


@app.get("/1/threads/{thread_id}")
async def quip_thread(thread_id: str) -> dict:
    return {"thread": {"id": thread_id, "title": "Revenue glossary", "html": "LTV maps to customer revenue."}}


@app.get("/wiki/rest/api/user/current")
async def confluence_user() -> dict:
    return {"accountId": "fake-user", "displayName": "DataClaw Integration"}


@app.get("/wiki/rest/api/content")
async def confluence_content() -> dict:
    return {
        "results": [
            {"id": "conf-revenue-glossary", "title": "Revenue glossary", "type": "page"},
            {"id": "conf-pipeline-runbook", "title": "Pipeline runbook", "type": "page"},
            *CONFLUENCE_PAGES,
        ]
    }


@app.get("/wiki/rest/api/search")
async def confluence_search() -> dict:
    return {
        "results": [
            {"id": "conf-revenue-glossary", "title": "Revenue glossary", "type": "page"},
            {"id": "conf-pipeline-runbook", "title": "Pipeline runbook", "type": "page"},
            *CONFLUENCE_PAGES,
        ]
    }


@app.post("/wiki/rest/api/content")
async def confluence_create_content(request: Request) -> dict:
    payload = await request.json()
    page = {
        "id": f"conf-created-{len(CONFLUENCE_PAGES) + 1}",
        "title": payload.get("title") or "DataClaw page",
        "type": payload.get("type") or "page",
        "space": payload.get("space") or {"key": "ENG"},
        "body": payload.get("body") or {"storage": {"value": "", "representation": "storage"}},
        "createdAt": datetime.now(UTC).isoformat(),
    }
    CONFLUENCE_PAGES.append(page)
    return page


@app.get("/wiki/rest/api/content-created")
async def confluence_created_content() -> dict:
    return {"results": CONFLUENCE_PAGES}


@app.get("/wiki/rest/api/content/{page_id}")
async def confluence_page(page_id: str) -> dict:
    return {
        "id": page_id,
        "title": "Revenue glossary",
        "version": {"number": 1},
        "body": {"storage": {"value": "LTV maps to customer revenue."}},
    }


@app.put("/wiki/rest/api/content/{page_id}")
async def confluence_update_page(page_id: str, request: Request) -> dict:
    payload = await request.json()
    page = {
        "id": page_id,
        "title": payload.get("title") or "Revenue glossary",
        "version": payload.get("version") or {"number": 2},
        "body": payload.get("body") or {"storage": {"value": "", "representation": "storage"}},
    }
    CONFLUENCE_PAGES.append(page)
    return page


@app.get("/v1/health")
async def airbyte_health() -> dict:
    return {"status": "ok"}


@app.get("/v1/connections")
async def airbyte_connections() -> dict:
    return {"objects": list(AIRBYTE_CONNECTIONS.values())}


@app.post("/api/v1/connections/list")
async def airbyte_connections_list() -> dict:
    return {"connections": list(AIRBYTE_CONNECTIONS.values())}


@app.post("/api/v1/connections/get")
async def airbyte_connection_get(request: Request) -> dict:
    payload = await request.json()
    connection_id = payload.get("connectionId") or payload.get("connection_id")
    return AIRBYTE_CONNECTIONS.get(connection_id, {"connectionId": connection_id, "status": "not_found"})


@app.get("/v1/connections/{connection_id}")
async def airbyte_cloud_connection_get(connection_id: str) -> dict:
    return AIRBYTE_CONNECTIONS.get(connection_id, {"connectionId": connection_id, "status": "not_found"})


@app.post("/api/v1/state/get")
async def airbyte_state_get(request: Request) -> dict:
    payload = await request.json()
    connection_id = payload.get("connectionId") or payload.get("connection_id")
    return {"connectionId": connection_id, "state": {"type": "STREAM", "stream": {"streamState": {"cursor": "2026-05-20T00:00:00Z"}}}}


@app.post("/api/v1/sources/list")
async def airbyte_sources_list() -> dict:
    return {"sources": list(AIRBYTE_SOURCES.values())}


@app.get("/v1/sources")
async def airbyte_cloud_sources() -> dict:
    return {"objects": list(AIRBYTE_SOURCES.values())}


@app.post("/api/v1/sources/get")
async def airbyte_source_get(request: Request) -> dict:
    payload = await request.json()
    source_id = payload.get("sourceId") or payload.get("source_id")
    return AIRBYTE_SOURCES.get(source_id, {"sourceId": source_id, "status": "not_found"})


@app.get("/v1/sources/{source_id}")
async def airbyte_cloud_source_get(source_id: str) -> dict:
    return AIRBYTE_SOURCES.get(source_id, {"sourceId": source_id, "status": "not_found"})


@app.post("/api/v1/destinations/list")
async def airbyte_destinations_list() -> dict:
    return {"destinations": list(AIRBYTE_DESTINATIONS.values())}


@app.get("/v1/destinations")
async def airbyte_cloud_destinations() -> dict:
    return {"objects": list(AIRBYTE_DESTINATIONS.values())}


@app.post("/api/v1/destinations/get")
async def airbyte_destination_get(request: Request) -> dict:
    payload = await request.json()
    destination_id = payload.get("destinationId") or payload.get("destination_id")
    return AIRBYTE_DESTINATIONS.get(destination_id, {"destinationId": destination_id, "status": "not_found"})


@app.get("/v1/destinations/{destination_id}")
async def airbyte_cloud_destination_get(destination_id: str) -> dict:
    return AIRBYTE_DESTINATIONS.get(destination_id, {"destinationId": destination_id, "status": "not_found"})


@app.get("/v1/jobs")
async def airbyte_jobs() -> dict:
    return {"objects": AIRBYTE_JOBS}


@app.post("/api/v1/jobs/list")
async def airbyte_jobs_list() -> dict:
    return {"jobs": AIRBYTE_JOBS}


@app.post("/api/v1/jobs/get")
async def airbyte_job_get(request: Request) -> dict:
    payload = await request.json()
    job_id = payload.get("id") or payload.get("jobId") or payload.get("job_id")
    job = next((item for item in AIRBYTE_JOBS if str(item.get("id")) == str(job_id)), None)
    return {"job": job or {"id": job_id, "status": "not_found"}, "logs": (job or {}).get("logs", [])}


@app.get("/v1/jobs/{job_id}")
async def airbyte_cloud_job_get(job_id: str) -> dict:
    job = next((item for item in AIRBYTE_JOBS if str(item.get("id")) == str(job_id)), None)
    return {"job": job or {"id": job_id, "status": "not_found"}, "logs": (job or {}).get("logs", [])}


@app.post("/api/v1/workspaces/get")
async def airbyte_workspace_get(request: Request) -> dict:
    payload = await request.json()
    return {**AIRBYTE_WORKSPACE, "requestedWorkspaceId": payload.get("workspaceId")}


@app.post("/api/v1/workspaces/list")
async def airbyte_workspaces_list() -> dict:
    return {"workspaces": [AIRBYTE_WORKSPACE]}


@app.post("/v1/jobs")
async def airbyte_trigger_job(request: Request) -> dict:
    payload = await request.json()
    job = {
        "id": len(AIRBYTE_JOBS) + 1,
        "connectionId": payload.get("connectionId") or payload.get("connection_id"),
        "jobType": payload.get("jobType") or "sync",
        "status": "running",
        "createdAt": datetime.now(UTC).isoformat(),
        "logs": ["Acme Airbyte job triggered"],
    }
    AIRBYTE_JOBS.append(job)
    return {"job": job}


@app.post("/api/v1/connections/create")
async def airbyte_create_connection(request: Request) -> dict:
    payload = await request.json()
    connection_id = payload.get("connectionId") or f"connection-{len(AIRBYTE_CONNECTIONS) + 1}"
    connection = {
        "connectionId": connection_id,
        "name": payload.get("name") or "Acme coverage connection",
        "sourceId": payload.get("sourceId"),
        "destinationId": payload.get("destinationId"),
        "status": payload.get("status") or "active",
    }
    AIRBYTE_CONNECTIONS[connection_id] = connection
    return connection


@app.post("/api/v1/connections/update")
async def airbyte_connection_update(request: Request) -> dict:
    payload = await request.json()
    connection_id = payload.get("connectionId") or payload.get("connection_id")
    connection = AIRBYTE_CONNECTIONS.setdefault(connection_id, {"connectionId": connection_id, "name": connection_id})
    connection.update(payload)
    return connection


@app.post("/api/v1/connections/reset")
async def airbyte_connection_reset(request: Request) -> dict:
    payload = await request.json()
    job = {
        "id": len(AIRBYTE_JOBS) + 1,
        "connectionId": payload.get("connectionId") or payload.get("connection_id"),
        "jobType": "reset",
        "status": "running",
        "createdAt": datetime.now(UTC).isoformat(),
        "logs": ["Acme Airbyte reset triggered"],
    }
    AIRBYTE_JOBS.append(job)
    return {"job": job}


@app.post("/api/v1/jobs/cancel")
async def airbyte_job_cancel(request: Request) -> dict:
    payload = await request.json()
    job_id = payload.get("id") or payload.get("jobId") or payload.get("job_id")
    job = next((item for item in AIRBYTE_JOBS if str(item.get("id")) == str(job_id)), None)
    if job is None:
        job = {"id": job_id, "status": "cancelled"}
    else:
        job["status"] = "cancelled"
    return {"job": job}


@app.delete("/v1/jobs/{job_id}")
async def airbyte_cloud_job_cancel(job_id: str) -> dict:
    job = next((item for item in AIRBYTE_JOBS if str(item.get("id")) == str(job_id)), {"id": job_id})
    job["status"] = "cancelled"
    return {"job": job}


@app.patch("/v1/connections/{connection_id}")
async def airbyte_update_connection(connection_id: str, request: Request) -> dict:
    payload = await request.json()
    connection = AIRBYTE_CONNECTIONS.setdefault(
        connection_id,
        {"connectionId": connection_id, "name": connection_id, "status": "active"},
    )
    connection.update(payload)
    return connection


@app.get("/api/health")
async def prefect_health() -> dict:
    return {"status": "healthy"}


@app.get("/api/flows")
async def prefect_flows() -> dict:
    return {"objects": PREFECT_FLOWS}


@app.post("/api/flows/filter")
async def prefect_flows_filter() -> dict:
    return {"data": PREFECT_FLOWS}


@app.get("/api/flows/filter")
async def prefect_flows_filter_get() -> dict:
    return {"data": PREFECT_FLOWS}


@app.post("/api/deployments/filter")
async def prefect_deployments_filter() -> dict:
    return {"data": PREFECT_DEPLOYMENTS}


@app.get("/api/deployments/{deployment_id}")
async def prefect_get_deployment(deployment_id: str) -> dict:
    for deployment in PREFECT_DEPLOYMENTS:
        if deployment["id"] == deployment_id:
            return deployment
    return {"id": deployment_id, "name": deployment_id, "flow_id": PREFECT_LEGACY_FLOW["id"], "entrypoint": None}


@app.post("/api/flow_runs/filter")
async def prefect_flow_runs_filter() -> dict:
    return {"data": PREFECT_RUNS}


@app.post("/api/logs/filter")
async def prefect_logs_filter(request: Request) -> dict:
    await request.json()
    return {
        "data": [
            {
                "id": "log-acme-revenue-recalc-1",
                "level": "INFO",
                "message": "acme_revenue_recalc refreshed Snowflake ACME.MARTS.REVENUE_DAILY from churn_events",
                "timestamp": datetime.now(UTC).isoformat(),
            },
            {
                "id": "log-failure-test-1",
                "level": "ERROR",
                "message": "failure_test simulated retry exhausted",
                "timestamp": datetime.now(UTC).isoformat(),
            }
        ]
    }


@app.get("/api/flow_runs/{run_id}")
async def prefect_flow_run(run_id: str) -> dict:
    for run in PREFECT_RUNS:
        if run["id"] == run_id:
            return run
    return {"id": run_id, "state": {"type": "NOT_FOUND"}}


@app.post("/api/deployments/{deployment_id}/create_flow_run")
async def prefect_create_flow_run(deployment_id: str, request: Request) -> dict:
    payload = await request.json()
    run = {
        "id": f"prefect-run-{len(PREFECT_RUNS) + 1}",
        "deployment_id": deployment_id,
        "name": payload.get("name") or f"{deployment_id}-run",
        "flow_id": next(
            (
                deployment["flow_id"]
                for deployment in PREFECT_DEPLOYMENTS
                if deployment["id"] == deployment_id
            ),
            PREFECT_LEGACY_FLOW["id"],
        ),
        "state": {"type": "SCHEDULED", "name": "Scheduled"},
        "parameters": payload.get("parameters") or {},
        "created": datetime.now(UTC).isoformat(),
    }
    PREFECT_RUNS.append(run)
    return run


@app.post("/api/deployments")
async def prefect_create_deployment(request: Request) -> dict:
    payload = await request.json()
    deployment = {
        "id": f"deployment-{len(PREFECT_DEPLOYMENTS) + 1}",
        "name": payload.get("name") or "DataClaw deployment",
        "flow_id": payload.get("flow_id") or PREFECT_LEGACY_FLOW["id"],
        "entrypoint": payload.get("entrypoint"),
        "created": datetime.now(UTC).isoformat(),
    }
    PREFECT_DEPLOYMENTS.append(deployment)
    return deployment


@app.post("/graphql")
async def dagster_graphql(request: Request) -> dict:
    payload = await request.json()
    query = payload.get("query", "")
    variables = payload.get("variables") or {}
    if "launchPipelineExecution" in query or "launchRun" in query or "materialize" in query:
        run = {
            "runId": f"dagster-run-{len(DAGSTER_RUNS) + 1}",
            "status": "STARTED",
            "pipelineName": (
                variables.get("executionParams", {})
                .get("selector", {})
                .get("pipelineName", DAGSTER_JOB_NAME)
            ),
            "stepKeysToExecute": ["customers", "orders"],
            "selector": variables.get("selector") or variables.get("assetKey") or variables.get("executionParams", {}).get("selector") or {},
            "createdAt": datetime.now(UTC).isoformat(),
        }
        DAGSTER_RUNS.append(run)
        return {"data": {"launchPipelineExecution": {"__typename": "LaunchRunSuccess", "run": run}}}
    if "launchPartitionBackfill" in query:
        return {"data": {"launchPartitionBackfill": {"__typename": "LaunchBackfillSuccess", "backfillId": "dagster-backfill-acme", "launchedRunIds": [DAGSTER_RUNS[0]["runId"]]}}}
    if "terminateRun" in query:
        run_id = variables.get("runId") or DAGSTER_RUNS[0]["runId"]
        run = next((item for item in DAGSTER_RUNS if item.get("runId") == run_id), {"runId": run_id})
        run["status"] = "CANCELED"
        return {"data": {"terminateRun": {"__typename": "TerminateRunSuccess", "run": run}}}
    if "startSensor" in query:
        return {"data": {"startSensor": {"__typename": "Sensor", "name": variables.get("selector", {}).get("sensorName", DAGSTER_SENSOR_NAME)}}}
    if "startSchedule" in query:
        return {"data": {"startSchedule": {"__typename": "Schedule", "name": variables.get("selector", {}).get("scheduleName", DAGSTER_SCHEDULE_NAME)}}}
    if "stopRunningSchedule" in query:
        return {"data": {"stopRunningSchedule": {"__typename": "Schedule", "name": variables.get("selector", {}).get("scheduleName", DAGSTER_SCHEDULE_NAME)}}}
    if "runOrError" in query or "logsForRun" in query:
        run_id = variables.get("runId") or DAGSTER_RUNS[0]["runId"]
        run = next((item for item in DAGSTER_RUNS if item.get("runId") == run_id), DAGSTER_RUNS[0])
        return {"data": {"runOrError": {"__typename": "Run", **run}, "logsForRun": {"__typename": "EventConnection", "events": DAGSTER_EVENTS}}}
    if "assetNodeOrError" in query:
        asset_key = variables.get("assetKey") or ["customers"]
        asset = next((item for item in DAGSTER_ASSETS if item["key"]["path"] == asset_key), DAGSTER_ASSETS[0])
        return {"data": {"assetNodeOrError": {"__typename": "AssetNode", **asset}}}
    if "repositoriesOrError" in query:
        return {"data": {"repositoriesOrError": {"__typename": "RepositoryConnection", "nodes": DAGSTER_REPOSITORIES}}}
    if "instigationStateOrError" in query:
        selector = variables.get("selector") or {}
        name = selector.get("name") or selector.get("sensorName") or selector.get("scheduleName") or DAGSTER_SENSOR_NAME
        state_type = "SCHEDULE" if name == DAGSTER_SCHEDULE_NAME else "SENSOR"
        return {"data": {"instigationStateOrError": {"__typename": "InstigationState", "id": f"dagster-{name}", "name": name, "status": "RUNNING", "type": state_type}}}
    return {
        "data": {
            "assetsOrError": {
                "__typename": "AssetConnection",
                "nodes": DAGSTER_ASSETS,
            }
        }
    }


@app.get("/server_info")
async def dagster_server_info() -> dict:
    return {"dagster_version": "fixture", "status": "ok"}


@app.get("/api/2.0/clusters/list")
async def databricks_clusters() -> dict:
    return {"clusters": [DATABRICKS_CLUSTER]}


@app.get("/api/2.0/jobs/list")
async def databricks_jobs() -> dict:
    return {"jobs": [DATABRICKS_JOB]}


@app.get("/api/2.0/sql/warehouses")
async def databricks_warehouses() -> dict:
    return {"warehouses": [DATABRICKS_WAREHOUSE]}


@app.get("/api/2.0/workspace/export")
async def databricks_export_notebook() -> dict:
    return DATABRICKS_NOTEBOOK


@app.get("/api/2.0/jobs/runs/get-output")
async def databricks_run_output(run_id: int) -> dict:
    run = next((item for item in DATABRICKS_JOB_RUNS if item["run_id"] == run_id), DATABRICKS_JOB_RUNS[0])
    return {"metadata": run, "logs": run["logs"], "notebook_output": {"result": run["logs"]}}


@app.post("/api/2.0/jobs/run-now")
async def databricks_run_job(request: Request) -> dict:
    payload = await request.json()
    run = {
        "run_id": 5000 + len(DATABRICKS_JOB_RUNS),
        "job_id": payload.get("job_id"),
        "state": {"life_cycle_state": "PENDING"},
        "created_at": datetime.now(UTC).isoformat(),
        "logs": "Queued Acme Databricks job run.",
    }
    DATABRICKS_JOB_RUNS.append(run)
    return run


@app.post("/api/2.0/jobs/runs/submit")
async def databricks_submit_run(request: Request) -> dict:
    payload = await request.json()
    run = {
        "run_id": 5000 + len(DATABRICKS_JOB_RUNS),
        "job_id": None,
        "run_name": payload.get("run_name"),
        "state": {"life_cycle_state": "PENDING"},
        "created_at": datetime.now(UTC).isoformat(),
        "logs": f"Submitted notebook {payload.get('notebook_task', {}).get('notebook_path')}",
    }
    DATABRICKS_JOB_RUNS.append(run)
    return run


@app.post("/api/2.0/clusters/start")
async def databricks_start_cluster(request: Request) -> dict:
    payload = await request.json()
    return {"cluster_id": payload.get("cluster_id"), "state": "RUNNING"}


@app.post("/api/2.0/clusters/delete")
async def databricks_stop_cluster(request: Request) -> dict:
    payload = await request.json()
    return {"cluster_id": payload.get("cluster_id"), "state": "TERMINATING"}


@app.get("/api/2.0/unity-catalog/tables/{full_name:path}")
async def databricks_unity_table(full_name: str) -> dict:
    return {
        "full_name": full_name,
        "name": full_name.split(".")[-1],
        "catalog_name": full_name.split(".")[0] if "." in full_name else "acme",
        "schema_name": full_name.split(".")[1] if full_name.count(".") >= 2 else "silver",
        "table_type": "MANAGED",
    }


@app.patch("/api/2.1/unity-catalog/permissions/{securable_type}/{full_name:path}")
async def databricks_update_permissions(securable_type: str, full_name: str, request: Request) -> dict:
    payload = await request.json()
    return {"securable_type": securable_type, "full_name": full_name, "privilege_assignments": payload.get("changes", [])}


@app.post("/api/2.0/sql/statements")
async def databricks_statement(request: Request) -> dict:
    payload = await request.json()
    statement_id = f"stmt-{len(DATABRICKS_STATEMENTS) + 1}"
    sql = payload.get("statement", "")
    rows: list[list] = []
    columns = [{"name": "result", "type_text": "STRING"}]
    lower = sql.lower()
    if "information_schema.tables" in lower:
        columns = [{"name": "table_schema", "type_text": "STRING"}, {"name": "table_name", "type_text": "STRING"}]
        rows = [["silver", "events"]]
    elif "information_schema.columns" in lower:
        columns = [{"name": "column_name", "type_text": "STRING"}, {"name": "data_type", "type_text": "STRING"}, {"name": "is_nullable", "type_text": "STRING"}]
        rows = [["event_id", "STRING", "NO"], ["customer_id", "INT", "NO"], ["event_name", "STRING", "NO"], ["event_at", "TIMESTAMP", "YES"]]
    elif "count(*)" in lower:
        columns = [{"name": "row_count", "type_text": "LONG"}]
        rows = [[1000]]
    elif "system.access.table_lineage" in lower:
        columns = [
            {"name": "source_table_full_name", "type_text": "STRING"},
            {"name": "target_table_full_name", "type_text": "STRING"},
            {"name": "entity_type", "type_text": "STRING"},
            {"name": "entity_id", "type_text": "STRING"},
            {"name": "event_time", "type_text": "TIMESTAMP"},
        ]
        rows = [["raw.events", "acme.silver.events", "JOB", "acme_events_refresh", datetime.now(UTC).isoformat()]]
    elif "system.query.history" in lower:
        columns = [
            {"name": "statement_id", "type_text": "STRING"},
            {"name": "executed_by", "type_text": "STRING"},
            {"name": "start_time", "type_text": "TIMESTAMP"},
            {"name": "end_time", "type_text": "TIMESTAMP"},
            {"name": "status", "type_text": "STRING"},
            {"name": "statement_text", "type_text": "STRING"},
        ]
        rows = [["stmt-acme-events", "dataclaw", datetime.now(UTC).isoformat(), datetime.now(UTC).isoformat(), "SUCCEEDED", "select count(*) from acme.silver.events"]]
    elif "max(" in lower:
        columns = [{"name": "max_value", "type_text": "TIMESTAMP"}]
        rows = [[datetime.now(UTC).isoformat()]]
    elif lower.strip().startswith("select"):
        columns = [{"name": "event_id", "type_text": "STRING"}, {"name": "customer_id", "type_text": "INT"}, {"name": "event_name", "type_text": "STRING"}]
        rows = [["evt-1", 1, "trial_started"], ["evt-2", 14, "order_paid"], ["evt-3", 27, "downgrade"]]
    result = {
        "statement_id": statement_id,
        "status": {"state": "SUCCEEDED"},
        "manifest": {"schema": {"columns": columns}},
        "result": {"data_array": rows},
        "statement": sql,
    }
    DATABRICKS_STATEMENTS[statement_id] = result
    return result


@app.get("/api/2.0/sql/statements/{statement_id}")
async def databricks_statement_status(statement_id: str) -> dict:
    return DATABRICKS_STATEMENTS.get(
        statement_id,
        {"statement_id": statement_id, "status": {"state": "NOT_FOUND"}, "manifest": {"schema": {"columns": []}}, "result": {"data_array": []}},
    )
