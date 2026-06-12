from __future__ import annotations

import json
import os
import subprocess
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

import pytest

from app.services.connectors.adapters import adapter_for
from app.services.connectors.catalog import CATALOG_BY_SLUG

ROOT = Path(__file__).resolve().parents[3]
COMPOSE_FILE = ROOT / "tests" / "integration" / "docker-compose.yml"
RESULTS_FILE = ROOT / "tests" / "integration" / "results" / "connector-results.json"


def _load_env_file(path: Path) -> None:
    explicit_run_flags = {"RUN_CONNECTOR_INTEGRATION", "RUN_SAAS_CONNECTOR_INTEGRATION"}
    if not path.exists():
        return
    for raw_line in path.read_text().splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        if key.strip() in explicit_run_flags:
            continue
        os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def _docker_cmd(*args: str) -> list[str]:
    docker = ["docker", *args]
    try:
        subprocess.run(["docker", "info"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        return docker
    except Exception:
        return ["sudo", *docker]


def _run(cmd: list[str], timeout: int = 240) -> None:
    subprocess.run(cmd, cwd=ROOT, check=True, timeout=timeout)


def _compose_down() -> None:
    _run(_docker_cmd("compose", "-f", str(COMPOSE_FILE), "down", "--remove-orphans"), timeout=180)


@pytest.fixture(scope="session")
def integration_env() -> None:
    _load_env_file(ROOT / ".env.integration")


@pytest.fixture(scope="session")
def integration_results_file(integration_env: None) -> Path:
    if os.getenv("RUN_CONNECTOR_INTEGRATION") != "1":
        pytest.skip("Set RUN_CONNECTOR_INTEGRATION=1 to run local Docker connector tests.")
    RESULTS_FILE.parent.mkdir(parents=True, exist_ok=True)
    RESULTS_FILE.unlink(missing_ok=True)
    return RESULTS_FILE


def _seed_prefect_flow() -> None:
    base_url = "http://127.0.0.1:18082"
    flow_id: str | None = None
    deadline = time.time() + 30
    last_error: Exception | None = None
    while time.time() < deadline:
        try:
            request = urllib.request.Request(
                f"{base_url}/api/flows/",
                data=json.dumps({"name": "orders-flow", "tags": ["dataclaw"]}).encode(),
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=5) as response:
                flow_id = json.loads(response.read().decode())["id"]
            break
        except urllib.error.HTTPError as exc:
            last_error = exc
            if exc.code not in {409, 422}:
                time.sleep(1)
                continue
        except Exception as exc:  # pragma: no cover - Docker startup timing varies
            last_error = exc
            time.sleep(1)
    if flow_id is None:
        raise RuntimeError(f"Could not seed Prefect flow: {last_error}")

    request = urllib.request.Request(
        f"{base_url}/api/deployments/",
        data=json.dumps(
            {
                "name": "deployment-orders",
                "flow_id": flow_id,
                "entrypoint": "flows.py:orders",
                "version": "integration",
                "tags": ["dataclaw"],
            }
        ).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        urllib.request.urlopen(request, timeout=5).read()
    except urllib.error.HTTPError as exc:
        if exc.code not in {409, 422}:
            raise


def _connector_slug_from_request(request: pytest.FixtureRequest) -> str:
    callspec = getattr(request.node, "callspec", None)
    slug = callspec.params.get("slug") if callspec else None
    if not isinstance(slug, str):
        raise RuntimeError("local_connector_stack requires a parametrized slug.")
    return slug


@pytest.fixture()
def local_connector_stack(integration_results_file: Path, request: pytest.FixtureRequest) -> None:
    if os.getenv("RUN_CONNECTOR_INTEGRATION") != "1":
        pytest.skip("Set RUN_CONNECTOR_INTEGRATION=1 to run local Docker connector tests.")
    slug = _connector_slug_from_request(request)
    service = LOCAL_CONNECTOR_SERVICES[slug]
    if service == "dagster":
        _run(_docker_cmd("compose", "-f", str(COMPOSE_FILE), "build", service), timeout=420)
    _run(_docker_cmd("compose", "-f", str(COMPOSE_FILE), "up", "-d", "--wait", service), timeout=420)
    if slug == "prefect":
        _seed_prefect_flow()
    try:
        yield
    finally:
        _compose_down()


LOCAL_CONNECTORS: dict[str, dict[str, Any]] = {
    "postgres": {
        "database_url": "postgresql+psycopg://dataclaw:dataclaw@127.0.0.1:15432/dataclaw_integration"
    },
    "mysql": {
        "host": "127.0.0.1",
        "port": "13306",
        "database": "dataclaw_integration",
        "user": "dataclaw",
        "password": "dataclaw",
    },
    "airflow": {"base_url": "http://127.0.0.1:18080", "username": "admin", "password": "admin"},
    "airbyte": {"api_url": "http://127.0.0.1:18081", "api_key": "integration-token"},
    "prefect": {"api_url": "http://127.0.0.1:18082", "api_key": "integration-token"},
    "dagster": {"graphql_url": "http://127.0.0.1:18083/graphql", "token": "integration-token"},
}

LOCAL_CONNECTOR_SERVICES = {
    "postgres": "postgres",
    "mysql": "mysql",
    "airflow": "airflow",
    "airbyte": "airbyte",
    "prefect": "prefect",
    "dagster": "dagster",
}

LOCAL_CONNECTOR_MIN_OBJECTS = {
    "airbyte": 0,
}


def test_local_connector_integration_uses_one_compose_service_per_case() -> None:
    assert set(LOCAL_CONNECTOR_SERVICES) == set(LOCAL_CONNECTORS)
    assert all(isinstance(service, str) and service for service in LOCAL_CONNECTOR_SERVICES.values())


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("slug,credentials", LOCAL_CONNECTORS.items())
async def test_local_docker_connector_test_and_sync(
    local_connector_stack: None, slug: str, credentials: dict[str, Any]
) -> None:
    adapter = adapter_for(slug)
    test_result = await adapter.test(credentials)
    sync_result = await adapter.sync(credentials)

    assert test_result.status == "ok", test_result.message
    assert test_result.mode == "real"
    assert sync_result["mode"] == "real"
    assert sync_result["objects_synced"] >= LOCAL_CONNECTOR_MIN_OBJECTS.get(slug, 1)

    existing = json.loads(RESULTS_FILE.read_text()) if RESULTS_FILE.exists() else {}
    existing[slug] = {
        "test": test_result.model_dump(mode="json"),
        "sync": sync_result,
    }
    RESULTS_FILE.write_text(json.dumps(existing, indent=2, sort_keys=True))


SAAS_CONNECTORS: dict[str, dict[str, str]] = {
    "notion": {
        "integration_token": "NOTION_INTEGRATION_TOKEN",
    },
    "google_docs": {"service_account_json": "GOOGLE_DOCS_SERVICE_ACCOUNT_JSON"},
    "quip": {"access_token": "QUIP_ACCESS_TOKEN"},
    "github": {"token": "GITHUB_TOKEN", "repositories": "GITHUB_REPOSITORIES"},
    "confluence": {
        "site_url": "CONFLUENCE_SITE_URL",
        "email": "CONFLUENCE_EMAIL",
        "api_token": "CONFLUENCE_API_TOKEN",
    },
    "snowflake": {
        "account": "SNOWFLAKE_ACCOUNT",
        "warehouse": "SNOWFLAKE_WAREHOUSE",
        "database": "SNOWFLAKE_DATABASE",
        "schema": "SNOWFLAKE_SCHEMA",
        "user": "SNOWFLAKE_USER",
        "password": "SNOWFLAKE_PASSWORD",
    },
    "redshift": {
        "cluster_endpoint": "REDSHIFT_CLUSTER_ENDPOINT",
        "database": "REDSHIFT_DATABASE",
        "user": "REDSHIFT_USER",
        "password": "REDSHIFT_PASSWORD",
    },
    "databricks": {
        "workspace_url": "DATABRICKS_WORKSPACE_URL",
        "http_path": "DATABRICKS_HTTP_PATH",
        "token": "DATABRICKS_TOKEN",
    },
    "bigquery": {
        "service_account_json": "BIGQUERY_SERVICE_ACCOUNT_JSON",
        "project_id": "BIGQUERY_PROJECT_ID",
    },
    "dbt": {"api_token": "DBT_API_TOKEN", "account_id": "DBT_ACCOUNT_ID"},
    "fivetran": {"api_key": "FIVETRAN_API_KEY", "api_secret": "FIVETRAN_API_SECRET"},
}


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.parametrize("slug,env_map", SAAS_CONNECTORS.items())
async def test_saas_connector_test_and_sync(integration_env: None, slug: str, env_map: dict[str, str]) -> None:
    if os.getenv("RUN_SAAS_CONNECTOR_INTEGRATION") != "1":
        pytest.skip("Set RUN_SAAS_CONNECTOR_INTEGRATION=1 to run live SaaS connector tests.")

    credentials = {field: os.getenv(env_name, "") for field, env_name in env_map.items()}
    optional_fields = {f.name for f in CATALOG_BY_SLUG[slug].credential_schema if not f.required}
    missing = [
        env_name
        for field, env_name in env_map.items()
        if field not in optional_fields and not os.getenv(env_name)
    ]
    if missing:
        pytest.skip(f"Missing SaaS credentials for {slug}: {', '.join(missing)}")

    adapter = adapter_for(slug)
    test_result = await adapter.test(credentials)
    assert test_result.status == "ok", test_result.message

    sync_result = await adapter.sync(credentials)
    assert sync_result["mode"] == "real"
    assert sync_result["objects_synced"] >= 0


@pytest.mark.integration
def test_integration_result_file_has_all_local_connectors(integration_results_file: Path) -> None:
    if os.getenv("RUN_CONNECTOR_INTEGRATION") != "1":
        pytest.skip("Set RUN_CONNECTOR_INTEGRATION=1 to run local Docker connector tests.")
    deadline = time.time() + 10
    while time.time() < deadline:
        if RESULTS_FILE.exists():
            results = json.loads(RESULTS_FILE.read_text())
            if set(LOCAL_CONNECTORS).issubset(results):
                break
        time.sleep(0.5)
    results = json.loads(RESULTS_FILE.read_text())
    assert set(LOCAL_CONNECTORS) == set(results)
