import asyncio
import base64
import logging
import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import quote_plus, urlparse

import httpx
import pymssql
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.config import get_settings
from app.services.connectors.catalog import CATALOG_BY_SLUG, TestResult, VerificationMode

# Redshift advertises `client_encoding=UNICODE` in its startup packet, but
# psycopg3 only knows the canonical Postgres name `UTF8`. Without this alias
# every connection fails with `NotSupportedError: codec not available: UNICODE`
# before any SQL runs (#redshift-psycopg3-codec).
try:
    import psycopg._encodings as _psycopg_encodings  # type: ignore[import-not-found]

    _psycopg_encodings.py_codecs.setdefault(b"UNICODE", "utf-8")
    _psycopg_encodings._py_codecs.setdefault("UNICODE", "utf-8")
except ImportError:
    pass

DEMO_SQLITE_PATH = Path("/tmp/dataclaw_demo.sqlite")


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


def parse_redshift_endpoint(value: str, default_port: str = "5439") -> tuple[str, str, str | None]:
    raw = value.strip()
    parsed = urlparse(raw if "://" in raw else f"redshift://{raw}")
    host = parsed.hostname or raw.split(":", 1)[0]
    port = str(parsed.port or default_port)
    database = parsed.path.lstrip("/") or None
    return host, port, database


class ConnectorAdapterError(RuntimeError):
    label = "Connector error"

    def __init__(self, connector_slug: str, message: str) -> None:
        self.connector_slug = connector_slug
        super().__init__(message)

    def clean_message(self) -> str:
        return f"{self.label}: {self}"


class AdapterAuthError(ConnectorAdapterError):
    label = "Authentication failed"


class AdapterReachabilityError(ConnectorAdapterError):
    label = "Connector unreachable"


class AdapterApiError(ConnectorAdapterError):
    label = "Connector API error"


class AdapterRateLimitError(ConnectorAdapterError):
    label = "Connector rate limited"


class ConnectorAdapter(Protocol):
    async def test(self, credentials: dict[str, Any]) -> TestResult: ...

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]: ...

    async def list_failed_runs(
        self, credentials: dict[str, Any], since: datetime | None = None
    ) -> list[dict[str, Any]]: ...


class BaseAdapter:
    slug: str

    def definition(self):
        return CATALOG_BY_SLUG[self.slug]

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        return {
            "mode": self.definition().local_verification,
            "summary": self.definition().sync_behavior,
            "objects_synced": 0,
        }

    async def list_failed_runs(
        self, credentials: dict[str, Any], since: datetime | None = None
    ) -> list[dict[str, Any]]:
        return _collect_failed_objects(await self.sync(credentials))


class CredentialRequiredAdapter(BaseAdapter):
    def __init__(self, slug: str) -> None:
        self.slug = slug

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        missing = [
            field.name
            for field in self.definition().credential_schema
            if field.required and not credentials.get(field.name)
        ]
        if missing:
            return TestResult(
                slug=self.slug,
                status="credential_required",
                mode=VerificationMode.CREDENTIAL_REQUIRED,
                message=f"Missing required credentials: {', '.join(missing)}.",
            )
        return TestResult(
            slug=self.slug,
            status="credential_required",
            mode=VerificationMode.CREDENTIAL_REQUIRED,
            message="Credentials are present but this SaaS connector requires live customer access to verify.",
            details={"label": "Credential required"},
        )


def _missing_required(definition, credentials: dict[str, Any]) -> list[str]:
    return [
        field.name
        for field in definition.credential_schema
        if field.required and not credentials.get(field.name)
    ]


def _required_result(slug: str, definition, credentials: dict[str, Any]) -> TestResult | None:
    missing = _missing_required(definition, credentials)
    if missing:
        return TestResult(
            slug=slug,
            status="credential_required",
            mode=VerificationMode.CREDENTIAL_REQUIRED,
            message=f"Missing required credentials: {', '.join(missing)}.",
        )
    return None


def _adapter_error(slug: str, display_name: str, exc: Exception) -> ConnectorAdapterError:
    if isinstance(exc, ConnectorAdapterError):
        return exc
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        if status in {401, 403}:
            return AdapterAuthError(slug, f"{display_name} rejected credentials.")
        if status == 429:
            return AdapterRateLimitError(slug, f"{display_name} rate limit exceeded.")
        return AdapterApiError(slug, f"{display_name} returned HTTP {status}.")
    if isinstance(exc, httpx.TimeoutException):
        return AdapterReachabilityError(slug, f"{display_name} did not respond before the timeout.")
    if isinstance(exc, httpx.TransportError):
        return AdapterReachabilityError(slug, f"{display_name} could not be reached.")
    message = str(exc).strip() or exc.__class__.__name__
    lower = message.lower()
    if any(
        term in lower
        for term in ("password", "authentication", "auth", "login failed", "access denied")
    ):
        return AdapterAuthError(slug, f"{display_name} rejected credentials.")
    if any(
        term in lower
        for term in ("timeout", "could not connect", "connection refused", "network", "unreachable")
    ):
        return AdapterReachabilityError(slug, f"{display_name} could not be reached.")
    return AdapterApiError(slug, f"{display_name} request failed: {message}")


FAILURE_STATES = {
    "failed",
    "failure",
    "error",
    "errored",
    "cancelled",
    "canceled",
    "crashed",
    "timeout",
    "timed_out",
}


def _collect_failed_objects(payload: object) -> list[dict[str, Any]]:
    failures: list[dict[str, Any]] = []

    def visit(value: object) -> None:
        if isinstance(value, list):
            for item in value:
                visit(item)
            return
        if not isinstance(value, dict):
            return
        status = str(
            value.get("status")
            or value.get("state")
            or value.get("sync_state")
            or value.get("last_sync_state")
            or ""
        ).lower()
        if status in FAILURE_STATES or any(term in status for term in ("fail", "error")):
            failures.append(value)
            return
        for nested in value.values():
            if isinstance(nested, (dict, list)):
                visit(nested)

    visit(payload)
    return failures


def clean_connector_error(slug: str, exc: Exception) -> ConnectorAdapterError:
    display_name = CATALOG_BY_SLUG.get(slug).display_name if slug in CATALOG_BY_SLUG else slug
    return _adapter_error(slug, display_name, exc)


def _failed_result(slug: str, mode: VerificationMode, exc: Exception) -> TestResult:
    error = clean_connector_error(slug, exc)
    return TestResult(
        slug=slug,
        status="failed",
        mode=mode,
        message=error.clean_message(),
        details={"error_type": error.__class__.__name__},
    )


class MySQLAdapter(BaseAdapter):
    slug = "mysql"

    def _url(self, credentials: dict[str, Any]) -> str:
        host = credentials["host"]
        port = credentials.get("port") or "3306"
        user = quote_plus(credentials["user"])
        password = quote_plus(credentials["password"])
        database = credentials["database"]
        return f"mysql+aiomysql://{user}:{password}@{host}:{port}/{database}"

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        if result := _required_result(self.slug, self.definition(), credentials):
            return result
        engine = create_async_engine(self._url(credentials), pool_pre_ping=True)
        try:
            async with engine.connect() as conn:
                value = await conn.scalar(text("select 1"))
            return TestResult(
                slug=self.slug,
                status="ok",
                mode=VerificationMode.REAL,
                message="MySQL connection succeeded.",
                details={"select_1": value, "label": "Real"},
            )
        except Exception as exc:  # pragma: no cover - driver errors vary
            return _failed_result(self.slug, VerificationMode.REAL, exc)
        finally:
            await engine.dispose()

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        engine = create_async_engine(self._url(credentials), pool_pre_ping=True)
        try:
            async with engine.connect() as conn:
                table_rows = (
                    (
                        await conn.execute(
                            text(
                                "select table_name as name "
                                "from information_schema.tables "
                                "where table_schema = :schema and table_type = 'BASE TABLE' "
                                "order by table_name"
                            ),
                            {"schema": credentials["database"]},
                        )
                    )
                    .mappings()
                    .all()
                )
                tables = []
                for row in table_rows:
                    name = row["name"]
                    column_rows = (
                        (
                            await conn.execute(
                                text(
                                    "select column_name as name, data_type as type "
                                    "from information_schema.columns "
                                    "where table_schema = :schema and table_name = :table "
                                    "order by ordinal_position"
                                ),
                                {"schema": credentials["database"], "table": name},
                            )
                        )
                        .mappings()
                        .all()
                    )
                    count = await conn.scalar(text(f"select count(*) from `{name}`"))
                    tables.append(
                        {
                            "name": name,
                            "schema": credentials["database"],
                            "row_count": int(count or 0),
                            "columns": [
                                {"name": col["name"], "type": col["type"], "description": ""}
                                for col in column_rows
                            ],
                        }
                    )
            return {
                "mode": "real",
                "objects_synced": len(tables),
                "tables": tables,
                "summary": "Synced MySQL INFORMATION_SCHEMA tables from a live connection.",
                "source_type": "mysql",
                "schema_name": credentials["database"],
            }
        finally:
            await engine.dispose()


class SQLServerAdapter(BaseAdapter):
    slug = "sql_server"

    def _connect_sync(self, credentials: dict[str, Any]):
        host = str(credentials["host"])
        port = credentials.get("port") or 1433
        if "," in host and not credentials.get("port"):
            host, port = host.rsplit(",", 1)
        return pymssql.connect(
            server=host,
            port=int(port),
            user=credentials["user"],
            password=credentials["password"],
            database=credentials["database"],
            login_timeout=10,
            timeout=20,
        )

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        if result := _required_result(self.slug, self.definition(), credentials):
            return result

        def run_select() -> int:
            with self._connect_sync(credentials) as conn:
                with conn.cursor() as cursor:
                    cursor.execute("select 1")
                    row = cursor.fetchone()
                    return int(row[0])

        try:
            value = await asyncio.to_thread(run_select)
            return TestResult(
                slug=self.slug,
                status="ok",
                mode=VerificationMode.REAL,
                message="SQL Server connection succeeded.",
                details={"select_1": value, "label": "Real"},
            )
        except Exception as exc:  # pragma: no cover - driver errors vary
            return _failed_result(self.slug, VerificationMode.REAL, exc)

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        def load_tables() -> list[dict[str, Any]]:
            with self._connect_sync(credentials) as conn:
                with conn.cursor(as_dict=True) as cursor:
                    cursor.execute(
                        "select table_schema, table_name "
                        "from information_schema.tables "
                        "where table_type = 'BASE TABLE' "
                        "and table_schema not in ('sys', 'INFORMATION_SCHEMA') "
                        "and table_name not like 'MSreplication_%' "
                        "and table_name not like 'spt_%' "
                        "order by table_schema, table_name"
                    )
                    base = list(cursor.fetchall())
                    enriched = []
                    for row in base:
                        schema = row["table_schema"]
                        name = row["table_name"]
                        cursor.execute(
                            "select column_name, data_type "
                            "from information_schema.columns "
                            "where table_schema = %s and table_name = %s "
                            "order by ordinal_position",
                            (schema, name),
                        )
                        columns = [
                            {
                                "name": col["column_name"],
                                "type": col["data_type"],
                                "description": "",
                            }
                            for col in cursor.fetchall()
                        ]
                        cursor.execute(f"select count(*) as n from [{schema}].[{name}]")
                        count_row = cursor.fetchone() or {"n": 0}
                        enriched.append(
                            {
                                "name": name,
                                "schema": schema,
                                "row_count": int(count_row["n"]),
                                "columns": columns,
                            }
                        )
                    return enriched

        tables = await asyncio.to_thread(load_tables)
        return {
            "mode": "real",
            "objects_synced": len(tables),
            "tables": tables,
            "summary": "Synced SQL Server INFORMATION_SCHEMA tables from a live connection.",
            "source_type": "sql_server",
            "schema_name": tables[0]["schema"] if tables else "dbo",
        }


class TrinoAdapter(BaseAdapter):
    slug = "trino"

    def _connect_sync(self, credentials: dict[str, Any]):
        try:
            import trino  # type: ignore[import-not-found]
            from trino.auth import BasicAuthentication  # type: ignore[import-not-found]
        except ImportError as exc:  # pragma: no cover - depends on deployment extras
            raise RuntimeError(
                "trino Python client not installed; run `pip install trino[sqlalchemy]`."
            ) from exc

        kwargs: dict[str, Any] = {
            "host": credentials["host"],
            "port": int(credentials.get("port") or 8080),
            "user": credentials["user"],
            "catalog": credentials["catalog"],
            "schema": credentials["schema"],
            "http_scheme": credentials.get("http_scheme")
            or ("https" if credentials.get("password") else "http"),
        }
        if credentials.get("password"):
            kwargs["auth"] = BasicAuthentication(credentials["user"], credentials["password"])
        return trino.dbapi.connect(**kwargs)

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        if result := _required_result(self.slug, self.definition(), credentials):
            return result

        def run_select() -> int:
            with self._connect_sync(credentials) as conn:
                cursor = conn.cursor()
                cursor.execute("select 1")
                row = cursor.fetchone()
                return int(row[0])

        try:
            value = await asyncio.to_thread(run_select)
            return TestResult(
                slug=self.slug,
                status="ok",
                mode=VerificationMode.REAL,
                message="Trino connection succeeded.",
                details={"select_1": value, "label": "Real"},
            )
        except Exception as exc:  # pragma: no cover - driver errors vary
            return _failed_result(self.slug, VerificationMode.REAL, exc)

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        # Only iterate tables in the user-configured schema. Without this,
        # connectors like TPC-H expose `sf100000` (600B rows in `lineitem`) and
        # the count(*) loop runs forever, abandons the query, and never lands.
        def load_tables() -> list[dict[str, Any]]:
            schema = credentials.get("schema")
            with self._connect_sync(credentials) as conn:
                cursor = conn.cursor()
                if schema:
                    cursor.execute(
                        "select table_schema, table_name "
                        "from information_schema.tables "
                        "where table_catalog = ? and table_schema = ? "
                        "order by table_name",
                        [credentials["catalog"], schema],
                    )
                else:
                    cursor.execute(
                        "select table_schema, table_name "
                        "from information_schema.tables "
                        "where table_catalog = ? and table_schema not in ('information_schema') "
                        "order by table_schema, table_name",
                        [credentials["catalog"]],
                    )
                base = cursor.fetchall()
                enriched = []
                for schema_name, table_name in base:
                    cursor.execute(
                        "select column_name, data_type "
                        "from information_schema.columns "
                        "where table_catalog = ? and table_schema = ? and table_name = ? "
                        "order by ordinal_position",
                        [credentials["catalog"], schema_name, table_name],
                    )
                    columns = [
                        {"name": column_name, "type": data_type, "description": ""}
                        for column_name, data_type in cursor.fetchall()
                    ]
                    cursor.execute(f'select count(*) from "{schema_name}"."{table_name}"')
                    count_row = cursor.fetchone() or [0]
                    enriched.append(
                        {
                            "name": table_name,
                            "schema": schema_name,
                            "row_count": int(count_row[0] or 0),
                            "columns": columns,
                        }
                    )
                return enriched

        tables = await asyncio.to_thread(load_tables)
        return {
            "mode": "real",
            "objects_synced": len(tables),
            "tables": tables,
            "summary": "Synced Trino INFORMATION_SCHEMA tables from a live connection.",
            "source_type": "trino",
            "schema_name": credentials.get("schema") or (tables[0]["schema"] if tables else ""),
        }


class HTTPConnectorAdapter(BaseAdapter):
    health_path = "/health"
    sync_path = "/objects"
    auth_header_name: str | None = None
    auth_prefix = ""

    def __init__(self, slug: str) -> None:
        self.slug = slug

    def base_url(self, credentials: dict[str, Any]) -> str:
        for key in ("base_url", "api_url", "graphql_url", "workspace_url"):
            if credentials.get(key):
                value = str(credentials[key]).rstrip("/")
                if key == "graphql_url" and value.endswith("/graphql"):
                    return value[: -len("/graphql")]
                return value
        return ""

    def headers(self, credentials: dict[str, Any]) -> dict[str, str]:
        token = credentials.get("api_key") or credentials.get("token")
        if token and self.auth_header_name:
            return {self.auth_header_name: f"{self.auth_prefix}{token}"}
        return {}

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        if result := _required_result(self.slug, self.definition(), credentials):
            return result
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                response = await client.get(
                    f"{self.base_url(credentials)}{self.health_path}",
                    headers=self.headers(credentials),
                )
                response.raise_for_status()
            return TestResult(
                slug=self.slug,
                status="ok",
                mode=VerificationMode.REAL,
                message=f"{self.definition().display_name} API connection succeeded.",
                details={"label": "Real"},
            )
        except Exception as exc:  # pragma: no cover - live service errors vary
            return _failed_result(self.slug, VerificationMode.REAL, exc)

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(
                f"{self.base_url(credentials)}{self.sync_path}",
                headers=self.headers(credentials),
            )
            response.raise_for_status()
            payload = response.json()
        objects = payload.get("objects") or payload.get("items") or payload.get("data") or []
        return {
            "mode": "real",
            "objects_synced": len(objects),
            "objects": objects,
            "summary": f"Synced {self.definition().display_name} objects from a live API.",
        }


class AirflowAdapter(HTTPConnectorAdapter):
    health_path = "/api/v1/health"
    sync_path = "/api/v1/dags"

    def headers(self, credentials: dict[str, Any]) -> dict[str, str]:
        if credentials.get("username") and credentials.get("password"):
            raw = f"{credentials['username']}:{credentials['password']}".encode()
            return {"Authorization": f"Basic {base64.b64encode(raw).decode()}"}
        return {}

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        # Airflow sandbox containers occasionally fail to fetch configuration on
        # first boot, returning 5xx or a connection error briefly. Retry with
        # backoff so the transient case clears itself.
        result = await super().test(credentials)
        for delay in (1.0, 3.0):
            if result.status != "failed":
                return result
            await asyncio.sleep(delay)
            result = await super().test(credentials)
        return result

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(
                f"{self.base_url(credentials)}{self.sync_path}",
                headers=self.headers(credentials),
            )
            response.raise_for_status()
            payload = response.json()
        dags = payload.get("dags", [])
        return {
            "mode": "real",
            "objects_synced": len(dags),
            "dags": dags,
            "summary": "Synced Airflow DAG metadata from a live REST API.",
        }

    async def fetch_content(self, credentials: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.get(
                f"{self.base_url(credentials)}{self.sync_path}",
                headers=self.headers(credentials),
            )
            response.raise_for_status()
            dags = response.json().get("dags", [])
            enriched = []
            for dag in dags:
                dag_id = dag.get("dag_id") or dag.get("id")
                if not dag_id:
                    continue
                source_code = ""
                recent_runs: list[dict[str, Any]] = []
                file_token = dag.get("file_token")
                if not file_token:
                    detail_response = await client.get(
                        f"{self.base_url(credentials)}/api/v1/dags/{dag_id}",
                        headers=self.headers(credentials),
                    )
                    if detail_response.status_code < 400:
                        file_token = detail_response.json().get("file_token")
                if file_token:
                    source_response = await client.get(
                        f"{self.base_url(credentials)}/api/v1/dagSources/{file_token}",
                        headers={**self.headers(credentials), "Accept": "text/plain"},
                    )
                    if source_response.status_code < 400:
                        source_code = source_response.text
                if not source_code:
                    source_response = await client.get(
                        f"{self.base_url(credentials)}/api/v1/dags/{dag_id}/source",
                        headers=self.headers(credentials),
                    )
                    if source_response.status_code < 400:
                        content_type = source_response.headers.get("content-type", "")
                        if "application/json" in content_type:
                            source_payload = source_response.json()
                            source_code = (
                                source_payload.get("source_code")
                                or source_payload.get("source")
                                or ""
                            )
                        else:
                            source_code = source_response.text
                runs_response = await client.get(
                    f"{self.base_url(credentials)}/api/v1/dags/{dag_id}/dagRuns",
                    headers=self.headers(credentials),
                    params={"limit": 10, "order_by": "-execution_date"},
                )
                if runs_response.status_code < 400:
                    runs_payload = runs_response.json()
                    recent_runs = runs_payload.get("dag_runs") or runs_payload.get("runs") or []
                enriched.append(
                    {
                        "dag_id": dag_id,
                        "description": dag.get("description") or "",
                        "schedule_interval": (dag.get("schedule_interval") or {}).get("value")
                        if isinstance(dag.get("schedule_interval"), dict)
                        else dag.get("schedule_interval"),
                        "tags": [
                            t.get("name") if isinstance(t, dict) else t
                            for t in (dag.get("tags") or [])
                        ],
                        "is_paused": dag.get("is_paused"),
                        "fileloc": dag.get("fileloc"),
                        "source_code": source_code,
                        "recent_runs": recent_runs[:10],
                        "owners": dag.get("owners") or dag.get("owner") or [],
                    }
                )
        return {"dags": enriched}


class AirbyteAdapter(HTTPConnectorAdapter):
    """Speaks both Airbyte Cloud (OAuth client_credentials → short-lived JWT)
    and self-hosted Airbyte (long-lived static `api_key`).

    Cloud bearer tokens expire after ~15 minutes. Storing a static JWT will
    break the next sync; we accept `client_id` + `client_secret` instead and
    fetch a fresh token at the start of every call.
    """

    health_path = "/api/v1/health"
    auth_header_name = "Authorization"
    auth_prefix = "Bearer "

    async def _resolve_token(self, client: httpx.AsyncClient, credentials: dict[str, Any]) -> str:
        """Return a bearer token for this call.

        Order of preference:
        1. `client_id` + `client_secret` → exchange for a fresh JWT every call.
        2. `api_key` → use as-is (long-lived self-hosted token or a manually
           refreshed Cloud JWT).
        """
        client_id = credentials.get("client_id")
        client_secret = credentials.get("client_secret")
        if client_id and client_secret:
            token_url = (
                credentials.get("token_url") or "https://api.airbyte.com/v1/applications/token"
            )
            response = await client.post(
                token_url,
                json={
                    "client_id": str(client_id),
                    "client_secret": str(client_secret),
                    "grant-type": "client_credentials",
                },
                timeout=10,
            )
            response.raise_for_status()
            payload = response.json()
            access_token = payload.get("access_token")
            if not access_token:
                raise RuntimeError("Airbyte token endpoint returned no access_token")
            return str(access_token)
        token = credentials.get("api_key") or credentials.get("token") or ""
        if not token:
            raise RuntimeError(
                "Airbyte requires either api_key (long-lived) or client_id + client_secret (OAuth)."
            )
        return str(token)

    def headers(self, credentials: dict[str, Any]) -> dict[str, str]:
        # Static path — used by the parent class's health check. The full
        # exchange flow is in _resolve_token() and is what every real call
        # below uses. This stays so the parent test() compiles, but the
        # overridden test() below uses _resolve_token().
        token = credentials.get("api_key") or credentials.get("token")
        return {"Authorization": f"Bearer {token}"} if token else {}

    def _is_cloud(self, credentials: dict[str, Any]) -> bool:
        """Cloud public API lives at api.airbyte.com and uses `/v1/...` paths;
        self-hosted Airbyte serves the legacy admin API under `/api/v1/...`."""
        return "api.airbyte.com" in str(credentials.get("api_url", ""))

    def _path(self, credentials: dict[str, Any], endpoint: str) -> str:
        prefix = "/v1" if self._is_cloud(credentials) else "/api/v1"
        return f"{self.base_url(credentials)}{prefix}{endpoint}"

    async def _workspace_id(
        self,
        client: httpx.AsyncClient,
        headers: dict[str, str],
        credentials: dict[str, Any],
    ) -> str | None:
        explicit = credentials.get("workspace_id") or credentials.get("workspaceId")
        if explicit:
            return str(explicit)
        try:
            if self._is_cloud(credentials):
                response = await client.get(
                    self._path(credentials, "/workspaces"), headers=headers, timeout=5
                )
            else:
                response = await client.post(
                    self._path(credentials, "/workspaces/list"), headers=headers, json={}, timeout=5
                )
        except httpx.TimeoutException:
            return None
        if response.status_code in (404, 403):
            return None
        response.raise_for_status()
        payload = response.json()
        workspaces = payload.get("workspaces") or payload.get("data") or []
        if not workspaces:
            return None
        workspace = workspaces[0]
        return str(
            workspace.get("workspaceId")
            or workspace.get("workspace_id")
            or workspace.get("id")
            or ""
        )

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        if not any(
            [
                credentials.get("api_key"),
                credentials.get("client_id") and credentials.get("client_secret"),
            ]
        ):
            return TestResult(
                slug=self.slug,
                status="credential_required",
                mode=VerificationMode.CREDENTIAL_REQUIRED,
                message="Airbyte requires either api_key OR client_id + client_secret.",
            )
        if not credentials.get("api_url"):
            return TestResult(
                slug=self.slug,
                status="credential_required",
                mode=VerificationMode.CREDENTIAL_REQUIRED,
                message="Airbyte requires api_url.",
            )
        try:
            async with httpx.AsyncClient(timeout=15) as client:
                token = await self._resolve_token(client, credentials)
                headers = {"Authorization": f"Bearer {token}"}
                response = await client.get(self._path(credentials, "/health"), headers=headers)
                response.raise_for_status()
            return TestResult(
                slug=self.slug,
                status="ok",
                mode=VerificationMode.REAL,
                message="Airbyte API connection succeeded.",
                details={"label": "Real"},
            )
        except Exception as exc:  # pragma: no cover - live service errors vary
            return _failed_result(self.slug, VerificationMode.REAL, exc)

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=20) as client:
            token = await self._resolve_token(client, credentials)
            headers = {"Authorization": f"Bearer {token}"}
            workspace_id = await self._workspace_id(client, headers, credentials)
            response: httpx.Response | None = None
            if self._is_cloud(credentials):
                # Cloud public API: GET /v1/connections (optionally filtered)
                params = {"workspaceIds": workspace_id} if workspace_id else None
                response = await client.get(
                    self._path(credentials, "/connections"),
                    headers=headers,
                    params=params,
                    timeout=10,
                )
            else:
                # Self-hosted: POST /api/v1/connections/list
                if workspace_id:
                    response = await client.post(
                        self._path(credentials, "/connections/list"),
                        headers=headers,
                        json={"workspaceId": workspace_id},
                        timeout=10,
                    )
                if response is None or response.status_code == 404:
                    response = await client.get(
                        f"{self.base_url(credentials)}/v1/connections",
                        headers=headers,
                        timeout=10,
                    )
            response.raise_for_status()
            payload = response.json()
        connections = (
            payload.get("connections") or payload.get("data") or payload.get("objects") or []
        )
        return {
            "mode": "real",
            "objects_synced": len(connections),
            "connections": connections,
            "summary": "Synced Airbyte connections from a live API.",
        }

    async def list_failed_runs(
        self, credentials: dict[str, Any], since: datetime | None = None
    ) -> list[dict[str, Any]]:
        async with httpx.AsyncClient(timeout=20) as client:
            token = await self._resolve_token(client, credentials)
            headers = {"Authorization": f"Bearer {token}"}
            if self._is_cloud(credentials):
                params = {"limit": 50, "status": "failed"}
                response = await client.get(
                    self._path(credentials, "/jobs"), headers=headers, params=params
                )
            else:
                body: dict[str, Any] = {
                    "configTypes": ["sync", "reset"],
                    "statuses": ["failed", "cancelled"],
                    "pagination": {"pageSize": 50},
                }
                if since:
                    body["createdAtStart"] = int(since.timestamp())
                response = await client.post(
                    self._path(credentials, "/jobs/list"), headers=headers, json=body
                )
            response.raise_for_status()
            payload = response.json()
        jobs = payload.get("jobs") or payload.get("data") or []
        return _collect_failed_objects(jobs)


class PrefectAdapter(HTTPConnectorAdapter):
    """Speaks both Prefect Cloud (workspace-scoped URL) and self-hosted Prefect.

    Cloud workspace URLs look like
    `https://api.prefect.cloud/api/accounts/<a>/workspaces/<w>` — the `/api/`
    prefix is already part of the workspace path, so endpoints are reached
    directly (e.g. `<base>/health`). Self-hosted Prefect API URLs typically
    end in `/api`, and endpoints are reached via `<base>/api/<endpoint>`.
    """

    auth_header_name = "Authorization"
    auth_prefix = "Bearer "

    def base_url(self, credentials: dict[str, Any]) -> str:
        value = super().base_url(credentials)
        # For Cloud workspace URLs we keep the path as-is. For self-hosted we
        # strip a trailing `/api` so the endpoint paths below can prefix it.
        if "/workspaces/" in value:
            return value
        return value[: -len("/api")] if value.endswith("/api") else value

    def _path(self, credentials: dict[str, Any], endpoint: str) -> str:
        base = self.base_url(credentials)
        prefix = "" if "/workspaces/" in base else "/api"
        return f"{base}{prefix}{endpoint}"

    def headers(self, credentials: dict[str, Any]) -> dict[str, str]:
        token = credentials.get("api_key") or credentials.get("token")
        return {"Authorization": f"Bearer {token}"} if token else {}

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        if result := _required_result(self.slug, self.definition(), credentials):
            return result
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                response = await client.get(
                    self._path(credentials, "/health"),
                    headers=self.headers(credentials),
                )
                response.raise_for_status()
            return TestResult(
                slug=self.slug,
                status="ok",
                mode=VerificationMode.REAL,
                message="Prefect API connection succeeded.",
                details={"label": "Real"},
            )
        except Exception as exc:  # pragma: no cover - live service errors vary
            return _failed_result(self.slug, VerificationMode.REAL, exc)

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.post(
                self._path(credentials, "/flows/filter"),
                headers=self.headers(credentials),
                json={"limit": 50, "sort": "CREATED_DESC"},
            )
            response.raise_for_status()
            payload = response.json()
        flows = payload if isinstance(payload, list) else payload.get("data") or []
        return {
            "mode": "real",
            "objects_synced": len(flows),
            "flows": flows,
            "summary": "Synced Prefect flows from a live API.",
        }

    async def list_failed_runs(
        self, credentials: dict[str, Any], since: datetime | None = None
    ) -> list[dict[str, Any]]:
        body: dict[str, Any] = {
            "limit": 50,
            "sort": "START_TIME_DESC",
            "flow_runs": {"state": {"type": {"any_": ["FAILED", "CRASHED", "CANCELLED"]}}},
        }
        if since:
            body["flow_runs"]["start_time"] = {"after_": since.isoformat()}
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.post(
                self._path(credentials, "/flow_runs/filter"),
                headers=self.headers(credentials),
                json=body,
            )
            response.raise_for_status()
            payload = response.json()
        return payload if isinstance(payload, list) else payload.get("data") or []


class DagsterAdapter(HTTPConnectorAdapter):
    """Dagster Cloud uses the `Dagster-Cloud-Api-Token` header and exposes only
    the `/graphql` endpoint (no `/server_info`). Self-hosted OSS Dagster has
    no auth header and serves `/server_info` for liveness — but the GraphQL
    `{ version }` query works on both, so we use that uniformly.
    """

    def base_url(self, credentials: dict[str, Any]) -> str:
        value = str(credentials.get("graphql_url", "")).rstrip("/")
        return value if value.endswith("/graphql") else f"{value}/graphql"

    def headers(self, credentials: dict[str, Any]) -> dict[str, str]:
        token = credentials.get("token") or credentials.get("api_key") or ""
        if not token:
            return {}
        # Dagster Cloud user/agent tokens go on a dedicated header; the OSS
        # webserver ignores unknown headers, so this is safe to send always.
        return {"Dagster-Cloud-Api-Token": token}

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        if result := _required_result(self.slug, self.definition(), credentials):
            return result
        try:
            async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
                response = await client.post(
                    self.base_url(credentials),
                    json={"query": "{ version }"},
                    headers={
                        "Content-Type": "application/json",
                        **self.headers(credentials),
                    },
                )
                response.raise_for_status()
                payload = response.json()
            if payload.get("errors"):
                raise RuntimeError(str(payload["errors"]))
            version = payload.get("data", {}).get("version", "unknown")
            return TestResult(
                slug=self.slug,
                status="ok",
                mode=VerificationMode.REAL,
                message=f"Dagster GraphQL connection succeeded (version {version}).",
                details={"label": "Real", "version": version},
            )
        except Exception as exc:  # pragma: no cover - live service errors vary
            return _failed_result(self.slug, VerificationMode.REAL, exc)

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        query = (
            "{ assetsOrError { __typename ... on AssetConnection { nodes { id key { path } } } } }"
        )
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.post(
                self.base_url(credentials),
                json={"query": query},
                headers={"Content-Type": "application/json", **self.headers(credentials)},
            )
            response.raise_for_status()
            payload = response.json()
        nodes = payload.get("data", {}).get("assetsOrError", {}).get("nodes", [])
        return {
            "mode": "real",
            "objects_synced": len(nodes),
            "assets": nodes,
            "summary": "Synced Dagster asset metadata from a live GraphQL API.",
        }

    async def list_failed_runs(
        self, credentials: dict[str, Any], since: datetime | None = None
    ) -> list[dict[str, Any]]:
        query = (
            "query FailedRuns($filter: RunsFilter) { runsOrError(filter: $filter, limit: 50) "
            "{ __typename ... on Runs { results { runId status pipelineName startTime endTime } } } }"
        )
        variables: dict[str, Any] = {"filter": {"statuses": ["FAILURE", "CANCELED"]}}
        if since:
            variables["filter"]["createdAfter"] = int(since.timestamp())
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            response = await client.post(
                self.base_url(credentials),
                json={"query": query, "variables": variables},
                headers={"Content-Type": "application/json", **self.headers(credentials)},
            )
            response.raise_for_status()
            payload = response.json()
        runs = payload.get("data", {}).get("runsOrError", {}).get("results", [])
        return runs if isinstance(runs, list) else []


class GitHubAdapter(BaseAdapter):
    slug = "github"

    def base_url(self, credentials: dict[str, Any]) -> str:
        return str(credentials.get("base_url") or "https://api.github.com").rstrip("/")

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        if result := _required_result(self.slug, self.definition(), credentials):
            return result
        repos = [repo.strip() for repo in credentials["repositories"].split(",") if repo.strip()]
        try:
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(
                    f"{self.base_url(credentials)}/user",
                    headers={
                        "Authorization": f"Bearer {credentials['token']}",
                        "Accept": "application/vnd.github+json",
                    },
                )
                response.raise_for_status()
            return TestResult(
                slug=self.slug,
                status="ok",
                mode=VerificationMode.REAL,
                message="GitHub API token is valid.",
                details={"repositories": repos, "label": "Real"},
            )
        except Exception as exc:  # pragma: no cover - depends on live credentials
            return _failed_result(self.slug, VerificationMode.REAL, exc)

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        repos = [repo.strip() for repo in credentials["repositories"].split(",") if repo.strip()]
        synced = []
        async with httpx.AsyncClient(timeout=20) as client:
            for repo in repos:
                response = await client.get(
                    f"{self.base_url(credentials)}/repos/{repo}",
                    headers={
                        "Authorization": f"Bearer {credentials['token']}",
                        "Accept": "application/vnd.github+json",
                    },
                )
                response.raise_for_status()
                data = response.json()
                synced.append(
                    {
                        "full_name": data.get("full_name"),
                        "default_branch": data.get("default_branch"),
                    }
                )
        return {
            "mode": "real",
            "objects_synced": len(synced),
            "repositories": synced,
            "summary": "Synced GitHub repository metadata from the live REST API.",
        }

    async def fetch_content(self, credentials: dict[str, Any]) -> dict[str, Any]:
        repos = [repo.strip() for repo in credentials["repositories"].split(",") if repo.strip()]
        headers = {
            "Authorization": f"Bearer {credentials['token']}",
            "Accept": "application/vnd.github+json",
        }
        synced = []
        async with httpx.AsyncClient(timeout=30) as client:
            for repo in repos:
                repo_response = await client.get(
                    f"{self.base_url(credentials)}/repos/{repo}", headers=headers
                )
                repo_response.raise_for_status()
                repo_data = repo_response.json()
                default_branch = repo_data.get("default_branch")
                files: list[dict[str, Any]] = []
                stack = [""]
                while stack:
                    path = stack.pop()
                    response = await client.get(
                        f"{self.base_url(credentials)}/repos/{repo}/contents/{path}",
                        headers=headers,
                        params={"ref": default_branch} if default_branch else None,
                    )
                    if response.status_code >= 400:
                        continue
                    payload = response.json()
                    entries = payload if isinstance(payload, list) else [payload]
                    for item in entries:
                        item_path = item.get("path") or ""
                        item_type = item.get("type")
                        if item_type == "dir":
                            stack.append(item_path)
                            continue
                        suffix = item_path.lower()
                        if not (
                            suffix.endswith(".md")
                            or suffix.endswith(".sql")
                            or suffix == "dbt_project.yml"
                        ):
                            continue
                        download_url = item.get("download_url")
                        content = ""
                        if download_url:
                            content_response = await client.get(download_url, headers=headers)
                            if content_response.status_code < 400:
                                content = content_response.text
                        elif item.get("content"):
                            content = base64.b64decode(str(item["content"])).decode(
                                "utf-8", errors="replace"
                            )
                        files.append(
                            {
                                "path": item_path,
                                "content": content,
                                "type": suffix.rsplit(".", 1)[-1],
                            }
                        )
                synced.append({"full_name": repo_data.get("full_name") or repo, "files": files})
        return {"repos": synced}


class NotionAdapter(HTTPConnectorAdapter):
    health_path = "/v1/users/me"
    sync_path = "/v1/search"

    def base_url(self, credentials: dict[str, Any]) -> str:
        return str(credentials.get("base_url") or "https://api.notion.com").rstrip("/")

    def headers(self, credentials: dict[str, Any]) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {credentials['integration_token']}",
            "Notion-Version": "2022-06-28",
        }

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.post(
                f"{self.base_url(credentials)}{self.sync_path}",
                headers=self.headers(credentials),
                json={"page_size": 10},
            )
            response.raise_for_status()
            payload = response.json()
        results = payload.get("results", [])
        return {
            "mode": "real",
            "objects_synced": len(results),
            "objects": [{"id": item.get("id"), "object": item.get("object")} for item in results],
            "summary": "Synced Notion search results from the live API.",
        }

    def _title_from_page(self, page: dict[str, Any]) -> str:
        for prop in (page.get("properties") or {}).values():
            values = prop.get("title") or []
            if values:
                return "".join(part.get("plain_text", "") for part in values) or page.get(
                    "id", "Untitled"
                )
        return page.get("title") or page.get("id", "Untitled")

    def _block_text(self, block: dict[str, Any]) -> str:
        block_type = block.get("type")
        payload = block.get(block_type or "", {})
        rich = payload.get("rich_text") or []
        text = "".join(part.get("plain_text", "") for part in rich)
        if block_type in {"heading_1", "heading_2", "heading_3"}:
            return f"\n# {text}\n"
        if block_type == "bulleted_list_item":
            return f"- {text}"
        if block_type == "numbered_list_item":
            return f"1. {text}"
        return text

    async def _page_body(
        self, client: httpx.AsyncClient, credentials: dict[str, Any], page_id: str
    ) -> str:
        lines: list[str] = []
        cursor: str | None = None
        while True:
            params = {"page_size": 100}
            if cursor:
                params["start_cursor"] = cursor
            response = await client.get(
                f"{self.base_url(credentials)}/v1/blocks/{page_id}/children",
                headers=self.headers(credentials),
                params=params,
            )
            response.raise_for_status()
            payload = response.json()
            for block in payload.get("results", []):
                text = self._block_text(block)
                if text:
                    lines.append(text)
                if block.get("has_children"):
                    child = await self._page_body(client, credentials, block["id"])
                    if child:
                        lines.append(child)
            if not payload.get("has_more"):
                break
            cursor = payload.get("next_cursor")
        return "\n".join(lines)

    async def fetch_content(self, credentials: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=30) as client:
            response = await client.post(
                f"{self.base_url(credentials)}{self.sync_path}",
                headers=self.headers(credentials),
                json={"page_size": 50, "filter": {"property": "object", "value": "page"}},
            )
            response.raise_for_status()
            results = response.json().get("results", [])
            pages = []
            for page in results:
                page_id = page.get("id")
                if not page_id:
                    continue
                pages.append(
                    {
                        "id": page_id,
                        "title": self._title_from_page(page),
                        "body": await self._page_body(client, credentials, page_id),
                        "parent_id": (page.get("parent") or {}).get("page_id")
                        or (page.get("parent") or {}).get("database_id"),
                        "last_edited_at": page.get("last_edited_time"),
                    }
                )
        return {"pages": pages}


class SaaSHTTPAdapter(HTTPConnectorAdapter):
    """Dependency-light live HTTP adapters for SaaS APIs with credential-gated tests."""

    auth_header_name = "Authorization"
    auth_prefix = "Bearer "

    def base_url(self, credentials: dict[str, Any]) -> str:
        if self.slug == "quip":
            if credentials.get("base_url"):
                return str(credentials["base_url"]).rstrip("/")
            return "https://platform.quip.com"
        if self.slug == "confluence":
            if credentials.get("base_url"):
                return str(credentials["base_url"]).rstrip("/")
            return str(credentials["site_url"]).rstrip("/")
        if self.slug == "databricks":
            return self._databricks_base_url(credentials)
        if self.slug == "dbt":
            if credentials.get("base_url"):
                return str(credentials["base_url"]).rstrip("/")
            return f"https://cloud.getdbt.com/api/v2/accounts/{credentials['account_id']}"
        if self.slug == "fivetran":
            return "https://api.fivetran.com"
        return super().base_url(credentials)

    def _databricks_base_url(self, credentials: dict[str, Any]) -> str:
        workspace_url = str(credentials["workspace_url"]).rstrip("/")
        if not workspace_url.startswith(("http://", "https://")):
            workspace_url = f"https://{workspace_url}"
        if workspace_url.endswith("/api/2.0"):
            return workspace_url
        return f"{workspace_url}/api/2.0"

    def headers(self, credentials: dict[str, Any]) -> dict[str, str]:
        if self.slug == "quip":
            return {"Authorization": f"Bearer {credentials['access_token']}"}
        if self.slug == "confluence":
            raw = f"{credentials['email']}:{credentials['api_token']}".encode()
            return {"Authorization": f"Basic {base64.b64encode(raw).decode()}"}
        if self.slug == "fivetran":
            raw = f"{credentials['api_key']}:{credentials['api_secret']}".encode()
            return {"Authorization": f"Basic {base64.b64encode(raw).decode()}"}
        if self.slug == "dbt":
            return {"Authorization": f"Token {credentials['api_token']}"}
        return super().headers(credentials)

    @property
    def health_path(self) -> str:  # type: ignore[override]
        return {
            "quip": "/1/users/current",
            "confluence": "/wiki/rest/api/user/current",
            "databricks": "/clusters/list",
            "dbt": "/projects/",
            "fivetran": "/v1/groups",
        }.get(self.slug, "/health")

    @property
    def sync_path(self) -> str:  # type: ignore[override]
        return {
            "quip": "/1/threads/recent",
            "confluence": "/wiki/rest/api/content?limit=10",
            "databricks": "/jobs/list",
            "dbt": "/runs/?limit=10",
            "fivetran": "/v1/connections",
        }.get(self.slug, "/objects")

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=20) as client:
            response = await client.get(
                f"{self.base_url(credentials)}{self.sync_path}",
                headers=self.headers(credentials),
            )
            response.raise_for_status()
            payload = response.json()
        data = payload.get("data")
        objects = (
            payload.get("objects")
            or payload.get("items")
            or (data.get("items") if isinstance(data, dict) else data)
            or payload.get("results")
            or payload.get("threads")
            or payload.get("jobs")
            or payload.get("connectors")
            or []
        )
        return {
            "mode": "real",
            "objects_synced": len(objects) if isinstance(objects, list) else 1,
            "summary": f"Synced {self.definition().display_name} metadata from the live API.",
        }

    async def list_failed_runs(
        self, credentials: dict[str, Any], since: datetime | None = None
    ) -> list[dict[str, Any]]:
        if self.slug == "fivetran":
            async with httpx.AsyncClient(timeout=20) as client:
                response = await client.get(
                    f"{self.base_url(credentials)}/v1/connections",
                    headers=self.headers(credentials),
                )
                if response.status_code == 404:
                    response = await client.get(
                        f"{self.base_url(credentials)}/v1/connectors",
                        headers=self.headers(credentials),
                    )
                response.raise_for_status()
                payload = response.json()
            connectors = payload.get("data", {}).get("items") or payload.get("items") or []
            return [connector for connector in connectors if _collect_failed_objects(connector)]
        return await super().list_failed_runs(credentials, since)


class ConfluenceAdapter(SaaSHTTPAdapter):
    def __init__(self) -> None:
        super().__init__("confluence")

    async def fetch_content(self, credentials: dict[str, Any]) -> dict[str, Any]:
        pages: list[dict[str, Any]] = []
        start = 0
        limit = 25
        async with httpx.AsyncClient(timeout=30) as client:
            while True:
                response = await client.get(
                    f"{self.base_url(credentials)}/wiki/rest/api/content",
                    headers=self.headers(credentials),
                    params={
                        "type": "page",
                        "expand": "body.storage,version,space,_links",
                        "start": start,
                        "limit": limit,
                    },
                )
                response.raise_for_status()
                payload = response.json()
                results = payload.get("results") or []
                for page in results:
                    body = ((page.get("body") or {}).get("storage") or {}).get("value") or ""
                    pages.append(
                        {
                            "id": page.get("id"),
                            "title": page.get("title") or page.get("id") or "Untitled",
                            "body": body,
                            "space_key": ((page.get("space") or {}).get("key")),
                            "version": ((page.get("version") or {}).get("number")),
                            "url": ((page.get("_links") or {}).get("webui")),
                        }
                    )
                if not payload.get("_links", {}).get("next") and len(results) < limit:
                    break
                start += limit
        return {"pages": pages}


class DbtAdapter(SaaSHTTPAdapter):
    def __init__(self) -> None:
        self.slug = "dbt"

    async def fetch_content(self, credentials: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=30) as client:
            manifest_response = await client.get(
                f"{self.base_url(credentials)}/manifest.json",
                headers=self.headers(credentials),
            )
            if manifest_response.status_code >= 400:
                manifest_response = await client.get(
                    f"{self.base_url(credentials)}/artifacts/manifest.json",
                    headers=self.headers(credentials),
                )
            manifest_response.raise_for_status()
            manifest = manifest_response.json()
            runs_response = await client.get(
                f"{self.base_url(credentials)}/runs/?limit=10",
                headers=self.headers(credentials),
            )
            runs = runs_response.json().get("data", []) if runs_response.status_code < 400 else []
        models = []
        for node in (manifest.get("nodes") or {}).values():
            if node.get("resource_type") != "model":
                continue
            models.append(
                {
                    "name": node.get("name") or node.get("alias"),
                    "sql": node.get("raw_code") or node.get("compiled_code") or "",
                    "depends_on": (node.get("depends_on") or {}).get("nodes", []),
                    "columns": list((node.get("columns") or {}).values()),
                }
            )
        return {"manifest": manifest, "models": models, "runs": runs}


class PostgresAdapter(BaseAdapter):
    slug = "postgres"

    @staticmethod
    def _build_url(credentials: dict[str, Any]) -> str | None:
        url = credentials.get("database_url")
        if url:
            return str(url).replace("postgresql+asyncpg://", "postgresql+psycopg://", 1)
        if all(credentials.get(key) for key in ["host", "database", "user", "password"]):
            host = credentials["host"]
            port = credentials.get("port") or "5432"
            user = quote_plus(credentials["user"])
            password = quote_plus(credentials["password"])
            database = credentials["database"]
            return f"postgresql+psycopg://{user}:{password}@{host}:{port}/{database}"
        return None

    @staticmethod
    def _engine_kwargs(credentials: dict[str, Any]) -> dict[str, Any]:
        kwargs: dict[str, Any] = {"pool_pre_ping": True}
        schema = str(credentials.get("default_schema") or "").strip()
        if schema:
            kwargs["connect_args"] = {"options": f"-c search_path={schema}"}
        return kwargs

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        settings = get_settings()
        url = self._build_url(credentials)
        mode = VerificationMode.REAL if url else VerificationMode.DEMO
        has_partial_fields = any(
            credentials.get(key) for key in ["host", "database", "user", "password", "port"]
        )
        if not url and has_partial_fields:
            return TestResult(
                slug=self.slug,
                status="credential_required",
                mode=VerificationMode.CREDENTIAL_REQUIRED,
                message="PostgreSQL requires database_url or host, database, user, and password.",
            )
        if not url:
            url = settings.demo_database_url
        engine = create_async_engine(url, **self._engine_kwargs(credentials))
        try:
            async with engine.connect() as conn:
                value = await conn.scalar(text("select 1"))
            return TestResult(
                slug=self.slug,
                status="ok",
                mode=mode,
                message="PostgreSQL connection succeeded.",
                details={
                    "select_1": value,
                    "label": "Real" if mode == VerificationMode.REAL else "Demo",
                },
            )
        except Exception as exc:  # pragma: no cover - exact driver errors vary
            return _failed_result(self.slug, mode, exc)
        finally:
            await engine.dispose()

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        settings = get_settings()
        url = self._build_url(credentials) or settings.demo_database_url
        engine = create_async_engine(url, **self._engine_kwargs(credentials))
        try:
            async with engine.connect() as conn:
                rows = (
                    (
                        await conn.execute(
                            text(
                                "select table_schema, table_name "
                                "from information_schema.tables "
                                "where table_schema not in ('pg_catalog', 'information_schema') "
                                "order by table_schema, table_name"
                            )
                        )
                    )
                    .mappings()
                    .all()
                )
                tables = []
                for row in rows:
                    schema = row["table_schema"]
                    name = row["table_name"]
                    columns_result = (
                        (
                            await conn.execute(
                                text(
                                    "select column_name, data_type "
                                    "from information_schema.columns "
                                    "where table_schema = :schema and table_name = :table "
                                    "order by ordinal_position"
                                ),
                                {"schema": schema, "table": name},
                            )
                        )
                        .mappings()
                        .all()
                    )
                    count = await conn.scalar(text(f'select count(*) from "{schema}"."{name}"'))
                    tables.append(
                        {
                            "name": name,
                            "schema": schema,
                            "row_count": int(count or 0),
                            "columns": [
                                {
                                    "name": col["column_name"],
                                    "type": col["data_type"],
                                    "description": "",
                                }
                                for col in columns_result
                            ],
                        }
                    )
            return {
                "mode": "real",
                "objects_synced": len(tables),
                "tables": tables,
                "summary": "Synced PostgreSQL INFORMATION_SCHEMA tables from a live connection.",
                "source_type": "postgres",
                "schema_name": tables[0]["schema"] if tables else "public",
            }
        finally:
            await engine.dispose()


class OpenAIAdapter(BaseAdapter):
    slug = "openai"

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        api_key = credentials.get("api_key")
        model = credentials.get("model") or get_settings().openai_model
        if not api_key:
            return TestResult(
                slug=self.slug,
                status="not_configured",
                mode=VerificationMode.NOT_CONFIGURED,
                message="OPENAI_API_KEY is not configured.",
            )
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                response = await client.get(
                    "https://api.openai.com/v1/models",
                    headers={"Authorization": f"Bearer {api_key}"},
                )
                response.raise_for_status()
            return TestResult(
                slug=self.slug,
                status="ok",
                mode=VerificationMode.REAL,
                message=f"OpenAI API key is valid; configured model is {model}.",
                details={"model": model},
            )
        except Exception as exc:  # pragma: no cover - depends on live OpenAI credentials
            return _failed_result(self.slug, VerificationMode.REAL, exc)


def seed_sqlite_demo(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with sqlite3.connect(path) as conn:
        cursor = conn.cursor()
        cursor.executescript(
            """
            create table if not exists customers (
              customer_id text primary key,
              segment text not null,
              arr real not null
            );
            create table if not exists products (
              product_id text primary key,
              family text not null,
              gross_margin real not null
            );
            create table if not exists orders (
              order_id text primary key,
              customer_id text not null references customers(customer_id),
              product_id text not null references products(product_id),
              net_revenue real not null,
              ordered_at text not null default current_timestamp
            );
            create table if not exists test_summary (
              id integer primary key,
              note text not null
            );
            """
        )
        cursor.execute("select count(*) from customers")
        if cursor.fetchone()[0] == 0:
            cursor.executemany(
                "insert into customers values (?, ?, ?)",
                [
                    ("demo-acme", "Enterprise", 248000.0),
                    ("demo-globex", "Mid-Market", 196500.0),
                    ("demo-initech", "Commercial", 121900.0),
                ],
            )
            cursor.executemany(
                "insert into products values (?, ?, ?)",
                [
                    ("platform", "Core Platform", 0.82),
                    ("governance", "Governance", 0.76),
                    ("connectors", "Connectivity", 0.69),
                ],
            )
            cursor.executemany(
                "insert into orders (order_id, customer_id, product_id, net_revenue) values (?, ?, ?, ?)",
                [
                    ("ord-001", "demo-acme", "platform", 148000.0),
                    ("ord-002", "demo-acme", "governance", 100000.0),
                    ("ord-003", "demo-globex", "platform", 196500.0),
                    ("ord-004", "demo-initech", "connectors", 121900.0),
                ],
            )
        cursor.execute(
            "insert or replace into test_summary (id, note) values (1, 'destructive approval fixture')"
        )
        conn.commit()


def _resolve_sqlite_path(credentials: dict[str, Any]) -> Path:
    raw = credentials.get("database_path")
    if raw:
        return Path(str(raw)).expanduser()
    seed_sqlite_demo(DEMO_SQLITE_PATH)
    return DEMO_SQLITE_PATH


class SQLiteAdapter(BaseAdapter):
    slug = "sqlite"

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        path = _resolve_sqlite_path(credentials)

        def run_select() -> dict[str, Any]:
            with sqlite3.connect(path) as conn:
                cursor = conn.cursor()
                cursor.execute("select 1")
                value = cursor.fetchone()[0]
                cursor.execute(
                    "select name from sqlite_master where type = 'table' and name not like 'sqlite_%'"
                )
                tables = [row[0] for row in cursor.fetchall()]
            return {"select_1": value, "tables": tables}

        try:
            details = await asyncio.to_thread(run_select)
            return TestResult(
                slug=self.slug,
                status="ok",
                mode=VerificationMode.REAL,
                message=f"SQLite database at {path} is reachable; {len(details['tables'])} tables visible.",
                details={**details, "label": "Real", "database_path": str(path)},
            )
        except Exception as exc:
            return _failed_result(self.slug, VerificationMode.REAL, exc)

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        path = _resolve_sqlite_path(credentials)

        def introspect() -> list[dict[str, Any]]:
            tables: list[dict[str, Any]] = []
            with sqlite3.connect(path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "select name from sqlite_master where type = 'table' and name not like 'sqlite_%' order by name"
                )
                names = [row[0] for row in cursor.fetchall()]
                for name in names:
                    cursor.execute(f"pragma table_info('{name}')")
                    columns = [
                        {"name": row[1], "type": (row[2] or "text").lower(), "description": ""}
                        for row in cursor.fetchall()
                    ]
                    cursor.execute(f"select count(*) from '{name}'")
                    row_count = int(cursor.fetchone()[0])
                    tables.append(
                        {
                            "name": name,
                            "columns": columns,
                            "row_count": row_count,
                            "schema": "main",
                        }
                    )
            return tables

        tables = await asyncio.to_thread(introspect)
        return {
            "mode": "real",
            "objects_synced": len(tables),
            "tables": tables,
            "summary": f"Synced {len(tables)} SQLite tables from {path}.",
            "database_path": str(path),
            "source_type": "sqlite",
            "schema_name": "main",
        }


class RedshiftAdapter(BaseAdapter):
    """Amazon Redshift speaks the Postgres wire protocol but is forked from
    Postgres 8.0, so SQLAlchemy's modern psycopg dialect probes parameters
    (`standard_conforming_strings`, etc.) that Redshift does not expose. We
    use raw psycopg in a thread to bypass dialect introspection entirely.
    """

    slug = "redshift"

    @staticmethod
    def _connect_kwargs(credentials: dict[str, Any]) -> dict[str, Any] | None:
        endpoint = str(credentials.get("cluster_endpoint") or credentials.get("host") or "")
        if not endpoint or not all(credentials.get(k) for k in ("database", "user", "password")):
            return None
        host, endpoint_port, _ = parse_redshift_endpoint(
            endpoint, str(credentials.get("port") or "5439")
        )
        port = int(credentials.get("port") or endpoint_port)
        return {
            "host": host,
            "port": port,
            "dbname": credentials["database"],
            "user": credentials["user"],
            "password": credentials["password"],
            "connect_timeout": 10,
        }

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        kwargs = self._connect_kwargs(credentials)
        if kwargs is None:
            return TestResult(
                slug=self.slug,
                status="credential_required",
                mode=VerificationMode.CREDENTIAL_REQUIRED,
                message="Redshift requires cluster_endpoint, database, user, and password.",
            )

        def run_check() -> int:
            import psycopg

            with psycopg.connect(**kwargs) as conn:
                with conn.cursor() as cur:
                    cur.execute("select 1")
                    row = cur.fetchone() or (0,)
                    return int(row[0])

        try:
            value = await asyncio.to_thread(run_check)
        except Exception as exc:  # pragma: no cover - driver errors vary
            return _failed_result(self.slug, VerificationMode.REAL, exc)
        return TestResult(
            slug=self.slug,
            status="ok",
            mode=VerificationMode.REAL,
            message="Amazon Redshift connection succeeded.",
            details={"select_1": value, "label": "Real"},
        )

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        kwargs = self._connect_kwargs(credentials)
        if kwargs is None:
            return {
                "mode": "real",
                "objects_synced": 0,
                "tables": [],
                "summary": "No credentials configured.",
            }

        def load_tables() -> list[dict[str, Any]]:
            import psycopg

            with psycopg.connect(**kwargs) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        "select table_schema, table_name "
                        "from information_schema.tables "
                        "where table_schema not in ("
                        "  'pg_catalog', 'information_schema', 'pg_internal',"
                        "  'pg_auto_copy', 'pg_automv', 'catalog_history'"
                        ") "
                        "and table_schema not like 'pg!_%' escape '!' "
                        "order by table_schema, table_name"
                    )
                    rows = cur.fetchall()
                    out: list[dict[str, Any]] = []
                    for schema, name in rows:
                        try:
                            cur.execute(
                                "select column_name, data_type from information_schema.columns "
                                "where table_schema = %s and table_name = %s order by ordinal_position",
                                (schema, name),
                            )
                            columns = [
                                {"name": c, "type": t, "description": ""} for c, t in cur.fetchall()
                            ]
                            cur.execute(f'select count(*) from "{schema}"."{name}"')
                            count_row = cur.fetchone() or (0,)
                            row_count = int(count_row[0] or 0)
                        except psycopg.errors.InsufficientPrivilege:
                            conn.rollback()
                            continue
                        out.append(
                            {
                                "name": name,
                                "schema": schema,
                                "row_count": row_count,
                                "columns": columns,
                            }
                        )
                    return out

        tables = await asyncio.to_thread(load_tables)
        return {
            "mode": "real",
            "objects_synced": len(tables),
            "tables": tables,
            "summary": f"Synced {len(tables)} Redshift tables from INFORMATION_SCHEMA.",
            "source_type": "redshift",
            "schema_name": tables[0]["schema"] if tables else "public",
        }


def _missing_module_result(slug: str, module: str, install_extra: str) -> TestResult:
    return TestResult(
        slug=slug,
        status="failed",
        mode=VerificationMode.REAL,
        message=(
            f"Optional driver `{module}` is not installed. "
            f"Run `uv pip install dataclaw-platform[{install_extra}]` (or pip equivalent) and retry."
        ),
    )


class SnowflakeAdapter(BaseAdapter):
    slug = "snowflake"

    def _connect(self, credentials: dict[str, Any]):
        import snowflake.connector  # type: ignore[import-not-found]

        kwargs: dict[str, Any] = {
            "account": normalize_snowflake_account(str(credentials["account"])),
            "user": credentials["user"],
            "warehouse": credentials.get("warehouse"),
            "database": credentials.get("database"),
            "schema": credentials.get("schema"),
            "login_timeout": 15,
            "network_timeout": 20,
        }
        if credentials.get("private_key"):
            from cryptography.hazmat.primitives import serialization

            key = serialization.load_pem_private_key(
                str(credentials["private_key"]).encode(),
                password=str(credentials.get("private_key_passphrase") or "").encode() or None,
            )
            kwargs["private_key"] = key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
        else:
            kwargs["password"] = credentials["password"]
        return snowflake.connector.connect(**{key: value for key, value in kwargs.items() if value})

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        if result := _required_result(self.slug, self.definition(), credentials):
            return result
        if not credentials.get("password") and not credentials.get("private_key"):
            return TestResult(
                slug=self.slug,
                status="credential_required",
                mode=VerificationMode.CREDENTIAL_REQUIRED,
                message="Missing required credentials: password or private_key.",
            )

        def run_check() -> int:
            with self._connect(credentials) as conn:
                with conn.cursor() as cur:
                    cur.execute("select 1")
                    row = cur.fetchone()
                    return int(row[0])

        try:
            value = await asyncio.to_thread(run_check)
        except ModuleNotFoundError:
            return _missing_module_result(self.slug, "snowflake.connector", "snowflake")
        except Exception as exc:
            return _failed_result(self.slug, VerificationMode.REAL, exc)
        return TestResult(
            slug=self.slug,
            status="ok",
            mode=VerificationMode.REAL,
            message="Snowflake connection succeeded.",
            details={"select_1": value, "label": "Real"},
        )

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        def introspect() -> list[dict[str, Any]]:
            tables: list[dict[str, Any]] = []
            with self._connect(credentials) as conn:
                with conn.cursor() as cur:
                    schema = credentials.get("schema") or "PUBLIC"
                    database = credentials.get("database")
                    if database:
                        cur.execute(f'use database "{database}"')
                    information_schema = (
                        f'"{database}".information_schema' if database else "information_schema"
                    )
                    cur.execute(
                        f"select table_schema, table_name from {information_schema}.tables "
                        "where table_schema = %s order by table_name",
                        (schema,),
                    )
                    rows = cur.fetchall()
                    for table_schema, table_name in rows:
                        cur.execute(
                            f"select column_name, data_type from {information_schema}.columns "
                            "where table_schema = %s and table_name = %s order by ordinal_position",
                            (table_schema, table_name),
                        )
                        cols = [
                            {
                                "name": c[0],
                                "type": c[1].lower() if c[1] else "unknown",
                                "description": "",
                            }
                            for c in cur.fetchall()
                        ]
                        table_ref = (
                            f'"{database}"."{table_schema}"."{table_name}"'
                            if database
                            else f'"{table_schema}"."{table_name}"'
                        )
                        cur.execute(f"select count(*) from {table_ref}")
                        count = int(cur.fetchone()[0])
                        tables.append(
                            {
                                "name": table_name,
                                "schema": table_schema,
                                "row_count": count,
                                "columns": cols,
                            }
                        )
            return tables

        try:
            tables = await asyncio.to_thread(introspect)
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "snowflake.connector not installed; run `pip install dataclaw-platform[snowflake]`."
            ) from exc
        return {
            "mode": "real",
            "objects_synced": len(tables),
            "tables": tables,
            "summary": f"Synced {len(tables)} Snowflake tables.",
            "source_type": "snowflake",
            "schema_name": credentials.get("schema") or "PUBLIC",
        }

    async def fetch_content(self, credentials: dict[str, Any]) -> dict[str, Any]:
        return await self.sync(credentials)


class BigQueryAdapter(BaseAdapter):
    slug = "bigquery"

    def _client(self, credentials: dict[str, Any]):
        import json as _json

        from google.cloud import bigquery  # type: ignore[import-not-found]

        emulator_host = str(credentials.get("emulator_host") or "").strip().rstrip("/")
        if emulator_host:
            from google.auth.credentials import (
                AnonymousCredentials,  # type: ignore[import-not-found]
            )

            project = credentials.get("project_id") or "dataclaw-integration"
            return bigquery.Client(
                project=project,
                credentials=AnonymousCredentials(),
                client_options={"api_endpoint": emulator_host},
            )

        from google.oauth2 import service_account  # type: ignore[import-not-found]

        info = credentials["service_account_json"]
        if isinstance(info, str):
            info = _json.loads(info)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(
            project=credentials.get("project_id") or info.get("project_id"), credentials=creds
        )

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        if not str(credentials.get("emulator_host") or "").strip():
            if result := _required_result(self.slug, self.definition(), credentials):
                return result

        def run_check() -> int:
            client = self._client(credentials)
            job = client.query("select 1")
            row = next(iter(job.result()))
            return int(row[0])

        try:
            value = await asyncio.to_thread(run_check)
        except ModuleNotFoundError:
            return _missing_module_result(self.slug, "google.cloud.bigquery", "bigquery")
        except Exception as exc:
            return _failed_result(self.slug, VerificationMode.REAL, exc)
        return TestResult(
            slug=self.slug,
            status="ok",
            mode=VerificationMode.REAL,
            message="BigQuery connection succeeded.",
            details={"select_1": value, "label": "Real"},
        )

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        def introspect() -> list[dict[str, Any]]:
            client = self._client(credentials)
            tables: list[dict[str, Any]] = []
            for dataset in client.list_datasets():
                for table_ref in client.list_tables(dataset.dataset_id):
                    table = client.get_table(table_ref)
                    tables.append(
                        {
                            "name": table.table_id,
                            "schema": dataset.dataset_id,
                            "row_count": int(table.num_rows or 0),
                            "columns": [
                                {
                                    "name": f.name,
                                    "type": f.field_type.lower(),
                                    "description": f.description or "",
                                }
                                for f in table.schema
                            ],
                        }
                    )
            return tables

        try:
            tables = await asyncio.to_thread(introspect)
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "google-cloud-bigquery not installed; run `pip install dataclaw-platform[bigquery]`."
            ) from exc
        return {
            "mode": "real",
            "objects_synced": len(tables),
            "tables": tables,
            "summary": f"Synced {len(tables)} BigQuery tables.",
            "source_type": "bigquery",
            "schema_name": tables[0]["schema"] if tables else "default",
        }

    async def fetch_content(self, credentials: dict[str, Any]) -> dict[str, Any]:
        return await self.sync(credentials)


class DatabricksAdapter(SaaSHTTPAdapter):
    """Real adapter on top of SaaSHTTPAdapter, with full SQL warehouse listing + jobs fetch."""

    def __init__(self) -> None:
        self.slug = "databricks"

    async def fetch_content(self, credentials: dict[str, Any]) -> dict[str, Any]:
        async with httpx.AsyncClient(timeout=30) as client:
            warehouses_resp = await client.get(
                f"{self.base_url(credentials)}/sql/warehouses",
                headers=self.headers(credentials),
            )
            warehouses = (
                warehouses_resp.json().get("warehouses")
                if warehouses_resp.status_code < 400
                else []
            )
            jobs_resp = await client.get(
                f"{self.base_url(credentials)}/jobs/list",
                headers=self.headers(credentials),
            )
            jobs = jobs_resp.json().get("jobs") if jobs_resp.status_code < 400 else []
        return {"warehouses": warehouses or [], "jobs": jobs or []}


class GoogleDocsAdapter(BaseAdapter):
    slug = "google_docs"

    def _services(self, credentials: dict[str, Any]):
        import json as _json

        from google.oauth2 import service_account  # type: ignore[import-not-found]
        from googleapiclient.discovery import build  # type: ignore[import-not-found]

        info = credentials["service_account_json"]
        if isinstance(info, str):
            info = _json.loads(info)
        scopes = [
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/documents",
        ]
        creds = service_account.Credentials.from_service_account_info(info, scopes=scopes)
        drive = build("drive", "v3", credentials=creds, cache_discovery=False)
        docs = build("docs", "v1", credentials=creds, cache_discovery=False)
        return drive, docs

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        if result := _required_result(self.slug, self.definition(), credentials):
            return result

        def run_check() -> int:
            drive, _ = self._services(credentials)
            response = (
                drive.files()
                .list(pageSize=1, q="mimeType='application/vnd.google-apps.document'")
                .execute()
            )
            return len(response.get("files", []))

        try:
            count = await asyncio.to_thread(run_check)
        except ModuleNotFoundError:
            return _missing_module_result(self.slug, "googleapiclient", "google")
        except Exception as exc:
            return _failed_result(self.slug, VerificationMode.REAL, exc)
        return TestResult(
            slug=self.slug,
            status="ok",
            mode=VerificationMode.REAL,
            message="Google Docs / Drive credentials accepted.",
            details={"sample_count": count, "label": "Real"},
        )

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        def list_docs() -> list[dict[str, Any]]:
            drive, _ = self._services(credentials)
            files: list[dict[str, Any]] = []
            page_token: str | None = None
            while True:
                response = (
                    drive.files()
                    .list(
                        q="mimeType='application/vnd.google-apps.document'",
                        pageSize=100,
                        fields="nextPageToken, files(id, name, modifiedTime, webViewLink)",
                        pageToken=page_token,
                    )
                    .execute()
                )
                files.extend(response.get("files", []))
                page_token = response.get("nextPageToken")
                if not page_token:
                    break
            return files

        try:
            docs = await asyncio.to_thread(list_docs)
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "google-api-python-client not installed; run `pip install dataclaw-platform[google]`."
            ) from exc
        return {
            "mode": "real",
            "objects_synced": len(docs),
            "docs": docs,
            "summary": f"Listed {len(docs)} Google Docs documents.",
            "source_type": "google_docs",
            "schema_name": "drive",
        }

    async def fetch_content(self, credentials: dict[str, Any]) -> dict[str, Any]:
        def fetch_bodies() -> list[dict[str, Any]]:
            drive, docs = self._services(credentials)
            files = (
                drive.files()
                .list(
                    q="mimeType='application/vnd.google-apps.document'",
                    pageSize=50,
                    fields="files(id, name, modifiedTime, webViewLink)",
                )
                .execute()
                .get("files", [])
            )
            results: list[dict[str, Any]] = []
            for f in files:
                doc = docs.documents().get(documentId=f["id"]).execute()
                body_chunks = []
                for el in doc.get("body", {}).get("content", []):
                    para = el.get("paragraph")
                    if not para:
                        continue
                    for run in para.get("elements", []):
                        text_run = run.get("textRun")
                        if text_run and text_run.get("content"):
                            body_chunks.append(text_run["content"])
                results.append(
                    {
                        "id": f["id"],
                        "title": f["name"],
                        "url": f.get("webViewLink"),
                        "modified": f.get("modifiedTime"),
                        "body": "".join(body_chunks),
                    }
                )
            return results

        try:
            documents = await asyncio.to_thread(fetch_bodies)
        except ModuleNotFoundError as exc:
            raise RuntimeError(
                "google-api-python-client not installed; run `pip install dataclaw-platform[google]`."
            ) from exc
        return {"documents": documents}


# Exceptions worth retrying. Permanent failures (auth, bad creds, validation
# errors raised as ConnectorAdapterError subclasses except the two transient
# ones below) are deliberately excluded so we don't paper over real problems.
_TRANSIENT_EXCEPTIONS: tuple[type[BaseException], ...] = (
    AdapterReachabilityError,
    AdapterRateLimitError,
    httpx.TimeoutException,
    httpx.TransportError,
    ConnectionError,
    TimeoutError,
)

_retry_logger = logging.getLogger("dataclaw.connectors.retry")


async def _run_with_backoff(
    func,
    *,
    slug: str,
    op: str,
    attempts: int,
    base_delay: float,
):
    """Call an async fn with exponential backoff on transient errors.

    Permanent errors propagate on the first attempt. Each retry logs with the
    connector slug so failures stay traceable in production logs.
    """
    last_exc: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return await func()
        except _TRANSIENT_EXCEPTIONS as exc:
            last_exc = exc
            if attempt >= attempts:
                break
            delay = base_delay * (3 ** (attempt - 1))
            _retry_logger.warning(
                "connector_op_retrying",
                extra={
                    "_slug": slug,
                    "_op": op,
                    "_attempt": attempt,
                    "_next_delay_s": delay,
                    "_error": exc.__class__.__name__,
                },
            )
            await asyncio.sleep(delay)
    assert last_exc is not None
    raise last_exc


class _RetryingAdapter:
    """Decorator over a ConnectorAdapter that retries transient failures.

    test() is cheap so we attempt more times with short delays; sync() is
    expensive so we cap at two attempts to avoid amplifying load on a hot
    upstream.
    """

    __slots__ = ("_inner", "_slug")

    def __init__(self, inner, slug: str) -> None:
        self._inner = inner
        self._slug = slug

    async def test(self, credentials: dict[str, Any]) -> TestResult:
        return await _run_with_backoff(
            lambda: self._inner.test(credentials),
            slug=self._slug,
            op="test",
            attempts=3,
            base_delay=0.5,
        )

    async def sync(self, credentials: dict[str, Any]) -> dict[str, Any]:
        return await _run_with_backoff(
            lambda: self._inner.sync(credentials),
            slug=self._slug,
            op="sync",
            attempts=2,
            base_delay=5.0,
        )

    async def list_failed_runs(
        self, credentials: dict[str, Any], since: datetime | None = None
    ) -> list[dict[str, Any]]:
        # Failure-listing is read-only and infrequent; retry like test().
        return await _run_with_backoff(
            lambda: self._inner.list_failed_runs(credentials, since=since),
            slug=self._slug,
            op="list_failed_runs",
            attempts=3,
            base_delay=0.5,
        )

    def __getattr__(self, name: str):
        # Forward adapter-specific helpers (e.g. ConfluenceAdapter.fetch_content)
        # to the inner instance without retry, so callers that depend on
        # adapter-specific methods continue to work transparently.
        return getattr(self._inner, name)


def _build_adapter(slug: str) -> ConnectorAdapter:
    if slug == "sqlite":
        return SQLiteAdapter()
    if slug == "postgres":
        return PostgresAdapter()
    if slug == "redshift":
        return RedshiftAdapter()
    if slug == "snowflake":
        return SnowflakeAdapter()
    if slug == "bigquery":
        return BigQueryAdapter()
    if slug == "databricks":
        return DatabricksAdapter()
    if slug == "google_docs":
        return GoogleDocsAdapter()
    if slug == "openai":
        return OpenAIAdapter()
    if slug == "mysql":
        return MySQLAdapter()
    if slug == "sql_server":
        return SQLServerAdapter()
    if slug == "trino":
        return TrinoAdapter()
    if slug == "airflow":
        return AirflowAdapter(slug)
    if slug == "airbyte":
        return AirbyteAdapter(slug)
    if slug == "prefect":
        return PrefectAdapter(slug)
    if slug == "dagster":
        return DagsterAdapter(slug)
    if slug == "github":
        return GitHubAdapter()
    if slug == "notion":
        return NotionAdapter(slug)
    if slug == "dbt":
        return DbtAdapter()
    if slug == "confluence":
        return ConfluenceAdapter()
    if slug in {"quip", "fivetran"}:
        return SaaSHTTPAdapter(slug)
    return CredentialRequiredAdapter(slug)


def adapter_for(slug: str) -> ConnectorAdapter:
    return _RetryingAdapter(_build_adapter(slug), slug)
