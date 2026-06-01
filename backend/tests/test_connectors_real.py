"""Cloud connector adapters: factory returns dedicated classes (not stubs)."""
from __future__ import annotations

import sys
import types
from unittest.mock import Mock

import pytest

from app.services.connectors.adapters import (
    BigQueryAdapter,
    CredentialRequiredAdapter,
    DatabricksAdapter,
    GoogleDocsAdapter,
    RedshiftAdapter,
    SnowflakeAdapter,
    TrinoAdapter,
    adapter_for,
    normalize_snowflake_account,
    parse_redshift_endpoint,
)


def _underlying(adapter):
    # adapter_for() wraps every adapter in _RetryingAdapter; tests check the
    # concrete implementation behind the wrapper.
    return getattr(adapter, "_inner", adapter)


@pytest.mark.parametrize(
    "slug, expected",
    [
        ("snowflake", SnowflakeAdapter),
        ("bigquery", BigQueryAdapter),
        ("databricks", DatabricksAdapter),
        ("redshift", RedshiftAdapter),
        ("google_docs", GoogleDocsAdapter),
        ("trino", TrinoAdapter),
    ],
)
def test_factory_returns_real_adapter(slug: str, expected: type) -> None:
    adapter = _underlying(adapter_for(slug))
    assert isinstance(adapter, expected), f"{slug} resolved to {type(adapter).__name__}"
    assert not isinstance(adapter, CredentialRequiredAdapter)


def test_no_catalog_slug_falls_through_to_credential_required() -> None:
    from app.services.connectors.catalog import CATALOG_BY_SLUG

    fallthroughs: list[str] = []
    for slug in CATALOG_BY_SLUG:
        adapter = _underlying(adapter_for(slug))
        if isinstance(adapter, CredentialRequiredAdapter) and slug not in {"quip", "confluence", "fivetran"}:
            fallthroughs.append(slug)
    assert fallthroughs == [], f"Catalog entries with no real adapter: {fallthroughs}"


def test_bigquery_adapter_uses_emulator_host_with_anonymous_credentials(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    class FakeClient:
        def __init__(self, **kwargs):
            seen.update(kwargs)

    google_module = types.ModuleType("google")
    auth_module = types.ModuleType("google.auth")
    auth_credentials_module = types.ModuleType("google.auth.credentials")
    auth_credentials_module.AnonymousCredentials = lambda: {"anonymous": True}
    cloud_module = types.ModuleType("google.cloud")
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    bigquery_module.Client = FakeClient
    cloud_module.bigquery = bigquery_module
    auth_module.credentials = auth_credentials_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.auth", auth_module)
    monkeypatch.setitem(sys.modules, "google.auth.credentials", auth_credentials_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery_module)

    client = BigQueryAdapter()._client(
        {
            "project_id": "dataclaw-integration",
            "emulator_host": "http://127.0.0.1:19050/",
        }
    )

    assert isinstance(client, FakeClient)
    assert seen == {
        "project": "dataclaw-integration",
        "credentials": {"anonymous": True},
        "client_options": {"api_endpoint": "http://127.0.0.1:19050"},
    }


@pytest.mark.asyncio
async def test_bigquery_adapter_test_allows_emulator_without_service_account(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeQueryJob:
        def result(self):
            return [(1,)]

    class FakeClient:
        def __init__(self, **kwargs):
            pass

        def query(self, sql: str):
            assert sql == "select 1"
            return FakeQueryJob()

    google_module = types.ModuleType("google")
    auth_module = types.ModuleType("google.auth")
    auth_credentials_module = types.ModuleType("google.auth.credentials")
    auth_credentials_module.AnonymousCredentials = lambda: {"anonymous": True}
    cloud_module = types.ModuleType("google.cloud")
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    bigquery_module.Client = FakeClient
    cloud_module.bigquery = bigquery_module
    auth_module.credentials = auth_credentials_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.auth", auth_module)
    monkeypatch.setitem(sys.modules, "google.auth.credentials", auth_credentials_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery_module)

    result = await BigQueryAdapter().test(
        {
            "project_id": "dataclaw-integration",
            "emulator_host": "http://127.0.0.1:19050/",
        }
    )

    assert result.status == "ok"


def test_trino_adapter_connects_with_basic_auth_when_password_present(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    class FakeAuth:
        def __init__(self, user: str, password: str) -> None:
            self.user = user
            self.password = password

    class FakeDbapi:
        @staticmethod
        def connect(**kwargs):
            seen.update(kwargs)
            return object()

    trino_module = types.ModuleType("trino")
    trino_module.dbapi = FakeDbapi
    auth_module = types.ModuleType("trino.auth")
    auth_module.BasicAuthentication = FakeAuth

    monkeypatch.setitem(sys.modules, "trino", trino_module)
    monkeypatch.setitem(sys.modules, "trino.auth", auth_module)

    conn = TrinoAdapter()._connect_sync(
        {
            "host": "127.0.0.1",
            "catalog": "memory",
            "schema": "core",
            "user": "data user",
            "password": "secret",
        }
    )

    assert conn is not None
    assert seen["host"] == "127.0.0.1"
    assert seen["port"] == 8080
    assert seen["catalog"] == "memory"
    assert seen["schema"] == "core"
    assert seen["user"] == "data user"
    assert seen["http_scheme"] == "https"
    assert isinstance(seen["auth"], FakeAuth)


def test_snowflake_account_normalization_accepts_login_url() -> None:
    assert normalize_snowflake_account("https://wadmyyq-mdb74768.snowflakecomputing.com/") == "wadmyyq-mdb74768"
    assert normalize_snowflake_account("org-account.us-east-1.aws.snowflakecomputing.com") == "org-account.us-east-1.aws"


def test_redshift_endpoint_parser_accepts_database_path() -> None:
    assert parse_redshift_endpoint("default-workgroup.example.us-east-1.redshift-serverless.amazonaws.com:5439/dev") == (
        "default-workgroup.example.us-east-1.redshift-serverless.amazonaws.com",
        "5439",
        "dev",
    )


def test_snowflake_adapter_accepts_private_key_without_password(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: dict[str, object] = {}

    class FakeSnowflakeConnector:
        @staticmethod
        def connect(**kwargs):
            seen.update(kwargs)
            return object()

    snowflake_module = types.ModuleType("snowflake")
    connector_module = types.ModuleType("snowflake.connector")
    connector_module.connect = FakeSnowflakeConnector.connect
    snowflake_module.connector = connector_module

    key = Mock()
    key.private_bytes.return_value = b"der-private-key"

    serialization_module = types.ModuleType("cryptography.hazmat.primitives.serialization")
    serialization_module.Encoding = types.SimpleNamespace(DER="DER")
    serialization_module.PrivateFormat = types.SimpleNamespace(PKCS8="PKCS8")
    serialization_module.NoEncryption = lambda: "no-encryption"
    serialization_module.load_pem_private_key = Mock(return_value=key)

    monkeypatch.setitem(sys.modules, "snowflake", snowflake_module)
    monkeypatch.setitem(sys.modules, "snowflake.connector", connector_module)
    monkeypatch.setitem(sys.modules, "cryptography.hazmat.primitives.serialization", serialization_module)
    # The adapter does `from cryptography.hazmat.primitives import serialization`,
    # which is an attribute lookup on the parent. If a prior test in the suite
    # already imported the real `cryptography.hazmat.primitives` package, the
    # parent's `serialization` attribute is bound to the real submodule and
    # `monkeypatch.setitem(sys.modules, ...)` alone won't redirect the import.
    # Patch the parent's attribute too when it's already loaded.
    _primitives = sys.modules.get("cryptography.hazmat.primitives")
    if _primitives is not None:
        monkeypatch.setattr(_primitives, "serialization", serialization_module, raising=False)

    conn = SnowflakeAdapter()._connect(
        {
            "account": "acct",
            "user": "dataclaw",
            "private_key": "-----BEGIN PRIVATE KEY-----\\nkey\\n-----END PRIVATE KEY-----",
            "warehouse": "COMPUTE_WH",
            "database": "ACME",
            "schema": "MARTS",
        }
    )

    assert conn is not None
    assert seen["account"] == "acct"
    assert seen["user"] == "dataclaw"
    assert seen["private_key"] == b"der-private-key"
    assert "password" not in seen


@pytest.mark.asyncio
async def test_snowflake_adapter_test_accepts_private_key_without_password(monkeypatch: pytest.MonkeyPatch) -> None:
    class FakeCursor:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def execute(self, sql: str) -> None:
            assert sql == "select 1"

        def fetchone(self):
            return (1,)

    class FakeConnection:
        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def cursor(self):
            return FakeCursor()

    snowflake_module = types.ModuleType("snowflake")
    connector_module = types.ModuleType("snowflake.connector")
    connector_module.connect = Mock(return_value=FakeConnection())
    snowflake_module.connector = connector_module

    key = Mock()
    key.private_bytes.return_value = b"der-private-key"

    serialization_module = types.ModuleType("cryptography.hazmat.primitives.serialization")
    serialization_module.Encoding = types.SimpleNamespace(DER="DER")
    serialization_module.PrivateFormat = types.SimpleNamespace(PKCS8="PKCS8")
    serialization_module.NoEncryption = lambda: "no-encryption"
    serialization_module.load_pem_private_key = Mock(return_value=key)

    monkeypatch.setitem(sys.modules, "snowflake", snowflake_module)
    monkeypatch.setitem(sys.modules, "snowflake.connector", connector_module)
    monkeypatch.setitem(sys.modules, "cryptography.hazmat.primitives.serialization", serialization_module)
    # The adapter does `from cryptography.hazmat.primitives import serialization`,
    # which is an attribute lookup on the parent. If a prior test in the suite
    # already imported the real `cryptography.hazmat.primitives` package, the
    # parent's `serialization` attribute is bound to the real submodule and
    # `monkeypatch.setitem(sys.modules, ...)` alone won't redirect the import.
    # Patch the parent's attribute too when it's already loaded.
    _primitives = sys.modules.get("cryptography.hazmat.primitives")
    if _primitives is not None:
        monkeypatch.setattr(_primitives, "serialization", serialization_module, raising=False)

    result = await SnowflakeAdapter().test(
        {
            "account": "acct",
            "warehouse": "COMPUTE_WH",
            "database": "ACME",
            "schema": "MARTS",
            "user": "dataclaw",
            "private_key": "-----BEGIN PRIVATE KEY-----\\nkey\\n-----END PRIVATE KEY-----",
        }
    )

    assert result.status == "ok"
