"""Mock-based unit tests for SaaS connector adapters.

These exercise the HTTP request shape (URL, headers, body) the adapters
produce, without requiring real credentials. They guard against regressions
in the adapter's wire format and run on every `make test`.
"""
from __future__ import annotations

import httpx
import pytest
import respx

from app.services.connectors.adapters import adapter_for


@respx.mock
@pytest.mark.asyncio
async def test_github_test_calls_user_endpoint_with_bearer() -> None:
    route = respx.get("https://api.github.com/user").mock(
        return_value=httpx.Response(200, json={"login": "octocat"})
    )
    result = await adapter_for("github").test(
        {"token": "ghp_test", "repositories": "octocat/hello-world"}
    )
    assert result.status == "ok"
    request = route.calls.last.request
    assert request.headers["Authorization"] == "Bearer ghp_test"
    assert "vnd.github+json" in request.headers["Accept"]


@respx.mock
@pytest.mark.asyncio
async def test_github_sync_fetches_repo_metadata() -> None:
    respx.get("https://api.github.com/repos/octocat/hello-world").mock(
        return_value=httpx.Response(200, json={"full_name": "octocat/hello-world", "default_branch": "main"})
    )
    result = await adapter_for("github").sync(
        {"token": "ghp_test", "repositories": "octocat/hello-world"}
    )
    assert result["mode"] == "real"
    assert result["objects_synced"] == 1
    assert result["repositories"][0]["default_branch"] == "main"


@respx.mock
@pytest.mark.asyncio
async def test_notion_test_uses_users_me() -> None:
    route = respx.get("https://api.notion.com/v1/users/me").mock(
        return_value=httpx.Response(200, json={"object": "user"})
    )
    result = await adapter_for("notion").test(
        {"integration_token": "secret_xyz"}
    )
    assert result.status == "ok"
    assert route.calls.last.request.headers["Notion-Version"] == "2022-06-28"


@respx.mock
@pytest.mark.asyncio
async def test_notion_sync_calls_search_with_page_size() -> None:
    route = respx.post("https://api.notion.com/v1/search").mock(
        return_value=httpx.Response(200, json={"results": [{"id": "p1", "object": "page"}]})
    )
    result = await adapter_for("notion").sync(
        {"integration_token": "secret_xyz"}
    )
    assert result["mode"] == "real"
    assert result["objects_synced"] == 1
    assert b'"page_size":10' in route.calls.last.request.content.replace(b" ", b"")


@respx.mock
@pytest.mark.asyncio
async def test_confluence_test_uses_basic_auth() -> None:
    route = respx.get("https://acme.atlassian.net/wiki/rest/api/user/current").mock(
        return_value=httpx.Response(200, json={"accountId": "abc"})
    )
    result = await adapter_for("confluence").test(
        {
            "site_url": "https://acme.atlassian.net",
            "email": "user@acme.com",
            "api_token": "tok",
        }
    )
    assert result.status == "ok"
    assert route.calls.last.request.headers["Authorization"].startswith("Basic ")


@respx.mock
@pytest.mark.asyncio
async def test_confluence_fetch_content_returns_page_bodies() -> None:
    route = respx.get("https://acme.atlassian.net/wiki/rest/api/content").mock(
        return_value=httpx.Response(
            200,
            json={
                "results": [
                    {
                        "id": "123",
                        "title": "Data team architecture",
                        "space": {"key": "ACME"},
                        "version": {"number": 4},
                        "body": {"storage": {"value": "<p>On-call runbook deployment notes.</p>"}},
                        "_links": {"webui": "/spaces/ACME/pages/123"},
                    }
                ],
                "_links": {},
            },
        )
    )
    result = await adapter_for("confluence").fetch_content(
        {
            "site_url": "https://acme.atlassian.net",
            "email": "user@acme.com",
            "api_token": "tok",
        }
    )
    assert route.calls.last.request.url.params["expand"] == "body.storage,version,space,_links"
    assert result["pages"] == [
        {
            "id": "123",
            "title": "Data team architecture",
            "body": "<p>On-call runbook deployment notes.</p>",
            "space_key": "ACME",
            "version": 4,
            "url": "/spaces/ACME/pages/123",
        }
    ]


@respx.mock
@pytest.mark.asyncio
async def test_fivetran_test_uses_basic_auth_with_secret() -> None:
    route = respx.get("https://api.fivetran.com/v1/groups").mock(
        return_value=httpx.Response(200, json={"data": {"items": []}})
    )
    result = await adapter_for("fivetran").test({"api_key": "key", "api_secret": "secret"})
    assert result.status == "ok"
    assert route.calls.last.request.headers["Authorization"].startswith("Basic ")


@respx.mock
@pytest.mark.asyncio
async def test_fivetran_sync_uses_connections_endpoint() -> None:
    route = respx.get("https://api.fivetran.com/v1/connections").mock(
        return_value=httpx.Response(200, json={"data": {"items": [{"id": "conn_1"}]}})
    )
    result = await adapter_for("fivetran").sync({"api_key": "key", "api_secret": "secret"})
    assert result["mode"] == "real"
    assert result["objects_synced"] == 1
    assert route.calls.last.request.headers["Authorization"].startswith("Basic ")


@respx.mock
@pytest.mark.asyncio
async def test_fivetran_failed_runs_falls_back_to_connectors_endpoint() -> None:
    respx.get("https://api.fivetran.com/v1/connections").mock(return_value=httpx.Response(404, json={}))
    route = respx.get("https://api.fivetran.com/v1/connectors").mock(
        return_value=httpx.Response(
            200,
            json={
                "data": {
                    "items": [
                        {"id": "conn_1", "status": {"sync_state": "failed"}},
                        {"id": "conn_2", "status": {}},
                    ]
                }
            },
        )
    )
    failed = await adapter_for("fivetran").list_failed_runs({"api_key": "key", "api_secret": "secret"})
    assert [item["id"] for item in failed] == ["conn_1"]
    assert route.called


@respx.mock
@pytest.mark.asyncio
async def test_dbt_test_uses_token_scheme() -> None:
    route = respx.get("https://cloud.getdbt.com/api/v2/accounts/42/projects/").mock(
        return_value=httpx.Response(200, json={"data": []})
    )
    result = await adapter_for("dbt").test({"api_token": "abc", "account_id": "42"})
    assert result.status == "ok"
    assert route.calls.last.request.headers["Authorization"] == "Token abc"


@respx.mock
@pytest.mark.asyncio
async def test_databricks_accepts_hostname_without_scheme() -> None:
    route = respx.get("https://dbc-acme.cloud.databricks.com/api/2.0/clusters/list").mock(
        return_value=httpx.Response(200, json={"clusters": []})
    )
    result = await adapter_for("databricks").test(
        {
            "workspace_url": "dbc-acme.cloud.databricks.com",
            "http_path": "/sql/1.0/warehouses/abc",
            "token": "dapi-test",
        }
    )
    assert result.status == "ok"
    assert route.calls.last.request.headers["Authorization"] == "Bearer dapi-test"


def test_databricks_base_url_does_not_duplicate_api_path() -> None:
    adapter = adapter_for("databricks")
    assert (
        adapter.base_url({"workspace_url": "https://dbc-acme.cloud.databricks.com/api/2.0"})
        == "https://dbc-acme.cloud.databricks.com/api/2.0"
    )
