from __future__ import annotations

import os
from typing import Any

from tests.integration.acme.seed.saas.common import (
    ACME_DOCS,
    SAAS_ENV,
    env_first,
    missing_env,
    record_action,
    sdk_missing,
    skipped,
)


def _rich_text(text: str) -> list[dict[str, Any]]:
    return [{"type": "text", "text": {"content": text[:2000]}}]


def _paragraph_block(text: str) -> dict[str, Any]:
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {"rich_text": _rich_text(text)},
    }


def _notion_id(value: str | None) -> str:
    return (value or "").replace("-", "").lower()


def _page_title(page: dict[str, Any]) -> str:
    title_property = page.get("properties", {}).get("title", {}).get("title", [])
    return "".join(part.get("plain_text") or part.get("text", {}).get("content") or "" for part in title_property)


def _page_children(notion: Any, page_id: str) -> list[dict[str, Any]]:
    children: list[dict[str, Any]] = []
    cursor: str | None = None
    while True:
        kwargs: dict[str, Any] = {"block_id": page_id, "page_size": 100}
        if cursor:
            kwargs["start_cursor"] = cursor
        response = notion.blocks.children.list(**kwargs)
        children.extend(response.get("results") or [])
        cursor = response.get("next_cursor")
        if not response.get("has_more") or not cursor:
            return children


def _replace_page_body(notion: Any, page_id: str, body: str) -> None:
    for block in _page_children(notion, page_id):
        block_id = block.get("id")
        if block_id:
            notion.blocks.delete(block_id=block_id)
    notion.blocks.children.append(block_id=page_id, children=[_paragraph_block(body)])


def seed_notion() -> dict[str, Any]:
    missing = missing_env(SAAS_ENV["notion"])
    if missing:
        return skipped(f"no creds: {', '.join(missing)}")
    try:
        from notion_client import Client
    except ImportError as exc:
        return sdk_missing("notion-client", exc)

    notion_token = env_first("NOTION_INTEGRATION_TOKEN", "NOTION_TOKEN")
    assert notion_token is not None
    parent_page_id = os.environ["NOTION_TEST_PARENT_PAGE_ID"]
    notion = Client(auth=notion_token)
    actions: list[dict[str, str]] = []
    seeded: dict[str, Any] = {"status": "seeded", "parent_page_id": parent_page_id, "actions": actions}
    key_by_title = {
        "Customers data model": "data_model_page_id",
        "Churn definition": "churn_page_id",
        "On-call runbook": "runbook_page_id",
        "Finance duplicate-payment runbook": "duplicate_payment_runbook_page_id",
        "Order status definitions": "order_status_definitions_page_id",
    }
    for title, body in ACME_DOCS.items():
        existing = notion.search(
            query=title,
            filter={"property": "object", "value": "page"},
            page_size=10,
        )
        page = next(
            (
                item
                for item in existing.get("results", [])
                if _notion_id(item.get("parent", {}).get("page_id")) == _notion_id(parent_page_id)
                and _page_title(item) in {title, f"Acme - {title}"}
            ),
            None,
        )
        if page is None:
            page = notion.pages.create(
                parent={"page_id": parent_page_id},
                properties={"title": {"title": _rich_text(title)}},
                children=[_paragraph_block(body)],
            )
            record_action(actions, "notion", title, "created")
        else:
            notion.pages.update(
                page_id=page["id"],
                properties={"title": {"title": _rich_text(title)}},
            )
            _replace_page_body(notion, page["id"], body)
            record_action(actions, "notion", title, "updated")
        seeded[key_by_title[title]] = page["id"]
    return seeded


__all__ = ["seed_notion"]
