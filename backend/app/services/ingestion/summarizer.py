from __future__ import annotations

import hashlib
import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any

from openai import AsyncOpenAI

logger = logging.getLogger("dataclaw.ingestion.summarizer")
LLM_TIMEOUT_SECONDS = 25.0

_FALLBACK_NOTES = {
    "no_llm_configured": "_Summary pending — no LLM provider is configured._",
    "llm_failed": "_Summary pending — the LLM summary timed out or errored; it will be retried on the next sync._",
    "empty_llm_response": "_Summary pending — the LLM returned an empty summary._",
}


def _placeholder_body(
    title: str, source_type: str, source_id: str, entities: list[str], reason: str | None
) -> str:
    """Wiki body used when no LLM summary is available.

    Deliberately does *not* embed the raw artifact JSON — a clear "pending"
    note plus the detected entities reads far better than a dumped blob.
    """
    note = _FALLBACK_NOTES.get(reason or "", "_Summary pending._")
    linked = ", ".join(f"[[{entity}]]" for entity in entities[:12]) or "No entities detected."
    return f"# {title}\n\n{note}\n\nSource: `{source_type}` / `{source_id}`.\n\nEntities: {linked}\n"


@dataclass
class WikiPageDraft:
    workspace_id: str
    path: str
    tier: int
    source_type: str
    source_id: str
    title: str
    body: str
    frontmatter: dict[str, Any] = field(default_factory=dict)
    entities: list[str] = field(default_factory=list)
    content_hash: str = ""


def content_hash(content: Any) -> str:
    raw = json.dumps(content, sort_keys=True, default=str) if not isinstance(content, str) else content
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def slugify(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", value.lower()).strip("-")
    return slug or "untitled"


def _artifact_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    return json.dumps(content, indent=2, sort_keys=True, default=str)


def _extract_entities(text: str) -> list[str]:
    explicit = re.findall(r"\[\[([^\]]+)\]\]", text)
    candidates = {item.strip().lower() for item in explicit}
    for token in re.findall(r"\b[a-zA-Z_][a-zA-Z0-9_]{2,}\b", text):
        lower = token.lower()
        if lower in {"select", "from", "where", "with", "table", "source", "owner", "model"}:
            continue
        if "_" in lower or lower.endswith("s"):
            candidates.add(lower)
    return sorted(candidates)[:40]


def _extract_produced_tables(text: str) -> list[str]:
    produced = set()
    for match in re.findall(r"\b(?:insert\s+into|create\s+(?:or\s+replace\s+)?table)\s+([a-zA-Z_][\w.]*)(?:\s|$)", text, flags=re.IGNORECASE):
        table = match.split(".")[-1].strip().lower()
        if table:
            produced.add(f"postgres/{table}")
    return sorted(produced)


async def summarize_artifact(
    *,
    workspace_id: str,
    source_type: str,
    source_id: str,
    content: Any,
    existing_page: str | None = None,
    openai_config: tuple[str | None, str | None, str | None, str | None] = (None, None, None, None),
) -> WikiPageDraft:
    text = _artifact_text(content)
    digest = content_hash(text)
    title = str(
        content.get("title")
        if isinstance(content, dict) and content.get("title")
        else content.get("name")
        if isinstance(content, dict) and content.get("name")
        else source_id
    )
    entities = _extract_entities(text)
    frontmatter: dict[str, Any] = {
        "title": title,
        "source_type": source_type,
        "source_id": source_id,
        "entities": entities,
        "last_content_hash": digest,
    }
    if isinstance(content, dict):
        for key in ("owner", "owners", "produces", "consumes", "depends_on", "columns", "row_count"):
            if key in content and content[key] not in (None, "", []):
                frontmatter[key] = content[key]
    if source_type == "airflow" and "produces" not in frontmatter:
        produced_tables = _extract_produced_tables(text)
        if produced_tables:
            frontmatter["produces"] = produced_tables

    api_key, model, base_url, _embedding_model = openai_config
    body = ""
    fallback_reason: str | None = None
    if not api_key:
        fallback_reason = "no_llm_configured"
    else:
        try:
            client = AsyncOpenAI(api_key=api_key, base_url=base_url, timeout=LLM_TIMEOUT_SECONDS)
            prompt = (
                "Update the existing page with new info; preserve user edits; output concise markdown "
                "for a data-team wiki page. Include important tables, owners, fields, and links using [[entity]]."
            )
            completion = await client.chat.completions.create(
                model=model or "gpt-4.1-mini",
                temperature=0,
                messages=[
                    {"role": "system", "content": prompt},
                    {
                        "role": "user",
                        "content": json.dumps(
                            {
                                "source_type": source_type,
                                "source_id": source_id,
                                "existing_page": existing_page,
                                "artifact": text[:24000],
                            }
                        ),
                    },
                ],
            )
            body = completion.choices[0].message.content or ""
            if not body:
                fallback_reason = "empty_llm_response"
        except Exception as exc:
            fallback_reason = "llm_failed"
            logger.error(
                "summarizer_llm_failed",
                extra={"_source_type": source_type, "_source_id": source_id, "_error": exc.__class__.__name__},
            )
            body = ""
    if not body:
        body = _placeholder_body(title, source_type, source_id, entities, fallback_reason)
    return WikiPageDraft(
        workspace_id=workspace_id,
        path=f"wiki/{source_type}/{slugify(title)}.md",
        tier=1,
        source_type=source_type,
        source_id=source_id,
        title=title,
        body=body,
        frontmatter=frontmatter,
        entities=entities,
        content_hash=digest,
    )
