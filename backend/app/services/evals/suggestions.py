"""Eval-suggestion lifecycle service: apply + dismiss.

Apply paths fully wired (Phase 5 v1):
* ``golden_query``  - creates a new EvalCase via EvalCaseService and
                       promotes it through approve → golden so chat starts
                       short-circuiting immediately.
* ``prompt_diff``   - writes the proposed text into the
                       ``chat:system_prompt_override`` AppSetting; chat
                       prepends it to the system message at request time.
* ``rules_md``      - appends to the workspace's ``chat:rules_md`` AppSetting,
                       which chat folds into the system prompt as a
                       "Workspace rules" preamble.

Apply paths NOT yet wired (recorded as ``apply_result.status='preview_only'``):
* ``tool_description_diff``
* ``retrieval_context``
* ``connector_routing``  (the rules_md path covers the common case; the
                          structured connector-routing table is follow-up)

Dismiss is always available — sets ``status='dismissed'`` and stamps the user.
"""

from __future__ import annotations

import logging
import re
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.models.domain import Connector, EvalCase, EvalSuggestion, User
from app.services.evals.cases import EvalCaseInput, EvalCaseService
from app.services.evals.diagnose import APPLY_SUPPORTED_KINDS, SUGGESTION_KINDS
from app.services.settings_store import _read, _write

logger = logging.getLogger(__name__)


# AppSetting keys consumed by chat at runtime (see chat.py integration).
PROMPT_OVERRIDE_KEY = "chat:system_prompt_override"
RULES_MD_KEY = "chat:rules_md"


class SuggestionNotFound(LookupError):
    """Surface as 404."""


class SuggestionTransitionError(ValueError):
    """Disallowed transition (e.g. apply on an already-applied suggestion).
    Surface as 409."""


class SuggestionValidationError(ValueError):
    """The suggestion's payload is malformed (e.g. a ``golden_query``
    whose ``proposed_value`` won't parse as SQL or won't execute against
    the target connector). Surface as 422 so the UI can distinguish
    "user did the wrong thing" (409) from "the suggestion content itself
    is broken" (422)."""


class SuggestionNotImplemented(NotImplementedError):
    """Kind is recognised but its Apply path isn't wired yet. Surface as
    409 with a clear message instead of 500 so the UI can show 'preview
    only' rather than a generic error."""


# Cheap text gate: golden_query proposed_value must read as a query.
# Anything else (column listings, schema descriptions, prose) is rejected
# without ever touching the connector. This catches the bulk of bad LLM
# output before we pay for a dry-run.
_SQL_QUERY_HEAD_RE = re.compile(
    r"^\s*(?:--[^\n]*\n\s*|/\*.*?\*/\s*)*(select|with)\b",
    re.IGNORECASE | re.DOTALL,
)


def _looks_like_sql(value: str) -> bool:
    return bool(_SQL_QUERY_HEAD_RE.match(value or ""))


async def _resolve_sqlite_demo_path(session: AsyncSession) -> str | None:
    """Return the sqlite demo db path from the configured connector, if any.
    Mirrors the lookup in ``main._resolve_query_engine`` so dry-run uses the
    exact engine chat would hit."""
    connector = await session.scalar(
        select(Connector).where(Connector.slug == "sqlite")
    )
    if connector and isinstance(connector.sync_summary, dict):
        path = connector.sync_summary.get("database_path")
        if isinstance(path, str) and path:
            return path
    return None


async def _dry_run_golden_sql(
    session: AsyncSession,
    *,
    sql: str,
    connector_slug: str | None,
) -> None:
    """Parse + plan the SQL against the target connector. Raises
    ``SuggestionValidationError`` with the underlying error message when
    the SQL won't run. Only SQLite is dry-run today because that's the
    only connector with a deterministic demo db on disk; other connectors
    fall back to the text gate above (best-effort)."""
    if not _looks_like_sql(sql):
        raise SuggestionValidationError(
            "Suggestion's proposed_value does not look like a SQL query "
            "(must start with SELECT or WITH)."
        )
    # Only attempt a real EXPLAIN for SQLite — that's where the bad
    # fixtures + LLM output actually land in this build.
    if (connector_slug or "").lower() != "sqlite":
        return
    path = await _resolve_sqlite_demo_path(session)
    if not path:
        return  # no configured engine to dry-run against; nothing to do
    engine = create_async_engine(f"sqlite+aiosqlite:///{path}")
    try:
        async with engine.connect() as conn:
            # EXPLAIN parses + plans without fetching rows. SQLite accepts
            # EXPLAIN in front of any SELECT/WITH, so wrapping is safe.
            await conn.execute(text(f"EXPLAIN {sql}"))
    except Exception as exc:
        raise SuggestionValidationError(
            f"SQL won't execute against sqlite: {exc.__class__.__name__}: {exc}"
        ) from exc
    finally:
        await engine.dispose()


class SuggestionService:
    def __init__(self, session: AsyncSession):
        self._session = session

    # ---------- queries ----------

    async def get(self, suggestion_id: str) -> EvalSuggestion:
        row = await self._session.get(EvalSuggestion, suggestion_id)
        if row is None:
            raise SuggestionNotFound(suggestion_id)
        return row

    # ---------- mutations ----------

    async def apply(
        self,
        suggestion_id: str,
        *,
        user: User | None = None,
    ) -> EvalSuggestion:
        row = await self.get(suggestion_id)
        if row.status == "applied":
            raise SuggestionTransitionError("suggestion already applied")
        if row.status == "reverted":
            raise SuggestionTransitionError(
                "reverted suggestion cannot be re-applied; run diagnose again"
            )
        if row.status == "dismissed":
            raise SuggestionTransitionError("dismissed suggestion cannot be applied")
        if row.kind not in SUGGESTION_KINDS:
            raise SuggestionTransitionError(f"unknown kind {row.kind!r}")

        if row.kind not in APPLY_SUPPORTED_KINDS:
            row.status = "applied"
            row.applied_at = datetime.now(UTC)
            row.applied_by = user.id if user else None
            row.apply_result = {
                "status": "preview_only",
                "message": (
                    f"Apply for `{row.kind}` is preview-only in this release. "
                    "The suggestion is recorded; manual follow-up needed to "
                    "wire it into the runtime."
                ),
                # Preview-only Apply has nothing to revert at runtime.
                "revert_supported": False,
            }
            await self._session.commit()
            return row

        handler = {
            "golden_query": self._apply_golden_query,
            "prompt_diff": self._apply_prompt_diff,
            "rules_md": self._apply_rules_md,
        }[row.kind]
        result = await handler(row, user=user)
        # Apply handlers stash a ``revert_payload`` dict with whatever data
        # ``revert`` needs to undo the change (prior prompt text, created
        # case id, the exact rule line, etc).
        row.status = "applied"
        row.applied_at = datetime.now(UTC)
        row.applied_by = user.id if user else None
        row.apply_result = {"status": "ok", "revert_supported": True, **result}
        await self._session.commit()
        return row

    async def dismiss(
        self,
        suggestion_id: str,
        *,
        user: User | None = None,
    ) -> EvalSuggestion:
        row = await self.get(suggestion_id)
        if row.status == "dismissed":
            return row
        if row.status == "applied":
            raise SuggestionTransitionError(
                "applied suggestion cannot be dismissed; revert it first"
            )
        if row.status == "reverted":
            raise SuggestionTransitionError("reverted suggestion cannot be dismissed")
        row.status = "dismissed"
        row.applied_at = datetime.now(UTC)
        row.applied_by = user.id if user else None
        await self._session.commit()
        return row

    async def revert(
        self,
        suggestion_id: str,
        *,
        user: User | None = None,
    ) -> EvalSuggestion:
        """Undo a previously-applied suggestion.

        Per-kind:
        * ``golden_query``  - archives the created EvalCase (chat stops
                              short-circuiting; case is kept for audit).
        * ``prompt_diff``   - restores the chat:system_prompt_override text
                              that was present before this apply.
        * ``rules_md``      - removes the specific rule line this apply
                              added (line-level revert, even if other
                              rules were added in between).

        Preview-only Apply paths have no runtime change to undo; reverting
        them just flips the status back to ``pending`` for re-review.
        """
        row = await self.get(suggestion_id)
        if row.status != "applied":
            raise SuggestionTransitionError(
                f"only applied suggestions can be reverted (got status={row.status!r})"
            )
        apply_result = row.apply_result or {}
        if apply_result.get("status") == "preview_only":
            # No runtime change to undo; flip status back to pending.
            row.status = "pending"
            row.applied_at = None
            row.applied_by = None
            row.apply_result = {"status": "reverted", "from": "preview_only"}
            await self._session.commit()
            return row

        handler = {
            "golden_query": self._revert_golden_query,
            "prompt_diff": self._revert_prompt_diff,
            "rules_md": self._revert_rules_md,
        }.get(row.kind)
        if handler is None:
            raise SuggestionTransitionError(
                f"revert not implemented for kind {row.kind!r}"
            )
        revert_outcome = await handler(row)
        row.status = "reverted"
        row.applied_at = datetime.now(UTC)
        row.applied_by = user.id if user else None
        row.apply_result = {
            **(row.apply_result or {}),
            "revert": {"status": "ok", **revert_outcome},
        }
        await self._session.commit()
        return row

    # ---------- apply handlers ----------

    async def _apply_golden_query(
        self,
        row: EvalSuggestion,
        *,
        user: User | None,
    ) -> dict[str, Any]:
        """Create a new EvalCase from the suggestion's payload, then walk it
        candidate → approved → golden so chat picks it up immediately.

        We intentionally create a NEW case rather than mutating the source
        case — the diagnose flow learned something new from this failure,
        and a fresh row keeps the eval-case history honest."""
        payload = row.apply_payload or {}
        question = payload.get("question")
        expected_sql = payload.get("expected_sql") or row.proposed_value
        if not question or not expected_sql:
            raise SuggestionTransitionError(
                "golden_query suggestion missing question or expected_sql"
            )
        # Dry-run before we promote: the chat path serves the canonical SQL
        # verbatim on a future GOLDEN_QUERY_HIT, so bad SQL here turns into
        # a /ide/query 400 in chat the next time the question is asked.
        await _dry_run_golden_sql(
            self._session,
            sql=expected_sql,
            connector_slug=payload.get("expected_connector_slug"),
        )
        service = EvalCaseService(self._session)
        new_case = await service.create(
            EvalCaseInput(
                workspace_id=row.workspace_id,
                question=question,
                expected_answer=payload.get("expected_answer"),
                expected_sql=expected_sql,
                expected_connector_slug=payload.get("expected_connector_slug"),
                expected_tool=payload.get("expected_tool"),
                expected_citations=payload.get("expected_citations") or [],
                tags=["from_suggestion"],
                origin="manual",
                status="candidate",
            ),
            created_by=user,
        )
        await service.approve(new_case.id, workspace_id=row.workspace_id)
        promoted = await service.promote_golden(new_case.id, workspace_id=row.workspace_id)
        return {
            "created_eval_case_id": promoted.id,
            "promoted_to_golden": True,
            # Revert metadata: which case to archive on revert.
            "revert_payload": {"created_eval_case_id": promoted.id},
        }

    async def _apply_prompt_diff(
        self,
        row: EvalSuggestion,
        *,
        user: User | None,
    ) -> dict[str, Any]:
        """Append the suggested grounding text to the workspace's chat
        prompt override. We append (rather than replace) so multiple
        suggestions over time accumulate guidance — the user can edit the
        AppSetting directly to prune."""
        addition = (
            row.apply_payload.get("append_to_prompt")
            if isinstance(row.apply_payload, dict)
            else None
        ) or row.proposed_value
        current = await _read(self._session, PROMPT_OVERRIDE_KEY)
        prev_text = (current or {}).get("text") or ""
        new_text = (prev_text + "\n\n" + addition).strip() if prev_text else addition
        await _write(
            self._session,
            PROMPT_OVERRIDE_KEY,
            {"text": new_text, "updated_by": user.id if user else None},
        )
        return {
            "setting_key": PROMPT_OVERRIDE_KEY,
            "text_length": len(new_text),
            # Revert metadata: snapshot the prior text so revert can restore it.
            # NOTE: if the user / another apply edits the AppSetting in between
            # apply and revert, revert restores to THIS snapshot (not the
            # in-between state). That's the safest semantic for "undo my apply".
            "revert_payload": {"prev_text": prev_text},
        }

    async def _apply_rules_md(
        self,
        row: EvalSuggestion,
        *,
        user: User | None,
    ) -> dict[str, Any]:
        """Append a single rule line to chat:rules_md. The chat path
        prepends rules_md as a 'Workspace rules' preamble (see chat.py
        integration). One rule per line keeps diffs readable."""
        rule = (
            row.apply_payload.get("rule")
            if isinstance(row.apply_payload, dict)
            else None
        ) or row.proposed_value
        rule = rule.strip()
        current = await _read(self._session, RULES_MD_KEY)
        existing = (current or {}).get("text") or ""
        # De-dupe — applying the same suggestion twice shouldn't double the rule.
        added_line: str | None = None
        if rule and rule not in existing:
            added_line = f"- {rule}"
            new_text = (existing + "\n" + added_line).strip() if existing else added_line
        else:
            new_text = existing
        await _write(
            self._session,
            RULES_MD_KEY,
            {"text": new_text, "updated_by": user.id if user else None},
        )
        return {
            "setting_key": RULES_MD_KEY,
            "rule_count": new_text.count("\n- ") + (1 if new_text else 0),
            # Revert metadata: the exact line to remove on revert. None means
            # the apply was a no-op (rule already present) so revert is no-op.
            "revert_payload": {"added_line": added_line},
        }

    # ---------- revert handlers ----------

    async def _revert_golden_query(self, row: EvalSuggestion) -> dict[str, Any]:
        """Archive the created EvalCase. Keeps the row for audit; chat
        stops short-circuiting because archived cases are excluded from
        golden lookup."""
        revert_payload = (row.apply_result or {}).get("revert_payload") or {}
        case_id = revert_payload.get("created_eval_case_id")
        if not case_id:
            return {"skipped": "no created_eval_case_id recorded"}
        service = EvalCaseService(self._session)
        try:
            archived = await service.archive(case_id, workspace_id=row.workspace_id)
        except Exception as exc:
            # Case might have been archived/deleted independently.
            return {"warning": f"archive failed: {exc.__class__.__name__}"}
        return {"archived_eval_case_id": archived.id}

    async def _revert_prompt_diff(self, row: EvalSuggestion) -> dict[str, Any]:
        """Restore the prompt override text to what it was before apply.
        Setting to empty string removes the override entirely (chat won't
        prepend the workspace prompt extension system message)."""
        revert_payload = (row.apply_result or {}).get("revert_payload") or {}
        prev_text = revert_payload.get("prev_text")
        if prev_text is None:
            return {"skipped": "no prev_text recorded"}
        # An empty prev_text means "no override existed before"; we still
        # write the empty value so load_chat_prompt_override returns None.
        await _write(
            self._session,
            PROMPT_OVERRIDE_KEY,
            {"text": prev_text},
        )
        return {"setting_key": PROMPT_OVERRIDE_KEY, "restored_length": len(prev_text)}

    async def _revert_rules_md(self, row: EvalSuggestion) -> dict[str, Any]:
        """Remove the specific rule line this apply added. Line-level
        revert means other rules added before/after are preserved."""
        revert_payload = (row.apply_result or {}).get("revert_payload") or {}
        added_line = revert_payload.get("added_line")
        if not added_line:
            return {"skipped": "apply was a no-op (rule already present)"}
        current = await _read(self._session, RULES_MD_KEY)
        existing = (current or {}).get("text") or ""
        # Remove the exact line (with or without surrounding newlines).
        lines = [line for line in existing.split("\n") if line.strip() != added_line.strip()]
        new_text = "\n".join(lines).strip()
        await _write(self._session, RULES_MD_KEY, {"text": new_text})
        return {"setting_key": RULES_MD_KEY, "removed_line": added_line}


# ---------- helpers for chat integration ----------


async def load_chat_prompt_override(session: AsyncSession) -> str | None:
    """Return the workspace's current system_prompt_override text, or None."""
    payload = await _read(session, PROMPT_OVERRIDE_KEY)
    text = (payload or {}).get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    return None


async def load_chat_rules_md(session: AsyncSession) -> str | None:
    payload = await _read(session, RULES_MD_KEY)
    text = (payload or {}).get("text")
    if isinstance(text, str) and text.strip():
        return text.strip()
    return None


# Used by services that need to lifecycle-link suggestions back to the
# created EvalCase (e.g. surface "→ promoted to golden case X" in the UI).
def created_case_id(row: EvalSuggestion) -> str | None:
    if not isinstance(row.apply_result, dict):
        return None
    cid = row.apply_result.get("created_eval_case_id")
    return cid if isinstance(cid, str) else None


# Make EvalCase importable from this module for callers who want a
# round-trip (Apply → newly created case).
__all__ = [
    "PROMPT_OVERRIDE_KEY",
    "RULES_MD_KEY",
    "SuggestionNotFound",
    "SuggestionNotImplemented",
    "SuggestionService",
    "SuggestionTransitionError",
    "created_case_id",
    "load_chat_prompt_override",
    "load_chat_rules_md",
    "EvalCase",
]
