"""Evals API (Phase 2).

POST   /evals/cases                       - create a case (manual or auto-gen)
POST   /evals/cases/from-feedback         - promote 👎 + correction into a case
GET    /evals/cases                       - list with filters
GET    /evals/cases/{id}                  - read
PATCH  /evals/cases/{id}                  - edit
POST   /evals/cases/{id}/approve          - candidate -> approved
POST   /evals/cases/{id}/promote-golden   - approved  -> golden
POST   /evals/cases/{id}/archive          - any       -> archived
POST   /evals/cases/{id}/unarchive        - archived  -> candidate

Auth: all routes require an authenticated user. Workspace scoping is applied
through the user's session (single-workspace assumption matches the rest of
the app today; revisit when multi-tenant lands in Theme 5).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
from sqlalchemy import case, desc, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import current_user
from app.db.session import get_session
from app.models.domain import EvalCase, EvalResult, EvalRun, EvalSuggestion, User, Workspace
from app.services.evals.cases import (
    CASE_ORIGINS,
    CASE_STATUSES,
    EvalCaseInput,
    EvalCaseNotFound,
    EvalCasePatch,
    EvalCaseService,
    EvalCaseTransitionError,
    EvalCaseValidationError,
)
from app.services.evals.diagnose import (
    APPLY_SUPPORTED_KINDS,
    SUGGESTION_KINDS,
    DiagnoseError,
    DiagnoseService,
    diagnose_status,
    schedule_background_diagnose,
)
from app.services.evals.generators import ALL_PRODUCERS, run_generators
from app.services.evals.generators.coordinator import (
    DEFAULT_LIMIT_PER_SOURCE,
    WORKSPACE_CANDIDATE_CEILING,
)
from app.services.evals.runner import run_batch as run_eval_batch
from app.services.evals.suggestions import (
    SuggestionNotFound,
    SuggestionService,
    SuggestionTransitionError,
    SuggestionValidationError,
)

router = APIRouter(prefix="/evals", tags=["evals"])


# ---------- DTOs (HTTP shapes) ----------


class CitationDTO(BaseModel):
    source: str | None = None
    table: str | None = None
    columns: list[str] | None = None
    # Allow pass-through of arbitrary citation fields produced by Phase 1
    # tool_call_provenance markers without us having to enumerate them.
    type: str | None = None
    connector: str | None = None
    tool: str | None = None
    title: str | None = None
    path: str | None = None


class EvalCaseDTO(BaseModel):
    id: str
    workspace_id: str
    question: str
    expected_answer: str | None
    expected_sql: str | None
    expected_connector_slug: str | None
    expected_tool: str | None
    expected_citations: list[dict[str, Any]] = Field(default_factory=list)
    expected_result_hash: str | None
    expected_result_preview: list[dict[str, Any]] = Field(default_factory=list)
    tags: list[str] = Field(default_factory=list)
    status: str
    origin: str
    source_chat_message_id: str | None
    created_by: str | None
    created_at: str
    updated_at: str

    @classmethod
    def from_row(cls, row: EvalCase) -> EvalCaseDTO:
        return cls(
            id=row.id,
            workspace_id=row.workspace_id,
            question=row.question,
            expected_answer=row.expected_answer,
            expected_sql=row.expected_sql,
            expected_connector_slug=row.expected_connector_slug,
            expected_tool=row.expected_tool,
            expected_citations=row.expected_citations or [],
            expected_result_hash=row.expected_result_hash,
            expected_result_preview=row.expected_result_preview or [],
            tags=row.tags or [],
            status=row.status,
            origin=row.origin,
            source_chat_message_id=row.source_chat_message_id,
            created_by=row.created_by,
            created_at=row.created_at.isoformat(),
            updated_at=row.updated_at.isoformat(),
        )


class EvalCaseCreateRequest(BaseModel):
    question: str
    expected_answer: str | None = None
    expected_sql: str | None = None
    expected_connector_slug: str | None = None
    expected_tool: str | None = None
    expected_citations: list[dict[str, Any]] | None = None
    tags: list[str] | None = None
    origin: str = "manual"
    status: str = "candidate"


class EvalCaseFromFeedbackRequest(BaseModel):
    chat_message_id: str
    question: str | None = None  # explicit override; otherwise looked up from thread
    expected_answer: str | None = None
    expected_sql: str | None = None
    expected_connector_slug: str | None = None
    expected_tool: str | None = None
    expected_citations: list[dict[str, Any]] | None = None
    tags: list[str] | None = None


class EvalCasePatchRequest(BaseModel):
    question: str | None = None
    expected_answer: str | None = None
    expected_sql: str | None = None
    expected_connector_slug: str | None = None
    expected_tool: str | None = None
    expected_citations: list[dict[str, Any]] | None = None
    tags: list[str] | None = None


# ---------- generation DTOs ----------


class GenerateCandidatesRequest(BaseModel):
    # None / empty list = all producers.
    sources: list[str] | None = None
    limit_per_source: int = Field(
        default=DEFAULT_LIMIT_PER_SOURCE, ge=1, le=DEFAULT_LIMIT_PER_SOURCE
    )


class ProducerResultDTO(BaseModel):
    slug: str
    display_name: str
    produced: int
    inserted: int
    skipped_duplicate: int
    skipped_cap: int
    error: str | None


class GenerateCandidatesResponse(BaseModel):
    workspace_id: str
    inserted_ids: list[str]
    producers: list[ProducerResultDTO]
    candidate_ceiling: int
    candidates_in_queue_before: int
    candidates_in_queue_after: int
    ceiling_reached: bool


class ProducerCatalogItem(BaseModel):
    slug: str
    display_name: str
    origin: str


class ProducerCatalogResponse(BaseModel):
    producers: list[ProducerCatalogItem]
    default_limit_per_source: int
    workspace_candidate_ceiling: int


class BulkCaseRequest(BaseModel):
    # Hard upper bound to prevent accidental / malicious DoS. 500 covers
    # every realistic workspace queue (matches WORKSPACE_CANDIDATE_CEILING)
    # without letting an attacker submit a million-id list.
    case_ids: list[str] = Field(min_length=1, max_length=500)


class BulkActionResult(BaseModel):
    id: str
    ok: bool
    status: str | None = None
    error: str | None = None


class BulkActionResponse(BaseModel):
    results: list[BulkActionResult]


# ---------- helpers ----------


async def _current_workspace(session: AsyncSession) -> Workspace:
    workspace = await session.scalar(select(Workspace).limit(1))
    if workspace is None:
        raise HTTPException(status_code=409, detail="No workspace; complete onboarding first.")
    return workspace


def _service_error_to_http(exc: Exception) -> HTTPException:
    if isinstance(exc, EvalCaseValidationError):
        return HTTPException(status_code=400, detail=str(exc))
    if isinstance(exc, EvalCaseNotFound):
        return HTTPException(status_code=404, detail=f"Eval case {exc} not found.")
    if isinstance(exc, EvalCaseTransitionError):
        return HTTPException(status_code=409, detail=str(exc))
    raise exc  # pragma: no cover - bubbles up as 500


# ---------- routes ----------


@router.get("/cases", response_model=list[EvalCaseDTO])
async def list_cases(
    status: str | None = Query(default=None, description=f"Filter by status: {CASE_STATUSES}"),
    origin: str | None = Query(default=None, description=f"Filter by origin: {CASE_ORIGINS}"),
    tag: str | None = Query(default=None),
    q: str | None = Query(default=None, description="Substring search across question + expected_*"),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> list[EvalCaseDTO]:
    workspace = await _current_workspace(session)
    try:
        rows = await EvalCaseService(session).list(
            workspace_id=workspace.id,
            status=status,
            origin=origin,
            tag=tag,
            q=q,
            limit=limit,
            offset=offset,
        )
    except EvalCaseValidationError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return [EvalCaseDTO.from_row(r) for r in rows]


@router.get("/cases/{case_id}", response_model=EvalCaseDTO)
async def get_case(
    case_id: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> EvalCaseDTO:
    workspace = await _current_workspace(session)
    try:
        row = await EvalCaseService(session).get(case_id, workspace_id=workspace.id)
    except EvalCaseNotFound as exc:
        raise HTTPException(status_code=404, detail=f"Eval case {exc} not found.") from exc
    return EvalCaseDTO.from_row(row)


@router.post("/cases", response_model=EvalCaseDTO)
async def create_case(
    payload: EvalCaseCreateRequest,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
) -> EvalCaseDTO:
    workspace = await _current_workspace(session)
    try:
        row = await EvalCaseService(session).create(
            EvalCaseInput(
                workspace_id=workspace.id,
                question=payload.question,
                expected_answer=payload.expected_answer,
                expected_sql=payload.expected_sql,
                expected_connector_slug=payload.expected_connector_slug,
                expected_tool=payload.expected_tool,
                expected_citations=payload.expected_citations,
                tags=payload.tags,
                origin=payload.origin,
                status=payload.status,
            ),
            created_by=user,
        )
    except (EvalCaseValidationError, EvalCaseTransitionError, EvalCaseNotFound) as exc:
        raise _service_error_to_http(exc) from exc
    await session.commit()
    return EvalCaseDTO.from_row(row)


@router.post("/cases/from-feedback", response_model=EvalCaseDTO)
async def create_case_from_feedback(
    payload: EvalCaseFromFeedbackRequest,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
) -> EvalCaseDTO:
    workspace = await _current_workspace(session)
    try:
        row = await EvalCaseService(session).create_from_feedback(
            workspace_id=workspace.id,
            chat_message_id=payload.chat_message_id,
            question=payload.question,
            expected_answer=payload.expected_answer,
            expected_sql=payload.expected_sql,
            expected_connector_slug=payload.expected_connector_slug,
            expected_tool=payload.expected_tool,
            expected_citations=payload.expected_citations,
            tags=payload.tags,
            created_by=user,
        )
    except (EvalCaseValidationError, EvalCaseTransitionError, EvalCaseNotFound) as exc:
        raise _service_error_to_http(exc) from exc
    await session.commit()
    return EvalCaseDTO.from_row(row)


@router.patch("/cases/{case_id}", response_model=EvalCaseDTO)
async def patch_case(
    case_id: str,
    payload: EvalCasePatchRequest,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> EvalCaseDTO:
    workspace = await _current_workspace(session)
    try:
        row = await EvalCaseService(session).update(
            case_id,
            EvalCasePatch(
                question=payload.question,
                expected_answer=payload.expected_answer,
                expected_sql=payload.expected_sql,
                expected_connector_slug=payload.expected_connector_slug,
                expected_tool=payload.expected_tool,
                expected_citations=payload.expected_citations,
                tags=payload.tags,
            ),
            workspace_id=workspace.id,
        )
    except (EvalCaseValidationError, EvalCaseTransitionError, EvalCaseNotFound) as exc:
        raise _service_error_to_http(exc) from exc
    await session.commit()
    return EvalCaseDTO.from_row(row)


@router.post("/cases/{case_id}/approve", response_model=EvalCaseDTO)
async def approve_case(
    case_id: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> EvalCaseDTO:
    workspace = await _current_workspace(session)
    return await _transition(session, case_id, "approve", workspace_id=workspace.id)


@router.post("/cases/{case_id}/promote-golden", response_model=EvalCaseDTO)
async def promote_golden(
    case_id: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> EvalCaseDTO:
    workspace = await _current_workspace(session)
    return await _transition(session, case_id, "promote_golden", workspace_id=workspace.id)


@router.post("/cases/{case_id}/archive", response_model=EvalCaseDTO)
async def archive_case(
    case_id: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> EvalCaseDTO:
    workspace = await _current_workspace(session)
    return await _transition(session, case_id, "archive", workspace_id=workspace.id)


@router.post("/cases/{case_id}/unarchive", response_model=EvalCaseDTO)
async def unarchive_case(
    case_id: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> EvalCaseDTO:
    workspace = await _current_workspace(session)
    return await _transition(session, case_id, "unarchive", workspace_id=workspace.id)


async def _transition(
    session: AsyncSession, case_id: str, op: str, *, workspace_id: str
) -> EvalCaseDTO:
    service = EvalCaseService(session)
    handler = getattr(service, op)
    try:
        row = await handler(case_id, workspace_id=workspace_id)
    except (EvalCaseValidationError, EvalCaseTransitionError, EvalCaseNotFound) as exc:
        raise _service_error_to_http(exc) from exc
    await session.commit()
    return EvalCaseDTO.from_row(row)


# ---------- Phase 3: auto-generation + bulk lifecycle ----------


@router.get("/producers", response_model=ProducerCatalogResponse)
async def list_producers(_user: User = Depends(current_user)) -> ProducerCatalogResponse:
    return ProducerCatalogResponse(
        producers=[
            ProducerCatalogItem(
                slug=p.slug,
                display_name=p.display_name,
                origin=p.origin,
            )
            for p in ALL_PRODUCERS
        ],
        default_limit_per_source=DEFAULT_LIMIT_PER_SOURCE,
        workspace_candidate_ceiling=WORKSPACE_CANDIDATE_CEILING,
    )


@router.post("/cases/generate-candidates", response_model=GenerateCandidatesResponse)
async def generate_candidates(
    payload: GenerateCandidatesRequest,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
) -> GenerateCandidatesResponse:
    workspace = await _current_workspace(session)
    sources = payload.sources or None
    if sources:
        known = {p.slug for p in ALL_PRODUCERS}
        unknown = [s for s in sources if s not in known]
        if unknown:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown producer slugs: {unknown}. Known: {sorted(known)}.",
            )
    result = await run_generators(
        session,
        producers=ALL_PRODUCERS,
        workspace_id=workspace.id,
        limit_per_source=payload.limit_per_source,
        created_by=user,
        sources=sources,
    )
    return GenerateCandidatesResponse(
        workspace_id=result.workspace_id,
        inserted_ids=result.inserted_ids,
        producers=[
            ProducerResultDTO(
                slug=p.slug,
                display_name=p.display_name,
                produced=p.produced,
                inserted=p.inserted,
                skipped_duplicate=p.skipped_duplicate,
                skipped_cap=p.skipped_cap,
                error=p.error,
            )
            for p in result.producers
        ],
        candidate_ceiling=result.candidate_ceiling,
        candidates_in_queue_before=result.candidates_in_queue_before,
        candidates_in_queue_after=result.candidates_in_queue_after,
        ceiling_reached=result.ceiling_reached,
    )


@router.post("/cases/bulk-approve", response_model=BulkActionResponse)
async def bulk_approve(
    payload: BulkCaseRequest,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> BulkActionResponse:
    workspace = await _current_workspace(session)
    return await _bulk_transition(session, payload, "approve", workspace_id=workspace.id)


@router.post("/cases/bulk-archive", response_model=BulkActionResponse)
async def bulk_archive(
    payload: BulkCaseRequest,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> BulkActionResponse:
    workspace = await _current_workspace(session)
    return await _bulk_transition(session, payload, "archive", workspace_id=workspace.id)


# ---------- Phase 4: eval runs + dashboard + Promptfoo export ----------


class RunBatchRequest(BaseModel):
    case_ids: list[str] | None = None
    status_filter: list[str] | None = None  # default: approved + golden
    repeat: int = Field(default=1, ge=1, le=10)


class EvalResultDTO(BaseModel):
    metric: str
    status: str
    score: float | None
    passed: bool | None
    detail: dict[str, Any] = Field(default_factory=dict)


class EvalRunDTO(BaseModel):
    id: str
    workspace_id: str
    batch_id: str
    eval_case_id: str
    chat_message_id: str | None
    passed: bool
    failure_category: str | None
    actual_answer: str | None
    actual_sql: str | None
    actual_citations: list[dict[str, Any]] = Field(default_factory=list)
    actual_result_preview: list[dict[str, Any]] = Field(default_factory=list)
    actual_result_hash: str | None
    langfuse_trace_id: str | None
    duration_ms: int
    prompt_tokens: int | None
    completion_tokens: int | None
    cost_usd: float | None
    model: str | None
    error: str | None
    created_at: str

    @classmethod
    def from_row(cls, row: EvalRun) -> EvalRunDTO:
        return cls(
            id=row.id,
            workspace_id=row.workspace_id,
            batch_id=row.batch_id,
            eval_case_id=row.eval_case_id,
            chat_message_id=row.chat_message_id,
            passed=row.passed,
            failure_category=row.failure_category,
            actual_answer=row.actual_answer,
            actual_sql=row.actual_sql,
            actual_citations=row.actual_citations or [],
            actual_result_preview=row.actual_result_preview or [],
            actual_result_hash=row.actual_result_hash,
            langfuse_trace_id=row.langfuse_trace_id,
            duration_ms=row.duration_ms,
            prompt_tokens=row.prompt_tokens,
            completion_tokens=row.completion_tokens,
            cost_usd=row.cost_usd,
            model=row.model,
            error=row.error,
            created_at=row.created_at.isoformat(),
        )


class EvalRunDetailDTO(EvalRunDTO):
    case: EvalCaseDTO
    results: list[EvalResultDTO] = Field(default_factory=list)


class BatchSummaryDTO(BaseModel):
    batch_id: str
    workspace_id: str
    total: int
    passed: int
    failed: int
    errored: int
    run_ids: list[str]
    # Cost-budget abort (Phase P1.3) — populated when the runner stopped
    # before the next case because the per-batch budget was hit.
    aborted: bool = False
    abort_reason: str | None = None
    cost_usd: float = 0.0


class EvalConfigDTO(BaseModel):
    schedule_enabled: bool
    schedule_interval_minutes: int = Field(ge=5, le=24 * 60)
    schedule_status_filter: list[str]
    batch_max_cost_usd: float = Field(ge=0.0)
    judges_enabled: bool
    # P2.3: bounded asyncio.gather. 1 = original serial path; >1 runs cases
    # in parallel with a semaphore. Clamped to [1, 16] in the settings store.
    batch_max_concurrency: int = Field(ge=1, le=16)


class EvalConfigUpdateRequest(BaseModel):
    schedule_enabled: bool | None = None
    schedule_interval_minutes: int | None = Field(default=None, ge=5, le=24 * 60)
    schedule_status_filter: list[str] | None = None
    batch_max_cost_usd: float | None = Field(default=None, ge=0.0)
    judges_enabled: bool | None = None
    batch_max_concurrency: int | None = Field(default=None, ge=1, le=16)


class DashboardMetricSummary(BaseModel):
    metric: str
    mean: float | None
    pass_rate: float | None
    sample_size: int


class DashboardBucket(BaseModel):
    date: str
    runs: int
    passed: int
    failed: int
    avg_duration_ms: float | None
    avg_cost_usd: float | None


class DashboardResponse(BaseModel):
    workspace_id: str
    range_days: int
    total_runs: int
    pass_rate: float | None
    p50_latency_ms: float | None
    p95_latency_ms: float | None
    total_cost_usd: float
    total_tokens: int
    regression_count: int
    failure_category_counts: dict[str, int] = Field(default_factory=dict)
    metrics: list[DashboardMetricSummary] = Field(default_factory=list)
    daily: list[DashboardBucket] = Field(default_factory=list)


async def _bulk_transition(
    session: AsyncSession,
    payload: BulkCaseRequest,
    op: str,
    *,
    workspace_id: str,
) -> BulkActionResponse:
    """Apply ``op`` (approve|archive) to every id, collecting per-id outcomes
    so the caller can render a partial-success summary instead of all-or-
    nothing. We don't short-circuit on the first failure — that matches the
    bulk-review UX (one bad row shouldn't block a queue cleanup)."""
    service = EvalCaseService(session)
    handler = getattr(service, op)
    results: list[BulkActionResult] = []
    any_changed = False
    for case_id in payload.case_ids:
        try:
            row = await handler(case_id, workspace_id=workspace_id)
            any_changed = True
            results.append(BulkActionResult(id=case_id, ok=True, status=row.status))
        except EvalCaseNotFound:
            results.append(BulkActionResult(id=case_id, ok=False, error="not_found"))
        except EvalCaseTransitionError as exc:
            results.append(BulkActionResult(id=case_id, ok=False, error=str(exc)))
        except EvalCaseValidationError as exc:
            results.append(BulkActionResult(id=case_id, ok=False, error=str(exc)))
    if any_changed:
        await session.commit()
    return BulkActionResponse(results=results)


# ---------- Phase 4 routes ----------


@router.post("/runs", response_model=BatchSummaryDTO)
async def run_evals_batch(
    payload: RunBatchRequest,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
) -> BatchSummaryDTO:
    """Kick a synchronous batch. Returns the batch_id + aggregate counts.

    Synchronous to keep the API simple: the runner serialises per-case
    chat calls inside the request. For large batches users can fire the
    scheduled job (off by default) or split into smaller `case_ids`
    invocations — Phase 5 will move this to a background task with a
    polling status endpoint when batch sizes warrant it."""
    workspace = await _current_workspace(session)
    status_filter = tuple(payload.status_filter or ("approved", "golden"))
    try:
        result = await run_eval_batch(
            session,
            workspace_id=workspace.id,
            case_ids=payload.case_ids,
            status_filter=status_filter,
            user=user,
            repeat=payload.repeat,
        )
    except LookupError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return BatchSummaryDTO(
        batch_id=result.batch_id,
        workspace_id=result.workspace_id,
        total=result.total,
        passed=result.passed,
        failed=result.failed,
        errored=result.errored,
        run_ids=result.run_ids,
        aborted=result.aborted,
        abort_reason=result.abort_reason,
        cost_usd=result.cost_usd,
    )


@router.get("/runs", response_model=list[EvalRunDTO])
async def list_runs(
    batch_id: str | None = Query(default=None),
    eval_case_id: str | None = Query(default=None),
    passed: bool | None = Query(default=None),
    since_hours: int | None = Query(default=None, ge=1, le=24 * 90),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> list[EvalRunDTO]:
    workspace = await _current_workspace(session)
    stmt = select(EvalRun).where(EvalRun.workspace_id == workspace.id)
    if batch_id:
        stmt = stmt.where(EvalRun.batch_id == batch_id)
    if eval_case_id:
        stmt = stmt.where(EvalRun.eval_case_id == eval_case_id)
    if passed is not None:
        stmt = stmt.where(EvalRun.passed.is_(passed))
    if since_hours:
        cutoff = datetime.now(UTC) - timedelta(hours=since_hours)
        stmt = stmt.where(EvalRun.created_at >= cutoff)
    stmt = stmt.order_by(desc(EvalRun.created_at)).offset(offset).limit(limit)
    rows = list((await session.scalars(stmt)).all())
    return [EvalRunDTO.from_row(r) for r in rows]


@router.get("/runs/{run_id}", response_model=EvalRunDetailDTO)
async def get_run(
    run_id: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> EvalRunDetailDTO:
    run = await session.get(EvalRun, run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="Eval run not found.")
    case = await session.get(EvalCase, run.eval_case_id)
    if case is None:
        raise HTTPException(status_code=404, detail="Linked eval case not found.")
    results = list(
        (
            await session.scalars(
                select(EvalResult).where(EvalResult.eval_run_id == run_id)
            )
        ).all()
    )
    return EvalRunDetailDTO(
        **EvalRunDTO.from_row(run).model_dump(),
        case=EvalCaseDTO.from_row(case),
        results=[
            EvalResultDTO(
                metric=r.metric,
                status=r.status,
                score=r.score,
                passed=r.passed,
                detail=r.detail or {},
            )
            for r in results
        ],
    )


@router.get("/metrics/dashboard", response_model=DashboardResponse)
async def metrics_dashboard(
    range_days: int = Query(default=7, ge=1, le=90),
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> DashboardResponse:
    """Aggregate the dashboard via SQL ``GROUP BY`` queries instead of
    loading every EvalRun + every EvalResult into Python (the old shape
    OOM'd around ~50k runs × 14 metrics = 700k EvalResult rows). Each
    query returns at most O(metric_count) or O(days_in_window) rows."""
    workspace = await _current_workspace(session)
    cutoff = datetime.now(UTC) - timedelta(days=range_days)

    # 1) Run-level scalars in a single round-trip.
    totals_row = (
        await session.execute(
            select(
                func.count(EvalRun.id).label("total_runs"),
                func.sum(case((EvalRun.passed.is_(True), 1), else_=0)).label("passed"),
                func.coalesce(func.sum(EvalRun.cost_usd), 0.0).label("total_cost"),
                func.coalesce(
                    func.sum(
                        func.coalesce(EvalRun.prompt_tokens, 0)
                        + func.coalesce(EvalRun.completion_tokens, 0)
                    ),
                    0,
                ).label("total_tokens"),
            ).where(
                EvalRun.workspace_id == workspace.id,
                EvalRun.created_at >= cutoff,
            )
        )
    ).one()
    total_runs = int(totals_row.total_runs or 0)
    if total_runs == 0:
        return DashboardResponse(
            workspace_id=workspace.id,
            range_days=range_days,
            total_runs=0,
            pass_rate=None,
            p50_latency_ms=None,
            p95_latency_ms=None,
            total_cost_usd=0.0,
            total_tokens=0,
            regression_count=0,
        )
    passed = int(totals_row.passed or 0)
    total_cost = float(totals_row.total_cost or 0.0)
    total_tokens = int(totals_row.total_tokens or 0)

    # 2) Percentiles: pull only the duration_ms column, sorted, into memory.
    #    One column for N runs is small (a few hundred KB at 100k runs).
    duration_rows = (
        await session.scalars(
            select(EvalRun.duration_ms)
            .where(
                EvalRun.workspace_id == workspace.id,
                EvalRun.created_at >= cutoff,
            )
            .order_by(EvalRun.duration_ms.asc())
        )
    ).all()
    durations = [int(d or 0) for d in duration_rows]
    p50 = _percentile(durations, 0.50)
    p95 = _percentile(durations, 0.95)

    # 3) Failure-category histogram — one row per category.
    fc_rows = (
        await session.execute(
            select(EvalRun.failure_category, func.count(EvalRun.id))
            .where(
                EvalRun.workspace_id == workspace.id,
                EvalRun.created_at >= cutoff,
                EvalRun.passed.is_(False),
                EvalRun.failure_category.is_not(None),
            )
            .group_by(EvalRun.failure_category)
        )
    ).all()
    failure_category_counts: dict[str, int] = {row[0]: int(row[1]) for row in fc_rows}

    # 4) Per-metric aggregation — one row per metric. Three SUMs:
    #    sample_size (all statuses), scored_sum + scored_count (status='ok',
    #    score IS NOT NULL), gating_pass_count + gating_count (status='ok',
    #    passed IS NOT NULL).
    metric_rows = (
        await session.execute(
            select(
                EvalResult.metric,
                func.count(EvalResult.id).label("sample_size"),
                func.sum(
                    case(
                        (
                            (EvalResult.status == "ok")
                            & (EvalResult.score.is_not(None)),
                            EvalResult.score,
                        ),
                        else_=0.0,
                    )
                ).label("score_sum"),
                func.sum(
                    case(
                        (
                            (EvalResult.status == "ok")
                            & (EvalResult.score.is_not(None)),
                            1,
                        ),
                        else_=0,
                    )
                ).label("scored_count"),
                func.sum(
                    case(
                        (
                            (EvalResult.status == "ok")
                            & (EvalResult.passed.is_(True)),
                            1,
                        ),
                        else_=0,
                    )
                ).label("gating_pass"),
                func.sum(
                    case(
                        (
                            (EvalResult.status == "ok")
                            & (EvalResult.passed.is_not(None)),
                            1,
                        ),
                        else_=0,
                    )
                ).label("gating_count"),
            )
            .join(EvalRun, EvalRun.id == EvalResult.eval_run_id)
            .where(
                EvalRun.workspace_id == workspace.id,
                EvalRun.created_at >= cutoff,
            )
            .group_by(EvalResult.metric)
            .order_by(EvalResult.metric.asc())
        )
    ).all()
    metric_summaries: list[DashboardMetricSummary] = []
    for row in metric_rows:
        scored_count = int(row.scored_count or 0)
        gating_count = int(row.gating_count or 0)
        mean = (float(row.score_sum) / scored_count) if scored_count else None
        pass_rate = (int(row.gating_pass) / gating_count) if gating_count else None
        metric_summaries.append(
            DashboardMetricSummary(
                metric=row.metric,
                mean=mean,
                pass_rate=pass_rate,
                sample_size=int(row.sample_size or 0),
            )
        )

    # 5) Regression count — only the rows that already say "prior passed"
    #    AND whose run failed. One join, one row count.
    regression_row = (
        await session.execute(
            select(func.count(EvalResult.id))
            .join(EvalRun, EvalRun.id == EvalResult.eval_run_id)
            .where(
                EvalRun.workspace_id == workspace.id,
                EvalRun.created_at >= cutoff,
                EvalRun.passed.is_(False),
                EvalResult.metric == "regression",
                EvalResult.status == "ok",
                EvalResult.detail.contains({"previous_run_passed": True}),
            )
        )
    ).one()
    regression_count = int(regression_row[0] or 0)

    # 6) Daily buckets via SQL date-trunc. SQLite + Postgres handle DATE()
    #    semantically; we keep the python coercion for safety.
    daily_rows = (
        await session.execute(
            select(
                func.date(EvalRun.created_at).label("day"),
                func.count(EvalRun.id).label("runs"),
                func.sum(case((EvalRun.passed.is_(True), 1), else_=0)).label("passed"),
                func.sum(case((EvalRun.passed.is_(False), 1), else_=0)).label("failed"),
                func.avg(EvalRun.duration_ms).label("avg_duration"),
                func.avg(EvalRun.cost_usd).label("avg_cost"),
            )
            .where(
                EvalRun.workspace_id == workspace.id,
                EvalRun.created_at >= cutoff,
            )
            .group_by("day")
            .order_by("day")
        )
    ).all()
    daily = [
        DashboardBucket(
            date=str(row.day),
            runs=int(row.runs or 0),
            passed=int(row.passed or 0),
            failed=int(row.failed or 0),
            avg_duration_ms=float(row.avg_duration) if row.avg_duration is not None else None,
            avg_cost_usd=float(row.avg_cost) if row.avg_cost is not None else None,
        )
        for row in daily_rows
    ]

    return DashboardResponse(
        workspace_id=workspace.id,
        range_days=range_days,
        total_runs=total_runs,
        pass_rate=passed / total_runs if total_runs else None,
        p50_latency_ms=p50,
        p95_latency_ms=p95,
        total_cost_usd=total_cost,
        total_tokens=total_tokens,
        regression_count=regression_count,
        failure_category_counts=failure_category_counts,
        metrics=metric_summaries,
        daily=daily,
    )


@router.get("/cases.promptfoo.yaml", response_class=PlainTextResponse)
async def export_promptfoo(
    status: str = Query(default="golden", description="case status to export"),
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> PlainTextResponse:
    workspace = await _current_workspace(session)
    rows = await EvalCaseService(session).list(
        workspace_id=workspace.id, status=status, limit=500
    )
    yaml_body = _emit_promptfoo_yaml(rows)
    return PlainTextResponse(content=yaml_body, media_type="text/yaml")


def _percentile(sorted_values: list[int], pct: float) -> float | None:
    if not sorted_values:
        return None
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    k = (len(sorted_values) - 1) * pct
    f = int(k)
    c = min(f + 1, len(sorted_values) - 1)
    if f == c:
        return float(sorted_values[f])
    return sorted_values[f] + (sorted_values[c] - sorted_values[f]) * (k - f)


def _emit_promptfoo_yaml(cases: list[EvalCase]) -> str:
    """Hand-roll Promptfoo YAML without taking on PyYAML.

    Promptfoo accepts a `tests` list where each item has `vars` (used in the
    prompt template via {{ }}) and `assert` entries that drive grading. We
    map each EvalCase to:
      vars.question, vars.expected_sql (when present)
      assert: contains (expected_answer or canonical fragment)
              equals (expected_sql normalized; promptfoo supports llm-rubric
                       which the user can swap in for richer grading)
    """
    lines: list[str] = []
    lines.append("# Generated by DataClaw — promote a golden case to update this list.")
    lines.append("# Usage: npx promptfoo eval -c <(curl …)")
    lines.append("description: DataClaw golden evals")
    lines.append("prompts:")
    lines.append("  - \"{{question}}\"")
    lines.append("tests:")
    if not cases:
        lines.append("  []")
        return "\n".join(lines) + "\n"
    for eval_case in cases:
        lines.append("  - description: |")
        for chunk in eval_case.question.splitlines() or [eval_case.question]:
            lines.append(f"      {chunk}")
        lines.append("    vars:")
        lines.append(f"      question: {_yaml_str(eval_case.question)}")
        if eval_case.expected_sql:
            lines.append(f"      expected_sql: {_yaml_str(eval_case.expected_sql)}")
        lines.append("    assert:")
        if eval_case.expected_answer:
            lines.append("      - type: contains")
            lines.append(f"        value: {_yaml_str(eval_case.expected_answer)}")
        if eval_case.expected_sql:
            lines.append("      - type: llm-rubric")
            lines.append(
                "        value: |"
            )
            lines.append(
                "          The model's SQL must be semantically equivalent to:"
            )
            for chunk in eval_case.expected_sql.splitlines():
                lines.append(f"          {chunk}")
        if not (eval_case.expected_answer or eval_case.expected_sql):
            lines.append("      - type: not-empty")
    return "\n".join(lines) + "\n"


def _yaml_str(value: str) -> str:
    """Quote a string for safe inclusion in a YAML scalar."""
    if "\n" in value:
        return "|\n      " + value.replace("\n", "\n      ")
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


# ---------- Phase 5: diagnose + suggestions ----------


class SuggestionDTO(BaseModel):
    id: str
    eval_run_id: str
    workspace_id: str
    kind: str
    title: str
    rationale: str
    current_value: str | None
    proposed_value: str
    target: str | None
    confidence: float
    source: str
    status: str
    apply_payload: dict[str, Any] = Field(default_factory=dict)
    apply_result: dict[str, Any] = Field(default_factory=dict)
    applied_at: str | None
    applied_by: str | None
    created_at: str
    updated_at: str
    apply_supported: bool

    @classmethod
    def from_row(cls, row: EvalSuggestion) -> SuggestionDTO:
        return cls(
            id=row.id,
            eval_run_id=row.eval_run_id,
            workspace_id=row.workspace_id,
            kind=row.kind,
            title=row.title,
            rationale=row.rationale,
            current_value=row.current_value,
            proposed_value=row.proposed_value,
            target=row.target,
            confidence=row.confidence,
            source=row.source,
            status=row.status,
            apply_payload=row.apply_payload or {},
            apply_result=row.apply_result or {},
            applied_at=row.applied_at.isoformat() if row.applied_at else None,
            applied_by=row.applied_by,
            created_at=row.created_at.isoformat(),
            updated_at=row.updated_at.isoformat(),
            apply_supported=row.kind in APPLY_SUPPORTED_KINDS,
        )


class DiagnoseRequest(BaseModel):
    use_llm: bool = True
    # P2.5: when True, kicks the diagnose service in a background task
    # and returns 202 immediately. Client polls
    # GET /evals/runs/{id}/suggestions (or .../diagnose/status) for results.
    background: bool = False


class DiagnoseStatusResponse(BaseModel):
    run_id: str
    status: str  # "running" | "completed" | "error: ..." | "idle"


class SuggestionCatalogResponse(BaseModel):
    kinds: list[str]
    apply_supported: list[str]


@router.get("/suggestions/kinds", response_model=SuggestionCatalogResponse)
async def list_suggestion_kinds(
    _user: User = Depends(current_user),
) -> SuggestionCatalogResponse:
    return SuggestionCatalogResponse(
        kinds=list(SUGGESTION_KINDS),
        apply_supported=sorted(APPLY_SUPPORTED_KINDS),
    )


@router.post("/runs/{run_id}/diagnose", response_model=list[SuggestionDTO])
async def diagnose_run(
    run_id: str,
    payload: DiagnoseRequest | None = None,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> list[SuggestionDTO]:
    use_llm = (payload.use_llm if payload else True)
    background = bool(payload.background) if payload else False
    # Fast 404 on unknown run — checked regardless of mode so the user
    # gets a clean error instead of a silent background failure.
    run_row = await session.get(EvalRun, run_id)
    if run_row is None:
        raise HTTPException(status_code=404, detail=f"eval_run {run_id!r} not found")
    if background:
        schedule_background_diagnose(run_id, use_llm=use_llm)
        # Return any suggestions that already exist (e.g. from an earlier
        # diagnose), so the client always has a known shape.
        existing = list(
            (
                await session.scalars(
                    select(EvalSuggestion).where(EvalSuggestion.eval_run_id == run_id)
                )
            ).all()
        )
        return [SuggestionDTO.from_row(r) for r in existing]
    try:
        rows = await DiagnoseService(session).diagnose(run_id, use_llm=use_llm)
    except DiagnoseError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    return [SuggestionDTO.from_row(r) for r in rows]


@router.get("/runs/{run_id}/diagnose/status", response_model=DiagnoseStatusResponse)
async def diagnose_run_status(
    run_id: str,
    _user: User = Depends(current_user),
) -> DiagnoseStatusResponse:
    """Background-diagnose status. Returns 'idle' if no background
    diagnose has been scheduled for this run; otherwise 'running',
    'completed', or 'error: <reason>'."""
    status = diagnose_status(run_id) or "idle"
    return DiagnoseStatusResponse(run_id=run_id, status=status)


@router.get("/runs/{run_id}/suggestions", response_model=list[SuggestionDTO])
async def list_suggestions_for_run(
    run_id: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> list[SuggestionDTO]:
    # Idempotent diagnose: returns existing rows if already generated, else
    # empty list. The frontend calls POST diagnose explicitly when the user
    # hits "Diagnose this run" — listing is just a viewer.
    rows = list(
        (
            await session.scalars(
                select(EvalSuggestion)
                .where(EvalSuggestion.eval_run_id == run_id)
                .order_by(EvalSuggestion.created_at.asc())
            )
        ).all()
    )
    return [SuggestionDTO.from_row(r) for r in rows]


@router.get("/suggestions/{suggestion_id}", response_model=SuggestionDTO)
async def get_suggestion(
    suggestion_id: str,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> SuggestionDTO:
    try:
        row = await SuggestionService(session).get(suggestion_id)
    except SuggestionNotFound as exc:
        raise HTTPException(status_code=404, detail=f"Suggestion {exc} not found.") from exc
    return SuggestionDTO.from_row(row)


@router.post("/suggestions/{suggestion_id}/apply", response_model=SuggestionDTO)
async def apply_suggestion(
    suggestion_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
) -> SuggestionDTO:
    try:
        row = await SuggestionService(session).apply(suggestion_id, user=user)
    except SuggestionNotFound as exc:
        raise HTTPException(status_code=404, detail=f"Suggestion {exc} not found.") from exc
    except SuggestionValidationError as exc:
        # 422: the suggestion content itself is malformed (bad SQL, etc.) —
        # distinct from 409 which means "valid suggestion, wrong lifecycle".
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except SuggestionTransitionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return SuggestionDTO.from_row(row)


@router.post("/suggestions/{suggestion_id}/dismiss", response_model=SuggestionDTO)
async def dismiss_suggestion(
    suggestion_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
) -> SuggestionDTO:
    try:
        row = await SuggestionService(session).dismiss(suggestion_id, user=user)
    except SuggestionNotFound as exc:
        raise HTTPException(status_code=404, detail=f"Suggestion {exc} not found.") from exc
    except SuggestionTransitionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return SuggestionDTO.from_row(row)


# ---------- P1: Settings → Evals (schedule + cost budget + judges) ----------


@router.get("/config", response_model=EvalConfigDTO)
async def get_evals_config(
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> EvalConfigDTO:
    from app.services.settings_store import get_eval_config

    cfg = await get_eval_config(session)
    return EvalConfigDTO(**cfg)


class InvalidateStaleRequest(BaseModel):
    qualified_table_name: str = Field(min_length=1, max_length=255)


class InvalidateStaleResponse(BaseModel):
    workspace_id: str
    affected_table: str
    demoted_case_ids: list[str]


@router.post("/cases/invalidate-stale", response_model=InvalidateStaleResponse)
async def invalidate_stale_golden_cases(
    payload: InvalidateStaleRequest,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> InvalidateStaleResponse:
    """Demote golden eval cases whose expected_citations reference the
    given table. Intended to be called from the connector sync path when
    schema drift is detected; also exposed for manual cleanup."""
    from app.services.evals.staleness import invalidate_golden_cases_for_table

    workspace = await _current_workspace(session)
    result = await invalidate_golden_cases_for_table(
        session,
        workspace_id=workspace.id,
        qualified_table_name=payload.qualified_table_name,
    )
    await session.commit()
    return InvalidateStaleResponse(
        workspace_id=result.workspace_id,
        affected_table=result.affected_table,
        demoted_case_ids=result.demoted_case_ids,
    )


@router.put("/config", response_model=EvalConfigDTO)
async def update_evals_config(
    payload: EvalConfigUpdateRequest,
    session: AsyncSession = Depends(get_session),
    _user: User = Depends(current_user),
) -> EvalConfigDTO:
    from app.services.settings_store import update_eval_config

    values = {k: v for k, v in payload.model_dump().items() if v is not None}
    cfg = await update_eval_config(session, values)
    await session.commit()
    return EvalConfigDTO(**cfg)


@router.post("/suggestions/{suggestion_id}/revert", response_model=SuggestionDTO)
async def revert_suggestion(
    suggestion_id: str,
    session: AsyncSession = Depends(get_session),
    user: User = Depends(current_user),
) -> SuggestionDTO:
    """Undo a previously-applied suggestion. See
    SuggestionService.revert for per-kind semantics."""
    try:
        row = await SuggestionService(session).revert(suggestion_id, user=user)
    except SuggestionNotFound as exc:
        raise HTTPException(status_code=404, detail=f"Suggestion {exc} not found.") from exc
    except SuggestionTransitionError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return SuggestionDTO.from_row(row)
