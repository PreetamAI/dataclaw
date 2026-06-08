"""Langfuse integration sink.

Lazy-loads the Langfuse Python SDK v4 only when a workspace has Langfuse
configured AND enabled. If the SDK isn't installed or config is missing, this
module's helpers degrade to no-ops so the rest of the platform keeps working.

Design notes
------------
* The Langfuse SDK is **not** a hard dependency. Listed in pyproject as an
  optional ``[evals]`` extra; we catch ImportError and disable cleanly.
* We do **not** use the Langfuse ``@observe`` decorator on internal functions —
  too invasive. Instead, the TracingService layer drives Langfuse calls
  explicitly via ``begin_span`` / ``end_span``, with our own DB sink as the
  primary always-on store. Langfuse is a forwarding sink, not the system of
  record.
* Spans are emitted **live** (start at span open, end at span close) so
  Langfuse sees real wall-clock timing rather than a flush-time burst.
* All Langfuse network I/O happens on the SDK's own background thread; we
  never await it on the request hot path. Failures are logged at debug level
  and swallowed — chat must never fail because Langfuse is down.

SDK compatibility: pinned to ``langfuse>=3.0`` (works with v3 and v4 SDKs;
tested against v4.7+). The SDK surface we touch:

* ``Langfuse(host=, public_key=, secret_key=)``
* ``langfuse.create_trace_id(seed=)`` → deterministic id derived from a seed
* ``langfuse.start_observation(as_type=, name=, trace_context=, input=,
   output=, metadata=, model=, usage_details=, level=, status_message=)``
   returns a span/generation wrapper with ``.update(...)`` and ``.end(...)``
* ``langfuse.create_score(name=, value=, trace_id=, comment=)``
* ``langfuse.flush()``
"""

from __future__ import annotations

import atexit
import logging
import threading
import uuid
from dataclasses import dataclass
from typing import Any

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class LangfuseConfig:
    host: str
    public_key: str
    secret_key: str
    project: str | None = None

    @classmethod
    def from_settings(cls, payload: dict[str, Any]) -> LangfuseConfig | None:
        if not payload.get("enabled"):
            return None
        host = (payload.get("host") or "").strip()
        public_key = (payload.get("public_key") or "").strip()
        secret_key = (payload.get("secret_key") or "").strip()
        if not (host and public_key and secret_key):
            return None
        return cls(
            host=host,
            public_key=public_key,
            secret_key=secret_key,
            project=(payload.get("project") or "").strip() or None,
        )


# An opaque handle returned by ``begin_span``. It either wraps a real Langfuse
# span object (which the SDK gives us a wrapper for) or is a no-op marker.
@dataclass
class _SpanHandle:
    obs: Any | None  # the SDK-returned span wrapper, or None for no-op
    observation_id: str | None  # cached id for parent_observation_id linking


class LangfuseSink:
    """Thin wrapper around the Langfuse SDK client.

    Every public method is best-effort: any exception is caught and logged at
    debug, never re-raised. The caller (TracingService) drives semantics —
    this class only forwards calls to the SDK.
    """

    def __init__(self, config: LangfuseConfig):
        self._config = config
        self._client: Any | None = None
        self._lock = threading.Lock()
        self._import_ok: bool | None = None
        self._atexit_registered = False

    # ---------- private ----------

    def _ensure_client(self) -> Any | None:
        if self._client is not None:
            return self._client
        if self._import_ok is False:
            return None
        with self._lock:
            if self._client is not None:
                return self._client
            try:
                from langfuse import Langfuse  # type: ignore[import-not-found]
            except ImportError:
                self._import_ok = False
                logger.info(
                    "langfuse_sdk_unavailable",
                    extra={"hint": "pip install dataclaw[evals]"},
                )
                return None
            try:
                # Both v3 and v4 SDKs accept these three kwargs; v4 prefers
                # ``base_url`` but still honours ``host`` for back-compat.
                self._client = Langfuse(
                    host=self._config.host,
                    public_key=self._config.public_key,
                    secret_key=self._config.secret_key,
                )
                self._import_ok = True
                if not self._atexit_registered:
                    atexit.register(self._safe_flush_atexit)
                    self._atexit_registered = True
            except Exception:
                logger.exception("langfuse_client_init_failed")
                self._import_ok = False
                return None
            return self._client

    def _safe_flush_atexit(self) -> None:
        try:
            if self._client is not None:
                self._client.flush()
        except Exception:
            pass

    # ---------- public ----------

    @property
    def config(self) -> LangfuseConfig:
        return self._config

    def is_available(self) -> bool:
        return self._ensure_client() is not None

    def create_trace_id(self, seed: str) -> str | None:
        """Deterministic trace id from our ChatMessage.id so the client can
        cross-link via /trace-link without round-tripping Langfuse."""
        client = self._ensure_client()
        if client is None:
            return None
        try:
            return client.create_trace_id(seed=seed)
        except Exception:
            logger.debug("langfuse_create_trace_id_failed", exc_info=True)
            return None

    def begin_span(
        self,
        *,
        trace_id: str,
        parent_observation_id: str | None,
        as_type: str,
        name: str,
        input: Any,
        metadata: dict[str, Any] | None,
        model: str | None,
    ) -> _SpanHandle:
        """Open a Langfuse span/generation NOW. Returns an opaque handle to
        close later via ``end_span``. Always returns a handle (no-op when
        the sink is unavailable) so the caller doesn't need a None check."""
        client = self._ensure_client()
        if client is None:
            return _SpanHandle(obs=None, observation_id=None)
        try:
            kwargs: dict[str, Any] = {
                "trace_context": {
                    "trace_id": trace_id,
                    "parent_observation_id": parent_observation_id,
                },
                "name": name,
                "input": _safe(input),
                "metadata": metadata or {},
            }
            if as_type == "generation" and model:
                kwargs["model"] = model
            sdk_as_type = "generation" if as_type == "generation" else "span"
            obs = client.start_observation(as_type=sdk_as_type, **kwargs)
            # The wrapper exposes the OTel observation id under several
            # attribute names depending on SDK version. We probe a few; if
            # none matches we mint a stable random id so child spans can
            # still link to it via parent_observation_id (the SDK accepts
            # any string and resolves it internally).
            obs_id = (
                getattr(obs, "id", None)
                or getattr(obs, "observation_id", None)
                or getattr(obs, "span_id", None)
            )
            if not isinstance(obs_id, str):
                obs_id = uuid.uuid4().hex
            return _SpanHandle(obs=obs, observation_id=obs_id)
        except Exception:
            logger.debug("langfuse_begin_span_failed", exc_info=True)
            return _SpanHandle(obs=None, observation_id=None)

    def end_span(
        self,
        handle: _SpanHandle,
        *,
        output: Any,
        usage: dict[str, Any] | None,
        status: str,
        error: str | None,
    ) -> None:
        """Close the span opened by ``begin_span``. Attaches output, usage,
        and (for errored spans) the error level + status message."""
        if handle.obs is None:
            return
        try:
            update_kwargs: dict[str, Any] = {"output": _safe(output)}
            if usage:
                update_kwargs["usage_details"] = usage
            if status == "error":
                update_kwargs["level"] = "ERROR"
                if error:
                    update_kwargs["status_message"] = error
            handle.obs.update(**update_kwargs)
            handle.obs.end()
        except Exception:
            logger.debug("langfuse_end_span_failed", exc_info=True)

    def score_trace(
        self,
        *,
        trace_id: str,
        name: str,
        value: float,
        comment: str | None = None,
    ) -> str | None:
        """Attach a score to a trace. SDK v4's create_score returns None — we
        mint a local id so the caller can store a non-null marker indicating
        the score was forwarded."""
        client = self._ensure_client()
        if client is None:
            return None
        try:
            client.create_score(
                name=name,
                value=value,
                trace_id=trace_id,
                comment=comment,
            )
            return f"lf:{uuid.uuid4().hex}"
        except Exception:
            logger.debug("langfuse_score_failed", exc_info=True)
            return None

    def flush(self) -> None:
        if self._client is None:
            return
        try:
            self._client.flush()
        except Exception:
            logger.debug("langfuse_flush_failed", exc_info=True)


# ---------- helpers ----------


def _safe(value: Any) -> Any:
    """Pass-through with a defensive cast: Langfuse SDK serialises with its
    own json encoder, but exotic objects (datetime, decimal) may still trip
    it. We only intervene when the value isn't already JSON-safe — and even
    then we fall back to ``str(value)`` rather than dropping it."""
    if value is None:
        return None
    if isinstance(value, str | int | float | bool | list | dict):
        return value
    try:
        return str(value)
    except Exception:
        return None


# ---------- module-level cached resolver ----------

_cache_lock = threading.Lock()
_cached: tuple[LangfuseConfig, LangfuseSink] | None = None


def resolve_sink(payload: dict[str, Any]) -> LangfuseSink | None:
    """Return a singleton LangfuseSink for the given settings payload, or None
    if the payload doesn't configure a usable client. Cached by config so a
    settings change rebuilds the client."""
    config = LangfuseConfig.from_settings(payload)
    if config is None:
        return None
    global _cached
    with _cache_lock:
        if _cached is not None and _cached[0] == config:
            return _cached[1]
        sink = LangfuseSink(config)
        _cached = (config, sink)
        return sink


def invalidate_cache() -> None:
    """Call after a settings update so the next resolve_sink rebuilds the
    client."""
    global _cached
    with _cache_lock:
        if _cached is not None:
            try:
                _cached[1].flush()
            except Exception:
                pass
        _cached = None
