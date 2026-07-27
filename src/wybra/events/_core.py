"""Typed observational events with contextual scopes and async dispatch.

Routine dispatch diagnostics are opt-in. Handler failures are recorded through
the dedicated ``events.errors`` scope. Operational exception logging preserves
the original exception traceback, so event handlers must not raise exceptions
whose messages contain secrets. When routine telemetry is selected outside a
diagnostic context, each observation is retained as an individual snapshot.
"""

from __future__ import annotations

import logging
import time
from asyncio import (
    CancelledError,
    Task,
    create_task,
    current_task,
    ensure_future,
)
from asyncio import (
    Event as AsyncEvent,
)
from collections import deque
from collections.abc import Awaitable, Callable, Iterable
from contextvars import Context, ContextVar, Token, copy_context
from dataclasses import InitVar, dataclass, field
from functools import wraps
from gc import collect
from inspect import BoundArguments, iscoroutinefunction, signature
from threading import Lock
from typing import (
    TYPE_CHECKING,
    ClassVar,
    Final,
    ParamSpec,
    Protocol,
    TypeVar,
    runtime_checkable,
)
from uuid import UUID
from weakref import ReferenceType, ref

from wybra.config import BaseSettings, ConfigDef, to_bool
from wybra.events_config import EVENTS_CONFIG_DEF, EVENTS_CONFIG_SECTION

if TYPE_CHECKING:
    from wybra.site import Site

P = ParamSpec("P")
T = TypeVar("T")

logger = logging.getLogger(__name__)
EVENT_HISTORY_LIMIT: Final = 32
EVENT_DELIVERY_PENDING_LIMIT: Final = 1024


class EventScopeError(ValueError):
    """Raised when an event scope or topic is invalid."""


class EventRuntimeError(RuntimeError):
    """Raised when event runtime lifecycle rules are violated."""


@dataclass(frozen=True, slots=True)
class EventSegment:
    """A reusable validated segment in an event topic."""

    value: str

    def __post_init__(self) -> None:
        object.__setattr__(self, "value", _normalise_segment(self.value))

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True, slots=True)
class EventScope:
    """A validated dot-notation event selector or concrete topic."""

    segments: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.segments:
            raise EventScopeError("Event scopes must contain at least one segment.")
        object.__setattr__(
            self,
            "segments",
            tuple(_normalise_segment(segment) for segment in self.segments),
        )

    def __call__(self, *segments: EventSegment | str) -> EventScope:
        """Create a child topic using reusable or dynamic validated segments."""

        return EventScope(
            self.segments
            + tuple(
                segment.value if isinstance(segment, EventSegment) else segment
                for segment in segments
            )
        )

    def matches(self, selector: EventScope) -> bool:
        """Return whether this concrete scope belongs to ``selector``."""

        return self.segments[: len(selector.segments)] == selector.segments

    def __str__(self) -> str:
        return ".".join(self.segments)


@dataclass(frozen=True, slots=True)
class EventContext:
    """Immutable inherited correlation context for event observations."""

    request_id: UUID | None = None
    segments: tuple[str, ...] = ()

    def extend(self, *segments: str) -> EventContext:
        """Derive a context with additional declared levels."""

        return EventContext(
            request_id=self.request_id,
            segments=(
                self.segments
                + tuple(_normalise_segment(segment) for segment in segments)
            ),
        )


@dataclass(frozen=True, slots=True, kw_only=True)
class Event:
    """An immutable observation classified by its current event scope."""

    event_scope: ClassVar[EventScope]
    topic: InitVar[EventScope | None] = None
    scope: EventScope = field(init=False)
    context: EventContext | None = field(init=False)
    occurred_at: float = field(default_factory=time.time)

    def __post_init__(self, topic: EventScope | None) -> None:
        fixed_scope = topic or getattr(type(self), "event_scope", None)
        if isinstance(fixed_scope, EventScope):
            object.__setattr__(self, "scope", fixed_scope)
        else:
            raise EventScopeError(
                "Event type "
                f"{type(self).__name__} must declare an explicit event topic."
            )
        object.__setattr__(self, "context", current_context())


type EventHandler = Callable[[Event], Awaitable[None]]
type _QueuedEvent = tuple[Event, tuple[EventHandler, ...], Context]


@dataclass(frozen=True, slots=True)
class EventsSettings(BaseSettings):
    """Core event-delivery settings loaded before configured modules."""

    module_config: ClassVar[ConfigDef] = EVENTS_CONFIG_DEF
    config_section: ClassVar[str | None] = EVENTS_CONFIG_SECTION

    enabled: bool = False

    def __post_init__(self) -> None:
        object.__setattr__(self, "enabled", to_bool(self.enabled))


@dataclass(frozen=True, slots=True)
class EventOutcome:
    """Secret-safe completion state supplied to a post-operation event factory."""

    succeeded: bool
    duration_seconds: float = 0.0
    error_type: str | None = None
    result: object | None = None


@runtime_checkable
class EventsCapability(Protocol):
    """Process-local delivery of typed observational events."""

    async def subscribe(
        self,
        selector: EventScope,
        handler: EventHandler,
        *,
        history: bool = False,
    ) -> None: ...

    async def publish(self, event: Event) -> None: ...


class _DisabledEventsCapability:
    """No-op core sink used when optional event delivery is disabled."""

    async def subscribe(
        self,
        selector: EventScope,
        handler: EventHandler,
        *,
        history: bool = False,
    ) -> None:
        del selector, handler, history
        return None

    async def publish(self, event: Event) -> None:
        del event
        return None

    async def close(self) -> None:
        """Release the disabled no-op runtime."""
        return None

    def enabled(self) -> bool:
        """Return whether delivery can accept observations."""

        return False


@dataclass(slots=True)
class _LazyEventsCapability:
    """Resolve the configured process event sink on first use."""

    site: ReferenceType[Site]
    _capability: EventsCapability | None = field(default=None, init=False)
    _resolve_lock: Lock = field(default_factory=Lock, init=False, repr=False)

    async def subscribe(
        self,
        selector: EventScope,
        handler: EventHandler,
        *,
        history: bool = False,
    ) -> None:
        await self._resolve().subscribe(selector, handler, history=history)

    async def publish(self, event: Event) -> None:
        await self._resolve().publish(event)

    def enabled(self) -> bool:
        """Return whether the resolved sink is accepting observations."""

        return event_delivery_enabled(self._resolve())

    async def close(self) -> None:
        """Release this runtime when its owning site shuts down."""
        capability = self._capability
        if capability is not None:
            close = getattr(capability, "close", None)
            if callable(close) and iscoroutinefunction(close):
                await close()
        _release_events_runtime(self)

    async def _drain(self) -> None:
        """Wait for queued delivery; used only by deterministic test support."""
        capability = self._capability
        if capability is None:
            return
        drain = getattr(capability, "_drain", None)
        if callable(drain) and iscoroutinefunction(drain):
            await drain()

    def _resolve(self) -> EventsCapability:
        capability = self._capability
        if capability is None:
            # A thread lock makes first resolution safe for event-loop tasks
            # that reach this lazy process runtime concurrently.
            with self._resolve_lock:
                capability = self._capability
                if capability is None:
                    site = self.site()
                    if site is None:
                        return _DisabledEventsCapability()
                    settings = EventsSettings.load_settings(site.config)
                    capability = (
                        EventDispatcher()
                        if settings.enabled
                        else _DisabledEventsCapability()
                    )
                    self._capability = capability
        return capability


@dataclass(slots=True)
class EventDispatcher:
    """Deliver events sequentially without controlling their producers."""

    _handlers: list[tuple[EventScope, EventHandler]] = field(default_factory=list)
    _history: deque[Event] = field(
        default_factory=lambda: deque(maxlen=EVENT_HISTORY_LIMIT),
        init=False,
        repr=False,
    )
    _pending: deque[_QueuedEvent] = field(
        default_factory=deque,
        init=False,
        repr=False,
    )
    _pending_available: AsyncEvent = field(
        default_factory=AsyncEvent,
        init=False,
        repr=False,
    )
    _pending_drained: AsyncEvent = field(
        default_factory=AsyncEvent,
        init=False,
        repr=False,
    )
    _worker: Task[None] | None = field(default=None, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)
    _dropped_pending_events: int = field(default=0, init=False, repr=False)

    def __post_init__(self) -> None:
        self._pending_drained.set()

    def enabled(self) -> bool:
        """Return whether this concrete dispatcher accepts observations."""

        return True

    async def subscribe(
        self,
        selector: EventScope,
        handler: EventHandler,
        *,
        history: bool = False,
    ) -> None:
        if not iscoroutinefunction(handler) and not iscoroutinefunction(
            handler.__call__
        ):
            raise TypeError("Event handlers must be async callables.")
        retained: tuple[Event, ...] = ()
        if history:
            retained = tuple(
                event for event in self._history if event.scope.matches(selector)
            )
        self._handlers.append((selector, handler))
        for event in retained:
            await self._dispatch_handler(event, handler, copy_context())

    async def publish(self, event: Event) -> None:
        """Queue an event without waiting for its handlers to finish."""
        if self._closed:
            return
        self._history.append(event)
        handlers = tuple(
            handler
            for selector, handler in self._handlers
            if event.scope.matches(selector)
        )
        if len(self._pending) >= EVENT_DELIVERY_PENDING_LIMIT:
            self._pending.popleft()
            self._dropped_pending_events += 1
            if self._should_log_pending_eviction():
                logger.warning(
                    "Event delivery backlog is full; dropping oldest observation",
                    extra={
                        "pending_limit": EVENT_DELIVERY_PENDING_LIMIT,
                        "dropped_pending_events": self._dropped_pending_events,
                    },
                )
        self._pending.append((event, handlers, copy_context()))
        self._pending_drained.clear()
        self._pending_available.set()
        if self._worker is None or self._worker.done():
            self._worker = create_task(self._dispatch_queued_events())

    async def close(self) -> None:
        """Finish accepted delivery before releasing the dispatcher worker."""
        self._closed = True
        worker = self._worker
        if worker is None or worker.done():
            return
        await self._pending_drained.wait()
        worker.cancel()
        try:
            await worker
        except CancelledError:
            if _external_cancellation_requested():
                raise

    async def _drain(self) -> None:
        """Wait until all currently queued events have been delivered."""
        await self._pending_drained.wait()

    async def _dispatch_queued_events(self) -> None:
        while True:
            await self._pending_available.wait()
            while self._pending:
                event, handlers, producer_context = self._pending.popleft()
                if not self._pending:
                    self._pending_available.clear()
                await self._dispatch_event(event, handlers, producer_context)
            self._pending_drained.set()

    def _should_log_pending_eviction(self) -> bool:
        """Rate-limit backlog warnings while retaining useful load visibility."""

        return self._dropped_pending_events == 1 or (
            self._dropped_pending_events & (self._dropped_pending_events - 1) == 0
        )

    async def _dispatch_event(
        self,
        event: Event,
        handlers: tuple[EventHandler, ...],
        producer_context: Context,
    ) -> None:
        dispatch_started = time.perf_counter()
        handler_failed = False
        for handler in handlers:
            handler_failed = (
                await self._dispatch_handler(event, handler, producer_context)
                or handler_failed
            )
        _record_dispatch_diagnostic(
            event,
            duration_seconds=time.perf_counter() - dispatch_started,
            handler_failed=handler_failed,
        )

    async def _dispatch_handler(
        self,
        event: Event,
        handler: EventHandler,
        producer_context: Context,
    ) -> bool:
        started = time.perf_counter()
        handler_task = producer_context.run(ensure_future, handler(event))
        try:
            await handler_task
        except BaseException as exc:
            if _external_cancellation_requested():
                handler_task.cancel()
                raise
            logger.exception(
                "Event handler failed",
                extra={
                    "event_type": _callable_identity(type(event)),
                    "event_scope": str(event.scope),
                    "handler": _callable_identity(handler),
                    "error_type": type(exc).__name__,
                },
            )
            _record_handler_diagnostic(
                event,
                handler,
                duration_seconds=time.perf_counter() - started,
                error=exc,
            )
            return True
        else:
            _record_handler_diagnostic(
                event,
                handler,
                duration_seconds=time.perf_counter() - started,
            )
            return False


_EVENTS_RUNTIME: _LazyEventsCapability | None = None
_EVENTS_RUNTIME_LOCK = Lock()


def setup_core_events(site: Site) -> None:
    """Register the one lazy process event runtime before module setup."""

    global _EVENTS_RUNTIME
    runtime = _LazyEventsCapability(ref(site))
    with _EVENTS_RUNTIME_LOCK:
        if _EVENTS_RUNTIME is not None and _EVENTS_RUNTIME.site() is not None:
            # A caller that has discarded its app/site can leave the app-state
            # reference cycle awaiting ordinary garbage collection. It is no
            # longer a live Site, so collect once before rejecting a genuine
            # concurrently active Site.
            collect()
        if _EVENTS_RUNTIME is not None and _EVENTS_RUNTIME.site() is not None:
            raise EventRuntimeError("Only one Wybra Site may run in a process.")
        _EVENTS_RUNTIME = runtime
    try:
        site.provide_capability(EventsCapability, runtime)
    except BaseException:
        _release_events_runtime(runtime)
        raise


def events_enabled() -> bool:
    """Resolve and return whether central event delivery is enabled."""

    runtime = _EVENTS_RUNTIME
    return runtime is not None and runtime.enabled()


type EventDescriptor = Callable[..., Event | None]


def observe(
    descriptor: EventDescriptor,
    *options: object,
    context: str | Iterable[str] | None = None,
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Observe an async boundary without exposing publication mechanics."""

    segments = _normalise_context_segments(context)

    def decorate(function: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        if not iscoroutinefunction(function):
            raise TypeError("Observed functions must be async callables.")
        function_signature = signature(function)

        @wraps(function)
        async def observed(*args: P.args, **kwargs: P.kwargs) -> T:
            if not events_enabled():
                return await function(*args, **kwargs)
            call = function_signature.bind(*args, **kwargs)
            call.apply_defaults()
            token = _set_observation_context(segments)
            started = time.perf_counter()
            try:
                await _publish_descriptor_safely(descriptor, call, None, options)
                result = await function(*args, **kwargs)
            except BaseException as exc:
                await _publish_descriptor_safely(
                    descriptor,
                    call,
                    EventOutcome(
                        succeeded=False,
                        duration_seconds=time.perf_counter() - started,
                        error_type=type(exc).__name__,
                    ),
                    options,
                )
                raise
            else:
                await _publish_descriptor_safely(
                    descriptor,
                    call,
                    EventOutcome(
                        succeeded=True,
                        duration_seconds=time.perf_counter() - started,
                        result=result,
                    ),
                    options,
                )
                return result
            finally:
                if token is not None:
                    _CURRENT_CONTEXT.reset(token)

        return observed

    return decorate


def _normalise_context_segments(value: str | Iterable[str] | None) -> tuple[str, ...]:
    if value is None:
        return ()
    values = (value,) if isinstance(value, str) else tuple(value)
    return tuple(_normalise_segment(segment) for segment in values)


def _set_observation_context(
    segments: tuple[str, ...],
) -> Token[EventContext | None] | None:
    if not segments:
        return None
    parent = current_context() or EventContext()
    return _CURRENT_CONTEXT.set(parent.extend(*segments))


async def _publish_descriptor_safely(
    descriptor: EventDescriptor,
    call: BoundArguments,
    outcome: EventOutcome | None,
    options: tuple[object, ...],
) -> None:
    try:
        event = descriptor(call, outcome, *options)
    except BaseException:
        logger.exception("Unable to create observed event")
        return
    if event is None:
        return
    runtime = _EVENTS_RUNTIME
    if runtime is not None:
        await _publish_observation(runtime, event, message="observed event")


def _release_events_runtime(runtime: _LazyEventsCapability) -> None:
    """Release the process runtime after its owning site has closed."""

    global _EVENTS_RUNTIME
    with _EVENTS_RUNTIME_LOCK:
        if _EVENTS_RUNTIME is runtime:
            _EVENTS_RUNTIME = None


async def _publish_safely(
    dispatcher: EventsCapability,
    event: Event,
    *,
    message: str,
) -> None:
    try:
        await dispatcher.publish(event)
    except BaseException:
        if _external_cancellation_requested():
            raise
        logger.exception("Unable to publish %s", message)


async def _publish_observation(
    dispatcher: EventsCapability,
    event: Event,
    *,
    message: str,
) -> None:
    """Publish an event without allowing delivery to alter an operation."""

    await _publish_safely(dispatcher, event, message=message)


_CURRENT_CONTEXT: ContextVar[EventContext | None] = ContextVar(
    "wybra_current_event_context",
    default=None,
)


def current_context() -> EventContext | None:
    """Return the inherited event context for the current async execution."""

    return _CURRENT_CONTEXT.get()


def context(
    values: str | Iterable[str],
) -> Callable[[Callable[P, Awaitable[T]]], Callable[P, Awaitable[T]]]:
    """Decorate an async callable with additional immutable context levels."""

    segments = (values,) if isinstance(values, str) else tuple(values)
    if not segments:
        raise EventScopeError("Event context extensions require at least one segment.")
    normalised = tuple(_normalise_segment(segment) for segment in segments)

    def decorate(function: Callable[P, Awaitable[T]]) -> Callable[P, Awaitable[T]]:
        @wraps(function)
        async def contextual(*args: P.args, **kwargs: P.kwargs) -> T:
            parent = current_context() or EventContext()
            token = _CURRENT_CONTEXT.set(parent.extend(*normalised))
            try:
                return await function(*args, **kwargs)
            finally:
                _CURRENT_CONTEXT.reset(token)

        return contextual

    return decorate


def event_segment(value: str) -> EventSegment:
    """Create a reusable event-topic segment."""

    return EventSegment(value)


def _callable_identity(value: object) -> str:
    """Return a stable diagnostic identity without inspecting payload values."""

    value_type = type(value)
    module = getattr(value, "__module__", value_type.__module__)
    qualname = getattr(value, "__qualname__", value_type.__qualname__)
    return f"{module}.{qualname}"


def _record_dispatch_diagnostic(
    event: Event,
    *,
    duration_seconds: float,
    handler_failed: bool,
) -> None:
    try:
        # Diagnostics owns its context API and imports scope values from this
        # module, so defer this optional observer import to avoid a cycle.
        from wybra.diagnostics.context import record_topic

        record_topic(
            "trace",
            EVT_EVENTS(DISPATCH),
            attributes={
                "event_type": _callable_identity(type(event)),
                "event_scope": str(event.scope),
            },
            duration_seconds=duration_seconds,
            result="error" if handler_failed else "ok",
        )
    except Exception:
        logger.exception("Unable to record event dispatch diagnostics")


def _record_handler_diagnostic(
    event: Event,
    handler: EventHandler,
    *,
    duration_seconds: float,
    error: BaseException | None = None,
) -> None:
    try:
        # See _record_dispatch_diagnostic() for the deferred observer import.
        from wybra.diagnostics.context import record_topic

        attributes: dict[str, str] = {
            "event_type": _callable_identity(type(event)),
            "event_scope": str(event.scope),
            "handler": _callable_identity(handler),
        }
        if error is not None:
            attributes["error_type"] = type(error).__name__
        failure = error is not None
        record_topic(
            "info" if failure else "debug",
            (
                EVT_EVENTS_ERRORS(HANDLER, FAILED)
                if failure
                else EVT_EVENTS(HANDLER, SUCCEEDED)
            ),
            attributes=attributes,
            duration_seconds=duration_seconds,
            result="error" if failure else "ok",
        )
    except Exception:
        logger.exception("Unable to record event handler diagnostics")


def event_scope(value: str) -> EventScope:
    """Create a root event scope from dot-separated literal segments."""

    return EventScope(tuple(value.split(".")))


def parse_event_scopes(
    value: str | Iterable[str | EventScope],
) -> tuple[EventScope, ...]:
    """Parse configured selectors against Wybra's registered root scopes."""

    values = value.split(",") if isinstance(value, str) else value
    scopes = tuple(
        item if isinstance(item, EventScope) else event_scope(item.strip())
        for item in values
        if isinstance(item, EventScope) or item.strip()
    )
    if not scopes:
        raise EventScopeError("At least one event scope must be configured.")
    unknown_roots = sorted({scope.segments[0] for scope in scopes} - _ROOT_SCOPE_NAMES)
    if unknown_roots:
        raise EventScopeError(
            "Unknown event scope root: " + ", ".join(unknown_roots) + "."
        )
    return scopes


def _normalise_segment(value: str) -> str:
    segment = value.strip().lower()
    if not segment or segment == "*" or "." in segment:
        raise EventScopeError(
            "Event scope segments must be non-blank dot-free names; wildcards are "
            "not supported."
        )
    if not all(
        character.isascii() and (character.isalnum() or character == "_")
        for character in segment
    ):
        raise EventScopeError(
            f"Event scope segment {value!r} must use ASCII letters, digits, or '_'."
        )
    return segment


SQL_STATEMENT: Final = event_segment("statement")
CONNECTION: Final = event_segment("connection")
MODEL: Final = event_segment("model")
TEMPLATE_RENDER: Final = event_segment("render")
CACHE_READ: Final = event_segment("read")
CACHE_SET: Final = event_segment("set")
CACHE_DELETE: Final = event_segment("delete")
CACHE_FILL: Final = event_segment("fill")
FORM: Final = event_segment("form")
VALIDATION: Final = event_segment("validation")
PERSISTENCE: Final = event_segment("persistence")
ACCOUNT: Final = event_segment("account")
CREDENTIAL: Final = event_segment("credential")
SESSION: Final = event_segment("session")
SECURITY: Final = event_segment("security")
POLICY: Final = event_segment("policy")
CSRF: Final = event_segment("csrf")
TRANSACTION: Final = event_segment("transaction")
SAVEPOINT: Final = event_segment("savepoint")
MODULE: Final = event_segment("module")
CAPABILITY: Final = event_segment("capability")
REQUEST: Final = event_segment("request")
ROUTE: Final = event_segment("route")
VIEW: Final = event_segment("view")
GENERIC: Final = event_segment("generic")
COLLECTION: Final = event_segment("collection")
ITEM: Final = event_segment("item")
BULK: Final = event_segment("bulk")
SETUP: Final = event_segment("setup")
POST_SETUP: Final = event_segment("post_setup")
SETUP_FINALISATION: Final = event_segment("setup_finalisation")
STARTUP: Final = event_segment("startup")
SHUTDOWN: Final = event_segment("shutdown")
STARTED: Final = event_segment("started")
COMPLETED: Final = event_segment("completed")
RESOLVED: Final = event_segment("resolved")
UNAVAILABLE: Final = event_segment("unavailable")
DISPATCH: Final = event_segment("dispatch")
HANDLER: Final = event_segment("handler")
ERRORS: Final = event_segment("errors")
SUCCEEDED: Final = event_segment("succeeded")
FAILED: Final = event_segment("failed")
DENIED: Final = event_segment("denied")
BEGIN: Final = event_segment("begin")
COMMIT: Final = event_segment("commit")
ROLLBACK: Final = event_segment("rollback")
RELEASE: Final = event_segment("release")
EVT_SQL: Final = event_scope("sql")
EVT_TEMPLATE: Final = event_scope("template")
EVT_CACHE: Final = event_scope("cache")
EVT_FORM: Final = event_scope("form")
EVT_ACCOUNT: Final = event_scope("account")
EVT_CREDENTIAL: Final = event_scope("credential")
EVT_SESSION: Final = event_scope("session")
EVT_SECURITY: Final = event_scope("security")
EVT_CONTENT_TYPES: Final = event_scope("content_types")
EVT_EVENTS: Final = event_scope("events")
EVT_EVENTS_ERRORS: Final = EVT_EVENTS(ERRORS)
EVT_SITE: Final = event_scope("site")
EVT_REQUEST: Final = event_scope("request")
EVT_ROUTE: Final = event_scope("route")
EVT_VIEW: Final = event_scope("view")
EVT_TASK: Final = event_scope("task")
DEFAULT_EVENT_SCOPES: Final = (EVT_SQL, EVT_TEMPLATE, EVT_EVENTS_ERRORS)
_ROOT_SCOPE_NAMES: Final = frozenset(
    scope.segments[0]
    for scope in (
        EVT_SQL,
        EVT_TEMPLATE,
        EVT_CACHE,
        EVT_FORM,
        EVT_ACCOUNT,
        EVT_CREDENTIAL,
        EVT_SESSION,
        EVT_SECURITY,
        EVT_CONTENT_TYPES,
        EVT_EVENTS,
        EVT_SITE,
        EVT_REQUEST,
        EVT_ROUTE,
        EVT_VIEW,
        EVT_TASK,
    )
)
_SCOPE_DESCRIPTIONS: Final = {
    EVT_SQL: "Database statement and transaction diagnostics.",
    EVT_TEMPLATE: "Template rendering diagnostics.",
    EVT_CACHE: "Cache operation observations.",
    EVT_FORM: "Form validation and persistence observations.",
    EVT_ACCOUNT: "Account lifecycle observations.",
    EVT_CREDENTIAL: "Credential and access-control observations.",
    EVT_SESSION: "Session lifecycle observations without session data.",
    EVT_SECURITY: "Security-policy and denial observations.",
    EVT_CONTENT_TYPES: "Finalised content-type mappings.",
    EVT_EVENTS: "Application event dispatch and handler outcomes.",
    EVT_EVENTS_ERRORS: "Application event-handler failures.",
    EVT_SITE: "Site and configured-module lifecycle observations.",
    EVT_REQUEST: "HTTP request lifecycle observations.",
    EVT_ROUTE: "Resolved HTTP route dispatch observations.",
    EVT_VIEW: "Class-based view execution observations.",
    EVT_TASK: "Background task lifecycle observations.",
}


def available_event_scopes() -> tuple[tuple[EventScope, str], ...]:
    """Return public event selectors and their developer-facing descriptions."""

    return tuple(_SCOPE_DESCRIPTIONS.items())


def event_delivery_enabled(dispatcher: EventsCapability | None) -> bool:
    """Return whether an optional dispatcher will retain observations.

    Test and application-provided dispatchers that predate the optional
    ``enabled()`` fast path remain treated as enabled.
    """

    if dispatcher is None:
        return False
    enabled = getattr(dispatcher, "enabled", None)
    if not callable(enabled):
        return True
    try:
        return bool(enabled())
    except BaseException:
        logger.exception("Unable to determine whether event delivery is enabled")
        return False


def _external_cancellation_requested() -> bool:
    """Return whether cancellation was requested for the producer task itself."""

    task = current_task()
    return task is not None and task.cancelling() > 0


__all__ = (
    "EVT_CONTENT_TYPES",
    "EVT_EVENTS",
    "EVT_EVENTS_ERRORS",
    "EVT_REQUEST",
    "EVT_CACHE",
    "EVT_FORM",
    "EVT_ACCOUNT",
    "EVT_CREDENTIAL",
    "EVT_SESSION",
    "EVT_SECURITY",
    "EVT_ROUTE",
    "EVT_SITE",
    "DEFAULT_EVENT_SCOPES",
    "event_delivery_enabled",
    "events_enabled",
    "EVT_SQL",
    "EVT_TEMPLATE",
    "EVT_TASK",
    "EVT_VIEW",
    "BEGIN",
    "CACHE_DELETE",
    "CACHE_FILL",
    "CACHE_READ",
    "CACHE_SET",
    "ACCOUNT",
    "CREDENTIAL",
    "SESSION",
    "SECURITY",
    "POLICY",
    "CSRF",
    "FORM",
    "BULK",
    "COLLECTION",
    "COMMIT",
    "CONNECTION",
    "current_context",
    "MODEL",
    "MODULE",
    "CAPABILITY",
    "REQUEST",
    "ROUTE",
    "SETUP",
    "POST_SETUP",
    "SETUP_FINALISATION",
    "STARTUP",
    "SHUTDOWN",
    "PERSISTENCE",
    "STARTED",
    "COMPLETED",
    "RESOLVED",
    "UNAVAILABLE",
    "DISPATCH",
    "ERRORS",
    "FAILED",
    "DENIED",
    "HANDLER",
    "ITEM",
    "RELEASE",
    "ROLLBACK",
    "SAVEPOINT",
    "SQL_STATEMENT",
    "TEMPLATE_RENDER",
    "TRANSACTION",
    "VALIDATION",
    "SUCCEEDED",
    "VIEW",
    "EventScope",
    "EventScopeError",
    "EventRuntimeError",
    "EventSegment",
    "EventContext",
    "EventsSettings",
    "Event",
    "GENERIC",
    "EventDispatcher",
    "EventHandler",
    "EventDescriptor",
    "EventOutcome",
    "EventsCapability",
    "available_event_scopes",
    "event_scope",
    "event_segment",
    "parse_event_scopes",
    "observe",
    "context",
    "setup_core_events",
)
