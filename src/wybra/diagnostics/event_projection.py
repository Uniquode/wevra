"""Passive projection of typed Wybra events into diagnostics contexts."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Final
from uuid import UUID

from wybra.diagnostics.context import record_sql_operation, record_topic
from wybra.events import (
    Event,
    EventsCapability,
    EventScope,
    available_event_scopes,
    event_segment,
)

_EVENT_ATTRIBUTE_NAMES: Final = {
    "ModuleSetupEvent": ("module", "outcome", "error_type"),
    "ModulePostSetupEvent": ("module", "outcome", "error_type"),
    "RequestStartedEvent": ("method",),
    "RequestCompletedEvent": (
        "method",
        "status_code",
        "route_name",
        "duration_seconds",
        "error_type",
    ),
    "RouteDispatchedEvent": ("method", "route_name", "view_type"),
    "ViewCompletedEvent": (
        "method",
        "route_name",
        "view_type",
        "duration_seconds",
        "status_code",
        "error_type",
    ),
    "GenericViewCompletedEvent": (
        "operation",
        "model_type",
        "content_type",
        "duration_seconds",
        "status_code",
        "affected_count",
        "skipped_count",
        "failed_count",
        "error_type",
    ),
    "TemplateRenderCompletedEvent": ("template_name", "duration_seconds", "error_type"),
    "CacheOperationCompletedEvent": (
        "owner",
        "key_fingerprint",
        "outcome",
        "duration_seconds",
    ),
    "CacheOperationFailedEvent": (
        "owner",
        "key_fingerprint",
        "operation",
        "duration_seconds",
        "error_type",
    ),
    "FormValidationCompletedEvent": (
        "form_type",
        "field_count",
        "invalid_field_count",
        "valid",
        "duration_seconds",
        "error_type",
    ),
    "FormPersistenceCompletedEvent": (
        "form_type",
        "operation",
        "affected_count",
        "created",
        "updated",
        "deleted",
        "stale_conflict",
        "duration_seconds",
    ),
    "FormPersistenceFailedEvent": (
        "form_type",
        "operation",
        "duration_seconds",
        "error_type",
    ),
    "AccountLifecycleEvent": (
        "operation",
        "outcome",
        "user_id",
        "masked_email",
        "error_type",
    ),
    "CredentialAccessEvent": (
        "operation",
        "provider",
        "outcome",
        "user_id",
        "masked_email",
        "error_type",
    ),
    "SessionLifecycleEvent": ("operation", "backend", "outcome", "error_type"),
    "SecurityPolicyEvent": ("policy", "outcome"),
    "SecurityDenialEvent": ("mechanism",),
    "CapabilityResolvedEvent": ("capability_type",),
    "CapabilityUnavailableEvent": ("capability_type",),
    "CapabilityProvidedEvent": ("capability_type",),
    "SiteLifecycleEvent": ("phase", "error_count"),
    "DatabaseConnectionEvent": ("connection_name",),
    "DatabaseStatementEvent": (
        "connection_name",
        "operation",
        "duration_seconds",
        "result",
        "result_count",
        "inserted_id",
    ),
    "DatabaseTransactionEvent": ("connection_name", "transaction_kind", "outcome"),
    "DatabaseSavepointEvent": ("connection_name", "outcome"),
    "TaskLifecycleObservationEvent": (
        "kind",
        "task_id",
        "task_name",
        "schema_version",
        "queue",
        "correlation_id",
        "causation_id",
        "attempt",
        "worker_id",
        "error_type",
    ),
}


class DiagnosticsEventProjection:
    """Record selected, safe event metadata through the active diagnostics context."""

    def __init__(self, selectors: Iterable[EventScope]) -> None:
        self._selectors = tuple(selectors)

    async def __call__(self, event: Event) -> None:
        if not any(event.scope.matches(selector) for selector in self._selectors):
            return
        if str(event.scope) == "sql.statement":
            _record_sql_statement(event)
            return
        record_topic(
            "trace",
            _diagnostic_topic(event),
            attributes=_event_attributes(event),
            result=_event_result(event),
        )


async def register_event_projection(
    capability: EventsCapability,
    selectors: Iterable[EventScope],
) -> None:
    """Register and replay one passive projection for public event roots."""

    projection = DiagnosticsEventProjection(selectors)
    for scope, _description in available_event_scopes():
        if len(scope.segments) == 1:
            await capability.subscribe(scope, projection, history=True)


def _event_attributes(event: Event) -> dict[str, str | int | float | bool | None]:
    attributes: dict[str, str | int | float | bool | None] = {
        "event_type": f"{type(event).__module__}.{type(event).__qualname__}",
    }
    if event.context is not None and event.context.request_id is not None:
        attributes["event_context_request_id"] = str(event.context.request_id)
    for attribute_name in _EVENT_ATTRIBUTE_NAMES.get(type(event).__name__, ()):
        value = getattr(event, attribute_name, None)
        if isinstance(value, UUID):
            attributes[attribute_name] = str(value)
        elif isinstance(value, str | int | float | bool) or value is None:
            attributes[attribute_name] = value
    return attributes


def _event_result(event: Event) -> str | None:
    error_type = getattr(event, "error_type", None)
    if isinstance(error_type, str) and error_type:
        return "error"
    outcome = getattr(event, "outcome", None)
    if outcome == "succeeded":
        return "ok"
    if outcome == "failed":
        return "error"
    return None


def _record_sql_statement(event: Event) -> None:
    duration_seconds = getattr(event, "duration_seconds", None)
    if not isinstance(duration_seconds, float):
        return
    operation = getattr(event, "operation", None)
    result = getattr(event, "result", None)
    result_count = getattr(event, "result_count", None)
    inserted_id = getattr(event, "inserted_id", None)
    record_sql_operation(
        duration_seconds=duration_seconds,
        result=result if isinstance(result, str) else "ok",
        operation=operation if isinstance(operation, str) else None,
        result_count=result_count if isinstance(result_count, int) else None,
        inserted_id=inserted_id if isinstance(inserted_id, int) else None,
        attributes=_event_attributes(event),
    )


def _diagnostic_topic(event: Event) -> EventScope:
    if str(event.scope) not in {"sql.transaction", "sql.savepoint"}:
        return event.scope
    outcome = getattr(event, "outcome", None)
    return (
        event.scope(event_segment(outcome)) if isinstance(outcome, str) else event.scope
    )


__all__ = (
    "DiagnosticsEventProjection",
    "register_event_projection",
)
