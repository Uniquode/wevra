from __future__ import annotations

import time
from collections import Counter
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Final
from uuid import uuid7

from wybra.diagnostics.levels import (
    DIAGNOSTIC_LEVEL_VALUES,
    DiagnosticLevel,
    normalise_diagnostics_level,
)
from wybra.events import (
    EventScope,
    event_scope,
)
from wybra.utils.safety import (
    MAX_SAFE_STRING_LENGTH,
    is_sensitive_attribute_name,
    truncate_safe_string,
)

MAX_CONTEXT_DESCRIPTION_LENGTH: Final = 200
_SQL_STATEMENT_TOPIC: Final = event_scope("sql.statement")
_TEMPLATE_RENDER_TOPIC: Final = event_scope("template.render")


@dataclass(frozen=True, slots=True)
class DiagnosticContext:
    """Correlation metadata for request, task, lifecycle, or explicit work."""

    kind: str
    description: str
    identifier: str = field(default_factory=lambda: str(uuid7()))
    parent_identifier: str | None = None

    def __post_init__(self) -> None:
        kind = self.kind.strip().lower()
        description = self.description.strip()
        if not kind or not kind.replace("_", "").isalnum():
            raise ValueError("Diagnostic context kind must be a non-blank identifier.")
        if not description:
            raise ValueError("Diagnostic context description must not be blank.")
        if len(description) > MAX_CONTEXT_DESCRIPTION_LENGTH:
            description = f"{description[:MAX_CONTEXT_DESCRIPTION_LENGTH]}..."
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "description", description)


@dataclass(frozen=True, slots=True)
class DiagnosticEvent:
    level: DiagnosticLevel
    category: str
    name: str
    timestamp: float
    attributes: Mapping[str, object] = field(default_factory=dict)
    duration_seconds: float | None = None
    result: str | None = None

    def as_dict(self) -> dict[str, object]:
        values: dict[str, object] = {
            "level": self.level,
            "category": self.category,
            "name": self.name,
            "timestamp": self.timestamp,
            "attributes": dict(self.attributes),
        }
        if self.duration_seconds is not None:
            values["duration_seconds"] = self.duration_seconds
        if self.result is not None:
            values["result"] = self.result
        return values


@dataclass(slots=True)
class RequestDiagnostics:
    method: str
    path: str
    level: DiagnosticLevel = "info"
    slow_sql_threshold_seconds: float = 0.5
    route_name: str | None = None
    status_code: int | None = None
    exception_type: str | None = None
    duration_seconds: float | None = None
    events: list[DiagnosticEvent] = field(default_factory=list)
    sql_query_count: int = 0
    sql_total_duration_seconds: float = 0.0
    template_render_count: int = 0
    template_total_duration_seconds: float = 0.0
    backend_operation_count: int = 0
    context: DiagnosticContext | None = None

    def allows(self, level: DiagnosticLevel) -> bool:
        return DIAGNOSTIC_LEVEL_VALUES[level] >= DIAGNOSTIC_LEVEL_VALUES[self.level]

    def record_event(
        self,
        level: DiagnosticLevel,
        category: str,
        name: str,
        *,
        attributes: Mapping[str, object] | None = None,
        duration_seconds: float | None = None,
        result: str | None = None,
    ) -> None:
        if not self.allows(level):
            return
        self.events.append(
            DiagnosticEvent(
                level=level,
                category=category.strip() or "diagnostic",
                name=name.strip() or "event",
                timestamp=time.time(),
                attributes=_safe_attributes(attributes or {}),
                duration_seconds=_positive_duration(duration_seconds),
                result=_safe_result(result),
            )
        )

    def record_sql_query(
        self,
        statement: str,
        *,
        duration_seconds: float,
        result: str = "ok",
        operation: str | None = None,
        result_count: int | None = None,
        inserted_id: int | None = None,
    ) -> None:
        self._record_sql_observation(
            duration_seconds=duration_seconds,
            result=result,
            operation=operation,
            result_count=result_count,
            inserted_id=inserted_id,
            attributes={"statement": _normalise_sql_statement(statement)},
        )

    def record_sql_operation(
        self,
        *,
        duration_seconds: float,
        result: str = "ok",
        operation: str | None = None,
        result_count: int | None = None,
        inserted_id: int | None = None,
        attributes: dict[str, object] | None = None,
    ) -> None:
        """Record a SQL observation whose statement text is intentionally absent."""

        self._record_sql_observation(
            duration_seconds=duration_seconds,
            result=result,
            operation=operation,
            result_count=result_count,
            inserted_id=inserted_id,
            attributes=dict(attributes or {}),
        )

    def _record_sql_observation(
        self,
        *,
        duration_seconds: float,
        result: str,
        operation: str | None,
        result_count: int | None,
        inserted_id: int | None,
        attributes: dict[str, object],
    ) -> None:
        duration = _positive_duration(duration_seconds) or 0.0
        self.sql_query_count += 1
        self.sql_total_duration_seconds += duration
        if self.allows("trace"):
            level: DiagnosticLevel | None = "trace"
        elif duration >= self.slow_sql_threshold_seconds:
            level = "info"
        else:
            level = None
        if level is not None:
            if operation is not None:
                attributes["operation"] = operation
            if result_count is not None:
                attributes["result_count"] = result_count
            if inserted_id is not None:
                attributes["inserted_id"] = inserted_id
            self.record_topic(
                level,
                _SQL_STATEMENT_TOPIC,
                attributes=attributes,
                duration_seconds=duration,
                result=result,
            )

    def record_template_render(
        self,
        template_name: str,
        *,
        duration_seconds: float,
        result: str = "ok",
    ) -> None:
        duration = _positive_duration(duration_seconds) or 0.0
        self.template_render_count += 1
        self.template_total_duration_seconds += duration
        self.record_topic(
            "trace",
            _TEMPLATE_RENDER_TOPIC,
            attributes={"template": template_name},
            duration_seconds=duration,
            result=result,
        )

    def record_backend_operation(
        self,
        category: str,
        name: str,
        *,
        attributes: Mapping[str, object] | None = None,
        duration_seconds: float | None = None,
        result: str | None = None,
        level: DiagnosticLevel = "debug",
    ) -> None:
        self.backend_operation_count += 1
        self.record_event(
            level,
            category,
            name,
            attributes=attributes,
            duration_seconds=duration_seconds,
            result=result,
        )

    def record_topic(
        self,
        level: DiagnosticLevel,
        topic: EventScope,
        *,
        attributes: Mapping[str, object] | None = None,
        duration_seconds: float | None = None,
        result: str | None = None,
    ) -> None:
        """Record an event from a validated public topic value."""

        category, *name = topic.segments
        self.record_event(
            level,
            category,
            ".".join(name),
            attributes=attributes,
            duration_seconds=duration_seconds,
            result=result,
        )

    def finish(
        self,
        *,
        route_name: str | None,
        status_code: int | None,
        exception_type: str | None,
        duration_seconds: float,
    ) -> None:
        self.route_name = route_name
        self.status_code = status_code
        self.exception_type = exception_type
        self.duration_seconds = _positive_duration(duration_seconds)
        self.record_event(
            "info",
            "request",
            "completed",
            attributes={
                "method": self.method,
                "route": route_name,
                "status_code": status_code,
                "exception_type": exception_type,
            },
            duration_seconds=self.duration_seconds,
            result="error" if exception_type else "ok",
        )

    def summary(self) -> dict[str, object]:
        event_counts = Counter(event.category for event in self.events)
        return {
            "context": (
                {
                    "id": self.context.identifier,
                    "parent_id": self.context.parent_identifier,
                    "kind": self.context.kind,
                    "description": self.context.description,
                }
                if self.context is not None
                else None
            ),
            "method": self.method,
            "route": self.route_name,
            "status_code": self.status_code,
            "exception_type": self.exception_type,
            "duration_seconds": self.duration_seconds,
            "event_counts": dict(event_counts),
            "sql_query_count": self.sql_query_count,
            "sql_total_duration_seconds": self.sql_total_duration_seconds,
            "template_render_count": self.template_render_count,
            "template_total_duration_seconds": self.template_total_duration_seconds,
            "backend_operation_count": self.backend_operation_count,
        }


def _safe_attributes(attributes: Mapping[str, object]) -> dict[str, object]:
    safe: dict[str, object] = {}
    for key, value in attributes.items():
        key_name = str(key)
        if is_sensitive_attribute_name(key_name):
            safe[key_name] = "[redacted]"
        else:
            safe[key_name] = _safe_value(value)
    return safe


def _safe_value(value: object) -> object:
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, str):
        return truncate_safe_string(
            value,
            maximum_length=MAX_SAFE_STRING_LENGTH,
        )
    return f"<{type(value).__name__}>"


def _safe_result(result: str | None) -> str | None:
    if result is None:
        return None
    return str(_safe_value(result))


def _positive_duration(duration_seconds: float | None) -> float | None:
    if duration_seconds is None:
        return None
    return max(0.0, float(duration_seconds))


def _normalise_sql_statement(statement: str) -> str:
    return " ".join(statement.split())


__all__ = (
    "DIAGNOSTIC_LEVEL_VALUES",
    "DiagnosticContext",
    "DiagnosticEvent",
    "DiagnosticLevel",
    "RequestDiagnostics",
    "normalise_diagnostics_level",
)
