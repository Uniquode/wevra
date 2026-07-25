"""Secret-safe process-local task lifecycle observations."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from uuid import UUID

from wybra.events import Event, EventScope, event_scope
from wybra.tasks.lifecycle import (
    TaskLifecycleEvent,
    TaskLifecycleKind,
    TaskProgressError,
)
from wybra.utils.safety import SafeJsonMetadata, safe_json_metadata

TASK_EVENT_SCOPE = event_scope("task")


@dataclass(frozen=True, slots=True, kw_only=True)
class TaskLifecycleObservationEvent(Event):
    """A provider-neutral observation of one task lifecycle transition."""

    kind: TaskLifecycleKind
    task_id: UUID
    task_name: str
    schema_version: int
    queue: str
    correlation_id: UUID
    causation_id: UUID | None
    attempt: int
    worker_id: str | None
    progress: Mapping[str, object] | None = field(default=None, repr=False)
    error_type: str | None = None

    def __post_init__(self, topic: EventScope | None) -> None:
        Event.__post_init__(self, topic)
        if self.progress is None or isinstance(self.progress, SafeJsonMetadata):
            return
        try:
            progress = safe_json_metadata(self.progress)
        except (TypeError, ValueError) as exc:
            raise TaskProgressError(
                "Task observation progress must be JSON-compatible and within "
                "safe limits."
            ) from exc
        object.__setattr__(self, "progress", progress)

    @classmethod
    def from_lifecycle(
        cls,
        event: TaskLifecycleEvent,
    ) -> TaskLifecycleObservationEvent:
        return cls(
            topic=TASK_EVENT_SCOPE(event.kind.value),
            occurred_at=event.occurred_at,
            kind=event.kind,
            task_id=event.task_id,
            task_name=event.task_name,
            schema_version=event.schema_version,
            queue=event.queue,
            correlation_id=event.correlation_id,
            causation_id=event.causation_id,
            attempt=event.attempt,
            worker_id=event.worker_id,
            progress=event.progress,
            error_type=event.error_type,
        )


__all__ = ("TASK_EVENT_SCOPE", "TaskLifecycleObservationEvent")
