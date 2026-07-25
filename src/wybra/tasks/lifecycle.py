from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from math import isfinite
from typing import Final
from uuid import UUID, uuid7

from wybra.utils.safety import SafeJsonMetadata, safe_json_metadata


class TaskLifecycleError(RuntimeError):
    """Raised when a task lifecycle transition is invalid."""


class TaskProgressError(TaskLifecycleError):
    """Raised when task progress metadata is invalid."""


class TaskLifecycleKind(StrEnum):
    SUBMITTED = "submitted"
    SCHEDULED = "scheduled"
    STARTED = "started"
    PROGRESS = "progress"
    RETRY_SCHEDULED = "retry_scheduled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"


class TaskState(StrEnum):
    SUBMITTED = "submitted"
    SCHEDULED = "scheduled"
    RUNNING = "running"
    RETRY_SCHEDULED = "retry_scheduled"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    DEAD_LETTERED = "dead_lettered"


_KIND_STATE: Final = {
    TaskLifecycleKind.SUBMITTED: TaskState.SUBMITTED,
    TaskLifecycleKind.SCHEDULED: TaskState.SCHEDULED,
    TaskLifecycleKind.STARTED: TaskState.RUNNING,
    TaskLifecycleKind.PROGRESS: TaskState.RUNNING,
    TaskLifecycleKind.RETRY_SCHEDULED: TaskState.RETRY_SCHEDULED,
    TaskLifecycleKind.SUCCEEDED: TaskState.SUCCEEDED,
    TaskLifecycleKind.FAILED: TaskState.FAILED,
    TaskLifecycleKind.DEAD_LETTERED: TaskState.DEAD_LETTERED,
}
_ALLOWED_TRANSITIONS: Final = {
    None: frozenset({TaskLifecycleKind.SUBMITTED}),
    TaskState.SUBMITTED: frozenset(
        {TaskLifecycleKind.SCHEDULED, TaskLifecycleKind.STARTED}
    ),
    TaskState.SCHEDULED: frozenset({TaskLifecycleKind.STARTED}),
    TaskState.RUNNING: frozenset(
        {
            TaskLifecycleKind.PROGRESS,
            TaskLifecycleKind.RETRY_SCHEDULED,
            TaskLifecycleKind.SUCCEEDED,
            TaskLifecycleKind.FAILED,
        }
    ),
    TaskState.RETRY_SCHEDULED: frozenset({TaskLifecycleKind.STARTED}),
    TaskState.FAILED: frozenset({TaskLifecycleKind.DEAD_LETTERED}),
    TaskState.SUCCEEDED: frozenset(),
    TaskState.DEAD_LETTERED: frozenset(),
}
_TERMINAL_STATES: Final = frozenset(
    {
        TaskState.SUCCEEDED,
        TaskState.DEAD_LETTERED,
    }
)
DEFAULT_TASK_HISTORY_LIMIT: Final = 100
DEFAULT_TASK_REPLAY_LIMIT: Final = 1000
DEFAULT_TASK_STATUS_RETENTION_SECONDS: Final = 3600.0
type _TaskEventKey = tuple[object, ...]


@dataclass(frozen=True, slots=True)
class TaskLifecycleEvent:
    kind: TaskLifecycleKind
    task_id: UUID
    task_name: str
    schema_version: int
    queue: str
    correlation_id: UUID
    causation_id: UUID | None = None
    attempt: int = 1
    worker_id: str | None = None
    progress: Mapping[str, object] | None = field(default=None, repr=False)
    error_type: str | None = None
    occurred_at: float = field(default_factory=time.time)

    def __post_init__(self) -> None:
        if self.progress is None or isinstance(self.progress, SafeJsonMetadata):
            return
        try:
            progress = safe_json_metadata(self.progress)
        except (TypeError, ValueError) as exc:
            raise TaskProgressError(
                "Task progress must be JSON-compatible and within safe limits."
            ) from exc
        object.__setattr__(self, "progress", progress)

    @classmethod
    def new(
        cls,
        *,
        kind: TaskLifecycleKind,
        task_name: str,
        schema_version: int,
        queue: str,
        task_id: UUID | None = None,
        correlation_id: UUID | None = None,
        causation_id: UUID | None = None,
        attempt: int = 1,
        worker_id: str | None = None,
        progress: Mapping[str, object] | None = None,
        error_type: str | None = None,
    ) -> TaskLifecycleEvent:
        resolved_task_id = task_id or uuid7()
        return cls(
            kind=kind,
            task_id=resolved_task_id,
            task_name=task_name,
            schema_version=schema_version,
            queue=queue,
            correlation_id=correlation_id or resolved_task_id,
            causation_id=causation_id,
            attempt=attempt,
            worker_id=worker_id,
            progress=progress,
            error_type=error_type,
        )


@dataclass(frozen=True, slots=True)
class TaskStatus:
    task_id: UUID
    task_name: str
    schema_version: int
    state: TaskState
    queue: str
    correlation_id: UUID
    causation_id: UUID | None
    attempt: int
    submitted_at: float
    updated_at: float
    worker_id: str | None = None
    progress: Mapping[str, object] | None = field(default=None, repr=False)
    error_type: str | None = None


@dataclass(slots=True)
class TaskStatusProjection:
    retention_seconds: float = DEFAULT_TASK_STATUS_RETENTION_SECONDS
    history_limit: int = DEFAULT_TASK_HISTORY_LIMIT
    replay_limit: int | None = None
    _clock: Callable[[], float] = field(default=time.time, repr=False)
    _statuses: dict[UUID, TaskStatus] = field(default_factory=dict)
    _history: dict[UUID, deque[TaskLifecycleEvent]] = field(default_factory=dict)
    _applied_event_keys: dict[UUID, set[_TaskEventKey]] = field(
        default_factory=dict,
        repr=False,
    )
    _applied_event_order: dict[UUID, deque[_TaskEventKey]] = field(
        default_factory=dict,
        repr=False,
    )

    def __post_init__(self) -> None:
        if (
            isinstance(self.retention_seconds, bool)
            or not isinstance(self.retention_seconds, (int, float))
            or not isfinite(self.retention_seconds)
            or self.retention_seconds <= 0
        ):
            raise ValueError("Task status retention must be a positive finite number.")
        if (
            isinstance(self.history_limit, bool)
            or not isinstance(self.history_limit, int)
            or self.history_limit < 1
        ):
            raise ValueError("Task lifecycle history limit must be a positive integer.")
        replay_limit = self.replay_limit
        if replay_limit is None:
            replay_limit = max(DEFAULT_TASK_REPLAY_LIMIT, self.history_limit)
        if (
            isinstance(replay_limit, bool)
            or not isinstance(replay_limit, int)
            or replay_limit < self.history_limit
        ):
            raise ValueError(
                "Task lifecycle replay limit must be an integer greater than "
                "or equal to the history limit."
            )
        self.replay_limit = replay_limit

    def apply(self, event: TaskLifecycleEvent) -> TaskStatus:
        if (
            isinstance(event.occurred_at, bool)
            or not isinstance(event.occurred_at, (int, float))
            or not isfinite(event.occurred_at)
        ):
            raise TaskLifecycleError(
                "Task lifecycle event timestamp must be a finite number."
            )
        self._prune_expired(self._clock())
        current = self._statuses.get(event.task_id)
        event_key = _event_key(event)
        applied_event_keys = self._applied_event_keys.get(event.task_id)
        if (
            current is not None
            and applied_event_keys is not None
            and event_key in applied_event_keys
        ):
            return current
        current_state = current.state if current is not None else None
        if event.kind not in _ALLOWED_TRANSITIONS[current_state]:
            raise TaskLifecycleError(
                "Invalid task lifecycle transition "
                f"{current_state!s} -> {event.kind.value}."
            )
        if current is not None:
            _validate_identity(current, event)
            if event.occurred_at < current.updated_at:
                raise TaskLifecycleError(
                    "Task lifecycle event timestamp cannot move backwards."
                )
        _validate_attempt(current, event)
        submitted_at = event.occurred_at if current is None else current.submitted_at
        status = TaskStatus(
            task_id=event.task_id,
            task_name=event.task_name,
            schema_version=event.schema_version,
            state=_KIND_STATE[event.kind],
            queue=event.queue,
            correlation_id=event.correlation_id,
            causation_id=event.causation_id,
            attempt=event.attempt,
            submitted_at=submitted_at,
            updated_at=event.occurred_at,
            worker_id=event.worker_id
            if event.worker_id is not None
            else (current.worker_id if current is not None else None),
            progress=_next_progress(current, event),
            error_type=event.error_type,
        )
        self._statuses[event.task_id] = status
        self._history.setdefault(
            event.task_id,
            deque(maxlen=self.history_limit),
        ).append(event)
        self._record_applied_event(event.task_id, event_key)
        return status

    def status(self, task_id: UUID) -> TaskStatus | None:
        self._prune_expired(self._clock())
        return self._statuses.get(task_id)

    def lifecycle(self, task_id: UUID) -> tuple[TaskLifecycleEvent, ...]:
        self._prune_expired(self._clock())
        return tuple(self._history.get(task_id, ()))

    def _prune_expired(self, now: float) -> None:
        expired = tuple(
            task_id
            for task_id, status in self._statuses.items()
            if status.state in _TERMINAL_STATES
            and now - status.updated_at > self.retention_seconds
        )
        for task_id in expired:
            self._statuses.pop(task_id, None)
            self._history.pop(task_id, None)
            self._applied_event_keys.pop(task_id, None)
            self._applied_event_order.pop(task_id, None)

    def _record_applied_event(
        self,
        task_id: UUID,
        event_key: _TaskEventKey,
    ) -> None:
        replay_limit = self.replay_limit
        if replay_limit is None:  # pragma: no cover - resolved during initialisation
            raise TaskLifecycleError("Task lifecycle replay limit is unavailable.")
        event_keys = self._applied_event_keys.setdefault(task_id, set())
        event_order = self._applied_event_order.setdefault(task_id, deque())
        if len(event_order) >= replay_limit:
            event_keys.discard(event_order.popleft())
        event_keys.add(event_key)
        event_order.append(event_key)


def _event_key(event: TaskLifecycleEvent) -> _TaskEventKey:
    progress = (
        event.progress.to_json()
        if isinstance(event.progress, SafeJsonMetadata)
        else None
    )
    return (
        event.kind,
        event.task_id,
        event.task_name,
        event.schema_version,
        event.queue,
        event.correlation_id,
        event.causation_id,
        event.attempt,
        event.worker_id,
        progress,
        event.error_type,
        event.occurred_at,
    )


def _next_progress(
    current: TaskStatus | None,
    event: TaskLifecycleEvent,
) -> Mapping[str, object] | None:
    if event.progress is not None:
        return event.progress
    if current is None:
        return None
    if (
        current.state is TaskState.RETRY_SCHEDULED
        and event.kind is TaskLifecycleKind.STARTED
        and event.attempt > current.attempt
    ):
        return None
    return current.progress


def _validate_identity(
    current: TaskStatus,
    event: TaskLifecycleEvent,
) -> None:
    if (
        event.task_name != current.task_name
        or event.schema_version != current.schema_version
        or event.queue != current.queue
        or event.correlation_id != current.correlation_id
        or event.causation_id != current.causation_id
    ):
        raise TaskLifecycleError(
            "Task lifecycle event identity does not match the current task."
        )


def _validate_attempt(
    current: TaskStatus | None,
    event: TaskLifecycleEvent,
) -> None:
    if (
        isinstance(event.attempt, bool)
        or not isinstance(event.attempt, int)
        or event.attempt < 1
    ):
        raise TaskLifecycleError("Task lifecycle attempt must be a positive integer.")
    expected = 1
    if current is not None:
        expected = current.attempt
        if (
            current.state is TaskState.RETRY_SCHEDULED
            and event.kind is TaskLifecycleKind.STARTED
        ):
            expected += 1
    if event.attempt != expected:
        raise TaskLifecycleError(
            f"Task lifecycle attempt must be {expected}, got {event.attempt}."
        )


__all__ = (
    "TaskLifecycleError",
    "TaskLifecycleEvent",
    "TaskLifecycleKind",
    "TaskProgressError",
    "TaskState",
    "TaskStatus",
    "TaskStatusProjection",
)
