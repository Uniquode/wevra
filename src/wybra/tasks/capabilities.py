from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from math import log
from random import random
from typing import Protocol, runtime_checkable
from uuid import UUID

import anyio

from wybra.tasks.context import current_task_context
from wybra.tasks.declarations import TaskDefinition
from wybra.tasks.lifecycle import (
    DEFAULT_TASK_HISTORY_LIMIT,
    TaskLifecycleEvent,
    TaskLifecycleKind,
    TaskStatus,
    TaskStatusProjection,
)
from wybra.tasks.models import (
    RetryPolicy,
    TaskExecutionContext,
    TaskIdentity,
    TaskPayload,
)
from wybra.tasks.settings import TasksSettings


class TaskDispatchPolicy(StrEnum):
    DIRECT = "direct"
    BACKGROUND = "background"
    PREFER_BACKGROUND = "prefer_background"


@dataclass(frozen=True, slots=True)
class TaskSubmissionOptions:
    queue: str | None = None
    idempotency_key: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "queue",
            _normalise_submission_value(self.queue, "queue"),
        )
        object.__setattr__(
            self,
            "idempotency_key",
            _normalise_submission_value(
                self.idempotency_key,
                "idempotency key",
            ),
        )


@dataclass(frozen=True, slots=True)
class TaskHandle:
    task_id: UUID
    identity: TaskIdentity
    _status_getter: Callable[[UUID], Awaitable[TaskStatus | None]] = field(repr=False)

    async def status(self) -> TaskStatus | None:
        return await self._status_getter(self.task_id)


@runtime_checkable
class TasksCapability(Protocol):
    async def submit(
        self,
        definition: TaskDefinition,
        payload: TaskPayload,
        *,
        options: TaskSubmissionOptions | None = None,
    ) -> TaskHandle: ...

    async def status(self, task_id: UUID) -> TaskStatus | None: ...

    async def lifecycle(
        self,
        task_id: UUID,
    ) -> tuple[TaskLifecycleEvent, ...]: ...


@dataclass(slots=True)
class ImmediateTasksCapability:
    settings: TasksSettings
    _random: Callable[[], float] = field(default=random, repr=False)
    _sleep: Callable[[float], Awaitable[object]] = field(
        default=anyio.sleep,
        repr=False,
    )
    _projection: TaskStatusProjection = field(init=False, repr=False)

    def __post_init__(self) -> None:
        self._projection = TaskStatusProjection(
            retention_seconds=self.settings.status_retention_seconds,
            history_limit=DEFAULT_TASK_HISTORY_LIMIT,
        )

    async def submit(
        self,
        definition: TaskDefinition,
        payload: TaskPayload,
        *,
        options: TaskSubmissionOptions | None = None,
    ) -> TaskHandle:
        payload = definition.validate_payload(payload)
        selected = options or TaskSubmissionOptions()
        queue = selected.queue or self.settings.default_queue
        parent = current_task_context()
        submitted = TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.SUBMITTED,
            task_name=definition.identity.name,
            schema_version=definition.identity.version,
            queue=queue,
            correlation_id=parent.correlation_id if parent is not None else None,
            causation_id=parent.task_id if parent is not None else None,
        )
        self._projection.apply(submitted)
        retry = _effective_retry(definition.retry, self.settings.retry_policy)
        for attempt in range(1, retry.max_attempts + 1):
            started = _next_event(
                submitted,
                TaskLifecycleKind.STARTED,
                attempt=attempt,
                worker_id="immediate",
            )
            self._projection.apply(started)
            context = TaskExecutionContext(
                task_id=submitted.task_id,
                task_name=submitted.task_name,
                schema_version=submitted.schema_version,
                attempt=attempt,
                correlation_id=submitted.correlation_id,
                causation_id=submitted.causation_id,
                idempotency_key=selected.idempotency_key,
                queue=queue,
                worker_id="immediate",
            )
            try:
                await definition.execute(payload, context)
            except Exception as exc:
                if attempt < retry.max_attempts:
                    self._projection.apply(
                        _next_event(
                            submitted,
                            TaskLifecycleKind.RETRY_SCHEDULED,
                            attempt=attempt,
                            error_type=type(exc).__name__,
                        )
                    )
                    await self._sleep(
                        _retry_delay(retry, attempt, random_value=self._random())
                    )
                    continue
                self._projection.apply(
                    _next_event(
                        submitted,
                        TaskLifecycleKind.FAILED,
                        attempt=attempt,
                        error_type=type(exc).__name__,
                    )
                )
                self._projection.apply(
                    _next_event(
                        submitted,
                        TaskLifecycleKind.DEAD_LETTERED,
                        attempt=attempt,
                        error_type=type(exc).__name__,
                    )
                )
                break
            else:
                self._projection.apply(
                    _next_event(
                        submitted,
                        TaskLifecycleKind.SUCCEEDED,
                        attempt=attempt,
                    )
                )
                break
        return TaskHandle(
            task_id=submitted.task_id,
            identity=definition.identity,
            _status_getter=self.status,
        )

    async def status(self, task_id: UUID) -> TaskStatus | None:
        return self._projection.status(task_id)

    async def lifecycle(
        self,
        task_id: UUID,
    ) -> tuple[TaskLifecycleEvent, ...]:
        return self._projection.lifecycle(task_id)

    async def close(self) -> None:
        return None


async def dispatch(
    site,
    definition: TaskDefinition,
    payload: TaskPayload,
    *,
    policy: TaskDispatchPolicy,
    options: TaskSubmissionOptions | None = None,
) -> object:
    from wybra.site import Site

    if not isinstance(site, Site):
        raise TypeError("Task dispatch requires a Site.")
    if policy is TaskDispatchPolicy.DIRECT:
        _reject_direct_submission_options(options)
        return await definition.run(**payload.arguments)
    capability = site.optional_capability(TasksCapability)
    if policy is TaskDispatchPolicy.BACKGROUND:
        if capability is None:
            raise RuntimeError("Background task dispatch requires TasksCapability.")
        return await capability.submit(definition, payload, options=options)
    if policy is TaskDispatchPolicy.PREFER_BACKGROUND:
        if capability is None:
            _reject_direct_submission_options(options)
            return await definition.run(**payload.arguments)
        return await capability.submit(definition, payload, options=options)
    raise ValueError(f"Unsupported task dispatch policy: {policy!r}.")


def _effective_retry(
    declared: RetryPolicy | None,
    configured: RetryPolicy,
) -> RetryPolicy:
    return configured if declared is None else declared


def _retry_delay(
    policy: RetryPolicy,
    attempt: int,
    *,
    random_value: float,
) -> float:
    exponent = max(0, attempt - 1)
    delay = policy.initial_delay_seconds
    maximum_delay = policy.maximum_delay_seconds
    if exponent > 0 and delay > 0 and policy.backoff_multiplier > 1:
        if maximum_delay is not None:
            cap_exponent = (log(maximum_delay) - log(delay)) / log(
                policy.backoff_multiplier
            )
            if exponent >= cap_exponent:
                delay = maximum_delay
            else:
                delay *= policy.backoff_multiplier**exponent
        else:
            delay *= policy.backoff_multiplier**exponent
    delay += policy.jitter_seconds * random_value
    if maximum_delay is not None:
        return min(delay, maximum_delay)
    return delay


def _normalise_submission_value(
    value: str | None,
    label: str,
) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError(f"Task submission {label} must be a string.")
    normalised = value.strip()
    if not normalised:
        raise ValueError(f"Task submission {label} cannot be blank.")
    return normalised


def _reject_direct_submission_options(
    options: TaskSubmissionOptions | None,
) -> None:
    if options is not None:
        raise ValueError(
            "Task submission options cannot be used with direct execution."
        )


def _next_event(
    origin: TaskLifecycleEvent,
    kind: TaskLifecycleKind,
    *,
    attempt: int,
    worker_id: str | None = None,
    error_type: str | None = None,
) -> TaskLifecycleEvent:
    return TaskLifecycleEvent.new(
        kind=kind,
        task_id=origin.task_id,
        task_name=origin.task_name,
        schema_version=origin.schema_version,
        queue=origin.queue,
        correlation_id=origin.correlation_id,
        causation_id=origin.causation_id,
        attempt=attempt,
        worker_id=worker_id,
        error_type=error_type,
    )


__all__ = (
    "ImmediateTasksCapability",
    "TaskDispatchPolicy",
    "TaskHandle",
    "TaskSubmissionOptions",
    "TasksCapability",
    "dispatch",
)
