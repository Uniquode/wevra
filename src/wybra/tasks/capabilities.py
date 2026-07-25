from __future__ import annotations

import logging
from asyncio import CancelledError, current_task
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from enum import StrEnum
from math import log
from random import random
from typing import Protocol, runtime_checkable
from uuid import UUID

import anyio

from wybra.events import EventsCapability, event_delivery_enabled
from wybra.tasks.context import current_task_context
from wybra.tasks.declarations import TaskDefinition
from wybra.tasks.events import TaskLifecycleObservationEvent
from wybra.tasks.lifecycle import (
    DEFAULT_TASK_HISTORY_LIMIT,
    TaskLifecycleEvent,
    TaskLifecycleKind,
    TaskProgressError,
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

logger = logging.getLogger(__name__)


class TaskDispatchPolicy(StrEnum):
    DIRECT = "direct"
    BACKGROUND = "background"
    PREFER_BACKGROUND = "prefer_background"


class TaskFeature(StrEnum):
    DEFERRED = "deferred"
    RECURRING = "recurring"


class TaskFeatureUnavailableError(RuntimeError):
    """Raised when the selected task provider cannot perform an operation."""


@dataclass(frozen=True, slots=True)
class TaskFeatures:
    deferred: bool = False
    recurring: bool = False

    def supports(self, feature: TaskFeature | str) -> bool:
        selected = TaskFeature(feature)
        if selected is TaskFeature.DEFERRED:
            return self.deferred
        if selected is TaskFeature.RECURRING:
            return self.recurring
        raise ValueError(f"Unsupported task feature: {feature!r}.")

    def require(self, feature: TaskFeature | str) -> None:
        selected = TaskFeature(feature)
        if self.supports(selected):
            return
        raise TaskFeatureUnavailableError(
            f"Task feature {selected.value!r} is unavailable from the configured "
            "provider; select a provider that supports this operation."
        )


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
    features: TaskFeatures

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
    features: TaskFeatures = field(default_factory=TaskFeatures, init=False)
    events: EventsCapability | None = field(default=None, repr=False, compare=False)
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
        worker_id = self.settings.worker_id or "immediate"
        parent = current_task_context()
        submitted = TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.SUBMITTED,
            task_name=definition.identity.name,
            schema_version=definition.identity.version,
            queue=queue,
            correlation_id=parent.correlation_id if parent is not None else None,
            causation_id=parent.task_id if parent is not None else None,
        )
        await self._record_event(submitted)
        retry = _effective_retry(definition.retry, self.settings.retry_policy)
        for attempt in range(1, retry.max_attempts + 1):
            started = _next_event(
                submitted,
                TaskLifecycleKind.STARTED,
                attempt=attempt,
                worker_id=worker_id,
            )
            await self._record_event(started)
            context = TaskExecutionContext(
                task_id=submitted.task_id,
                task_name=submitted.task_name,
                schema_version=submitted.schema_version,
                attempt=attempt,
                correlation_id=submitted.correlation_id,
                causation_id=submitted.causation_id,
                idempotency_key=selected.idempotency_key,
                queue=queue,
                worker_id=worker_id,
                progress_reporter=self._progress_reporter(
                    submitted,
                    attempt=attempt,
                    worker_id=worker_id,
                ),
            )
            try:
                await definition.execute(payload, context)
            except TaskProgressError as exc:
                await self._record_terminal_failure(
                    submitted,
                    attempt=attempt,
                    worker_id=worker_id,
                    error_type=type(exc).__name__,
                )
                break
            except Exception as exc:
                if attempt < retry.max_attempts:
                    await self._record_event(
                        _next_event(
                            submitted,
                            TaskLifecycleKind.RETRY_SCHEDULED,
                            attempt=attempt,
                            worker_id=worker_id,
                            error_type=type(exc).__name__,
                        )
                    )
                    await self._sleep(
                        _retry_delay(retry, attempt, random_value=self._random())
                    )
                    continue
                await self._record_terminal_failure(
                    submitted,
                    attempt=attempt,
                    worker_id=worker_id,
                    error_type=type(exc).__name__,
                )
                break
            else:
                await self._record_event(
                    _next_event(
                        submitted,
                        TaskLifecycleKind.SUCCEEDED,
                        attempt=attempt,
                        worker_id=worker_id,
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

    async def _record_event(self, event: TaskLifecycleEvent) -> None:
        self._projection.apply(event)
        self._log_lifecycle_event(event)
        events = self.events
        if events is not None and event_delivery_enabled(events):
            try:
                observation = TaskLifecycleObservationEvent.from_lifecycle(event)
                await events.publish(observation)
            except CancelledError as exc:
                if _external_cancellation_requested():
                    raise
                self._warn_event_publication_failure(event, exc)
            except Exception as exc:
                self._warn_event_publication_failure(event, exc)

    @staticmethod
    def _log_lifecycle_event(event: TaskLifecycleEvent) -> None:
        try:
            logger.info(
                "task lifecycle transition",
                extra={
                    "wybra_task": {
                        "kind": event.kind.value,
                        "task_id": str(event.task_id),
                        "task_name": event.task_name,
                        "schema_version": event.schema_version,
                        "queue": event.queue,
                        "correlation_id": str(event.correlation_id),
                        "causation_id": (
                            str(event.causation_id)
                            if event.causation_id is not None
                            else None
                        ),
                        "attempt": event.attempt,
                        "worker_id": event.worker_id,
                        "error_type": event.error_type,
                    }
                },
            )
        except Exception:
            pass

    @staticmethod
    def _warn_event_publication_failure(
        event: TaskLifecycleEvent,
        exc: BaseException,
    ) -> None:
        try:
            logger.warning(
                "task lifecycle event publication failed",
                extra={
                    "wybra_task": {
                        "kind": event.kind.value,
                        "task_id": str(event.task_id),
                        "error_type": type(exc).__name__,
                    }
                },
            )
        except Exception:
            pass

    async def _record_terminal_failure(
        self,
        origin: TaskLifecycleEvent,
        *,
        attempt: int,
        worker_id: str,
        error_type: str,
    ) -> None:
        await self._record_event(
            _next_event(
                origin,
                TaskLifecycleKind.FAILED,
                attempt=attempt,
                worker_id=worker_id,
                error_type=error_type,
            )
        )
        await self._record_event(
            _next_event(
                origin,
                TaskLifecycleKind.DEAD_LETTERED,
                attempt=attempt,
                worker_id=worker_id,
                error_type=error_type,
            )
        )

    def _progress_reporter(
        self,
        origin: TaskLifecycleEvent,
        *,
        attempt: int,
        worker_id: str,
    ) -> Callable[[Mapping[str, object]], Awaitable[None]]:
        async def report(progress: Mapping[str, object]) -> None:
            await self._record_event(
                _next_event(
                    origin,
                    TaskLifecycleKind.PROGRESS,
                    attempt=attempt,
                    worker_id=worker_id,
                    progress=progress,
                )
            )

        return report

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


def _external_cancellation_requested() -> bool:
    task = current_task()
    return task is not None and task.cancelling() > 0


def _next_event(
    origin: TaskLifecycleEvent,
    kind: TaskLifecycleKind,
    *,
    attempt: int,
    worker_id: str | None = None,
    progress: Mapping[str, object] | None = None,
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
        progress=progress,
        error_type=error_type,
    )


__all__ = (
    "ImmediateTasksCapability",
    "TaskDispatchPolicy",
    "TaskFeature",
    "TaskFeatures",
    "TaskFeatureUnavailableError",
    "TaskHandle",
    "TaskSubmissionOptions",
    "TasksCapability",
    "dispatch",
)
