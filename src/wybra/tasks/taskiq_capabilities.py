"""Private bridge from registered Wybra tasks to a cache-backed Taskiq broker."""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from math import isfinite
from typing import Any
from uuid import UUID, uuid7

from taskiq import AsyncTaskiqDecoratedTask
from taskiq.exceptions import SendTaskError

from wybra.cache import CacheFeatureError, CacheWorkQueueRejectedError
from wybra.tasks.capabilities import (
    TaskFeatures,
    TaskFeatureUnavailableError,
    TaskHandle,
    TaskSubmissionOptions,
)
from wybra.tasks.context import current_task_context
from wybra.tasks.declarations import TaskDefinition
from wybra.tasks.lifecycle import TaskLifecycleError, TaskLifecycleEvent, TaskStatus
from wybra.tasks.models import (
    RetryPolicy,
    TaskIdentity,
    TaskPayload,
    TaskRegistrationError,
    TaskSubmissionError,
)
from wybra.tasks.taskiq_broker import CacheTaskiqBroker
from wybra.tasks.taskiq_lifecycle import (
    CacheTaskiqLifecycleMiddleware,
    CacheTaskLifecycle,
)
from wybra.tasks.taskiq_protocol import (
    TASK_CAUSATION_ID_LABEL,
    TASK_CORRELATION_ID_LABEL,
    TASK_IDEMPOTENCY_KEY_LABEL,
    TASK_NAME_LABEL,
    TASK_QUEUE_LABEL,
    TASK_RETRY_BACKOFF_MULTIPLIER_LABEL,
    TASK_RETRY_INITIAL_DELAY_LABEL,
    TASK_RETRY_JITTER_SECONDS_LABEL,
    TASK_RETRY_MAXIMUM_DELAY_LABEL,
    TASK_SCHEMA_VERSION_LABEL,
    TASK_VISIBILITY_TIMEOUT_LABEL,
)

_LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class CacheTaskiqTasksCapability:
    """Submit registered Wybra task definitions through one Taskiq broker."""

    broker: CacheTaskiqBroker
    lifecycle_store: CacheTaskLifecycle
    lifecycle_middleware: CacheTaskiqLifecycleMiddleware
    retry_policy: RetryPolicy = field(default_factory=RetryPolicy)
    worker_concurrency: int = 1
    worker_shutdown_grace_seconds: float = 30.0
    features: TaskFeatures = field(default_factory=TaskFeatures, init=False)
    _tasks: dict[TaskIdentity, AsyncTaskiqDecoratedTask[Any, Any]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _definitions: dict[TaskIdentity, TaskDefinition] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        if (
            isinstance(self.worker_concurrency, bool)
            or not isinstance(self.worker_concurrency, int)
            or self.worker_concurrency < 1
        ):
            raise ValueError("Worker concurrency must be a positive integer.")
        if (
            isinstance(self.worker_shutdown_grace_seconds, bool)
            or not isinstance(self.worker_shutdown_grace_seconds, int | float)
            or self.worker_shutdown_grace_seconds <= 0
            or not isfinite(self.worker_shutdown_grace_seconds)
        ):
            raise ValueError("Worker shutdown grace must be a positive finite number.")

    def register(self, definition: TaskDefinition) -> None:
        existing = self._definitions.get(definition.identity)
        if existing is definition:
            return
        if existing is not None:
            identity = definition.identity
            raise TaskRegistrationError(
                f"Task {identity.name!r} version {identity.version} has more than "
                "one definition in this runtime."
            )
        retry = _effective_retry(definition.retry, self.retry_policy)
        labels: dict[str, str | float] = {
            TASK_NAME_LABEL: definition.identity.name,
            TASK_SCHEMA_VERSION_LABEL: str(definition.identity.version),
            "retry_on_error": str(retry.max_attempts > 1).lower(),
            "max_retries": str(retry.max_attempts),
            TASK_RETRY_INITIAL_DELAY_LABEL: retry.initial_delay_seconds,
            TASK_RETRY_BACKOFF_MULTIPLIER_LABEL: retry.backoff_multiplier,
            TASK_RETRY_JITTER_SECONDS_LABEL: retry.jitter_seconds,
        }
        if retry.maximum_delay_seconds is not None:
            labels[TASK_RETRY_MAXIMUM_DELAY_LABEL] = retry.maximum_delay_seconds
        if definition.visibility_timeout_seconds is not None:
            labels[TASK_VISIBILITY_TIMEOUT_LABEL] = (
                definition.visibility_timeout_seconds
            )

        async def execute(**arguments: object) -> object:
            return await definition.execute(
                TaskPayload(arguments),
                self.lifecycle_middleware.execution_context(),
            )

        self._tasks[definition.identity] = self.broker.task(
            task_name=_routing_name(definition.identity),
            **labels,
        )(execute)
        self._definitions[definition.identity] = definition

    async def submit(
        self,
        definition: TaskDefinition,
        payload: TaskPayload,
        *,
        options: TaskSubmissionOptions | None = None,
    ) -> TaskHandle:
        selected = options or TaskSubmissionOptions()
        queue = selected.queue or self.broker.queue
        if queue != self.broker.queue:
            raise TaskFeatureUnavailableError(
                "The configured Taskiq runtime only consumes queue "
                f"{self.broker.queue!r}; configure a worker for queue {queue!r}."
            )
        try:
            registered_definition = self._definitions[definition.identity]
            task = self._tasks[definition.identity]
        except KeyError as exc:
            identity = definition.identity
            raise TaskRegistrationError(
                f"Task {identity.name!r} version {identity.version} is not declared "
                "by this site's configured modules."
            ) from exc
        if registered_definition is not definition:
            identity = definition.identity
            raise TaskRegistrationError(
                f"Task {identity.name!r} version {identity.version} does not match "
                "the definition registered by this site's configured modules."
            )
        validated = registered_definition.validate_payload(payload)
        parent = current_task_context()
        labels: dict[str, str | float] = {TASK_QUEUE_LABEL: queue}
        if selected.idempotency_key is not None:
            labels[TASK_IDEMPOTENCY_KEY_LABEL] = selected.idempotency_key
        if parent is not None:
            labels[TASK_CORRELATION_ID_LABEL] = str(parent.correlation_id)
            labels[TASK_CAUSATION_ID_LABEL] = str(parent.task_id)
        task_id = uuid7()
        with self.broker.track_publication(str(task_id)) as publication:
            try:
                submitted = await (
                    task.kicker()
                    .with_labels(**labels)
                    .with_task_id(str(task_id))
                    .kiq(**validated.arguments)
                )
            except SendTaskError as error:
                if isinstance(error.__cause__, CacheWorkQueueRejectedError):
                    raise TaskSubmissionError(
                        "The configured task provider rejected the submission.",
                        task_id=task_id,
                        acceptance_unknown=False,
                    ) from None
                if not publication.reached_broker:
                    await self._repair_pre_publication_failure(task_id)
                    raise TaskSubmissionError(
                        "The task submission failed before publication began.",
                        task_id=task_id,
                        acceptance_unknown=False,
                    ) from None
                raise TaskSubmissionError(
                    "The task submission outcome is unknown; query its task ID before "
                    "submitting again.",
                    task_id=task_id,
                    acceptance_unknown=True,
                ) from None
            except CacheFeatureError, TaskLifecycleError:
                if not publication.reached_broker:
                    await self._repair_pre_publication_failure(task_id)
                    raise TaskSubmissionError(
                        "The task submission failed before publication began.",
                        task_id=task_id,
                        acceptance_unknown=False,
                    ) from None
                raise TaskSubmissionError(
                    "The task submission outcome is unknown; query its task ID before "
                    "submitting again.",
                    task_id=task_id,
                    acceptance_unknown=True,
                ) from None
        task_id = UUID(submitted.task_id)
        return TaskHandle(
            task_id=task_id,
            identity=definition.identity,
            _status_getter=self.status,
        )

    async def _repair_pre_publication_failure(self, task_id: UUID) -> None:
        try:
            await self.lifecycle_middleware.submission_aborted(task_id)
        except CacheFeatureError, TaskLifecycleError:
            _LOGGER.warning(
                "Task lifecycle could not immediately reconcile a pre-publication "
                "failure for task %s.",
                task_id,
            )

    async def status(self, task_id: UUID) -> TaskStatus | None:
        return await self.lifecycle_store.status(task_id)

    async def lifecycle(self, task_id: UUID) -> tuple[TaskLifecycleEvent, ...]:
        return await self.lifecycle_store.lifecycle(task_id)

    def receiver(self, **kwargs: object) -> Any:
        """Build the supported, secret-safe cache-aware worker receiver.

        Task declarations already supply argument validation and require async
        handlers.  Taskiq parameter validation and executor settings are not
        part of this receiver's supported contract because it owns execution to
        keep task arguments out of Taskiq's diagnostic path.
        """
        from wybra.tasks.taskiq_receiver import CacheTaskiqReceiver

        if kwargs.pop("validate_params", False):
            raise ValueError(
                "CacheTaskiqReceiver does not support Taskiq parameter validation; "
                "Wybra task definitions validate payloads before submission."
            )
        if kwargs.get("executor") is not None:
            raise ValueError(
                "CacheTaskiqReceiver does not support a Taskiq executor; "
                "Wybra tasks must be async callables."
            )
        kwargs.pop("executor", None)
        kwargs["validate_params"] = False
        kwargs.setdefault("max_async_tasks", self.worker_concurrency)
        kwargs.setdefault("wait_tasks_timeout", self.worker_shutdown_grace_seconds)
        return CacheTaskiqReceiver(self.broker, **kwargs)

    async def close(self) -> None:
        await self.broker.shutdown()


__all__ = ("CacheTaskiqTasksCapability",)


def _routing_name(identity: TaskIdentity) -> str:
    return f"wybra.{identity.name}.__v{identity.version}"


def _effective_retry(
    declared: RetryPolicy | None,
    configured: RetryPolicy,
) -> RetryPolicy:
    return configured if declared is None else declared
