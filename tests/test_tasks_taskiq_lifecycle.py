from __future__ import annotations

import json
import time
from asyncio import Event, sleep
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from taskiq import (
    InMemoryBroker,
    SimpleRetryMiddleware,
    SmartRetryMiddleware,
    TaskiqMessage,
    TaskiqResult,
)
from taskiq.abc.schedule_source import ScheduleSource
from taskiq.exceptions import SendTaskError

from wybra.cache import (
    MAX_CACHE_FEATURE_LIMIT,
    CacheConflictError,
    CacheFeatureError,
    CachePositionExpiredError,
    CacheWorkQueueRejectedError,
    InMemoryAtomicCache,
    InMemoryCacheFeatures,
    InMemoryCacheTime,
    InMemoryLeaseCache,
    InMemoryStreamCache,
)
from wybra.tasks.lifecycle import (
    TaskLifecycleError,
    TaskLifecycleEvent,
    TaskLifecycleKind,
    TaskState,
)
from wybra.tasks.taskiq_lifecycle import (
    CacheTaskiqLifecycleMiddleware,
    CacheTaskiqSimpleRetryMiddleware,
    CacheTaskiqSmartRetryMiddleware,
    CacheTaskLifecycle,
    TaskiqLifecyclePolicy,
)
from wybra.tasks.taskiq_protocol import (
    DELIVERY_ATTEMPT_LABEL,
    DELIVERY_IDENTITY_LABEL,
    DELIVERY_RECEIPT_LABEL,
    TASK_RETRY_BACKOFF_MULTIPLIER_LABEL,
    TASK_RETRY_INITIAL_DELAY_LABEL,
    TASK_RETRY_JITTER_SECONDS_LABEL,
    TASK_RETRY_MAXIMUM_DELAY_LABEL,
)


def _memory_lifecycle(
    streams: InMemoryStreamCache,
    atomic: InMemoryAtomicCache,
    leases: InMemoryLeaseCache,
    policy: TaskiqLifecyclePolicy,
    *,
    _clock: _Clock | None = None,
    cache_time: InMemoryCacheTime | None = None,
) -> CacheTaskLifecycle:
    InMemoryCacheFeatures(atomic=atomic, leases=leases, streams=streams)
    return CacheTaskLifecycle(
        streams,
        atomic,
        leases,
        policy,
        cache_time or InMemoryCacheTime(time.time if _clock is None else _clock),
    )


def _lifecycle() -> CacheTaskLifecycle:
    return _memory_lifecycle(
        InMemoryStreamCache(),
        InMemoryAtomicCache(),
        InMemoryLeaseCache(),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            worker_id="worker-1",
            status_retention_seconds=60,
        ),
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("kind", "progress"),
    (
        (TaskLifecycleKind.STARTED, None),
        (TaskLifecycleKind.PROGRESS, {"complete": 50}),
    ),
)
async def test_lifecycle_rejects_distinct_duplicate_delivery_provenance(
    kind: TaskLifecycleKind,
    progress: dict[str, int] | None,
) -> None:
    lifecycle = _lifecycle()
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.duplicate_delivery",
        schema_version=1,
        queue="default",
    )
    started = TaskLifecycleEvent(
        kind=TaskLifecycleKind.STARTED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
        delivery_attempt=1,
        _delivery_identity="first-delivery",
        worker_id="worker-1",
    )
    await lifecycle.record(submitted)
    await lifecycle.record(started)

    with pytest.raises(TaskLifecycleError, match="distinct delivery"):
        await lifecycle.record(
            TaskLifecycleEvent(
                kind=kind,
                task_id=submitted.task_id,
                task_name=submitted.task_name,
                schema_version=submitted.schema_version,
                queue=submitted.queue,
                correlation_id=submitted.correlation_id,
                delivery_attempt=1,
                _delivery_identity="second-delivery",
                worker_id="worker-1",
                progress=progress,
            )
        )


class _Clock:
    def __init__(self) -> None:
        self.value = 0.0

    def __call__(self) -> float:
        return self.value


class _CountingCacheTime:
    def __init__(self, clock: _Clock) -> None:
        self.clock = clock
        self.refreshes = 0

    async def refresh(self) -> float:
        self.refreshes += 1
        return self.now()

    def now(self) -> float:
        return self.clock()


class _LeaseAwareCacheTime(_CountingCacheTime):
    def __init__(self, clock: _Clock, leases: InMemoryLeaseCache) -> None:
        super().__init__(clock)
        self.leases = leases

    async def refresh(self) -> float:
        assert self.leases._leases
        return await super().refresh()


class _FailingAtomic(InMemoryAtomicCache):
    async def create(self, *args: object, **kwargs: object):
        del args, kwargs
        raise RuntimeError("projection unavailable")


class _LingeringProjectionAtomic(InMemoryAtomicCache):
    async def create(self, *args: object, **kwargs: object):
        kwargs["ttl"] = float(kwargs["ttl"]) * 10
        return await super().create(*args, **kwargs)

    async def compare_and_swap(self, *args: object, **kwargs: object):
        kwargs["ttl"] = float(kwargs["ttl"]) * 10
        return await super().compare_and_swap(*args, **kwargs)


class _TargetEvictingStream(InMemoryStreamCache):
    def __init__(self) -> None:
        super().__init__(max_records=1)

    async def append(self, owner: str, stream: str, payload: bytes, **kwargs: object):
        position = await super().append(owner, stream, payload, **kwargs)
        await super().append(owner, stream, b"evicted-target")
        return position


class _RetryRejectingBroker(InMemoryBroker):
    async def kick(self, message: object) -> None:
        del message
        raise CacheWorkQueueRejectedError("Queue rejected the retry publication.")


class _TransientReplayConflictAtomic(InMemoryAtomicCache):
    failed_key: str | None = None

    async def create(
        self,
        owner: str,
        key: str,
        *args: object,
        **kwargs: object,
    ):
        if key == self.failed_key:
            self.failed_key = None
            raise CacheConflictError("projection is temporarily busy")
        return await super().create(owner, key, *args, **kwargs)


class _SelectedKeyFailingAtomic(InMemoryAtomicCache):
    failed_key: str | None = None

    async def create(
        self,
        owner: str,
        key: str,
        *args: object,
        **kwargs: object,
    ):
        if key == self.failed_key:
            self.failed_key = None
            raise CacheConflictError("projection unavailable")
        return await super().create(owner, key, *args, **kwargs)


class _RecoverableProjectionFailureAtomic(InMemoryAtomicCache):
    fail_updates: bool = False

    async def compare_and_swap(self, *args: object, **kwargs: object):
        if self.fail_updates:
            raise CacheConflictError("projection unavailable")
        return await super().compare_and_swap(*args, **kwargs)


class _SlowProjectionAtomic(InMemoryAtomicCache):
    _get_count: int = 0

    async def get(self, *args: object, **kwargs: object):
        self._get_count += 1
        if self._get_count > 1:
            await sleep(0.2)
        return await super().get(*args, **kwargs)


class _SlowCreateAtomic(InMemoryAtomicCache):
    async def create(self, *args: object, **kwargs: object):
        await sleep(0.2)
        return await super().create(*args, **kwargs)


class _FailingRenewalLease(InMemoryLeaseCache):
    async def renew(self, *args: object, **kwargs: object):
        del args, kwargs
        raise CacheConflictError("lease unavailable")


class _CursorExpiringStream(InMemoryStreamCache):
    expire_after_first_page: bool = False

    async def read(self, *args: object, **kwargs: object):
        if self.expire_after_first_page and kwargs.get("after") is not None:
            self.expire_after_first_page = False
            raise CachePositionExpiredError("Stream position is no longer retained.")
        return await super().read(*args, **kwargs)


class _LeaseExpiringStream(InMemoryStreamCache):
    def __init__(self, clock: _Clock) -> None:
        super().__init__()
        self._clock = clock

    async def append(self, *args: object, **kwargs: object):
        position = await super().append(*args, **kwargs)
        self._clock.value += 31
        return position


class _InterruptedDeadLetterLifecycle(CacheTaskLifecycle):
    async def record(self, event: TaskLifecycleEvent):
        if event.kind is TaskLifecycleKind.DEAD_LETTERED:
            raise CacheConflictError("dead-letter projection interrupted")
        return await super().record(event)


class _InterruptedDeadLetterRepairLifecycle(CacheTaskLifecycle):
    interrupted: bool = True

    async def record_repair(self, event: TaskLifecycleEvent, **kwargs: object):
        if event.kind is TaskLifecycleKind.DEAD_LETTERED and self.interrupted:
            self.interrupted = False
            raise CacheConflictError("dead-letter repair interrupted")
        return await super().record_repair(event, **kwargs)


def _message(
    *,
    task_id: str | None = None,
    labels: dict[str, object] | None = None,
) -> TaskiqMessage:
    return TaskiqMessage(
        task_id=task_id or str(uuid4()),
        task_name="example.cleanup",
        labels={} if labels is None else labels,
        args=["secret-argument"],
        kwargs={"secret": "value"},
    )


def test_taskiq_lifecycle_policy_bounds_replay_pages() -> None:
    with pytest.raises(ValueError, match="no greater"):
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
            replay_page_limit=MAX_CACHE_FEATURE_LIMIT + 1,
        )


@pytest.mark.anyio
async def test_taskiq_lifecycle_records_safe_successful_transitions() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    message = _message()

    await middleware.pre_send(message)
    await middleware.pre_execute(message)
    await middleware.post_execute(
        message,
        TaskiqResult(
            is_err=False,
            return_value={"secret": "result"},
            execution_time=0.1,
        ),
    )
    await middleware.post_save(
        message,
        TaskiqResult(
            is_err=False,
            return_value={"secret": "result"},
            execution_time=0.1,
        ),
    )

    status = await lifecycle.status(UUID(message.task_id))
    assert status is not None
    assert status.state is TaskState.SUCCEEDED
    events = await lifecycle.lifecycle(status.task_id)
    assert [event.kind for event in events] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.SUCCEEDED,
    ]
    assert events[0].worker_id is None
    assert events[1].worker_id == "worker-1"


@pytest.mark.anyio
async def test_terminal_delivery_remains_obsolete_after_projection_retention() -> None:
    clock = _Clock()
    lifecycle = _memory_lifecycle(
        InMemoryStreamCache(),
        InMemoryAtomicCache(clock),
        InMemoryLeaseCache(clock),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            worker_id="worker-1",
            status_retention_seconds=1,
        ),
        _clock=clock,
    )
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    message = _message()

    await middleware.pre_send(message)
    await middleware.pre_execute(message)
    await middleware.post_execute(
        message,
        TaskiqResult(is_err=False, return_value=None, execution_time=0.1),
    )
    await middleware.post_save(
        message,
        TaskiqResult(is_err=False, return_value=None, execution_time=0.1),
    )
    clock.value = 2

    assert await lifecycle.status(UUID(message.task_id)) is None
    assert await middleware.is_obsolete_retry_delivery(message)


@pytest.mark.anyio
async def test_lifecycle_requires_precomposed_memory_features_for_fenced_writes() -> (
    None
):
    lifecycle = CacheTaskLifecycle(
        InMemoryStreamCache(),
        InMemoryAtomicCache(),
        InMemoryLeaseCache(),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
        ),
        InMemoryCacheTime(),
    )
    event = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )

    with pytest.raises(CacheFeatureError, match="does not support lease-fenced"):
        await lifecycle.record(event)

    assert (
        await lifecycle.atomic.get("task-lifecycle", f"status:{event.task_id}") is None
    )


@pytest.mark.anyio
async def test_live_write_ignores_producer_clock_skew() -> None:
    clock = _Clock()
    clock.value = 100
    lifecycle = _memory_lifecycle(
        InMemoryStreamCache(),
        InMemoryAtomicCache(clock),
        InMemoryLeaseCache(),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
            active_status_timeout_seconds=10,
        ),
        _clock=clock,
    )
    event = TaskLifecycleEvent(
        kind=TaskLifecycleKind.SUBMITTED,
        task_id=uuid4(),
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
        correlation_id=uuid4(),
        occurred_at=0,
    )

    await lifecycle.record(event)

    assert await lifecycle.status(event.task_id) is not None


@pytest.mark.anyio
async def test_redelivered_attempt_rehydrates_an_expired_active_projection() -> None:
    clock = _Clock()
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache(clock)
    leases = InMemoryLeaseCache(clock)
    lifecycle = _memory_lifecycle(
        streams,
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
            active_status_timeout_seconds=10,
        ),
        _clock=clock,
    )
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    started = TaskLifecycleEvent(
        kind=TaskLifecycleKind.STARTED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
        occurred_at=1,
    )
    await lifecycle.record(submitted)
    await lifecycle.record(started)
    clock.value = 11

    status = await lifecycle.record(started)

    assert status.state is TaskState.RUNNING
    assert status.updated_at == clock.value
    assert await lifecycle.status(submitted.task_id) == status
    records = await lifecycle.streams.read("task-lifecycle", "events")
    assert len(records) == 3
    assert b"secret-argument" not in records[0].payload
    assert b'secret":"result' not in records[-1].payload


@pytest.mark.anyio
async def test_status_does_not_rehydrate_an_abandoned_active_projection() -> None:
    clock = _Clock()
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache(clock)
    lifecycle = _memory_lifecycle(
        streams,
        atomic,
        InMemoryLeaseCache(clock),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
            active_status_timeout_seconds=10,
        ),
        _clock=clock,
    )
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    started = TaskLifecycleEvent(
        kind=TaskLifecycleKind.STARTED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
    )
    await lifecycle.record(submitted)
    await lifecycle.record(started)

    clock.value = 11

    assert await lifecycle.status(submitted.task_id) is None


@pytest.mark.anyio
async def test_taskiq_retry_records_retry_before_next_attempt() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    broker = InMemoryBroker()
    broker.add_middlewares(
        CacheTaskiqSimpleRetryMiddleware(
            middleware,
            default_retry_count=2,
            default_retry_label=True,
        ),
        middleware,
    )
    message = _message()

    await middleware.pre_send(message)
    await middleware.pre_execute(message)
    await middleware.on_error(
        message,
        TaskiqResult(
            is_err=True,
            return_value=None,
            execution_time=0.1,
            error=RuntimeError("secret"),
        ),
        RuntimeError("secret"),
    )
    message.labels["_retries"] = 1
    retry_message = _message(task_id=message.task_id, labels=dict(message.labels))
    await middleware.pre_send(retry_message)
    await middleware.post_send(retry_message)
    await middleware.pre_execute(retry_message)

    status = await lifecycle.status(UUID(message.task_id))
    assert status is not None
    assert status.state is TaskState.RUNNING
    assert status.attempt == 2
    events = await lifecycle.lifecycle(status.task_id)
    assert [event.kind for event in events] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.ATTEMPT_FAILED,
        TaskLifecycleKind.RETRY_SCHEDULED,
        TaskLifecycleKind.STARTED,
    ]
    assert events[2].error_type == "RuntimeError"


@pytest.mark.anyio
async def test_replay_dead_letters_an_abandoned_failed_retry_handoff() -> None:
    clock = _Clock()
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache(clock)
    leases = InMemoryLeaseCache(clock)
    policy = TaskiqLifecyclePolicy(
        owner="task-lifecycle",
        stream="events",
        queue="default",
        status_retention_seconds=60,
        active_status_timeout_seconds=10,
    )
    lifecycle = _memory_lifecycle(
        streams,
        atomic,
        leases,
        policy,
        _clock=clock,
    )
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    broker = InMemoryBroker()
    broker.add_middlewares(
        CacheTaskiqSimpleRetryMiddleware(
            middleware,
            default_retry_count=2,
            default_retry_label=True,
        ),
        middleware,
    )
    message = _message()
    await middleware.pre_send(message)
    await middleware.pre_execute(message)

    await middleware.on_error(
        message,
        TaskiqResult(
            is_err=True,
            return_value=None,
            execution_time=0.1,
            error=RuntimeError("retry hand-off failed"),
        ),
        RuntimeError("retry hand-off failed"),
    )

    status = await lifecycle.status(UUID(message.task_id))
    assert status is not None
    assert status.state is TaskState.RUNNING
    assert status.error_type == "RuntimeError"
    assert [event.kind for event in await lifecycle.lifecycle(status.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.ATTEMPT_FAILED,
    ]

    clock.value = 11
    recovered = _memory_lifecycle(streams, atomic, leases, policy, _clock=clock)
    await recovered.replay()

    status = await recovered.status(UUID(message.task_id))
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert [event.kind for event in await recovered.lifecycle(status.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.ATTEMPT_FAILED,
        TaskLifecycleKind.FAILED,
        TaskLifecycleKind.DEAD_LETTERED,
    ]


@pytest.mark.anyio
async def test_exhausted_original_delivery_does_not_terminalise_running_retry() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    retry = CacheTaskiqSimpleRetryMiddleware(
        middleware,
        default_retry_count=2,
        default_retry_label=True,
    )
    broker = InMemoryBroker()
    broker.add_middlewares(retry, middleware)
    original = _message(
        labels={
            DELIVERY_ATTEMPT_LABEL: 1,
            DELIVERY_IDENTITY_LABEL: "original-delivery",
        }
    )

    await middleware.pre_send(original)
    exhausted_original = _message(
        task_id=original.task_id,
        labels={**original.labels, DELIVERY_ATTEMPT_LABEL: 3},
    )
    await middleware.pre_execute(original)
    failure = RuntimeError("handler failed")
    await middleware.on_error(
        original,
        TaskiqResult(
            is_err=True,
            return_value=None,
            execution_time=0.1,
            error=failure,
        ),
        failure,
    )
    retry_message = _message(
        task_id=original.task_id,
        labels={**original.labels, "_retries": 1, DELIVERY_ATTEMPT_LABEL: 1},
    )
    await middleware.pre_send(retry_message)
    await middleware.post_send(retry_message)
    await middleware.pre_execute(retry_message)

    await middleware.delivery_exhausted(exhausted_original)

    status = await lifecycle.status(UUID(original.task_id))
    assert status is not None
    assert status.state is TaskState.RUNNING
    assert status.attempt == 2


@pytest.mark.anyio
async def test_exhausted_redelivery_terminalises_a_stale_running_attempt() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    original = _message(
        labels={
            DELIVERY_ATTEMPT_LABEL: 1,
            DELIVERY_IDENTITY_LABEL: "original-delivery",
        }
    )

    await middleware.pre_send(original)
    await middleware.pre_execute(original)
    exhausted_redelivery = _message(
        task_id=original.task_id,
        labels={**original.labels, DELIVERY_ATTEMPT_LABEL: 3},
    )

    await middleware.delivery_exhausted(exhausted_redelivery)

    status = await lifecycle.status(UUID(original.task_id))
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert [event.kind for event in await lifecycle.lifecycle(status.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.FAILED,
        TaskLifecycleKind.DEAD_LETTERED,
    ]


@pytest.mark.anyio
async def test_exhausted_duplicate_work_does_not_terminalise_live_delivery() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    original = _message(
        labels={
            DELIVERY_ATTEMPT_LABEL: 1,
            DELIVERY_IDENTITY_LABEL: "live-delivery",
        }
    )

    await middleware.pre_send(original)
    await middleware.pre_execute(original)
    duplicate = _message(
        task_id=original.task_id,
        labels={
            **original.labels,
            DELIVERY_ATTEMPT_LABEL: 3,
            DELIVERY_IDENTITY_LABEL: "duplicate-delivery",
        },
    )

    await middleware.delivery_exhausted(duplicate)

    status = await lifecycle.status(UUID(original.task_id))
    assert status is not None
    assert status.state is TaskState.RUNNING


@pytest.mark.anyio
async def test_exhausted_pending_retry_delivery_is_dead_lettered() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    retry = CacheTaskiqSimpleRetryMiddleware(
        middleware,
        default_retry_count=2,
        default_retry_label=True,
    )
    broker = InMemoryBroker()
    broker.add_middlewares(retry, middleware)
    original = _message(
        labels={
            DELIVERY_ATTEMPT_LABEL: 1,
            DELIVERY_IDENTITY_LABEL: "original-delivery",
            DELIVERY_RECEIPT_LABEL: "original-receipt",
        }
    )

    await middleware.pre_send(original)
    await middleware.pre_execute(original)
    failure = RuntimeError("handler failed")
    await middleware.on_error(
        original,
        TaskiqResult(
            is_err=True,
            return_value=None,
            execution_time=0.1,
            error=failure,
        ),
        failure,
    )
    retry_message = _message(
        task_id=original.task_id,
        labels={**original.labels, "_retries": 1, DELIVERY_ATTEMPT_LABEL: 3},
    )
    await middleware.pre_send(retry_message)
    assert not {
        DELIVERY_ATTEMPT_LABEL,
        DELIVERY_IDENTITY_LABEL,
        DELIVERY_RECEIPT_LABEL,
    }.intersection(retry_message.labels)
    await middleware.post_send(retry_message)

    await middleware.delivery_exhausted(retry_message)

    status = await lifecycle.status(UUID(original.task_id))
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert [event.kind for event in await lifecycle.lifecycle(status.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.ATTEMPT_FAILED,
        TaskLifecycleKind.RETRY_SCHEDULED,
        TaskLifecycleKind.FAILED,
        TaskLifecycleKind.DEAD_LETTERED,
    ]


@pytest.mark.anyio
async def test_exhausted_retry_without_post_send_is_dead_lettered() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    retry = CacheTaskiqSimpleRetryMiddleware(
        middleware,
        default_retry_count=2,
        default_retry_label=True,
    )
    broker = InMemoryBroker()
    broker.add_middlewares(retry, middleware)
    original = _message(labels={DELIVERY_ATTEMPT_LABEL: 1})

    await middleware.pre_send(original)
    await middleware.pre_execute(original)
    failure = RuntimeError("handler failed")
    await middleware.on_error(
        original,
        TaskiqResult(
            is_err=True,
            return_value=None,
            execution_time=0.1,
            error=failure,
        ),
        failure,
    )
    retry_message = _message(
        task_id=original.task_id,
        labels={**original.labels, "_retries": 1, DELIVERY_ATTEMPT_LABEL: 3},
    )
    await middleware.pre_send(retry_message)

    await middleware.delivery_exhausted(retry_message)

    status = await lifecycle.status(UUID(original.task_id))
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert [event.kind for event in await lifecycle.lifecycle(status.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.ATTEMPT_FAILED,
        TaskLifecycleKind.FAILED,
        TaskLifecycleKind.DEAD_LETTERED,
    ]


@pytest.mark.anyio
async def test_redelivery_resumes_original_after_indeterminate_retry_handoff() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    retry = CacheTaskiqSimpleRetryMiddleware(
        middleware,
        default_retry_count=2,
        default_retry_label=True,
    )
    broker = InMemoryBroker()
    broker.add_middlewares(retry, middleware)
    original = _message(
        labels={
            DELIVERY_ATTEMPT_LABEL: 1,
            DELIVERY_IDENTITY_LABEL: "original-delivery",
        }
    )

    await middleware.pre_send(original)
    await middleware.pre_execute(original)
    failure = RuntimeError("handler failed")
    await middleware.on_error(
        original,
        TaskiqResult(
            is_err=True,
            return_value=None,
            execution_time=0.1,
            error=failure,
        ),
        failure,
    )

    await middleware.pre_execute(original)

    status = await lifecycle.status(UUID(original.task_id))
    assert status is not None
    assert status.state is TaskState.RUNNING
    assert await middleware.is_obsolete_retry_delivery(original) is False


@pytest.mark.anyio
async def test_exhausted_delivery_repairs_interrupted_dead_letter_transition() -> None:
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache()
    leases = InMemoryLeaseCache()
    InMemoryCacheFeatures(atomic=atomic, leases=leases, streams=streams)
    lifecycle = _InterruptedDeadLetterRepairLifecycle(
        streams,
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            worker_id="worker-1",
            status_retention_seconds=60,
        ),
        InMemoryCacheTime(time.time),
    )
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    original = _message(
        labels={
            DELIVERY_ATTEMPT_LABEL: 1,
            DELIVERY_IDENTITY_LABEL: "original-delivery",
        }
    )
    await middleware.pre_send(original)
    await middleware.pre_execute(original)
    exhausted_redelivery = _message(
        task_id=original.task_id,
        labels={**original.labels, DELIVERY_ATTEMPT_LABEL: 3},
    )

    with pytest.raises(CacheConflictError, match="dead-letter repair"):
        await middleware.delivery_exhausted(exhausted_redelivery)

    await middleware.delivery_exhausted(exhausted_redelivery)

    status = await lifecycle.status(UUID(original.task_id))
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED


@pytest.mark.anyio
async def test_taskiq_retry_dead_letters_a_definitively_rejected_handoff() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    retry = CacheTaskiqSimpleRetryMiddleware(
        middleware,
        default_retry_count=2,
        default_retry_label=True,
    )
    broker = _RetryRejectingBroker()
    broker.add_middlewares(retry, middleware)
    message = _message()

    await middleware.pre_send(message)
    await middleware.pre_execute(message)
    failure = RuntimeError("handler failed")
    result = TaskiqResult(
        is_err=True,
        return_value=None,
        execution_time=0.1,
        error=failure,
    )
    await middleware.on_error(message, result, failure)

    with pytest.raises(SendTaskError):
        await retry.on_error(message, result, failure)

    status = await lifecycle.status(UUID(message.task_id))
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert status.error_type == "RuntimeError"
    assert [event.kind for event in await lifecycle.lifecycle(status.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.ATTEMPT_FAILED,
        TaskLifecycleKind.FAILED,
        TaskLifecycleKind.DEAD_LETTERED,
    ]


@pytest.mark.anyio
async def test_obsolete_delivery_repairs_interrupted_dead_letter_transition() -> None:
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache()
    leases = InMemoryLeaseCache()
    InMemoryCacheFeatures(atomic=atomic, leases=leases, streams=streams)
    lifecycle = _InterruptedDeadLetterLifecycle(
        streams,
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
        ),
        InMemoryCacheTime(time.time),
    )
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    message = _message()
    await middleware.pre_send(message)

    with pytest.raises(CacheConflictError, match="dead-letter projection"):
        await middleware.submission_rejected(message)

    status = await lifecycle.status(UUID(message.task_id))
    assert status is not None
    assert status.state is TaskState.FAILED

    assert await middleware.is_obsolete_retry_delivery(message) is True
    repaired = await lifecycle.status(UUID(message.task_id))
    assert repaired is not None
    assert repaired.state is TaskState.DEAD_LETTERED


@pytest.mark.anyio
async def test_retry_rejection_cannot_terminalise_a_replacement_delivery() -> None:
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache()
    leases = InMemoryLeaseCache()
    first_policy = TaskiqLifecyclePolicy(
        owner="task-lifecycle",
        stream="events",
        queue="default",
        worker_id="worker-1",
        status_retention_seconds=60,
    )
    first = _memory_lifecycle(streams, atomic, leases, first_policy)
    second = _memory_lifecycle(
        streams,
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner=first_policy.owner,
            stream=first_policy.stream,
            queue=first_policy.queue,
            worker_id="worker-2",
            status_retention_seconds=first_policy.status_retention_seconds,
        ),
    )
    first_middleware = CacheTaskiqLifecycleMiddleware(first)
    second_middleware = CacheTaskiqLifecycleMiddleware(second)
    first_message = _message()

    await first_middleware.pre_send(first_message)
    first_message.labels["_wybra_task_delivery_attempt"] = 1
    await first_middleware.pre_execute(first_message)
    replacement = _message(
        task_id=first_message.task_id,
        labels=dict(first_message.labels),
    )
    replacement.labels["_wybra_task_delivery_attempt"] = 2
    await second_middleware.pre_execute(replacement)

    metadata = json.loads(first_message.labels["_wybra_task_lifecycle"])
    metadata["retry_pending"] = True
    metadata["retry_origin_attempt"] = metadata["attempt"]
    metadata["retry_origin_delivery_attempt"] = metadata["delivery_attempt"]
    metadata["retry_origin_worker_id"] = metadata["worker_id"]
    first_message.labels["_wybra_task_lifecycle"] = json.dumps(metadata)
    retry_message = _message(
        task_id=first_message.task_id,
        labels=dict(first_message.labels),
    )
    retry_message.labels["_retries"] = 1
    await second_middleware.pre_send(retry_message)
    await second_middleware.post_send(retry_message)
    scheduled_status = await second.status(UUID(first_message.task_id))
    assert scheduled_status is not None
    assert scheduled_status.state is TaskState.RUNNING
    assert scheduled_status.worker_id == "worker-2"
    assert scheduled_status.delivery_attempt == 2
    try:
        raise SendTaskError from CacheWorkQueueRejectedError("queue capacity")
    except SendTaskError as error:
        await second_middleware.retry_handoff_rejected(first_message, error)

    status = await second.status(UUID(first_message.task_id))
    assert status is not None
    assert status.state is TaskState.RUNNING
    assert status.worker_id == "worker-2"
    assert status.delivery_attempt == 2


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("retry_middleware", "no_result_on_retry"),
    [
        (CacheTaskiqSimpleRetryMiddleware, True),
        (CacheTaskiqSimpleRetryMiddleware, False),
        (CacheTaskiqSmartRetryMiddleware, True),
        (CacheTaskiqSmartRetryMiddleware, False),
    ],
)
async def test_taskiq_retry_round_trips_lifecycle_metadata(
    retry_middleware: type[
        CacheTaskiqSimpleRetryMiddleware | CacheTaskiqSmartRetryMiddleware
    ],
    no_result_on_retry: bool,
) -> None:
    lifecycle = _lifecycle()
    broker = InMemoryBroker(await_inplace=True, propagate_exceptions=False)
    lifecycle_middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    broker.add_middlewares(
        retry_middleware(
            lifecycle_middleware,
            default_retry_count=2,
            default_retry_label=True,
            no_result_on_retry=no_result_on_retry,
        ),
        lifecycle_middleware,
    )
    attempts = 0

    @broker.task(retry_on_error=True)
    async def retry_once() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient")

    task = await retry_once.kiq()

    status = await lifecycle.status(UUID(task.task_id))
    assert attempts == 2
    assert status is not None
    assert status.state is TaskState.SUCCEEDED
    assert status.attempt == 2
    assert [event.kind for event in await lifecycle.lifecycle(status.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.ATTEMPT_FAILED,
        TaskLifecycleKind.RETRY_SCHEDULED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.SUCCEEDED,
    ]


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("retry_middleware", "default_retry_count", "types_of_exceptions"),
    [
        (CacheTaskiqSimpleRetryMiddleware, 1, None),
        (CacheTaskiqSmartRetryMiddleware, 1, None),
        (CacheTaskiqSimpleRetryMiddleware, 2, (ValueError,)),
        (CacheTaskiqSmartRetryMiddleware, 2, (ValueError,)),
    ],
)
async def test_taskiq_retry_policy_records_terminal_failure_when_not_retrying(
    retry_middleware: type[
        CacheTaskiqSimpleRetryMiddleware | CacheTaskiqSmartRetryMiddleware
    ],
    default_retry_count: int,
    types_of_exceptions: tuple[type[BaseException], ...] | None,
) -> None:
    lifecycle = _lifecycle()
    middleware_options: dict[str, object] = {
        "default_retry_count": default_retry_count,
        "default_retry_label": True,
    }
    if types_of_exceptions is not None:
        middleware_options["types_of_exceptions"] = types_of_exceptions
    broker = InMemoryBroker(await_inplace=True, propagate_exceptions=False)
    lifecycle_middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    broker.add_middlewares(
        retry_middleware(lifecycle_middleware, **middleware_options),
        lifecycle_middleware,
    )

    @broker.task(retry_on_error=True)
    async def always_fails() -> None:
        raise RuntimeError("not retried")

    task = await always_fails.kiq()

    status = await lifecycle.status(UUID(task.task_id))
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert [event.kind for event in await lifecycle.lifecycle(status.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.FAILED,
        TaskLifecycleKind.DEAD_LETTERED,
    ]


@pytest.mark.anyio
async def test_taskiq_exhausted_failure_is_dead_lettered() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    message = _message()

    await middleware.pre_send(message)
    await middleware.pre_execute(message)
    await middleware.on_error(
        message,
        TaskiqResult(
            is_err=True,
            return_value=None,
            execution_time=0.1,
            error=RuntimeError("failure"),
        ),
        RuntimeError("secret"),
    )

    status = await lifecycle.status(UUID(message.task_id))
    assert status is not None
    assert status.state is TaskState.RUNNING

    await middleware.post_save(
        message,
        TaskiqResult(
            is_err=True,
            return_value=None,
            execution_time=0.1,
            error=RuntimeError("failure"),
        ),
    )

    status = await lifecycle.status(UUID(message.task_id))
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert [event.kind for event in await lifecycle.lifecycle(status.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
        TaskLifecycleKind.FAILED,
        TaskLifecycleKind.DEAD_LETTERED,
    ]


@pytest.mark.anyio
async def test_replay_completes_an_interrupted_terminal_failure() -> None:
    lifecycle = _lifecycle()
    message = _message()
    await CacheTaskiqLifecycleMiddleware(lifecycle).pre_send(message)
    await CacheTaskiqLifecycleMiddleware(lifecycle).pre_execute(message)
    metadata = {
        "version": 1,
        "task_id": message.task_id,
        "task_name": message.task_name,
        "schema_version": 1,
        "queue": "default",
        "correlation_id": message.task_id,
        "causation_id": None,
        "attempt": 1,
        "worker_id": "worker-1",
        "submitted": True,
    }
    await lifecycle.record(
        TaskLifecycleEvent(
            kind=TaskLifecycleKind.FAILED,
            task_id=UUID(metadata["task_id"]),
            task_name=metadata["task_name"],
            schema_version=metadata["schema_version"],
            queue=metadata["queue"],
            correlation_id=UUID(metadata["correlation_id"]),
            attempt=metadata["attempt"],
            worker_id=metadata["worker_id"],
            error_type="RuntimeError",
        )
    )

    recovered = _memory_lifecycle(
        lifecycle.streams,
        lifecycle.atomic,
        lifecycle.leases,
        lifecycle.policy,
    )
    await recovered.replay()

    status = await recovered.status(UUID(message.task_id))
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED


@pytest.mark.anyio
async def test_taskiq_redelivery_of_same_attempt_preserves_running_status() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    message = _message()

    await middleware.pre_send(message)
    await middleware.pre_execute(message)
    await middleware.pre_execute(message)

    status = await lifecycle.status(UUID(message.task_id))
    assert status is not None
    assert status.state is TaskState.RUNNING
    assert [event.kind for event in await lifecycle.lifecycle(status.task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
    ]


@pytest.mark.anyio
async def test_lifecycle_does_not_bind_provider_feature_composition() -> None:
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache()
    leases = InMemoryLeaseCache()

    CacheTaskLifecycle(
        streams,
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
        ),
        InMemoryCacheTime(),
    )

    assert atomic.leases is None
    assert streams.leases is None


@pytest.mark.anyio
async def test_memory_features_reject_rebinding_shared_lease_fences() -> None:
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache()
    first_leases = InMemoryLeaseCache()
    first = _memory_lifecycle(
        streams,
        atomic,
        first_leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
        ),
    )

    with pytest.raises(CacheFeatureError, match="already bound"):
        _memory_lifecycle(
            streams,
            atomic,
            InMemoryLeaseCache(),
            first.policy,
        )

    status = await first.record(
        TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.SUBMITTED,
            task_name="example.cleanup",
            schema_version=1,
            queue="default",
        )
    )
    assert status.state is TaskState.SUBMITTED


@pytest.mark.anyio
async def test_taskiq_redelivery_updates_the_replacement_worker() -> None:
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache()
    leases = InMemoryLeaseCache()
    policy = TaskiqLifecyclePolicy(
        owner="task-lifecycle",
        stream="events",
        queue="default",
        worker_id="worker-1",
        status_retention_seconds=60,
    )
    first = _memory_lifecycle(streams, atomic, leases, policy)
    second = _memory_lifecycle(
        streams,
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner=policy.owner,
            stream=policy.stream,
            queue=policy.queue,
            worker_id="worker-2",
            status_retention_seconds=policy.status_retention_seconds,
        ),
    )
    message = _message()

    await CacheTaskiqLifecycleMiddleware(first).pre_send(message)
    message.labels["_wybra_task_delivery_attempt"] = 1
    await CacheTaskiqLifecycleMiddleware(first).pre_execute(message)
    await first.record(
        TaskLifecycleEvent(
            kind=TaskLifecycleKind.PROGRESS,
            task_id=UUID(message.task_id),
            task_name=message.task_name,
            schema_version=policy.schema_version,
            queue=policy.queue,
            correlation_id=UUID(message.task_id),
            delivery_attempt=1,
            worker_id="worker-1",
            progress={"completed": 1},
        )
    )
    message.labels["_wybra_task_delivery_attempt"] = 2
    await CacheTaskiqLifecycleMiddleware(second).pre_execute(message)

    status = await second.status(UUID(message.task_id))
    assert status is not None
    assert status.worker_id == "worker-2"
    assert status.delivery_attempt == 2
    assert status.progress is None
    assert [event.worker_id for event in await second.lifecycle(status.task_id)] == [
        None,
        "worker-1",
        "worker-1",
        "worker-2",
    ]
    message.labels["_wybra_task_delivery_attempt"] = 1
    with pytest.raises(TaskLifecycleError, match="earlier delivery"):
        await CacheTaskiqLifecycleMiddleware(first).pre_execute(message)
    with pytest.raises(TaskLifecycleError, match="earlier or unknown delivery"):
        await first.record(
            TaskLifecycleEvent.new(
                kind=TaskLifecycleKind.PROGRESS,
                task_id=status.task_id,
                task_name=status.task_name,
                schema_version=status.schema_version,
                queue=status.queue,
                correlation_id=status.correlation_id,
                delivery_attempt=1,
                worker_id="worker-1",
                progress={"completed": 2},
            )
        )


@pytest.mark.anyio
async def test_delivery_attempt_fences_stale_worker_without_worker_identity() -> None:
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache()
    leases = InMemoryLeaseCache()
    lifecycle = _memory_lifecycle(
        streams,
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
        ),
    )
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    first = _message()

    await middleware.pre_send(first)
    first.labels["_wybra_task_delivery_attempt"] = 1
    await middleware.pre_execute(first)
    replacement = _message(task_id=first.task_id, labels=dict(first.labels))
    replacement.labels["_wybra_task_delivery_attempt"] = 2
    await middleware.pre_execute(replacement)

    with pytest.raises(TaskLifecycleError, match="earlier or unknown delivery"):
        await middleware.post_save(
            first,
            TaskiqResult(is_err=False, return_value=None, execution_time=0.1),
        )

    status = await lifecycle.status(UUID(first.task_id))
    assert status is not None
    assert status.state is TaskState.RUNNING
    assert status.delivery_attempt == 2


@pytest.mark.anyio
async def test_new_delivery_without_worker_id_clears_replaced_worker() -> None:
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache()
    leases = InMemoryLeaseCache()
    first = _memory_lifecycle(
        streams,
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            worker_id="worker-1",
            status_retention_seconds=60,
        ),
    )
    second = _memory_lifecycle(
        streams,
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
        ),
    )
    first_message = _message()

    await CacheTaskiqLifecycleMiddleware(first).pre_send(first_message)
    first_message.labels["_wybra_task_delivery_attempt"] = 1
    await CacheTaskiqLifecycleMiddleware(first).pre_execute(first_message)
    replacement = _message(
        task_id=first_message.task_id,
        labels=dict(first_message.labels),
    )
    replacement.labels["_wybra_task_delivery_attempt"] = 2
    await CacheTaskiqLifecycleMiddleware(second).pre_execute(replacement)

    status = await second.status(UUID(first_message.task_id))
    assert status is not None
    assert status.delivery_attempt == 2
    assert status.worker_id is None


@pytest.mark.anyio
async def test_replay_repairs_an_event_appended_before_projection_failure() -> None:
    streams = InMemoryStreamCache()
    policy = TaskiqLifecyclePolicy(
        owner="task-lifecycle",
        stream="events",
        queue="default",
        status_retention_seconds=60,
    )
    leases = InMemoryLeaseCache()
    failed = _memory_lifecycle(streams, _FailingAtomic(), leases, policy)
    middleware = CacheTaskiqLifecycleMiddleware(failed)
    message = _message()

    with pytest.raises(RuntimeError, match="projection unavailable"):
        await middleware.pre_send(message)

    recovered = _memory_lifecycle(streams, InMemoryAtomicCache(), leases, policy)
    await CacheTaskiqLifecycleMiddleware(recovered).startup()

    status = await recovered.status(UUID(message.task_id))
    assert status is not None
    assert status.state is TaskState.SUBMITTED


@pytest.mark.anyio
async def test_status_recovers_stream_event_after_projection_update_failure() -> None:
    streams = InMemoryStreamCache()
    atomic = _RecoverableProjectionFailureAtomic()
    lifecycle = _memory_lifecycle(
        streams,
        atomic,
        InMemoryLeaseCache(),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
        ),
    )
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.recovery",
        schema_version=1,
        queue="default",
    )
    started = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.STARTED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
    )
    succeeded = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUCCEEDED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
    )
    await lifecycle.record(submitted)
    await lifecycle.record(started)
    atomic.fail_updates = True

    with pytest.raises(CacheConflictError, match="projection unavailable"):
        await lifecycle.record(succeeded)

    atomic.fail_updates = False
    status = await lifecycle.status(submitted.task_id)

    assert status is not None
    assert status.state is TaskState.SUCCEEDED


@pytest.mark.anyio
async def test_projection_write_recovers_intervening_stream_facts() -> None:
    streams = InMemoryStreamCache()
    atomic = _RecoverableProjectionFailureAtomic()
    lifecycle = _memory_lifecycle(
        streams,
        atomic,
        InMemoryLeaseCache(),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
        ),
    )
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.recovery",
        schema_version=1,
        queue="default",
    )
    started = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.STARTED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
    )
    attempt_failed = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.ATTEMPT_FAILED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
        error_type="RuntimeError",
    )
    retry_scheduled = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.RETRY_SCHEDULED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
    )
    await lifecycle.record(submitted)
    await lifecycle.record(started)
    atomic.fail_updates = True

    with pytest.raises(CacheConflictError, match="projection unavailable"):
        await lifecycle.record(attempt_failed)

    atomic.fail_updates = False
    await lifecycle.record(retry_scheduled)

    status = await lifecycle.status(submitted.task_id)
    assert status is not None
    assert status.state is TaskState.RETRY_SCHEDULED
    assert status.error_type == "RuntimeError"


@pytest.mark.anyio
async def test_replay_skips_invalid_record_and_advances_its_position() -> None:
    lifecycle = _lifecycle()
    invalid = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUCCEEDED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    await lifecycle.streams.append(
        lifecycle.policy.owner,
        lifecycle.policy.stream,
        b'{"event":{"kind":"succeeded"},"version":1}',
    )
    await lifecycle.streams.append(
        lifecycle.policy.owner,
        lifecycle.policy.stream,
        (
            b'{"event":{"attempt":1,"causation_id":null,'
            b'"correlation_id":"' + str(invalid.correlation_id).encode() + b'",'
            b'"error_type":null,"kind":"submitted","occurred_at":'
            + str(time.time()).encode()
            + b',"progress":null,"queue":"default","schema_version":1,'
            b'"task_id":"' + str(invalid.task_id).encode() + b'",'
            b'"task_name":"example.cleanup","unexpected":true,'
            b'"worker_id":null},"version":1}'
        ),
    )
    await lifecycle.streams.append(
        lifecycle.policy.owner,
        lifecycle.policy.stream,
        (
            b'{"event":{"attempt":1,"causation_id":null,'
            b'"correlation_id":"' + str(invalid.correlation_id).encode() + b'",'
            b'"error_type":null,"kind":"submitted","occurred_at":'
            + str(time.time()).encode()
            + b',"progress":null,"queue":"default","schema_version":1,'
            + b'"task_id":"'
            + str(invalid.task_id).encode()
            + b'",'
            b'"task_name":"example.cleanup","delivery_attempt":null,'
            b'"delivery_identity":null,'
            b'"worker_id":null},"version":1}'
        ),
    )

    await lifecycle.replay()

    status = await lifecycle.status(invalid.task_id)
    assert status is not None
    assert status.state is TaskState.SUBMITTED
    assert [event.kind for event in await lifecycle.lifecycle(invalid.task_id)] == [
        TaskLifecycleKind.SUBMITTED
    ]


@pytest.mark.anyio
async def test_projection_does_not_reapply_an_invalid_target_record() -> None:
    lifecycle = _lifecycle()
    event = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    position = await lifecycle.streams.append(
        lifecycle.policy.owner,
        lifecycle.policy.stream,
        b'{"event":{"kind":"submitted"},"version":1}',
    )

    status = await lifecycle._project_through(
        event.task_id,
        status=None,
        after=None,
        target=position,
        lease_lost=Event(),
        target_event=event,
    )

    assert status is None


@pytest.mark.anyio
async def test_lifecycle_restarts_after_history_cursor_expiry() -> None:
    streams = _CursorExpiringStream()
    policy = TaskiqLifecyclePolicy(
        owner="task-lifecycle",
        stream="events",
        queue="default",
        status_retention_seconds=60,
        replay_page_limit=1,
    )
    lifecycle = _memory_lifecycle(
        streams,
        InMemoryAtomicCache(),
        InMemoryLeaseCache(),
        policy,
    )
    task_id = uuid4()
    submitted = TaskLifecycleEvent(
        kind=TaskLifecycleKind.SUBMITTED,
        task_id=task_id,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
        correlation_id=task_id,
    )
    await lifecycle.record(submitted)
    await lifecycle.record(
        TaskLifecycleEvent(
            kind=TaskLifecycleKind.STARTED,
            task_id=task_id,
            task_name="example.cleanup",
            schema_version=1,
            queue="default",
            correlation_id=task_id,
            occurred_at=submitted.occurred_at + 1,
        )
    )
    streams.expire_after_first_page = True

    events = await lifecycle.lifecycle(task_id)

    assert [event.kind for event in events] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
    ]


@pytest.mark.anyio
async def test_expired_projection_lease_rejects_stale_status_write() -> None:
    clock = _Clock()
    task_id = uuid4()
    atomic = InMemoryAtomicCache(clock)
    lifecycle = _memory_lifecycle(
        _LeaseExpiringStream(clock),
        atomic,
        InMemoryLeaseCache(clock),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
            active_status_timeout_seconds=60,
            lifecycle_lease_ttl_seconds=10,
        ),
        _clock=clock,
    )

    with pytest.raises(CacheConflictError, match="stale or no longer held"):
        await lifecycle.record(
            TaskLifecycleEvent(
                kind=TaskLifecycleKind.SUBMITTED,
                task_id=task_id,
                task_name="example.cleanup",
                schema_version=1,
                queue="default",
                correlation_id=task_id,
                occurred_at=clock.value,
            )
        )

    assert await atomic.get(lifecycle.policy.owner, f"status:{task_id}") is None
    status = await lifecycle.status(task_id)
    assert status is not None
    assert status.state is TaskState.SUBMITTED


@pytest.mark.anyio
async def test_replay_preserves_live_terminal_status_despite_producer_clock_skew() -> (
    None
):
    lifecycle = _lifecycle()
    task_id = uuid4()
    occurred_at = time.time() - 120
    events = (
        TaskLifecycleEvent(
            kind=TaskLifecycleKind.SUBMITTED,
            task_id=task_id,
            task_name="example.cleanup",
            schema_version=1,
            queue="default",
            correlation_id=task_id,
            occurred_at=occurred_at,
        ),
        TaskLifecycleEvent(
            kind=TaskLifecycleKind.STARTED,
            task_id=task_id,
            task_name="example.cleanup",
            schema_version=1,
            queue="default",
            correlation_id=task_id,
            occurred_at=occurred_at + 1,
        ),
        TaskLifecycleEvent(
            kind=TaskLifecycleKind.SUCCEEDED,
            task_id=task_id,
            task_name="example.cleanup",
            schema_version=1,
            queue="default",
            correlation_id=task_id,
            occurred_at=occurred_at + 2,
        ),
    )
    for event in events:
        await lifecycle.record(event)

    recovered = _memory_lifecycle(
        lifecycle.streams,
        lifecycle.atomic,
        lifecycle.leases,
        lifecycle.policy,
    )
    await recovered.replay()

    status = await recovered.status(task_id)
    assert status is not None
    assert status.state is TaskState.SUCCEEDED


@pytest.mark.anyio
async def test_duplicate_terminal_event_does_not_recreate_expired_status() -> None:
    clock = _Clock()
    leases = InMemoryLeaseCache(clock)
    atomic = InMemoryAtomicCache(clock)
    lifecycle = _memory_lifecycle(
        InMemoryStreamCache(),
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=10,
        ),
        _clock=clock,
    )
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    started = TaskLifecycleEvent(
        kind=TaskLifecycleKind.STARTED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
        occurred_at=1,
    )
    succeeded = TaskLifecycleEvent(
        kind=TaskLifecycleKind.SUCCEEDED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
        occurred_at=2,
    )
    for event in (submitted, started, succeeded):
        await lifecycle.record(event)

    clock.value = 11
    assert await lifecycle.status(submitted.task_id) is None

    status = await lifecycle.record(succeeded)

    assert status.state is TaskState.SUCCEEDED
    assert await lifecycle.status(submitted.task_id) is None


@pytest.mark.anyio
async def test_replay_does_not_invent_dead_letter_from_orphaned_failure() -> None:
    streams = InMemoryStreamCache(max_records=1)
    atomic = InMemoryAtomicCache()
    policy = TaskiqLifecyclePolicy(
        owner="task-lifecycle",
        stream="events",
        queue="default",
        status_retention_seconds=60,
    )
    leases = InMemoryLeaseCache()
    lifecycle = _memory_lifecycle(streams, atomic, leases, policy)
    task_id = uuid4()
    submitted = TaskLifecycleEvent(
        kind=TaskLifecycleKind.SUBMITTED,
        task_id=task_id,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
        correlation_id=task_id,
    )
    started = TaskLifecycleEvent(
        kind=TaskLifecycleKind.STARTED,
        task_id=task_id,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
        correlation_id=task_id,
        occurred_at=submitted.occurred_at + 1,
    )
    failed = TaskLifecycleEvent(
        kind=TaskLifecycleKind.FAILED,
        task_id=task_id,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
        correlation_id=task_id,
        error_type="RuntimeError",
        occurred_at=submitted.occurred_at + 2,
    )
    for event in (submitted, started, failed):
        await lifecycle.record(event)
    snapshot = await atomic.get(policy.owner, f"status:{task_id}")
    assert snapshot is not None
    assert await atomic.compare_and_delete(
        policy.owner,
        f"status:{task_id}",
        snapshot.revision,
    )

    recovered = _memory_lifecycle(streams, atomic, leases, policy)
    await recovered.replay()

    assert await recovered.status(task_id) is None
    assert len(await streams.read(policy.owner, policy.stream)) == 1


@pytest.mark.anyio
async def test_replay_waits_for_a_concurrent_projection_writer() -> None:
    atomic = _TransientReplayConflictAtomic()
    lifecycle = _memory_lifecycle(
        InMemoryStreamCache(),
        atomic,
        InMemoryLeaseCache(),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
            projection_cas_max_attempts=1,
        ),
    )
    task_id = uuid4()
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_id=task_id,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    started = TaskLifecycleEvent(
        kind=TaskLifecycleKind.STARTED,
        task_id=task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
        occurred_at=submitted.occurred_at + 1,
    )
    failed = TaskLifecycleEvent(
        kind=TaskLifecycleKind.FAILED,
        task_id=task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
        error_type="RuntimeError",
        occurred_at=submitted.occurred_at + 2,
    )
    for event in (submitted, started, failed):
        await lifecycle.record(event)

    recovered = _memory_lifecycle(
        lifecycle.streams,
        atomic,
        lifecycle.leases,
        lifecycle.policy,
    )
    atomic.failed_key = f"status:{task_id}"
    await recovered.replay()

    status = await recovered.status(task_id)
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED


@pytest.mark.anyio
async def test_replay_retains_dead_letter_repair_after_projection_contention() -> None:
    streams = InMemoryStreamCache()
    leases = InMemoryLeaseCache()
    policy = TaskiqLifecyclePolicy(
        owner="task-lifecycle",
        stream="events",
        queue="default",
        status_retention_seconds=60,
    )
    source = _memory_lifecycle(
        streams,
        InMemoryAtomicCache(),
        leases,
        policy,
    )
    failed_task = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.failed",
        schema_version=1,
        queue="default",
    )
    started = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.STARTED,
        task_id=failed_task.task_id,
        task_name=failed_task.task_name,
        schema_version=failed_task.schema_version,
        queue=failed_task.queue,
        correlation_id=failed_task.correlation_id,
    )
    failed = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.FAILED,
        task_id=failed_task.task_id,
        task_name=failed_task.task_name,
        schema_version=failed_task.schema_version,
        queue=failed_task.queue,
        correlation_id=failed_task.correlation_id,
        error_type="RuntimeError",
    )
    later_task = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.later",
        schema_version=1,
        queue="default",
    )
    for event in (failed_task, started, failed, later_task):
        await source.record(event)

    atomic = _SelectedKeyFailingAtomic()
    atomic.failed_key = f"status:{later_task.task_id}"
    recovered = _memory_lifecycle(streams, atomic, leases, policy)

    await recovered.replay()

    status = await recovered.status(failed_task.task_id)
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED


@pytest.mark.anyio
async def test_record_fails_when_projection_lease_is_lost_during_persist() -> None:
    streams = InMemoryStreamCache()
    atomic = _SlowProjectionAtomic()
    lifecycle = _memory_lifecycle(
        streams,
        atomic,
        _FailingRenewalLease(),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
            lifecycle_lease_ttl_seconds=0.05,
        ),
    )
    event = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )

    with pytest.raises(CacheConflictError, match="lease was lost"):
        await lifecycle.record(event)

    assert len(await streams.read("task-lifecycle", "events")) == 1
    assert await atomic.get("task-lifecycle", f"status:{event.task_id}") is None
    status = await lifecycle.status(event.task_id)
    assert status is not None
    assert status.state is TaskState.SUBMITTED


@pytest.mark.anyio
async def test_record_renews_during_a_slow_fenced_projection_write() -> None:
    streams = InMemoryStreamCache()
    atomic = _SlowCreateAtomic()
    leases = InMemoryLeaseCache()
    lifecycle = _memory_lifecycle(
        streams,
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
            lifecycle_lease_ttl_seconds=0.05,
        ),
    )
    event = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )

    status = await lifecycle.record(event)

    assert status.state is TaskState.SUBMITTED
    assert await lifecycle.status(event.task_id) is not None


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("replacement", "match"),
    [
        ("queue", "identity does not match"),
        ("attempt", "attempt must be 1"),
    ],
)
async def test_same_worker_start_is_not_deduplicated_when_identity_changes(
    replacement: str,
    match: str,
) -> None:
    lifecycle = _lifecycle()
    task_id = uuid4()
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
        task_id=task_id,
        worker_id="worker-1",
    )
    started = TaskLifecycleEvent(
        kind=TaskLifecycleKind.STARTED,
        task_id=task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
        worker_id="worker-1",
    )
    await lifecycle.record(submitted)
    await lifecycle.record(started)
    changed = TaskLifecycleEvent(
        kind=TaskLifecycleKind.STARTED,
        task_id=task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue="other" if replacement == "queue" else submitted.queue,
        correlation_id=submitted.correlation_id,
        attempt=2 if replacement == "attempt" else 1,
        worker_id="worker-1",
    )

    with pytest.raises(TaskLifecycleError, match=match):
        await lifecycle.record(changed)

    assert [event.kind for event in await lifecycle.lifecycle(task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.STARTED,
    ]


@pytest.mark.anyio
async def test_replay_retention_uses_shared_cache_time_not_worker_time() -> None:
    provider_clock = _Clock()
    provider_clock.value = 100
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache(provider_clock)
    leases = InMemoryLeaseCache(provider_clock)
    policy = TaskiqLifecyclePolicy(
        owner="task-lifecycle",
        stream="events",
        queue="default",
        status_retention_seconds=10,
    )
    first = _memory_lifecycle(
        streams,
        atomic,
        leases,
        policy,
        _clock=_Clock(),
        cache_time=InMemoryCacheTime(provider_clock),
    )
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    started = TaskLifecycleEvent(
        kind=TaskLifecycleKind.STARTED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
    )
    succeeded = TaskLifecycleEvent(
        kind=TaskLifecycleKind.SUCCEEDED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
    )
    for event in (submitted, started, succeeded):
        await first.record(event)

    provider_clock.value = 105
    skewed_worker_clock = _Clock()
    skewed_worker_clock.value = 10_000
    recovered = _memory_lifecycle(
        streams,
        atomic,
        leases,
        policy,
        _clock=skewed_worker_clock,
        cache_time=InMemoryCacheTime(provider_clock),
    )
    await recovered.replay()

    assert await recovered.status(submitted.task_id) is not None


@pytest.mark.anyio
async def test_replay_refreshes_cache_time_before_each_persisted_event() -> None:
    clock = _Clock()
    streams = InMemoryStreamCache()
    source_time = InMemoryCacheTime(clock)
    leases = InMemoryLeaseCache(clock)
    source = _memory_lifecycle(
        streams,
        InMemoryAtomicCache(clock),
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
        ),
        cache_time=source_time,
    )
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    started = TaskLifecycleEvent(
        kind=TaskLifecycleKind.STARTED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
    )
    await source.record(submitted)
    await source.record(started)

    replay_time = _CountingCacheTime(clock)
    recovered = _memory_lifecycle(
        streams,
        InMemoryAtomicCache(clock),
        leases,
        source.policy,
        cache_time=replay_time,
    )

    await recovered.replay()

    assert replay_time.refreshes == 4


@pytest.mark.anyio
async def test_lifecycle_refreshes_time_after_acquiring_its_lease() -> None:
    clock = _Clock()
    leases = InMemoryLeaseCache(clock)
    lifecycle = _memory_lifecycle(
        InMemoryStreamCache(),
        InMemoryAtomicCache(clock),
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
        ),
        cache_time=_LeaseAwareCacheTime(clock, leases),
    )

    await lifecycle.record(
        TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.SUBMITTED,
            task_name="example.cleanup",
            schema_version=1,
            queue="default",
        )
    )


@pytest.mark.anyio
async def test_record_rejects_status_write_after_lease_expires_during_create() -> None:
    streams = InMemoryStreamCache()
    lifecycle = _memory_lifecycle(
        streams,
        _SlowCreateAtomic(),
        _FailingRenewalLease(),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
            lifecycle_lease_ttl_seconds=0.05,
        ),
    )
    event = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )

    with pytest.raises(CacheConflictError, match="stale or no longer held"):
        await lifecycle.record(event)

    assert (
        await lifecycle.atomic.get(
            "task-lifecycle",
            f"status:{event.task_id}",
        )
        is None
    )
    status = await lifecycle.status(event.task_id)
    assert status is not None
    assert status.state is TaskState.SUBMITTED
    assert [record.kind for record in await lifecycle.lifecycle(event.task_id)] == [
        TaskLifecycleKind.SUBMITTED
    ]


@pytest.mark.anyio
async def test_taskiq_lifecycle_rejects_retry_middleware_after_lifecycle() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    broker = InMemoryBroker()
    broker.add_middlewares(
        middleware,
        CacheTaskiqSimpleRetryMiddleware(middleware),
    )

    with pytest.raises(RuntimeError, match="must be installed after"):
        await middleware.startup()


@pytest.mark.anyio
@pytest.mark.parametrize(
    "retry_middleware", (SimpleRetryMiddleware, SmartRetryMiddleware)
)
async def test_taskiq_lifecycle_requires_a_retry_handoff_observer(
    retry_middleware: type[SimpleRetryMiddleware] | type[SmartRetryMiddleware],
) -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    broker = InMemoryBroker()
    broker.add_middlewares(retry_middleware(), middleware)

    with pytest.raises(RuntimeError, match="requires a cache-aware"):
        await middleware.startup()


@pytest.mark.anyio
async def test_taskiq_lifecycle_rejects_scheduled_smart_retries() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    broker = InMemoryBroker()
    broker.add_middlewares(
        CacheTaskiqSmartRetryMiddleware(
            middleware,
            schedule_source=Mock(spec=ScheduleSource),
        ),
        middleware,
    )

    with pytest.raises(RuntimeError, match="does not support"):
        await middleware.startup()


def test_taskiq_smart_retry_uses_declared_backoff_and_jitter_labels() -> None:
    lifecycle = _lifecycle()
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    retry = CacheTaskiqSmartRetryMiddleware(
        middleware,
        default_delay=1,
    )
    message = TaskiqMessage(
        task_id=str(uuid4()),
        task_name="tests.task",
        labels={
            TASK_RETRY_INITIAL_DELAY_LABEL: 2,
            TASK_RETRY_BACKOFF_MULTIPLIER_LABEL: 3,
            TASK_RETRY_MAXIMUM_DELAY_LABEL: 10,
            TASK_RETRY_JITTER_SECONDS_LABEL: 0,
        },
        args=[],
        kwargs={},
    )

    assert retry.make_delay(message, retries=1) == 2
    assert retry.make_delay(message, retries=2) == 6
    assert retry.make_delay(message, retries=3) == 10


@pytest.mark.anyio
async def test_active_status_expires_after_abandonment_timeout() -> None:
    clock = _Clock()
    atomic = InMemoryAtomicCache(clock)
    streams = InMemoryStreamCache()
    lifecycle = _memory_lifecycle(
        streams,
        atomic,
        InMemoryLeaseCache(),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=10,
            active_status_timeout_seconds=10,
        ),
        _clock=clock,
    )
    task_id = uuid4()
    submitted = TaskLifecycleEvent(
        kind=TaskLifecycleKind.SUBMITTED,
        task_id=task_id,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
        correlation_id=task_id,
        occurred_at=clock.value,
    )
    started = TaskLifecycleEvent(
        kind=TaskLifecycleKind.STARTED,
        task_id=task_id,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
        correlation_id=task_id,
        occurred_at=clock.value + 1,
    )
    await lifecycle.record(submitted)
    await lifecycle.record(started)

    clock.value = 11
    status = await lifecycle.status(task_id)
    assert status is None

    with pytest.raises(TaskLifecycleError, match="Invalid task lifecycle"):
        await lifecycle.record(
            TaskLifecycleEvent(
                kind=TaskLifecycleKind.SUCCEEDED,
                task_id=task_id,
                task_name=submitted.task_name,
                schema_version=submitted.schema_version,
                queue=submitted.queue,
                correlation_id=submitted.correlation_id,
                occurred_at=clock.value,
            )
        )


@pytest.mark.anyio
async def test_active_status_is_hidden_when_projection_ttl_lingers() -> None:
    clock = _Clock()
    atomic = _LingeringProjectionAtomic(clock)
    lifecycle = _memory_lifecycle(
        InMemoryStreamCache(),
        atomic,
        InMemoryLeaseCache(clock),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
            active_status_timeout_seconds=10,
        ),
        _clock=clock,
    )
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    await lifecycle.record(submitted)
    await lifecycle.record(
        TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.STARTED,
            task_id=submitted.task_id,
            task_name=submitted.task_name,
            schema_version=submitted.schema_version,
            queue=submitted.queue,
            correlation_id=submitted.correlation_id,
        )
    )
    clock.value = 11

    assert await lifecycle.status(submitted.task_id) is None
    with pytest.raises(TaskLifecycleError, match="Invalid task lifecycle"):
        await lifecycle.record(
            TaskLifecycleEvent.new(
                kind=TaskLifecycleKind.SUCCEEDED,
                task_id=submitted.task_id,
                task_name=submitted.task_name,
                schema_version=submitted.schema_version,
                queue=submitted.queue,
                correlation_id=submitted.correlation_id,
            )
        )


@pytest.mark.anyio
async def test_orphaned_terminal_after_abandonment_is_rejected() -> None:
    clock = _Clock()
    streams = InMemoryStreamCache(max_records=1)
    lifecycle = _memory_lifecycle(
        streams,
        InMemoryAtomicCache(clock),
        InMemoryLeaseCache(),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
            active_status_timeout_seconds=10,
        ),
        _clock=clock,
    )
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    started = TaskLifecycleEvent(
        kind=TaskLifecycleKind.STARTED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
        occurred_at=1,
    )
    await lifecycle.record(submitted)
    await lifecycle.record(started)

    clock.value = 11
    assert await lifecycle.status(submitted.task_id) is None
    with pytest.raises(TaskLifecycleError, match="Invalid task lifecycle"):
        await lifecycle.record(
            TaskLifecycleEvent(
                kind=TaskLifecycleKind.SUCCEEDED,
                task_id=submitted.task_id,
                task_name=submitted.task_name,
                schema_version=submitted.schema_version,
                queue=submitted.queue,
                correlation_id=submitted.correlation_id,
                occurred_at=clock.value,
            )
        )


@pytest.mark.anyio
async def test_projection_preserves_an_event_evicted_after_its_append() -> None:
    lifecycle = _memory_lifecycle(
        _TargetEvictingStream(),
        InMemoryAtomicCache(),
        InMemoryLeaseCache(),
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
        ),
    )
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    await lifecycle.record(submitted)
    await lifecycle.record(
        TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.STARTED,
            task_id=submitted.task_id,
            task_name=submitted.task_name,
            schema_version=submitted.schema_version,
            queue=submitted.queue,
            correlation_id=submitted.correlation_id,
        )
    )
    status = await lifecycle.record(
        TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.SUCCEEDED,
            task_id=submitted.task_id,
            task_name=submitted.task_name,
            schema_version=submitted.schema_version,
            queue=submitted.queue,
            correlation_id=submitted.correlation_id,
        )
    )

    assert status.state is TaskState.SUCCEEDED
    assert (await lifecycle.status(submitted.task_id)).state is TaskState.SUCCEEDED


@pytest.mark.anyio
async def test_dead_letter_repair_preserves_terminal_retention_age() -> None:
    clock = _Clock()
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache(clock)
    leases = InMemoryLeaseCache(clock)
    policy = TaskiqLifecyclePolicy(
        owner="task-lifecycle",
        stream="events",
        queue="default",
        status_retention_seconds=10,
    )
    lifecycle = _memory_lifecycle(streams, atomic, leases, policy, _clock=clock)
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    await lifecycle.record(submitted)
    await lifecycle.record(
        TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.STARTED,
            task_id=submitted.task_id,
            task_name=submitted.task_name,
            schema_version=submitted.schema_version,
            queue=submitted.queue,
            correlation_id=submitted.correlation_id,
        )
    )
    await lifecycle.record(
        TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.FAILED,
            task_id=submitted.task_id,
            task_name=submitted.task_name,
            schema_version=submitted.schema_version,
            queue=submitted.queue,
            correlation_id=submitted.correlation_id,
            error_type="RuntimeError",
        )
    )
    snapshot = await atomic.get(policy.owner, f"status:{submitted.task_id}")
    assert snapshot is not None
    assert await atomic.compare_and_delete(
        policy.owner,
        f"status:{submitted.task_id}",
        snapshot.revision,
    )
    clock.value = 5

    recovered = _memory_lifecycle(streams, atomic, leases, policy, _clock=clock)
    await recovered.replay()
    repaired = await recovered.status(submitted.task_id)
    assert repaired is not None
    assert repaired.state is TaskState.DEAD_LETTERED

    clock.value = 11
    assert await recovered.status(submitted.task_id) is None


@pytest.mark.anyio
async def test_dead_letter_transition_preserves_failed_retention_age() -> None:
    clock = _Clock()
    policy = TaskiqLifecyclePolicy(
        owner="task-lifecycle",
        stream="events",
        queue="default",
        status_retention_seconds=10,
    )
    lifecycle = _memory_lifecycle(
        InMemoryStreamCache(),
        InMemoryAtomicCache(clock),
        InMemoryLeaseCache(clock),
        policy,
        _clock=clock,
    )
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    await lifecycle.record(submitted)
    await lifecycle.record(
        TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.STARTED,
            task_id=submitted.task_id,
            task_name=submitted.task_name,
            schema_version=submitted.schema_version,
            queue=submitted.queue,
            correlation_id=submitted.correlation_id,
        )
    )
    await lifecycle.record(
        TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.FAILED,
            task_id=submitted.task_id,
            task_name=submitted.task_name,
            schema_version=submitted.schema_version,
            queue=submitted.queue,
            correlation_id=submitted.correlation_id,
            error_type="RuntimeError",
        )
    )
    clock.value = 5
    await lifecycle.record(
        TaskLifecycleEvent.new(
            kind=TaskLifecycleKind.DEAD_LETTERED,
            task_id=submitted.task_id,
            task_name=submitted.task_name,
            schema_version=submitted.schema_version,
            queue=submitted.queue,
            correlation_id=submitted.correlation_id,
            error_type="RuntimeError",
        )
    )

    clock.value = 11
    assert await lifecycle.status(submitted.task_id) is None


@pytest.mark.anyio
async def test_empty_progress_metadata_survives_lifecycle_replay() -> None:
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache()
    leases = InMemoryLeaseCache()
    policy = TaskiqLifecyclePolicy(
        owner="task-lifecycle",
        stream="events",
        queue="default",
        status_retention_seconds=60,
    )
    lifecycle = _memory_lifecycle(streams, atomic, leases, policy)
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    await lifecycle.record(submitted)
    for progress in ({"completed": 1}, {}):
        await lifecycle.record(
            TaskLifecycleEvent.new(
                kind=(
                    TaskLifecycleKind.STARTED
                    if progress == {"completed": 1}
                    else TaskLifecycleKind.PROGRESS
                ),
                task_id=submitted.task_id,
                task_name=submitted.task_name,
                schema_version=submitted.schema_version,
                queue=submitted.queue,
                correlation_id=submitted.correlation_id,
                progress=progress,
            )
        )
    snapshot = await atomic.get(policy.owner, f"status:{submitted.task_id}")
    assert snapshot is not None
    assert await atomic.compare_and_delete(
        policy.owner,
        f"status:{submitted.task_id}",
        snapshot.revision,
    )

    recovered = _memory_lifecycle(streams, atomic, leases, policy)
    await recovered.replay()
    status = await recovered.status(submitted.task_id)
    assert status is not None
    assert status.progress == {}


@pytest.mark.anyio
async def test_replay_retains_terminal_status_for_its_configured_retention() -> None:
    clock = _Clock()
    atomic = InMemoryAtomicCache(clock)
    policy = TaskiqLifecyclePolicy(
        owner="task-lifecycle",
        stream="events",
        queue="default",
        status_retention_seconds=10_000,
        active_status_timeout_seconds=60,
    )
    lifecycle = _memory_lifecycle(
        InMemoryStreamCache(),
        atomic,
        InMemoryLeaseCache(),
        policy,
        _clock=clock,
    )
    submitted = TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.SUBMITTED,
        task_name="example.cleanup",
        schema_version=1,
        queue="default",
    )
    started = TaskLifecycleEvent(
        kind=TaskLifecycleKind.STARTED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
        occurred_at=1,
    )
    succeeded = TaskLifecycleEvent(
        kind=TaskLifecycleKind.SUCCEEDED,
        task_id=submitted.task_id,
        task_name=submitted.task_name,
        schema_version=submitted.schema_version,
        queue=submitted.queue,
        correlation_id=submitted.correlation_id,
        occurred_at=2,
    )
    for event in (submitted, started, succeeded):
        await lifecycle.record(event)
    snapshot = await atomic.get(policy.owner, f"status:{submitted.task_id}")
    assert snapshot is not None
    assert await atomic.compare_and_delete(
        policy.owner,
        f"status:{submitted.task_id}",
        snapshot.revision,
    )

    clock.value = 4_000
    recovered = _memory_lifecycle(
        lifecycle.streams,
        atomic,
        lifecycle.leases,
        policy,
        _clock=clock,
    )
    await recovered.replay()

    status = await recovered.status(submitted.task_id)
    assert status is not None
    assert status.state is TaskState.SUCCEEDED
