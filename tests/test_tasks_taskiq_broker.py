from __future__ import annotations

import asyncio
from contextlib import suppress
from dataclasses import dataclass, field
from typing import cast

import pytest
from taskiq import AckableMessage, BrokerMessage, TaskiqEvents, TaskiqState
from taskiq.message import TaskiqMessage
from taskiq.utils import maybe_awaitable

from wybra.cache import (
    CacheConflictError,
    CacheFeatureError,
    InMemoryWorkQueue,
    WorkDelivery,
    WorkIdentity,
    WorkQueueCacheCapability,
)
from wybra.tasks.lifecycle import TaskLifecycleError
from wybra.tasks.taskiq_broker import CacheTaskiqBroker, TaskiqBrokerPolicy
from wybra.tasks.taskiq_protocol import (
    DELIVERY_ATTEMPT_LABEL,
    DELIVERY_RECEIPT_LABEL,
    TASK_VISIBILITY_TIMEOUT_LABEL,
)


@dataclass
class RecordingWorkQueue:
    publications: list[tuple[str, str, bytes, float, int]] = field(default_factory=list)

    async def publish(
        self,
        owner: str,
        queue: str,
        payload: bytes,
        *,
        delay: float = 0,
        max_attempts: int = 3,
    ) -> WorkIdentity:
        self.publications.append((owner, queue, payload, delay, max_attempts))
        return WorkIdentity("recorded-work")


class FailingReserveWorkQueue:
    async def reserve(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        raise AssertionError("A stopped broker must not reserve work.")


class FailFirstDeadLetterReadWorkQueue(InMemoryWorkQueue):
    def __init__(self) -> None:
        super().__init__()
        self._fail_next_dead_letter_read = True

    async def dead_letters(
        self,
        owner: str,
        queue: str,
        *,
        limit: int = 100,
    ) -> tuple[WorkDelivery, ...]:
        if self._fail_next_dead_letter_read:
            self._fail_next_dead_letter_read = False
            raise CacheFeatureError("Dead-letter store temporarily unavailable.")
        return await super().dead_letters(owner, queue, limit=limit)


@dataclass
class BlockingReserveWorkQueue:
    reserve_started: asyncio.Event = field(default_factory=asyncio.Event)
    reserve_cancelled: asyncio.Event = field(default_factory=asyncio.Event)

    async def reserve(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        self.reserve_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.reserve_cancelled.set()
            raise


@dataclass
class EmptyThenBlockingWorkQueue:
    empty_wait_completed: asyncio.Event = field(default_factory=asyncio.Event)
    second_reservation_started: asyncio.Event = field(default_factory=asyncio.Event)
    reserve_cancelled: asyncio.Event = field(default_factory=asyncio.Event)
    _calls: int = 0

    async def reserve(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        self._calls += 1
        if self._calls == 1:
            self.empty_wait_completed.set()
            return None
        self.second_reservation_started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.reserve_cancelled.set()
            raise


@dataclass
class DelayedCancellationWorkQueue:
    first_reservation_started: asyncio.Event = field(default_factory=asyncio.Event)
    cancellation_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_cancellation: asyncio.Event = field(default_factory=asyncio.Event)
    second_reservation_started: asyncio.Event = field(default_factory=asyncio.Event)
    _calls: int = 0

    async def reserve(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        self._calls += 1
        if self._calls == 1:
            self.first_reservation_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                self.cancellation_started.set()
                await self.release_cancellation.wait()
                raise
        self.second_reservation_started.set()
        await asyncio.Event().wait()


@dataclass
class DeferredReservationWorkQueue:
    queue: InMemoryWorkQueue
    reservation_started: asyncio.Event = field(default_factory=asyncio.Event)
    release_reservation: asyncio.Event = field(default_factory=asyncio.Event)

    async def publish(
        self,
        owner: str,
        queue: str,
        payload: bytes,
        *,
        delay: float = 0,
        max_attempts: int = 3,
    ) -> WorkIdentity:
        return await self.queue.publish(
            owner,
            queue,
            payload,
            delay=delay,
            max_attempts=max_attempts,
        )

    async def reserve(
        self,
        owner: str,
        queue: str,
        consumer: str,
        *,
        visibility_timeout: float,
        wait_timeout: float = 0,
    ) -> WorkDelivery | None:
        delivery = await self.queue.reserve(
            owner,
            queue,
            consumer,
            visibility_timeout=visibility_timeout,
            wait_timeout=wait_timeout,
        )
        if delivery is not None:
            self.reservation_started.set()
            await self.release_reservation.wait()
        return delivery

    async def acknowledge(self, delivery: WorkDelivery) -> None:
        await self.queue.acknowledge(delivery)


@dataclass
class FailingWorkQueue:
    operation: str
    delivery: WorkDelivery = field(
        default_factory=lambda: WorkDelivery(
            queue="default",
            identity=WorkIdentity("delivery"),
            payload=b"taskiq-message",
            attempt=1,
            visible_until=30,
            receipt="receipt",
        )
    )

    async def publish(
        self,
        owner: str,
        queue: str,
        payload: bytes,
        *,
        delay: float = 0,
        max_attempts: int = 3,
    ) -> WorkIdentity:
        del owner, queue, payload, delay, max_attempts
        if self.operation == "publish":
            raise OSError("publish failed")
        return WorkIdentity("published-work")

    async def reserve(
        self,
        owner: str,
        queue: str,
        consumer: str,
        *,
        visibility_timeout: float,
        wait_timeout: float = 0,
    ) -> WorkDelivery:
        del owner, queue, consumer, visibility_timeout, wait_timeout
        if self.operation == "reserve":
            raise OSError("reserve failed")
        return self.delivery

    async def acknowledge(self, delivery: WorkDelivery) -> None:
        del delivery
        if self.operation == "acknowledge":
            raise OSError("acknowledge failed")


@dataclass
class FakeClock:
    now: float = 0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _require_ackable(message: bytes | AckableMessage) -> AckableMessage:
    assert isinstance(message, AckableMessage)
    return message


async def _acknowledge(message: AckableMessage) -> None:
    await maybe_awaitable(message.ack())


@pytest.mark.parametrize(
    (
        "visibility_timeout_seconds",
        "wait_timeout_seconds",
        "maximum_delivery_attempts",
        "message",
    ),
    (
        (0, 1, 3, "Visibility timeout"),
        (float("nan"), 1, 3, "Visibility timeout"),
        (True, 1, 3, "Visibility timeout"),
        (30, 0, 3, "Wait timeout"),
        (30, float("inf"), 3, "Wait timeout"),
        (30, True, 3, "Wait timeout"),
        (30, 1, 0, "Maximum delivery attempts"),
        (30, 1, 4, "Maximum delivery attempts"),
        (30, 1, True, "Maximum delivery attempts"),
    ),
)
def test_taskiq_broker_policy_rejects_invalid_delivery_controls(
    visibility_timeout_seconds: float,
    wait_timeout_seconds: float,
    maximum_delivery_attempts: int,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        TaskiqBrokerPolicy(
            visibility_timeout_seconds=visibility_timeout_seconds,
            wait_timeout_seconds=wait_timeout_seconds,
            maximum_delivery_attempts=maximum_delivery_attempts,
        )


def test_taskiq_broker_does_not_shorten_default_delivery_visibility() -> None:
    broker = CacheTaskiqBroker(
        InMemoryWorkQueue(),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )

    assert (
        broker.delivery_visibility_timeout(
            {TASK_VISIBILITY_TIMEOUT_LABEL: 1},
        )
        == 30
    )


@pytest.mark.parametrize(
    ("queue", "consumer", "message"),
    (
        (" ", "worker-1", "Queue"),
        ("default", " ", "Consumer"),
    ),
)
def test_cache_taskiq_broker_rejects_blank_queue_or_consumer(
    queue: str,
    consumer: str,
    message: str,
) -> None:
    with pytest.raises(ValueError, match=message):
        CacheTaskiqBroker(
            InMemoryWorkQueue(),
            queue=queue,
            consumer=consumer,
            policy=TaskiqBrokerPolicy(
                visibility_timeout_seconds=30,
                wait_timeout_seconds=1,
                maximum_delivery_attempts=3,
            ),
        )


@pytest.mark.anyio
async def test_cache_taskiq_broker_publishes_and_acknowledges_delivery() -> None:
    broker = CacheTaskiqBroker(
        InMemoryWorkQueue(),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )
    message = BrokerMessage(
        task_id="task-123",
        task_name="tests.example",
        message=b"taskiq-message",
        labels={},
    )

    await broker.kick(message)
    listener = broker.listen()
    received = _require_ackable(await anext(listener))

    assert received.data == b"taskiq-message"
    await _acknowledge(received)
    with pytest.raises(CacheConflictError, match="stale or no longer reserved"):
        await _acknowledge(received)
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_broker_preserves_taskiq_task_identity() -> None:
    broker = CacheTaskiqBroker(
        InMemoryWorkQueue(),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )
    message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task-123",
            task_name="tests.example",
            labels={},
            args=[1],
            kwargs={"name": "Wybra"},
        )
    )

    await broker.kick(message)
    listener = broker.listen()
    received = _require_ackable(await anext(listener))
    restored = broker.formatter.loads(message=received.data)

    assert restored.task_id == "task-123"
    assert restored.task_name == "tests.example"
    assert restored.args == [1]
    assert restored.kwargs == {"name": "Wybra"}
    assert restored.labels[DELIVERY_ATTEMPT_LABEL] == 1
    assert isinstance(restored.labels[DELIVERY_RECEIPT_LABEL], str)
    await _acknowledge(received)
    await listener.aclose()


@pytest.mark.anyio
async def test_broker_keeps_unvalidated_delivery_at_default_visibility() -> None:
    clock = FakeClock()
    work_queue = InMemoryWorkQueue(clock)
    broker = CacheTaskiqBroker(
        work_queue,
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=0.1,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )
    message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task-123",
            task_name="tests.removed",
            labels={TASK_VISIBILITY_TIMEOUT_LABEL: 1},
            args=[],
            kwargs={},
        )
    )

    await broker.kick(message)
    listener = broker.listen()
    received = _require_ackable(await anext(listener))
    restored = broker.formatter.loads(message=received.data)
    receipt = restored.labels[DELIVERY_RECEIPT_LABEL]

    assert isinstance(receipt, str)
    assert broker.delivery_visibility_timeout(restored.labels) == 1
    await broker.relinquish_delivery(receipt)
    with pytest.raises(CacheConflictError, match="stale or no longer reserved"):
        await broker.renew_delivery(receipt, visibility_timeout=1)
    clock.advance(0.2)
    redelivered = await work_queue.reserve(
        "taskiq-broker",
        "default",
        "worker-2",
        visibility_timeout=0.1,
    )
    assert redelivered is not None
    assert redelivered.payload == message.message
    await work_queue.acknowledge(redelivered)
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_broker_does_not_restore_delivery_after_acknowledgement_race(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    work_queue = InMemoryWorkQueue()
    broker = CacheTaskiqBroker(
        work_queue,
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=1,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )
    renewal_completed = asyncio.Event()
    release_renewal = asyncio.Event()
    original_renew = InMemoryWorkQueue.renew

    async def held_renew(
        queue: InMemoryWorkQueue,
        *args: object,
        **kwargs: object,
    ) -> WorkDelivery:
        renewed = await original_renew(queue, *args, **kwargs)
        renewal_completed.set()
        await release_renewal.wait()
        return renewed

    monkeypatch.setattr(InMemoryWorkQueue, "renew", held_renew)
    message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task-123",
            task_name="tests.example",
            labels={},
            args=[],
            kwargs={},
        )
    )
    await broker.kick(message)
    listener = broker.listen()
    received = _require_ackable(await anext(listener))
    restored = broker.formatter.loads(message=received.data)
    receipt = restored.labels[DELIVERY_RECEIPT_LABEL]

    assert isinstance(receipt, str)
    renewal = asyncio.create_task(broker.renew_delivery(receipt, visibility_timeout=1))
    await renewal_completed.wait()
    await _acknowledge(received)
    release_renewal.set()

    with pytest.raises(CacheConflictError, match="stale or no longer reserved"):
        await renewal

    assert receipt not in broker._outstanding_deliveries
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_broker_settles_inflight_delivery_after_shutdown() -> None:
    broker = CacheTaskiqBroker(
        InMemoryWorkQueue(),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )
    message = broker.formatter.dumps(
        TaskiqMessage(
            task_id="task-123",
            task_name="tests.example",
            labels={},
            args=[],
            kwargs={},
        )
    )

    await broker.kick(message)
    listener = broker.listen()
    received = _require_ackable(await anext(listener))

    await broker.shutdown()
    await _acknowledge(received)

    await listener.aclose()


@pytest.mark.anyio
@pytest.mark.parametrize(("label", "expected_delay"), ((2.5, 2.5), ("2.5", 2.5)))
async def test_cache_taskiq_broker_publishes_taskiq_retry_delay(
    label: float | str,
    expected_delay: float,
) -> None:
    work_queue = RecordingWorkQueue()
    broker = CacheTaskiqBroker(
        cast(WorkQueueCacheCapability, work_queue),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )

    await broker.kick(
        BrokerMessage(
            task_id="task-123",
            task_name="tests.example",
            message=b"taskiq-message",
            labels={"delay": label},
        )
    )

    assert work_queue.publications == [
        ("taskiq-broker", "default", b"taskiq-message", expected_delay, 3)
    ]


@pytest.mark.anyio
async def test_cache_taskiq_broker_honours_actual_retry_delay() -> None:
    clock = FakeClock()
    work_queue = InMemoryWorkQueue(clock)
    broker = CacheTaskiqBroker(
        work_queue,
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )

    await broker.kick(
        BrokerMessage(
            task_id="task-123",
            task_name="tests.example",
            message=b"delayed",
            labels={"delay": 5},
        )
    )

    assert (
        await work_queue.reserve(
            "taskiq-broker",
            "default",
            "probe",
            visibility_timeout=30,
        )
        is None
    )
    clock.advance(5)
    listener = broker.listen()
    delivery = _require_ackable(await anext(listener))

    assert delivery.data == b"delayed"
    await _acknowledge(delivery)
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_broker_stops_listening_after_shutdown() -> None:
    broker = CacheTaskiqBroker(
        cast(WorkQueueCacheCapability, FailingReserveWorkQueue()),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )

    await broker.shutdown()

    with pytest.raises(StopAsyncIteration):
        await anext(broker.listen())


@pytest.mark.anyio
async def test_cache_taskiq_broker_failed_startup_leaves_listening_stopped() -> None:
    broker = CacheTaskiqBroker(
        cast(WorkQueueCacheCapability, FailingReserveWorkQueue()),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )

    async def fail_startup(_state: TaskiqState) -> None:
        raise RuntimeError("startup failed")

    broker.add_event_handler(TaskiqEvents.CLIENT_STARTUP, fail_startup)

    with pytest.raises(RuntimeError, match="startup failed"):
        await broker.startup()
    with pytest.raises(StopAsyncIteration):
        await anext(broker.listen())


@pytest.mark.anyio
async def test_cache_taskiq_broker_serialises_overlapping_startup_and_shutdown() -> (
    None
):
    startup_started = asyncio.Event()
    release_startup = asyncio.Event()
    broker = CacheTaskiqBroker(
        cast(WorkQueueCacheCapability, FailingReserveWorkQueue()),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )

    async def block_startup(_state: TaskiqState) -> None:
        startup_started.set()
        await release_startup.wait()

    broker.add_event_handler(TaskiqEvents.CLIENT_STARTUP, block_startup)
    startup = asyncio.create_task(broker.startup())
    await startup_started.wait()
    shutdown = asyncio.create_task(broker.shutdown())
    await asyncio.sleep(0)

    assert not shutdown.done()
    release_startup.set()
    await startup
    await shutdown

    with pytest.raises(StopAsyncIteration):
        await anext(broker.listen())


@pytest.mark.anyio
async def test_cache_taskiq_broker_cancels_pending_reservation_on_shutdown() -> None:
    work_queue = BlockingReserveWorkQueue()
    broker = CacheTaskiqBroker(
        cast(WorkQueueCacheCapability, work_queue),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )
    listener_task = asyncio.create_task(anext(broker.listen()))

    try:
        await work_queue.reserve_started.wait()
        await broker.shutdown()

        with pytest.raises(StopAsyncIteration):
            await listener_task
        assert work_queue.reserve_cancelled.is_set()
    finally:
        listener_task.cancel()
        with suppress(asyncio.CancelledError, StopAsyncIteration):
            await listener_task


@pytest.mark.anyio
async def test_cache_taskiq_broker_keeps_listening_after_an_empty_wait() -> None:
    work_queue = EmptyThenBlockingWorkQueue()
    broker = CacheTaskiqBroker(
        cast(WorkQueueCacheCapability, work_queue),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )
    listener_task = asyncio.create_task(anext(broker.listen()))

    try:
        await work_queue.empty_wait_completed.wait()
        await work_queue.second_reservation_started.wait()
        await broker.shutdown()

        with pytest.raises(StopAsyncIteration):
            await listener_task
        assert work_queue.reserve_cancelled.is_set()
    finally:
        listener_task.cancel()
        with suppress(asyncio.CancelledError, StopAsyncIteration):
            await listener_task


@pytest.mark.anyio
async def test_cache_taskiq_broker_does_not_yield_superseded_delivery() -> None:
    clock = FakeClock()
    queue = InMemoryWorkQueue(clock)
    work_queue = DeferredReservationWorkQueue(queue)
    broker = CacheTaskiqBroker(
        cast(WorkQueueCacheCapability, work_queue),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )
    await broker.kick(
        BrokerMessage(
            task_id="task-123",
            task_name="tests.example",
            message=b"taskiq-message",
            labels={},
        )
    )
    listener_task = asyncio.create_task(anext(broker.listen()))

    try:
        await work_queue.reservation_started.wait()
        await broker.startup()
        work_queue.release_reservation.set()

        with pytest.raises(StopAsyncIteration):
            await listener_task

        clock.advance(30)
        recovered = await queue.reserve(
            "taskiq-broker",
            "default",
            "worker-2",
            visibility_timeout=30,
        )

        assert recovered is not None
        assert recovered.payload == b"taskiq-message"
        await queue.acknowledge(recovered)
    finally:
        listener_task.cancel()
        with suppress(asyncio.CancelledError, StopAsyncIteration):
            await listener_task


@pytest.mark.anyio
async def test_cache_taskiq_broker_does_not_revive_old_listener_after_restart() -> None:
    work_queue = DelayedCancellationWorkQueue()
    broker = CacheTaskiqBroker(
        cast(WorkQueueCacheCapability, work_queue),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )
    listener_task = asyncio.create_task(anext(broker.listen()))
    second_reservation = asyncio.create_task(
        work_queue.second_reservation_started.wait()
    )

    try:
        await work_queue.first_reservation_started.wait()
        await broker.shutdown()
        await work_queue.cancellation_started.wait()
        await broker.startup()
        work_queue.release_cancellation.set()

        completed, _ = await asyncio.wait(
            (listener_task, second_reservation),
            return_when=asyncio.FIRST_COMPLETED,
        )

        assert listener_task in completed
        with pytest.raises(StopAsyncIteration):
            await listener_task
        assert not work_queue.second_reservation_started.is_set()
    finally:
        listener_task.cancel()
        second_reservation.cancel()
        with suppress(asyncio.CancelledError, StopAsyncIteration):
            await listener_task
        with suppress(asyncio.CancelledError):
            await second_reservation


@pytest.mark.anyio
async def test_cache_taskiq_broker_consumes_after_lifecycle_restart() -> None:
    broker = CacheTaskiqBroker(
        InMemoryWorkQueue(),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )

    await broker.shutdown()
    await broker.startup()
    await broker.kick(
        BrokerMessage(
            task_id="task-123",
            task_name="tests.example",
            message=b"after-restart",
            labels={},
        )
    )
    listener = broker.listen()
    delivery = _require_ackable(await anext(listener))

    assert delivery.data == b"after-restart"
    await _acknowledge(delivery)
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_broker_delivers_to_independent_consumers() -> None:
    work_queue = InMemoryWorkQueue()
    policy = TaskiqBrokerPolicy(
        visibility_timeout_seconds=30,
        wait_timeout_seconds=1,
        maximum_delivery_attempts=3,
    )
    first_broker = CacheTaskiqBroker(
        work_queue,
        queue="default",
        consumer="worker-1",
        policy=policy,
    )
    second_broker = CacheTaskiqBroker(
        work_queue,
        queue="default",
        consumer="worker-2",
        policy=policy,
    )

    await first_broker.kick(
        BrokerMessage(
            task_id="task-1",
            task_name="tests.example",
            message=b"first",
            labels={},
        )
    )
    await first_broker.kick(
        BrokerMessage(
            task_id="task-2",
            task_name="tests.example",
            message=b"second",
            labels={},
        )
    )
    first_listener = first_broker.listen()
    second_listener = second_broker.listen()
    first_received, second_received = await asyncio.gather(
        anext(first_listener),
        anext(second_listener),
    )
    first_delivery = _require_ackable(first_received)
    second_delivery = _require_ackable(second_received)

    assert {first_delivery.data, second_delivery.data} == {b"first", b"second"}
    await _acknowledge(first_delivery)
    await _acknowledge(second_delivery)
    await first_listener.aclose()
    await second_listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_broker_renews_expired_delivery_until_recovered(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    monkeypatch.setattr("wybra.tasks.taskiq_broker.monotonic", clock)
    work_queue = InMemoryWorkQueue(clock)
    broker = CacheTaskiqBroker(
        work_queue,
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )
    await broker.kick(
        broker.formatter.dumps(
            TaskiqMessage(
                task_id="task-conditional-ownership",
                task_name="tests.example",
                labels={},
                args=[],
                kwargs={},
            )
        )
    )
    listener = broker.listen()
    delivery = _require_ackable(await anext(listener))
    message = broker.formatter.loads(message=delivery.data)
    receipt = message.labels[DELIVERY_RECEIPT_LABEL]
    assert isinstance(receipt, str)

    clock.advance(30)
    await broker.renew_delivery(receipt, visibility_timeout=30)
    assert (
        await work_queue.reserve(
            "taskiq-broker",
            "default",
            "worker-2",
            visibility_timeout=30,
        )
        is None
    )
    await _acknowledge(delivery)
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_broker_redelivers_unacknowledged_work() -> None:
    clock = FakeClock()
    work_queue = InMemoryWorkQueue(clock)
    policy = TaskiqBrokerPolicy(
        visibility_timeout_seconds=30,
        wait_timeout_seconds=1,
        maximum_delivery_attempts=3,
    )
    first_broker = CacheTaskiqBroker(
        work_queue,
        queue="default",
        consumer="worker-1",
        policy=policy,
    )
    second_broker = CacheTaskiqBroker(
        work_queue,
        queue="default",
        consumer="worker-2",
        policy=policy,
    )

    await first_broker.kick(
        first_broker.formatter.dumps(
            TaskiqMessage(
                task_id="task-123",
                task_name="tests.example",
                labels={},
                args=[],
                kwargs={},
            )
        )
    )
    first_listener = first_broker.listen()
    first_delivery = _require_ackable(await anext(first_listener))
    clock.advance(30)

    second_listener = second_broker.listen()
    second_delivery = _require_ackable(await anext(second_listener))

    first_message = first_broker.formatter.loads(message=first_delivery.data)
    second_message = second_broker.formatter.loads(message=second_delivery.data)
    assert first_message.task_id == second_message.task_id == "task-123"
    assert [
        first_message.labels[DELIVERY_ATTEMPT_LABEL],
        second_message.labels[DELIVERY_ATTEMPT_LABEL],
    ] == [1, 2]
    with pytest.raises(CacheConflictError, match="stale or no longer reserved"):
        await _acknowledge(first_delivery)
    await _acknowledge(second_delivery)
    await first_listener.aclose()
    await second_listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_broker_dead_letters_after_delivery_attempts() -> None:
    clock = FakeClock()
    work_queue = InMemoryWorkQueue(clock)
    policy = TaskiqBrokerPolicy(
        visibility_timeout_seconds=30,
        wait_timeout_seconds=1,
        maximum_delivery_attempts=2,
    )
    first_broker = CacheTaskiqBroker(
        work_queue,
        queue="default",
        consumer="worker-1",
        policy=policy,
    )
    second_broker = CacheTaskiqBroker(
        work_queue,
        queue="default",
        consumer="worker-2",
        policy=policy,
    )

    await first_broker.kick(
        BrokerMessage(
            task_id="task-123",
            task_name="tests.example",
            message=b"taskiq-message",
            labels={},
        )
    )
    first_listener = first_broker.listen()
    first_delivery = _require_ackable(await anext(first_listener))
    clock.advance(30)
    second_listener = second_broker.listen()
    second_delivery = _require_ackable(await anext(second_listener))
    clock.advance(30)

    assert (
        await work_queue.reserve(
            "taskiq-broker",
            "default",
            "worker-3",
            visibility_timeout=30,
        )
        is None
    )
    dead_letters = await work_queue.dead_letters("taskiq-broker", "default")

    assert len(dead_letters) == 1
    assert dead_letters[0].payload == b"taskiq-message"
    assert dead_letters[0].attempt == 2
    with pytest.raises(CacheConflictError, match="stale or no longer reserved"):
        await _acknowledge(first_delivery)
    with pytest.raises(CacheConflictError, match="stale or no longer reserved"):
        await _acknowledge(second_delivery)
    await first_listener.aclose()
    await second_listener.aclose()


@pytest.mark.anyio
async def test_broker_observes_retained_dead_letter_once() -> None:
    clock = FakeClock()
    work_queue = InMemoryWorkQueue(clock)
    observations = 0

    async def invalid_lifecycle_metadata(_message: TaskiqMessage) -> None:
        nonlocal observations
        observations += 1
        raise TaskLifecycleError("dead-letter metadata is invalid")

    broker = CacheTaskiqBroker(
        work_queue,
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=1,
        ),
        on_delivery_exhausted=invalid_lifecycle_metadata,
    )
    await broker.kick(
        broker.formatter.dumps(
            TaskiqMessage(
                task_id="dead-letter",
                task_name="tests.example",
                labels={},
                args=[],
                kwargs={},
            )
        )
    )
    first_listener = broker.listen()
    assert isinstance(await anext(first_listener), AckableMessage)
    await first_listener.aclose()
    clock.advance(30)
    assert (
        await work_queue.reserve(
            "taskiq-broker",
            "default",
            "worker-2",
            visibility_timeout=30,
        )
        is None
    )
    assert await work_queue.dead_letters("taskiq-broker", "default")

    await broker._observe_delivery_exhaustion()
    await broker._observe_delivery_exhaustion()

    assert observations == 1

    await broker.kick(
        broker.formatter.dumps(
            TaskiqMessage(
                task_id="live-work",
                task_name="tests.example",
                labels={},
                args=[],
                kwargs={},
            )
        )
    )
    listener = broker.listen()
    delivered = await anext(listener)

    assert isinstance(delivered, AckableMessage)
    await listener.aclose()


@pytest.mark.anyio
async def test_broker_continues_after_transient_dead_letter_read_failure() -> None:
    work_queue = FailFirstDeadLetterReadWorkQueue()

    async def observe_dead_letter(_message: TaskiqMessage) -> None:
        return None

    broker = CacheTaskiqBroker(
        work_queue,
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=1,
        ),
        on_delivery_exhausted=observe_dead_letter,
    )
    await broker.kick(
        broker.formatter.dumps(
            TaskiqMessage(
                task_id="live-work",
                task_name="tests.example",
                labels={},
                args=[],
                kwargs={},
            )
        )
    )

    listener = broker.listen()
    delivered = await anext(listener)

    assert isinstance(delivered, AckableMessage)
    await listener.aclose()


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ("publish", "reserve", "acknowledge"))
async def test_cache_taskiq_broker_propagates_provider_failures(
    operation: str,
) -> None:
    broker = CacheTaskiqBroker(
        cast(WorkQueueCacheCapability, FailingWorkQueue(operation)),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )
    message = BrokerMessage(
        task_id="task-123",
        task_name="tests.example",
        message=b"taskiq-message",
        labels={},
    )

    match operation:
        case "publish":
            with pytest.raises(OSError, match="publish failed"):
                await broker.kick(message)
        case "reserve":
            with pytest.raises(OSError, match="reserve failed"):
                await anext(broker.listen())
        case "acknowledge":
            listener = broker.listen()
            delivery = _require_ackable(await anext(listener))
            with pytest.raises(OSError, match="acknowledge failed"):
                await _acknowledge(delivery)
            await listener.aclose()


@pytest.mark.anyio
@pytest.mark.parametrize("delay", (True, -1, float("inf"), "later"))
async def test_cache_taskiq_broker_rejects_invalid_retry_delay(
    delay: object,
) -> None:
    work_queue = RecordingWorkQueue()
    broker = CacheTaskiqBroker(
        cast(WorkQueueCacheCapability, work_queue),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=30,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )

    with pytest.raises(ValueError, match="retry delay") as caught:
        await broker.kick(
            BrokerMessage(
                task_id="task-123",
                task_name="tests.example",
                message=b"taskiq-message",
                labels={"delay": delay},
            )
        )

    assert caught.value.__context__ is None
    assert work_queue.publications == []
