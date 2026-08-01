from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from dataclasses import dataclass, field
from functools import partial
from math import isfinite

from taskiq import AckableMessage, AsyncBroker, BrokerMessage

from wybra.cache import WorkDelivery, WorkQueueCacheCapability

_TASKIQ_WORK_QUEUE_OWNER = "taskiq-broker"


@dataclass(slots=True)
class _BrokerLifecycle:
    shutdown_requested: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass(frozen=True, slots=True)
class TaskiqBrokerPolicy:
    """Delivery controls for the cache-backed Taskiq broker."""

    visibility_timeout_seconds: float
    wait_timeout_seconds: float
    maximum_delivery_attempts: int

    def __post_init__(self) -> None:
        _validate_positive_finite(
            self.visibility_timeout_seconds,
            label="Visibility timeout",
        )
        _validate_positive_finite(self.wait_timeout_seconds, label="Wait timeout")
        if (
            isinstance(self.maximum_delivery_attempts, bool)
            or not isinstance(self.maximum_delivery_attempts, int)
            or self.maximum_delivery_attempts < 1
        ):
            raise ValueError("Maximum delivery attempts must be a positive integer.")


class CacheTaskiqBroker(AsyncBroker):
    """Adapt Taskiq broker messages to a durable Wybra work queue."""

    def __init__(
        self,
        work_queue: WorkQueueCacheCapability,
        *,
        queue: str,
        consumer: str,
        policy: TaskiqBrokerPolicy,
    ) -> None:
        super().__init__()
        self._work_queue = work_queue
        self._queue = _require_non_blank(queue, label="Queue")
        self._consumer = _require_non_blank(consumer, label="Consumer")
        self._policy = policy
        self._lifecycle = _BrokerLifecycle()
        self._lifecycle_lock = asyncio.Lock()

    async def startup(self) -> None:
        async with self._lifecycle_lock:
            self._lifecycle.shutdown_requested.set()
            lifecycle = _BrokerLifecycle()
            try:
                await super().startup()
            except BaseException:
                lifecycle.shutdown_requested.set()
                raise
            self._lifecycle = lifecycle

    async def shutdown(self) -> None:
        async with self._lifecycle_lock:
            self._lifecycle.shutdown_requested.set()
            await super().shutdown()

    async def kick(self, message: BrokerMessage) -> None:
        await self._work_queue.publish(
            _TASKIQ_WORK_QUEUE_OWNER,
            self._queue,
            message.message,
            delay=_retry_delay(message),
            max_attempts=self._policy.maximum_delivery_attempts,
        )

    def listen(self) -> AsyncGenerator[bytes | AckableMessage]:
        return self._listen(self._lifecycle)

    async def _listen(
        self,
        lifecycle: _BrokerLifecycle,
    ) -> AsyncGenerator[AckableMessage]:
        shutdown = asyncio.create_task(lifecycle.shutdown_requested.wait())
        try:
            while (
                self._lifecycle is lifecycle
                and not lifecycle.shutdown_requested.is_set()
            ):
                delivery = await self._reserve_or_stop(shutdown)
                if (
                    delivery is not None
                    and self._lifecycle is lifecycle
                    and not lifecycle.shutdown_requested.is_set()
                ):
                    yield AckableMessage(
                        data=delivery.payload,
                        ack=partial(self._acknowledge, delivery),
                    )
        finally:
            await _cancel_task(shutdown)

    async def _reserve_or_stop(
        self,
        shutdown: asyncio.Task[bool],
    ) -> WorkDelivery | None:
        reservation = asyncio.create_task(
            self._work_queue.reserve(
                _TASKIQ_WORK_QUEUE_OWNER,
                self._queue,
                self._consumer,
                visibility_timeout=self._policy.visibility_timeout_seconds,
                wait_timeout=self._policy.wait_timeout_seconds,
            )
        )
        try:
            completed, _ = await asyncio.wait(
                (reservation, shutdown),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if shutdown in completed:
                await _cancel_task(reservation)
                return None
            return reservation.result()
        except BaseException:
            await _cancel_task(reservation)
            raise

    async def _acknowledge(self, delivery: WorkDelivery) -> None:
        await self._work_queue.acknowledge(delivery)


async def _cancel_task[T](task: asyncio.Task[T]) -> None:
    if not task.done():
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)


def _validate_positive_finite(value: object, *, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{label} must be a positive finite number.")


def _require_non_blank(value: str, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-blank string.")
    return value.strip()


def _retry_delay(message: BrokerMessage) -> float:
    value = message.labels.get("delay", 0)
    if isinstance(value, bool):
        raise ValueError("Taskiq retry delay must be a non-negative finite number.")
    try:
        delay = float(value)
    except TypeError, ValueError, OverflowError:
        delay = None
    if delay is None:
        raise ValueError("Taskiq retry delay must be a non-negative finite number.")
    if not isfinite(delay) or delay < 0:
        raise ValueError("Taskiq retry delay must be a non-negative finite number.")
    return delay


__all__ = ("CacheTaskiqBroker", "TaskiqBrokerPolicy")
