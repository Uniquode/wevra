from __future__ import annotations

import asyncio
import heapq
import time
from collections import deque
from collections.abc import Callable
from dataclasses import dataclass, field
from uuid import uuid4

from wybra.cache.feature_models import (
    CacheConflictError,
    CacheFeatureError,
    CacheWorkQueueRejectedError,
    WorkDelivery,
    WorkIdentity,
    validate_limit,
    validate_non_negative_finite,
    validate_payload,
    validate_positive_finite,
    validate_positive_integer,
    validate_resource,
)

type Clock = Callable[[], float]
type QueueKey = tuple[str, str]


@dataclass(slots=True)
class _WorkItem:
    identity: WorkIdentity
    payload: bytes
    max_attempts: int
    attempt: int = 0


@dataclass(slots=True)
class _Reservation:
    queue_key: QueueKey
    item: _WorkItem
    visible_until: float
    receipt: str

    def delivery(self) -> WorkDelivery:
        return WorkDelivery(
            queue=self.queue_key[1],
            identity=self.item.identity,
            payload=self.item.payload,
            attempt=self.item.attempt,
            visible_until=self.visible_until,
            receipt=self.receipt,
        )


@dataclass(slots=True)
class InMemoryWorkQueue:
    clock: Clock = field(default=time.time, repr=False)
    max_items_per_queue: int = 10_000
    max_dead_letters_per_queue: int = 1_000
    max_queues: int = 1_000
    _ready: dict[QueueKey, deque[_WorkItem]] = field(default_factory=dict, init=False)
    _delayed: list[tuple[float, int, QueueKey, _WorkItem]] = field(
        default_factory=list,
        init=False,
    )
    _inflight: dict[str, _Reservation] = field(default_factory=dict, init=False)
    _dead: dict[QueueKey, deque[_WorkItem]] = field(default_factory=dict, init=False)
    _sequence: int = field(default=0, init=False)
    _closed: bool = field(default=False, init=False)
    _condition: asyncio.Condition = field(
        default_factory=asyncio.Condition,
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        validate_positive_integer(
            self.max_items_per_queue,
            label="maximum work items per queue",
        )
        validate_positive_integer(
            self.max_dead_letters_per_queue,
            label="maximum dead letters per queue",
        )
        validate_positive_integer(self.max_queues, label="maximum work queues")

    async def publish(
        self,
        owner: str,
        queue: str,
        payload: bytes,
        *,
        delay: float = 0,
        max_attempts: int = 3,
    ) -> WorkIdentity:
        queue_key = _queue_key(owner, queue)
        payload = validate_payload(payload)
        delay = validate_non_negative_finite(delay, label="work delay")
        max_attempts = validate_positive_integer(
            max_attempts,
            label="maximum delivery attempts",
        )
        identity = WorkIdentity(uuid4().hex)
        item = _WorkItem(identity, payload, max_attempts)
        async with self._condition:
            self._require_open()
            self._promote_delayed()
            self._ensure_queue_capacity(queue_key)
            if self._active_count(queue_key) >= self.max_items_per_queue:
                raise CacheWorkQueueRejectedError(
                    f"Queue {queue!r} has reached its configured item capacity."
                )
            self._enqueue(queue_key, item, delay=delay)
            self._condition.notify_all()
        return identity

    async def reserve(
        self,
        owner: str,
        queue: str,
        consumer: str,
        *,
        visibility_timeout: float,
        wait_timeout: float = 0,
    ) -> WorkDelivery | None:
        queue_key = _queue_key(owner, queue)
        validate_resource(consumer, label="queue consumer")
        visibility_timeout = validate_positive_finite(
            visibility_timeout,
            label="visibility timeout",
        )
        wait_timeout = validate_non_negative_finite(
            wait_timeout,
            label="queue wait timeout",
        )
        loop = asyncio.get_running_loop()
        wait_deadline = loop.time() + wait_timeout
        async with self._condition:
            while True:
                self._require_open()
                self._recover()
                ready = self._ready.get(queue_key)
                if ready:
                    item = ready.popleft()
                    item.attempt += 1
                    receipt = uuid4().hex
                    reservation = _Reservation(
                        queue_key=queue_key,
                        item=item,
                        visible_until=self.clock() + visibility_timeout,
                        receipt=receipt,
                    )
                    self._inflight[receipt] = reservation
                    self._prune_queue(queue_key)
                    return reservation.delivery()
                remaining = wait_deadline - loop.time()
                if remaining <= 0:
                    return None
                next_wakeup = self._next_wakeup(queue_key)
                wait_for = remaining
                if next_wakeup is not None:
                    wait_for = min(
                        wait_for,
                        max(0, next_wakeup - self.clock()),
                    )
                    if wait_for == 0:
                        continue
                try:
                    await asyncio.wait_for(
                        self._condition.wait(),
                        timeout=wait_for,
                    )
                except TimeoutError:
                    if wait_for >= remaining:
                        return None

    async def acknowledge(self, delivery: WorkDelivery) -> None:
        async with self._condition:
            self._promote_delayed()
            reservation = self._required_reservation(delivery)
            self._inflight.pop(reservation.receipt, None)
            self._prune_queue(reservation.queue_key)

    async def renew(
        self,
        delivery: WorkDelivery,
        *,
        visibility_timeout: float,
    ) -> WorkDelivery:
        visibility_timeout = validate_positive_finite(
            visibility_timeout,
            label="visibility timeout",
        )
        async with self._condition:
            self._promote_delayed()
            reservation = self._required_reservation(delivery)
            reservation.visible_until = self.clock() + visibility_timeout
            return reservation.delivery()

    async def reject(self, delivery: WorkDelivery, *, delay: float = 0) -> None:
        delay = validate_non_negative_finite(delay, label="retry delay")
        async with self._condition:
            self._promote_delayed()
            reservation = self._required_reservation(delivery)
            self._inflight.pop(reservation.receipt, None)
            self._retry_or_dead_letter(
                reservation.queue_key,
                reservation.item,
                delay=delay,
            )
            self._condition.notify_all()

    async def dead_letter(self, delivery: WorkDelivery) -> None:
        async with self._condition:
            self._promote_delayed()
            reservation = self._required_reservation(delivery)
            self._inflight.pop(reservation.receipt, None)
            self._append_dead_letter(reservation.queue_key, reservation.item)
            self._condition.notify_all()

    async def dead_letters(
        self,
        owner: str,
        queue: str,
        *,
        limit: int = 100,
    ) -> tuple[WorkDelivery, ...]:
        queue_key = _queue_key(owner, queue)
        limit = validate_limit(limit)
        async with self._condition:
            self._promote_delayed()
            dead_letters = self._dead.get(queue_key, ())
            return tuple(
                WorkDelivery(
                    queue=queue,
                    identity=item.identity,
                    payload=item.payload,
                    attempt=item.attempt,
                    visible_until=0,
                    receipt=f"dead-{item.identity.value}",
                )
                for item in tuple(dead_letters)[:limit]
            )

    async def close(self) -> None:
        async with self._condition:
            self._closed = True
            self._condition.notify_all()

    def _enqueue(self, queue_key: QueueKey, item: _WorkItem, *, delay: float) -> None:
        if delay == 0:
            self._ready.setdefault(queue_key, deque()).append(item)
            return
        self._sequence += 1
        heapq.heappush(
            self._delayed,
            (self.clock() + delay, self._sequence, queue_key, item),
        )

    def _recover(self) -> None:
        self._promote_delayed()
        self._recover_expired()

    def _promote_delayed(self) -> None:
        now = self.clock()
        while self._delayed and self._delayed[0][0] <= now:
            _available_at, _sequence, queue_key, item = heapq.heappop(self._delayed)
            self._ready.setdefault(queue_key, deque()).append(item)

    def _recover_expired(self) -> None:
        now = self.clock()
        expired = [
            receipt
            for receipt, reservation in self._inflight.items()
            if reservation.visible_until <= now
        ]
        for receipt in expired:
            reservation = self._inflight.pop(receipt)
            self._retry_or_dead_letter(
                reservation.queue_key,
                reservation.item,
                delay=0,
            )
            self._prune_queue(reservation.queue_key)

    def _retry_or_dead_letter(
        self,
        queue_key: QueueKey,
        item: _WorkItem,
        *,
        delay: float,
    ) -> None:
        if item.attempt >= item.max_attempts:
            self._append_dead_letter(queue_key, item)
            return
        self._enqueue(queue_key, item, delay=delay)

    def _append_dead_letter(self, queue_key: QueueKey, item: _WorkItem) -> None:
        dead_letters = self._dead.setdefault(queue_key, deque())
        dead_letters.append(item)
        while len(dead_letters) > self.max_dead_letters_per_queue:
            dead_letters.popleft()

    def _required_reservation(self, delivery: WorkDelivery) -> _Reservation:
        reservation = self._inflight.get(delivery.receipt)
        if (
            reservation is None
            or reservation.item.identity != delivery.identity
            or reservation.queue_key[1] != delivery.queue
            or reservation.item.attempt != delivery.attempt
        ):
            raise CacheConflictError(
                f"Delivery {delivery.identity.value!r} is stale or no longer reserved."
            )
        return reservation

    def _next_wakeup(self, queue_key: QueueKey) -> float | None:
        wakeups = [
            available_at
            for available_at, _sequence, delayed_key, _item in self._delayed
            if delayed_key == queue_key
        ]
        wakeups.extend(
            reservation.visible_until
            for reservation in self._inflight.values()
            if reservation.queue_key == queue_key
        )
        return min(wakeups, default=None)

    def _active_count(self, queue_key: QueueKey) -> int:
        return (
            len(self._ready.get(queue_key, ()))
            + sum(
                delayed_key == queue_key
                for _available_at, _sequence, delayed_key, _item in self._delayed
            )
            + sum(
                reservation.queue_key == queue_key
                for reservation in self._inflight.values()
            )
        )

    def _ensure_queue_capacity(self, queue_key: QueueKey) -> None:
        known_queues = self._known_queues()
        if queue_key not in known_queues and len(known_queues) >= self.max_queues:
            raise CacheWorkQueueRejectedError(
                "The in-memory work queue has reached its configured queue capacity."
            )

    def _known_queues(self) -> set[QueueKey]:
        return (
            {key for key, items in self._ready.items() if items}
            | {key for key, items in self._dead.items() if items}
            | {queue_key for _available, _sequence, queue_key, _item in self._delayed}
            | {reservation.queue_key for reservation in self._inflight.values()}
        )

    def _prune_queue(self, queue_key: QueueKey) -> None:
        ready = self._ready.get(queue_key)
        if ready is not None and not ready:
            self._ready.pop(queue_key, None)
        dead = self._dead.get(queue_key)
        if dead is not None and not dead:
            self._dead.pop(queue_key, None)

    def _require_open(self) -> None:
        if self._closed:
            raise CacheFeatureError("The in-memory work queue is closed.")


def _queue_key(owner: str, queue: str) -> QueueKey:
    return (
        validate_resource(owner, label="cache owner"),
        validate_resource(queue, label="queue"),
    )


__all__ = ("InMemoryWorkQueue",)
