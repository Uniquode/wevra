from __future__ import annotations

import asyncio
from collections import deque
from collections.abc import AsyncIterator, Awaitable, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from wybra.cache.feature_models import (
    DEFAULT_STREAM_MAX_CONSUMERS,
    DEFAULT_STREAM_RETENTION_COUNT,
    CacheConflictError,
    CacheFeatureError,
    CachePositionExpiredError,
    LeaseToken,
    StreamPosition,
    StreamRecord,
    validate_limit,
    validate_payload,
    validate_positive_finite,
    validate_positive_integer,
    validate_resource,
)

if TYPE_CHECKING:
    from wybra.cache.memory_leases import InMemoryLeaseCache

type StreamKey = tuple[str, str]
type SubscriptionCloser = Callable[[], Awaitable[None]]
_SUBSCRIPTION_CLOSED = object()


@dataclass(slots=True)
class InMemoryStreamCache:
    max_records: int = DEFAULT_STREAM_RETENTION_COUNT
    max_streams: int = 1_000
    max_consumers: int = DEFAULT_STREAM_MAX_CONSUMERS
    leases: InMemoryLeaseCache | None = field(default=None, repr=False)
    _records: dict[StreamKey, deque[StreamRecord]] = field(
        default_factory=dict,
        init=False,
    )
    _next_positions: dict[StreamKey, int] = field(default_factory=dict, init=False)
    _consumer_positions: dict[tuple[StreamKey, str], StreamPosition] = field(
        default_factory=dict,
        init=False,
    )
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        validate_positive_integer(self.max_records, label="stream retention")
        validate_positive_integer(self.max_streams, label="maximum streams")
        validate_positive_integer(
            self.max_consumers,
            label="maximum stream consumers",
        )

    async def append(
        self,
        owner: str,
        stream: str,
        payload: bytes,
        *,
        lease: LeaseToken | None = None,
    ) -> StreamPosition:
        stream_key = _stream_key(owner, stream)
        payload = validate_payload(payload)
        async with self._fence(lease):
            if (
                stream_key not in self._records
                and len(self._records) >= self.max_streams
            ):
                raise CacheFeatureError(
                    "The in-memory stream cache has reached its configured "
                    "stream capacity."
                )
            position = StreamPosition(self._next_positions.get(stream_key, 0) + 1)
            self._next_positions[stream_key] = position.value
            records = self._records.setdefault(stream_key, deque())
            records.append(StreamRecord(stream, position, payload))
            while len(records) > self.max_records:
                records.popleft()
            return position

    async def read(
        self,
        owner: str,
        stream: str,
        *,
        after: StreamPosition | None = None,
        limit: int = 100,
    ) -> tuple[StreamRecord, ...]:
        stream_key = _stream_key(owner, stream)
        limit = validate_limit(limit)
        async with self._lock:
            return self._read(stream_key, after=after, limit=limit)

    async def read_consumer(
        self,
        owner: str,
        stream: str,
        consumer: str,
        *,
        limit: int = 100,
    ) -> tuple[StreamRecord, ...]:
        stream_key = _stream_key(owner, stream)
        consumer = validate_resource(consumer, label="stream consumer")
        limit = validate_limit(limit)
        async with self._lock:
            after = self._consumer_positions.get((stream_key, consumer))
            return self._read(stream_key, after=after, limit=limit)

    async def acknowledge(
        self,
        owner: str,
        stream: str,
        consumer: str,
        position: StreamPosition,
    ) -> None:
        stream_key = _stream_key(owner, stream)
        consumer = validate_resource(consumer, label="stream consumer")
        async with self._lock:
            latest = self._next_positions.get(stream_key, 0)
            if position.value > latest:
                raise CacheConflictError(
                    f"Stream position {position.value} does not exist in {stream!r}."
                )
            consumer_key = (stream_key, consumer)
            current = self._consumer_positions.get(consumer_key)
            if current is not None and position < current:
                raise CacheConflictError(
                    f"Stream consumer {consumer!r} cannot move backwards."
                )
            if (
                current is None
                and self._consumer_count(stream_key) >= self.max_consumers
            ):
                raise CacheFeatureError(
                    "The in-memory stream cache has reached its configured "
                    "consumer capacity."
                )
            self._consumer_positions[consumer_key] = position

    async def forget_consumer(
        self,
        owner: str,
        stream: str,
        consumer: str,
    ) -> bool:
        stream_key = _stream_key(owner, stream)
        consumer = validate_resource(consumer, label="stream consumer")
        async with self._lock:
            return (
                self._consumer_positions.pop((stream_key, consumer), None) is not None
            )

    def _consumer_count(self, stream_key: StreamKey) -> int:
        return sum(
            1
            for consumer_stream_key, _consumer in self._consumer_positions
            if consumer_stream_key == stream_key
        )

    def bind_lease_fence(self, leases: InMemoryLeaseCache) -> None:
        if self.leases is not None and self.leases is not leases:
            raise CacheFeatureError(
                "Stream cache lease fencing is already bound to another lease cache."
            )
        self.leases = leases

    def _read(
        self,
        stream_key: StreamKey,
        *,
        after: StreamPosition | None,
        limit: int,
    ) -> tuple[StreamRecord, ...]:
        records = self._records.get(stream_key)
        if not records:
            return ()
        earliest = records[0].position.value
        if after is not None and after.value < earliest - 1:
            raise CachePositionExpiredError(
                f"Stream position {after.value} is no longer retained."
            )
        after_value = 0 if after is None else after.value
        return tuple(
            record for record in records if record.position.value > after_value
        )[:limit]

    @asynccontextmanager
    async def _fence(self, lease: LeaseToken | None):
        if lease is None:
            async with self._lock:
                yield
            return
        if self.leases is None:
            raise CacheFeatureError(
                "Stream cache does not support lease-fenced appends."
            )
        async with self.leases.hold_fence(lease) as validate:
            async with self._lock:
                validate(lease)
                yield


@dataclass(slots=True, eq=False)
class InMemoryPubSubSubscription:
    _close_callback: SubscriptionCloser = field(repr=False)
    _messages: asyncio.Queue[bytes | object] = field(repr=False)
    _closed: bool = field(default=False, init=False)

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self

    async def __anext__(self) -> bytes:
        try:
            return await self.receive()
        except CacheFeatureError as exc:
            raise StopAsyncIteration from exc

    async def receive(self, *, timeout: float | None = None) -> bytes:
        if self._closed:
            raise CacheFeatureError("The pub/sub subscription is closed.")
        if timeout is None:
            message = await self._messages.get()
        else:
            timeout = validate_positive_finite(
                timeout,
                label="subscription timeout",
            )
            message = await asyncio.wait_for(self._messages.get(), timeout=timeout)
        if message is _SUBSCRIPTION_CLOSED:
            raise CacheFeatureError("The pub/sub subscription is closed.")
        assert isinstance(message, bytes)
        return message

    async def close(self) -> None:
        if self._closed:
            return
        await self._close_callback()
        self._mark_closed()

    def _mark_closed(self) -> None:
        if self._closed:
            return
        self._closed = True
        if self._messages.full():
            self._messages.get_nowait()
        self._messages.put_nowait(_SUBSCRIPTION_CLOSED)


@dataclass(slots=True)
class InMemoryPubSubCache:
    subscriber_buffer: int = 1_000
    max_topics: int = 1_000
    max_subscriptions: int = 10_000
    _subscriptions: dict[StreamKey, set[InMemoryPubSubSubscription]] = field(
        default_factory=dict,
        init=False,
    )
    _closed: bool = field(default=False, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        validate_positive_integer(
            self.subscriber_buffer,
            label="subscriber buffer",
        )
        validate_positive_integer(self.max_topics, label="maximum pub/sub topics")
        validate_positive_integer(
            self.max_subscriptions,
            label="maximum pub/sub subscriptions",
        )

    async def publish(self, owner: str, topic: str, payload: bytes) -> int:
        topic_key = _stream_key(owner, topic)
        payload = validate_payload(payload)
        async with self._lock:
            self._require_open()
            subscriptions = tuple(self._subscriptions.get(topic_key, ()))
            delivered = 0
            for subscription in subscriptions:
                try:
                    subscription._messages.put_nowait(payload)
                except asyncio.QueueFull:
                    subscription._mark_closed()
                    self._remove_subscription(topic_key, subscription)
                else:
                    delivered += 1
            return delivered

    async def subscribe(
        self,
        owner: str,
        topic: str,
    ) -> InMemoryPubSubSubscription:
        topic_key = _stream_key(owner, topic)
        async with self._lock:
            self._require_open()
            if (
                topic_key not in self._subscriptions
                and len(self._subscriptions) >= self.max_topics
            ):
                raise CacheFeatureError(
                    "The in-memory pub/sub cache has reached its configured "
                    "topic capacity."
                )
            if self._subscription_count() >= self.max_subscriptions:
                raise CacheFeatureError(
                    "The in-memory pub/sub cache has reached its configured "
                    "subscription capacity."
                )
            subscription: InMemoryPubSubSubscription

            async def remove() -> None:
                async with self._lock:
                    self._remove_subscription(topic_key, subscription)

            subscription = InMemoryPubSubSubscription(
                remove,
                asyncio.Queue(maxsize=self.subscriber_buffer),
            )
            self._subscriptions.setdefault(topic_key, set()).add(subscription)
            return subscription

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            for subscriptions in self._subscriptions.values():
                for subscription in subscriptions:
                    subscription._mark_closed()
            self._subscriptions.clear()

    def _require_open(self) -> None:
        if self._closed:
            raise CacheFeatureError("The in-memory pub/sub service is closed.")

    def _remove_subscription(
        self,
        topic_key: StreamKey,
        subscription: InMemoryPubSubSubscription,
    ) -> None:
        subscriptions = self._subscriptions.get(topic_key)
        if subscriptions is None:
            return
        subscriptions.discard(subscription)
        if not subscriptions:
            self._subscriptions.pop(topic_key, None)

    def _subscription_count(self) -> int:
        return sum(len(subscriptions) for subscriptions in self._subscriptions.values())


def _stream_key(owner: str, stream: str) -> StreamKey:
    return (
        validate_resource(owner, label="cache owner"),
        validate_resource(stream, label="stream or topic"),
    )


__all__ = (
    "InMemoryPubSubCache",
    "InMemoryPubSubSubscription",
    "InMemoryStreamCache",
)
