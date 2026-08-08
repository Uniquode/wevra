from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Protocol, runtime_checkable

from wybra.cache.feature_models import (
    AtomicCacheValue,
    CacheRevision,
    CounterCacheValue,
    LeaseToken,
    ScheduleClaim,
    ScheduleCursor,
    ScheduleRecord,
    StreamPosition,
    StreamRecord,
    WorkDelivery,
    WorkIdentity,
)


@runtime_checkable
class AtomicCacheCapability(Protocol):
    async def get(self, owner: str, key: str) -> AtomicCacheValue | None: ...

    async def create(
        self,
        owner: str,
        key: str,
        value: bytes,
        *,
        ttl: float,
        lease: LeaseToken | None = None,
    ) -> AtomicCacheValue | None: ...

    async def compare_and_swap(
        self,
        owner: str,
        key: str,
        expected: CacheRevision,
        value: bytes,
        *,
        ttl: float,
        lease: LeaseToken | None = None,
    ) -> AtomicCacheValue | None: ...

    async def compare_and_delete(
        self,
        owner: str,
        key: str,
        expected: CacheRevision,
        *,
        lease: LeaseToken | None = None,
    ) -> bool: ...

    async def increment(
        self,
        owner: str,
        key: str,
        *,
        amount: int = 1,
        ttl: float,
    ) -> CounterCacheValue: ...


@runtime_checkable
class LeaseCacheCapability(Protocol):
    async def acquire(
        self,
        owner: str,
        resource: str,
        holder: str,
        *,
        ttl: float,
    ) -> LeaseToken | None: ...

    async def renew(self, lease: LeaseToken, *, ttl: float) -> LeaseToken:
        """Extend the same lease identity with a new expiry."""
        ...

    async def release(self, lease: LeaseToken) -> None: ...


@runtime_checkable
class CacheTimeCapability(Protocol):
    """Provide the cache provider's current Unix timestamp."""

    async def refresh(self) -> float: ...

    def now(self) -> float: ...


@runtime_checkable
class WorkQueueCacheCapability(Protocol):
    async def publish(
        self,
        owner: str,
        queue: str,
        payload: bytes,
        *,
        delay: float = 0,
        max_attempts: int = 3,
    ) -> WorkIdentity: ...

    async def reserve(
        self,
        owner: str,
        queue: str,
        consumer: str,
        *,
        visibility_timeout: float,
        wait_timeout: float = 0,
    ) -> WorkDelivery | None: ...

    async def renew(
        self,
        delivery: WorkDelivery,
        *,
        visibility_timeout: float,
    ) -> WorkDelivery:
        """Extend a live delivery lease and return its new visibility deadline."""
        ...

    async def acknowledge(self, delivery: WorkDelivery) -> None: ...

    async def reject(self, delivery: WorkDelivery, *, delay: float = 0) -> None: ...

    async def dead_letter(self, delivery: WorkDelivery) -> None: ...

    async def dead_letters(
        self,
        owner: str,
        queue: str,
        *,
        limit: int = 100,
    ) -> tuple[WorkDelivery, ...]: ...


@runtime_checkable
class StreamCacheCapability(Protocol):
    async def append(
        self,
        owner: str,
        stream: str,
        payload: bytes,
        *,
        lease: LeaseToken | None = None,
    ) -> StreamPosition: ...

    async def read(
        self,
        owner: str,
        stream: str,
        *,
        after: StreamPosition | None = None,
        limit: int = 100,
    ) -> tuple[StreamRecord, ...]: ...

    async def read_consumer(
        self,
        owner: str,
        stream: str,
        consumer: str,
        *,
        limit: int = 100,
    ) -> tuple[StreamRecord, ...]: ...

    async def acknowledge(
        self,
        owner: str,
        stream: str,
        consumer: str,
        position: StreamPosition,
    ) -> None: ...

    async def forget_consumer(
        self,
        owner: str,
        stream: str,
        consumer: str,
    ) -> bool: ...


@runtime_checkable
class PubSubSubscription(Protocol):
    def __aiter__(self) -> AsyncIterator[bytes]: ...

    async def receive(self, *, timeout: float | None = None) -> bytes: ...

    async def close(self) -> None: ...


@runtime_checkable
class PubSubCacheCapability(Protocol):
    async def publish(self, owner: str, topic: str, payload: bytes) -> int: ...

    async def subscribe(self, owner: str, topic: str) -> PubSubSubscription: ...


@runtime_checkable
class ScheduleCacheCapability(Protocol):
    @property
    def maximum_records(self) -> int: ...

    async def create(
        self,
        owner: str,
        identity: str,
        payload: bytes,
        *,
        next_due_at: float,
        interval_seconds: float | None = None,
    ) -> ScheduleRecord | None: ...

    async def update(
        self,
        owner: str,
        identity: str,
        expected: CacheRevision,
        payload: bytes,
        *,
        next_due_at: float,
        interval_seconds: float | None = None,
    ) -> ScheduleRecord | None: ...

    async def delete(self, owner: str, identity: str) -> bool: ...

    async def due(
        self,
        owner: str,
        *,
        before: float,
        limit: int = 100,
        after: ScheduleCursor | None = None,
    ) -> tuple[ScheduleRecord, ...]: ...

    async def claim(
        self,
        owner: str,
        identity: str,
        claimant: str,
        *,
        ttl: float,
    ) -> ScheduleClaim | None: ...

    async def complete(self, claim: ScheduleClaim) -> ScheduleRecord | None: ...

    async def discard(self, claim: ScheduleClaim) -> None: ...

    async def held(self, claim: ScheduleClaim) -> bool: ...

    async def advance(
        self,
        claim: ScheduleClaim,
        payload: bytes,
        *,
        next_due_at: float,
    ) -> ScheduleRecord: ...

    async def release(self, claim: ScheduleClaim) -> None: ...


__all__ = (
    "AtomicCacheCapability",
    "CacheTimeCapability",
    "LeaseCacheCapability",
    "PubSubCacheCapability",
    "PubSubSubscription",
    "ScheduleCacheCapability",
    "StreamCacheCapability",
    "WorkQueueCacheCapability",
)
