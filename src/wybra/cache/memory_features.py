from __future__ import annotations

import asyncio
from dataclasses import dataclass, field

from wybra.cache.feature_contracts import (
    AtomicCacheCapability,
    LeaseCacheCapability,
    PubSubCacheCapability,
    ScheduleCacheCapability,
    StreamCacheCapability,
    WorkQueueCacheCapability,
)
from wybra.cache.feature_models import (
    CacheFeatureGuarantees,
    CacheFeatureMetadata,
    CacheFeatureRegistration,
)
from wybra.cache.memory_atomic import InMemoryAtomicCache
from wybra.cache.memory_leases import InMemoryLeaseCache
from wybra.cache.memory_queues import InMemoryWorkQueue
from wybra.cache.memory_schedules import InMemoryScheduleCache
from wybra.cache.memory_streams import InMemoryPubSubCache, InMemoryStreamCache


def _process_local(
    *,
    ordering_scope: str | None = None,
    replay: bool = False,
    retention: bool = False,
    acknowledgement: bool = False,
    redelivery: bool = False,
    scheduling: bool = False,
) -> CacheFeatureGuarantees:
    return CacheFeatureGuarantees(
        scope="process",
        durable=False,
        restart_recovery=False,
        horizontal_consumers=False,
        ordering_scope=ordering_scope,
        replay=replay,
        retention=retention,
        acknowledgement=acknowledgement,
        redelivery=redelivery,
        scheduling=scheduling,
    )


MEMORY_ATOMIC_FEATURE = CacheFeatureMetadata(
    "atomic",
    _process_local(ordering_scope="key"),
)
MEMORY_LEASE_FEATURE = CacheFeatureMetadata(
    "lease",
    _process_local(ordering_scope="resource"),
)
MEMORY_WORK_QUEUE_FEATURE = CacheFeatureMetadata(
    "work-queue",
    _process_local(
        ordering_scope="queue",
        acknowledgement=True,
        redelivery=True,
    ),
)
MEMORY_STREAM_FEATURE = CacheFeatureMetadata(
    "stream",
    _process_local(
        ordering_scope="stream",
        replay=True,
        retention=True,
        acknowledgement=True,
    ),
)
MEMORY_PUBSUB_FEATURE = CacheFeatureMetadata(
    "pub-sub",
    _process_local(ordering_scope="topic"),
)
MEMORY_SCHEDULE_FEATURE = CacheFeatureMetadata(
    "schedule",
    _process_local(
        ordering_scope="due-time",
        scheduling=True,
    ),
)
MEMORY_CACHE_FEATURES = frozenset(
    feature.name
    for feature in (
        MEMORY_ATOMIC_FEATURE,
        MEMORY_LEASE_FEATURE,
        MEMORY_WORK_QUEUE_FEATURE,
        MEMORY_STREAM_FEATURE,
        MEMORY_PUBSUB_FEATURE,
        MEMORY_SCHEDULE_FEATURE,
    )
)


@dataclass(slots=True)
class InMemoryCacheFeatures:
    atomic: InMemoryAtomicCache = field(default_factory=InMemoryAtomicCache)
    leases: InMemoryLeaseCache = field(default_factory=InMemoryLeaseCache)
    work_queue: InMemoryWorkQueue = field(default_factory=InMemoryWorkQueue)
    streams: InMemoryStreamCache = field(default_factory=InMemoryStreamCache)
    pubsub: InMemoryPubSubCache = field(default_factory=InMemoryPubSubCache)
    schedules: InMemoryScheduleCache = field(default_factory=InMemoryScheduleCache)
    _closed: bool = field(default=False, init=False, repr=False)
    _work_queue_closed: bool = field(default=False, init=False, repr=False)
    _pubsub_closed: bool = field(default=False, init=False, repr=False)

    def registrations(self) -> tuple[CacheFeatureRegistration, ...]:
        return (
            CacheFeatureRegistration(
                AtomicCacheCapability,
                self.atomic,
                MEMORY_ATOMIC_FEATURE,
            ),
            CacheFeatureRegistration(
                LeaseCacheCapability,
                self.leases,
                MEMORY_LEASE_FEATURE,
            ),
            CacheFeatureRegistration(
                WorkQueueCacheCapability,
                self.work_queue,
                MEMORY_WORK_QUEUE_FEATURE,
            ),
            CacheFeatureRegistration(
                StreamCacheCapability,
                self.streams,
                MEMORY_STREAM_FEATURE,
            ),
            CacheFeatureRegistration(
                PubSubCacheCapability,
                self.pubsub,
                MEMORY_PUBSUB_FEATURE,
            ),
            CacheFeatureRegistration(
                ScheduleCacheCapability,
                self.schedules,
                MEMORY_SCHEDULE_FEATURE,
            ),
        )

    async def close(self) -> None:
        if self._closed:
            return
        cancellation: asyncio.CancelledError | None = None
        errors: list[Exception] = []
        if not self._work_queue_closed:
            try:
                await self.work_queue.close()
            except asyncio.CancelledError as error:
                cancellation = error
            except Exception as error:
                errors.append(error)
            else:
                self._work_queue_closed = True
        if not self._pubsub_closed:
            try:
                await self.pubsub.close()
            except asyncio.CancelledError as error:
                cancellation = cancellation or error
            except Exception as error:
                errors.append(error)
            else:
                self._pubsub_closed = True
        self._closed = self._work_queue_closed and self._pubsub_closed
        if cancellation is not None:
            if errors:
                cancellation.add_note(
                    "Additional in-memory cache feature cleanup errors: "
                    + ", ".join(repr(error) for error in errors)
                )
            raise cancellation
        if len(errors) == 1:
            raise errors[0]
        if errors:
            raise ExceptionGroup(
                "In-memory cache feature cleanup failed.",
                errors,
            )


__all__ = (
    "InMemoryCacheFeatures",
    "MEMORY_CACHE_FEATURES",
    "MEMORY_ATOMIC_FEATURE",
    "MEMORY_LEASE_FEATURE",
    "MEMORY_PUBSUB_FEATURE",
    "MEMORY_SCHEDULE_FEATURE",
    "MEMORY_STREAM_FEATURE",
    "MEMORY_WORK_QUEUE_FEATURE",
)
