from __future__ import annotations

from dataclasses import dataclass, field

from wybra.cache.feature_contracts import (
    AtomicCacheCapability,
    CacheTimeCapability,
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
from wybra.cache.lifecycle import close_all, raise_cleanup_errors
from wybra.cache.nats_coordination import NatsCoordination
from wybra.cache.nats_pubsub import NatsPubSubCache
from wybra.cache.nats_queues import NatsWorkQueue
from wybra.cache.nats_runtime import NatsJetStreamRuntime
from wybra.cache.nats_schedules import NatsScheduleCache
from wybra.cache.nats_streams import NatsStreamCache
from wybra.cache.nats_time import NatsCacheTime

NATS_ATOMIC_FEATURE = CacheFeatureMetadata(
    "atomic",
    CacheFeatureGuarantees(
        scope="shared",
        durable=True,
        restart_recovery=True,
        horizontal_consumers=True,
        ordering_scope="provider",
    ),
)
NATS_LEASE_FEATURE = CacheFeatureMetadata(
    "lease",
    CacheFeatureGuarantees(
        scope="shared",
        durable=True,
        restart_recovery=True,
        horizontal_consumers=True,
        ordering_scope="provider",
    ),
)
NATS_TIME_FEATURE = CacheFeatureMetadata(
    "time",
    CacheFeatureGuarantees(
        scope="shared",
        durable=False,
        restart_recovery=False,
        horizontal_consumers=True,
        ordering_scope="provider-clock",
    ),
)
NATS_PUBSUB_FEATURE = CacheFeatureMetadata(
    "pub-sub",
    CacheFeatureGuarantees(
        scope="shared",
        durable=False,
        restart_recovery=False,
        horizontal_consumers=True,
        ordering_scope="topic",
    ),
)
NATS_STREAM_FEATURE = CacheFeatureMetadata(
    "stream",
    CacheFeatureGuarantees(
        scope="shared",
        durable=True,
        restart_recovery=True,
        horizontal_consumers=True,
        ordering_scope="stream",
        replay=True,
        retention=True,
        acknowledgement=True,
    ),
)
NATS_WORK_QUEUE_FEATURE = CacheFeatureMetadata(
    "work-queue",
    CacheFeatureGuarantees(
        scope="shared",
        durable=True,
        restart_recovery=True,
        horizontal_consumers=True,
        ordering_scope="queue",
        acknowledgement=True,
        redelivery=True,
    ),
)
NATS_SCHEDULE_FEATURE = CacheFeatureMetadata(
    "schedule",
    CacheFeatureGuarantees(
        scope="shared",
        durable=True,
        restart_recovery=True,
        horizontal_consumers=True,
        ordering_scope="due-time",
        scheduling=True,
    ),
)
NATS_CACHE_FEATURES = frozenset(
    {
        "atomic",
        "lease",
        "pub-sub",
        "schedule",
        "stream",
        "time",
        "work-queue",
    }
)


@dataclass(slots=True)
class NatsJetStreamCacheFeatures:
    _runtime: NatsJetStreamRuntime = field(repr=False)
    coordination: NatsCoordination = field(init=False)
    time: NatsCacheTime = field(init=False)
    pubsub: NatsPubSubCache = field(init=False)
    streams: NatsStreamCache = field(init=False)
    work_queue: NatsWorkQueue = field(init=False)
    schedules: NatsScheduleCache = field(init=False)

    def __post_init__(self) -> None:
        self.coordination = NatsCoordination(self._runtime)
        self.time = NatsCacheTime(self._runtime)
        self.pubsub = NatsPubSubCache(self._runtime)
        self.streams = NatsStreamCache(self._runtime, self.coordination)
        self.work_queue = NatsWorkQueue(self._runtime)
        self.schedules = NatsScheduleCache(self._runtime, self.coordination)

    async def start(self) -> None:
        await self.coordination.start()

    async def close(self) -> None:
        errors = await close_all(
            (self.pubsub.close, self.work_queue.close, self.coordination.close)
        )
        raise_cleanup_errors("NATS cache feature cleanup failed.", errors)

    def registrations(self) -> tuple[CacheFeatureRegistration, ...]:
        return (
            CacheFeatureRegistration(
                AtomicCacheCapability,
                self.coordination,
                NATS_ATOMIC_FEATURE,
            ),
            CacheFeatureRegistration(
                LeaseCacheCapability,
                self.coordination,
                NATS_LEASE_FEATURE,
            ),
            CacheFeatureRegistration(
                CacheTimeCapability,
                self.time,
                NATS_TIME_FEATURE,
            ),
            CacheFeatureRegistration(
                PubSubCacheCapability,
                self.pubsub,
                NATS_PUBSUB_FEATURE,
            ),
            CacheFeatureRegistration(
                WorkQueueCacheCapability,
                self.work_queue,
                NATS_WORK_QUEUE_FEATURE,
            ),
            CacheFeatureRegistration(
                ScheduleCacheCapability,
                self.schedules,
                NATS_SCHEDULE_FEATURE,
            ),
            CacheFeatureRegistration(
                StreamCacheCapability,
                self.streams,
                NATS_STREAM_FEATURE,
            ),
        )


__all__ = (
    "NATS_ATOMIC_FEATURE",
    "NATS_CACHE_FEATURES",
    "NATS_LEASE_FEATURE",
    "NATS_PUBSUB_FEATURE",
    "NATS_SCHEDULE_FEATURE",
    "NATS_STREAM_FEATURE",
    "NATS_TIME_FEATURE",
    "NATS_WORK_QUEUE_FEATURE",
    "NatsJetStreamCacheFeatures",
)
