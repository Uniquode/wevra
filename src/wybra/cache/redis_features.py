from __future__ import annotations

from dataclasses import dataclass, field

from wybra.cache.feature_contracts import (
    AtomicCacheCapability,
    LeaseCacheCapability,
    WorkQueueCacheCapability,
)
from wybra.cache.feature_models import (
    CacheFeatureGuarantees,
    CacheFeatureMetadata,
    CacheFeatureRegistration,
)
from wybra.cache.redis_atomic import RedisAtomicCache
from wybra.cache.redis_leases import RedisLeaseCache
from wybra.cache.redis_queues import RedisWorkQueue
from wybra.cache.redis_runtime import RedisCacheRuntime

REDIS_ATOMIC_FEATURE = CacheFeatureMetadata(
    "atomic",
    CacheFeatureGuarantees(
        scope="shared",
        durable=True,
        restart_recovery=True,
        horizontal_consumers=True,
        ordering_scope="key",
    ),
)
REDIS_LEASE_FEATURE = CacheFeatureMetadata(
    "lease",
    CacheFeatureGuarantees(
        scope="shared",
        durable=True,
        restart_recovery=True,
        horizontal_consumers=True,
        ordering_scope="resource",
    ),
)
REDIS_WORK_QUEUE_FEATURE = CacheFeatureMetadata(
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
REDIS_CACHE_FEATURES = frozenset(
    feature.name
    for feature in (REDIS_ATOMIC_FEATURE, REDIS_LEASE_FEATURE, REDIS_WORK_QUEUE_FEATURE)
)


@dataclass(frozen=True, slots=True)
class RedisCacheFeatures:
    runtime: RedisCacheRuntime
    atomic: RedisAtomicCache = field(init=False)
    leases: RedisLeaseCache = field(init=False)
    work_queue: RedisWorkQueue = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "atomic", RedisAtomicCache(self.runtime))
        object.__setattr__(self, "leases", RedisLeaseCache(self.runtime))
        object.__setattr__(self, "work_queue", RedisWorkQueue(self.runtime))

    def registrations(self) -> tuple[CacheFeatureRegistration, ...]:
        return (
            CacheFeatureRegistration(
                AtomicCacheCapability,
                self.atomic,
                REDIS_ATOMIC_FEATURE,
            ),
            CacheFeatureRegistration(
                LeaseCacheCapability,
                self.leases,
                REDIS_LEASE_FEATURE,
            ),
            CacheFeatureRegistration(
                WorkQueueCacheCapability,
                self.work_queue,
                REDIS_WORK_QUEUE_FEATURE,
            ),
        )

    async def close(self) -> None:
        await self.work_queue.close()


__all__ = (
    "REDIS_ATOMIC_FEATURE",
    "REDIS_CACHE_FEATURES",
    "REDIS_LEASE_FEATURE",
    "REDIS_WORK_QUEUE_FEATURE",
    "RedisCacheFeatures",
)
