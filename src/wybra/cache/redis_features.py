from __future__ import annotations

from dataclasses import dataclass, field

from wybra.cache.feature_contracts import AtomicCacheCapability, LeaseCacheCapability
from wybra.cache.feature_models import (
    CacheFeatureGuarantees,
    CacheFeatureMetadata,
    CacheFeatureRegistration,
)
from wybra.cache.redis_atomic import RedisAtomicCache
from wybra.cache.redis_leases import RedisLeaseCache
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
REDIS_CACHE_FEATURES = frozenset(
    feature.name for feature in (REDIS_ATOMIC_FEATURE, REDIS_LEASE_FEATURE)
)


@dataclass(frozen=True, slots=True)
class RedisCacheFeatures:
    runtime: RedisCacheRuntime
    atomic: RedisAtomicCache = field(init=False)
    leases: RedisLeaseCache = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "atomic", RedisAtomicCache(self.runtime))
        object.__setattr__(self, "leases", RedisLeaseCache(self.runtime))

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
        )


__all__ = (
    "REDIS_ATOMIC_FEATURE",
    "REDIS_CACHE_FEATURES",
    "REDIS_LEASE_FEATURE",
    "RedisCacheFeatures",
)
