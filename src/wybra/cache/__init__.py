"""Application cache capability."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORT_MODULES = {
    "AtomicCacheCapability": "wybra.cache.feature_contracts",
    "AtomicCacheValue": "wybra.cache.feature_models",
    "CacheBackend": "wybra.cache.registry",
    "CacheBackendFactory": "wybra.cache.registry",
    "CacheCapability": "wybra.cache.capabilities",
    "CacheConfigurationDiagnostic": "wybra.cache.settings",
    "CacheFactory": "wybra.cache.capabilities",
    "CacheFeatureError": "wybra.cache.feature_models",
    "CacheFeatureGuarantees": "wybra.cache.feature_models",
    "CacheFeatureMetadata": "wybra.cache.feature_models",
    "CacheFeatureRegistration": "wybra.cache.feature_models",
    "CacheFeatureUnavailableError": "wybra.cache.feature_models",
    "CacheInstance": "wybra.cache.registry",
    "CacheNotFoundError": "wybra.cache.registry",
    "CacheSettings": "wybra.cache.settings",
    "CachesCapability": "wybra.cache.registry",
    "CachesSettings": "wybra.cache.settings",
    "CacheConflictError": "wybra.cache.feature_models",
    "CachePositionExpiredError": "wybra.cache.feature_models",
    "CacheRevision": "wybra.cache.feature_models",
    "CounterCacheValue": "wybra.cache.feature_models",
    "DEFAULT_CACHE_NAME": "wybra.cache.config",
    "DefaultCachesCapability": "wybra.cache.registry",
    "FencingToken": "wybra.cache.feature_models",
    "InMemoryCache": "wybra.cache.capabilities",
    "InMemoryCacheFeatures": "wybra.cache.memory_features",
    "InMemoryAtomicCache": "wybra.cache.memory_atomic",
    "InMemoryLeaseCache": "wybra.cache.memory_leases",
    "InMemoryPubSubCache": "wybra.cache.memory_streams",
    "InMemoryPubSubSubscription": "wybra.cache.memory_streams",
    "InMemoryScheduleCache": "wybra.cache.memory_schedules",
    "InMemoryStreamCache": "wybra.cache.memory_streams",
    "InMemoryWorkQueue": "wybra.cache.memory_queues",
    "LeaseCacheCapability": "wybra.cache.feature_contracts",
    "LeaseToken": "wybra.cache.feature_models",
    "MAX_CACHE_FEATURE_PAYLOAD_BYTES": "wybra.cache.feature_models",
    "MAX_STREAM_POSITION": "wybra.cache.feature_models",
    "PubSubCacheCapability": "wybra.cache.feature_contracts",
    "PubSubSubscription": "wybra.cache.feature_contracts",
    "RedisCache": "wybra.cache.capabilities",
    "ScheduleCacheCapability": "wybra.cache.feature_contracts",
    "ScheduleClaim": "wybra.cache.feature_models",
    "ScheduleRecord": "wybra.cache.feature_models",
    "StreamCacheCapability": "wybra.cache.feature_contracts",
    "StreamPosition": "wybra.cache.feature_models",
    "StreamRecord": "wybra.cache.feature_models",
    "WorkDelivery": "wybra.cache.feature_models",
    "WorkIdentity": "wybra.cache.feature_models",
    "WorkQueueCacheCapability": "wybra.cache.feature_contracts",
    "build_caches": "wybra.cache.registry",
    "cache_provider_configured": "wybra.cache.discovery",
    "module_config": "wybra.cache.config",
    "setup_site": "wybra.cache.setup",
    "to_cache_name": "wybra.cache.config",
}

provides_cache_capability = True


def __getattr__(name: str) -> Any:
    module_name = _EXPORT_MODULES.get(name)
    if module_name is None:
        raise AttributeError(f"module 'wybra.cache' has no attribute {name!r}")
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value


__all__ = [*sorted(_EXPORT_MODULES), "provides_cache_capability"]
