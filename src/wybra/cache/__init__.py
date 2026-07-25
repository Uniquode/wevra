"""Application cache capability."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_EXPORT_MODULES = {
    "CacheBackend": "wybra.cache.registry",
    "CacheBackendFactory": "wybra.cache.registry",
    "CacheCapability": "wybra.cache.capabilities",
    "CacheConfigurationDiagnostic": "wybra.cache.settings",
    "CacheFactory": "wybra.cache.capabilities",
    "CacheInstance": "wybra.cache.registry",
    "CacheNotFoundError": "wybra.cache.registry",
    "CacheSettings": "wybra.cache.settings",
    "CachesCapability": "wybra.cache.registry",
    "CachesSettings": "wybra.cache.settings",
    "DefaultCachesCapability": "wybra.cache.registry",
    "InMemoryCache": "wybra.cache.capabilities",
    "RedisCache": "wybra.cache.capabilities",
    "build_caches": "wybra.cache.registry",
    "module_config": "wybra.cache.config",
    "setup_site": "wybra.cache.setup",
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
