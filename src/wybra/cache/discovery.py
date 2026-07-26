from __future__ import annotations

from collections.abc import Iterable
from importlib import import_module


def cache_provider_configured(modules: Iterable[str]) -> bool:
    configured_modules = tuple(modules)
    if "wybra.cache" in configured_modules:
        return True
    return any(
        _module_provides_cache_capability(module_name)
        for module_name in configured_modules
    )


def _module_provides_cache_capability(module_name: str) -> bool:
    try:
        module = import_module(module_name)
    except ImportError:
        return False
    return getattr(module, "provides_cache_capability", False) is True


__all__ = ("cache_provider_configured",)
