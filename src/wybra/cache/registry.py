from __future__ import annotations

from asyncio import CancelledError
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Protocol, runtime_checkable

from wybra.cache.capabilities import (
    DEFAULT_CACHE_FILL_TIMEOUT_SECONDS,
    CacheCapability,
    InMemoryCache,
    RedisCache,
)
from wybra.cache.config import to_cache_name
from wybra.cache.settings import (
    CacheConfigurationDiagnostic,
    CacheSettings,
    CachesSettings,
)
from wybra.core.exceptions import ConfigurationError

type CacheBackendCloser = Callable[[], Awaitable[None]]


class CacheNotFoundError(LookupError):
    """Raised when a configured named cache cannot be resolved."""


@dataclass(frozen=True, slots=True)
class CacheInstance:
    name: str
    values: CacheCapability
    backend: str
    partition: str
    features: tuple[str, ...] = ()


@runtime_checkable
class CachesCapability(Protocol):
    def require(
        self,
        name: str,
        *,
        consumer: str | None = None,
    ) -> CacheInstance: ...

    def optional(self, name: str) -> CacheInstance | None: ...

    def diagnostics(self) -> tuple[CacheConfigurationDiagnostic, ...]: ...

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class CacheBackend:
    values: CacheCapability
    close: CacheBackendCloser | None = field(default=None, repr=False)
    lifecycle_owner: object | None = field(default=None, repr=False)


class CacheBackendFactory(Protocol):
    async def __call__(self, settings: CacheSettings) -> CacheBackend: ...


@dataclass(frozen=True, slots=True)
class _CacheValues:
    backend: CacheCapability = field(repr=False)

    async def get(self, owner: str, key: str) -> bytes | None:
        return await self.backend.get(owner, key)

    async def set(self, owner: str, key: str, value: bytes, *, ttl: float) -> None:
        await self.backend.set(owner, key, value, ttl=ttl)

    async def delete(self, owner: str, key: str) -> None:
        await self.backend.delete(owner, key)

    async def get_or_set(
        self,
        owner: str,
        key: str,
        *,
        ttl: float,
        factory: Callable[[], Awaitable[bytes]],
        timeout: float = DEFAULT_CACHE_FILL_TIMEOUT_SECONDS,
    ) -> bytes:
        return await self.backend.get_or_set(
            owner,
            key,
            ttl=ttl,
            factory=factory,
            timeout=timeout,
        )


@dataclass(slots=True)
class DefaultCachesCapability:
    _instances: Mapping[str, CacheInstance]
    _closers: tuple[CacheBackendCloser, ...] = field(repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def require(
        self,
        name: str,
        *,
        consumer: str | None = None,
    ) -> CacheInstance:
        try:
            requested = to_cache_name(name)
        except ValueError as exc:
            message = f"Invalid cache name {name!r}"
            if consumer is not None:
                message += f" for consumer {consumer!r}"
            raise CacheNotFoundError(f"{message}: {exc}") from exc
        instance = self._instances.get(requested)
        if instance is not None:
            return instance
        message = f"Configured cache {requested!r} was not found"
        if consumer is not None:
            message += f" for consumer {consumer!r}"
        raise CacheNotFoundError(message + ".")

    def optional(self, name: str) -> CacheInstance | None:
        try:
            requested = to_cache_name(name)
        except ValueError:
            return None
        return self._instances.get(requested)

    def diagnostics(self) -> tuple[CacheConfigurationDiagnostic, ...]:
        return tuple(
            CacheConfigurationDiagnostic(
                name=instance.name,
                backend=instance.backend,
                partition=instance.partition,
                features=instance.features,
            )
            for instance in self._instances.values()
        )

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        errors = await _close_all(self._closers)
        if errors:
            raise BaseExceptionGroup("Named cache shutdown failed.", errors)


async def build_caches(
    settings: CachesSettings,
    *,
    factories: Mapping[str, CacheBackendFactory] | None = None,
) -> DefaultCachesCapability:
    backend_factories = _DEFAULT_FACTORIES if factories is None else factories
    instances: dict[str, CacheInstance] = {}
    closers: list[CacheBackendCloser] = []
    lifecycle_owners: list[object] = []
    try:
        for instance_settings in settings.instances:
            try:
                factory = backend_factories[instance_settings.backend]
            except KeyError as exc:
                raise ConfigurationError(
                    f"No cache backend factory is registered for "
                    f"{instance_settings.backend!r}."
                ) from exc
            try:
                backend = await factory(instance_settings)
            except (ConfigurationError, ValueError) as exc:
                raise ConfigurationError(
                    f"Cache {instance_settings.name!r} backend "
                    f"{instance_settings.backend!r} configuration failed: {exc}"
                ) from exc
            _register_backend_closer(backend, lifecycle_owners, closers)
            if not isinstance(backend.values, CacheCapability):
                raise ConfigurationError(
                    f"Cache {instance_settings.name!r} backend "
                    f"{instance_settings.backend!r} does not provide the mandatory "
                    "cache baseline."
                )
            values = _CacheValues(backend.values)
            instances[instance_settings.name] = CacheInstance(
                name=instance_settings.name,
                values=values,
                backend=instance_settings.backend,
                partition=instance_settings.partition,
            )
    except BaseException as startup_error:
        cleanup_errors = await _close_all(tuple(closers))
        if cleanup_errors:
            raise BaseExceptionGroup(
                "Named cache startup and cleanup failed.",
                [startup_error, *cleanup_errors],
            ) from startup_error
        raise
    return DefaultCachesCapability(MappingProxyType(instances), tuple(closers))


async def _memory_backend(settings: CacheSettings) -> CacheBackend:
    del settings
    return CacheBackend(InMemoryCache())


async def _redis_backend(settings: CacheSettings) -> CacheBackend:
    assert settings.url is not None
    cache = RedisCache(settings.url)
    return CacheBackend(cache, cache.close, lifecycle_owner=cache)


def _register_backend_closer(
    backend: CacheBackend,
    lifecycle_owners: list[object],
    closers: list[CacheBackendCloser],
) -> None:
    if backend.close is None:
        return
    lifecycle_owner = (
        backend.lifecycle_owner
        if backend.lifecycle_owner is not None
        else backend.close
    )
    if any(lifecycle_owner is existing_owner for existing_owner in lifecycle_owners):
        return
    lifecycle_owners.append(lifecycle_owner)
    closers.append(backend.close)


async def _close_all(
    closers: tuple[CacheBackendCloser, ...],
) -> list[BaseException]:
    errors: list[BaseException] = []
    cancellation: CancelledError | None = None
    for close in reversed(closers):
        try:
            await close()
        except CancelledError as exc:
            if cancellation is None:
                cancellation = exc
        except BaseException as exc:
            errors.append(exc)
    if cancellation is not None:
        if errors:
            cancellation.add_note(
                f"{len(errors)} cache backend error(s) also occurred during shutdown."
            )
        raise cancellation
    return errors


_DEFAULT_FACTORIES: Mapping[str, CacheBackendFactory] = {
    "memory": _memory_backend,
    "redis": _redis_backend,
}


__all__ = (
    "CacheBackend",
    "CacheBackendFactory",
    "CacheInstance",
    "CacheNotFoundError",
    "CachesCapability",
    "DefaultCachesCapability",
    "build_caches",
)
