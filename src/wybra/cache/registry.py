from __future__ import annotations

from asyncio import CancelledError
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from functools import partial
from types import MappingProxyType
from typing import Protocol, TypeVar, cast, runtime_checkable

from wybra.cache.capabilities import (
    DEFAULT_CACHE_FILL_TIMEOUT_SECONDS,
    CacheCapability,
    InMemoryCache,
    RedisCache,
)
from wybra.cache.config import to_cache_name
from wybra.cache.feature_models import (
    CacheFeatureMetadata,
    CacheFeatureRegistration,
    CacheFeatureUnavailableError,
)
from wybra.cache.memory_features import InMemoryCacheFeatures
from wybra.cache.redis_features import RedisCacheFeatures
from wybra.cache.redis_runtime import RedisCacheRuntime
from wybra.cache.settings import (
    CacheConfigurationDiagnostic,
    CacheSettings,
    CachesSettings,
)
from wybra.core.exceptions import ConfigurationError

type CacheBackendCloser = Callable[[], Awaitable[None]]
FeatureT = TypeVar("FeatureT")


class CacheNotFoundError(LookupError):
    """Raised when a configured named cache cannot be resolved."""


@dataclass(frozen=True, slots=True)
class CacheInstance:
    name: str
    values: CacheCapability
    backend: str
    partition: str
    features: tuple[str, ...] = ()
    feature_metadata: tuple[CacheFeatureMetadata, ...] = ()
    _feature_implementations: Mapping[type[object], object] = field(
        default_factory=dict,
        repr=False,
        compare=False,
    )

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "_feature_implementations",
            MappingProxyType(dict(self._feature_implementations)),
        )
        metadata_names = tuple(metadata.name for metadata in self.feature_metadata)
        if self.features != metadata_names:
            raise ValueError(
                "Cache instance feature names must match its feature metadata."
            )

    def optional(self, feature: type[FeatureT]) -> FeatureT | None:
        implementation = self._feature_implementations.get(feature)
        return cast(FeatureT | None, implementation)

    def require(
        self,
        feature: type[FeatureT],
        *,
        consumer: str | None = None,
    ) -> FeatureT:
        implementation = self.optional(feature)
        if implementation is not None:
            return implementation
        feature_name = getattr(feature, "__name__", repr(feature))
        message = (
            f"Cache {self.name!r} using backend {self.backend!r} does not provide "
            f"required feature {feature_name!r}"
        )
        if consumer is not None:
            message += f" for consumer {consumer!r}"
        raise CacheFeatureUnavailableError(message + ".")


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
    features: tuple[CacheFeatureRegistration, ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "features", tuple(self.features))


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
                health="ready",
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
    redis_urls: Mapping[str, str] | None = None,
) -> DefaultCachesCapability:
    backend_factories = _DEFAULT_FACTORIES if factories is None else factories
    instances: dict[str, CacheInstance] = {}
    closers: list[CacheBackendCloser] = []
    lifecycle_owners: list[object] = []
    try:
        for instance_settings in settings.instances:
            if (
                instance_settings.backend == "redis"
                and instance_settings.requires_secret_resolution
                and (redis_urls is None or instance_settings.name not in redis_urls)
            ):
                raise ConfigurationError(
                    f"Cache {instance_settings.name!r} Redis connection material "
                    "must be resolved before backend construction."
                )
            if factories is None and instance_settings.backend == "redis":
                factory = partial(
                    _redis_backend,
                    url=(
                        redis_urls.get(instance_settings.name)
                        if redis_urls is not None
                        else None
                    ),
                )
            else:
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
            feature_implementations, feature_metadata = _registered_features(
                instance_settings,
                backend.features,
            )
            instances[instance_settings.name] = CacheInstance(
                name=instance_settings.name,
                values=values,
                backend=instance_settings.backend,
                partition=instance_settings.partition,
                features=tuple(metadata.name for metadata in feature_metadata),
                feature_metadata=feature_metadata,
                _feature_implementations=feature_implementations,
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


def _registered_features(
    settings: CacheSettings,
    registrations: tuple[CacheFeatureRegistration, ...],
) -> tuple[
    Mapping[type[object], object],
    tuple[CacheFeatureMetadata, ...],
]:
    implementations: dict[type[object], object] = {}
    metadata_by_name: dict[str, CacheFeatureMetadata] = {}
    for registration in registrations:
        if (
            settings.features is not None
            and registration.metadata.name not in settings.features
        ):
            continue
        if registration.protocol in implementations:
            raise ConfigurationError(
                f"Cache {settings.name!r} backend {settings.backend!r} registered "
                f"feature protocol {registration.protocol.__name__!r} more than once."
            )
        if registration.metadata.name in metadata_by_name:
            raise ConfigurationError(
                f"Cache {settings.name!r} backend {settings.backend!r} registered "
                f"feature name {registration.metadata.name!r} more than once."
            )
        try:
            implementation_matches = isinstance(
                registration.implementation,
                registration.protocol,
            )
        except TypeError as exc:
            raise ConfigurationError(
                f"Cache feature protocol {registration.protocol.__name__!r} must "
                "support runtime structural validation."
            ) from exc
        if not implementation_matches:
            raise ConfigurationError(
                f"Cache {settings.name!r} backend {settings.backend!r} advertised "
                f"feature {registration.metadata.name!r} with an implementation "
                f"that does not satisfy {registration.protocol.__name__!r}."
            )
        implementations[registration.protocol] = registration.implementation
        metadata_by_name[registration.metadata.name] = registration.metadata
    if settings.features is not None:
        unavailable = sorted(set(settings.features) - metadata_by_name.keys())
        if unavailable:
            raise ConfigurationError(
                f"Cache {settings.name!r} backend {settings.backend!r} did not "
                "register configured feature(s): " + ", ".join(unavailable) + "."
            )
    ordered_metadata = tuple(
        metadata_by_name[name] for name in sorted(metadata_by_name)
    )
    return MappingProxyType(implementations), ordered_metadata


async def _memory_backend(settings: CacheSettings) -> CacheBackend:
    del settings
    features = InMemoryCacheFeatures()
    return CacheBackend(
        InMemoryCache(),
        close=features.close,
        lifecycle_owner=features,
        features=features.registrations(),
    )


async def _redis_backend(
    settings: CacheSettings,
    *,
    url: str | None = None,
) -> CacheBackend:
    if url is None and settings.requires_secret_resolution:
        raise ConfigurationError(
            f"{settings.name!r} cache Redis connection material must be resolved "
            "before backend construction."
        )
    resolved_url = url or settings.url
    if resolved_url is None:
        raise ConfigurationError(
            f"{settings.name!r} cache URL is required for the Redis backend."
        )
    runtime = RedisCacheRuntime(resolved_url, settings.resolved_namespace)
    features = RedisCacheFeatures(runtime)
    selected_features = frozenset(
        registration.metadata.name
        for registration in features.registrations()
        if settings.features is None or registration.metadata.name in settings.features
    )
    try:
        await runtime.health_check()
        await runtime.validate_features(selected_features)
    except BaseException as startup_error:
        try:
            await runtime.close()
        except BaseException as cleanup_error:
            raise BaseExceptionGroup(
                "Redis cache startup and cleanup failed.",
                [startup_error, cleanup_error],
            ) from startup_error
        raise
    cache = RedisCache.from_runtime(runtime)

    async def close() -> None:
        try:
            await features.close()
        finally:
            await runtime.close()

    return CacheBackend(
        cache,
        close,
        lifecycle_owner=features,
        features=features.registrations(),
    )


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
    "CacheFeatureUnavailableError",
    "CacheInstance",
    "CacheNotFoundError",
    "CachesCapability",
    "DefaultCachesCapability",
    "build_caches",
)
