from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol, runtime_checkable

from wybra.cache.redis_runtime import RedisCacheRuntime
from wybra.core.exceptions import ConfigurationError
from wybra.events import observe
from wybra.events.cache import cache_event

type CacheFactory = Callable[[], Awaitable[bytes]]
DEFAULT_CACHE_FILL_TIMEOUT_SECONDS = 30.0


@runtime_checkable
class CacheCapability(Protocol):
    async def get(self, owner: str, key: str) -> bytes | None: ...

    async def set(self, owner: str, key: str, value: bytes, *, ttl: float) -> None: ...

    async def delete(self, owner: str, key: str) -> None: ...

    async def get_or_set(
        self,
        owner: str,
        key: str,
        *,
        ttl: float,
        factory: CacheFactory,
        timeout: float = DEFAULT_CACHE_FILL_TIMEOUT_SECONDS,
    ) -> bytes: ...


@dataclass(slots=True)
class _SingleFlightCache:
    """Coordinate one in-process cache fill for each backend key."""

    _fills: dict[str, asyncio.Event] = field(default_factory=dict, init=False)
    _fills_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    async def get(self, owner: str, key: str) -> bytes | None:
        raise NotImplementedError

    async def set(self, owner: str, key: str, value: bytes, *, ttl: float) -> None:
        raise NotImplementedError

    async def _get_value(self, owner: str, key: str) -> tuple[bytes | None, str]:
        """Read a value and outcome without recording an observation."""
        raise NotImplementedError

    async def _set_value(
        self, owner: str, key: str, value: bytes, *, ttl: float
    ) -> None:
        """Store a value without recording an observation."""
        raise NotImplementedError

    async def _get_or_set(
        self,
        owner: str,
        key: str,
        *,
        ttl: float,
        factory: CacheFactory,
        timeout: float,
    ) -> bytes:
        cache_key = _cache_key(owner, key)
        timeout = _fill_timeout(timeout)
        while True:
            read_started = time.perf_counter()
            try:
                value, read_outcome = await self._get_value(owner, key)
            except Exception as exc:
                await self._record_failed(
                    "read", owner, key, started=read_started, error=exc
                )
                raise
            if value is not None:
                await self._record_completed(
                    "read", owner, key, outcome=read_outcome, started=read_started
                )
                return value
            await self._record_completed(
                "read", owner, key, outcome=read_outcome, started=read_started
            )

            async with self._fills_lock:
                completed = self._fills.get(cache_key)
                if completed is None:
                    completed = asyncio.Event()
                    self._fills[cache_key] = completed
                    is_filler = True
                else:
                    is_filler = False

            if not is_filler:
                await asyncio.wait_for(completed.wait(), timeout=timeout)
                continue

            fill_started = time.perf_counter()
            try:
                try:
                    value = await asyncio.wait_for(factory(), timeout=timeout)
                    await self._set_value(owner, key, value, ttl=ttl)
                finally:
                    await self._release_fill(cache_key, completed)
            except Exception as exc:
                await self._record_failed(
                    "fill",
                    owner,
                    key,
                    started=fill_started,
                    error=exc,
                )
                raise
            else:
                await self._record_completed(
                    "set", owner, key, outcome="stored", started=fill_started
                )
                await self._record_completed(
                    "fill",
                    owner,
                    key,
                    outcome="filled",
                    started=fill_started,
                )
                return value

    async def _release_fill(self, cache_key: str, completed: asyncio.Event) -> None:
        """Wake waiters before observational event delivery can delay them."""

        async with self._fills_lock:
            if self._fills.get(cache_key) is completed:
                self._fills.pop(cache_key, None)
            completed.set()

    @observe(cache_event)
    async def _record_completed(
        self,
        operation: str,
        owner: str,
        key: str,
        *,
        outcome: str,
        started: float,
    ) -> None:
        del operation, owner, key, outcome, started

    @observe(cache_event)
    async def _record_failed(
        self,
        operation: str,
        owner: str,
        key: str,
        *,
        started: float,
        error: Exception,
    ) -> None:
        del operation, owner, key, started, error


@dataclass(slots=True)
class InMemoryCache(_SingleFlightCache):
    _entries: dict[str, tuple[float, bytes]] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def get(self, owner: str, key: str) -> bytes | None:
        started = time.perf_counter()
        try:
            value, outcome = await self._get_value(owner, key)
        except Exception as exc:
            await self._record_failed("read", owner, key, started=started, error=exc)
            raise
        await self._record_completed(
            "read", owner, key, outcome=outcome, started=started
        )
        return value

    async def set(self, owner: str, key: str, value: bytes, *, ttl: float) -> None:
        started = time.perf_counter()
        try:
            await self._set_value(owner, key, value, ttl=ttl)
        except Exception as exc:
            await self._record_failed("set", owner, key, started=started, error=exc)
            raise
        await self._record_completed(
            "set", owner, key, outcome="stored", started=started
        )

    async def _get_value(self, owner: str, key: str) -> tuple[bytes | None, str]:
        cache_key = _cache_key(owner, key)
        async with self._lock:
            entry = self._entries.get(cache_key)
            if entry is None:
                return None, "miss"
            expires_at, value = entry
            if expires_at <= time.monotonic():
                self._entries.pop(cache_key, None)
                return None, "expired"
            return value, "hit"

    async def _set_value(
        self, owner: str, key: str, value: bytes, *, ttl: float
    ) -> None:
        if not isinstance(value, bytes):
            raise TypeError("Cache values must be bytes.")
        cache_key = _cache_key(owner, key)
        expires_at = time.monotonic() + _ttl(ttl)
        async with self._lock:
            self._entries[cache_key] = (expires_at, value)

    async def delete(self, owner: str, key: str) -> None:
        started = time.perf_counter()
        try:
            async with self._lock:
                self._entries.pop(_cache_key(owner, key), None)
        except Exception as exc:
            await self._record_failed("delete", owner, key, started=started, error=exc)
            raise
        await self._record_completed(
            "delete", owner, key, outcome="deleted", started=started
        )

    async def get_or_set(
        self,
        owner: str,
        key: str,
        *,
        ttl: float,
        factory: CacheFactory,
        timeout: float = DEFAULT_CACHE_FILL_TIMEOUT_SECONDS,
    ) -> bytes:
        return await self._get_or_set(
            owner,
            key,
            ttl=ttl,
            factory=factory,
            timeout=timeout,
        )


@dataclass(slots=True)
class RedisCache(_SingleFlightCache):
    url: str = field(repr=False)
    namespace: str | None = None
    _runtime_owner: RedisCacheRuntime | None = field(
        default=None,
        repr=False,
    )

    def __post_init__(self) -> None:
        if self._runtime_owner is None:
            self._runtime_owner = RedisCacheRuntime(self.url, self.namespace)

    @classmethod
    def from_runtime(cls, runtime: RedisCacheRuntime) -> RedisCache:
        return cls(runtime.url, runtime.namespace, runtime)

    async def get(self, owner: str, key: str) -> bytes | None:
        started = time.perf_counter()
        try:
            value, _outcome = await self._get_value(owner, key)
        except Exception as exc:
            await self._record_failed("read", owner, key, started=started, error=exc)
            raise
        await self._record_completed(
            "read",
            owner,
            key,
            outcome="hit" if value is not None else "miss",
            started=started,
        )
        return value

    async def set(self, owner: str, key: str, value: bytes, *, ttl: float) -> None:
        started = time.perf_counter()
        try:
            await self._set_value(owner, key, value, ttl=ttl)
        except Exception as exc:
            await self._record_failed("set", owner, key, started=started, error=exc)
            raise
        await self._record_completed(
            "set", owner, key, outcome="stored", started=started
        )

    async def _get_value(self, owner: str, key: str) -> tuple[bytes | None, str]:
        runtime = self._runtime()
        cache_key = runtime.baseline_key(owner, key)

        async def get_value(client: Any) -> object:
            return await client.get(cache_key)

        value = await runtime.feature_call(get_value)
        result = value if isinstance(value, bytes) else None
        return result, "hit" if result is not None else "miss"

    async def _set_value(
        self, owner: str, key: str, value: bytes, *, ttl: float
    ) -> None:
        if not isinstance(value, bytes):
            raise TypeError("Cache values must be bytes.")
        runtime = self._runtime()
        cache_key = runtime.baseline_key(owner, key)
        ttl_milliseconds = runtime.ttl_milliseconds(ttl, label="cache TTL")

        async def set_value(client: Any) -> object:
            return await client.set(cache_key, value, px=ttl_milliseconds)

        await runtime.feature_call(set_value)

    async def delete(self, owner: str, key: str) -> None:
        started = time.perf_counter()
        try:
            runtime = self._runtime()
            cache_key = runtime.baseline_key(owner, key)

            async def delete_value(client: Any) -> object:
                return await client.delete(cache_key)

            await runtime.feature_call(delete_value)
        except Exception as exc:
            await self._record_failed("delete", owner, key, started=started, error=exc)
            raise
        await self._record_completed(
            "delete", owner, key, outcome="deleted", started=started
        )

    async def get_or_set(
        self,
        owner: str,
        key: str,
        *,
        ttl: float,
        factory: CacheFactory,
        timeout: float = DEFAULT_CACHE_FILL_TIMEOUT_SECONDS,
    ) -> bytes:
        return await self._get_or_set(
            owner,
            key,
            ttl=ttl,
            factory=factory,
            timeout=timeout,
        )

    async def close(self) -> None:
        await self._runtime().close()

    def _runtime(self) -> RedisCacheRuntime:
        if self._runtime_owner is None:
            raise ConfigurationError("Redis cache runtime is not configured.")
        return self._runtime_owner


def _cache_key(owner: str, key: str) -> str:
    if not isinstance(owner, str) or not owner.strip():
        raise ValueError("Cache owner must be a non-blank string.")
    if ":" in owner:
        raise ValueError("Cache owner must not contain ':'.")
    if not isinstance(key, str) or not key.strip():
        raise ValueError("Cache key must be a non-blank string.")
    return f"{owner.strip()}:{key}"


def _ttl(value: float) -> float:
    if not isinstance(value, int | float) or value <= 0:
        raise ValueError("Cache TTL must be positive.")
    return float(value)


def _fill_timeout(value: float) -> float:
    if isinstance(value, bool) or not isinstance(value, int | float) or value <= 0:
        raise ValueError("Cache fill timeout must be positive.")
    return float(value)


__all__ = (
    "CacheCapability",
    "CacheFactory",
    "DEFAULT_CACHE_FILL_TIMEOUT_SECONDS",
    "InMemoryCache",
    "RedisCache",
)
