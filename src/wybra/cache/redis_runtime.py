from __future__ import annotations

import asyncio
import importlib
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, TypeVar
from urllib.parse import quote
from uuid import uuid4

from wybra.cache.feature_models import (
    CacheFeatureError,
    validate_positive_finite,
    validate_resource,
)
from wybra.core.exceptions import ConfigurationError

ResultT = TypeVar("ResultT")
logger = logging.getLogger(__name__)
MAX_REDIS_TTL_MILLISECONDS = 2**62

_ADVANCED_FEATURES = frozenset({"atomic", "lease"})
_ATOMIC_READINESS_SCRIPT = """
if redis.call('exists', KEYS[1]) ~= 0 then
    return {0}
end
redis.call('hset', KEYS[1], 'type', 'value', 'payload', '1')
redis.call('hget', KEYS[1], 'type')
redis.call('hincrby', KEYS[1], 'counter', 1)
redis.call('pexpire', KEYS[1], ARGV[1])
local sequence = redis.call('incr', KEYS[2])
local sequence_value = redis.call('get', KEYS[2])
redis.call('del', KEYS[1], KEYS[2])
return {sequence, sequence_value, 1}
"""

_LEASE_READINESS_SCRIPT = """
redis.call('exists', KEYS[1])
redis.call('hset', KEYS[1], 'holder', 'readiness')
redis.call('hget', KEYS[1], 'holder')
redis.call('pexpire', KEYS[1], ARGV[1])
local sequence = redis.call('incr', KEYS[2])
local sequence_value = redis.call('get', KEYS[2])
local now = redis.call('time')
redis.call('del', KEYS[1], KEYS[2])
return {sequence, sequence_value, now[1]}
"""


@dataclass(slots=True)
class RedisCacheRuntime:
    url: str = field(repr=False)
    namespace: str | None = None
    _client: Any = field(default=None, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    def client(self) -> Any:
        if self._closed:
            raise CacheFeatureError("The Redis cache backend is closed.")
        if self._client is None:
            try:
                redis_module = importlib.import_module("redis.asyncio")
            except ImportError as exc:
                raise ConfigurationError(
                    "Redis cache backend requires the optional cache dependency. "
                    "Install wybra[cache]."
                ) from exc
            self._client = redis_module.Redis.from_url(
                self.url,
                decode_responses=False,
            )
        return self._client

    async def health_check(self) -> None:
        try:
            healthy = await self.client().ping()
        except asyncio.CancelledError:
            raise
        except ConfigurationError:
            raise
        except Exception as exc:
            _log_failure("health check", exc)
            raise ConfigurationError(
                "Redis cache backend health check failed."
            ) from None
        if healthy is not True:
            raise ConfigurationError("Redis cache backend health check failed.")

    async def validate_features(self, features: frozenset[str]) -> None:
        """Verify Redis can safely provide the selected advanced features."""
        if not features & _ADVANCED_FEATURES:
            return
        if self.namespace is None:
            raise ConfigurationError(
                "Advanced Redis cache features require an instance namespace."
            )
        suffix = uuid4().hex
        try:
            client = self.client()
            await self._validate_advanced_configuration(client)
            if "atomic" in features:
                result = await client.eval(
                    _ATOMIC_READINESS_SCRIPT,
                    2,
                    self.key("readiness", "cache", f"atomic-{suffix}"),
                    self.sequence_key(f"atomic-readiness-{suffix}"),
                    1_000,
                )
                _validate_readiness_result(result)
                await client.hmget(
                    self.key("readiness", "cache", f"atomic-{suffix}"),
                    "type",
                    "payload",
                    "revision",
                )
            if "lease" in features:
                result = await client.eval(
                    _LEASE_READINESS_SCRIPT,
                    2,
                    self.key("readiness", "cache", f"lease-{suffix}"),
                    self.sequence_key(f"lease-readiness-{suffix}"),
                    1_000,
                )
                _validate_readiness_result(result)
        except asyncio.CancelledError:
            raise
        except ConfigurationError:
            raise
        except Exception as exc:
            _log_failure("advanced-feature readiness", exc)
            raise ConfigurationError(
                "Redis cache backend cannot provide the configured advanced features."
            ) from None

    async def _validate_advanced_configuration(self, client: Any) -> None:
        try:
            configuration = await client.config_get("appendonly", "maxmemory-policy")
            appendonly = _configuration_value(configuration, "appendonly")
            eviction_policy = _configuration_value(configuration, "maxmemory-policy")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "Redis advanced-feature configuration could not be inspected "
                "(%s); continuing with operational readiness checks.",
                type(exc).__name__,
            )
            return
        if appendonly != "yes" or eviction_policy.startswith("allkeys-"):
            raise ConfigurationError(
                "Redis advanced cache features require append-only persistence "
                "and a non-allkeys eviction policy."
            )

    async def feature_call(
        self,
        operation: Callable[[Any], Awaitable[ResultT]],
    ) -> ResultT:
        try:
            return await operation(self.client())
        except asyncio.CancelledError:
            raise
        except ConfigurationError:
            raise
        except CacheFeatureError:
            raise
        except Exception as exc:
            _log_failure("feature operation", exc)
            raise CacheFeatureError("Redis cache feature operation failed.") from None

    def baseline_key(self, owner: str, key: str) -> str:
        owner = validate_resource(owner, label="cache owner").strip()
        if ":" in owner:
            raise ValueError("Cache owner must not contain ':'.")
        key = validate_resource(key, label="cache key")
        if self.namespace is None:
            return f"{owner}:{key}"
        return self.key("baseline", owner, key)

    def key(self, domain: str, owner: str, key: str) -> str:
        if self.namespace is None:
            raise CacheFeatureError(
                "Advanced Redis cache features require an instance namespace."
            )
        namespace = validate_resource(self.namespace, label="cache namespace")
        if ":" in namespace:
            raise ValueError("Cache namespace must not contain ':'.")
        domain = validate_resource(domain, label="cache feature domain")
        owner = validate_resource(owner, label="cache owner")
        key = validate_resource(key, label="cache key")
        return ":".join(
            (
                namespace,
                quote(domain, safe=""),
                quote(owner, safe=""),
                quote(key, safe=""),
            )
        )

    def sequence_key(self, domain: str) -> str:
        if self.namespace is None:
            raise CacheFeatureError(
                "Advanced Redis cache features require an instance namespace."
            )
        namespace = validate_resource(self.namespace, label="cache namespace")
        if ":" in namespace:
            raise ValueError("Cache namespace must not contain ':'.")
        domain = validate_resource(domain, label="cache sequence domain")
        return f"{namespace}:sequence:{quote(domain, safe='')}"

    def ttl_milliseconds(self, value: float, *, label: str) -> int:
        ttl = validate_positive_finite(value, label=label)
        milliseconds = max(1, round(ttl * 1000))
        if milliseconds > MAX_REDIS_TTL_MILLISECONDS:
            raise ValueError(f"{label.capitalize()} exceeds the Redis TTL limit.")
        return milliseconds

    async def close(self) -> None:
        if self._closed:
            return
        client = self._client
        if client is None:
            self._closed = True
            return
        try:
            await client.aclose()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("shutdown", exc)
            raise CacheFeatureError("Redis cache backend shutdown failed.") from None
        self._client = None
        self._closed = True


def _configuration_value(configuration: object, key: str) -> str:
    if not isinstance(configuration, dict):
        raise ConfigurationError(
            "Redis cache backend cannot inspect advanced feature configuration."
        )
    value = configuration.get(key)
    if value is None:
        value = configuration.get(key.encode("utf-8"))
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if not isinstance(value, str):
        raise ConfigurationError(
            "Redis cache backend cannot inspect advanced feature configuration."
        )
    return value.lower()


def _validate_readiness_result(result: object) -> None:
    if not isinstance(result, list | tuple) or len(result) != 3:
        raise ConfigurationError(
            "Redis cache backend cannot provide the configured advanced features."
        )


def _log_failure(operation: str, error: Exception) -> None:
    logger.warning(
        "Redis cache %s failed (%s).",
        operation,
        type(error).__name__,
    )


__all__ = ("RedisCacheRuntime",)
