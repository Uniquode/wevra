from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from wybra.cache.feature_models import (
    AtomicCacheValue,
    CacheConflictError,
    CacheFeatureError,
    CacheRevision,
    CounterCacheValue,
    LeaseToken,
    validate_lease_token,
    validate_payload,
    validate_resource,
)
from wybra.cache.redis_runtime import RedisCacheRuntime

CREATE_SCRIPT = """
if ARGV[3] ~= '' then
    if redis.call('hget', KEYS[3], 'holder') ~= ARGV[3]
        or redis.call('hget', KEYS[3], 'token') ~= ARGV[4]
        or redis.call('hget', KEYS[3], 'fencing_token') ~= ARGV[5]
    then
        return {-2}
    end
end
if redis.call('exists', KEYS[1]) == 1 then
    return {0}
end
redis.call('incr', KEYS[2])
local revision = redis.call('get', KEYS[2])
redis.call(
    'hset',
    KEYS[1],
    'type',
    'value',
    'payload',
    ARGV[1],
    'revision',
    revision
)
redis.call('pexpire', KEYS[1], ARGV[2])
return {1, revision}
"""

COMPARE_AND_SWAP_SCRIPT = """
if ARGV[4] ~= '' then
    if redis.call('hget', KEYS[3], 'holder') ~= ARGV[4]
        or redis.call('hget', KEYS[3], 'token') ~= ARGV[5]
        or redis.call('hget', KEYS[3], 'fencing_token') ~= ARGV[6]
    then
        return {-2}
    end
end
local current_revision = redis.call('hget', KEYS[1], 'revision')
if not current_revision then
    return {0}
end
if current_revision ~= ARGV[1] then
    return {0}
end
if redis.call('hget', KEYS[1], 'type') ~= 'value' then
    return {-1}
end
redis.call('incr', KEYS[2])
local revision = redis.call('get', KEYS[2])
redis.call(
    'hset',
    KEYS[1],
    'payload',
    ARGV[2],
    'revision',
    revision
)
redis.call('pexpire', KEYS[1], ARGV[3])
return {1, revision}
"""

COMPARE_AND_DELETE_SCRIPT = """
if ARGV[2] ~= '' then
    if redis.call('hget', KEYS[2], 'holder') ~= ARGV[2]
        or redis.call('hget', KEYS[2], 'token') ~= ARGV[3]
        or redis.call('hget', KEYS[2], 'fencing_token') ~= ARGV[4]
    then
        return {-2}
    end
end
local current_revision = redis.call('hget', KEYS[1], 'revision')
if not current_revision then
    return {0}
end
if current_revision ~= ARGV[1] then
    return {0}
end
if redis.call('hget', KEYS[1], 'type') ~= 'value' then
    return {-1}
end
redis.call('del', KEYS[1])
return {1}
"""

INCREMENT_SCRIPT = """
local current_type = redis.call('hget', KEYS[1], 'type')
if current_type and current_type ~= 'counter' then
    return {-1}
end
redis.call('hincrby', KEYS[1], 'payload', ARGV[1])
local value = redis.call('hget', KEYS[1], 'payload')
redis.call('incr', KEYS[2])
local revision = redis.call('get', KEYS[2])
redis.call(
    'hset',
    KEYS[1],
    'type',
    'counter',
    'revision',
    revision
)
redis.call('pexpire', KEYS[1], ARGV[2])
return {1, value, revision}
"""


@dataclass(frozen=True, slots=True)
class RedisAtomicCache:
    runtime: RedisCacheRuntime

    async def get(self, owner: str, key: str) -> AtomicCacheValue | None:
        entry_key = self._entry_key(owner, key)

        async def read(client: Any) -> Any:
            return await client.hmget(entry_key, "type", "payload", "revision")

        result = await self.runtime.feature_call(read)
        if not isinstance(result, list | tuple) or len(result) != 3:
            raise CacheFeatureError("Redis atomic cache state is invalid.")
        if all(field is None for field in result):
            return None
        value_type, payload, revision_value = result
        if value_type == b"counter":
            raise CacheConflictError("The requested atomic key contains a counter.")
        if (
            value_type != b"value"
            or not isinstance(payload, bytes)
            or revision_value is None
        ):
            raise CacheFeatureError("Redis atomic cache state is invalid.")
        return AtomicCacheValue(payload, CacheRevision(_integer(revision_value)))

    async def create(
        self,
        owner: str,
        key: str,
        value: bytes,
        *,
        ttl: float,
        lease: LeaseToken | None = None,
    ) -> AtomicCacheValue | None:
        entry_key = self._entry_key(owner, key)
        value = validate_payload(value)
        ttl_ms = self.runtime.ttl_milliseconds(ttl, label="atomic value TTL")
        lease_key, lease_arguments = self._lease_arguments(lease)

        async def create_value(client: Any) -> Any:
            return await client.eval(
                CREATE_SCRIPT,
                3,
                entry_key,
                self.runtime.sequence_key("atomic-revision"),
                lease_key,
                value,
                ttl_ms,
                *lease_arguments,
            )

        result = _script_result(await self.runtime.feature_call(create_value))
        if result[0] == 0:
            return None
        if result[0] == -2:
            raise CacheConflictError("Lease is stale or no longer held.")
        _require_success(result, length=2)
        return AtomicCacheValue(value, CacheRevision(_integer(result[1])))

    async def compare_and_swap(
        self,
        owner: str,
        key: str,
        expected: CacheRevision,
        value: bytes,
        *,
        ttl: float,
        lease: LeaseToken | None = None,
    ) -> AtomicCacheValue | None:
        entry_key = self._entry_key(owner, key)
        expected = _revision(expected)
        value = validate_payload(value)
        ttl_ms = self.runtime.ttl_milliseconds(ttl, label="atomic value TTL")
        lease_key, lease_arguments = self._lease_arguments(lease)

        async def swap(client: Any) -> Any:
            return await client.eval(
                COMPARE_AND_SWAP_SCRIPT,
                3,
                entry_key,
                self.runtime.sequence_key("atomic-revision"),
                lease_key,
                expected.value,
                value,
                ttl_ms,
                *lease_arguments,
            )

        result = _script_result(await self.runtime.feature_call(swap))
        if result[0] == 0:
            return None
        if result[0] == -2:
            raise CacheConflictError("Lease is stale or no longer held.")
        if result[0] == -1:
            raise CacheConflictError("The requested atomic key contains a counter.")
        _require_success(result, length=2)
        return AtomicCacheValue(value, CacheRevision(_integer(result[1])))

    async def compare_and_delete(
        self,
        owner: str,
        key: str,
        expected: CacheRevision,
        *,
        lease: LeaseToken | None = None,
    ) -> bool:
        entry_key = self._entry_key(owner, key)
        expected = _revision(expected)
        lease_key, lease_arguments = self._lease_arguments(lease)

        async def delete(client: Any) -> Any:
            return await client.eval(
                COMPARE_AND_DELETE_SCRIPT,
                2,
                entry_key,
                lease_key,
                expected.value,
                *lease_arguments,
            )

        result = _script_result(await self.runtime.feature_call(delete))
        if result[0] == -1:
            raise CacheConflictError("The requested atomic key contains a counter.")
        if result[0] == -2:
            raise CacheConflictError("Lease is stale or no longer held.")
        if result[0] not in {0, 1}:
            raise CacheFeatureError(
                "Redis atomic cache operation returned invalid state."
            )
        return result[0] == 1

    async def increment(
        self,
        owner: str,
        key: str,
        *,
        amount: int = 1,
        ttl: float,
    ) -> CounterCacheValue:
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise TypeError("Counter amount must be an integer.")
        entry_key = self._entry_key(owner, key)
        ttl_ms = self.runtime.ttl_milliseconds(ttl, label="counter TTL")

        async def increment_value(client: Any) -> Any:
            return await client.eval(
                INCREMENT_SCRIPT,
                2,
                entry_key,
                self.runtime.sequence_key("atomic-revision"),
                amount,
                ttl_ms,
            )

        result = _script_result(await self.runtime.feature_call(increment_value))
        if result[0] == -1:
            raise CacheConflictError(
                "The requested counter key contains an atomic byte value."
            )
        _require_success(result, length=3)
        return CounterCacheValue(
            _integer(result[1]),
            CacheRevision(_integer(result[2])),
        )

    def _entry_key(self, owner: str, key: str) -> str:
        validate_resource(owner, label="cache owner")
        validate_resource(key, label="cache key")
        return self.runtime.key("atomic", owner, key)

    def _lease_arguments(
        self,
        lease: LeaseToken | None,
    ) -> tuple[str, tuple[str, str, int]]:
        if lease is None:
            return self.runtime.key("lease-fence-unused", "fence", "unused"), (
                "",
                "",
                0,
            )
        lease = validate_lease_token(lease)
        return (
            self.runtime.key("lease", lease.owner, lease.resource),
            (lease.holder, lease.token, lease.fencing_token.value),
        )


def _revision(value: CacheRevision) -> CacheRevision:
    if not isinstance(value, CacheRevision):
        raise TypeError("Expected revision must be a CacheRevision.")
    return value


def _script_result(value: Any) -> tuple[Any, ...]:
    if not isinstance(value, list | tuple) or not value:
        raise CacheFeatureError("Redis atomic cache operation returned invalid state.")
    return tuple(value)


def _require_success(result: tuple[Any, ...], *, length: int) -> None:
    if result[0] != 1 or len(result) != length:
        raise CacheFeatureError("Redis atomic cache operation returned invalid state.")


def _integer(value: Any) -> int:
    if isinstance(value, bytes):
        try:
            value = int(value)
        except ValueError:
            raise CacheFeatureError("Redis cache integer state is invalid.") from None
    if isinstance(value, bool) or not isinstance(value, int):
        raise CacheFeatureError("Redis cache integer state is invalid.")
    return value


__all__ = ("RedisAtomicCache",)
