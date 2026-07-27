from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from wybra.cache.feature_models import (
    CacheConflictError,
    CacheFeatureError,
    FencingToken,
    LeaseToken,
    validate_resource,
)
from wybra.cache.redis_runtime import RedisCacheRuntime

ACQUIRE_SCRIPT = """
if redis.call('exists', KEYS[1]) == 1 then
    return {0}
end
redis.call('incr', KEYS[2])
local fencing_token = redis.call('get', KEYS[2])
local now = redis.call('time')
local expires_at = now[1] * 1000 + math.floor(now[2] / 1000) + ARGV[3]
redis.call(
    'hset',
    KEYS[1],
    'holder',
    ARGV[1],
    'token',
    ARGV[2],
    'fencing_token',
    fencing_token
)
redis.call('pexpire', KEYS[1], ARGV[3])
return {1, fencing_token, expires_at}
"""

RENEW_SCRIPT = """
if redis.call('hget', KEYS[1], 'holder') ~= ARGV[1]
    or redis.call('hget', KEYS[1], 'token') ~= ARGV[2]
    or redis.call('hget', KEYS[1], 'fencing_token') ~= ARGV[3]
then
    return {0}
end
local now = redis.call('time')
local expires_at = now[1] * 1000 + math.floor(now[2] / 1000) + ARGV[4]
redis.call('pexpire', KEYS[1], ARGV[4])
return {1, expires_at}
"""

RELEASE_SCRIPT = """
if redis.call('hget', KEYS[1], 'holder') ~= ARGV[1]
    or redis.call('hget', KEYS[1], 'token') ~= ARGV[2]
    or redis.call('hget', KEYS[1], 'fencing_token') ~= ARGV[3]
then
    return {0}
end
redis.call('del', KEYS[1])
return {1}
"""


@dataclass(frozen=True, slots=True)
class RedisLeaseCache:
    runtime: RedisCacheRuntime

    async def acquire(
        self,
        owner: str,
        resource: str,
        holder: str,
        *,
        ttl: float,
    ) -> LeaseToken | None:
        owner, resource = _lease_key(owner, resource)
        holder = validate_resource(holder, label="lease holder")
        ttl_ms = self.runtime.ttl_milliseconds(ttl, label="lease TTL")
        token = uuid4().hex
        entry_key = self.runtime.key("lease", owner, resource)

        async def acquire_lease(client: Any) -> Any:
            return await client.eval(
                ACQUIRE_SCRIPT,
                2,
                entry_key,
                self.runtime.sequence_key("lease-fencing"),
                holder,
                token,
                ttl_ms,
            )

        result = _script_result(await self.runtime.feature_call(acquire_lease))
        if result[0] == 0:
            return None
        _require_success(result, length=3)
        return LeaseToken(
            owner=owner,
            resource=resource,
            holder=holder,
            fencing_token=FencingToken(_integer(result[1])),
            expires_at=_integer(result[2]) / 1000,
            token=token,
        )

    async def renew(self, lease: LeaseToken, *, ttl: float) -> LeaseToken:
        lease = _lease(lease)
        ttl_ms = self.runtime.ttl_milliseconds(ttl, label="lease TTL")
        entry_key = self.runtime.key("lease", lease.owner, lease.resource)

        async def renew_lease(client: Any) -> Any:
            return await client.eval(
                RENEW_SCRIPT,
                1,
                entry_key,
                lease.holder,
                lease.token,
                lease.fencing_token.value,
                ttl_ms,
            )

        result = _script_result(await self.runtime.feature_call(renew_lease))
        if result[0] == 0:
            raise CacheConflictError("Lease is stale or no longer held.")
        _require_success(result, length=2)
        return LeaseToken(
            owner=lease.owner,
            resource=lease.resource,
            holder=lease.holder,
            fencing_token=lease.fencing_token,
            expires_at=_integer(result[1]) / 1000,
            token=lease.token,
        )

    async def release(self, lease: LeaseToken) -> None:
        lease = _lease(lease)
        entry_key = self.runtime.key("lease", lease.owner, lease.resource)

        async def release_lease(client: Any) -> Any:
            return await client.eval(
                RELEASE_SCRIPT,
                1,
                entry_key,
                lease.holder,
                lease.token,
                lease.fencing_token.value,
            )

        result = _script_result(await self.runtime.feature_call(release_lease))
        if result[0] == 0:
            raise CacheConflictError("Lease is stale or no longer held.")
        _require_success(result, length=1)


def _lease(value: LeaseToken) -> LeaseToken:
    if not isinstance(value, LeaseToken):
        raise TypeError("Lease must be a LeaseToken.")
    _lease_key(value.owner, value.resource)
    validate_resource(value.holder, label="lease holder")
    validate_resource(value.token, label="lease token")
    return value


def _lease_key(owner: str, resource: str) -> tuple[str, str]:
    return (
        validate_resource(owner, label="cache owner"),
        validate_resource(resource, label="lease resource"),
    )


def _script_result(value: Any) -> tuple[Any, ...]:
    if not isinstance(value, list | tuple) or not value:
        raise CacheFeatureError("Redis lease operation returned invalid state.")
    return tuple(value)


def _require_success(result: tuple[Any, ...], *, length: int) -> None:
    if result[0] != 1 or len(result) != length:
        raise CacheFeatureError("Redis lease operation returned invalid state.")


def _integer(value: Any) -> int:
    if isinstance(value, bytes):
        try:
            value = int(value)
        except ValueError:
            raise CacheFeatureError("Redis lease integer state is invalid.") from None
    if isinstance(value, bool) or not isinstance(value, int):
        raise CacheFeatureError("Redis lease integer state is invalid.")
    return value


__all__ = ("RedisLeaseCache",)
