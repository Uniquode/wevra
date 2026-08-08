from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass, field
from time import monotonic
from typing import Any, cast
from uuid import uuid4

from wybra.cache.feature_models import (
    CacheConflictError,
    CacheFeatureError,
    WorkDelivery,
    WorkIdentity,
    validate_limit,
    validate_non_negative_finite,
    validate_payload,
    validate_positive_finite,
    validate_positive_integer,
    validate_resource,
)
from wybra.cache.redis_runtime import RedisCacheRuntime

_GROUP = "wybra"
_DEAD_LETTER_LIMIT = 1_000
_GROUP_CACHE_LIMIT = 256
_CONSUMER_PREFIX = "wybra:"
_CONSUMER_CLEANUP_INTERVAL_SECONDS = 60
_CONSUMER_IDLE_CLEANUP_MILLISECONDS = 3_600_000
_PROMOTER_IDLE_SECONDS = 60
_PROMOTER_RETRY_SECONDS = 1
_SIGNAL_STREAM_LIMIT = 1_000

logger = logging.getLogger(__name__)


class _GroupMissing:
    pass


_GROUP_MISSING = _GroupMissing()

_PUBLISH_SCRIPT = """
redis.call('hset', KEYS[3], ARGV[1], ARGV[2])
redis.call('hset', KEYS[4], ARGV[1], ARGV[3])
redis.call('hset', KEYS[5], ARGV[1], 0)
if tonumber(ARGV[4]) == 0 then
    redis.call('xadd', KEYS[1], '*', 'i', ARGV[1])
else
    local now = redis.call('time')
    local due = tonumber(now[1]) * 1000
        + math.floor(tonumber(now[2]) / 1000)
        + tonumber(ARGV[4])
    redis.call('zadd', KEYS[2], due, ARGV[1])
    redis.call('xadd', KEYS[6], 'MAXLEN', '~', ARGV[5], '*', 'i', ARGV[1])
end
return ARGV[1]
"""
_REGISTER_CONSUMER_SCRIPT = """
redis.call('hset', KEYS[1], ARGV[1], ARGV[2])
redis.call('pexpire', KEYS[1], math.max(tonumber(ARGV[2]), 604800000))
return 1
"""
_PROMOTE_SCRIPT = """
local now = redis.call('time')
local milliseconds = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local identities = redis.call(
    'zrangebyscore', KEYS[2], '-inf', milliseconds,
    'LIMIT', 0, tonumber(ARGV[1])
)
for _, identity in ipairs(identities) do
    redis.call('xadd', KEYS[1], '*', 'i', identity)
    redis.call('zrem', KEYS[2], identity)
end
return #identities
"""
_ISSUE_SCRIPT = """
local pending = redis.call('xpending', KEYS[1], ARGV[1], ARGV[2], ARGV[2], 1)
if #pending == 0 or pending[1][2] ~= ARGV[3] then
    return {}
end
local payload = redis.call('hget', KEYS[2], ARGV[4])
local maximum = redis.call('hget', KEYS[3], ARGV[4])
if not payload or not maximum then
    redis.call('xack', KEYS[1], ARGV[1], ARGV[2])
    redis.call('xdel', KEYS[1], ARGV[2])
    redis.call('hdel', KEYS[5], ARGV[2])
    redis.call('zrem', KEYS[6], ARGV[2])
    return {}
end
local attempt = redis.call('hincrby', KEYS[4], ARGV[4], 1)
local now = redis.call('time')
local deadline = tonumber(now[1]) * 1000
    + math.floor(tonumber(now[2]) / 1000)
    + tonumber(ARGV[6])
redis.call('hset', KEYS[5], ARGV[2], ARGV[5] .. '|' .. ARGV[4])
redis.call('zadd', KEYS[6], deadline, ARGV[2])
return {attempt, payload, maximum, deadline}
"""
_CLAIM_SCRIPT = """
local now = redis.call('time')
local milliseconds = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local due = redis.call('zrangebyscore', KEYS[6], '-inf', milliseconds, 'LIMIT', 0, 1)
local entry_id = due[1]
local previous_consumer = nil
if not entry_id then
    local cursor = redis.call('get', KEYS[8])
    local pending = redis.call('xpending', KEYS[1], ARGV[1], cursor or '-', '+', 100)
    local first = 1
    if cursor and #pending > 0 and pending[1][1] == cursor then
        first = 2
    end
    for index = first, #pending do
        local reservation = pending[index]
        local candidate = reservation[1]
        local idle = tonumber(reservation[3])
        local timeout = tonumber(redis.call('hget', KEYS[9], reservation[2]))
            or tonumber(ARGV[4])
        if idle >= timeout
            and not redis.call('zscore', KEYS[6], candidate) then
            entry_id = candidate
            previous_consumer = reservation[2]
            break
        end
    end
    if entry_id then
        redis.call('del', KEYS[8])
    elseif #pending == 100 then
        redis.call('set', KEYS[8], pending[#pending][1])
        return {'scan'}
    else
        redis.call('del', KEYS[8])
    end
end
if not entry_id then
    return {}
end
if not previous_consumer then
    local reservation = redis.call(
        'xpending', KEYS[1], ARGV[1], entry_id, entry_id, 1
    )
    if #reservation > 0 then
        previous_consumer = reservation[1][2]
    end
end
local entries = redis.call('xclaim', KEYS[1], ARGV[1], ARGV[2], 0, entry_id)
if #entries == 0 then
    redis.call('zrem', KEYS[6], entry_id)
    redis.call('hdel', KEYS[5], entry_id)
    return {}
end
if previous_consumer then
    redis.call('hdel', KEYS[9], previous_consumer)
end
local entry = entries[1]
local identity = nil
for index = 1, #entry[2], 2 do
    if entry[2][index] == 'i' then
        identity = entry[2][index + 1]
        break
    end
end
if not identity then
    redis.call('xack', KEYS[1], ARGV[1], entry_id)
    redis.call('xdel', KEYS[1], entry_id)
    redis.call('hdel', KEYS[5], entry_id)
    redis.call('zrem', KEYS[6], entry_id)
    return {}
end
local payload = redis.call('hget', KEYS[2], identity)
local maximum = redis.call('hget', KEYS[3], identity)
if not payload or not maximum then
    redis.call('xack', KEYS[1], ARGV[1], entry_id)
    redis.call('xdel', KEYS[1], entry_id)
    redis.call('hdel', KEYS[5], entry_id)
    redis.call('zrem', KEYS[6], entry_id)
    return {}
end
local attempt = tonumber(redis.call('hget', KEYS[4], identity))
if attempt >= tonumber(maximum) then
    redis.call('xack', KEYS[1], ARGV[1], entry_id)
    redis.call('xdel', KEYS[1], entry_id)
    redis.call('hdel', KEYS[5], entry_id)
    redis.call('zrem', KEYS[6], entry_id)
    redis.call(
        'xadd', KEYS[7], 'MAXLEN', '~', ARGV[5], '*',
        'i', identity, 'p', payload, 'a', attempt
    )
    redis.call('hdel', KEYS[2], identity)
    redis.call('hdel', KEYS[3], identity)
    redis.call('hdel', KEYS[4], identity)
    return {}
end
attempt = redis.call('hincrby', KEYS[4], identity, 1)
local deadline = milliseconds + tonumber(ARGV[4])
redis.call('hset', KEYS[5], entry_id, ARGV[3] .. '|' .. identity)
redis.call('zadd', KEYS[6], deadline, entry_id)
return {
    entry_id, identity, attempt, payload, maximum, deadline,
    previous_consumer or ''
}
"""
_ACKNOWLEDGE_SCRIPT = """
if redis.call('hget', KEYS[5], ARGV[2]) ~= ARGV[3] .. '|' .. ARGV[4] then
    return 0
end
redis.call('xack', KEYS[1], ARGV[1], ARGV[2])
redis.call('xdel', KEYS[1], ARGV[2])
redis.call('hdel', KEYS[2], ARGV[4])
redis.call('hdel', KEYS[3], ARGV[4])
redis.call('hdel', KEYS[4], ARGV[4])
redis.call('hdel', KEYS[5], ARGV[2])
redis.call('zrem', KEYS[6], ARGV[2])
return 1
"""
_RENEW_SCRIPT = """
if redis.call('hget', KEYS[1], ARGV[2]) ~= ARGV[3] .. '|' .. ARGV[4] then
    return {}
end
local now = redis.call('time')
local milliseconds = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local deadline = milliseconds
    + tonumber(ARGV[6])
redis.call('zadd', KEYS[2], deadline, ARGV[2])
redis.call('hset', KEYS[3], ARGV[5], ARGV[6])
redis.call('pexpire', KEYS[3], math.max(tonumber(ARGV[6]), 604800000))
return {deadline}
"""
_REJECT_SCRIPT = """
if redis.call('hget', KEYS[7], ARGV[2]) ~= ARGV[3] .. '|' .. ARGV[4] then
    return 0
end
local now = redis.call('time')
local milliseconds = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local attempt = tonumber(redis.call('hget', KEYS[5], ARGV[4]))
local maximum = tonumber(redis.call('hget', KEYS[4], ARGV[4]))
local payload = redis.call('hget', KEYS[3], ARGV[4])
redis.call('xack', KEYS[1], ARGV[1], ARGV[2])
redis.call('xdel', KEYS[1], ARGV[2])
redis.call('hdel', KEYS[7], ARGV[2])
redis.call('zrem', KEYS[8], ARGV[2])
if attempt >= maximum then
    redis.call(
        'xadd', KEYS[6], 'MAXLEN', '~', ARGV[6], '*',
        'i', ARGV[4], 'p', payload, 'a', attempt
    )
    redis.call('hdel', KEYS[3], ARGV[4])
    redis.call('hdel', KEYS[4], ARGV[4])
    redis.call('hdel', KEYS[5], ARGV[4])
    return 2
end
if tonumber(ARGV[5]) == 0 then
    redis.call('xadd', KEYS[1], '*', 'i', ARGV[4])
    return 1
end
local due = milliseconds + tonumber(ARGV[5])
redis.call('zadd', KEYS[2], due, ARGV[4])
redis.call('xadd', KEYS[9], 'MAXLEN', '~', ARGV[7], '*', 'i', ARGV[4])
return 1
"""
_DEAD_LETTER_SCRIPT = """
if redis.call('hget', KEYS[6], ARGV[2]) ~= ARGV[3] .. '|' .. ARGV[4] then
    return 0
end
local payload = redis.call('hget', KEYS[2], ARGV[4])
local attempt = redis.call('hget', KEYS[4], ARGV[4])
redis.call('xack', KEYS[1], ARGV[1], ARGV[2])
redis.call('xdel', KEYS[1], ARGV[2])
redis.call('hdel', KEYS[6], ARGV[2])
redis.call('zrem', KEYS[7], ARGV[2])
redis.call(
    'xadd', KEYS[5], 'MAXLEN', '~', ARGV[5], '*',
    'i', ARGV[4], 'p', payload, 'a', attempt
)
redis.call('hdel', KEYS[2], ARGV[4])
redis.call('hdel', KEYS[3], ARGV[4])
redis.call('hdel', KEYS[4], ARGV[4])
return 1
"""
_DISCARD_SCRIPT = """
redis.call('xack', KEYS[1], ARGV[1], ARGV[2])
redis.call('xdel', KEYS[1], ARGV[2])
redis.call('hdel', KEYS[2], ARGV[2])
redis.call('zrem', KEYS[3], ARGV[2])
return 1
"""
_NEXT_WAKEUP_SCRIPT = """
local now = redis.call('time')
local milliseconds = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local delayed = redis.call('zrange', KEYS[1], 0, 0, 'WITHSCORES')
local visible = redis.call('zrange', KEYS[2], 0, 0, 'WITHSCORES')
return {milliseconds, delayed[2] or -1, visible[2] or -1}
"""
_NEXT_DELAYED_WAKEUP_SCRIPT = """
local now = redis.call('time')
local milliseconds = tonumber(now[1]) * 1000 + math.floor(tonumber(now[2]) / 1000)
local delayed = redis.call('zrange', KEYS[1], 0, 0, 'WITHSCORES')
return {milliseconds, delayed[2] or -1}
"""


@dataclass(frozen=True, slots=True)
class RedisWorkQueue:
    runtime: RedisCacheRuntime
    _deliveries: dict[str, _DeliveryState] = field(default_factory=dict, init=False)
    _groups: dict[_QueueKeys, None] = field(default_factory=dict, init=False)
    _consumers: set[tuple[_QueueKeys, str]] = field(default_factory=set, init=False)
    _next_consumer_cleanup: dict[_QueueKeys, float] = field(
        default_factory=dict,
        init=False,
    )
    _promoter: asyncio.Task[None] | None = field(default=None, init=False)
    _promoter_positions: dict[_QueueKeys, str] = field(
        default_factory=dict,
        init=False,
    )

    async def publish(
        self,
        owner: str,
        queue: str,
        payload: bytes,
        *,
        delay: float = 0,
        max_attempts: int = 3,
    ) -> WorkIdentity:
        owner, queue = _queue(owner, queue)
        payload = validate_payload(payload)
        delay = validate_non_negative_finite(delay, label="work delay")
        max_attempts = validate_positive_integer(
            max_attempts,
            label="maximum delivery attempts",
        )
        delay_milliseconds = self.runtime.duration_milliseconds(
            delay,
            label="work delay",
            allow_zero=True,
        )
        identity = WorkIdentity(uuid4().hex)
        keys = self._keys(owner, queue)

        async def publish_work(client: Any) -> object:
            return await client.eval(
                _PUBLISH_SCRIPT,
                6,
                keys.stream,
                keys.delayed,
                keys.payloads,
                keys.maximums,
                keys.attempts,
                keys.delay_signals,
                identity.value,
                payload,
                max_attempts,
                delay_milliseconds,
                _SIGNAL_STREAM_LIMIT,
            )

        await self.runtime.feature_call(publish_work)
        if delay_milliseconds:
            self._ensure_promoter(keys)
        return identity

    async def reserve(
        self,
        owner: str,
        queue: str,
        consumer: str,
        *,
        visibility_timeout: float,
        wait_timeout: float = 0,
    ) -> WorkDelivery | None:
        owner, queue = _queue(owner, queue)
        consumer = validate_resource(consumer, label="queue consumer")
        visibility_timeout = validate_positive_finite(
            visibility_timeout,
            label="visibility timeout",
        )
        wait_timeout = validate_non_negative_finite(
            wait_timeout,
            label="queue wait timeout",
        )
        keys = self._keys(owner, queue)
        visibility_milliseconds = self.runtime.duration_milliseconds(
            visibility_timeout,
            label="visibility timeout",
        )
        redis_consumer = f"{_CONSUMER_PREFIX}{uuid4().hex}:{consumer}"
        try:
            delivery = await self._reserve(
                keys,
                redis_consumer,
                visibility_timeout,
                visibility_milliseconds,
                wait_timeout,
            )
        except BaseException:
            self._consumers.discard((keys, redis_consumer))
            await self._cleanup_generated_consumer(
                keys,
                redis_consumer,
                suppress_errors=True,
            )
            raise
        if delivery is None:
            self._consumers.discard((keys, redis_consumer))
            await self._cleanup_generated_consumer(
                keys,
                redis_consumer,
                suppress_errors=False,
            )
        return delivery

    async def _cleanup_generated_consumer(
        self,
        keys: _QueueKeys,
        consumer: str,
        *,
        suppress_errors: bool,
    ) -> None:
        cleanup = asyncio.create_task(
            self._remove_consumer_if_idle(keys, consumer),
            name="wybra-cache-consumer-cleanup",
        )
        cancelled = False
        while not cleanup.done():
            try:
                await asyncio.shield(cleanup)
            except asyncio.CancelledError:
                cancelled = True
            except Exception:
                pass
        try:
            cleanup.result()
        except Exception as error:
            if not suppress_errors:
                raise
            logger.warning(
                "Redis work-queue consumer cleanup failed after reserve error: %s.",
                type(error).__name__,
            )
        if cancelled:
            raise asyncio.CancelledError

    async def _reserve(
        self,
        keys: _QueueKeys,
        consumer: str,
        visibility_timeout: float,
        visibility_milliseconds: int,
        wait_timeout: float,
    ) -> WorkDelivery | None:
        if wait_timeout:
            self._ensure_promoter(keys)
        await self._register_consumer(keys, consumer, visibility_milliseconds)
        self._consumers.add((keys, consumer))
        await self._ensure_group(keys)
        await self._prune_idle_consumers(keys)
        deadline = monotonic() + wait_timeout
        final_check = False
        while True:
            await self._promote(keys)
            delivery = await self._claim_expired(
                keys,
                consumer,
                visibility_timeout,
                visibility_milliseconds,
            )
            if delivery is not None:
                return delivery
            if wait_timeout == 0:
                remaining = 0.001
            else:
                remaining = deadline - monotonic()
                if remaining <= 0:
                    if final_check:
                        return None
                    final_check = True
                    remaining = 0.001
                else:
                    remaining = min(remaining, await self._next_wakeup(keys))
            delivery = await self._read_new(
                keys,
                consumer,
                visibility_timeout,
                visibility_milliseconds,
                remaining,
            )
            if isinstance(delivery, WorkDelivery):
                return delivery
            if wait_timeout == 0 or final_check:
                return None

    async def acknowledge(self, delivery: WorkDelivery) -> None:
        await self._settle(delivery, action="acknowledge", delay_milliseconds=0)

    async def renew(
        self,
        delivery: WorkDelivery,
        *,
        visibility_timeout: float,
    ) -> WorkDelivery:
        visibility_timeout = validate_positive_finite(
            visibility_timeout,
            label="visibility timeout",
        )
        visibility_milliseconds = self.runtime.duration_milliseconds(
            visibility_timeout,
            label="visibility timeout",
        )
        state = self._state_for_delivery(delivery)

        async def renew(client: Any) -> object:
            return await client.eval(
                _RENEW_SCRIPT,
                3,
                state.keys.receipts,
                state.keys.visible,
                state.keys.consumers,
                _GROUP,
                state.entry_id,
                delivery.receipt,
                delivery.identity.value,
                state.consumer,
                visibility_milliseconds,
            )

        renewed = cast(tuple[Any, ...], await self.runtime.feature_call(renew))
        if len(renewed) != 1:
            self._deliveries.pop(delivery.receipt, None)
            await self._remove_consumer_if_idle(state.keys, state.consumer)
            self._consumers.discard((state.keys, state.consumer))
            raise CacheConflictError(
                f"Delivery {delivery.identity.value!r} is stale or no longer reserved."
            )
        visible_until = _integer(renewed[0]) / 1_000
        return WorkDelivery(
            queue=delivery.queue,
            identity=delivery.identity,
            payload=delivery.payload,
            attempt=delivery.attempt,
            visible_until=visible_until,
            receipt=delivery.receipt,
        )

    async def reject(self, delivery: WorkDelivery, *, delay: float = 0) -> None:
        delay = validate_non_negative_finite(delay, label="retry delay")
        delay_milliseconds = self.runtime.duration_milliseconds(
            delay,
            label="retry delay",
            allow_zero=True,
        )
        await self._settle(
            delivery,
            action="reject",
            delay_milliseconds=delay_milliseconds,
        )

    async def dead_letter(self, delivery: WorkDelivery) -> None:
        await self._settle(delivery, action="dead-letter", delay_milliseconds=0)

    async def dead_letters(
        self,
        owner: str,
        queue: str,
        *,
        limit: int = 100,
    ) -> tuple[WorkDelivery, ...]:
        owner, queue = _queue(owner, queue)
        limit = validate_limit(limit)
        keys = self._keys(owner, queue)

        async def read_dead_letters(client: Any) -> object:
            return await client.xrange(keys.dead_letters, count=limit)

        entries = cast(list[Any], await self.runtime.feature_call(read_dead_letters))
        return tuple(_dead_delivery(queue, entry) for entry in entries)

    async def _ensure_group(self, keys: _QueueKeys) -> None:
        if keys in self._groups:
            self._groups.pop(keys)
            self._groups[keys] = None
            return

        async def create_group(client: Any) -> object:
            try:
                return await client.xgroup_create(
                    keys.stream,
                    _GROUP,
                    id="0-0",
                    mkstream=True,
                )
            except Exception as error:
                if "BUSYGROUP" not in str(error):
                    raise
                return None

        await self.runtime.feature_call(create_group)
        self._groups[keys] = None
        if len(self._groups) > _GROUP_CACHE_LIMIT:
            oldest = next(iter(self._groups))
            self._groups.pop(oldest)

    async def _register_consumer(
        self,
        keys: _QueueKeys,
        consumer: str,
        visibility_milliseconds: int,
    ) -> None:
        async def register_consumer(client: Any) -> object:
            return await client.eval(
                _REGISTER_CONSUMER_SCRIPT,
                1,
                keys.consumers,
                consumer,
                visibility_milliseconds,
            )

        await self.runtime.feature_call(register_consumer)

    async def close(self) -> None:
        promoter = self._promoter
        if promoter is not None:
            promoter.cancel()
            await asyncio.gather(promoter, return_exceptions=True)
        object.__setattr__(self, "_promoter", None)
        self._promoter_positions.clear()
        for keys, consumer in tuple(self._consumers):
            await self._remove_consumer_if_idle(keys, consumer)
            self._consumers.discard((keys, consumer))
        self._deliveries.clear()
        self._groups.clear()
        self._next_consumer_cleanup.clear()

    async def _prune_idle_consumers(self, keys: _QueueKeys) -> None:
        now = monotonic()
        if now < self._next_consumer_cleanup.get(keys, 0):
            return
        self._next_consumer_cleanup[keys] = now + _CONSUMER_CLEANUP_INTERVAL_SECONDS

        async def stale_consumers(client: Any) -> object:
            try:
                consumers = await client.xinfo_consumers(keys.stream, _GROUP)
            except Exception as error:
                if "NOGROUP" in str(error):
                    return ()
                raise
            return tuple(
                _consumer_name(entry)
                for entry in consumers
                if _is_stale_consumer(entry)
                and (keys, _consumer_name(entry)) not in self._consumers
            )

        stale = cast(
            tuple[str, ...],
            await self.runtime.feature_call(stale_consumers),
        )
        for consumer in stale:
            await self._remove_consumer_if_idle(keys, consumer)

    async def _remove_consumer_if_idle(
        self,
        keys: _QueueKeys,
        consumer: str,
    ) -> None:
        async def remove_consumer(client: Any) -> object:
            try:
                pending = await client.xpending_range(
                    keys.stream,
                    _GROUP,
                    "-",
                    "+",
                    1,
                    consumer,
                )
            except Exception as error:
                if "NOGROUP" in str(error):
                    await client.hdel(keys.consumers, consumer)
                    return None
                raise
            if pending:
                return None
            try:
                await client.xgroup_delconsumer(keys.stream, _GROUP, consumer)
            except Exception as error:
                if "NOGROUP" not in str(error):
                    raise
            return await client.hdel(keys.consumers, consumer)

        await self.runtime.feature_call(remove_consumer)

    def _ensure_promoter(self, keys: _QueueKeys) -> None:
        added = keys not in self._promoter_positions
        self._promoter_positions.setdefault(keys, "$")
        promoter = self._promoter
        if promoter is not None and not promoter.done():
            if not added:
                return
            promoter.cancel()
        object.__setattr__(
            self,
            "_promoter",
            asyncio.create_task(
                self._promote_delayed_work(),
                name="wybra-cache-promoter",
            ),
        )

    async def _promote_delayed_work(self) -> None:
        try:
            while True:
                try:
                    queues = tuple(self._promoter_positions)
                    if not queues:
                        return
                    delays: list[float] = []
                    for keys in queues:
                        await self._promote(keys)
                        delays.append(await self._next_delayed_wakeup(keys))
                    delay = min(delays)
                    block = (
                        _PROMOTER_IDLE_SECONDS * 1_000
                        if delay == float("inf")
                        else self.runtime.duration_milliseconds(
                            delay,
                            label="delayed work wake-up",
                        )
                    )
                    positions = {
                        keys.delay_signals: self._promoter_positions[keys]
                        for keys in queues
                    }
                    queues_by_signal = {keys.delay_signals: keys for keys in queues}

                    async def wait_for_signal(
                        client: Any,
                        signal_positions: dict[str, str] = positions,
                        signal_block: int = block,
                    ) -> object:
                        return await client.xread(
                            signal_positions,
                            count=1,
                            block=signal_block,
                        )

                    records = cast(
                        list[Any],
                        await self.runtime.feature_call(wait_for_signal),
                    )
                    if not records:
                        if delay == float("inf"):
                            if asyncio.current_task() is self._promoter:
                                for keys in queues:
                                    self._promoter_positions.pop(keys, None)
                            return
                        continue
                    for stream, entries in records:
                        if entries:
                            keys = queues_by_signal[_text(stream)]
                            self._promoter_positions[keys] = _text(entries[-1][0])
                except CacheFeatureError:
                    logger.warning(
                        "Redis delayed-work promoter failed; retrying in %s seconds.",
                        _PROMOTER_RETRY_SECONDS,
                    )
                    await asyncio.sleep(_PROMOTER_RETRY_SECONDS)
        except asyncio.CancelledError:
            raise
        finally:
            current = asyncio.current_task()
            if self._promoter is current:
                object.__setattr__(self, "_promoter", None)

    async def _promote(self, keys: _QueueKeys) -> None:
        async def promote(client: Any) -> object:
            return await client.eval(_PROMOTE_SCRIPT, 2, keys.stream, keys.delayed, 100)

        await self.runtime.feature_call(promote)

    async def _next_wakeup(self, keys: _QueueKeys) -> float:
        async def next_wakeup(client: Any) -> object:
            return await client.eval(
                _NEXT_WAKEUP_SCRIPT,
                2,
                keys.delayed,
                keys.visible,
            )

        result = cast(tuple[Any, ...], await self.runtime.feature_call(next_wakeup))
        if len(result) != 3:
            raise CacheFeatureError(
                "Redis work queue returned an invalid wake-up time."
            )
        now, delayed, visible = (_integer(value) for value in result)
        candidates = [value for value in (delayed, visible) if value >= 0]
        if not candidates:
            return float("inf")
        due = min(candidates)
        return max(0.001, (due - now) / 1_000)

    async def _next_delayed_wakeup(self, keys: _QueueKeys) -> float:
        async def next_delayed_wakeup(client: Any) -> object:
            return await client.eval(
                _NEXT_DELAYED_WAKEUP_SCRIPT,
                1,
                keys.delayed,
            )

        result = cast(
            tuple[Any, ...],
            await self.runtime.feature_call(next_delayed_wakeup),
        )
        if len(result) != 2:
            raise CacheFeatureError(
                "Redis work queue returned an invalid delayed wake-up time."
            )
        now, delayed = (_integer(value) for value in result)
        if delayed < 0:
            return float("inf")
        return max(0.001, (delayed - now) / 1_000)

    async def _discard_entry(self, keys: _QueueKeys, entry_id: object) -> None:
        async def discard(client: Any) -> object:
            return await client.eval(
                _DISCARD_SCRIPT,
                3,
                keys.stream,
                keys.receipts,
                keys.visible,
                _GROUP,
                entry_id,
            )

        await self.runtime.feature_call(discard)

    async def _claim_expired(
        self,
        keys: _QueueKeys,
        consumer: str,
        visibility_timeout: float,
        visibility_milliseconds: int,
    ) -> WorkDelivery | None:
        receipt = uuid4().hex

        async def claim(client: Any) -> object:
            try:
                return await client.eval(
                    _CLAIM_SCRIPT,
                    9,
                    keys.stream,
                    keys.payloads,
                    keys.maximums,
                    keys.attempts,
                    keys.receipts,
                    keys.visible,
                    keys.dead_letters,
                    keys.recovery,
                    keys.consumers,
                    _GROUP,
                    consumer,
                    receipt,
                    visibility_milliseconds,
                    _DEAD_LETTER_LIMIT,
                )
            except Exception as error:
                if "NOGROUP" in str(error):
                    return _GROUP_MISSING
                raise

        while True:
            claimed = await self.runtime.feature_call(claim)
            if claimed is _GROUP_MISSING:
                self._groups.pop(keys, None)
                await self._ensure_group(keys)
                return None
            claimed = cast(tuple[Any, ...], claimed)
            if _recovery_scan_required(claimed):
                await asyncio.sleep(0)
                continue
            if not claimed:
                return None
            return await self._delivery_from_claim(
                keys,
                claimed,
                receipt,
                consumer,
                visibility_timeout,
            )

    async def _read_new(
        self,
        keys: _QueueKeys,
        consumer: str,
        visibility_timeout: float,
        visibility_milliseconds: int,
        remaining: float,
    ) -> WorkDelivery | None:
        block = max(1, round(remaining * 1_000))

        async def read(client: Any) -> object:
            try:
                return await client.xreadgroup(
                    _GROUP,
                    consumer,
                    {keys.stream: ">"},
                    count=1,
                    block=block,
                )
            except Exception as error:
                if "NOGROUP" in str(error):
                    return _GROUP_MISSING
                raise

        records = await self.runtime.feature_call(read)
        if records is _GROUP_MISSING:
            self._groups.pop(keys, None)
            await self._ensure_group(keys)
            return None
        records = cast(list[Any], records)
        if not records:
            return None
        _stream, entries = records[0]
        if not entries:
            return None
        entry_id, fields = entries[0]
        identity = _optional_field(fields, b"i")
        if identity is None:
            await self._discard_entry(keys, entry_id)
            return None
        receipt = uuid4().hex

        async def issue(client: Any) -> object:
            return await client.eval(
                _ISSUE_SCRIPT,
                6,
                keys.stream,
                keys.payloads,
                keys.maximums,
                keys.attempts,
                keys.receipts,
                keys.visible,
                _GROUP,
                entry_id,
                consumer,
                identity,
                receipt,
                visibility_milliseconds,
            )

        issued = cast(tuple[Any, ...], await self.runtime.feature_call(issue))
        if not issued:
            return None
        return await self._delivery_from_claim(
            keys,
            (entry_id, identity, *issued),
            receipt,
            consumer,
            visibility_timeout,
        )

    async def _delivery_from_claim(
        self,
        keys: _QueueKeys,
        claimed: tuple[Any, ...],
        receipt: str,
        consumer: str,
        visibility_timeout: float,
    ) -> WorkDelivery | None:
        entry_id, identity, attempt, payload, maximum, visible_until, *previous = (
            claimed
        )
        attempt = _integer(attempt)
        maximum = _integer(maximum)
        identity = WorkIdentity(_text(identity))
        delivery = WorkDelivery(
            queue=keys.queue,
            identity=identity,
            payload=_bytes(payload),
            attempt=attempt,
            visible_until=_integer(visible_until) / 1_000,
            receipt=receipt,
        )
        self._deliveries[receipt] = _DeliveryState(
            keys=keys,
            entry_id=_text(entry_id),
            identity=identity,
            attempt=attempt,
            consumer=consumer,
        )
        if previous and _text(previous[0]):
            previous_consumer = _text(previous[0])
            await self._remove_consumer_if_idle(keys, previous_consumer)
            self._consumers.discard((keys, previous_consumer))
        if attempt <= maximum:
            return delivery
        await self.dead_letter(delivery)
        return None

    async def _settle(
        self,
        delivery: WorkDelivery,
        *,
        action: str,
        delay_milliseconds: int,
    ) -> None:
        state = self._state_for_delivery(delivery)
        keys = state.keys
        entry_id = state.entry_id

        async def settle(client: Any) -> object:
            if action == "acknowledge":
                return await client.eval(
                    _ACKNOWLEDGE_SCRIPT,
                    6,
                    keys.stream,
                    keys.payloads,
                    keys.maximums,
                    keys.attempts,
                    keys.receipts,
                    keys.visible,
                    _GROUP,
                    entry_id,
                    delivery.receipt,
                    delivery.identity.value,
                )
            if action == "reject":
                return await client.eval(
                    _REJECT_SCRIPT,
                    9,
                    keys.stream,
                    keys.delayed,
                    keys.payloads,
                    keys.maximums,
                    keys.attempts,
                    keys.dead_letters,
                    keys.receipts,
                    keys.visible,
                    keys.delay_signals,
                    _GROUP,
                    entry_id,
                    delivery.receipt,
                    delivery.identity.value,
                    delay_milliseconds,
                    _DEAD_LETTER_LIMIT,
                    _SIGNAL_STREAM_LIMIT,
                )
            return await client.eval(
                _DEAD_LETTER_SCRIPT,
                7,
                keys.stream,
                keys.payloads,
                keys.maximums,
                keys.attempts,
                keys.dead_letters,
                keys.receipts,
                keys.visible,
                _GROUP,
                entry_id,
                delivery.receipt,
                delivery.identity.value,
                _DEAD_LETTER_LIMIT,
            )

        settled = await self.runtime.feature_call(settle)
        if not settled:
            self._deliveries.pop(delivery.receipt, None)
            await self._remove_consumer_if_idle(keys, state.consumer)
            self._consumers.discard((keys, state.consumer))
            raise CacheConflictError(
                f"Delivery {delivery.identity.value!r} is stale or no longer reserved."
            )
        self._deliveries.pop(delivery.receipt, None)
        if action == "reject" and delay_milliseconds:
            self._ensure_promoter(keys)
        if not any(
            candidate.keys == keys and candidate.consumer == state.consumer
            for candidate in self._deliveries.values()
        ):
            await self._remove_consumer_if_idle(keys, state.consumer)
            self._consumers.discard((keys, state.consumer))

    def _state_for_delivery(self, delivery: WorkDelivery) -> _DeliveryState:
        state = self._deliveries.get(delivery.receipt)
        if (
            state is None
            or state.identity != delivery.identity
            or state.attempt != delivery.attempt
            or state.keys.queue != delivery.queue
        ):
            raise CacheConflictError(
                f"Delivery {delivery.identity.value!r} is stale or no longer reserved."
            )
        return state

    def _keys(self, owner: str, queue: str) -> _QueueKeys:
        return _QueueKeys(
            queue=queue,
            stream=self.runtime.key("work-queue", owner, queue),
            delayed=self.runtime.key("work-queue-delayed", owner, queue),
            payloads=self.runtime.key("work-queue-payload", owner, queue),
            maximums=self.runtime.key("work-queue-maximum", owner, queue),
            attempts=self.runtime.key("work-queue-attempt", owner, queue),
            receipts=self.runtime.key("work-queue-receipt", owner, queue),
            visible=self.runtime.key("work-queue-visible", owner, queue),
            dead_letters=self.runtime.key("work-queue-dead", owner, queue),
            recovery=self.runtime.key("work-queue-recovery", owner, queue),
            consumers=self.runtime.key("work-queue-consumer", owner, queue),
            delay_signals=self.runtime.key("work-queue-delay-signal", owner, queue),
        )


@dataclass(frozen=True, slots=True)
class _QueueKeys:
    queue: str
    stream: str
    delayed: str
    payloads: str
    maximums: str
    attempts: str
    receipts: str
    visible: str
    dead_letters: str
    recovery: str
    consumers: str
    delay_signals: str


@dataclass(frozen=True, slots=True)
class _DeliveryState:
    keys: _QueueKeys
    entry_id: str
    identity: WorkIdentity
    attempt: int
    consumer: str


def _queue(owner: str, queue: str) -> tuple[str, str]:
    return (
        validate_resource(owner, label="cache owner"),
        validate_resource(queue, label="queue"),
    )


def _field(fields: Mapping[object, object], name: bytes) -> str:
    value = _optional_field(fields, name)
    if value is None:
        raise CacheFeatureError("Redis work queue entry is missing its identity.")
    return value


def _optional_field(fields: Mapping[object, object], name: bytes) -> str | None:
    for key, value in fields.items():
        if _bytes(key) == name:
            return _text(value)
    return None


def _dead_delivery(
    queue: str,
    entry: tuple[object, Mapping[object, object]],
) -> WorkDelivery:
    _entry_id, fields = entry
    identity = _field(fields, b"i")
    return WorkDelivery(
        queue=queue,
        identity=WorkIdentity(identity),
        payload=_bytes(_field_value(fields, b"p")),
        attempt=_integer(_field_value(fields, b"a")),
        visible_until=0,
        receipt=f"dead-{identity}",
    )


def _field_value(fields: Mapping[object, object], name: bytes) -> object:
    for key, value in fields.items():
        if _bytes(key) == name:
            return value
    raise CacheFeatureError("Redis work queue entry is incomplete.")


def _bytes(value: object) -> bytes:
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode()
    raise CacheFeatureError("Redis work queue returned an invalid value.")


def _text(value: object) -> str:
    return _bytes(value).decode()


def _integer(value: object) -> int:
    if isinstance(value, int):
        return value
    try:
        return int(_text(value))
    except ValueError as error:
        raise CacheFeatureError(
            "Redis work queue returned an invalid integer."
        ) from error


def _recovery_scan_required(claimed: tuple[Any, ...]) -> bool:
    return len(claimed) == 1 and _text(claimed[0]) == "scan"


def _is_stale_consumer(entry: Mapping[object, object]) -> bool:
    name = entry.get("name", entry.get(b"name"))
    pending = entry.get("pending", entry.get(b"pending"))
    idle = entry.get("idle", entry.get(b"idle"))
    return (
        name is not None
        and _text(name).startswith(_CONSUMER_PREFIX)
        and _integer(pending or 0) == 0
        and _integer(idle or 0) >= _CONSUMER_IDLE_CLEANUP_MILLISECONDS
    )


def _consumer_name(entry: Mapping[object, object]) -> str:
    name = entry.get("name", entry.get(b"name"))
    if name is None:
        raise CacheFeatureError("Redis work queue returned an invalid consumer.")
    return _text(name)


__all__ = ("RedisWorkQueue",)
