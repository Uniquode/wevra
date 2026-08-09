from __future__ import annotations

import asyncio
import math
from collections.abc import Awaitable, Callable

import pytest

from wybra.cache import (
    MAX_CACHE_VALUE_BYTES,
    AtomicCacheCapability,
    CacheCapability,
    CacheConflictError,
    CachePositionExpiredError,
    LeaseCacheCapability,
    PubSubCacheCapability,
    ScheduleCacheCapability,
    ScheduleCursor,
    StreamCacheCapability,
    WorkQueueCacheCapability,
)

type AdvanceClock = Callable[[float], Awaitable[None]]
CONFORMANCE_TIMEOUT_SECONDS = 60.0


async def assert_baseline_cache_conformance(
    cache: CacheCapability,
    advance: AdvanceClock,
    *,
    owner: str = "conformance-baseline",
) -> None:
    async def unexpected_factory() -> bytes:
        pytest.fail("A cache hit must not run its factory.")

    await cache.set(owner, "value", b"first", ttl=60)
    assert await cache.get(owner, "value") == b"first"
    assert (
        await cache.get_or_set(
            owner,
            "value",
            ttl=60,
            factory=unexpected_factory,
        )
        == b"first"
    )
    await cache.delete(owner, "value")
    assert await cache.get(owner, "value") is None

    maximum_value = b"x" * MAX_CACHE_VALUE_BYTES
    await cache.set(owner, "maximum", maximum_value, ttl=60)
    assert await cache.get(owner, "maximum") == maximum_value
    with pytest.raises(ValueError, match="cannot exceed"):
        await cache.set(owner, "oversized", maximum_value + b"x", ttl=60)

    fill_started = asyncio.Event()
    release_fill = asyncio.Event()
    factory_calls = 0

    async def factory() -> bytes:
        nonlocal factory_calls
        factory_calls += 1
        fill_started.set()
        await release_fill.wait()
        return b"filled"

    first_fill = asyncio.create_task(
        cache.get_or_set(owner, "fill", ttl=60, factory=factory)
    )
    second_fill: asyncio.Task[bytes] | None = None
    second_started = asyncio.Event()

    async def second_get_or_set() -> bytes:
        second_started.set()
        return await cache.get_or_set(owner, "fill", ttl=60, factory=factory)

    try:
        await asyncio.wait_for(fill_started.wait(), timeout=CONFORMANCE_TIMEOUT_SECONDS)
        second_fill = asyncio.create_task(second_get_or_set())
        await asyncio.wait_for(
            second_started.wait(),
            timeout=CONFORMANCE_TIMEOUT_SECONDS,
        )
        await asyncio.sleep(0)
        assert factory_calls == 1
        release_fill.set()
        assert await asyncio.wait_for(
            asyncio.gather(first_fill, second_fill),
            timeout=CONFORMANCE_TIMEOUT_SECONDS,
        ) == [b"filled", b"filled"]
    finally:
        release_fill.set()
        fills = (first_fill,) if second_fill is None else (first_fill, second_fill)
        for fill in fills:
            if not fill.done():
                fill.cancel()
        await asyncio.gather(*fills, return_exceptions=True)
    assert factory_calls == 1
    assert await cache.get(owner, "fill") == b"filled"

    await cache.set(owner, "expires", b"value", ttl=1)
    await advance(1)
    assert await cache.get(owner, "expires") is None


async def assert_atomic_conformance(
    feature: AtomicCacheCapability,
    *,
    owner: str = "conformance-atomic",
) -> None:
    creates = await asyncio.gather(
        *(
            feature.create(owner, "value", str(index).encode(), ttl=30)
            for index in range(8)
        )
    )
    winners = [created for created in creates if created is not None]
    assert len(winners) == 1
    created = winners[0]

    updated = await feature.compare_and_swap(
        owner,
        "value",
        created.revision,
        b"two",
        ttl=30,
    )
    assert updated is not None
    assert (
        await feature.compare_and_swap(
            owner,
            "value",
            created.revision,
            b"stale",
            ttl=30,
        )
        is None
    )
    assert not await feature.compare_and_delete(
        owner,
        "value",
        created.revision,
    )
    assert await feature.compare_and_delete(
        owner,
        "value",
        updated.revision,
    )
    assert await feature.get(owner, "value") is None

    counters = await asyncio.gather(
        *(feature.increment(owner, "counter", ttl=30) for _ in range(8))
    )
    assert sorted(counter.value for counter in counters) == list(range(1, 9))
    assert len({counter.revision for counter in counters}) == 8

    typed_value = await feature.create(owner, "typed-value", b"value", ttl=30)
    assert typed_value is not None
    with pytest.raises(CacheConflictError):
        await feature.increment(owner, "typed-value", ttl=30)

    typed_counter = await feature.increment(owner, "typed-counter", ttl=30)
    with pytest.raises(CacheConflictError):
        await feature.get(owner, "typed-counter")
    with pytest.raises(CacheConflictError):
        await feature.compare_and_swap(
            owner,
            "typed-counter",
            typed_counter.revision,
            b"value",
            ttl=30,
        )
    with pytest.raises(CacheConflictError):
        await feature.compare_and_delete(
            owner,
            "typed-counter",
            typed_counter.revision,
        )


async def assert_lease_conformance(
    feature: LeaseCacheCapability,
    advance: AdvanceClock,
    *,
    owner: str = "conformance-lease",
    lease_ttl: float = 5,
    renewed_ttl: float = 10,
) -> None:
    first = await feature.acquire(owner, "resource", "holder-1", ttl=lease_ttl)
    assert first is not None
    assert (
        await feature.acquire(
            owner,
            "resource",
            "holder-2",
            ttl=lease_ttl,
        )
        is None
    )

    renewed = await feature.renew(first, ttl=renewed_ttl)
    assert renewed.owner == first.owner
    assert renewed.resource == first.resource
    assert renewed.holder == first.holder
    assert renewed.token == first.token
    assert renewed.fencing_token == first.fencing_token
    assert renewed.expires_at > first.expires_at
    await feature.release(renewed)
    with pytest.raises(CacheConflictError):
        await feature.release(renewed)

    second = await feature.acquire(owner, "resource", "holder-2", ttl=lease_ttl)
    assert second is not None
    assert second.fencing_token > first.fencing_token
    with pytest.raises(CacheConflictError):
        await feature.renew(first, ttl=lease_ttl)

    await advance(lease_ttl)
    third = await feature.acquire(owner, "resource", "holder-3", ttl=lease_ttl)
    assert third is not None
    assert third.fencing_token > second.fencing_token
    with pytest.raises(CacheConflictError):
        await feature.release(second)


async def assert_lease_fenced_mutation_conformance(
    atomic: AtomicCacheCapability,
    streams: StreamCacheCapability,
    leases: LeaseCacheCapability,
    advance: AdvanceClock,
    *,
    owner: str = "conformance-fence",
    lease_ttl: float = 5,
) -> None:
    first = await leases.acquire(owner, "resource", "holder-1", ttl=lease_ttl)
    assert first is not None
    live = await atomic.create(owner, "value", b"live", ttl=30, lease=first)
    assert live is not None
    position = await streams.append(owner, "events", b"live", lease=first)
    assert position.value == 1

    await advance(lease_ttl)
    second = await leases.acquire(owner, "resource", "holder-2", ttl=lease_ttl)
    assert second is not None

    with pytest.raises(CacheConflictError):
        await atomic.create(owner, "stale-create", b"stale", ttl=30, lease=first)
    with pytest.raises(CacheConflictError):
        await atomic.compare_and_swap(
            owner,
            "value",
            live.revision,
            b"stale",
            ttl=30,
            lease=first,
        )
    with pytest.raises(CacheConflictError):
        await atomic.compare_and_delete(
            owner,
            "value",
            live.revision,
            lease=first,
        )
    with pytest.raises(CacheConflictError):
        await streams.append(owner, "events", b"stale", lease=first)

    current = await atomic.get(owner, "value")
    assert current is not None
    assert current.value == b"live"
    assert await atomic.get(owner, "stale-create") is None
    assert [record.payload for record in await streams.read(owner, "events")] == [
        b"live"
    ]


async def assert_work_queue_conformance(
    feature: WorkQueueCacheCapability,
    advance: AdvanceClock,
    *,
    owner: str = "conformance-queue",
) -> None:
    identity = await feature.publish(owner, "visibility", b"payload", max_attempts=3)
    first = await feature.reserve(
        owner,
        "visibility",
        "worker-1",
        visibility_timeout=5,
    )
    assert first is not None
    assert first.identity == identity

    renewed = await feature.renew(first, visibility_timeout=10)
    assert renewed.identity == first.identity
    assert renewed.attempt == first.attempt
    assert renewed.receipt == first.receipt
    assert renewed.visible_until > first.visible_until

    await advance(5)
    assert (
        await feature.reserve(
            owner,
            "visibility",
            "worker-2",
            visibility_timeout=5,
        )
        is None
    )
    await advance(5)
    conditionally_renewed = await feature.renew(renewed, visibility_timeout=5)
    assert conditionally_renewed.receipt == renewed.receipt
    assert (
        await feature.reserve(
            owner,
            "visibility",
            "worker-2",
            visibility_timeout=5,
        )
        is None
    )
    await advance(5)
    second = await feature.reserve(
        owner,
        "visibility",
        "worker-2",
        visibility_timeout=5,
    )
    assert second is not None
    assert second.identity == identity
    assert second.attempt == 2
    with pytest.raises(CacheConflictError):
        await feature.acknowledge(conditionally_renewed)
    await feature.reject(second, delay=5)
    assert (
        await feature.reserve(
            owner,
            "visibility",
            "worker-3",
            visibility_timeout=5,
        )
        is None
    )
    await advance(5)
    third = await feature.reserve(
        owner,
        "visibility",
        "worker-3",
        visibility_timeout=5,
    )
    assert third is not None
    assert third.identity == identity
    assert third.attempt == 3
    await feature.acknowledge(third)

    acknowledged_identity = await feature.publish(
        owner,
        "conditional-acknowledgement",
        b"acknowledged",
    )
    acknowledged = await feature.reserve(
        owner,
        "conditional-acknowledgement",
        "worker",
        visibility_timeout=5,
    )
    assert acknowledged is not None
    await advance(5)
    await feature.acknowledge(acknowledged)
    assert (
        await feature.reserve(
            owner,
            "conditional-acknowledgement",
            "worker",
            visibility_timeout=5,
        )
        is None
    )
    assert acknowledged.identity == acknowledged_identity

    retried_identity = await feature.publish(
        owner,
        "conditional-retry",
        b"retried",
    )
    retried = await feature.reserve(
        owner,
        "conditional-retry",
        "worker",
        visibility_timeout=5,
    )
    assert retried is not None
    await advance(5)
    await feature.reject(retried)
    retried_again = await feature.reserve(
        owner,
        "conditional-retry",
        "worker",
        visibility_timeout=5,
    )
    assert retried_again is not None
    assert retried_again.identity == retried_identity
    assert retried_again.attempt == 2
    await feature.acknowledge(retried_again)

    dead_identity = await feature.publish(
        owner,
        "conditional-dead-letter",
        b"dead",
    )
    dead = await feature.reserve(
        owner,
        "conditional-dead-letter",
        "worker",
        visibility_timeout=5,
    )
    assert dead is not None
    await advance(5)
    await feature.dead_letter(dead)
    dead_letters = await feature.dead_letters(owner, "conditional-dead-letter")
    assert [entry.identity for entry in dead_letters] == [dead_identity]

    delayed_identity = await feature.publish(
        owner,
        "delayed",
        b"delayed",
        delay=5,
    )
    assert (
        await feature.reserve(
            owner,
            "delayed",
            "worker",
            visibility_timeout=5,
        )
        is None
    )
    await advance(5)
    delayed = await feature.reserve(
        owner,
        "delayed",
        "worker",
        visibility_timeout=5,
    )
    assert delayed is not None
    assert delayed.identity == delayed_identity
    await feature.acknowledge(delayed)

    terminal_identity = await feature.publish(owner, "terminal", b"terminal")
    terminal = await feature.reserve(
        owner,
        "terminal",
        "worker",
        visibility_timeout=5,
    )
    assert terminal is not None
    await feature.dead_letter(terminal)
    assert (
        await feature.reserve(
            owner,
            "terminal",
            "worker",
            visibility_timeout=5,
        )
        is None
    )
    terminal_dead = await feature.dead_letters(owner, "terminal")
    assert [delivery.identity for delivery in terminal_dead] == [terminal_identity]

    first_dead_identity = await feature.publish(owner, "dead-order", b"first")
    first_dead = await feature.reserve(
        owner,
        "dead-order",
        "worker",
        visibility_timeout=5,
    )
    assert first_dead is not None
    await feature.dead_letter(first_dead)
    second_dead_identity = await feature.publish(owner, "dead-order", b"second")
    second_dead = await feature.reserve(
        owner,
        "dead-order",
        "worker",
        visibility_timeout=5,
    )
    assert second_dead is not None
    await feature.dead_letter(second_dead)
    ordered_dead = await feature.dead_letters(owner, "dead-order")
    assert [delivery.identity for delivery in ordered_dead] == [
        first_dead_identity,
        second_dead_identity,
    ]

    exhausted_identity = await feature.publish(
        owner,
        "exhausted",
        b"exhausted",
        max_attempts=1,
    )
    exhausted = await feature.reserve(
        owner,
        "exhausted",
        "worker",
        visibility_timeout=5,
    )
    assert exhausted is not None
    await advance(5)
    assert (
        await feature.reserve(
            owner,
            "exhausted",
            "worker",
            visibility_timeout=5,
        )
        is None
    )
    exhausted_dead = await feature.dead_letters(owner, "exhausted")
    assert [delivery.identity for delivery in exhausted_dead] == [exhausted_identity]


async def assert_stream_conformance(
    feature: StreamCacheCapability,
    *,
    retention_count: int,
    owner: str = "conformance-stream",
) -> None:
    first = await feature.append(owner, "events", b"one")
    second = await feature.append(owner, "events", b"two")
    records = await feature.read(owner, "events")
    assert [record.position for record in records] == [first, second]

    await feature.acknowledge(owner, "events", "projection", first)
    resumed = await feature.read_consumer(owner, "events", "projection")
    assert [record.position for record in resumed] == [second]
    await feature.acknowledge(owner, "events", "projection", second)
    with pytest.raises(CacheConflictError):
        await feature.acknowledge(owner, "events", "projection", first)
    assert await feature.forget_consumer(owner, "events", "missing") is False
    assert await feature.forget_consumer(owner, "events", "projection") is True
    replayed = await feature.read_consumer(owner, "events", "projection")
    assert [record.position for record in replayed] == [first, second]

    retained_positions = [
        await feature.append(owner, "retained", str(index).encode())
        for index in range(retention_count + 2)
    ]
    with pytest.raises(CachePositionExpiredError):
        await feature.read(owner, "retained", after=retained_positions[0])


async def assert_pubsub_conformance(
    feature: PubSubCacheCapability,
    *,
    owner: str = "conformance-pubsub",
) -> None:
    assert await feature.publish(owner, "live", b"offline") == 0
    subscription = await feature.subscribe(owner, "live")
    try:
        assert await feature.publish(owner, "live", b"online") == 1
        assert await subscription.receive(timeout=1) == b"online"
    finally:
        await subscription.close()
    assert await feature.publish(owner, "live", b"after") == 0


async def assert_schedule_conformance(
    feature: ScheduleCacheCapability,
    now: float,
    advance: AdvanceClock,
    *,
    owner: str = "conformance-schedule",
    claim_ttl: float = 5,
    recurring_interval: float = 10,
    recurring_advance: float = 35,
    due_tolerance: float = 0,
) -> None:
    current_time = now
    record = await feature.create(
        owner,
        "once",
        b"payload",
        next_due_at=current_time,
    )
    assert record is not None
    updated = await feature.update(
        owner,
        "once",
        record.revision,
        b"updated",
        next_due_at=current_time,
    )
    assert updated is not None
    assert (
        await feature.update(
            owner,
            "once",
            record.revision,
            b"stale",
            next_due_at=current_time,
        )
        is None
    )
    assert await feature.due(owner, before=current_time) == (updated,)

    following = await feature.create(
        owner,
        "zz-following",
        b"following",
        next_due_at=current_time,
    )
    assert following is not None
    first_page = await feature.due(owner, before=current_time, limit=1)
    assert first_page == (updated,)
    assert await feature.due(
        owner,
        before=current_time,
        limit=1,
        after=ScheduleCursor(updated.next_due_at, updated.identity),
    ) == (following,)
    assert await feature.delete(owner, following.identity)

    claim = await feature.claim(owner, "once", "scheduler", ttl=claim_ttl)
    assert claim is not None
    assert (
        await feature.update(
            owner,
            "once",
            updated.revision,
            b"claimed",
            next_due_at=current_time,
        )
        is None
    )
    assert await feature.claim(owner, "once", "other", ttl=5) is None
    await feature.release(claim)

    released = await feature.update(
        owner,
        "once",
        updated.revision,
        b"released",
        next_due_at=current_time,
    )
    assert released is not None
    first = await feature.claim(owner, "once", "scheduler-1", ttl=claim_ttl)
    assert first is not None
    await advance(claim_ttl)
    current_time += claim_ttl
    second = await feature.claim(owner, "once", "scheduler-2", ttl=claim_ttl)
    assert second is not None
    assert second.fencing_token > first.fencing_token
    with pytest.raises(CacheConflictError):
        await feature.release(first)
    assert await feature.complete(second) is None

    advanceable = await feature.create(
        owner,
        "advanceable",
        b"original",
        next_due_at=current_time,
        interval_seconds=recurring_interval,
    )
    assert advanceable is not None
    advance_claim = await feature.claim(owner, "advanceable", "scheduler", ttl=60)
    assert advance_claim is not None
    assert await feature.held(advance_claim)
    advanced_record = await feature.advance(
        advance_claim,
        b"advanced",
        next_due_at=current_time + recurring_interval,
    )
    assert advanced_record.payload == b"advanced"
    assert advanced_record.interval_seconds == recurring_interval
    assert advanced_record.revision > advanceable.revision
    assert not await feature.held(advance_claim)
    assert await feature.delete(owner, "advanceable")
    assert not await feature.held(advance_claim)
    assert not await feature.delete(owner, "advanceable")

    discardable = await feature.create(
        owner,
        "discardable",
        b"discardable",
        next_due_at=current_time,
        interval_seconds=recurring_interval,
    )
    assert discardable is not None
    discard_claim = await feature.claim(owner, "discardable", "scheduler", ttl=60)
    assert discard_claim is not None
    await feature.discard(discard_claim)
    assert not await feature.held(discard_claim)
    assert await feature.due(owner, before=current_time) == ()
    with pytest.raises(CacheConflictError):
        await feature.discard(discard_claim)

    recurring = await feature.create(
        owner,
        "recurring",
        b"recurring",
        next_due_at=current_time,
        interval_seconds=recurring_interval,
    )
    assert recurring is not None
    recurring_claim = await feature.claim(
        owner,
        "recurring",
        "scheduler",
        ttl=60,
    )
    assert recurring_claim is not None
    await advance(recurring_advance)
    current_time += recurring_advance
    advanced = await feature.complete(recurring_claim)
    assert advanced is not None
    expected_offset = recurring_interval - math.fmod(
        recurring_advance,
        recurring_interval,
    )
    assert math.isclose(
        advanced.next_due_at,
        current_time + expected_offset,
        abs_tol=due_tolerance,
    )


__all__ = (
    "AdvanceClock",
    "assert_atomic_conformance",
    "assert_lease_conformance",
    "assert_lease_fenced_mutation_conformance",
    "assert_pubsub_conformance",
    "assert_schedule_conformance",
    "assert_stream_conformance",
    "assert_work_queue_conformance",
)
