from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable

import pytest

from wybra.cache import (
    AtomicCacheCapability,
    CacheConflictError,
    CachePositionExpiredError,
    LeaseCacheCapability,
    PubSubCacheCapability,
    ScheduleCacheCapability,
    StreamCacheCapability,
    WorkQueueCacheCapability,
)

type AdvanceClock = Callable[[float], Awaitable[None]]


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

    claim = await feature.claim(owner, "once", "scheduler", ttl=5)
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
    first = await feature.claim(owner, "once", "scheduler-1", ttl=5)
    assert first is not None
    await advance(5)
    current_time += 5
    second = await feature.claim(owner, "once", "scheduler-2", ttl=5)
    assert second is not None
    assert second.fencing_token > first.fencing_token
    with pytest.raises(CacheConflictError):
        await feature.release(first)
    assert await feature.complete(second) is None

    recurring = await feature.create(
        owner,
        "recurring",
        b"recurring",
        next_due_at=current_time,
        interval_seconds=10,
    )
    assert recurring is not None
    recurring_claim = await feature.claim(
        owner,
        "recurring",
        "scheduler",
        ttl=60,
    )
    assert recurring_claim is not None
    await advance(35)
    current_time += 35
    advanced = await feature.complete(recurring_claim)
    assert advanced is not None
    assert advanced.next_due_at == current_time + 5


__all__ = (
    "AdvanceClock",
    "assert_atomic_conformance",
    "assert_lease_conformance",
    "assert_pubsub_conformance",
    "assert_schedule_conformance",
    "assert_stream_conformance",
    "assert_work_queue_conformance",
)
