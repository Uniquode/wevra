from __future__ import annotations

import time
from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from taskiq import ScheduledTask
from testcontainers.community.redis import RedisContainer

from wybra.cache import CacheConflictError, ScheduleCursor
from wybra.cache.redis_runtime import RedisCacheRuntime
from wybra.cache.redis_schedule_ordering import encode_due_order
from wybra.cache.redis_schedule_scripts import SCHEDULE_DUE_ORDER_FUNCTION
from wybra.cache.redis_schedules import RedisScheduleCache
from wybra.tasks.taskiq_schedule import CacheTaskiqScheduleSource, TaskiqSchedulePolicy


@pytest.mark.anyio
async def test_redis_schedule_advance_and_deletion_fence_live_claims() -> None:
    with RedisContainer("redis:7-alpine") as container:
        runtime = RedisCacheRuntime(
            f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}/0",
            namespace=f"schedule-test-{uuid4().hex}",
        )
        schedules = RedisScheduleCache(runtime)
        now = time.time()
        try:
            record = await schedules.create(
                "tasks",
                "hourly",
                b"original",
                next_due_at=now - 1,
                interval_seconds=60,
            )
            assert record is not None
            claim = await schedules.claim("tasks", "hourly", "scheduler", ttl=30)
            assert claim is not None

            advanced = await schedules.advance(
                claim,
                b"advanced",
                next_due_at=now + 60,
            )
            assert advanced.payload == b"advanced"
            assert advanced.interval_seconds == 60
            assert advanced.revision > record.revision
            assert not await schedules.held(claim)

            first_due = await schedules.create(
                "tasks",
                "first-due",
                b"payload",
                next_due_at=now - 1,
            )
            following = await schedules.create(
                "tasks",
                "later-due",
                b"payload",
                next_due_at=now - 1,
            )
            assert first_due is not None
            assert following is not None
            first_page = await schedules.due("tasks", before=now, limit=1)
            assert first_page == (first_due,)
            assert await schedules.due(
                "tasks",
                before=now,
                limit=1,
                after=ScheduleCursor(
                    first_due.next_due_at,
                    first_due.identity,
                ),
            ) == (following,)
            assert await schedules.delete("tasks", first_due.identity)
            assert await schedules.delete("tasks", following.identity)

            deleted = await schedules.create(
                "tasks",
                "deleted",
                b"payload",
                next_due_at=now - 1,
            )
            assert deleted is not None
            deleted_claim = await schedules.claim(
                "tasks", "deleted", "scheduler", ttl=30
            )
            assert deleted_claim is not None
            assert await schedules.held(deleted_claim)
            assert await schedules.delete("tasks", "deleted")
            assert not await schedules.held(deleted_claim)
            with pytest.raises(CacheConflictError, match="claim.*stale"):
                await schedules.complete(deleted_claim)

            discarded = await schedules.create(
                "tasks",
                "discarded",
                b"payload",
                next_due_at=now - 1,
                interval_seconds=60,
            )
            assert discarded is not None
            discarded_claim = await schedules.claim(
                "tasks", "discarded", "scheduler", ttl=30
            )
            assert discarded_claim is not None
            await schedules.discard(discarded_claim)
            assert not await schedules.held(discarded_claim)
            assert await schedules.due("tasks", before=now) == ()
            with pytest.raises(CacheConflictError, match="claim.*stale"):
                await schedules.discard(discarded_claim)
        finally:
            await runtime.close()


@pytest.mark.anyio
async def test_redis_taskiq_sources_fence_one_time_schedule_emission() -> None:
    with RedisContainer("redis:7-alpine") as container:
        runtime = RedisCacheRuntime(
            f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}/0",
            namespace=f"taskiq-schedule-test-{uuid4().hex}",
        )
        schedules = RedisScheduleCache(runtime)
        first_source = CacheTaskiqScheduleSource(
            schedules,
            policy=TaskiqSchedulePolicy(claimant="scheduler-a", claim_ttl_seconds=30),
        )
        second_source = CacheTaskiqScheduleSource(
            schedules,
            policy=TaskiqSchedulePolicy(claimant="scheduler-b", claim_ttl_seconds=30),
        )
        task = ScheduledTask(
            task_name="tests.redis_once",
            labels={},
            args=[],
            kwargs={},
            time=datetime.now(UTC) - timedelta(seconds=1),
            schedule_id="redis-once",
        )
        try:
            await first_source.add_schedule(task)

            first = await first_source.get_schedules()
            assert len(first) == 1
            assert await second_source.get_schedules() == []

            await first_source.post_send(first[0])
            assert await second_source.get_schedules() == []
        finally:
            await runtime.close()


@pytest.mark.anyio
async def test_redis_schedule_cursor_pages_equal_times_and_recurring() -> None:
    with RedisContainer("redis:7-alpine") as container:
        runtime = RedisCacheRuntime(
            f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}/0",
            namespace=f"schedule-cursor-test-{uuid4().hex}",
        )
        schedules = RedisScheduleCache(runtime)
        now = time.time()
        try:
            for index in range(64):
                record = await schedules.create(
                    "tasks",
                    f"schedule-{index:03d}",
                    b"payload",
                    next_due_at=now - 1,
                )
                assert record is not None

            cursor = None
            identities: list[str] = []
            while True:
                page = await schedules.due("tasks", before=now, limit=7, after=cursor)
                if not page:
                    break
                identities.extend(record.identity for record in page)
                cursor = ScheduleCursor(page[-1].next_due_at, page[-1].identity)

            assert identities == [f"schedule-{index:03d}" for index in range(64)]
            for identity in identities:
                assert await schedules.delete("tasks", identity)

            recurring = await schedules.create(
                "tasks",
                "recurring",
                b"payload",
                next_due_at=now - 1,
                interval_seconds=60,
            )
            assert recurring is not None
            claim = await schedules.claim(
                "tasks", recurring.identity, "scheduler", ttl=30
            )
            assert claim is not None
            advanced = await schedules.complete(claim)
            assert advanced is not None

            page = await schedules.due(
                "tasks",
                before=advanced.next_due_at,
                limit=1,
                after=ScheduleCursor(now - 2, "schedule-999"),
            )
            assert page == (advanced,)
        finally:
            await runtime.close()


@pytest.mark.anyio
async def test_redis_lua_due_order_matches_the_client_encoder() -> None:
    with RedisContainer("redis:7-alpine") as container:
        runtime = RedisCacheRuntime(
            f"redis://{container.get_container_host_ip()}:{container.get_exposed_port(6379)}/0",
            namespace=f"schedule-order-test-{uuid4().hex}",
        )
        values = (-1e300, -1e-300, -1.25, -0.0, 0.0, 1e-300, 1.25, 1e300)
        script = SCHEDULE_DUE_ORDER_FUNCTION + "\nreturn due_order(tonumber(ARGV[1]))"
        try:
            for value in values:

                async def encode(client, value: float = value) -> object:
                    return await client.eval(script, 0, repr(value))

                result = await runtime.feature_call(encode)
                assert result == encode_due_order(value).encode()
        finally:
            await runtime.close()
