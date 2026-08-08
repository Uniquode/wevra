from __future__ import annotations

import sys
from asyncio import gather, sleep
from uuid import UUID, uuid4

import pytest
from taskiq import TaskiqMessage, TaskiqResult
from testcontainers.community.redis import RedisContainer

from cache_feature_conformance import assert_lease_fenced_mutation_conformance
from wybra.cache import CacheConflictError
from wybra.cache.redis_atomic import RedisAtomicCache
from wybra.cache.redis_leases import RedisLeaseCache
from wybra.cache.redis_queues import RedisWorkQueue
from wybra.cache.redis_runtime import RedisCacheRuntime
from wybra.cache.redis_streams import RedisStreamCache
from wybra.cache.redis_time import RedisCacheTime
from wybra.tasks.taskiq_lifecycle import (
    CacheTaskiqLifecycleMiddleware,
    CacheTaskLifecycle,
    TaskiqLifecyclePolicy,
)

pytestmark = pytest.mark.skipif(
    sys.platform == "win32",
    reason="Redis Testcontainers require Linux containers.",
)


@pytest.mark.anyio
async def test_redis_fenced_mutations_reject_a_replaced_lease() -> None:
    async def advance_redis_clock(_seconds: float) -> None:
        await sleep(1.1)

    with RedisContainer("redis:7-alpine") as container:
        runtime = RedisCacheRuntime(
            f"redis://{container.get_container_host_ip()}:"
            f"{container.get_exposed_port(6379)}/0",
            namespace=f"lifecycle-test-{uuid4().hex}",
        )
        try:
            await assert_lease_fenced_mutation_conformance(
                RedisAtomicCache(runtime),
                RedisStreamCache(runtime),
                RedisLeaseCache(runtime),
                advance_redis_clock,
                owner="redis-fence",
                lease_ttl=1.0,
            )
        finally:
            await runtime.close()


@pytest.mark.anyio
async def test_redis_work_queue_rejects_settlement_after_visibility_expiry() -> None:
    with RedisContainer("redis:7-alpine") as container:
        runtime = RedisCacheRuntime(
            f"redis://{container.get_container_host_ip()}:"
            f"{container.get_exposed_port(6379)}/0",
            namespace=f"queue-test-{uuid4().hex}",
        )
        queue = RedisWorkQueue(runtime)
        try:
            await queue.publish("tasks", "default", b"payload")
            delivery = await queue.reserve(
                "tasks",
                "default",
                "worker-a",
                visibility_timeout=0.05,
            )
            assert delivery is not None
            await sleep(0.1)

            with pytest.raises(CacheConflictError, match="stale or no longer reserved"):
                await queue.renew(delivery, visibility_timeout=1)
            with pytest.raises(CacheConflictError, match="stale or no longer reserved"):
                await queue.acknowledge(delivery)

            redelivered = await queue.reserve(
                "tasks",
                "default",
                "worker-b",
                visibility_timeout=1,
            )
            assert redelivered is not None
            assert redelivered.attempt == 2
            renewed = await queue.renew(redelivered, visibility_timeout=1)
            await queue.acknowledge(renewed)
        finally:
            await queue.close()
            await runtime.close()


@pytest.mark.anyio
async def test_redis_lifecycle_recovers_active_and_terminal_status() -> None:
    with RedisContainer("redis:7-alpine") as container:
        runtime = RedisCacheRuntime(
            f"redis://{container.get_container_host_ip()}:"
            f"{container.get_exposed_port(6379)}/0",
            namespace=f"lifecycle-test-{uuid4().hex}",
        )
        lifecycle = CacheTaskLifecycle(
            RedisStreamCache(runtime),
            RedisAtomicCache(runtime),
            RedisLeaseCache(runtime),
            TaskiqLifecyclePolicy(
                owner="task-lifecycle",
                stream="events",
                queue="default",
                status_retention_seconds=60,
            ),
            RedisCacheTime(runtime),
        )
        middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
        message = TaskiqMessage(
            task_id=str(uuid4()),
            task_name="example.cleanup",
            labels={},
            args=[],
            kwargs={},
        )
        try:
            await middleware.pre_send(message)
            await middleware.pre_execute(message)

            recovered = CacheTaskLifecycle(
                RedisStreamCache(runtime),
                RedisAtomicCache(runtime),
                RedisLeaseCache(runtime),
                lifecycle.policy,
                RedisCacheTime(runtime),
            )
            await recovered.replay()
            active = await recovered.status(UUID(message.task_id))
            assert active is not None
            assert active.state.value == "running"

            await middleware.post_save(
                message,
                TaskiqResult(is_err=False, return_value=None, execution_time=0.1),
            )
            completed = await recovered.status(UUID(message.task_id))
            assert completed is not None
            assert completed.state.value == "succeeded"
        finally:
            await runtime.close()


@pytest.mark.anyio
async def test_redis_lifecycle_competing_projectors_recover_one_status() -> None:
    with RedisContainer("redis:7-alpine") as container:
        runtime = RedisCacheRuntime(
            f"redis://{container.get_container_host_ip()}:"
            f"{container.get_exposed_port(6379)}/0",
            namespace=f"lifecycle-test-{uuid4().hex}",
        )
        policy = TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            status_retention_seconds=60,
        )
        streams = RedisStreamCache(runtime)
        atomic = RedisAtomicCache(runtime)
        lifecycle = CacheTaskLifecycle(
            streams,
            atomic,
            RedisLeaseCache(runtime),
            policy,
            RedisCacheTime(runtime),
        )
        message = TaskiqMessage(
            task_id=str(uuid4()),
            task_name="example.cleanup",
            labels={},
            args=[],
            kwargs={},
        )
        middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
        try:
            await middleware.pre_send(message)
            await middleware.pre_execute(message)
            snapshot = await atomic.get(policy.owner, f"status:{message.task_id}")
            assert snapshot is not None
            assert await atomic.compare_and_delete(
                policy.owner,
                f"status:{message.task_id}",
                snapshot.revision,
            )
            first = CacheTaskLifecycle(
                RedisStreamCache(runtime),
                RedisAtomicCache(runtime),
                RedisLeaseCache(runtime),
                policy,
                RedisCacheTime(runtime),
            )
            second = CacheTaskLifecycle(
                RedisStreamCache(runtime),
                RedisAtomicCache(runtime),
                RedisLeaseCache(runtime),
                policy,
                RedisCacheTime(runtime),
            )

            await gather(first.replay(), second.replay())

            statuses = await gather(
                first.status(UUID(message.task_id)),
                second.status(UUID(message.task_id)),
            )
            assert all(status is not None for status in statuses)
            assert {
                status.state.value for status in statuses if status is not None
            } == {"running"}
        finally:
            await runtime.close()
