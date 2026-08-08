from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Awaitable, Callable, Iterator
from typing import Any
from uuid import uuid4

import pytest
import redis.asyncio as redis
from fastapi import FastAPI
from starlette.requests import Request
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import ExecWaitStrategy
from tests.cache_feature_conformance import (
    assert_atomic_conformance,
    assert_lease_conformance,
    assert_pubsub_conformance,
    assert_schedule_conformance,
    assert_stream_conformance,
    assert_work_queue_conformance,
)
from tests_support.database_containers import skip_if_docker_unavailable

from wybra.cache import (
    AtomicCacheCapability,
    CacheConflictError,
    CacheFeatureError,
    CacheRevision,
    CacheSettings,
    CachesSettings,
    LeaseCacheCapability,
    PubSubCacheCapability,
    RedisCache,
    ScheduleCacheCapability,
    StreamCacheCapability,
    StreamPosition,
    WorkQueueCacheCapability,
    build_caches,
)
from wybra.cache.feature_models import DEFAULT_STREAM_RETENTION_COUNT
from wybra.cache.redis_runtime import RedisCacheRuntime
from wybra.cache.redis_schedules import RedisScheduleCache
from wybra.cache.redis_streams import RedisStreamCache
from wybra.config import MappingConfigSource
from wybra.messages import MessagesCapability
from wybra.sessions import NamedCacheSessionStorage, SessionRecord
from wybra.site import start

DEFAULT_REDIS_IMAGE = "redis:8.2-alpine"
REDIS_IMAGE_ENV = "WYBRA_TESTCONTAINERS_REDIS_IMAGE"


def _redis_container() -> DockerContainer:
    return (
        DockerContainer(os.environ.get(REDIS_IMAGE_ENV, DEFAULT_REDIS_IMAGE))
        .with_command("redis-server --appendonly yes --maxmemory-policy noeviction")
        .with_exposed_ports(6379)
        .waiting_for(ExecWaitStrategy(["redis-cli", "ping"]).with_startup_timeout(30))
    )


@pytest.fixture(scope="module")
def redis_url() -> Iterator[str]:
    skip_if_docker_unavailable()
    container = _redis_container()
    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"Redis testcontainer could not start: {exc}")
    try:
        yield (
            f"redis://{container.get_container_host_ip()}:"
            f"{container.get_exposed_port(6379)}/0"
        )
    finally:
        container.stop()


@pytest.fixture
def isolated_redis_container() -> Iterator[tuple[str, DockerContainer]]:
    skip_if_docker_unavailable()
    container = _redis_container()
    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"Redis testcontainer could not start: {exc}")
    try:
        yield (
            f"redis://{container.get_container_host_ip()}:"
            f"{container.get_exposed_port(6379)}/0",
            container,
        )
    finally:
        container.stop()


@pytest.mark.anyio
async def test_redis_cache_round_trips_against_real_redis(redis_url: str) -> None:
    cache = RedisCache(redis_url)

    async def unexpected_factory() -> bytes:
        pytest.fail("A fresh Redis cache value must not run its factory.")

    try:
        await cache.set("integration", "round-trip", b"first", ttl=60)

        assert await cache.get("integration", "round-trip") == b"first"
        assert (
            await cache.get_or_set(
                "integration",
                "round-trip",
                ttl=60,
                factory=unexpected_factory,
            )
            == b"first"
        )
        await cache.delete("integration", "round-trip")
        assert await cache.get("integration", "round-trip") is None
    finally:
        await cache.close()


def redis_settings(
    redis_url: str,
    namespace: str,
    *,
    features: tuple[str, ...] | None = None,
) -> CachesSettings:
    return CachesSettings(
        instances=(
            CacheSettings(
                backend="redis",
                url=redis_url,
                namespace=namespace,
                features=features,
            ),
        )
    )


def request_with_session(session: dict[str, object]) -> Request:
    return Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/target",
            "headers": [],
            "query_string": b"",
            "scheme": "http",
            "server": ("testserver", 80),
            "client": ("testclient", 50000),
            "app": FastAPI(),
            "session": session,
        }
    )


@pytest.mark.anyio
async def test_redis_advanced_features_pass_shared_conformance(
    redis_url: str,
) -> None:
    namespace = f"conformance_{uuid4().hex}"
    caches = await build_caches(redis_settings(redis_url, namespace))
    instance = caches.require("default")

    async def advance(seconds: float) -> None:
        await asyncio.sleep(seconds + 0.05)

    try:
        assert instance.features == (
            "atomic",
            "lease",
            "pub-sub",
            "schedule",
            "stream",
            "time",
            "work-queue",
        )
        await assert_atomic_conformance(
            instance.require(AtomicCacheCapability),
            owner="redis-atomic",
        )
        await assert_lease_conformance(
            instance.require(LeaseCacheCapability),
            advance,
            owner="redis-lease",
            lease_ttl=0.5,
            renewed_ttl=1.0,
        )
        await assert_pubsub_conformance(
            instance.require(PubSubCacheCapability),
            owner="redis-pubsub",
        )
        await assert_schedule_conformance(
            instance.require(ScheduleCacheCapability),
            time.time() - 1,
            advance,
            owner="redis-schedule",
            claim_ttl=0.1,
            recurring_interval=0.5,
            recurring_advance=1.75,
            due_tolerance=0.5,
        )

        async def advance_queue(seconds: float) -> None:
            await asyncio.sleep(seconds + 0.2)

        await assert_work_queue_conformance(
            instance.require(WorkQueueCacheCapability),
            advance_queue,
            owner="redis-queue",
        )
        await assert_stream_conformance(
            instance.require(StreamCacheCapability),
            retention_count=DEFAULT_STREAM_RETENTION_COUNT,
            owner="redis-stream",
        )
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_pubsub_delivers_across_registries_and_isolates_namespaces(
    redis_url: str,
) -> None:
    namespace = f"pubsub_{uuid4().hex}"
    first = await build_caches(redis_settings(redis_url, namespace))
    second = await build_caches(redis_settings(redis_url, namespace))
    isolated = await build_caches(redis_settings(redis_url, f"{namespace}_other"))
    publisher = first.require("default").require(PubSubCacheCapability)
    subscriber = second.require("default").require(PubSubCacheCapability)
    other = isolated.require("default").require(PubSubCacheCapability)
    subscription = await subscriber.subscribe("events", "updates")
    other_owner = await subscriber.subscribe("other-events", "updates")
    other_topic = await subscriber.subscribe("events", "other-updates")
    isolated_subscription = await other.subscribe("events", "updates")
    try:
        assert await publisher.publish("events", "updates", b"shared") == 1
        assert await subscription.receive(timeout=1) == b"shared"
        for unrelated in (other_owner, other_topic, isolated_subscription):
            with pytest.raises(TimeoutError):
                await unrelated.receive(timeout=0.1)
    finally:
        await subscription.close()
        await other_owner.close()
        await other_topic.close()
        await isolated_subscription.close()
        await first.close()
        await second.close()
        await isolated.close()


@pytest.mark.anyio
async def test_redis_pubsub_registry_close_wakes_receive_and_iteration(
    redis_url: str,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"pubsub_close_{uuid4().hex}",
            features=("pub-sub",),
        )
    )
    pubsub = caches.require("default").require(PubSubCacheCapability)
    receiving = await pubsub.subscribe("events", "receive")
    iterating = await pubsub.subscribe("events", "iterate")
    receive_task = asyncio.create_task(receiving.receive())
    iteration_task = asyncio.create_task(anext(iterating))
    await asyncio.sleep(0)

    await caches.close()

    with pytest.raises(CacheFeatureError, match="subscription is closed"):
        await receive_task
    with pytest.raises(StopAsyncIteration):
        await iteration_task


@pytest.mark.anyio
async def test_redis_pubsub_reports_a_safe_error_after_outage(
    isolated_redis_container: tuple[str, DockerContainer],
) -> None:
    redis_url, container = isolated_redis_container
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"pubsub_outage_{uuid4().hex}",
            features=("pub-sub",),
        )
    )
    pubsub = caches.require("default").require(PubSubCacheCapability)
    subscription = await pubsub.subscribe("integration", "outage")
    try:
        container.get_wrapped_container().stop()
        with pytest.raises(
            CacheFeatureError,
            match="Redis cache feature operation failed",
        ):
            await subscription.receive(timeout=1)
        with pytest.raises(
            CacheFeatureError,
            match="Redis cache feature operation failed",
        ):
            await pubsub.publish("integration", "outage", b"payload")
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_schedules_preserve_revisions_and_fenced_claims(
    redis_url: str,
) -> None:
    namespace = f"schedule_{uuid4().hex}"
    settings = redis_settings(redis_url, namespace, features=("schedule",))
    first_caches = await build_caches(settings)
    second_caches = await build_caches(settings)
    first = first_caches.require("default").require(ScheduleCacheCapability)
    second = second_caches.require("default").require(ScheduleCacheCapability)
    try:
        created = await first.create(
            "tasks",
            "once",
            b"first",
            next_due_at=time.time() - 1,
        )
        assert created is not None
        assert (
            await second.update(
                "tasks",
                "once",
                created.revision,
                b"second",
                next_due_at=time.time() - 1,
            )
            is not None
        )
        assert (
            await first.update(
                "tasks",
                "once",
                created.revision,
                b"stale",
                next_due_at=time.time(),
            )
            is None
        )

        claims = await asyncio.gather(
            first.claim("tasks", "once", "scheduler-one", ttl=0.05),
            second.claim("tasks", "once", "scheduler-two", ttl=0.05),
        )
        winner = next(claim for claim in claims if claim is not None)
        assert sum(claim is not None for claim in claims) == 1

        await asyncio.sleep(0.1)
        recovered = await second.claim("tasks", "once", "scheduler-three", ttl=1)
        assert recovered is not None
        assert recovered.fencing_token > winner.fencing_token
        with pytest.raises(CacheConflictError, match="Schedule claim is stale"):
            await first.release(winner)
        assert await second.complete(recovered) is None
        assert await first.due("tasks", before=time.time()) == ()

        recurring = await first.create(
            "tasks",
            "recurring",
            b"recurring",
            next_due_at=time.time() - 1,
            interval_seconds=1,
        )
        assert recurring is not None
        recurring_claim = await first.claim("tasks", "recurring", "scheduler", ttl=1)
        assert recurring_claim is not None
        advanced = await first.complete(recurring_claim)
        assert advanced is not None
        assert advanced.revision > recurring.revision
        assert await first.due(
            "tasks",
            before=advanced.next_due_at,
        ) == (advanced,)
    finally:
        await first_caches.close()
        await second_caches.close()


@pytest.mark.anyio
async def test_redis_schedule_capacity_is_namespace_wide(redis_url: str) -> None:
    runtime = RedisCacheRuntime(redis_url, f"schedule_capacity_{uuid4().hex}")
    schedules = RedisScheduleCache(runtime, max_records=2)
    try:
        assert await schedules.create(
            "first-owner",
            "first",
            b"first",
            next_due_at=time.time(),
        )
        assert await schedules.create(
            "second-owner",
            "second",
            b"second",
            next_due_at=time.time(),
        )
        with pytest.raises(CacheFeatureError, match="record capacity"):
            await schedules.create(
                "third-owner",
                "third",
                b"third",
                next_due_at=time.time(),
            )
    finally:
        await runtime.close()


@pytest.mark.anyio
async def test_redis_schedule_due_recovery_is_bounded(redis_url: str) -> None:
    runtime = RedisCacheRuntime(redis_url, f"schedule_recovery_{uuid4().hex}")
    schedules = RedisScheduleCache(runtime)
    try:
        visible = await schedules.create(
            "tasks",
            "aaa-visible",
            b"visible",
            next_due_at=time.time() - 1,
        )
        assert visible is not None
        for index in range(101):
            identity = f"claimed-{index:03}"
            created = await schedules.create(
                "tasks",
                identity,
                b"claimed",
                next_due_at=time.time() - 1,
            )
            assert created is not None
            assert await schedules.claim("tasks", identity, "scheduler", ttl=0.01)
        await asyncio.sleep(0.05)

        first = await schedules.due("tasks", before=time.time(), limit=1)
        assert [record.identity for record in first] == ["aaa-visible"]
        recovered = await schedules.due("tasks", before=time.time(), limit=100)
        assert len(recovered) == 100
        assert recovered[0].identity == "aaa-visible"
    finally:
        await runtime.close()


@pytest.mark.anyio
async def test_redis_schedule_orders_equal_due_times_by_identity(
    redis_url: str,
) -> None:
    runtime = RedisCacheRuntime(redis_url, f"schedule_order_{uuid4().hex}")
    schedules = RedisScheduleCache(runtime)
    try:
        due_at = time.time() - 1
        assert await schedules.create(
            "tasks",
            "a:",
            b"colon",
            next_due_at=due_at,
        )
        assert await schedules.create(
            "tasks",
            "a0",
            b"zero",
            next_due_at=due_at,
        )
        due = await schedules.due("tasks", before=time.time(), limit=2)
        assert [record.identity for record in due] == ["a0", "a:"]
        assert [
            record.identity
            for record in await schedules.due("tasks", before=time.time(), limit=1)
        ] == ["a0"]
    finally:
        await runtime.close()


@pytest.mark.anyio
async def test_redis_schedule_preserves_large_revisions_and_fencing_tokens(
    redis_url: str,
) -> None:
    runtime = RedisCacheRuntime(redis_url, f"schedule_sequence_{uuid4().hex}")
    schedules = RedisScheduleCache(runtime)
    client = redis.from_url(redis_url, decode_responses=False)
    boundary = 2**53
    try:
        await client.set(runtime.sequence_key("schedule-revision"), boundary - 1)
        created = await schedules.create(
            "tasks",
            "sequence",
            b"created",
            next_due_at=time.time() - 1,
        )
        assert created is not None
        assert created.revision == CacheRevision(boundary)
        updated = await schedules.update(
            "tasks",
            "sequence",
            created.revision,
            b"updated",
            next_due_at=time.time() - 1,
        )
        assert updated is not None
        assert updated.revision == CacheRevision(boundary + 1)
        assert (
            await schedules.update(
                "tasks",
                "sequence",
                created.revision,
                b"stale",
                next_due_at=time.time() - 1,
            )
            is None
        )

        await client.set(runtime.sequence_key("schedule-fencing"), boundary - 1)
        first = await schedules.claim("tasks", "sequence", "first", ttl=1)
        assert first is not None
        assert first.fencing_token.value == boundary
        await schedules.release(first)
        second = await schedules.claim("tasks", "sequence", "second", ttl=1)
        assert second is not None
        assert second.fencing_token.value == boundary + 1
    finally:
        await client.aclose()
        await runtime.close()


@pytest.mark.anyio
async def test_redis_schedule_survives_registry_rebuild_and_namespace_isolation(
    redis_url: str,
) -> None:
    namespace = f"schedule_rebuild_{uuid4().hex}"
    first_settings = redis_settings(redis_url, namespace, features=("schedule",))
    first_caches = await build_caches(first_settings)
    first = first_caches.require("default").require(ScheduleCacheCapability)
    try:
        created = await first.create(
            "tasks",
            "persisted",
            b"payload",
            next_due_at=time.time() - 1,
        )
        assert created is not None
    finally:
        await first_caches.close()

    rebuilt_caches = await build_caches(first_settings)
    isolated_caches = await build_caches(
        redis_settings(
            redis_url,
            f"schedule_isolated_{uuid4().hex}",
            features=("schedule",),
        )
    )
    rebuilt = rebuilt_caches.require("default").require(ScheduleCacheCapability)
    isolated = isolated_caches.require("default").require(ScheduleCacheCapability)
    try:
        assert await rebuilt.due("tasks", before=time.time()) == (created,)
        assert await isolated.due("tasks", before=time.time()) == ()
    finally:
        await rebuilt_caches.close()
        await isolated_caches.close()


@pytest.mark.anyio
async def test_redis_schedule_readiness_isolated_from_application_owners(
    redis_url: str,
) -> None:
    namespace = f"schedule_readiness_{uuid4().hex}"
    settings = redis_settings(redis_url, namespace, features=("schedule",))
    initial_caches = await build_caches(settings)
    initial = initial_caches.require("default").require(ScheduleCacheCapability)
    try:
        created = await initial.create(
            "readiness",
            "application",
            b"payload",
            next_due_at=time.time() - 1,
        )
        assert created is not None
    finally:
        await initial_caches.close()

    first, second = await asyncio.gather(
        build_caches(settings),
        build_caches(settings),
    )
    try:
        schedules = first.require("default").require(ScheduleCacheCapability)
        assert await schedules.due("readiness", before=time.time()) == (created,)
    finally:
        await first.close()
        await second.close()


@pytest.mark.anyio
async def test_redis_schedule_survives_redis_restart(
    isolated_redis_container: tuple[str, DockerContainer],
) -> None:
    redis_url, container = isolated_redis_container
    namespace = f"schedule_restart_{uuid4().hex}"
    settings = redis_settings(redis_url, namespace, features=("schedule",))
    first_caches = await build_caches(settings)
    first = first_caches.require("default").require(ScheduleCacheCapability)
    try:
        created = await first.create(
            "tasks",
            "restart",
            b"payload",
            next_due_at=time.time() - 1,
        )
        assert created is not None
    finally:
        await first_caches.close()

    container.get_wrapped_container().restart()
    restarted_url = (
        f"redis://{container.get_container_host_ip()}:"
        f"{container.get_exposed_port(6379)}/0"
    )
    await _wait_for_redis(restarted_url)
    second_caches = await build_caches(
        redis_settings(restarted_url, namespace, features=("schedule",))
    )
    second = second_caches.require("default").require(ScheduleCacheCapability)
    try:
        assert await second.due("tasks", before=time.time()) == (created,)
    finally:
        await second_caches.close()


@pytest.mark.anyio
async def test_redis_schedule_reports_a_safe_error_after_outage(
    isolated_redis_container: tuple[str, DockerContainer],
) -> None:
    redis_url, container = isolated_redis_container
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"schedule_outage_{uuid4().hex}",
            features=("schedule",),
        )
    )
    schedules = caches.require("default").require(ScheduleCacheCapability)
    try:
        container.get_wrapped_container().stop()
        with pytest.raises(
            CacheFeatureError,
            match="Redis cache feature operation failed",
        ):
            await schedules.due("tasks", before=time.time())
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_round_trips_against_real_redis(
    redis_url: str,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        identity = await queue.publish("integration", "work", b"payload")
        delivery = await queue.reserve(
            "integration",
            "work",
            "worker",
            visibility_timeout=1,
        )
        assert delivery is not None
        assert delivery.identity == identity
        assert delivery.payload == b"payload"
        await queue.acknowledge(delivery)
        assert (
            await queue.reserve(
                "integration",
                "work",
                "worker",
                visibility_timeout=1,
            )
            is None
        )
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_cleans_consumers_after_settlement(
    redis_url: str,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_consumer_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        for visibility_timeout in (0.1, 0.2):
            await queue.publish("integration", "consumer", b"payload")
            delivery = await queue.reserve(
                "integration",
                "consumer",
                "worker",
                visibility_timeout=visibility_timeout,
            )
            assert delivery is not None
            await queue.acknowledge(delivery)

        keys = queue._keys("integration", "consumer")
        consumers = await queue.runtime.client().xinfo_consumers(keys.stream, "wybra")
        assert consumers == []
        assert await queue.runtime.client().hlen(keys.consumers) == 0

        for _index in range(3):
            assert (
                await queue.reserve(
                    "integration",
                    "consumer",
                    "worker",
                    visibility_timeout=1,
                )
                is None
            )
        assert await queue.runtime.client().hlen(keys.consumers) == 0
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_cleans_expired_consumer_metadata(
    redis_url: str,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_expired_consumer_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        await queue.publish("integration", "consumer", b"payload")
        first = await queue.reserve(
            "integration",
            "consumer",
            "worker",
            visibility_timeout=0.05,
        )
        assert first is not None
        keys = queue._keys("integration", "consumer")
        assert await queue.runtime.client().hlen(keys.consumers) == 1

        await asyncio.sleep(0.1)
        second = await queue.reserve(
            "integration",
            "consumer",
            "worker",
            visibility_timeout=1,
        )

        assert second is not None
        await queue.acknowledge(second)
        assert await queue.runtime.client().hlen(keys.consumers) == 0
        assert await queue.runtime.client().xinfo_consumers(keys.stream, "wybra") == []
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_is_shared_by_independent_runtimes(
    redis_url: str,
) -> None:
    settings = redis_settings(
        redis_url,
        f"shared_queue_{uuid4().hex}",
        features=("work-queue",),
    )
    first = await build_caches(settings)
    second = await build_caches(settings)
    first_queue = first.require("default").require(WorkQueueCacheCapability)
    second_queue = second.require("default").require(WorkQueueCacheCapability)
    try:
        identity = await first_queue.publish("integration", "shared", b"payload")
        first_delivery, second_delivery = await asyncio.gather(
            first_queue.reserve(
                "integration",
                "shared",
                "worker-one",
                visibility_timeout=1,
            ),
            second_queue.reserve(
                "integration",
                "shared",
                "worker-two",
                visibility_timeout=1,
            ),
        )
        deliveries = [
            delivery for delivery in (first_delivery, second_delivery) if delivery
        ]
        assert len(deliveries) == 1
        assert deliveries[0].identity == identity
        if first_delivery is not None:
            await first_queue.acknowledge(first_delivery)
        if second_delivery is not None:
            await second_queue.acknowledge(second_delivery)
    finally:
        await first.close()
        await second.close()


@pytest.mark.anyio
async def test_redis_work_queue_waits_for_delayed_work(redis_url: str) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_delayed_wait_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        identity = await queue.publish("integration", "delayed", b"payload", delay=0.1)
        started = time.monotonic()
        delivery = await queue.reserve(
            "integration",
            "delayed",
            "worker",
            visibility_timeout=1,
            wait_timeout=0.5,
        )
        assert delivery is not None
        assert delivery.identity == identity
        assert time.monotonic() - started < 0.4
        await queue.acknowledge(delivery)
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_non_blocking_reserve_skips_legacy_wake_records(
    redis_url: str,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_wake_poll_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        await queue.publish("integration", "wake", b"delayed", delay=60)
        keys = queue._keys("integration", "wake")
        await queue.runtime.client().xadd(keys.stream, {"w": "legacy"})
        identity = await queue.publish("integration", "wake", b"ready")

        assert (
            await queue.reserve(
                "integration",
                "wake",
                "worker",
                visibility_timeout=1,
            )
            is None
        )
        delivery = await queue.reserve(
            "integration",
            "wake",
            "worker",
            visibility_timeout=1,
        )

        assert delivery is not None
        assert delivery.identity == identity
        await queue.acknowledge(delivery)
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_non_blocking_reserve_bounds_legacy_wake_cleanup(
    redis_url: str,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_wake_bound_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        keys = queue._keys("integration", "wake")
        for _index in range(3):
            await queue.runtime.client().xadd(keys.stream, {"w": "legacy"})

        assert (
            await queue.reserve(
                "integration",
                "wake",
                "worker",
                visibility_timeout=1,
            )
            is None
        )
        assert await queue.runtime.client().xlen(keys.stream) == 2
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_defers_work_without_wake_records(
    redis_url: str,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_wake_coalesce_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        for _index in range(20):
            await queue.publish("integration", "wake", b"delayed", delay=60)

        keys = queue._keys("integration", "wake")
        assert await queue.runtime.client().xlen(keys.stream) == 0
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_uses_a_bounded_delay_signal_stream(
    redis_url: str,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_delay_signal_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        identity = await queue.publish(
            "integration",
            "signal",
            b"payload",
            delay=0.1,
        )
        keys = queue._keys("integration", "signal")
        assert await queue.runtime.client().xlen(keys.stream) == 0
        assert await queue.runtime.client().xlen(keys.delay_signals) == 1

        delivery = await queue.reserve(
            "integration",
            "signal",
            "worker",
            visibility_timeout=1,
            wait_timeout=0.5,
        )

        assert delivery is not None
        assert delivery.identity == identity
        await queue.acknowledge(delivery)
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_multiplexes_delayed_queue_promoters(
    redis_url: str,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_delay_multiplex_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        client = queue.runtime.client()
        baseline_connections = len(await client.client_list())
        for index in range(5):
            await queue.publish(
                "integration",
                f"signal-{index}",
                b"payload",
                delay=60,
            )
        await asyncio.sleep(0.05)

        assert len(await client.client_list()) <= baseline_connections + 1
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_same_name_waiters_share_wait_timeout(
    redis_url: str,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_waiters_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        started = time.monotonic()
        deliveries = await asyncio.gather(
            *(
                queue.reserve(
                    "integration",
                    "waiters",
                    "worker",
                    visibility_timeout=1,
                    wait_timeout=0.2,
                )
                for _index in range(3)
            )
        )

        assert deliveries == [None, None, None]
        assert time.monotonic() - started < 0.45
        keys = queue._keys("integration", "waiters")
        assert await queue.runtime.client().xinfo_consumers(keys.stream, "wybra") == []
    finally:
        await caches.close()


@pytest.mark.anyio
@pytest.mark.parametrize("delay", [0, 0.1])
async def test_redis_work_queue_reject_wakes_a_blocked_reserver(
    redis_url: str,
    delay: float,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_reject_wake_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        identity = await queue.publish("integration", "reject", b"payload")
        initial = await queue.reserve(
            "integration",
            "reject",
            "first-worker",
            visibility_timeout=10,
        )
        assert initial is not None
        waiting = asyncio.create_task(
            queue.reserve(
                "integration",
                "reject",
                "second-worker",
                visibility_timeout=1,
                wait_timeout=1,
            )
        )
        await asyncio.sleep(0.05)
        await queue.reject(initial, delay=delay)

        retried = await waiting

        assert retried is not None
        assert retried.identity == identity
        assert retried.attempt == 2
        await queue.acknowledge(retried)
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_recovers_unissued_delivery_with_stale_visibility(
    redis_url: str,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_stale_visibility_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        await queue.publish("integration", "stale", b"discarded")
        initial = await queue.reserve(
            "integration",
            "stale",
            "initial-worker",
            visibility_timeout=10,
        )
        assert initial is not None
        keys = queue._keys("integration", "stale")
        client = queue.runtime.client()
        await client.delete(keys.stream)
        await client.xgroup_create(keys.stream, "wybra", id="0-0", mkstream=True)

        identity = await queue.publish("integration", "stale", b"payload")
        records = await client.xreadgroup(
            "wybra",
            "interrupted-worker",
            {keys.stream: ">"},
            count=1,
        )
        assert records
        await asyncio.sleep(0.1)

        recovered = await queue.reserve(
            "integration",
            "stale",
            "recovery-worker",
            visibility_timeout=0.05,
            wait_timeout=0.2,
        )

        assert recovered is not None
        assert recovered.identity == identity
        assert recovered.attempt == 1
        await queue.acknowledge(recovered)
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_reserver_promotes_cross_instance_delayed_work(
    redis_url: str,
) -> None:
    settings = redis_settings(
        redis_url,
        f"queue_cross_instance_promoter_{uuid4().hex}",
        features=("work-queue",),
    )
    publisher_caches = await build_caches(settings)
    reserver_caches = await build_caches(settings)
    publisher = publisher_caches.require("default").require(WorkQueueCacheCapability)
    reserver = reserver_caches.require("default").require(WorkQueueCacheCapability)
    try:
        waiting = asyncio.create_task(
            reserver.reserve(
                "integration",
                "cross-instance",
                "worker",
                visibility_timeout=1,
                wait_timeout=3,
            )
        )
        await asyncio.sleep(0.05)
        identity = await publisher.publish(
            "integration",
            "cross-instance",
            b"payload",
            delay=0.1,
        )
        await publisher_caches.close()

        delivery = await waiting

        assert delivery is not None
        assert delivery.identity == identity
        await reserver.acknowledge(delivery)
    finally:
        await publisher_caches.close()
        await reserver_caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_retries_promoter_after_transient_failure(
    monkeypatch: pytest.MonkeyPatch,
    redis_url: str,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_promoter_retry_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    original_promote = type(queue)._promote
    failed = False

    async def fail_promoter_once(instance: Any, keys: Any) -> None:
        nonlocal failed
        task = asyncio.current_task()
        if (
            not failed
            and task is not None
            and task.get_name() == "wybra-cache-promoter"
        ):
            failed = True
            raise CacheFeatureError("transient Redis failure")
        await original_promote(instance, keys)

    monkeypatch.setattr(type(queue), "_promote", fail_promoter_once)
    try:
        identity = await queue.publish(
            "integration",
            "retry-promoter",
            b"payload",
            delay=0.05,
        )

        delivery = await queue.reserve(
            "integration",
            "retry-promoter",
            "worker",
            visibility_timeout=1,
            wait_timeout=2,
        )

        assert failed
        assert delivery is not None
        assert delivery.identity == identity
        await queue.acknowledge(delivery)
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_removes_idle_consumer_on_cache_close(
    redis_url: str,
) -> None:
    settings = redis_settings(
        redis_url,
        f"queue_consumer_cleanup_{uuid4().hex}",
        features=("work-queue",),
    )
    first = await build_caches(settings)
    observer = await build_caches(settings)
    queue = first.require("default").require(WorkQueueCacheCapability)
    observer_queue = observer.require("default").require(WorkQueueCacheCapability)
    try:
        await queue.publish("integration", "cleanup", b"payload")
        delivery = await queue.reserve(
            "integration",
            "cleanup",
            "worker",
            visibility_timeout=1,
        )
        assert delivery is not None
        await queue.acknowledge(delivery)
        keys = queue._keys("integration", "cleanup")

        await first.close()

        assert (
            await observer_queue.runtime.client().xinfo_consumers(
                keys.stream,
                "wybra",
            )
            == []
        )
    finally:
        await observer.close()


@pytest.mark.anyio
async def test_redis_work_queue_waits_for_visibility_recovery(redis_url: str) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_visibility_wait_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        identity = await queue.publish("integration", "visibility", b"payload")
        initial = await queue.reserve(
            "integration",
            "visibility",
            "first-worker",
            visibility_timeout=0.1,
        )
        assert initial is not None
        started = time.monotonic()
        recovered = await queue.reserve(
            "integration",
            "visibility",
            "second-worker",
            visibility_timeout=1,
            wait_timeout=0.5,
        )
        assert recovered is not None
        assert recovered.identity == identity
        assert recovered.attempt == 2
        assert time.monotonic() - started < 0.4
        await queue.acknowledge(recovered)
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_preserves_original_visibility_timeout(
    redis_url: str,
) -> None:
    settings = redis_settings(
        redis_url,
        f"queue_visibility_policy_{uuid4().hex}",
        features=("work-queue",),
    )
    first = await build_caches(settings)
    second = await build_caches(settings)
    first_queue = first.require("default").require(WorkQueueCacheCapability)
    second_queue = second.require("default").require(WorkQueueCacheCapability)
    try:
        identity = await first_queue.publish("integration", "visibility", b"payload")
        initial = await first_queue.reserve(
            "integration",
            "visibility",
            "first-worker",
            visibility_timeout=0.5,
        )
        assert initial is not None
        await asyncio.sleep(0.15)
        assert (
            await second_queue.reserve(
                "integration",
                "visibility",
                "second-worker",
                visibility_timeout=0.05,
            )
            is None
        )
        await asyncio.sleep(0.4)
        recovered = await second_queue.reserve(
            "integration",
            "visibility",
            "second-worker",
            visibility_timeout=1,
        )
        assert recovered is not None
        assert recovered.identity == identity
        await second_queue.acknowledge(recovered)
        with pytest.raises(CacheConflictError):
            await first_queue.acknowledge(initial)
        assert not first_queue._deliveries
    finally:
        await first.close()
        await second.close()


@pytest.mark.anyio
async def test_redis_work_queue_does_not_return_a_reclaimed_read(
    monkeypatch: pytest.MonkeyPatch,
    redis_url: str,
) -> None:
    settings = redis_settings(
        redis_url,
        f"queue_reclaim_race_{uuid4().hex}",
        features=("work-queue",),
    )
    first = await build_caches(settings)
    second = await build_caches(settings)
    first_queue = first.require("default").require(WorkQueueCacheCapability)
    second_queue = second.require("default").require(WorkQueueCacheCapability)
    issue_started = asyncio.Event()
    resume_issue = asyncio.Event()
    original_feature_call = type(first_queue.runtime).feature_call

    async def stall_issue(
        runtime: Any,
        operation: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        if (
            runtime is first_queue.runtime
            and getattr(operation, "__name__", "") == "issue"
        ):
            issue_started.set()
            await resume_issue.wait()
        return await original_feature_call(runtime, operation)

    monkeypatch.setattr(type(first_queue.runtime), "feature_call", stall_issue)
    try:
        identity = await first_queue.publish("integration", "race", b"payload")
        pending = asyncio.create_task(
            first_queue.reserve(
                "integration",
                "race",
                "worker",
                visibility_timeout=0.2,
            )
        )
        await issue_started.wait()
        await asyncio.sleep(0.1)
        assert (
            await second_queue.reserve(
                "integration",
                "race",
                "worker",
                visibility_timeout=0.05,
            )
            is None
        )
        await asyncio.sleep(0.15)
        reclaimed = await second_queue.reserve(
            "integration",
            "race",
            "worker",
            visibility_timeout=0.05,
        )
        resume_issue.set()
        assert await pending is None
        assert reclaimed is not None
        assert reclaimed.identity == identity
        await second_queue.acknowledge(reclaimed)
    finally:
        await first.close()
        await second.close()


@pytest.mark.anyio
async def test_redis_work_queue_recreates_a_lost_consumer_group(
    redis_url: str,
) -> None:
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_lost_group_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        first_identity = await queue.publish("integration", "group", b"first")
        first = await queue.reserve(
            "integration",
            "group",
            "worker",
            visibility_timeout=1,
        )
        assert first is not None
        assert first.identity == first_identity
        await queue.acknowledge(first)
        keys = queue._keys("integration", "group")
        await queue.runtime.client().delete(keys.stream)
        second_identity = await queue.publish("integration", "group", b"second")

        second = await queue.reserve(
            "integration",
            "group",
            "worker",
            visibility_timeout=1,
        )

        assert second is not None
        assert second.identity == second_identity
        await queue.acknowledge(second)
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_recovers_unissued_entries_beyond_one_scan_page(
    monkeypatch: pytest.MonkeyPatch,
    redis_url: str,
) -> None:
    settings = redis_settings(
        redis_url,
        f"queue_recovery_cursor_{uuid4().hex}",
        features=("work-queue",),
    )
    first = await build_caches(settings)
    second = await build_caches(settings)
    first_queue = first.require("default").require(WorkQueueCacheCapability)
    second_queue = second.require("default").require(WorkQueueCacheCapability)
    issue_count = 0
    all_issued = asyncio.Event()
    resume_issue = asyncio.Event()
    original_feature_call = type(first_queue.runtime).feature_call

    async def stall_issue(
        runtime: Any,
        operation: Callable[[Any], Awaitable[Any]],
    ) -> Any:
        nonlocal issue_count
        if (
            runtime is first_queue.runtime
            and getattr(operation, "__name__", "") == "issue"
        ):
            issue_count += 1
            if issue_count == 101:
                all_issued.set()
            await resume_issue.wait()
        return await original_feature_call(runtime, operation)

    monkeypatch.setattr(type(first_queue.runtime), "feature_call", stall_issue)
    pending: list[asyncio.Task[object]] = []
    try:
        for _index in range(101):
            await first_queue.publish("integration", "recovery", b"payload")
        pending.extend(
            asyncio.create_task(
                first_queue.reserve(
                    "integration",
                    "recovery",
                    f"long-{index}",
                    visibility_timeout=1,
                )
            )
            for index in range(100)
        )
        while issue_count < 100:
            await asyncio.sleep(0)
        pending.append(
            asyncio.create_task(
                first_queue.reserve(
                    "integration",
                    "recovery",
                    "short",
                    visibility_timeout=0.05,
                )
            )
        )
        await asyncio.wait_for(all_issued.wait(), timeout=5)
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        await asyncio.sleep(0.1)

        recovered = await second_queue.reserve(
            "integration",
            "recovery",
            "recovery-worker",
            visibility_timeout=1,
        )

        assert recovered is not None
        assert recovered.attempt == 1
        await second_queue.acknowledge(recovered)
    finally:
        resume_issue.set()
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        await first.close()
        await second.close()


@pytest.mark.anyio
async def test_redis_work_queue_recovers_pending_delivery_after_restart(
    isolated_redis_container: tuple[str, DockerContainer],
) -> None:
    redis_url, container = isolated_redis_container
    namespace = f"queue_restart_{uuid4().hex}"
    settings = redis_settings(redis_url, namespace, features=("work-queue",))
    first = await build_caches(settings)
    first_queue = first.require("default").require(WorkQueueCacheCapability)
    identity = await first_queue.publish("integration", "restart", b"payload")
    initial = await first_queue.reserve(
        "integration",
        "restart",
        "first-worker",
        visibility_timeout=0.5,
    )
    assert initial is not None
    await first.close()

    container.get_wrapped_container().restart()
    restarted_url = (
        f"redis://{container.get_container_host_ip()}:"
        f"{container.get_exposed_port(6379)}/0"
    )
    await _wait_for_redis(restarted_url)
    second = await build_caches(
        redis_settings(restarted_url, namespace, features=("work-queue",))
    )
    second_queue = second.require("default").require(WorkQueueCacheCapability)
    try:
        await asyncio.sleep(0.7)
        recovered = await second_queue.reserve(
            "integration",
            "restart",
            "second-worker",
            visibility_timeout=1,
        )
        assert recovered is not None
        assert recovered.identity == identity
        assert recovered.attempt == 2
        await second_queue.acknowledge(recovered)
    finally:
        await second.close()


@pytest.mark.anyio
async def test_redis_work_queue_reports_a_safe_error_after_outage(
    isolated_redis_container: tuple[str, DockerContainer],
) -> None:
    redis_url, container = isolated_redis_container
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"queue_outage_{uuid4().hex}",
            features=("work-queue",),
        )
    )
    queue = caches.require("default").require(WorkQueueCacheCapability)
    try:
        container.get_wrapped_container().stop()
        with pytest.raises(
            CacheFeatureError,
            match="Redis cache feature operation failed",
        ):
            await queue.publish("integration", "outage", b"payload")
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_work_queue_named_namespaces_are_isolated(redis_url: str) -> None:
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="redis",
                url=redis_url,
                namespace=f"queue_first_{uuid4().hex}",
                features=("work-queue",),
            ),
            CacheSettings(
                name="second",
                backend="redis",
                url=redis_url,
                namespace=f"queue_second_{uuid4().hex}",
                features=("work-queue",),
            ),
        )
    )
    caches = await build_caches(settings)
    first = caches.require("default").require(WorkQueueCacheCapability)
    second = caches.require("second").require(WorkQueueCacheCapability)
    try:
        identity = await first.publish("integration", "isolated", b"payload")
        assert (
            await second.reserve(
                "integration",
                "isolated",
                "second-worker",
                visibility_timeout=1,
            )
            is None
        )
        delivery = await first.reserve(
            "integration",
            "isolated",
            "first-worker",
            visibility_timeout=1,
        )
        assert delivery is not None
        assert delivery.identity == identity
        await first.acknowledge(delivery)
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_stream_named_namespaces_are_isolated(redis_url: str) -> None:
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="redis",
                url=redis_url,
                namespace=f"stream_first_{uuid4().hex}",
                features=("stream",),
            ),
            CacheSettings(
                name="second",
                backend="redis",
                url=redis_url,
                namespace=f"stream_second_{uuid4().hex}",
                features=("stream",),
            ),
        )
    )
    caches = await build_caches(settings)
    first = caches.require("default").require(StreamCacheCapability)
    second = caches.require("second").require(StreamCacheCapability)
    try:
        position = await first.append("integration", "isolated", b"payload")
        await first.acknowledge("integration", "isolated", "projection", position)
        assert await second.read("integration", "isolated") == ()
        second_position = await second.append("integration", "isolated", b"second")
        records = await first.read("integration", "isolated")
        assert len(records) == 1
        assert records[0].position == position
        assert records[0].payload == b"payload"
        second_records = await second.read_consumer(
            "integration",
            "isolated",
            "projection",
        )
        assert len(second_records) == 1
        assert second_records[0].position == second_position
        assert second_records[0].payload == b"second"
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_stream_consumer_position_survives_registry_rebuild(
    redis_url: str,
) -> None:
    namespace = f"stream_registry_{uuid4().hex}"
    settings = redis_settings(redis_url, namespace, features=("stream",))
    first_caches = await build_caches(settings)
    first = first_caches.require("default").require(StreamCacheCapability)
    try:
        first_position = await first.append("integration", "restart", b"first")
        second_position = await first.append("integration", "restart", b"second")
        await first.acknowledge(
            "integration",
            "restart",
            "projection",
            first_position,
        )
    finally:
        await first_caches.close()

    second_caches = await build_caches(settings)
    second = second_caches.require("default").require(StreamCacheCapability)
    try:
        records = await second.read_consumer("integration", "restart", "projection")
        assert [record.position for record in records] == [second_position]
        third_position = await second.append("integration", "restart", b"third")
        assert third_position > second_position
    finally:
        await second_caches.close()


@pytest.mark.anyio
async def test_redis_stream_bounds_durable_consumer_positions(
    redis_url: str,
) -> None:
    runtime = RedisCacheRuntime(redis_url, f"stream_consumers_{uuid4().hex}")
    streams = RedisStreamCache(runtime, max_consumers=1)
    try:
        await runtime.health_check()
        await runtime.validate_features(frozenset({"stream"}))
        position = await streams.append("integration", "consumers", b"payload")
        await streams.acknowledge(
            "integration",
            "consumers",
            "first",
            position,
        )
        with pytest.raises(CacheFeatureError, match="consumer capacity"):
            await streams.acknowledge(
                "integration",
                "consumers",
                "second",
                position,
            )
        assert await streams.forget_consumer(
            "integration",
            "consumers",
            "first",
        )
        await streams.acknowledge(
            "integration",
            "consumers",
            "second",
            position,
        )
    finally:
        await runtime.close()


@pytest.mark.anyio
async def test_redis_stream_preserves_precise_positions_and_exact_retention(
    redis_url: str,
) -> None:
    runtime = RedisCacheRuntime(redis_url, f"stream_precision_{uuid4().hex}")
    streams = RedisStreamCache(runtime, retention_count=2)
    starting_position = 2**53 - 1
    try:
        await runtime.health_check()
        await runtime.validate_features(frozenset({"stream"}))
        await runtime.client().set(
            runtime.key("stream-sequence", "integration", "precision"),
            starting_position,
        )
        first = await streams.append("integration", "precision", b"first")
        second = await streams.append("integration", "precision", b"second")
        third = await streams.append("integration", "precision", b"third")

        assert [first.value, second.value, third.value] == [
            starting_position + 1,
            starting_position + 2,
            starting_position + 3,
        ]
        records = await streams.read("integration", "precision")
        assert [record.position for record in records] == [second, third]
        await streams.acknowledge("integration", "precision", "projection", second)
        with pytest.raises(CacheConflictError, match="does not exist"):
            await streams.acknowledge(
                "integration",
                "precision",
                "projection",
                StreamPosition(third.value + 1),
            )
        resumed = await streams.read_consumer(
            "integration",
            "precision",
            "projection",
        )
        assert [record.position for record in resumed] == [third]
    finally:
        await runtime.close()


@pytest.mark.anyio
async def test_redis_stream_survives_redis_restart(
    isolated_redis_container: tuple[str, DockerContainer],
) -> None:
    redis_url, container = isolated_redis_container
    namespace = f"stream_restart_{uuid4().hex}"
    settings = redis_settings(redis_url, namespace, features=("stream",))
    first_caches = await build_caches(settings)
    first = first_caches.require("default").require(StreamCacheCapability)
    try:
        first_position = await first.append("integration", "restart", b"first")
        second_position = await first.append("integration", "restart", b"second")
        await first.acknowledge(
            "integration",
            "restart",
            "projection",
            first_position,
        )
    finally:
        await first_caches.close()

    container.get_wrapped_container().restart()
    restarted_url = (
        f"redis://{container.get_container_host_ip()}:"
        f"{container.get_exposed_port(6379)}/0"
    )
    await _wait_for_redis(restarted_url)
    second_caches = await build_caches(
        redis_settings(restarted_url, namespace, features=("stream",))
    )
    second = second_caches.require("default").require(StreamCacheCapability)
    try:
        records = await second.read_consumer("integration", "restart", "projection")
        assert [record.position for record in records] == [second_position]
        third_position = await second.append("integration", "restart", b"third")
        assert third_position > second_position
    finally:
        await second_caches.close()


@pytest.mark.anyio
async def test_redis_stream_reports_a_safe_error_after_outage(
    isolated_redis_container: tuple[str, DockerContainer],
) -> None:
    redis_url, container = isolated_redis_container
    caches = await build_caches(
        redis_settings(
            redis_url,
            f"stream_outage_{uuid4().hex}",
            features=("stream",),
        )
    )
    streams = caches.require("default").require(StreamCacheCapability)
    try:
        container.get_wrapped_container().stop()
        with pytest.raises(
            CacheFeatureError,
            match="Redis cache feature operation failed",
        ):
            await streams.append("integration", "outage", b"payload")
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_revisions_and_fencing_survive_runtime_restart(
    isolated_redis_container: tuple[str, DockerContainer],
) -> None:
    redis_url, container = isolated_redis_container
    namespace = f"restart_{uuid4().hex}"
    settings = redis_settings(redis_url, namespace)
    first_caches = await build_caches(settings)
    first_instance = first_caches.require("default")
    first_atomic = first_instance.require(AtomicCacheCapability)
    first_leases = first_instance.require(LeaseCacheCapability)
    first_value = await first_atomic.create("restart", "value", b"one", ttl=60)
    first_lease = await first_leases.acquire(
        "restart",
        "lease",
        "holder-1",
        ttl=60,
    )
    assert first_value is not None
    assert first_lease is not None
    assert await first_atomic.compare_and_delete(
        "restart",
        "value",
        first_value.revision,
    )
    await first_leases.release(first_lease)
    await first_caches.close()

    container.get_wrapped_container().restart()
    redis_url = (
        f"redis://{container.get_container_host_ip()}:"
        f"{container.get_exposed_port(6379)}/0"
    )
    await _wait_for_redis(redis_url)
    settings = redis_settings(redis_url, namespace)

    second_caches = await build_caches(settings)
    second_instance = second_caches.require("default")
    try:
        second_value = await second_instance.require(AtomicCacheCapability).create(
            "restart", "value", b"two", ttl=60
        )
        second_lease = await second_instance.require(LeaseCacheCapability).acquire(
            "restart",
            "lease",
            "holder-2",
            ttl=60,
        )

        assert second_value is not None
        assert second_lease is not None
        assert second_value.revision > first_value.revision
        assert second_lease.fencing_token > first_lease.fencing_token
    finally:
        await second_caches.close()


async def _wait_for_redis(redis_url: str) -> None:
    client = redis.Redis.from_url(redis_url, decode_responses=False)
    deadline = time.monotonic() + 10
    try:
        while True:
            try:
                if await client.ping():
                    return
            except Exception:
                if time.monotonic() >= deadline:
                    raise
                await asyncio.sleep(0.1)
    finally:
        await client.aclose()


@pytest.mark.anyio
async def test_redis_preserves_exact_large_revisions_and_fencing_tokens(
    redis_url: str,
) -> None:
    namespace = f"exact_sequence_{uuid4().hex}"
    starting_value = 2**53
    client = redis.Redis.from_url(redis_url, decode_responses=False)
    settings = redis_settings(redis_url, namespace)
    caches = await build_caches(settings)
    atomic = caches.require("default").require(AtomicCacheCapability)
    leases = caches.require("default").require(LeaseCacheCapability)
    try:
        await client.set(f"{namespace}:sequence:atomic-revision", starting_value)
        await client.set(f"{namespace}:sequence:lease-fencing", starting_value)

        first_value = await atomic.create("exact", "value", b"first", ttl=60)
        assert first_value is not None
        second_value = await atomic.compare_and_swap(
            "exact",
            "value",
            first_value.revision,
            b"second",
            ttl=60,
        )
        assert second_value is not None
        assert first_value.revision.value == starting_value + 1
        assert second_value.revision.value == starting_value + 2

        await client.hset(
            f"{namespace}:atomic:exact:counter",
            mapping={"type": "counter", "payload": starting_value},
        )
        first_counter = await atomic.increment("exact", "counter", ttl=60)
        second_counter = await atomic.increment("exact", "counter", ttl=60)

        assert first_counter.value == starting_value + 1
        assert second_counter.value == starting_value + 2

        first_lease = await leases.acquire("exact", "lease", "holder", ttl=60)
        assert first_lease is not None
        await leases.release(first_lease)
        second_lease = await leases.acquire("exact", "lease", "holder", ttl=60)
        assert second_lease is not None
        assert first_lease.fencing_token.value == starting_value + 1
        assert second_lease.fencing_token.value == starting_value + 2
    finally:
        await caches.close()
        await client.aclose()


@pytest.mark.anyio
async def test_redis_rejects_oversized_feature_ttls_before_mutating_state(
    redis_url: str,
) -> None:
    namespace = f"ttl_limit_{uuid4().hex}"
    caches = await build_caches(redis_settings(redis_url, namespace))
    atomic = caches.require("default").require(AtomicCacheCapability)
    leases = caches.require("default").require(LeaseCacheCapability)
    try:
        with pytest.raises(ValueError, match="exceeds the Redis TTL limit"):
            await atomic.create("ttl", "value", b"value", ttl=float(2**62))
        with pytest.raises(ValueError, match="exceeds the Redis TTL limit"):
            await leases.acquire("ttl", "lease", "holder", ttl=float(2**62))

        assert await atomic.create("ttl", "value", b"value", ttl=60) is not None
        assert await leases.acquire("ttl", "lease", "holder", ttl=60) is not None
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_cache_shutdown_is_idempotent(redis_url: str) -> None:
    caches = await build_caches(redis_settings(redis_url, f"close_{uuid4().hex}"))

    await caches.close()
    await caches.close()


@pytest.mark.anyio
async def test_redis_cache_reports_a_safe_baseline_error_after_outage(
    isolated_redis_container: tuple[str, DockerContainer],
) -> None:
    redis_url, container = isolated_redis_container
    caches = await build_caches(redis_settings(redis_url, f"outage_{uuid4().hex}"))
    try:
        container.get_wrapped_container().stop()

        with pytest.raises(
            CacheFeatureError,
            match="Redis cache feature operation failed",
        ):
            await caches.require("default").values.get("outage", "value")
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_named_namespaces_are_isolated(redis_url: str) -> None:
    first_namespace = f"first_{uuid4().hex}"
    second_namespace = f"second_{uuid4().hex}"
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="redis",
                url=redis_url,
                namespace=first_namespace,
            ),
            CacheSettings(
                name="second",
                backend="redis",
                url=redis_url,
                namespace=second_namespace,
            ),
        )
    )
    caches = await build_caches(settings)
    first = caches.require("default")
    second = caches.require("second")
    try:
        await first.values.set("isolation", "value", b"first", ttl=60)
        first_atomic = await first.require(AtomicCacheCapability).create(
            "isolation",
            "atomic",
            b"first",
            ttl=60,
        )

        assert first_atomic is not None
        assert await second.values.get("isolation", "value") is None
        assert (
            await second.require(AtomicCacheCapability).get(
                "isolation",
                "atomic",
            )
            is None
        )
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_competing_runtimes_have_one_atomic_winner(
    redis_url: str,
) -> None:
    namespace = f"competing_{uuid4().hex}"
    settings = redis_settings(redis_url, namespace, features=("atomic",))
    first_caches = await build_caches(settings)
    second_caches = await build_caches(settings)
    first = first_caches.require("default").require(AtomicCacheCapability)
    second = second_caches.require("default").require(AtomicCacheCapability)
    try:
        results = await asyncio.gather(
            first.create("competing", "value", b"first", ttl=60),
            second.create("competing", "value", b"second", ttl=60),
        )

        assert len([result for result in results if result is not None]) == 1

        counters = await asyncio.gather(
            *(first.increment("competing", "counter", ttl=60) for _ in range(4)),
            *(second.increment("competing", "counter", ttl=60) for _ in range(4)),
        )
        assert sorted(counter.value for counter in counters) == list(range(1, 9))
    finally:
        await first_caches.close()
        await second_caches.close()


@pytest.mark.anyio
async def test_redis_expired_atomic_value_recreates_with_newer_revision(
    redis_url: str,
) -> None:
    namespace = f"expiry_{uuid4().hex}"
    caches = await build_caches(redis_settings(redis_url, namespace))
    atomic = caches.require("default").require(AtomicCacheCapability)
    try:
        first = await atomic.create("expiry", "value", b"first", ttl=0.05)
        assert first is not None
        await asyncio.sleep(0.1)

        second = await atomic.create("expiry", "value", b"second", ttl=60)

        assert second is not None
        assert second.revision > first.revision
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_redis_competing_runtimes_have_one_lease_holder(
    redis_url: str,
) -> None:
    namespace = f"lease_competing_{uuid4().hex}"
    settings = redis_settings(redis_url, namespace, features=("lease",))
    first_caches = await build_caches(settings)
    second_caches = await build_caches(settings)
    first = first_caches.require("default").require(LeaseCacheCapability)
    second = second_caches.require("default").require(LeaseCacheCapability)
    try:
        leases = await asyncio.gather(
            first.acquire("competing", "lease", "holder-1", ttl=60),
            second.acquire("competing", "lease", "holder-2", ttl=60),
        )

        winners = [lease for lease in leases if lease is not None]
        assert len(winners) == 1
        assert winners[0].expires_at > 0
    finally:
        await first_caches.close()
        await second_caches.close()


@pytest.mark.anyio
async def test_named_redis_atomic_cache_persists_queued_messages(
    redis_url: str,
) -> None:
    namespace = f"messages_{uuid4().hex}"
    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": ("wybra.cache", "wybra.messages")},
                "cache": {},
                "cache.messages": {
                    "backend": "redis",
                    "url": redis_url,
                    "namespace": namespace,
                    "features": ("atomic",),
                },
                "wybra.messages": {
                    "storage_backend": "cache",
                    "cache_name": "messages",
                },
            }
        ),
        environ={},
    )
    session: dict[str, object] = {}
    request = request_with_session(session)
    try:
        messages = site.require_capability(MessagesCapability)

        await messages.success(request, "Stored in Redis")
        alerts = await messages.consume_alerts(request)

        assert [alert.message for alert in alerts] == ["Stored in Redis"]
    finally:
        await site.close()


@pytest.mark.anyio
async def test_named_redis_cache_stores_session_records(redis_url: str) -> None:
    caches = await build_caches(redis_settings(redis_url, f"sessions_{uuid4().hex}"))
    storage = NamedCacheSessionStorage(
        cache=caches.require("default").values,
        key_prefix="integration:",
        payload_max_bytes=1024,
    )
    record = SessionRecord(
        data={"source": "redis"},
        created_at=1.0,
        updated_at=1.0,
        expires_at=60.0,
    )
    try:
        await storage.save("session", record)

        assert await storage.load("session", now=2.0) == record
        await storage.delete("session")
        assert await storage.load("session", now=2.0) is None
    finally:
        await caches.close()
