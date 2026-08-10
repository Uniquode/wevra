from __future__ import annotations

import asyncio
import os
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import AsyncExitStack
from datetime import UTC, datetime, timedelta
from hashlib import sha256
from typing import Any
from uuid import uuid4

import nats
import pytest
from nats.js.api import (
    AckPolicy,
    ConsumerConfig,
    RePublish,
    RetentionPolicy,
    StorageType,
    StreamConfig,
)
from taskiq import AckableMessage, ScheduledTask
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from tests.cache_feature_conformance import (
    CONFORMANCE_TIMEOUT_SECONDS,
    assert_atomic_conformance,
    assert_baseline_cache_conformance,
    assert_lease_conformance,
    assert_pubsub_conformance,
    assert_schedule_conformance,
    assert_stream_conformance,
    assert_work_queue_conformance,
)
from tests_support.database_containers import skip_if_docker_unavailable

import wybra.cache.nats_coordination as nats_coordination
from wybra.cache import (
    AtomicCacheCapability,
    CacheFeatureUnavailableError,
    CacheSettings,
    CachesSettings,
    CacheTimeCapability,
    LeaseCacheCapability,
    NatsJetStreamCache,
    PubSubCacheCapability,
    ScheduleCacheCapability,
    StreamCacheCapability,
    StreamRecord,
    WorkQueueCacheCapability,
    build_caches,
)
from wybra.cache.feature_models import CacheConflictError, CacheFeatureError
from wybra.cache.nats_coordination import NatsCoordination
from wybra.cache.nats_runtime import NatsJetStreamRuntime
from wybra.cache.nats_schedules import NatsScheduleCache
from wybra.cache.nats_streams import NatsStreamCache
from wybra.core.exceptions import ConfigurationError
from wybra.tasks import RetryPolicy, current_task_context, task
from wybra.tasks.lifecycle import TaskLifecycleKind, TaskState
from wybra.tasks.settings import TasksSettings
from wybra.tasks.taskiq_protocol import (
    DELIVERY_ATTEMPT_LABEL,
    DELIVERY_IDENTITY_LABEL,
)
from wybra.tasks.taskiq_runtime import build_taskiq_capability
from wybra.tasks.taskiq_schedule import CacheTaskiqScheduleSource, TaskiqSchedulePolicy

DEFAULT_NATS_IMAGE = "nats:2.11.12-alpine"
NATS_IMAGE_ENV = "WYBRA_TESTCONTAINERS_NATS_IMAGE"


def _nats_container() -> DockerContainer:
    return (
        DockerContainer(os.environ.get(NATS_IMAGE_ENV, DEFAULT_NATS_IMAGE))
        .with_command("-js")
        .with_exposed_ports(4222)
        .waiting_for(LogMessageWaitStrategy("Server is ready"))
    )


def _nats_url(container: DockerContainer) -> str:
    return (
        f"nats://{container.get_container_host_ip()}:{container.get_exposed_port(4222)}"
    )


def nats_settings(nats_url: str, namespace: str) -> CachesSettings:
    return CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=namespace,
            ),
        )
    )


async def _build_nats_taskiq_runtime(
    cleanup: AsyncExitStack,
    nats_url: str,
    namespace: str,
    settings: TasksSettings,
) -> tuple[Any, Any]:
    caches = await build_caches(nats_settings(nats_url, namespace))
    cleanup.push_async_callback(caches.close)
    capability = build_taskiq_capability(caches.require("default"), settings, None)
    cleanup.push_async_callback(capability.close)
    return capability, caches.require("default")


async def _next_taskiq_delivery(
    listener: AsyncIterator[bytes | AckableMessage],
) -> AckableMessage:
    async with asyncio.timeout(5):
        delivery = await anext(listener)
    assert isinstance(delivery, AckableMessage)
    return delivery


@pytest.fixture(scope="module")
def nats_url() -> Iterator[str]:
    skip_if_docker_unavailable()
    container = _nats_container()
    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"NATS JetStream testcontainer could not start: {exc}")
    try:
        yield _nats_url(container)
    finally:
        container.stop()


@pytest.mark.anyio
async def test_nats_jetstream_cache_round_trips_with_native_ttl(
    nats_url: str,
) -> None:
    first_namespace = f"first_{uuid4().hex}"
    second_namespace = f"second_{uuid4().hex}"
    first_settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=first_namespace,
            ),
        )
    )
    second_settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=second_namespace,
            ),
        )
    )

    async def advance(seconds: float) -> None:
        await asyncio.sleep(seconds + 1.2)

    async with AsyncExitStack() as cleanup:
        first_caches = await build_caches(first_settings)
        cleanup.push_async_callback(first_caches.close)
        second_caches = await build_caches(second_settings)
        cleanup.push_async_callback(second_caches.close)
        first = first_caches.require("default")
        second = second_caches.require("default")

        await assert_baseline_cache_conformance(
            first.values,
            advance,
            owner="nats-baseline",
        )
        await first.values.set("integration", "shared:/ key", b"first", ttl=1)
        await second.values.set("integration", "shared:/ key", b"second", ttl=1)
        await first.values.set(
            "integration",
            "long-lived",
            b"value",
            ttl=14 * 24 * 60 * 60,
        )

        assert await first.values.get("integration", "shared:/ key") == b"first"
        assert await second.values.get("integration", "shared:/ key") == b"second"
        assert await first.values.get("integration", "long-lived") == b"value"
        assert first.features == (
            "atomic",
            "lease",
            "pub-sub",
            "schedule",
            "stream",
            "time",
            "work-queue",
        )
        assert second.features == (
            "atomic",
            "lease",
            "pub-sub",
            "schedule",
            "stream",
            "time",
            "work-queue",
        )

        await first.values.set("integration", "fractional", b"value", ttl=1.1)
        await asyncio.sleep(1.4)
        assert await first.values.get("integration", "fractional") is None

        reconstructed_caches = await build_caches(first_settings)
        cleanup.push_async_callback(reconstructed_caches.close)
        reconstructed = reconstructed_caches.require("default")
        assert await reconstructed.values.get("integration", "long-lived") == b"value"

        await first.values.delete("integration", "shared:/ key")
        assert await first.values.get("integration", "shared:/ key") is None


@pytest.mark.anyio
async def test_nats_jetstream_cache_coordinates_concurrent_fills(
    nats_url: str,
) -> None:
    class ObservedNatsJetStreamCache(NatsJetStreamCache):
        waiting_for_fill: asyncio.Event

        def __post_init__(self) -> None:
            self.waiting_for_fill = asyncio.Event()
            super().__post_init__()

        async def _wait_for_fill(
            self,
            completed: asyncio.Event,
            *,
            timeout: float,
        ) -> None:
            self.waiting_for_fill.set()
            await super()._wait_for_fill(completed, timeout=timeout)

    cache = ObservedNatsJetStreamCache(
        servers=(nats_url,),
        namespace=f"single_flight_{uuid4().hex}",
    )
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
        cache.get_or_set("integration", "single-flight", ttl=60, factory=factory)
    )
    second_fill: asyncio.Task[bytes] | None = None
    try:
        await asyncio.wait_for(
            fill_started.wait(),
            timeout=CONFORMANCE_TIMEOUT_SECONDS,
        )
        second_fill = asyncio.create_task(
            cache.get_or_set("integration", "single-flight", ttl=60, factory=factory)
        )
        await asyncio.wait_for(
            cache.waiting_for_fill.wait(),
            timeout=CONFORMANCE_TIMEOUT_SECONDS,
        )
        assert factory_calls == 1
        release_fill.set()
        assert await asyncio.wait_for(
            asyncio.gather(first_fill, second_fill),
            timeout=CONFORMANCE_TIMEOUT_SECONDS,
        ) == [b"filled", b"filled"]
        assert factory_calls == 1
    finally:
        release_fill.set()
        fills = (first_fill,) if second_fill is None else (first_fill, second_fill)
        for fill in fills:
            if not fill.done():
                fill.cancel()
        await asyncio.gather(*fills, return_exceptions=True)
        await cache.close()


@pytest.mark.anyio
async def test_nats_jetstream_cache_registers_atomic_and_lease_features(
    nats_url: str,
) -> None:
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=f"atomic_{uuid4().hex}",
                features=("atomic", "lease"),
            ),
        )
    )

    async with AsyncExitStack() as cleanup:
        first_caches = await build_caches(settings)
        cleanup.push_async_callback(first_caches.close)
        second_caches = await build_caches(settings)
        cleanup.push_async_callback(second_caches.close)
        first = first_caches.require("default")
        second = second_caches.require("default")
        atomic = first.require(AtomicCacheCapability)
        second_atomic = second.require(AtomicCacheCapability)
        leases = first.require(LeaseCacheCapability)
        second_leases = second.require(LeaseCacheCapability)

        async def advance(seconds: float) -> None:
            await asyncio.sleep(seconds + 1.2)

        await assert_atomic_conformance(atomic)
        await assert_lease_conformance(
            leases,
            advance,
            lease_ttl=1,
            renewed_ttl=1,
        )

        lease = await leases.acquire("integration", "resource", "holder", ttl=30)
        assert lease is not None
        assert (
            await second_leases.acquire("integration", "resource", "other", ttl=30)
            is None
        )
        created = await atomic.create(
            "integration",
            "value",
            b"created",
            ttl=30,
            lease=lease,
        )

        assert created is not None
        assert created.value == b"created"
        assert await second_atomic.get("integration", "value") == created
        assert (
            await second_atomic.create(
                "integration", "value", b"duplicate", ttl=30, lease=lease
            )
            is None
        )

        updated = await second_atomic.compare_and_swap(
            "integration",
            "value",
            created.revision,
            b"updated",
            ttl=30,
            lease=lease,
        )
        assert updated is not None
        assert updated.value == b"updated"
        assert await atomic.compare_and_delete(
            "integration", "value", updated.revision, lease=lease
        )
        assert await second_atomic.get("integration", "value") is None

        counter = await atomic.increment("integration", "counter", amount=2, ttl=30)
        assert counter.value == 2
        assert (
            await second_atomic.increment("integration", "counter", ttl=30)
        ).value == 3
        with pytest.raises(CacheConflictError, match="counter"):
            await atomic.get("integration", "counter")

        short_value = await atomic.create(
            "integration", "short-value", b"value", ttl=0.2
        )
        assert short_value is not None
        short_counter = await atomic.increment("integration", "short-counter", ttl=0.2)
        assert short_counter.value == 1
        await asyncio.sleep(0.3)
        assert await atomic.get("integration", "short-value") is None
        assert (
            await atomic.increment("integration", "short-counter", ttl=30)
        ).value == 1

        renewed = await second_leases.renew(lease, ttl=30)
        assert renewed.fencing_token == lease.fencing_token
        await leases.release(renewed)
        replacement = await second_leases.acquire(
            "integration", "resource", "other", ttl=30
        )
        assert replacement is not None
        assert replacement.fencing_token > renewed.fencing_token
        with pytest.raises(CacheConflictError, match="stale"):
            await leases.renew(renewed, ttl=30)
        with pytest.raises(CacheConflictError, match="conflicts"):
            await atomic.create(
                "integration",
                "stale-value",
                b"stale",
                ttl=30,
                lease=renewed,
            )


@pytest.mark.anyio
async def test_nats_pubsub_delivers_across_registries_and_isolates_namespaces(
    nats_url: str,
) -> None:
    namespace = f"pubsub_{uuid4().hex}"
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=namespace,
                features=("pub-sub",),
            ),
        )
    )
    isolated_settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=f"{namespace}_other",
                features=("pub-sub",),
            ),
        )
    )

    async with AsyncExitStack() as cleanup:
        first_caches = await build_caches(settings)
        cleanup.push_async_callback(first_caches.close)
        second_caches = await build_caches(settings)
        cleanup.push_async_callback(second_caches.close)
        isolated_caches = await build_caches(isolated_settings)
        cleanup.push_async_callback(isolated_caches.close)
        publisher = first_caches.require("default").require(PubSubCacheCapability)
        subscriber = second_caches.require("default").require(PubSubCacheCapability)
        isolated = isolated_caches.require("default").require(PubSubCacheCapability)

        await assert_pubsub_conformance(subscriber, owner="nats-pubsub")
        subscription = await subscriber.subscribe("events", "updates")
        unrelated = await isolated.subscribe("events", "updates")
        try:
            await publisher.publish("events", "updates", b"shared")
            assert (
                await subscription.receive(timeout=CONFORMANCE_TIMEOUT_SECONDS)
                == b"shared"
            )
            with pytest.raises(TimeoutError):
                await unrelated.receive(timeout=0.1)
        finally:
            await subscription.close()
            await unrelated.close()


@pytest.mark.anyio
async def test_nats_cache_time_uses_a_calibrated_server_timestamp(
    nats_url: str,
) -> None:
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=f"time_{uuid4().hex}",
                features=("time",),
            ),
        )
    )
    async with AsyncExitStack() as cleanup:
        caches = await build_caches(settings)
        cleanup.push_async_callback(caches.close)
        cache_time = caches.require("default").require(CacheTimeCapability)

        with pytest.raises(CacheFeatureError, match="has not been calibrated"):
            cache_time.now()
        refreshed = await cache_time.refresh()
        assert abs(refreshed - time.time()) < 5
        assert cache_time.now() >= refreshed


@pytest.mark.anyio
async def test_nats_work_queue_preserves_conditional_delivery_ownership(
    nats_url: str,
) -> None:
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=f"work_queue_{uuid4().hex}",
                features=("work-queue",),
            ),
        )
    )

    async def advance(seconds: float) -> None:
        await asyncio.sleep(seconds + 0.2)

    async with AsyncExitStack() as cleanup:
        first_caches = await build_caches(settings)
        cleanup.push_async_callback(first_caches.close)
        second_caches = await build_caches(settings)
        cleanup.push_async_callback(second_caches.close)
        first = first_caches.require("default").require(WorkQueueCacheCapability)
        second = second_caches.require("default").require(WorkQueueCacheCapability)

        await assert_work_queue_conformance(
            first,
            advance,
            owner="nats-work",
            visibility_timeout=1,
            renewed_visibility_timeout=2,
            retry_delay=1,
        )

        identity = await first.publish("nats-work", "shared", b"payload")
        delivery = await first.reserve(
            "nats-work",
            "shared",
            "worker-1",
            visibility_timeout=0.5,
        )
        assert delivery is not None
        assert delivery.identity == identity
        await advance(0.5)
        replacement = await second.reserve(
            "nats-work",
            "shared",
            "worker-2",
            visibility_timeout=1,
            wait_timeout=2,
        )
        assert replacement is not None
        await first_caches.close()
        await second.acknowledge(replacement)


@pytest.mark.anyio
async def test_nats_work_queue_reserves_and_acknowledges_work(
    nats_url: str,
) -> None:
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=f"work_queue_basic_{uuid4().hex}",
                features=("work-queue",),
            ),
        )
    )
    async with AsyncExitStack() as cleanup:
        caches = await build_caches(settings)
        cleanup.push_async_callback(caches.close)
        queue = caches.require("default").require(WorkQueueCacheCapability)
        identity = await queue.publish("nats-work", "jobs", b"payload")
        delivery = await queue.reserve(
            "nats-work",
            "jobs",
            "worker",
            visibility_timeout=1,
            wait_timeout=1,
        )
        assert delivery is not None
        assert delivery.identity == identity
        assert delivery.payload == b"payload"
        await queue.acknowledge(delivery)
        assert (
            await queue.reserve(
                "nats-work",
                "jobs",
                "worker",
                visibility_timeout=1,
            )
            is None
        )

        exhausted = await queue.publish(
            "nats-work",
            "jobs",
            b"terminal",
            max_attempts=1,
        )
        terminal = await queue.reserve(
            "nats-work",
            "jobs",
            "worker",
            visibility_timeout=1,
            wait_timeout=1,
        )
        assert terminal is not None
        assert terminal.identity == exhausted
        await queue.reject(terminal)
        assert [
            entry.identity for entry in await queue.dead_letters("nats-work", "jobs")
        ] == [exhausted]


@pytest.mark.anyio
async def test_nats_work_queue_finds_ready_work_after_many_delayed_items(
    nats_url: str,
) -> None:
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=f"work_queue_ready_tail_{uuid4().hex}",
                features=("work-queue",),
            ),
        )
    )
    async with AsyncExitStack() as cleanup:
        caches = await build_caches(settings)
        cleanup.push_async_callback(caches.close)
        queue = caches.require("default").require(WorkQueueCacheCapability)
        for _ in range(17):
            await queue.publish("nats-work", "jobs", b"delayed", delay=60)
        identity = await queue.publish("nats-work", "jobs", b"ready")

        delivery = await queue.reserve(
            "nats-work",
            "jobs",
            "worker",
            visibility_timeout=1,
        )

        assert delivery is not None
        assert delivery.identity == identity
        await queue.acknowledge(delivery)


@pytest.mark.anyio
async def test_nats_work_queue_recovers_abandoned_delivery_after_registry_close(
    nats_url: str,
) -> None:
    namespace = f"work_queue_recovery_{uuid4().hex}"
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=namespace,
                features=("work-queue",),
            ),
        )
    )
    first_caches = await build_caches(settings)
    try:
        first = first_caches.require("default").require(WorkQueueCacheCapability)
        identity = await first.publish("nats-work", "recover", b"payload")
        issued = await first.reserve(
            "nats-work",
            "recover",
            "first-worker",
            visibility_timeout=1,
            wait_timeout=1,
        )
        assert issued is not None
        await first_caches.close()

        recovered_caches = await build_caches(settings)
        try:
            recovered = recovered_caches.require("default").require(
                WorkQueueCacheCapability
            )
            recovered_at = time.monotonic()
            delivery = await recovered.reserve(
                "nats-work",
                "recover",
                "second-worker",
                visibility_timeout=1,
                wait_timeout=3,
            )
            assert delivery is not None
            assert delivery.identity == identity
            assert delivery.attempt == 1
            assert time.monotonic() - recovered_at < 0.5
            await recovered.acknowledge(delivery)
        finally:
            await recovered_caches.close()
    finally:
        await first_caches.close()


@pytest.mark.anyio
async def test_nats_schedule_store_claims_and_advances_due_records(
    nats_url: str,
) -> None:
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=f"schedule_{uuid4().hex}",
                features=("schedule",),
            ),
        )
    )
    async with AsyncExitStack() as cleanup:
        caches = await build_caches(settings)
        cleanup.push_async_callback(caches.close)
        schedules = caches.require("default").require(ScheduleCacheCapability)
        now = time.time()
        created = await schedules.create(
            "nats-schedule",
            "daily",
            b"payload",
            next_due_at=now - 1,
            interval_seconds=60,
        )
        assert created is not None
        assert await schedules.due("nats-schedule", before=now) == (created,)
        claim = await schedules.claim("nats-schedule", "daily", "scheduler", ttl=10)
        assert claim is not None
        assert await schedules.held(claim)
        advanced = await schedules.advance(
            claim,
            b"updated",
            next_due_at=now + 60,
        )
        assert advanced.payload == b"updated"
        assert not await schedules.held(claim)


@pytest.mark.anyio
async def test_nats_schedule_store_passes_shared_conformance(
    nats_url: str,
) -> None:
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=f"schedule_conformance_{uuid4().hex}",
                features=("schedule",),
            ),
        )
    )

    async def advance(seconds: float) -> None:
        await asyncio.sleep(seconds + 0.05)

    async with AsyncExitStack() as cleanup:
        caches = await build_caches(settings)
        cleanup.push_async_callback(caches.close)
        await assert_schedule_conformance(
            caches.require("default").require(ScheduleCacheCapability),
            time.time() - 1,
            advance,
            owner="nats-schedule-conformance",
            claim_ttl=1,
            recurring_interval=0.5,
            recurring_advance=1.75,
            due_tolerance=0.5,
        )


@pytest.mark.anyio
async def test_nats_schedule_capacity_is_namespace_wide_after_deletion(
    nats_url: str,
) -> None:
    runtime = NatsJetStreamRuntime(
        (nats_url,),
        f"schedule_capacity_{uuid4().hex}",
    )
    coordination = NatsCoordination(runtime)
    schedules = NatsScheduleCache(runtime, coordination, max_records=2)
    try:
        external = await coordination.acquire(
            "schedule-capacity", "records", "external", ttl=30
        )
        assert external is not None
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
        assert await schedules.delete("first-owner", "first")
        assert await schedules.create(
            "third-owner",
            "third",
            b"third",
            next_due_at=time.time(),
        )
        with pytest.raises(CacheFeatureError, match="record capacity"):
            await schedules.create(
                "first-owner",
                "first",
                b"replacement",
                next_due_at=time.time(),
            )
        await coordination.release(external)
    finally:
        await coordination.close()
        await runtime.close()


@pytest.mark.anyio
async def test_nats_schedule_claim_write_failure_releases_its_lease(
    nats_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = NatsJetStreamRuntime((nats_url,), f"schedule_claim_{uuid4().hex}")
    coordination = NatsCoordination(runtime)
    schedules = NatsScheduleCache(runtime, coordination)
    original = NatsScheduleCache._write_state
    failed = False

    async def fail_claim_write(
        self: NatsScheduleCache,
        address: Any,
        expected_sequence: int,
        state: Any,
        *,
        ttl: float | None = None,
    ) -> int | None:
        nonlocal failed
        if self is schedules and state.status == "claimed" and not failed:
            failed = True
            raise CacheFeatureError("simulated claim write failure")
        return await original(self, address, expected_sequence, state, ttl=ttl)

    monkeypatch.setattr(NatsScheduleCache, "_write_state", fail_claim_write)
    try:
        assert await schedules.create(
            "owner", "schedule", b"payload", next_due_at=time.time() - 1
        )
        with pytest.raises(CacheFeatureError, match="simulated"):
            await schedules.claim("owner", "schedule", "scheduler", ttl=30)
        assert await schedules.claim("owner", "schedule", "scheduler", ttl=30)
    finally:
        await coordination.close()
        await runtime.close()


@pytest.mark.anyio
async def test_nats_schedule_settlement_failure_retains_its_claim(
    nats_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = NatsJetStreamRuntime((nats_url,), f"schedule_settlement_{uuid4().hex}")
    coordination = NatsCoordination(runtime)
    schedules = NatsScheduleCache(runtime, coordination)
    original = NatsScheduleCache._write_state

    async def fail_release_write(
        self: NatsScheduleCache,
        address: Any,
        expected_sequence: int,
        state: Any,
        *,
        ttl: float | None = None,
    ) -> int | None:
        if self is schedules and state.status == "live":
            raise CacheFeatureError("simulated settlement write failure")
        return await original(self, address, expected_sequence, state, ttl=ttl)

    try:
        assert await schedules.create(
            "owner", "schedule", b"payload", next_due_at=time.time() - 1
        )
        claim = await schedules.claim("owner", "schedule", "scheduler", ttl=30)
        assert claim is not None
        monkeypatch.setattr(NatsScheduleCache, "_write_state", fail_release_write)

        with pytest.raises(CacheFeatureError, match="simulated"):
            await schedules.release(claim)

        assert await schedules.held(claim)
    finally:
        await coordination.close()
        await runtime.close()


@pytest.mark.anyio
async def test_nats_schedule_capacity_is_atomic_across_owners(
    nats_url: str,
) -> None:
    runtime = NatsJetStreamRuntime(
        (nats_url,),
        f"schedule_capacity_race_{uuid4().hex}",
    )
    first = NatsScheduleCache(runtime, NatsCoordination(runtime), max_records=1)
    second = NatsScheduleCache(runtime, NatsCoordination(runtime), max_records=1)
    try:
        results = await asyncio.gather(
            first.create("first-owner", "first", b"first", next_due_at=time.time()),
            second.create("second-owner", "second", b"second", next_due_at=time.time()),
            return_exceptions=True,
        )
        assert sum(not isinstance(result, BaseException) for result in results) == 1
        assert sum(isinstance(result, CacheFeatureError) for result in results) == 1
    finally:
        await first.coordination.close()
        await second.coordination.close()
        await runtime.close()


@pytest.mark.anyio
async def test_nats_coordinator_replays_a_committed_operation_after_outcome_failure(
    nats_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(nats_coordination, "_REPLY_TIMEOUT_SECONDS", 0.2)
    monkeypatch.setattr(nats_coordination, "_COORDINATOR_MAX_DELIVERY_ATTEMPTS", 2)
    runtime = NatsJetStreamRuntime((nats_url,), f"operation_replay_{uuid4().hex}")
    coordination = NatsCoordination(runtime)
    original = NatsCoordination._write_operation_reply
    failed = False

    async def fail_once(
        self: NatsCoordination,
        operation: bytes,
        reply: tuple[bytes, ...],
    ) -> None:
        nonlocal failed
        if self is coordination and not failed:
            failed = True
            raise CacheFeatureError("outcome write failed")
        await original(self, operation, reply)

    monkeypatch.setattr(NatsCoordination, "_write_operation_reply", fail_once)
    try:
        result = await coordination.increment("owner", "counter", amount=1, ttl=60)
        assert result.value == 1
    finally:
        await coordination.close()
        await runtime.close()


@pytest.mark.anyio
async def test_nats_stream_consumer_capacity_is_reclaimed_after_forget(
    nats_url: str,
) -> None:
    runtime = NatsJetStreamRuntime(
        (nats_url,),
        f"stream_capacity_{uuid4().hex}",
    )
    coordination = NatsCoordination(runtime)
    streams = NatsStreamCache(runtime, coordination, max_consumers=2)
    try:
        position = await streams.append("owner", "events", b"event")
        await streams.acknowledge("owner", "events", "first", position)
        await streams.acknowledge("owner", "events", "second", position)
        with pytest.raises(CacheFeatureError, match="consumer capacity"):
            await streams.acknowledge("owner", "events", "third", position)
        assert await streams.forget_consumer("owner", "events", "first")
        await streams.acknowledge("owner", "events", "third", position)
    finally:
        await coordination.close()
        await runtime.close()


@pytest.mark.anyio
async def test_nats_taskiq_runtime_executes_and_persists_results(
    nats_url: str,
) -> None:
    namespace = f"taskiq_{uuid4().hex}"

    @task(name=f"tests.nats_taskiq_{uuid4().hex}")
    async def operation(value: str) -> str:
        return value.upper()

    async with AsyncExitStack() as cleanup:
        submitter, submitter_cache = await _build_nats_taskiq_runtime(
            cleanup,
            nats_url,
            namespace,
            TasksSettings(backend="taskiq", worker_id="submitter"),
        )
        worker, _worker_cache = await _build_nats_taskiq_runtime(
            cleanup,
            nats_url,
            namespace,
            TasksSettings(backend="taskiq", worker_id="worker"),
        )
        observer, observer_cache = await _build_nats_taskiq_runtime(
            cleanup,
            nats_url,
            namespace,
            TasksSettings(backend="taskiq", worker_id="observer"),
        )
        for capability in (submitter, worker, observer):
            capability.register(operation)
        listener = worker.broker.listen()
        cleanup.push_async_callback(listener.aclose)

        handle = await submitter.submit(operation, operation.payload("complete"))
        await worker.receiver().callback(await _next_taskiq_delivery(listener))

        status = await observer.status(handle.task_id)
        lifecycle = await observer.lifecycle(handle.task_id)
        result = await observer.broker.result_backend.get_result(str(handle.task_id))
        assert status is not None
        assert status.state is TaskState.SUCCEEDED
        assert [event.kind for event in lifecycle] == [
            TaskLifecycleKind.SUBMITTED,
            TaskLifecycleKind.STARTED,
            TaskLifecycleKind.SUCCEEDED,
        ]
        assert result.is_err is False

        projection = await submitter_cache.require(AtomicCacheCapability).get(
            "task-lifecycle",
            f"status:{handle.task_id}",
        )
        assert projection is not None
        assert await submitter_cache.require(AtomicCacheCapability).compare_and_delete(
            "task-lifecycle",
            f"status:{handle.task_id}",
            projection.revision,
        )
        assert (
            await observer_cache.require(AtomicCacheCapability).get(
                "task-lifecycle",
                f"status:{handle.task_id}",
            )
            is None
        )

        recovered, recovered_cache = await _build_nats_taskiq_runtime(
            cleanup,
            nats_url,
            namespace,
            TasksSettings(backend="taskiq", worker_id="recovered"),
        )
        await recovered.broker.startup()
        assert (
            await recovered_cache.require(AtomicCacheCapability).get(
                "task-lifecycle",
                f"status:{handle.task_id}",
            )
            is not None
        )


@pytest.mark.anyio
async def test_nats_taskiq_retry_moves_between_worker_runtimes(
    nats_url: str,
) -> None:
    namespace = f"taskiq_retry_{uuid4().hex}"

    @task(
        name=f"tests.nats_taskiq_retry_{uuid4().hex}", retry=RetryPolicy(max_attempts=2)
    )
    async def operation() -> None:
        context = current_task_context()
        assert context is not None
        if context.attempt == 1:
            raise RuntimeError("retry")

    async with AsyncExitStack() as cleanup:
        submitter, _submitter_cache = await _build_nats_taskiq_runtime(
            cleanup,
            nats_url,
            namespace,
            TasksSettings(backend="taskiq", worker_id="submitter"),
        )
        first_worker, _first_cache = await _build_nats_taskiq_runtime(
            cleanup,
            nats_url,
            namespace,
            TasksSettings(backend="taskiq", worker_id="worker-one"),
        )
        second_worker, _second_cache = await _build_nats_taskiq_runtime(
            cleanup,
            nats_url,
            namespace,
            TasksSettings(backend="taskiq", worker_id="worker-two"),
        )
        for capability in (submitter, first_worker, second_worker):
            capability.register(operation)
        first_listener = first_worker.broker.listen()
        cleanup.push_async_callback(first_listener.aclose)
        second_listener = second_worker.broker.listen()
        cleanup.push_async_callback(second_listener.aclose)

        handle = await submitter.submit(operation, operation.payload())
        await first_worker.receiver().callback(
            await _next_taskiq_delivery(first_listener)
        )
        await second_worker.receiver().callback(
            await _next_taskiq_delivery(second_listener)
        )

        status = await submitter.status(handle.task_id)
        lifecycle = await submitter.lifecycle(handle.task_id)
        assert status is not None
        assert status.state is TaskState.SUCCEEDED
        assert status.attempt == 2
        assert [event.kind for event in lifecycle] == [
            TaskLifecycleKind.SUBMITTED,
            TaskLifecycleKind.STARTED,
            TaskLifecycleKind.ATTEMPT_FAILED,
            TaskLifecycleKind.RETRY_SCHEDULED,
            TaskLifecycleKind.STARTED,
            TaskLifecycleKind.SUCCEEDED,
        ]
        assert [
            event.worker_id
            for event in lifecycle
            if event.kind is TaskLifecycleKind.STARTED
        ] == ["worker-one", "worker-two"]


@pytest.mark.anyio
async def test_nats_taskiq_recovers_delivery_after_worker_loss(nats_url: str) -> None:
    namespace = f"taskiq_recovery_{uuid4().hex}"

    @task(name=f"tests.nats_taskiq_recovery_{uuid4().hex}")
    async def operation() -> None:
        return None

    async with AsyncExitStack() as cleanup:
        submitter, _submitter_cache = await _build_nats_taskiq_runtime(
            cleanup,
            nats_url,
            namespace,
            TasksSettings(backend="taskiq", worker_id="submitter"),
        )
        first_worker, _first_cache = await _build_nats_taskiq_runtime(
            cleanup,
            nats_url,
            namespace,
            TasksSettings(
                backend="taskiq",
                worker_id="worker-one",
                visibility_timeout_seconds=1,
            ),
        )
        second_worker, _second_cache = await _build_nats_taskiq_runtime(
            cleanup,
            nats_url,
            namespace,
            TasksSettings(
                backend="taskiq",
                worker_id="worker-two",
                visibility_timeout_seconds=1,
            ),
        )
        for capability in (submitter, first_worker, second_worker):
            capability.register(operation)
        first_listener = first_worker.broker.listen()
        cleanup.push_async_callback(first_listener.aclose)
        second_listener = second_worker.broker.listen()
        cleanup.push_async_callback(second_listener.aclose)

        handle = await submitter.submit(operation, operation.payload())
        first_delivery = await _next_taskiq_delivery(first_listener)
        second_delivery = await _next_taskiq_delivery(second_listener)

        first_message = first_worker.broker.formatter.loads(message=first_delivery.data)
        second_message = second_worker.broker.formatter.loads(
            message=second_delivery.data
        )
        assert first_message.task_id == second_message.task_id == str(handle.task_id)
        assert [
            first_message.labels[DELIVERY_ATTEMPT_LABEL],
            second_message.labels[DELIVERY_ATTEMPT_LABEL],
        ] == [1, 2]
        assert (
            first_message.labels[DELIVERY_IDENTITY_LABEL]
            == second_message.labels[DELIVERY_IDENTITY_LABEL]
        )
        with pytest.raises(CacheConflictError):
            acknowledgement = first_delivery.ack()
            assert acknowledgement is not None
            await acknowledgement

        await second_worker.receiver().callback(second_delivery)
        status = await submitter.status(handle.task_id)
        assert status is not None
        assert status.state is TaskState.SUCCEEDED


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("features", "missing_feature"),
    (
        (("lease", "stream", "time", "work-queue"), "AtomicCacheCapability"),
        (("atomic", "stream", "time", "work-queue"), "LeaseCacheCapability"),
        (("atomic", "lease", "time", "work-queue"), "StreamCacheCapability"),
        (("atomic", "lease", "stream", "work-queue"), "CacheTimeCapability"),
        (("atomic", "lease", "stream", "time"), "WorkQueueCacheCapability"),
    ),
    ids=("atomic", "lease", "stream", "time", "work-queue"),
)
async def test_nats_taskiq_requires_all_runtime_cache_features(
    nats_url: str,
    features: tuple[str, ...],
    missing_feature: str,
) -> None:
    caches = await build_caches(
        CachesSettings(
            instances=(
                CacheSettings(
                    backend="nats-jetstream",
                    servers=(nats_url,),
                    namespace=f"taskiq_features_{uuid4().hex}",
                    features=features,
                ),
            )
        )
    )
    try:
        with pytest.raises(CacheFeatureUnavailableError, match=missing_feature):
            build_taskiq_capability(
                caches.require("default"),
                TasksSettings(backend="taskiq"),
                None,
            )
    finally:
        await caches.close()


@pytest.mark.anyio
async def test_nats_taskiq_schedule_source_fences_one_time_dispatch(
    nats_url: str,
) -> None:
    namespace = f"taskiq_schedule_{uuid4().hex}"
    async with AsyncExitStack() as cleanup:
        first_caches = await build_caches(nats_settings(nats_url, namespace))
        cleanup.push_async_callback(first_caches.close)
        second_caches = await build_caches(nats_settings(nats_url, namespace))
        cleanup.push_async_callback(second_caches.close)
        first_cache = first_caches.require("default")
        second_cache = second_caches.require("default")
        first = CacheTaskiqScheduleSource(
            first_cache.require(ScheduleCacheCapability),
            policy=TaskiqSchedulePolicy(claimant="scheduler-a", claim_ttl_seconds=30),
            cache_time=first_cache.require(CacheTimeCapability),
        )
        second = CacheTaskiqScheduleSource(
            second_cache.require(ScheduleCacheCapability),
            policy=TaskiqSchedulePolicy(claimant="scheduler-b", claim_ttl_seconds=30),
            cache_time=second_cache.require(CacheTimeCapability),
        )
        scheduled = ScheduledTask(
            task_name="tests.nats_once",
            labels={},
            args=[],
            kwargs={},
            time=datetime.now(UTC) - timedelta(seconds=1),
            schedule_id="nats-once",
        )
        await first.add_schedule(scheduled)

        ready = await first.get_schedules()
        assert len(ready) == 1
        assert await second.get_schedules() == []
        await first.post_send(ready[0])
        assert await second.get_schedules() == []


@pytest.mark.anyio
async def test_nats_streams_preserve_replay_and_durable_consumer_positions(
    nats_url: str,
) -> None:
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=f"streams_{uuid4().hex}",
                features=("stream",),
            ),
        )
    )

    async with AsyncExitStack() as cleanup:
        first_caches = await build_caches(settings)
        cleanup.push_async_callback(first_caches.close)
        second_caches = await build_caches(settings)
        cleanup.push_async_callback(second_caches.close)
        first = first_caches.require("default").require(StreamCacheCapability)
        second = second_caches.require("default").require(StreamCacheCapability)

        await assert_stream_conformance(
            first,
            retention_count=1_000,
            owner="nats-stream",
        )
        first_position = await first.append("events", "shared", b"first")
        second_position = await second.append("events", "shared", b"second")
        assert [record.position for record in await first.read("events", "shared")] == [
            first_position,
            second_position,
        ]
        await first.acknowledge("events", "shared", "projection", first_position)
        assert [
            record.position
            for record in await second.read_consumer("events", "shared", "projection")
        ] == [second_position]


@pytest.mark.parametrize(
    "stream_configuration",
    (
        {"allow_msg_ttl": False},
        {"storage": StorageType.MEMORY},
        {"retention": RetentionPolicy.INTEREST},
        {"max_msgs": 1},
        {"max_bytes": 1_024},
        {"max_msg_size": 1},
        {"max_age": 60},
        {"no_ack": True},
        {"deny_delete": True},
        {"republish": RePublish(src=">", dest="leak.>")},
        {"subject_delete_marker_ttl": 60},
    ),
)
@pytest.mark.anyio
async def test_nats_jetstream_rejects_incompatible_existing_stream(
    nats_url: str,
    stream_configuration: dict[str, object],
) -> None:
    namespace = f"incompatible_{uuid4().hex}"
    stream_name = f"WYBRA_CACHE_{namespace.upper()}"
    client = await nats.connect(nats_url)
    jetstream = client.jetstream()
    try:
        await jetstream.add_stream(
            StreamConfig(
                **(
                    {
                        "name": stream_name,
                        "subjects": [f"wybra.cache.{namespace}.values.>"],
                        "retention": RetentionPolicy.LIMITS,
                        "max_msgs_per_subject": 1,
                        "max_msg_size": -1,
                        "no_ack": False,
                        "allow_direct": True,
                        "allow_msg_ttl": True,
                    }
                    | stream_configuration
                )
            )
        )
        settings = CachesSettings(
            instances=(
                CacheSettings(
                    backend="nats-jetstream",
                    servers=(nats_url,),
                    namespace=namespace,
                ),
            )
        )
        with pytest.raises(
            ConfigurationError, match="stream configuration is incompatible"
        ):
            await build_caches(settings)

        stream = await jetstream.stream_info(stream_name)
        for field_name, expected in stream_configuration.items():
            assert getattr(stream.config, field_name) == expected
    finally:
        await client.close()


@pytest.mark.parametrize("feature", ("stream", "work-queue"))
@pytest.mark.anyio
async def test_nats_jetstream_rejects_unsafe_existing_feature_stream(
    nats_url: str,
    feature: str,
) -> None:
    namespace = f"unsafe_{feature.replace('-', '_')}_{uuid4().hex}"
    owner = "owner"
    resource = "resource"
    digest = sha256(f"{owner}\0{resource}".encode()).hexdigest()
    prefix = f"wybra.cache.{namespace}"
    if feature == "stream":
        name = f"WYBRA_STREAM_{namespace.upper()}_{digest.upper()}"
        subject = f"{prefix}.streams.{digest}"
        configuration = StreamConfig(
            name=name,
            subjects=[subject],
            storage=StorageType.MEMORY,
            retention=RetentionPolicy.LIMITS,
            max_msgs=1_000,
            max_msgs_per_subject=-1,
            max_bytes=-1,
            max_age=0,
            max_msg_size=-1,
            no_ack=False,
            allow_direct=True,
            allow_msg_ttl=False,
        )
    else:
        name = f"WYBRA_WORK_{namespace.upper()}_{digest}"
        subject = f"{prefix}.work.{digest}"
        configuration = StreamConfig(
            name=name,
            subjects=[subject],
            storage=StorageType.MEMORY,
            retention=RetentionPolicy.WORK_QUEUE,
            max_msgs=10_000,
            max_msgs_per_subject=-1,
            max_bytes=-1,
            max_age=0,
            max_msg_size=-1,
            no_ack=False,
            allow_direct=False,
            allow_msg_ttl=False,
        )

    client = await nats.connect(nats_url)
    jetstream = client.jetstream()
    try:
        await jetstream.add_stream(configuration)
        settings = CachesSettings(
            instances=(
                CacheSettings(
                    backend="nats-jetstream",
                    servers=(nats_url,),
                    namespace=namespace,
                    features=(feature,),
                ),
            )
        )
        async with AsyncExitStack() as cleanup:
            caches = await build_caches(settings)
            cleanup.push_async_callback(caches.close)
            with pytest.raises(
                ConfigurationError, match="configuration is incompatible"
            ):
                if feature == "stream":
                    streams = caches.require("default").require(StreamCacheCapability)
                    await streams.append(owner, resource, b"payload")
                else:
                    queue = caches.require("default").require(WorkQueueCacheCapability)
                    await queue.publish(owner, resource, b"payload")

        stream = await jetstream.stream_info(name)
        assert stream.config.storage == StorageType.MEMORY.value
    finally:
        await client.close()


@pytest.mark.anyio
async def test_nats_jetstream_rejects_incompatible_existing_coordinator_consumer(
    nats_url: str,
) -> None:
    namespace = f"unsafe_consumer_{uuid4().hex}"
    runtime = NatsJetStreamRuntime((nats_url,), namespace)
    durable = f"WYBRA_{namespace.upper()}_COORDINATOR"
    try:
        await runtime.ensure_coordination_streams()
        jetstream = await runtime.coordination_jetstream()
        await jetstream.pull_subscribe(
            runtime.coordination_command_subject,
            durable=durable,
            stream=runtime.coordination_command_stream_name,
            config=ConsumerConfig(
                durable_name=durable,
                ack_policy=AckPolicy.EXPLICIT,
                ack_wait=10.0,
                max_deliver=3,
                max_ack_pending=1,
                headers_only=True,
            ),
        )
    finally:
        await runtime.close()

    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(nats_url,),
                namespace=namespace,
                features=("atomic",),
            ),
        )
    )
    with pytest.raises(
        ConfigurationError, match="consumer configuration is incompatible"
    ):
        await build_caches(settings)


@pytest.mark.anyio
async def test_nats_jetstream_requires_a_reachable_jetstream_service() -> None:
    skip_if_docker_unavailable()
    container = (
        DockerContainer(os.environ.get(NATS_IMAGE_ENV, DEFAULT_NATS_IMAGE))
        .with_exposed_ports(4222)
        .waiting_for(LogMessageWaitStrategy("Server is ready"))
    )
    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"NATS testcontainer could not start: {exc}")
    try:
        settings = CachesSettings(
            instances=(
                CacheSettings(
                    backend="nats-jetstream",
                    servers=(_nats_url(container),),
                    namespace=f"unavailable_{uuid4().hex}",
                ),
            )
        )

        with pytest.raises(ConfigurationError, match="cache backend startup failed"):
            await build_caches(settings)
    finally:
        container.stop()


@pytest.mark.anyio
async def test_nats_jetstream_reports_safe_cache_errors_after_an_outage() -> None:
    skip_if_docker_unavailable()
    container = _nats_container()
    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"NATS JetStream testcontainer could not start: {exc}")
    caches = None
    container_stopped = False
    try:
        settings = CachesSettings(
            instances=(
                CacheSettings(
                    backend="nats-jetstream",
                    servers=(_nats_url(container),),
                    namespace=f"outage_{uuid4().hex}",
                ),
            )
        )
        caches = await build_caches(settings)
        cache = caches.require("default").values
        await cache.set("integration", "value", b"before-outage", ttl=60)

        container.stop()
        container_stopped = True

        with pytest.raises(CacheFeatureError, match="cache operation failed"):
            await cache.get("integration", "value")
    finally:
        if caches is not None:
            await caches.close()
        if not container_stopped:
            container.stop()


@pytest.mark.anyio
async def test_nats_jetstream_recovers_durable_cache_features_after_restart() -> None:
    skip_if_docker_unavailable()
    container = _nats_container()
    try:
        container.start()
    except Exception as exc:
        pytest.skip(f"NATS JetStream testcontainer could not start: {exc}")
    namespace = f"restart_{uuid4().hex}"
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="nats-jetstream",
                servers=(_nats_url(container),),
                namespace=namespace,
            ),
        )
    )
    first_caches = None
    recovered_caches = None
    try:
        first_caches = await build_caches(settings)
        first = first_caches.require("default")
        atomic = first.require(AtomicCacheCapability)
        streams = first.require(StreamCacheCapability)
        schedules = first.require(ScheduleCacheCapability)
        queue = first.require(WorkQueueCacheCapability)

        await first.values.set("restart", "value", b"baseline", ttl=60)
        created = await atomic.create("restart", "atomic", b"atomic", ttl=60)
        assert created is not None
        position = await streams.append("restart", "events", b"event")
        schedule = await schedules.create(
            "restart",
            "schedule",
            b"schedule",
            next_due_at=time.time() + 60,
        )
        assert schedule is not None
        identity = await queue.publish("restart", "jobs", b"work")

        await first_caches.close()
        first_caches = None
        wrapped_container = container.get_wrapped_container()
        wrapped_container.stop()
        wrapped_container.start()
        container.reload()

        recovered_settings = CachesSettings(
            instances=(
                CacheSettings(
                    backend="nats-jetstream",
                    servers=(_nats_url(container),),
                    namespace=namespace,
                ),
            )
        )
        recovered_caches = await build_caches(recovered_settings)
        recovered = recovered_caches.require("default")
        assert await recovered.values.get("restart", "value") == b"baseline"
        assert (
            await recovered.require(AtomicCacheCapability).get("restart", "atomic")
            == created
        )
        assert await recovered.require(StreamCacheCapability).read(
            "restart", "events"
        ) == (StreamRecord("events", position, b"event"),)
        assert (
            await recovered.require(ScheduleCacheCapability).due(
                "restart", before=time.time() + 1
            )
            == ()
        )
        delivery = await recovered.require(WorkQueueCacheCapability).reserve(
            "restart", "jobs", "worker", visibility_timeout=1, wait_timeout=2
        )
        assert delivery is not None
        assert delivery.identity == identity
        await recovered.require(WorkQueueCacheCapability).acknowledge(delivery)
    finally:
        if recovered_caches is not None:
            await recovered_caches.close()
        if first_caches is not None:
            await first_caches.close()
        container.stop()
