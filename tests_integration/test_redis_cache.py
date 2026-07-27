from __future__ import annotations

import asyncio
import os
import time
from collections.abc import Iterator
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
)
from tests_support.database_containers import skip_if_docker_unavailable

from wybra.cache import (
    AtomicCacheCapability,
    CacheFeatureError,
    CacheSettings,
    CachesSettings,
    LeaseCacheCapability,
    RedisCache,
    build_caches,
)
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
        assert instance.features == ("atomic", "lease")
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
