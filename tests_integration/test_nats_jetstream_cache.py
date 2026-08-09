from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from contextlib import AsyncExitStack
from uuid import uuid4

import nats
import pytest
from nats.js.api import RePublish, RetentionPolicy, StreamConfig
from testcontainers.core.container import DockerContainer
from testcontainers.core.wait_strategies import LogMessageWaitStrategy
from tests.cache_feature_conformance import (
    CONFORMANCE_TIMEOUT_SECONDS,
    assert_baseline_cache_conformance,
)
from tests_support.database_containers import skip_if_docker_unavailable

from wybra.cache import CacheSettings, CachesSettings, NatsJetStreamCache, build_caches
from wybra.cache.feature_models import CacheFeatureError
from wybra.core.exceptions import ConfigurationError

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
        assert first.features == ()
        assert second.features == ()

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


@pytest.mark.parametrize(
    "stream_configuration",
    (
        {"allow_msg_ttl": False},
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
                        "subjects": [f"wybra.cache.{namespace}.>"],
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
