from __future__ import annotations

import asyncio
from types import SimpleNamespace

import pytest

from cache_feature_conformance import (
    assert_atomic_conformance,
    assert_lease_conformance,
    assert_pubsub_conformance,
    assert_schedule_conformance,
    assert_stream_conformance,
    assert_work_queue_conformance,
)
from wybra.cache import (
    MAX_CACHE_FEATURE_LIMIT,
    MAX_STREAM_POSITION,
    AtomicCacheCapability,
    AtomicCacheValue,
    CacheBackend,
    CacheConflictError,
    CacheFeatureError,
    CacheFeatureGuarantees,
    CacheFeatureMetadata,
    CacheFeatureRegistration,
    CacheFeatureUnavailableError,
    CachePositionExpiredError,
    CacheRevision,
    CacheSettings,
    CachesSettings,
    CounterCacheValue,
    InMemoryAtomicCache,
    InMemoryCache,
    InMemoryCacheFeatures,
    InMemoryLeaseCache,
    InMemoryPubSubCache,
    InMemoryScheduleCache,
    InMemoryStreamCache,
    InMemoryWorkQueue,
    LeaseCacheCapability,
    PubSubCacheCapability,
    ScheduleCacheCapability,
    StreamCacheCapability,
    StreamPosition,
    WorkDelivery,
    WorkIdentity,
    WorkQueueCacheCapability,
    build_caches,
)
from wybra.cache.redis_atomic import RedisAtomicCache
from wybra.cache.redis_features import RedisCacheFeatures
from wybra.cache.redis_pubsub import RedisPubSubCache, RedisPubSubSubscription
from wybra.cache.redis_queues import RedisWorkQueue
from wybra.cache.redis_runtime import RedisCacheRuntime
from wybra.cache.redis_schedule_scripts import (
    SCHEDULE_ADVANCE_SCRIPT,
    SCHEDULE_CLAIM_SCRIPT,
    SCHEDULE_COMPLETE_SCRIPT,
    SCHEDULE_CREATE_SCRIPT,
    SCHEDULE_DELETE_SCRIPT,
    SCHEDULE_DISCARD_SCRIPT,
    SCHEDULE_DUE_SCRIPT,
    SCHEDULE_HELD_SCRIPT,
    SCHEDULE_RELEASE_SCRIPT,
    SCHEDULE_UPDATE_SCRIPT,
)
from wybra.cache.redis_schedules import RedisScheduleCache
from wybra.cache.redis_streams import RedisStreamCache
from wybra.core.exceptions import ConfigurationError


class AtomicStub:
    async def get(self, owner: str, key: str) -> AtomicCacheValue | None:
        del owner, key
        return None

    async def create(
        self,
        owner: str,
        key: str,
        value: bytes,
        *,
        ttl: float,
    ) -> AtomicCacheValue | None:
        del owner, key, value, ttl
        return None

    async def compare_and_swap(
        self,
        owner: str,
        key: str,
        expected: CacheRevision,
        value: bytes,
        *,
        ttl: float,
    ) -> AtomicCacheValue | None:
        del owner, key, expected, value, ttl
        return None

    async def compare_and_delete(
        self,
        owner: str,
        key: str,
        expected: CacheRevision,
    ) -> bool:
        del owner, key, expected
        return False

    async def increment(
        self,
        owner: str,
        key: str,
        *,
        amount: int = 1,
        ttl: float,
    ) -> CounterCacheValue:
        del owner, key, amount, ttl
        return CounterCacheValue(0, CacheRevision(1))


PROCESS_LOCAL_ATOMIC = CacheFeatureMetadata(
    name="atomic",
    guarantees=CacheFeatureGuarantees(
        scope="process",
        durable=False,
        restart_recovery=False,
        horizontal_consumers=False,
        ordering_scope="key",
    ),
)


def cache_settings() -> CachesSettings:
    return CachesSettings(
        instances=(
            CacheSettings(
                name="default",
                backend="memory",
                url=None,
            ),
        ),
    )


@pytest.mark.anyio
async def test_cache_instance_probes_explicit_typed_feature() -> None:
    feature = AtomicStub()

    async def factory(_settings: CacheSettings) -> CacheBackend:
        return CacheBackend(
            InMemoryCache(),
            features=(
                CacheFeatureRegistration(
                    AtomicCacheCapability,
                    feature,
                    PROCESS_LOCAL_ATOMIC,
                ),
            ),
        )

    caches = await build_caches(cache_settings(), factories={"memory": factory})
    instance = caches.require("default")

    assert instance.optional(AtomicCacheCapability) is feature
    assert instance.features == ("atomic",)
    assert instance.feature_metadata == (PROCESS_LOCAL_ATOMIC,)


@pytest.mark.anyio
async def test_cache_instance_requires_feature_with_actionable_context() -> None:
    caches = await build_caches(
        cache_settings(),
        factories={"memory": lambda _settings: _memory_backend()},
    )

    with pytest.raises(
        CacheFeatureUnavailableError,
        match=(
            "Cache 'default' using backend 'memory' does not provide required "
            "feature 'AtomicCacheCapability' for consumer 'tasks'"
        ),
    ):
        caches.require("default").require(
            AtomicCacheCapability,
            consumer="tasks",
        )


@pytest.mark.anyio
async def test_cache_does_not_advertise_incidental_feature_methods() -> None:
    class IncidentalAtomicCache(InMemoryCache):
        async def create(self) -> None:
            return None

    async def factory(_settings: CacheSettings) -> CacheBackend:
        return CacheBackend(IncidentalAtomicCache())

    caches = await build_caches(cache_settings(), factories={"memory": factory})

    assert caches.require("default").optional(AtomicCacheCapability) is None
    assert caches.diagnostics()[0].features == ()


@pytest.mark.anyio
async def test_cache_rejects_duplicate_feature_protocol() -> None:
    feature = AtomicStub()

    async def factory(_settings: CacheSettings) -> CacheBackend:
        registration = CacheFeatureRegistration(
            AtomicCacheCapability,
            feature,
            PROCESS_LOCAL_ATOMIC,
        )
        return CacheBackend(InMemoryCache(), features=(registration, registration))

    with pytest.raises(
        ConfigurationError,
        match="registered feature protocol 'AtomicCacheCapability' more than once",
    ):
        await build_caches(cache_settings(), factories={"memory": factory})


@pytest.mark.anyio
async def test_default_memory_backend_advertises_process_local_features() -> None:
    caches = await build_caches(cache_settings())
    instance = caches.require("default")

    assert instance.require(AtomicCacheCapability) is not None
    assert instance.require(LeaseCacheCapability) is not None
    assert instance.require(WorkQueueCacheCapability) is not None
    assert instance.require(StreamCacheCapability) is not None
    assert instance.require(PubSubCacheCapability) is not None
    assert instance.require(ScheduleCacheCapability) is not None
    assert instance.features == (
        "atomic",
        "lease",
        "pub-sub",
        "schedule",
        "stream",
        "work-queue",
    )
    assert all(
        metadata.guarantees.scope == "process"
        and not metadata.guarantees.durable
        and not metadata.guarantees.restart_recovery
        and not metadata.guarantees.horizontal_consumers
        for metadata in instance.feature_metadata
    )
    assert caches.diagnostics()[0].features == instance.features


@pytest.mark.anyio
async def test_explicit_empty_feature_selection_advertises_baseline_only() -> None:
    settings = CachesSettings(
        instances=(CacheSettings(features=()),),
    )

    caches = await build_caches(settings)

    assert caches.require("default").features == ()
    assert caches.require("default").optional(AtomicCacheCapability) is None
    await caches.close()


@pytest.mark.anyio
async def test_redis_registers_selected_shared_features(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = SimpleNamespace()

    async def ping() -> bool:
        return True

    async def close() -> None:
        return None

    async def config_get(*_keys: str) -> dict[str, str]:
        return {"appendonly": "yes", "maxmemory-policy": "noeviction"}

    async def eval(*_args: object) -> list[bytes]:
        return [b"1", b"1", b"1"]

    async def hmget(*_args: object) -> list[None]:
        return [None, None, None]

    client.ping = ping
    client.aclose = close
    client.config_get = config_get
    client.eval = eval
    client.hmget = hmget
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    settings = CachesSettings(
        instances=(
            CacheSettings(
                backend="redis",
                url="redis://secret@cache/0",
                features=("lease",),
            ),
        )
    )

    caches = await build_caches(settings)
    instance = caches.require("default")

    assert instance.features == ("lease",)
    assert instance.optional(AtomicCacheCapability) is None
    lease = instance.require(LeaseCacheCapability)
    assert lease is not None
    assert instance.feature_metadata[0].guarantees.scope == "shared"
    assert instance.feature_metadata[0].guarantees.durable
    assert instance.feature_metadata[0].guarantees.restart_recovery
    assert instance.feature_metadata[0].guarantees.horizontal_consumers
    assert "secret" not in repr(caches.diagnostics())
    assert caches.diagnostics()[0].health == "ready"
    await caches.close()


@pytest.mark.anyio
async def test_redis_registers_stream_feature_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class StreamClient:
        def __init__(self) -> None:
            self.scripts: list[str] = []

        async def ping(self) -> bool:
            return True

        async def aclose(self) -> None:
            return None

        async def config_get(self, *_keys: str) -> dict[str, str]:
            return {"appendonly": "yes", "maxmemory-policy": "noeviction"}

        async def eval(self, script: str, *_args: object) -> object:
            self.scripts.append(script)
            if "xadd" in script:
                return b"1"
            if "xrange" in script:
                return [1, [b"1-0", [b"p", b"1", b"d", b"readiness"]]]
            return 1

        async def hget(self, *_args: object) -> None:
            return None

        async def delete(self, *_keys: str) -> None:
            return None

    client = StreamClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    caches = await build_caches(
        CachesSettings(
            instances=(
                CacheSettings(
                    backend="redis",
                    url="redis://secret@cache/0",
                    features=("stream",),
                ),
            ),
        )
    )
    instance = caches.require("default")

    assert instance.features == ("stream",)
    assert instance.require(StreamCacheCapability) is not None
    guarantees = instance.feature_metadata[0].guarantees
    assert guarantees.scope == "shared"
    assert guarantees.durable
    assert guarantees.restart_recovery
    assert guarantees.horizontal_consumers
    assert guarantees.ordering_scope == "stream"
    assert guarantees.replay
    assert guarantees.retention
    assert guarantees.acknowledgement
    assert len(client.scripts) == 4
    assert "xadd" in client.scripts[0]
    assert "xrange" in client.scripts[1]
    assert "hset" in client.scripts[2]
    assert "hdel" in client.scripts[3]
    await caches.close()


@pytest.mark.anyio
async def test_redis_registers_pubsub_feature_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PubSubHandle:
        def __init__(self) -> None:
            self.channels: list[str] = []
            self.messages: list[dict[str, bytes | int]] = []
            self.closed = False

        async def subscribe(self, channel: str) -> None:
            self.channels.append(channel)
            self.messages.append(
                {"type": b"subscribe", "channel": channel.encode(), "data": 1}
            )

        async def get_message(
            self,
            **_kwargs: object,
        ) -> dict[str, bytes | int] | None:
            if self.messages:
                return self.messages.pop(0)
            return None

        async def aclose(self) -> None:
            self.closed = True

    class PubSubClient:
        def __init__(self) -> None:
            self.subscription = PubSubHandle()
            self.published: list[tuple[str, bytes]] = []
            self.configuration_checked = False

        async def ping(self) -> bool:
            return True

        async def aclose(self) -> None:
            return None

        async def config_get(self, *_keys: str) -> dict[str, str]:
            self.configuration_checked = True
            return {"appendonly": "yes", "maxmemory-policy": "noeviction"}

        def pubsub(self) -> PubSubHandle:
            return self.subscription

        async def publish(self, channel: str, payload: bytes) -> int:
            self.published.append((channel, payload))
            self.subscription.messages.append(
                {"type": b"message", "channel": channel.encode(), "data": payload}
            )
            return 1

    client = PubSubClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    caches = await build_caches(
        CachesSettings(
            instances=(
                CacheSettings(
                    backend="redis",
                    url="redis://secret@cache/0",
                    features=("pub-sub",),
                ),
            ),
        )
    )
    instance = caches.require("default")

    assert instance.features == ("pub-sub",)
    assert instance.require(PubSubCacheCapability) is not None
    guarantees = instance.feature_metadata[0].guarantees
    assert guarantees.scope == "shared"
    assert not guarantees.durable
    assert not guarantees.restart_recovery
    assert guarantees.horizontal_consumers
    assert guarantees.ordering_scope == "topic"
    assert not guarantees.replay
    assert not guarantees.retention
    assert not guarantees.acknowledgement
    assert client.subscription.closed
    assert len(client.subscription.channels) == 1
    assert client.subscription.channels == [client.published[0][0]]
    assert not client.configuration_checked

    await caches.close()


@pytest.mark.anyio
async def test_redis_pubsub_delivers_live_messages_and_closes_subscriptions(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PubSubHandle:
        def __init__(self) -> None:
            self.channels: set[str] = set()
            self.messages: list[dict[str, bytes | int]] = []
            self.pending_channels: set[str] = set()
            self.closed = False

        async def subscribe(self, channel: str) -> None:
            self.messages.append(
                {"type": b"subscribe", "channel": channel.encode(), "data": 1}
            )
            self.pending_channels.add(channel)

        async def get_message(
            self,
            **_kwargs: object,
        ) -> dict[str, bytes | int] | None:
            if self.messages:
                message = self.messages.pop(0)
                received_channel = message.get("channel")
                if message["type"] == b"subscribe" and isinstance(
                    received_channel,
                    bytes,
                ):
                    self.channels.add(received_channel.decode())
                return message
            return None

        async def aclose(self) -> None:
            self.closed = True

    class PubSubClient:
        def __init__(self) -> None:
            self.subscriptions: list[PubSubHandle] = []

        def pubsub(self) -> PubSubHandle:
            handle = PubSubHandle()
            self.subscriptions.append(handle)
            return handle

        async def publish(self, channel: str, payload: bytes) -> int:
            subscribers = [
                subscription
                for subscription in self.subscriptions
                if not subscription.closed and channel in subscription.channels
            ]
            for subscription in subscribers:
                subscription.messages.append(
                    {
                        "type": b"message",
                        "channel": channel.encode(),
                        "data": payload,
                    }
                )
            return len(subscribers)

        async def aclose(self) -> None:
            return None

    client = PubSubClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    runtime = RedisCacheRuntime("redis://secret@cache/0", "test")
    pubsub = RedisPubSubCache(runtime)

    assert await pubsub.publish("owner", "topic", b"offline") == 0
    subscription = await pubsub.subscribe("owner", "topic")
    with pytest.raises(TimeoutError):
        await subscription.receive(timeout=0.1)
    assert await pubsub.publish("owner", "topic", b"online") == 1
    assert await subscription.receive() == b"online"
    await subscription.close()
    await subscription.close()
    assert client.subscriptions[0].closed
    with pytest.raises(CacheFeatureError, match="subscription is closed"):
        await subscription.receive()

    live = await pubsub.subscribe("owner", "second")
    await pubsub.close()
    assert client.subscriptions[1].closed
    with pytest.raises(CacheFeatureError, match="subscription is closed"):
        await live.receive()
    with pytest.raises(CacheFeatureError, match="pub/sub cache is closed"):
        await pubsub.publish("owner", "second", b"after-close")
    await runtime.close()


@pytest.mark.anyio
async def test_redis_pubsub_close_cancels_a_stalled_activation() -> None:
    activation_started = asyncio.Event()

    class Handle:
        closed = False

        async def aclose(self) -> None:
            self.closed = True

    class StalledRuntime:
        def __init__(self) -> None:
            self.handle = Handle()

        def key(self, domain: str, owner: str, topic: str) -> str:
            return f"test:{domain}:{owner}:{topic}"

        async def open_pubsub(self) -> Handle:
            return self.handle

        async def subscribe_pubsub(self, _handle: Handle, _channel: str) -> None:
            activation_started.set()
            await asyncio.Event().wait()

        async def subscription_call(self, operation: object) -> object:
            return await operation()

    runtime = StalledRuntime()
    pubsub = RedisPubSubCache(runtime)
    pending = asyncio.create_task(pubsub.subscribe("owner", "topic"))
    await activation_started.wait()

    await asyncio.wait_for(pubsub.close(), timeout=0.2)

    with pytest.raises(CacheFeatureError, match="pub/sub cache is closed"):
        await pending
    assert runtime.handle.closed


@pytest.mark.anyio
async def test_redis_pubsub_caller_cancellation_releases_completed_activation() -> None:
    activation_started = asyncio.Event()

    class Handle:
        closed = False

        async def aclose(self) -> None:
            self.closed = True

    class CancellationResistantRuntime:
        def __init__(self) -> None:
            self.handle = Handle()

        def key(self, domain: str, owner: str, topic: str) -> str:
            return f"test:{domain}:{owner}:{topic}"

        async def open_pubsub(self) -> Handle:
            return self.handle

        async def subscribe_pubsub(self, _handle: Handle, _channel: str) -> None:
            activation_started.set()
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                return

        async def subscription_call(self, operation: object) -> object:
            return await operation()

    runtime = CancellationResistantRuntime()
    pubsub = RedisPubSubCache(runtime)
    pending = asyncio.create_task(pubsub.subscribe("owner", "topic"))
    await activation_started.wait()

    pending.cancel()

    with pytest.raises(asyncio.CancelledError):
        await pending
    assert runtime.handle.closed
    assert not pubsub._subscriptions
    assert not pubsub._pending_handles
    assert not pubsub._activations


@pytest.mark.anyio
async def test_redis_pubsub_retries_retained_failed_activation_cleanup() -> None:
    class Handle:
        def __init__(self) -> None:
            self.close_attempts = 0

        async def aclose(self) -> None:
            self.close_attempts += 1
            if self.close_attempts == 1:
                raise RuntimeError("first cleanup failed")

    class FailingRuntime:
        def __init__(self) -> None:
            self.handle = Handle()

        def key(self, domain: str, owner: str, topic: str) -> str:
            return f"test:{domain}:{owner}:{topic}"

        async def open_pubsub(self) -> Handle:
            return self.handle

        async def subscribe_pubsub(self, _handle: Handle, _channel: str) -> None:
            raise CacheFeatureError("Redis cache feature operation failed.")

        async def subscription_call(self, operation: object) -> object:
            try:
                return await operation()
            except asyncio.CancelledError:
                raise
            except Exception:
                raise CacheFeatureError(
                    "Redis cache feature operation failed."
                ) from None

    runtime = FailingRuntime()
    pubsub = RedisPubSubCache(runtime)

    with pytest.raises(CacheFeatureError, match="feature operation failed") as raised:
        await pubsub.subscribe("owner", "topic")

    assert raised.value.__notes__ == [
        "Redis pub/sub activation cleanup also failed (CacheFeatureError); "
        "cache shutdown will retry."
    ]
    assert runtime.handle.close_attempts == 1

    await pubsub.close()

    assert runtime.handle.close_attempts == 2
    assert not pubsub._pending_handles


@pytest.mark.anyio
async def test_redis_pubsub_serialises_receives_with_one_total_timeout() -> None:
    started = asyncio.Event()
    release = asyncio.Event()
    active_reads = 0
    maximum_reads = 0
    channel = "test:pub-sub:owner:topic"

    class Handle:
        async def get_message(self, **_kwargs: object) -> dict[str, bytes]:
            nonlocal active_reads, maximum_reads
            active_reads += 1
            maximum_reads = max(maximum_reads, active_reads)
            started.set()
            try:
                await release.wait()
                return {
                    "type": b"message",
                    "channel": channel.encode(),
                    "data": b"first",
                }
            finally:
                active_reads -= 1

    class Runtime:
        async def subscription_call(self, operation: object) -> object:
            return await operation()

    async def close() -> None:
        return None

    subscription = RedisPubSubSubscription(Runtime(), Handle(), channel, close)
    first = asyncio.create_task(subscription.receive())
    await started.wait()

    with pytest.raises(TimeoutError, match="receive timed out"):
        await subscription.receive(timeout=0.01)

    release.set()
    assert await first == b"first"
    assert maximum_reads == 1


@pytest.mark.anyio
async def test_redis_pubsub_receive_timeout_bounds_an_overlong_provider_read() -> None:
    channel = "test:pub-sub:owner:topic"

    class Handle:
        cancelled = False

        async def get_message(self, **_kwargs: object) -> object:
            try:
                await asyncio.Event().wait()
            finally:
                self.cancelled = True

    class Runtime:
        async def subscription_call(self, operation: object) -> object:
            return await operation()

    async def close() -> None:
        return None

    handle = Handle()
    subscription = RedisPubSubSubscription(Runtime(), handle, channel, close)

    with pytest.raises(TimeoutError, match="receive timed out"):
        await asyncio.wait_for(subscription.receive(timeout=0.01), timeout=0.1)

    assert handle.cancelled


@pytest.mark.anyio
@pytest.mark.parametrize("received_channel", [None, b"wrong-channel"])
async def test_redis_pubsub_rejects_missing_or_wrong_delivery_channel(
    received_channel: bytes | None,
) -> None:
    channel = "test:pub-sub:owner:topic"

    class Handle:
        async def get_message(self, **_kwargs: object) -> dict[str, object]:
            return {
                "type": b"message",
                "channel": received_channel,
                "data": b"payload",
            }

    class Runtime:
        async def subscription_call(self, operation: object) -> object:
            return await operation()

    async def close() -> None:
        return None

    subscription = RedisPubSubSubscription(Runtime(), Handle(), channel, close)

    with pytest.raises(CacheFeatureError, match="invalid state"):
        await subscription.receive(timeout=0.1)


@pytest.mark.anyio
async def test_redis_pubsub_iteration_preserves_a_failure_during_close() -> None:
    channel = "test:pub-sub:owner:topic"
    subscription: RedisPubSubSubscription

    class Handle:
        async def get_message(self, **_kwargs: object) -> object:
            subscription._closed = True
            subscription._close_event.set()
            raise CacheFeatureError("Redis cache feature operation failed.")

    class Runtime:
        async def subscription_call(self, operation: object) -> object:
            return await operation()

    async def close() -> None:
        return None

    subscription = RedisPubSubSubscription(Runtime(), Handle(), channel, close)

    with pytest.raises(CacheFeatureError, match="feature operation failed"):
        await anext(subscription)


@pytest.mark.anyio
async def test_redis_pubsub_propagates_open_subscription_failures() -> None:
    class FailedRuntime:
        async def subscription_call(self, _operation: object) -> object:
            raise CacheFeatureError("Redis cache feature operation failed.")

    async def close() -> None:
        return None

    subscription = RedisPubSubSubscription(
        FailedRuntime(),
        SimpleNamespace(),
        "test:pub-sub:owner:topic",
        close,
    )

    with pytest.raises(CacheFeatureError, match="feature operation failed"):
        await anext(subscription)


@pytest.mark.anyio
async def test_redis_pubsub_subscription_retries_failed_close() -> None:
    attempts = 0

    async def close() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise CacheFeatureError("Redis cache feature operation failed.")

    subscription = RedisPubSubSubscription(
        SimpleNamespace(),
        SimpleNamespace(),
        "test:pub-sub:owner:topic",
        close,
    )

    with pytest.raises(CacheFeatureError, match="feature operation failed"):
        await subscription.close()
    with pytest.raises(CacheFeatureError, match="subscription is closed"):
        await subscription.receive()

    await subscription.close()
    await subscription.close()

    assert attempts == 2


@pytest.mark.anyio
async def test_redis_pubsub_cache_preserves_cancellation_after_full_cleanup() -> None:
    cancelled_attempts = 0
    released = False

    async def cancel_once() -> None:
        nonlocal cancelled_attempts
        cancelled_attempts += 1
        if cancelled_attempts == 1:
            raise asyncio.CancelledError

    async def release() -> None:
        nonlocal released
        released = True

    pubsub = RedisPubSubCache(SimpleNamespace())
    cancelling = RedisPubSubSubscription(
        SimpleNamespace(),
        SimpleNamespace(),
        "test:pub-sub:owner:cancelling",
        cancel_once,
    )
    succeeding = RedisPubSubSubscription(
        SimpleNamespace(),
        SimpleNamespace(),
        "test:pub-sub:owner:succeeding",
        release,
    )
    pubsub._subscriptions.update((cancelling, succeeding))

    with pytest.raises(asyncio.CancelledError):
        await pubsub.close()

    assert released

    await pubsub.close()

    assert cancelled_attempts == 2


@pytest.mark.anyio
async def test_redis_pubsub_cache_closes_every_subscription_after_failure() -> None:
    attempts = 0
    released = False

    async def fail_once() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise CacheFeatureError("Redis cache feature operation failed.")

    async def release() -> None:
        nonlocal released
        released = True

    pubsub = RedisPubSubCache(SimpleNamespace())
    failing = RedisPubSubSubscription(
        SimpleNamespace(),
        SimpleNamespace(),
        "test:pub-sub:owner:failing",
        fail_once,
    )
    succeeding = RedisPubSubSubscription(
        SimpleNamespace(),
        SimpleNamespace(),
        "test:pub-sub:owner:succeeding",
        release,
    )
    pubsub._subscriptions.update((failing, succeeding))

    with pytest.raises(ExceptionGroup, match="Redis pub/sub subscription cleanup"):
        await pubsub.close()

    assert released

    await pubsub.close()

    assert attempts == 2


@pytest.mark.anyio
async def test_redis_feature_close_attempts_every_capability() -> None:
    class FailedPubSub:
        async def close(self) -> None:
            raise CacheFeatureError("Redis cache feature operation failed.")

    class TrackingWorkQueue:
        closed = False

        async def close(self) -> None:
            self.closed = True

    features = RedisCacheFeatures(SimpleNamespace())
    work_queue = TrackingWorkQueue()
    object.__setattr__(features, "pubsub", FailedPubSub())
    object.__setattr__(features, "work_queue", work_queue)

    with pytest.raises(ExceptionGroup, match="Redis cache feature cleanup"):
        await features.close()

    assert work_queue.closed


@pytest.mark.anyio
async def test_redis_pubsub_readiness_rejects_wrong_delivery_channel() -> None:
    class PubSubHandle:
        def __init__(self) -> None:
            self.messages: list[dict[str, bytes | int]] = []

        async def subscribe(self, channel: str) -> None:
            self.messages.append(
                {"type": b"subscribe", "channel": channel.encode(), "data": 1}
            )

        async def get_message(
            self,
            **_kwargs: object,
        ) -> dict[str, bytes | int] | None:
            if self.messages:
                return self.messages.pop(0)
            return None

        async def aclose(self) -> None:
            return None

    class PubSubClient:
        def __init__(self) -> None:
            self.subscription = PubSubHandle()

        def pubsub(self) -> PubSubHandle:
            return self.subscription

        async def publish(self, _channel: str, payload: bytes) -> int:
            self.subscription.messages.append(
                {"type": b"message", "channel": b"wrong", "data": payload}
            )
            return 1

    runtime = RedisCacheRuntime("redis://secret@cache/0", "test")
    runtime._client = PubSubClient()

    with pytest.raises(
        ConfigurationError,
        match="cannot provide the configured advanced features",
    ):
        await runtime.validate_features(frozenset({"pub-sub"}))


@pytest.mark.anyio
async def test_redis_pubsub_readiness_rejects_failed_cleanup() -> None:
    class PubSubHandle:
        def __init__(self) -> None:
            self.messages: list[dict[str, bytes | int]] = []

        async def subscribe(self, channel: str) -> None:
            self.messages.append(
                {"type": b"subscribe", "channel": channel.encode(), "data": 1}
            )

        async def get_message(
            self,
            **_kwargs: object,
        ) -> dict[str, bytes | int] | None:
            if self.messages:
                return self.messages.pop(0)
            return None

        async def aclose(self) -> None:
            raise RuntimeError("connection lost")

    class PubSubClient:
        def __init__(self) -> None:
            self.subscription = PubSubHandle()

        def pubsub(self) -> PubSubHandle:
            return self.subscription

        async def publish(self, channel: str, payload: bytes) -> int:
            self.subscription.messages.append(
                {"type": b"message", "channel": channel.encode(), "data": payload}
            )
            return 1

    runtime = RedisCacheRuntime("redis://secret@cache/0", "test")
    runtime._client = PubSubClient()

    with pytest.raises(
        ConfigurationError,
        match="cannot provide the configured advanced features",
    ):
        await runtime.validate_features(frozenset({"pub-sub"}))


@pytest.mark.anyio
async def test_redis_pubsub_readiness_preserves_cancellation_after_failed_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PubSubHandle:
        async def aclose(self) -> None:
            raise RuntimeError("connection lost")

    class PubSubClient:
        def pubsub(self) -> PubSubHandle:
            return PubSubHandle()

    async def cancel_subscription(
        _runtime: RedisCacheRuntime,
        _subscription: object,
        _channel: str,
    ) -> None:
        raise asyncio.CancelledError

    monkeypatch.setattr(RedisCacheRuntime, "subscribe_pubsub", cancel_subscription)
    runtime = RedisCacheRuntime("redis://secret@cache/0", "test")
    runtime._client = PubSubClient()

    with pytest.raises(asyncio.CancelledError) as raised:
        await runtime.validate_features(frozenset({"pub-sub"}))

    assert raised.value.__notes__ == [
        "Redis pub/sub readiness cleanup after cancellation failed (CacheFeatureError)."
    ]


@pytest.mark.anyio
async def test_redis_registers_schedule_feature_metadata(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ScheduleClient:
        def __init__(self) -> None:
            self.scripts: list[str] = []
            self.calls: list[tuple[str, tuple[object, ...]]] = []

        async def ping(self) -> bool:
            return True

        async def aclose(self) -> None:
            return None

        async def config_get(self, *_keys: str) -> dict[str, str]:
            return {"appendonly": "yes", "maxmemory-policy": "noeviction"}

        async def eval(self, script: str, *_args: object) -> object:
            self.scripts.append(script)
            self.calls.append((script, _args))
            if script is SCHEDULE_CREATE_SCRIPT:
                return [1, b"1"]
            if script is SCHEDULE_UPDATE_SCRIPT:
                return [1, b"2"]
            if script is SCHEDULE_DUE_SCRIPT:
                return [[b"schedule", b"updated", b"2", b"0", b""]]
            if script is SCHEDULE_CLAIM_SCRIPT:
                return [
                    1,
                    b"schedule",
                    b"updated",
                    b"2",
                    b"0",
                    b"",
                    b"1",
                    b"0.5",
                ]
            if script is SCHEDULE_HELD_SCRIPT:
                return [1]
            if script is SCHEDULE_RELEASE_SCRIPT:
                return [1]
            if script is SCHEDULE_COMPLETE_SCRIPT:
                return [1, 0]
            if script is SCHEDULE_ADVANCE_SCRIPT:
                return [1, b"3"]
            if script is SCHEDULE_DELETE_SCRIPT:
                return [1]
            if script is SCHEDULE_DISCARD_SCRIPT:
                return [1]
            raise AssertionError("Unexpected schedule readiness script.")

        async def delete(self, *_keys: str) -> None:
            return None

    client = ScheduleClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )

    caches = await build_caches(
        CachesSettings(
            instances=(
                CacheSettings(
                    backend="redis",
                    url="redis://secret@cache/0",
                    features=("schedule",),
                ),
            ),
        )
    )
    instance = caches.require("default")

    assert instance.features == ("schedule",)
    assert instance.require(ScheduleCacheCapability) is not None
    guarantees = instance.feature_metadata[0].guarantees
    assert guarantees.scope == "shared"
    assert guarantees.durable
    assert guarantees.restart_recovery
    assert guarantees.horizontal_consumers
    assert guarantees.ordering_scope == "due-time"
    assert guarantees.scheduling
    assert client.scripts == [
        SCHEDULE_CREATE_SCRIPT,
        SCHEDULE_UPDATE_SCRIPT,
        SCHEDULE_DUE_SCRIPT,
        SCHEDULE_DUE_SCRIPT,
        SCHEDULE_CLAIM_SCRIPT,
        SCHEDULE_HELD_SCRIPT,
        SCHEDULE_RELEASE_SCRIPT,
        SCHEDULE_CLAIM_SCRIPT,
        SCHEDULE_COMPLETE_SCRIPT,
        SCHEDULE_CREATE_SCRIPT,
        SCHEDULE_CLAIM_SCRIPT,
        SCHEDULE_ADVANCE_SCRIPT,
        SCHEDULE_DELETE_SCRIPT,
        SCHEDULE_CREATE_SCRIPT,
        SCHEDULE_CLAIM_SCRIPT,
        SCHEDULE_DISCARD_SCRIPT,
    ]
    due_calls = [args for script, args in client.calls if script is SCHEDULE_DUE_SCRIPT]
    assert len(due_calls) == 2
    assert all(call[0] == 4 for call in due_calls)
    assert all("readiness-" in key for call in due_calls for key in call[1:5])
    assert due_calls[0][-1] == ""
    assert due_calls[1][-1] != ""

    await caches.close()


@pytest.mark.anyio
async def test_redis_schedule_translates_record_and_claim_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ScheduleClient:
        async def eval(self, script: str, *_args: object) -> object:
            if script is SCHEDULE_CREATE_SCRIPT:
                return [1, b"1"]
            if script is SCHEDULE_UPDATE_SCRIPT:
                return [1, b"2"]
            if script is SCHEDULE_DUE_SCRIPT:
                return [[b"once", b"updated", b"2", b"0", b""]]
            if script is SCHEDULE_CLAIM_SCRIPT:
                return [
                    1,
                    b"once",
                    b"updated",
                    b"2",
                    b"0",
                    b"",
                    b"1",
                    b"1.5",
                ]
            if script is SCHEDULE_RELEASE_SCRIPT:
                return [1]
            if script is SCHEDULE_COMPLETE_SCRIPT:
                return [1, 0]
            raise AssertionError("Unexpected schedule script.")

    client = ScheduleClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    schedules = RedisScheduleCache(RedisCacheRuntime("redis://cache", "safe"))

    created = await schedules.create("tasks", "once", b"payload", next_due_at=0)
    updated = await schedules.update(
        "tasks",
        "once",
        created.revision,
        b"updated",
        next_due_at=0,
    )
    due = await schedules.due("tasks", before=0)
    claim = await schedules.claim("tasks", "once", "scheduler", ttl=1)

    assert created is not None
    assert created.revision == CacheRevision(1)
    assert updated is not None
    assert due == (updated,)
    assert claim is not None
    assert claim.record == updated
    assert claim.fencing_token.value == 1
    await schedules.release(claim)
    claim = await schedules.claim("tasks", "once", "scheduler", ttl=1)
    assert claim is not None
    assert await schedules.complete(claim) is None


@pytest.mark.anyio
async def test_redis_schedule_translates_capacity_and_malformed_intervals(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ScheduleClient:
        async def eval(self, script: str, *_args: object) -> object:
            if script is SCHEDULE_CREATE_SCRIPT:
                return [-1]
            if script is SCHEDULE_DUE_SCRIPT:
                return [[b"once", b"payload", b"1", b"0", b"0"]]
            raise AssertionError("Unexpected schedule script.")

    client = ScheduleClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    schedules = RedisScheduleCache(RedisCacheRuntime("redis://cache", "safe"))

    with pytest.raises(CacheFeatureError, match="record capacity"):
        await schedules.create("tasks", "once", b"payload", next_due_at=0)
    with pytest.raises(CacheFeatureError, match="interval is invalid"):
        await schedules.due("tasks", before=0)


@pytest.mark.anyio
async def test_redis_schedule_rejects_malformed_identity_and_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ScheduleClient:
        def __init__(self) -> None:
            self.responses = iter(
                (
                    [[b"", b"payload", b"1", b"0", b""]],
                    [[b"valid", b"x" * 1_048_577, b"1", b"0", b""]],
                )
            )

        async def eval(self, script: str, *_args: object) -> object:
            assert script is SCHEDULE_DUE_SCRIPT
            return next(self.responses)

    client = ScheduleClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    schedules = RedisScheduleCache(RedisCacheRuntime("redis://cache", "safe"))

    with pytest.raises(CacheFeatureError, match="identity is invalid"):
        await schedules.due("tasks", before=0)
    with pytest.raises(CacheFeatureError, match="payload is invalid"):
        await schedules.due("tasks", before=0)


@pytest.mark.anyio
async def test_redis_schedule_rejects_unsafe_recurrence_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ScheduleClient:
        async def eval(self, script: str, *_args: object) -> object:
            if script is SCHEDULE_CREATE_SCRIPT:
                return [1, b"1"]
            if script is SCHEDULE_CLAIM_SCRIPT:
                return [
                    1,
                    b"recurring",
                    b"payload",
                    b"1",
                    b"0",
                    b"1",
                    b"1",
                    b"1.5",
                ]
            if script is SCHEDULE_COMPLETE_SCRIPT:
                return [-2]
            raise AssertionError("Unexpected schedule script.")

    client = ScheduleClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    schedules = RedisScheduleCache(RedisCacheRuntime("redis://cache", "safe"))

    created = await schedules.create(
        "tasks",
        "recurring",
        b"payload",
        next_due_at=0,
        interval_seconds=1,
    )
    assert created is not None
    claim = await schedules.claim("tasks", "recurring", "scheduler", ttl=1)
    assert claim is not None
    with pytest.raises(CacheFeatureError, match="cannot advance safely"):
        await schedules.complete(claim)


@pytest.mark.anyio
async def test_redis_schedule_readiness_rejects_operational_script_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ScheduleClient:
        async def ping(self) -> bool:
            return True

        async def aclose(self) -> None:
            return None

        async def config_get(self, *_keys: str) -> dict[str, str]:
            return {"appendonly": "yes", "maxmemory-policy": "noeviction"}

        async def eval(self, script: str, *_args: object) -> object:
            if script is SCHEDULE_CREATE_SCRIPT:
                return [1, b"1"]
            if script is SCHEDULE_UPDATE_SCRIPT:
                return [1, b"2"]
            if script is SCHEDULE_DUE_SCRIPT:
                raise RuntimeError("ACL denied HMGET")
            raise AssertionError("Unexpected schedule readiness script.")

        async def delete(self, *_keys: str) -> None:
            return None

    client = ScheduleClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )

    with pytest.raises(ConfigurationError, match="cannot provide"):
        await build_caches(
            CachesSettings(
                instances=(
                    CacheSettings(
                        backend="redis",
                        url="redis://cache",
                        namespace="safe",
                        features=("schedule",),
                    ),
                ),
            )
        )


@pytest.mark.anyio
async def test_redis_schedule_readiness_rejects_malformed_due_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class ScheduleClient:
        async def ping(self) -> bool:
            return True

        async def aclose(self) -> None:
            return None

        async def config_get(self, *_keys: str) -> dict[str, str]:
            return {"appendonly": "yes", "maxmemory-policy": "noeviction"}

        async def eval(self, script: str, *_args: object) -> object:
            if script is SCHEDULE_CREATE_SCRIPT:
                return [1, b"1"]
            if script is SCHEDULE_UPDATE_SCRIPT:
                return [1, b"2"]
            if script is SCHEDULE_DUE_SCRIPT:
                return [0]
            raise AssertionError("Unexpected schedule readiness script.")

        async def delete(self, *_keys: str) -> None:
            return None

    client = ScheduleClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )

    with pytest.raises(ConfigurationError, match="cannot provide"):
        await build_caches(
            CachesSettings(
                instances=(
                    CacheSettings(
                        backend="redis",
                        url="redis://cache",
                        namespace="safe",
                        features=("schedule",),
                    ),
                ),
            )
        )


@pytest.mark.anyio
async def test_redis_feature_failure_does_not_expose_provider_detail(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    class FailingClient:
        async def hmget(self, *_args: object) -> None:
            raise RuntimeError("redis://user:secret@cache raw-key stored-value")

    client = FailingClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    runtime = RedisCacheRuntime("redis://user:secret@cache", "safe")

    with pytest.raises(CacheFeatureError) as raised:
        await RedisAtomicCache(runtime).get("owner", "key")

    rendered = repr(raised.value)
    assert "secret" not in rendered
    assert "raw-key" not in rendered
    assert "stored-value" not in rendered
    assert raised.value.__cause__ is None
    assert "secret" not in caplog.text
    assert "raw-key" not in caplog.text
    assert "stored-value" not in caplog.text


@pytest.mark.anyio
async def test_redis_feature_rejects_malformed_script_reply(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MalformedClient:
        async def eval(self, *_args: object) -> list[int]:
            return [1]

    client = MalformedClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    runtime = RedisCacheRuntime("redis://cache", "safe")

    with pytest.raises(CacheFeatureError, match="invalid state"):
        await RedisAtomicCache(runtime).create("owner", "key", b"value", ttl=60)


@pytest.mark.anyio
async def test_redis_advanced_features_reject_allkeys_eviction_policy() -> None:
    class UnsafeClient:
        async def config_get(self, *_keys: str) -> dict[bytes, bytes]:
            return {
                b"appendonly": b"yes",
                b"maxmemory-policy": b"allkeys-lru",
            }

    runtime = RedisCacheRuntime("redis://cache", "safe")
    runtime._client = UnsafeClient()

    with pytest.raises(ConfigurationError, match="non-allkeys eviction policy"):
        await runtime.validate_features(frozenset({"atomic"}))


@pytest.mark.anyio
async def test_redis_advanced_features_warn_when_configuration_is_unavailable(
    caplog: pytest.LogCaptureFixture,
) -> None:
    class RestrictedClient:
        async def config_get(self, *_keys: str) -> None:
            raise RuntimeError("CONFIG command is disabled")

        async def eval(self, *_args: object) -> list[bytes]:
            return [b"1", b"1", b"1"]

        async def hmget(self, *_args: object) -> list[None]:
            return [None, None, None]

    runtime = RedisCacheRuntime("redis://cache", "safe")
    runtime._client = RestrictedClient()

    await runtime.validate_features(frozenset({"atomic"}))

    assert "could not be inspected" in caplog.text


@pytest.mark.anyio
async def test_redis_advanced_features_reject_missing_atomic_read_permission() -> None:
    class RestrictedClient:
        async def config_get(self, *_keys: str) -> dict[str, str]:
            return {"appendonly": "yes", "maxmemory-policy": "noeviction"}

        async def eval(self, *_args: object) -> list[bytes]:
            return [b"1", b"1", b"1"]

        async def hmget(self, *_args: object) -> None:
            raise PermissionError("HMGET is denied")

    runtime = RedisCacheRuntime("redis://cache", "safe")
    runtime._client = RestrictedClient()

    with pytest.raises(ConfigurationError, match="cannot provide"):
        await runtime.validate_features(frozenset({"atomic"}))


@pytest.mark.anyio
async def test_redis_lease_readiness_does_not_require_atomic_read_permission() -> None:
    class LeaseClient:
        async def config_get(self, *_keys: str) -> dict[str, str]:
            return {"appendonly": "yes", "maxmemory-policy": "noeviction"}

        async def eval(self, *_args: object) -> list[bytes]:
            return [b"1", b"1", b"1"]

        async def hmget(self, *_args: object) -> None:
            raise AssertionError("Lease readiness must not use HMGET.")

    runtime = RedisCacheRuntime("redis://cache", "safe")
    runtime._client = LeaseClient()

    await runtime.validate_features(frozenset({"lease"}))


@pytest.mark.anyio
async def test_redis_stream_readiness_requires_stream_read_permission() -> None:
    class RestrictedClient:
        async def config_get(self, *_keys: str) -> dict[str, str]:
            return {"appendonly": "yes", "maxmemory-policy": "noeviction"}

        async def eval(self, script: str, *_args: object) -> object:
            if "xrange" in script:
                raise PermissionError("XRANGE is denied")
            return 1

        async def delete(self, *_keys: str) -> None:
            return None

    runtime = RedisCacheRuntime("redis://cache", "safe")
    runtime._client = RestrictedClient()

    with pytest.raises(ConfigurationError, match="cannot provide"):
        await runtime.validate_features(frozenset({"stream"}))


@pytest.mark.anyio
async def test_redis_stream_readiness_rejects_invalid_script_result() -> None:
    class InvalidClient:
        async def config_get(self, *_keys: str) -> dict[str, str]:
            return {"appendonly": "yes", "maxmemory-policy": "noeviction"}

        async def eval(self, *_args: object) -> bytes:
            return b"0"

        async def delete(self, *_keys: str) -> None:
            return None

    runtime = RedisCacheRuntime("redis://cache", "safe")
    runtime._client = InvalidClient()

    with pytest.raises(ConfigurationError, match="cannot provide"):
        await runtime.validate_features(frozenset({"stream"}))


@pytest.mark.anyio
async def test_redis_stream_rejects_malformed_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class MalformedClient:
        async def eval(self, *_args: object) -> list[object]:
            return [1, [b"1-0", [b"p", b"1"]]]

    client = MalformedClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    streams = RedisStreamCache(RedisCacheRuntime("redis://cache", "safe"))

    with pytest.raises(CacheFeatureError, match="invalid records"):
        await streams.read("owner", "events")


@pytest.mark.anyio
async def test_redis_stream_reads_only_the_requested_page(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class PagingClient:
        def __init__(self) -> None:
            self.calls: list[tuple[object, ...]] = []

        async def eval(
            self,
            *_args: object,
        ) -> list[object]:
            self.calls.append(_args)
            return [1, [b"2-0", [b"p", b"2", b"d", b"second"]]]

    client = PagingClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    streams = RedisStreamCache(RedisCacheRuntime("redis://cache", "safe"))

    records = await streams.read(
        "owner",
        "events",
        after=StreamPosition(1),
        limit=1,
    )

    assert [record.position.value for record in records] == [2]
    assert len(client.calls) == 1
    assert client.calls[0][1:] == (1, "safe:stream:owner:events", 1, 1)


@pytest.mark.anyio
async def test_redis_stream_rejects_acknowledgements_beyond_latest_position(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FutureAcknowledgementClient:
        async def eval(self, *_args: object) -> int:
            return 0

    client = FutureAcknowledgementClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    streams = RedisStreamCache(RedisCacheRuntime("redis://cache", "safe"))

    with pytest.raises(CacheConflictError, match="does not exist"):
        await streams.acknowledge(
            "owner",
            "events",
            "projection",
            StreamPosition(2),
        )


def test_stream_positions_are_bounded_for_every_backend() -> None:
    assert StreamPosition(MAX_STREAM_POSITION).value == MAX_STREAM_POSITION

    with pytest.raises(ValueError, match="cannot exceed"):
        StreamPosition(MAX_STREAM_POSITION + 1)


@pytest.mark.anyio
async def test_redis_stream_rejects_new_consumers_at_capacity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CapacityClient:
        async def eval(self, *_args: object) -> int:
            return -2

    client = CapacityClient()
    monkeypatch.setattr(
        "wybra.cache.redis_runtime.importlib.import_module",
        lambda _: SimpleNamespace(
            Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
        ),
    )
    streams = RedisStreamCache(
        RedisCacheRuntime("redis://cache", "safe"),
        max_consumers=1,
    )

    with pytest.raises(CacheFeatureError, match="consumer capacity"):
        await streams.acknowledge(
            "owner",
            "events",
            "projection",
            StreamPosition(1),
        )


@pytest.mark.anyio
async def test_redis_work_queue_readiness_requires_pending_permission() -> None:
    calls: list[str] = []

    class RestrictedClient:
        async def config_get(self, *_keys: str) -> dict[str, str]:
            return {"appendonly": "yes", "maxmemory-policy": "noeviction"}

        async def eval(self, *_args: object) -> list[bytes]:
            return [b"1", b"1"]

        async def xgroup_create(self, *_args: object, **_kwargs: object) -> None:
            return None

        async def xreadgroup(
            self,
            *_args: object,
            **_kwargs: object,
        ) -> list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]]:
            return [(b"stream", [(b"1-0", {b"i": b"identity"})])]

        async def xread(self, *_args: object, **_kwargs: object) -> list[object]:
            calls.append("XREAD")
            return []

        async def xinfo_consumers(self, *_args: object) -> list[object]:
            calls.append("XINFO CONSUMERS")
            return []

        async def xpending_range(self, *_args: object) -> None:
            calls.append("XPENDING")
            raise PermissionError("XPENDING is denied")

        async def xgroup_destroy(self, *_args: object) -> None:
            return None

        async def delete(self, *_keys: str) -> None:
            return None

    runtime = RedisCacheRuntime("redis://cache", "safe")
    runtime._client = RestrictedClient()

    with pytest.raises(ConfigurationError, match="cannot provide"):
        await runtime.validate_features(frozenset({"work-queue"}))

    assert calls == ["XREAD", "XINFO CONSUMERS", "XPENDING"]


@pytest.mark.anyio
@pytest.mark.parametrize("denied_command", ["XINFO CONSUMERS", "XGROUP DELCONSUMER"])
async def test_redis_work_queue_readiness_requires_consumer_cleanup_permissions(
    denied_command: str,
) -> None:
    calls: list[str] = []

    class RestrictedClient:
        async def config_get(self, *_keys: str) -> dict[str, str]:
            return {"appendonly": "yes", "maxmemory-policy": "noeviction"}

        async def eval(self, *_args: object) -> list[bytes]:
            return [b"1", b"1"]

        async def xgroup_create(self, *_args: object, **_kwargs: object) -> None:
            return None

        async def xreadgroup(
            self,
            *_args: object,
            **_kwargs: object,
        ) -> list[tuple[bytes, list[tuple[bytes, dict[bytes, bytes]]]]]:
            return [(b"stream", [(b"1-0", {b"i": b"identity"})])]

        async def xread(self, *_args: object, **_kwargs: object) -> list[object]:
            return []

        async def xinfo_consumers(self, *_args: object) -> list[object]:
            calls.append("XINFO CONSUMERS")
            if denied_command == "XINFO CONSUMERS":
                raise PermissionError(f"{denied_command} is denied")
            return []

        async def xpending_range(self, *_args: object) -> list[object]:
            return []

        async def execute_command(self, *_args: object) -> list[object]:
            return []

        async def xgroup_delconsumer(self, *_args: object) -> int:
            calls.append("XGROUP DELCONSUMER")
            if denied_command == "XGROUP DELCONSUMER":
                raise PermissionError(f"{denied_command} is denied")
            return 0

        async def xrange(self, *_args: object, **_kwargs: object) -> list[object]:
            return []

        async def xgroup_destroy(self, *_args: object) -> None:
            return None

        async def delete(self, *_keys: str) -> None:
            return None

    runtime = RedisCacheRuntime("redis://cache", "safe")
    runtime._client = RestrictedClient()

    with pytest.raises(ConfigurationError, match="cannot provide"):
        await runtime.validate_features(frozenset({"work-queue"}))

    assert calls[-1] == denied_command


@pytest.mark.anyio
async def test_memory_feature_lifecycle_closes_once() -> None:
    features = InMemoryCacheFeatures()

    async def factory(_settings: CacheSettings) -> CacheBackend:
        return CacheBackend(
            InMemoryCache(),
            close=features.close,
            lifecycle_owner=features,
            features=features.registrations(),
        )

    caches = await build_caches(cache_settings(), factories={"memory": factory})
    queue = caches.require("default").require(WorkQueueCacheCapability)
    await caches.close()
    await caches.close()

    with pytest.raises(CacheFeatureError, match="work queue is closed"):
        await queue.publish("tasks", "default", b"payload")


@pytest.mark.anyio
async def test_memory_features_pass_shared_conformance() -> None:
    clock = FakeClock()
    features = InMemoryCacheFeatures(
        atomic=InMemoryAtomicCache(clock),
        leases=InMemoryLeaseCache(clock),
        work_queue=InMemoryWorkQueue(clock),
        streams=InMemoryStreamCache(max_records=2),
        pubsub=InMemoryPubSubCache(),
        schedules=InMemoryScheduleCache(clock),
    )

    async def advance(seconds: float) -> None:
        clock.advance(seconds)

    await assert_atomic_conformance(features.atomic)
    await assert_lease_conformance(features.leases, advance)
    await assert_work_queue_conformance(features.work_queue, advance)
    await assert_stream_conformance(features.streams, retention_count=2)
    await assert_pubsub_conformance(features.pubsub)
    await assert_schedule_conformance(features.schedules, clock(), advance)


def test_redis_duration_milliseconds_preserves_positive_submilliseconds() -> None:
    runtime = RedisCacheRuntime("redis://cache/0", namespace="cache")

    assert runtime.duration_milliseconds(0.0004, label="visibility timeout") == 1
    assert (
        runtime.duration_milliseconds(
            0,
            label="work delay",
            allow_zero=True,
        )
        == 0
    )


@pytest.mark.anyio
async def test_redis_work_queue_reject_preserves_positive_submilliseconds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = RedisWorkQueue(RedisCacheRuntime("redis://cache", "safe"))
    captured: dict[str, int] = {}

    async def capture_settlement(
        self: RedisWorkQueue,
        delivery: WorkDelivery,
        *,
        action: str,
        delay_milliseconds: int,
    ) -> None:
        del self, delivery
        assert action == "reject"
        captured["delay"] = delay_milliseconds

    monkeypatch.setattr(RedisWorkQueue, "_settle", capture_settlement)
    delivery = WorkDelivery(
        queue="default",
        identity=WorkIdentity("identity"),
        payload=b"payload",
        attempt=1,
        visible_until=0,
        receipt="receipt",
    )

    await queue.reject(delivery, delay=0.0004)

    assert captured == {"delay": 1}


@pytest.mark.anyio
async def test_redis_work_queue_cancellation_cleans_generated_consumer(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = RedisWorkQueue(RedisCacheRuntime("redis://cache", "safe"))
    reserve_started = asyncio.Event()
    cleanup_started = asyncio.Event()
    release_cleanup = asyncio.Event()
    cleanup_finished = asyncio.Event()

    async def blocking_reserve(
        self: RedisWorkQueue,
        keys: object,
        consumer: str,
        _visibility_timeout: float,
        _visibility_milliseconds: int,
        _wait_timeout: float,
    ) -> None:
        self._consumers.add((keys, consumer))  # type: ignore[arg-type]
        reserve_started.set()
        await asyncio.Event().wait()

    async def delayed_cleanup(
        _self: RedisWorkQueue,
        _keys: object,
        _consumer: str,
    ) -> None:
        cleanup_started.set()
        await release_cleanup.wait()
        cleanup_finished.set()

    monkeypatch.setattr(RedisWorkQueue, "_reserve", blocking_reserve)
    monkeypatch.setattr(
        RedisWorkQueue,
        "_remove_consumer_if_idle",
        delayed_cleanup,
    )
    reservation = asyncio.create_task(
        queue.reserve(
            "tasks",
            "default",
            "worker",
            visibility_timeout=30,
            wait_timeout=10,
        )
    )

    await reserve_started.wait()
    reservation.cancel()
    await cleanup_started.wait()

    assert not reservation.done()
    assert queue._consumers == set()
    release_cleanup.set()
    with pytest.raises(asyncio.CancelledError):
        await reservation
    assert cleanup_finished.is_set()


async def _memory_backend() -> CacheBackend:
    return CacheBackend(InMemoryCache())


class FakeClock:
    def __init__(self, value: float = 100.0) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value

    def advance(self, seconds: float) -> None:
        self.value += seconds


class CancelsOnceCloser:
    def __init__(self) -> None:
        self.calls = 0

    async def close(self) -> None:
        self.calls += 1
        if self.calls == 1:
            raise asyncio.CancelledError


class TrackingCloser:
    def __init__(self) -> None:
        self.calls = 0

    async def close(self) -> None:
        self.calls += 1


@pytest.mark.anyio
async def test_memory_feature_close_finishes_other_cleanup_and_remains_retryable() -> (
    None
):
    work_queue = CancelsOnceCloser()
    pubsub = TrackingCloser()
    features = InMemoryCacheFeatures(
        work_queue=work_queue,
        pubsub=pubsub,
    )

    with pytest.raises(asyncio.CancelledError):
        await features.close()

    assert work_queue.calls == 1
    assert pubsub.calls == 1

    await features.close()
    await features.close()

    assert work_queue.calls == 2
    assert pubsub.calls == 1


@pytest.mark.anyio
async def test_atomic_create_has_one_concurrent_winner() -> None:
    atomic = InMemoryAtomicCache()

    results = await asyncio.gather(
        *(
            atomic.create("tasks", "same", str(index).encode(), ttl=30)
            for index in range(20)
        )
    )

    winners = [result for result in results if result is not None]
    assert len(winners) == 1
    assert await atomic.get("tasks", "same") == winners[0]


@pytest.mark.anyio
async def test_atomic_compare_and_swap_rejects_stale_revision() -> None:
    atomic = InMemoryAtomicCache()
    original = await atomic.create("tasks", "key", b"one", ttl=30)
    assert original is not None

    updated = await atomic.compare_and_swap(
        "tasks",
        "key",
        original.revision,
        b"two",
        ttl=30,
    )

    assert updated is not None
    assert (
        await atomic.compare_and_swap(
            "tasks",
            "key",
            original.revision,
            b"stale",
            ttl=30,
        )
        is None
    )
    assert await atomic.get("tasks", "key") == updated


@pytest.mark.anyio
async def test_atomic_counters_return_unique_sequence_positions() -> None:
    atomic = InMemoryAtomicCache()

    results = await asyncio.gather(
        *(atomic.increment("tasks", "sequence", ttl=30) for _ in range(20))
    )

    assert sorted(result.value for result in results) == list(range(1, 21))
    assert len({result.revision for result in results}) == 20


@pytest.mark.anyio
async def test_atomic_values_expire_against_injected_clock() -> None:
    clock = FakeClock()
    atomic = InMemoryAtomicCache(clock)
    assert await atomic.create("tasks", "key", b"value", ttl=5) is not None

    clock.advance(5)

    assert await atomic.get("tasks", "key") is None


@pytest.mark.anyio
async def test_atomic_entry_capacity_is_global_and_expiry_releases_it() -> None:
    clock = FakeClock()
    atomic = InMemoryAtomicCache(clock, max_entries=1)
    await atomic.create("tasks", "one", b"one", ttl=5)

    with pytest.raises(CacheFeatureError, match="entry capacity"):
        await atomic.create("other", "two", b"two", ttl=5)

    clock.advance(5)
    assert await atomic.create("other", "two", b"two", ttl=5) is not None


@pytest.mark.anyio
async def test_lease_expiry_issues_new_fencing_token() -> None:
    clock = FakeClock()
    leases = InMemoryLeaseCache(clock)
    first = await leases.acquire("tasks", "scheduler", "worker-1", ttl=5)
    assert first is not None
    assert await leases.acquire("tasks", "scheduler", "worker-2", ttl=5) is None

    clock.advance(5)
    second = await leases.acquire("tasks", "scheduler", "worker-2", ttl=5)

    assert second is not None
    assert second.fencing_token.value > first.fencing_token.value
    with pytest.raises(CacheConflictError, match="stale or no longer held"):
        await leases.renew(first, ttl=5)


@pytest.mark.anyio
async def test_lease_renewal_and_release_require_current_token() -> None:
    leases = InMemoryLeaseCache()
    lease = await leases.acquire("tasks", "scheduler", "worker", ttl=5)
    assert lease is not None

    renewed = await leases.renew(lease, ttl=10)
    await leases.release(renewed)

    with pytest.raises(CacheConflictError, match="stale or no longer held"):
        await leases.release(renewed)


@pytest.mark.anyio
async def test_lease_capacity_is_global_and_release_reuses_it() -> None:
    leases = InMemoryLeaseCache(max_leases=1)
    first = await leases.acquire("tasks", "one", "worker", ttl=5)
    assert first is not None

    with pytest.raises(CacheFeatureError, match="lease capacity"):
        await leases.acquire("other", "two", "worker", ttl=5)

    await leases.release(first)
    second = await leases.acquire("other", "two", "worker", ttl=5)
    assert second is not None
    assert second.fencing_token > first.fencing_token


@pytest.mark.anyio
async def test_work_queue_reserves_each_item_once_until_visibility_expires() -> None:
    clock = FakeClock()
    queue = InMemoryWorkQueue(clock)
    identity = await queue.publish("tasks", "default", b"payload")

    first = await queue.reserve(
        "tasks",
        "default",
        "worker-1",
        visibility_timeout=5,
    )
    assert first is not None
    assert first.identity == identity
    assert (
        await queue.reserve(
            "tasks",
            "default",
            "worker-2",
            visibility_timeout=5,
        )
        is None
    )

    clock.advance(5)
    second = await queue.reserve(
        "tasks",
        "default",
        "worker-2",
        visibility_timeout=5,
    )

    assert second is not None
    assert second.identity == identity
    assert second.attempt == 2
    with pytest.raises(CacheConflictError, match="stale or no longer reserved"):
        await queue.acknowledge(first)
    await queue.acknowledge(second)


@pytest.mark.anyio
async def test_work_queue_delays_publication_and_retry() -> None:
    clock = FakeClock()
    queue = InMemoryWorkQueue(clock)
    await queue.publish("tasks", "default", b"payload", delay=5)

    assert (
        await queue.reserve(
            "tasks",
            "default",
            "worker",
            visibility_timeout=5,
        )
        is None
    )
    clock.advance(5)
    first = await queue.reserve(
        "tasks",
        "default",
        "worker",
        visibility_timeout=5,
    )
    assert first is not None
    await queue.reject(first, delay=10)

    assert (
        await queue.reserve(
            "tasks",
            "default",
            "worker",
            visibility_timeout=5,
        )
        is None
    )
    clock.advance(10)
    second = await queue.reserve(
        "tasks",
        "default",
        "worker",
        visibility_timeout=5,
    )
    assert second is not None
    assert second.identity == first.identity
    assert second.attempt == 2


@pytest.mark.anyio
async def test_work_queue_dead_letters_after_attempt_boundary() -> None:
    clock = FakeClock()
    queue = InMemoryWorkQueue(clock)
    await queue.publish("tasks", "default", b"payload", max_attempts=1)
    delivery = await queue.reserve(
        "tasks",
        "default",
        "worker",
        visibility_timeout=5,
    )
    assert delivery is not None

    clock.advance(5)

    assert (
        await queue.reserve(
            "tasks",
            "default",
            "worker",
            visibility_timeout=5,
        )
        is None
    )
    dead = await queue.dead_letters("tasks", "default")
    assert len(dead) == 1
    assert dead[0].identity == delivery.identity


@pytest.mark.anyio
async def test_work_queue_consumer_can_dead_letter_terminal_failure() -> None:
    queue = InMemoryWorkQueue()
    identity = await queue.publish(
        "tasks",
        "default",
        b"invalid",
        max_attempts=10,
    )
    delivery = await queue.reserve(
        "tasks",
        "default",
        "worker",
        visibility_timeout=5,
    )
    assert delivery is not None

    await queue.dead_letter(delivery)

    assert (
        await queue.reserve(
            "tasks",
            "default",
            "worker",
            visibility_timeout=5,
        )
        is None
    )
    assert [dead.identity for dead in await queue.dead_letters("tasks", "default")] == [
        identity
    ]


@pytest.mark.anyio
async def test_work_queue_rejects_publication_at_capacity() -> None:
    queue = InMemoryWorkQueue(max_items_per_queue=1)
    await queue.publish("tasks", "default", b"one")

    with pytest.raises(CacheFeatureError, match="item capacity"):
        await queue.publish("tasks", "default", b"two")


@pytest.mark.anyio
async def test_work_queue_bounds_namespaces_without_retaining_empty_misses() -> None:
    queue = InMemoryWorkQueue(max_queues=1)
    assert (
        await queue.reserve(
            "tasks",
            "missing",
            "worker",
            visibility_timeout=5,
        )
        is None
    )
    await queue.publish("tasks", "one", b"one")

    with pytest.raises(CacheFeatureError, match="queue capacity"):
        await queue.publish("tasks", "two", b"two")

    delivery = await queue.reserve(
        "tasks",
        "one",
        "worker",
        visibility_timeout=5,
    )
    assert delivery is not None
    await queue.acknowledge(delivery)
    await queue.publish("tasks", "two", b"two")


@pytest.mark.anyio
async def test_work_queue_wait_is_cancellation_safe() -> None:
    queue = InMemoryWorkQueue()
    waiting = asyncio.create_task(
        queue.reserve(
            "tasks",
            "default",
            "worker",
            visibility_timeout=5,
            wait_timeout=30,
        )
    )
    await asyncio.sleep(0)

    waiting.cancel()

    with pytest.raises(asyncio.CancelledError):
        await waiting
    await queue.publish("tasks", "default", b"payload")
    assert (
        await queue.reserve(
            "tasks",
            "default",
            "worker",
            visibility_timeout=5,
        )
        is not None
    )


@pytest.mark.anyio
async def test_work_queue_wait_wakes_for_delayed_item() -> None:
    queue = InMemoryWorkQueue()
    await queue.publish("tasks", "default", b"payload", delay=0.01)

    delivery = await queue.reserve(
        "tasks",
        "default",
        "worker",
        visibility_timeout=5,
        wait_timeout=0.2,
    )

    assert delivery is not None


@pytest.mark.anyio
async def test_stream_replays_in_order_and_tracks_consumer_position() -> None:
    stream = InMemoryStreamCache()
    first = await stream.append("tasks", "lifecycle", b"one")
    second = await stream.append("tasks", "lifecycle", b"two")

    records = await stream.read_consumer("tasks", "lifecycle", "projection")
    assert [record.position for record in records] == [first, second]

    await stream.acknowledge("tasks", "lifecycle", "projection", first)

    resumed = await stream.read_consumer("tasks", "lifecycle", "projection")
    assert [record.position for record in resumed] == [second]


@pytest.mark.anyio
async def test_stream_reports_evicted_replay_position() -> None:
    stream = InMemoryStreamCache(max_records=2)
    await stream.append("tasks", "lifecycle", b"one")
    await stream.append("tasks", "lifecycle", b"two")
    await stream.append("tasks", "lifecycle", b"three")
    await stream.append("tasks", "lifecycle", b"four")

    with pytest.raises(CachePositionExpiredError, match="no longer retained"):
        await stream.read(
            "tasks",
            "lifecycle",
            after=StreamPosition(1),
        )


@pytest.mark.anyio
async def test_stream_bounds_namespaces_and_consumer_positions() -> None:
    stream = InMemoryStreamCache(max_streams=1, max_consumers=1)
    assert await stream.read("tasks", "missing") == ()
    first = await stream.append("tasks", "one", b"one")

    with pytest.raises(CacheFeatureError, match="stream capacity"):
        await stream.append("tasks", "two", b"two")

    await stream.acknowledge("tasks", "one", "first", first)
    with pytest.raises(CacheFeatureError, match="consumer capacity"):
        await stream.acknowledge("tasks", "one", "second", first)


@pytest.mark.anyio
async def test_stream_bounds_and_releases_consumers_per_logical_stream() -> None:
    stream = InMemoryStreamCache(max_consumers=1)
    first = await stream.append("tasks", "first", b"one")
    second = await stream.append("tasks", "second", b"two")
    await stream.acknowledge("tasks", "first", "projection", first)
    await stream.acknowledge("tasks", "second", "projection", second)

    with pytest.raises(CacheFeatureError, match="consumer capacity"):
        await stream.acknowledge("tasks", "first", "other", first)

    assert await stream.forget_consumer("tasks", "first", "projection")
    await stream.acknowledge("tasks", "first", "other", first)


@pytest.mark.anyio
async def test_pubsub_delivers_only_to_active_subscribers() -> None:
    pubsub = InMemoryPubSubCache()
    assert await pubsub.publish("tasks", "updates", b"before") == 0
    subscription = await pubsub.subscribe("tasks", "updates")

    assert await pubsub.publish("tasks", "updates", b"during") == 1
    assert await subscription.receive(timeout=1) == b"during"
    await subscription.close()

    assert await pubsub.publish("tasks", "updates", b"after") == 0


@pytest.mark.anyio
async def test_pubsub_bounds_topics_without_retaining_empty_publications() -> None:
    pubsub = InMemoryPubSubCache(max_topics=1, max_subscriptions=1)
    assert await pubsub.publish("tasks", "missing", b"before") == 0
    first = await pubsub.subscribe("tasks", "one")

    with pytest.raises(CacheFeatureError, match="topic capacity"):
        await pubsub.subscribe("tasks", "two")
    with pytest.raises(CacheFeatureError, match="subscription capacity"):
        await pubsub.subscribe("tasks", "one")

    await first.close()
    second = await pubsub.subscribe("tasks", "two")
    await second.close()


@pytest.mark.anyio
async def test_pubsub_receive_preserves_cancellation_and_can_close() -> None:
    pubsub = InMemoryPubSubCache()
    subscription = await pubsub.subscribe("tasks", "updates")
    waiting = asyncio.create_task(subscription.receive())
    await asyncio.sleep(0)

    waiting.cancel()

    with pytest.raises(asyncio.CancelledError):
        await waiting
    await subscription.close()
    await pubsub.close()


@pytest.mark.anyio
async def test_pubsub_shutdown_wakes_blocked_receiver() -> None:
    pubsub = InMemoryPubSubCache()
    subscription = await pubsub.subscribe("tasks", "updates")
    waiting = asyncio.create_task(subscription.receive())
    await asyncio.sleep(0)

    await pubsub.close()

    with pytest.raises(CacheFeatureError, match="subscription is closed"):
        await waiting


@pytest.mark.anyio
async def test_schedule_updates_require_current_revision() -> None:
    schedules = InMemoryScheduleCache(FakeClock())
    original = await schedules.create(
        "tasks",
        "nightly",
        b"one",
        next_due_at=110,
    )
    assert original is not None

    updated = await schedules.update(
        "tasks",
        "nightly",
        original.revision,
        b"two",
        next_due_at=120,
    )

    assert updated is not None
    assert (
        await schedules.update(
            "tasks",
            "nightly",
            original.revision,
            b"stale",
            next_due_at=130,
        )
        is None
    )


@pytest.mark.anyio
async def test_schedule_store_rejects_creation_at_capacity() -> None:
    schedules = InMemoryScheduleCache(FakeClock(), max_records=1)
    await schedules.create("tasks", "one", b"one", next_due_at=100)

    with pytest.raises(CacheFeatureError, match="record capacity"):
        await schedules.create("tasks", "two", b"two", next_due_at=100)


@pytest.mark.parametrize(
    "factory",
    (
        InMemoryScheduleCache,
        lambda *, max_records: RedisScheduleCache(
            RedisCacheRuntime("redis://cache", "safe"),
            max_records=max_records,
        ),
    ),
)
def test_schedule_capacity_cannot_exceed_the_due_query_bound(factory) -> None:
    with pytest.raises(ValueError, match="cannot exceed"):
        factory(max_records=MAX_CACHE_FEATURE_LIMIT + 1)


@pytest.mark.anyio
async def test_schedule_due_queries_and_claims_are_competition_safe() -> None:
    clock = FakeClock()
    schedules = InMemoryScheduleCache(clock)
    await schedules.create("tasks", "later", b"later", next_due_at=120)
    due = await schedules.create("tasks", "due", b"due", next_due_at=100)
    assert due is not None

    assert await schedules.due("tasks", before=110) == (due,)
    claims = await asyncio.gather(
        schedules.claim("tasks", "due", "scheduler-1", ttl=5),
        schedules.claim("tasks", "due", "scheduler-2", ttl=5),
    )

    assert len([claim for claim in claims if claim is not None]) == 1
    assert await schedules.due("tasks", before=110) == ()


@pytest.mark.anyio
async def test_schedule_expired_claim_gets_new_fencing_token() -> None:
    clock = FakeClock()
    schedules = InMemoryScheduleCache(clock)
    await schedules.create("tasks", "due", b"due", next_due_at=100)
    first = await schedules.claim("tasks", "due", "scheduler-1", ttl=5)
    assert first is not None

    clock.advance(5)
    second = await schedules.claim("tasks", "due", "scheduler-2", ttl=5)

    assert second is not None
    assert second.fencing_token.value > first.fencing_token.value
    with pytest.raises(CacheConflictError, match="claim.*stale"):
        await schedules.release(first)


@pytest.mark.anyio
async def test_schedule_update_preserves_existing_claim() -> None:
    clock = FakeClock()
    schedules = InMemoryScheduleCache(clock)
    record = await schedules.create("tasks", "due", b"due", next_due_at=100)
    assert record is not None
    claim = await schedules.claim("tasks", "due", "scheduler", ttl=5)
    assert claim is not None

    assert (
        await schedules.update(
            "tasks",
            "due",
            record.revision,
            b"updated",
            next_due_at=100,
        )
        is None
    )
    assert await schedules.claim("tasks", "due", "other", ttl=5) is None
    assert await schedules.complete(claim) is None


@pytest.mark.anyio
async def test_schedule_update_succeeds_after_claim_release() -> None:
    clock = FakeClock()
    schedules = InMemoryScheduleCache(clock)
    record = await schedules.create("tasks", "due", b"due", next_due_at=100)
    assert record is not None
    claim = await schedules.claim("tasks", "due", "scheduler", ttl=5)
    assert claim is not None
    await schedules.release(claim)

    updated = await schedules.update(
        "tasks",
        "due",
        record.revision,
        b"updated",
        next_due_at=100,
    )

    assert updated is not None


@pytest.mark.anyio
async def test_schedule_completion_removes_once_and_advances_recurring() -> None:
    clock = FakeClock()
    schedules = InMemoryScheduleCache(clock)
    await schedules.create("tasks", "once", b"once", next_due_at=100)
    await schedules.create(
        "tasks",
        "recurring",
        b"recurring",
        next_due_at=100,
        interval_seconds=10,
    )
    once_claim = await schedules.claim("tasks", "once", "scheduler", ttl=5)
    recurring_claim = await schedules.claim(
        "tasks",
        "recurring",
        "scheduler",
        ttl=5,
    )
    assert once_claim is not None
    assert recurring_claim is not None

    assert await schedules.complete(once_claim) is None
    recurring = await schedules.complete(recurring_claim)

    assert recurring is not None
    assert recurring.next_due_at == 110
    assert await schedules.due("tasks", before=100) == ()


@pytest.mark.anyio
async def test_schedule_advance_replaces_claimed_record_and_releases_claim() -> None:
    clock = FakeClock()
    schedules = InMemoryScheduleCache(clock)
    record = await schedules.create(
        "tasks",
        "hourly",
        b"original",
        next_due_at=100,
        interval_seconds=60,
    )
    assert record is not None
    claim = await schedules.claim("tasks", "hourly", "scheduler", ttl=5)
    assert claim is not None

    advanced = await schedules.advance(
        claim,
        b"retained",
        next_due_at=160,
    )

    assert advanced.payload == b"retained"
    assert advanced.next_due_at == 160
    assert advanced.interval_seconds == 60
    assert advanced.revision > record.revision
    assert await schedules.claim("tasks", "hourly", "other", ttl=5) is None
    clock.value = 160
    recurring_claim = await schedules.claim("tasks", "hourly", "other", ttl=5)
    assert recurring_claim is not None
    recurring = await schedules.complete(recurring_claim)
    assert recurring is not None
    assert recurring.interval_seconds == 60
    assert recurring.next_due_at == 220


@pytest.mark.anyio
async def test_schedule_deletion_invalidates_a_live_claim() -> None:
    schedules = InMemoryScheduleCache(FakeClock())
    await schedules.create("tasks", "once", b"payload", next_due_at=100)
    claim = await schedules.claim("tasks", "once", "scheduler", ttl=5)
    assert claim is not None

    assert await schedules.delete("tasks", "once")
    assert not await schedules.delete("tasks", "once")
    assert not await schedules.held(claim)
    assert await schedules.due("tasks", before=100) == ()
    with pytest.raises(CacheConflictError, match="claim.*stale"):
        await schedules.complete(claim)


@pytest.mark.anyio
async def test_schedule_completion_handles_sub_resolution_interval() -> None:
    clock = FakeClock(1_700_000_000.0)
    schedules = InMemoryScheduleCache(clock)
    await schedules.create(
        "tasks",
        "recurring",
        b"recurring",
        next_due_at=clock(),
        interval_seconds=1e-9,
    )
    claim = await schedules.claim("tasks", "recurring", "scheduler", ttl=5)
    assert claim is not None

    recurring = await schedules.complete(claim)

    assert recurring is not None
    assert recurring.next_due_at > clock()
