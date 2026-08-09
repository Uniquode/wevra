from __future__ import annotations

import asyncio
import importlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from jinja2 import Environment
from jinja2.exceptions import TemplateRuntimeError

from wybra.cache import (
    CacheBackend,
    CacheCapability,
    CacheFeatureError,
    CacheNotFoundError,
    CachesCapability,
    CacheSettings,
    CachesSettings,
    InMemoryCache,
    NatsJetStreamCache,
    RedisCache,
    UncachedCacheValue,
    build_caches,
    cache_provider_configured,
    setup_site,
)
from wybra.cache.feature_models import MAX_CACHE_VALUE_BYTES
from wybra.cache.nats_runtime import NatsJetStreamRuntime
from wybra.cache.redis_connection import resolve_redis_urls
from wybra.cache.redis_features import RedisCacheFeatures
from wybra.cache.redis_runtime import RedisCacheRuntime
from wybra.config import (
    ConfigService,
    ConfigSourceError,
    FileConfigSource,
    MappingConfigSource,
)
from wybra.core.exceptions import ConfigurationError
from wybra.events import Event, EventsCapability, event_scope
from wybra.events.cache import CacheOperationCompletedEvent, CacheOperationFailedEvent
from wybra.secrets import DefaultSecretsCapability, EnvironmentSecretSourceDriver
from wybra.site import Site, SiteCapabilityError, start
from wybra.template import DefaultTemplateCapability, TemplateCapability
from wybra.template.cache import configure_cache_extension

EVT_CACHE = event_scope("cache")


def test_cache_provider_discovery_accepts_one_shot_iterables(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discovery_module = importlib.import_module("wybra.cache.discovery")
    monkeypatch.setattr(
        discovery_module,
        "import_module",
        lambda _module_name: SimpleNamespace(provides_cache_capability=True),
    )

    assert cache_provider_configured(iter(("custom.cache",))) is True


async def _cache_provider(cache: CacheCapability) -> CacheCapability:
    return cache


async def _healthy_ping() -> bool:
    return True


def invalid_redis_cache_settings() -> CacheSettings:
    settings = object.__new__(CacheSettings)
    object.__setattr__(settings, "name", "default")
    object.__setattr__(settings, "backend", "redis")
    object.__setattr__(settings, "url", None)
    object.__setattr__(settings, "url_source", None)
    object.__setattr__(settings, "url_key", None)
    object.__setattr__(settings, "credentials_source", None)
    object.__setattr__(settings, "credentials_key", None)
    return settings


@asynccontextmanager
async def _started_events_site() -> AsyncIterator[Site]:
    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": (), "deployment_environment": "local"},
                "wybra.events": {"enabled": True},
            }
        ),
    )
    try:
        yield site
    finally:
        await site.close()


class TestCacheSettings:
    def test_defaults_to_memory_backend(self) -> None:
        settings = CacheSettings.load_settings({"cache": {}})

        assert settings.name == "default"
        assert settings.backend == "memory"
        assert settings.url is None
        assert settings.namespace is None
        assert settings.resolved_namespace == "default"
        assert settings.features is None

    def test_loads_nats_jetstream_cache_servers_and_namespace(self) -> None:
        settings = CacheSettings.load_settings(
            {
                "cache": {
                    "backend": "nats-jetstream",
                    "servers": (
                        "nats://first.internal:4222",
                        "nats://second.internal:4222",
                    ),
                    "namespace": "task_cache",
                }
            }
        )

        assert settings.backend == "nats-jetstream"
        assert settings.servers == (
            "nats://first.internal:4222",
            "nats://second.internal:4222",
        )
        assert settings.resolved_namespace == "task_cache"

    @pytest.mark.parametrize(
        ("values", "match"),
        (
            ({"backend": "nats-jetstream"}, "servers is required"),
            (
                {"backend": "nats-jetstream", "servers": []},
                "servers must contain at least one",
            ),
            (
                {
                    "backend": "nats-jetstream",
                    "servers": ["nats://cache.internal:4222"],
                    "url": "redis://cache/0",
                },
                "url is not valid",
            ),
            (
                {
                    "backend": "memory",
                    "servers": ["nats://cache.internal:4222"],
                },
                "servers is not valid",
            ),
            (
                {
                    "backend": "redis",
                    "url": "redis://cache/0",
                    "servers": ["nats://cache.internal:4222"],
                },
                "servers is not valid",
            ),
        ),
    )
    def test_rejects_incompatible_nats_jetstream_connection_settings(
        self,
        values: dict[str, object],
        match: str,
    ) -> None:
        with pytest.raises(ConfigurationError, match=match):
            CacheSettings(**values)

    def test_named_nats_jetstream_cache_allows_environment_server_override(
        self,
    ) -> None:
        ConfigService.set_runtime_environment(
            {
                "WYBRA_CACHE__TASKS__BACKEND": "nats-jetstream",
                "WYBRA_CACHE__TASKS__SERVERS": (
                    "nats://first.internal:4222,nats://second.internal:4222"
                ),
            }
        )
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "cache": {},
                        "cache.tasks": {"backend": "memory"},
                    }
                )
            ],
            config_defs=(CacheSettings.module_config,),
            discover_module_config=False,
        )

        settings = CachesSettings.load_settings(config)

        assert settings.require("tasks").servers == (
            "nats://first.internal:4222",
            "nats://second.internal:4222",
        )

    def test_nats_jetstream_partition_diagnostics_are_secret_safe(self) -> None:
        first = CacheSettings(
            backend="nats-jetstream",
            servers=("nats://user:secret@cache.internal:4222",),
        )
        second = CacheSettings(
            backend="nats-jetstream",
            servers=("nats://user:rotated@cache.internal:4222",),
        )

        assert first.partition == second.partition
        assert "secret" not in first.partition

    def test_loads_independent_default_and_named_caches(self) -> None:
        settings = CachesSettings.load_settings(
            {
                "cache": {
                    "backend": "redis",
                    "url": "redis://default-secret@cache/0",
                },
                "cache.session": {"backend": "memory"},
                "cache.tasks": {
                    "backend": "redis",
                    "url": "redis://task-secret@cache/2",
                },
            }
        )

        assert settings.require("default").backend == "redis"
        assert settings.require("session") == CacheSettings(name="session")
        assert settings.require("tasks").url == "redis://task-secret@cache/2"

    def test_loads_default_and_named_caches_from_application_toml(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "app.toml"
        config_path.write_text(
            """
[app]
modules = ["wybra.cache"]

[app.templates]
auto_reload = true
cache_size = 0

[app.assets]
url_path = "/static/"

[cache]
backend = "redis"
url = "redis://cache/0"

[cache.session]
backend = "memory"

["cache.tasks"]
backend = "memory"
""".strip(),
            encoding="utf-8",
        )
        ConfigService.set_runtime_environment(
            {
                "WYBRA_CACHE__SESSION__BACKEND": "redis",
                "WYBRA_CACHE__SESSION__URL": "redis://cache/1",
            }
        )

        config = ConfigService([FileConfigSource(config_path, project_root=tmp_path)])
        settings = CachesSettings.load_settings(config)

        assert [(item.name, item.backend) for item in settings.instances] == [
            ("default", "redis"),
            ("session", "redis"),
            ("tasks", "memory"),
        ]
        assert settings.require("default").url == "redis://cache/0"
        assert settings.require("session").url == "redis://cache/1"
        assert config.config.sources["cache.session.backend"] == "environment"
        assert config.config.sources["cache.session.url"] == "environment"

    def test_rejects_ambiguous_nested_and_quoted_cache_tables(
        self,
        tmp_path: Path,
    ) -> None:
        config_path = tmp_path / "app.toml"
        config_path.write_text(
            """
[app]
modules = ["wybra.cache"]

[app.templates]
auto_reload = true
cache_size = 0

[app.assets]
url_path = "/static/"

[cache.session]
backend = "memory"

["cache.session"]
backend = "redis"
url = "redis://cache/1"
""".strip(),
            encoding="utf-8",
        )

        with pytest.raises(
            ConfigSourceError,
            match="both nested and quoted dotted cache tables",
        ):
            ConfigService([FileConfigSource(config_path, project_root=tmp_path)])

    @pytest.mark.parametrize(
        "section",
        (
            "cache.default",
            "cache.bad-name",
            "cache.deeply.nested",
            "cache.",
            "cache. session ",
        ),
    )
    def test_rejects_invalid_or_reserved_named_cache_sections(
        self, section: str
    ) -> None:
        with pytest.raises(ConfigurationError, match="cache name"):
            CachesSettings.load_settings({"cache": {}, section: {}})

    def test_rejects_backend_fields_irrelevant_to_memory_cache(self) -> None:
        with pytest.raises(
            ConfigurationError,
            match=r"cache\.session\.url.+memory",
        ):
            CachesSettings.load_settings(
                {
                    "cache": {},
                    "cache.session": {
                        "backend": "memory",
                        "url": "redis://cache/1",
                    },
                }
            )

    def test_resolves_redis_namespace_and_feature_selection(self) -> None:
        settings = CachesSettings.load_settings(
            {
                "cache": {
                    "backend": "redis",
                    "url": "redis://cache/0",
                },
                "cache.messages": {
                    "backend": "redis",
                    "url": "redis://cache/0",
                    "namespace": "website_messages",
                    "features": ["atomic"],
                },
                "cache.baseline": {
                    "backend": "redis",
                    "url": "redis://cache/0",
                    "features": [],
                },
            }
        )

        assert settings.require("default").resolved_namespace == "default"
        assert settings.require("default").features is None
        assert settings.require("messages").resolved_namespace == "website_messages"
        assert settings.require("messages").features == ("atomic",)
        assert settings.require("baseline").features == ()

    def test_accepts_redis_live_pubsub_feature_selection(self) -> None:
        settings = CachesSettings.load_settings(
            {
                "cache": {
                    "backend": "redis",
                    "url": "redis://cache/0",
                    "features": ["pub-sub"],
                }
            }
        )

        assert settings.require("default").features == ("pub-sub",)

    def test_treats_blank_environment_feature_selection_as_unset(self) -> None:
        ConfigService.set_runtime_environment({"WYBRA_CACHE_FEATURES": "   "})
        config = ConfigService(
            [
                MappingConfigSource(
                    {"cache": {"backend": "redis", "url": "redis://cache"}}
                )
            ],
            config_defs=(CacheSettings.module_config,),
            discover_module_config=False,
        )

        settings = CachesSettings.load_settings(config)

        assert settings.require("default").features is None

    @pytest.mark.parametrize(
        ("field", "value", "match"),
        (
            ("namespace", "", "non-blank"),
            ("namespace", "Redis://secret", "cache namespace"),
            ("namespace", "contains:separator", "cache namespace"),
            ("features", ["atomic", "atomic"], "duplicates"),
            ("features", ["unknown"], "not implemented"),
        ),
    )
    def test_rejects_invalid_redis_advanced_configuration(
        self,
        field: str,
        value: object,
        match: str,
    ) -> None:
        with pytest.raises(ConfigurationError, match=match):
            CachesSettings.load_settings(
                {
                    "cache": {
                        "backend": "redis",
                        "url": "redis://cache/0",
                        field: value,
                    }
                }
            )

    def test_rejects_namespace_for_memory_cache(self) -> None:
        with pytest.raises(ConfigurationError, match=r"namespace.+memory"):
            CachesSettings.load_settings({"cache": {"namespace": "memory_partition"}})

    def test_applies_named_environment_override_only_to_target_cache(self) -> None:
        ConfigService.set_runtime_environment(
            {
                "WYBRA_CACHE__SESSION__BACKEND": "redis",
                "WYBRA_CACHE__SESSION__FEATURES": "atomic",
                "WYBRA_CACHE__SESSION__NAMESPACE": "website_sessions",
                "WYBRA_CACHE__SESSION__URL": "redis://session-secret@cache/1",
            }
        )
        config = ConfigService(
            [
                MappingConfigSource(
                    {
                        "cache": {},
                        "cache.session": {"backend": "memory"},
                        "cache.tasks": {"backend": "memory"},
                    }
                )
            ],
            config_defs=(CacheSettings.module_config,),
            discover_module_config=False,
        )

        settings = CachesSettings.load_settings(config)

        assert config.get_config("cache.session") == {
            "backend": "redis",
            "features": ("atomic",),
            "namespace": "website_sessions",
            "url": "redis://session-secret@cache/1",
        }
        assert config.config.sources["cache.session.backend"] == "environment"
        assert config.config.sources["cache.session.url"] == "environment"
        assert settings.require("session").backend == "redis"
        assert settings.require("session").url == "redis://session-secret@cache/1"
        assert settings.require("session").features == ("atomic",)
        assert settings.require("session").resolved_namespace == "website_sessions"
        assert settings.require("tasks") == CacheSettings(name="tasks")

    def test_configuration_diagnostics_are_secret_safe(self) -> None:
        settings = CachesSettings.load_settings(
            {
                "cache": {
                    "backend": "redis",
                    "url": "redis://user:password@cache.internal/4",
                },
                "cache.session": {"backend": "memory"},
            }
        )

        diagnostics = settings.diagnostics()

        assert [(item.name, item.backend) for item in diagnostics] == [
            ("default", "redis"),
            ("session", "memory"),
        ]
        assert diagnostics[0].partition != diagnostics[1].partition
        assert diagnostics[0].features == (
            "atomic",
            "lease",
            "pub-sub",
            "schedule",
            "stream",
            "time",
            "work-queue",
        )
        assert diagnostics[1].features == (
            "atomic",
            "lease",
            "pub-sub",
            "schedule",
            "stream",
            "time",
            "work-queue",
        )
        assert diagnostics[0].health == "configured"
        rendered = repr(diagnostics)
        assert "password" not in rendered
        assert "redis://" not in rendered

    def test_partition_diagnostics_do_not_depend_on_redis_credentials(self) -> None:
        first = CacheSettings(
            backend="redis",
            url="redis://first:secret@cache.internal/4",
        )
        second = CacheSettings(
            backend="redis",
            url="redis://second:rotated@cache.internal/4",
        )

        assert first.partition == second.partition

    def test_partition_diagnostics_distinguish_effective_redis_partitions(
        self,
    ) -> None:
        query_first = CacheSettings(
            backend="redis",
            url="redis://cache.internal?db=1",
        )
        query_second = CacheSettings(
            backend="redis",
            url="redis://cache.internal?db=2",
        )
        unix_first = CacheSettings(
            backend="redis",
            url="unix:///tmp/first.sock?db=0",
        )
        unix_second = CacheSettings(
            backend="redis",
            url="unix:///tmp/second.sock?db=0",
        )

        assert query_first.partition != query_second.partition
        assert unix_first.partition != unix_second.partition

    def test_partition_diagnostics_canonicalise_implicit_localhost(self) -> None:
        implicit = CacheSettings(backend="redis", url="redis:///4")
        explicit = CacheSettings(
            backend="redis",
            url="redis://localhost:6379/4",
        )

        assert implicit.partition == explicit.partition

    def test_partition_diagnostics_include_the_redis_namespace(self) -> None:
        first = CacheSettings(
            backend="redis",
            url="redis://cache.internal/4",
            namespace="first",
        )
        second = CacheSettings(
            backend="redis",
            url="redis://cache.internal/4",
            namespace="second",
        )

        assert first.partition != second.partition

    def test_resolves_keychain_redis_url_references_per_cache_name(self) -> None:
        settings = CachesSettings.load_settings(
            {
                "cache": {"backend": "redis", "url_source": "keychain"},
                "cache.session": {
                    "backend": "redis",
                    "url_source": "keychain",
                },
            }
        )

        assert settings.require("default").url_reference == (
            "keychain",
            "cache/redis/url",
        )
        assert settings.require("session").url_reference == (
            "keychain",
            "cache/session/redis/url",
        )

    def test_allows_a_credentialless_endpoint_with_shared_credentials(self) -> None:
        settings = CacheSettings(
            backend="redis",
            url="rediss://cache.internal/4",
            credentials_source="keychain",
        )

        assert settings.url == "rediss://cache.internal/4"
        assert settings.credentials_reference == (
            "keychain",
            "cache/redis/credentials",
        )

    @pytest.mark.parametrize(
        ("values", "match"),
        (
            (
                {
                    "backend": "redis",
                    "url_source": "environment",
                },
                "url_key",
            ),
            (
                {"backend": "redis", "url_key": "CACHE_URL"},
                "requires cache.url_source",
            ),
            (
                {
                    "backend": "redis",
                    "url": "redis://user:password@cache/0",
                    "credentials_source": "keychain",
                },
                "must not contain credentials",
            ),
        ),
    )
    def test_rejects_ambiguous_redis_secret_configuration(
        self,
        values: dict[str, str],
        match: str,
    ) -> None:
        with pytest.raises(ConfigurationError, match=match):
            CacheSettings(**values)

    def test_partition_rechecks_the_redis_url_invariant(self) -> None:
        with pytest.raises(
            ConfigurationError,
            match=r"cache\.url is required when backend is 'redis'",
        ):
            assert invalid_redis_cache_settings().partition

    def test_rejects_duplicate_settings_names(self) -> None:
        with pytest.raises(ConfigurationError, match="duplicates: session"):
            CachesSettings(
                instances=(
                    CacheSettings(),
                    CacheSettings(name="session"),
                    CacheSettings(name="session"),
                )
            )


class TestInMemoryCache:
    @pytest.mark.anyio
    async def test_publishes_safe_operation_outcomes(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        now = 100.0
        monkeypatch.setattr("wybra.cache.capabilities.time.monotonic", lambda: now)
        observed: list[Event] = []

        async def handler(event: Event) -> None:
            observed.append(event)

        async with _started_events_site() as site:
            await site.require_capability(EventsCapability).subscribe(
                EVT_CACHE, handler
            )
            cache = InMemoryCache()

            assert await cache.get("template", "private-user-key") is None
            await cache.set("template", "private-user-key", b"content", ttl=1)
            assert await cache.get("template", "private-user-key") == b"content"
            now = 102.0
            assert await cache.get("template", "private-user-key") is None
            await cache.delete("template", "private-user-key")

            async def failing_factory() -> bytes:
                raise RuntimeError("source unavailable")

            with pytest.raises(RuntimeError, match="source unavailable"):
                await cache.get_or_set(
                    "template",
                    "private-user-key",
                    ttl=60,
                    factory=failing_factory,
                )

        assert [str(event.scope) for event in observed] == [
            "cache.read.completed",
            "cache.set.completed",
            "cache.read.completed",
            "cache.read.completed",
            "cache.delete.completed",
            "cache.read.completed",
            "cache.fill.failed",
        ]
        completed = [
            event
            for event in observed
            if isinstance(event, CacheOperationCompletedEvent)
        ]
        assert [event.outcome for event in completed] == [
            "miss",
            "stored",
            "hit",
            "expired",
            "deleted",
            "miss",
        ]
        assert all(event.owner == "template" for event in completed)
        assert all(
            "private-user-key" not in event.key_fingerprint for event in completed
        )
        assert isinstance(observed[-1], CacheOperationFailedEvent)
        assert observed[-1].error_type == "RuntimeError"

    @pytest.mark.anyio
    async def test_expires_entries_after_ttl(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        now = 100.0
        monkeypatch.setattr("wybra.cache.capabilities.time.monotonic", lambda: now)
        cache = InMemoryCache()

        await cache.set("template", "fragment", b"content", ttl=30)

        now = 131.0
        assert await cache.get("template", "fragment") is None

    @pytest.mark.anyio
    async def test_owner_prefixes_entries_and_supports_operations(self) -> None:
        cache = InMemoryCache()

        await cache.set("template", "fragment", b"content", ttl=60)

        assert await cache.get("template", "fragment") == b"content"
        assert await cache.get("other", "fragment") is None
        await cache.delete("template", "fragment")
        assert await cache.get("template", "fragment") is None

    @pytest.mark.anyio
    async def test_cancelling_event_handler_cannot_cancel_cache_operation(self) -> None:
        async def cancelling_handler(event: Event) -> None:
            raise asyncio.CancelledError()

        async with _started_events_site() as site:
            await site.require_capability(EventsCapability).subscribe(
                EVT_CACHE, cancelling_handler
            )
            cache = InMemoryCache()

            await cache.set("template", "fragment", b"content", ttl=60)

            assert await cache.get("template", "fragment") == b"content"

    @pytest.mark.anyio
    async def test_rejects_colons_in_owner_names(self) -> None:
        cache = InMemoryCache()

        with pytest.raises(ValueError, match="must not contain ':'"):
            await cache.set("template:fragment", "content", b"value", ttl=60)

    @pytest.mark.anyio
    async def test_get_or_set_uses_factory_only_for_missing_entry(self) -> None:
        cache = InMemoryCache()
        calls = 0

        async def factory() -> bytes:
            nonlocal calls
            calls += 1
            return b"value"

        assert (
            await cache.get_or_set("template", "fragment", ttl=60, factory=factory)
            == b"value"
        )
        assert (
            await cache.get_or_set("template", "fragment", ttl=60, factory=factory)
            == b"value"
        )
        assert calls == 1

    @pytest.mark.parametrize("cached", (False, True), ids=("miss", "hit"))
    @pytest.mark.anyio
    async def test_get_or_set_rejects_invalid_ttl_before_reading_or_filling(
        self,
        cached: bool,
    ) -> None:
        cache = InMemoryCache()
        factory_calls = 0

        if cached:
            await cache.set("template", "fragment", b"cached", ttl=60)

        async def factory() -> bytes:
            nonlocal factory_calls
            factory_calls += 1
            return b"filled"

        with pytest.raises(ValueError, match="at least one second"):
            await cache.get_or_set(
                "template",
                "fragment",
                ttl=0.2,
                factory=factory,
            )

        assert factory_calls == 0

    @pytest.mark.anyio
    async def test_get_or_set_allows_only_one_concurrent_factory(self) -> None:
        cache = InMemoryCache()
        started = asyncio.Event()
        release = asyncio.Event()
        calls = 0

        async def factory() -> bytes:
            nonlocal calls
            calls += 1
            started.set()
            await release.wait()
            return b"value"

        async def unexpected_factory() -> bytes:
            pytest.fail("A waiting cache caller must not run its factory.")

        first = asyncio.create_task(
            cache.get_or_set("template", "fragment", ttl=60, factory=factory)
        )
        await started.wait()
        second = asyncio.create_task(
            cache.get_or_set(
                "template",
                "fragment",
                ttl=60,
                factory=unexpected_factory,
            )
        )
        await asyncio.sleep(0)
        release.set()

        assert await first == b"value"
        assert await second == b"value"
        assert calls == 1

    @pytest.mark.anyio
    async def test_get_or_set_shares_uncached_factory_output_without_failure_events(
        self,
    ) -> None:
        class ObservedInMemoryCache(InMemoryCache):
            def __init__(self) -> None:
                super().__init__()
                self.waiting_for_fill = asyncio.Event()

            async def _wait_for_fill(
                self,
                completed: asyncio.Event,
                *,
                timeout: float,
            ) -> None:
                self.waiting_for_fill.set()
                await super()._wait_for_fill(completed, timeout=timeout)

        cache = ObservedInMemoryCache()
        factory_started = asyncio.Event()
        release_factory = asyncio.Event()
        factory_calls = 0
        observed: list[Event] = []

        async def factory() -> UncachedCacheValue:
            nonlocal factory_calls
            factory_calls += 1
            factory_started.set()
            await release_factory.wait()
            return UncachedCacheValue(b"x" * (MAX_CACHE_VALUE_BYTES + 1))

        async def handler(event: Event) -> None:
            observed.append(event)

        async with _started_events_site() as site:
            await site.require_capability(EventsCapability).subscribe(
                EVT_CACHE, handler
            )
            first = asyncio.create_task(
                cache.get_or_set("template", "oversized", ttl=60, factory=factory)
            )
            await asyncio.wait_for(factory_started.wait(), timeout=60)
            second = asyncio.create_task(
                cache.get_or_set("template", "oversized", ttl=60, factory=factory)
            )
            await asyncio.wait_for(cache.waiting_for_fill.wait(), timeout=60)
            release_factory.set()

            assert await asyncio.wait_for(
                asyncio.gather(first, second), timeout=60
            ) == [
                b"x" * (MAX_CACHE_VALUE_BYTES + 1),
                b"x" * (MAX_CACHE_VALUE_BYTES + 1),
            ]

        assert factory_calls == 1
        assert await cache.get("template", "oversized") is None
        assert not any(
            isinstance(event, CacheOperationFailedEvent) for event in observed
        )
        assert any(
            isinstance(event, CacheOperationCompletedEvent)
            and event.outcome == "uncached"
            for event in observed
        )

    @pytest.mark.anyio
    async def test_get_or_set_records_a_miss_before_waiting_for_the_factory(
        self,
    ) -> None:
        factory_started = asyncio.Event()
        release_factory = asyncio.Event()
        read_recorded = asyncio.Event()

        async def handler(event: Event) -> None:
            if (
                isinstance(event, CacheOperationCompletedEvent)
                and str(event.scope) == "cache.read.completed"
                and event.outcome == "miss"
            ):
                read_recorded.set()

        async def factory() -> bytes:
            factory_started.set()
            await release_factory.wait()
            return b"value"

        async with _started_events_site() as site:
            await site.require_capability(EventsCapability).subscribe(
                EVT_CACHE, handler
            )
            cache = InMemoryCache()
            fill = asyncio.create_task(
                cache.get_or_set("template", "fragment", ttl=60, factory=factory)
            )
            await factory_started.wait()
            await asyncio.sleep(0)
            await asyncio.sleep(0)

            assert read_recorded.is_set()

            release_factory.set()
            assert await fill == b"value"

    @pytest.mark.anyio
    async def test_get_or_set_releases_waiters_before_slow_event_delivery(self) -> None:
        event_started = asyncio.Event()
        release_event = asyncio.Event()

        async def slow_handler(event: Event) -> None:
            if (
                isinstance(event, CacheOperationCompletedEvent)
                and event.outcome == "filled"
            ):
                event_started.set()
                await release_event.wait()

        async with _started_events_site() as site:
            await site.require_capability(EventsCapability).subscribe(
                EVT_CACHE, slow_handler
            )
            cache = InMemoryCache()
            factory_started = asyncio.Event()
            release_factory = asyncio.Event()

            async def factory() -> bytes:
                factory_started.set()
                await release_factory.wait()
                return b"value"

            first = asyncio.create_task(
                cache.get_or_set("template", "fragment", ttl=60, factory=factory)
            )
            await factory_started.wait()
            second = asyncio.create_task(
                cache.get_or_set(
                    "template",
                    "fragment",
                    ttl=60,
                    factory=lambda: pytest.fail(
                        "A waiting cache caller must not fill."
                    ),
                )
            )
            release_factory.set()
            await event_started.wait()

            assert await second == b"value"
            release_event.set()
            assert await first == b"value"

    @pytest.mark.anyio
    async def test_get_or_set_waiters_are_not_delayed_by_slow_cache_subscribers(
        self,
    ) -> None:
        """The single-flight timeout covers only cache filling, never observers."""

        async def slow_handler(_event: Event) -> None:
            await asyncio.sleep(1)

        async with _started_events_site() as site:
            await site.require_capability(EventsCapability).subscribe(
                EVT_CACHE, slow_handler
            )
            cache = InMemoryCache()
            factory_started = asyncio.Event()

            async def factory() -> bytes:
                factory_started.set()
                await asyncio.sleep(0.35)
                return b"value"

            first = asyncio.create_task(
                cache.get_or_set(
                    "template", "fragment", ttl=60, factory=factory, timeout=0.8
                )
            )
            await factory_started.wait()
            second = asyncio.create_task(
                cache.get_or_set(
                    "template",
                    "fragment",
                    ttl=60,
                    factory=lambda: pytest.fail(
                        "A waiting cache caller must not fill."
                    ),
                    timeout=0.8,
                )
            )

            assert await asyncio.wait_for(second, timeout=0.8) == b"value"
            assert await asyncio.wait_for(first, timeout=0.8) == b"value"

    @pytest.mark.anyio
    async def test_get_or_set_releases_waiters_after_a_failed_factory(self) -> None:
        cache = InMemoryCache()

        async def failing_factory() -> bytes:
            raise RuntimeError("source unavailable")

        async def succeeding_factory() -> bytes:
            return b"recovered"

        with pytest.raises(RuntimeError, match="source unavailable"):
            await cache.get_or_set(
                "template",
                "fragment",
                ttl=60,
                factory=failing_factory,
            )

        assert (
            await cache.get_or_set(
                "template",
                "fragment",
                ttl=60,
                factory=succeeding_factory,
            )
            == b"recovered"
        )

    @pytest.mark.anyio
    async def test_get_or_set_times_out_a_stalled_factory(self) -> None:
        cache = InMemoryCache()
        release = asyncio.Event()

        async def factory() -> bytes:
            await release.wait()
            return b"value"

        with pytest.raises(TimeoutError):
            await cache.get_or_set(
                "template",
                "fragment",
                ttl=60,
                factory=factory,
                timeout=0.01,
            )

        assert await cache.get("template", "fragment") is None


class TestRedisCache:
    def test_redis_cache_representation_redacts_the_connection_url(self) -> None:
        assert "secret" not in repr(RedisCache("redis://user:secret@cache/0"))

    def test_resolves_environment_url_and_credentials_without_leaking_them(
        self,
    ) -> None:
        secrets = DefaultSecretsCapability.from_drivers(
            (
                EnvironmentSecretSourceDriver(
                    {
                        "PRIVATE_REDIS_URL": "redis://cache.internal/4",
                        "PRIVATE_REDIS_CREDENTIALS": "service:pass:@word/?",
                    }
                ),
            )
        )
        full_url = CachesSettings(
            instances=(
                CacheSettings(
                    backend="redis",
                    url_source="environment",
                    url_key="PRIVATE_REDIS_URL",
                ),
            )
        )
        split_credentials = CachesSettings(
            instances=(
                CacheSettings(
                    backend="redis",
                    url="rediss://cache.internal/4",
                    credentials_source="environment",
                    credentials_key="PRIVATE_REDIS_CREDENTIALS",
                ),
            )
        )

        assert resolve_redis_urls(full_url, secrets) == {
            "default": "redis://cache.internal/4"
        }
        assert resolve_redis_urls(split_credentials, secrets) == {
            "default": "rediss://service:pass%3A%40word%2F%3F@cache.internal/4"
        }

    def test_redis_secret_resolution_fails_closed_without_secret_details(self) -> None:
        settings = CachesSettings(
            instances=(
                CacheSettings(
                    backend="redis",
                    url="redis://cache.internal/4",
                    credentials_source="environment",
                    credentials_key="MISSING_REDIS_CREDENTIALS",
                ),
            )
        )
        secrets = DefaultSecretsCapability.from_drivers(
            (EnvironmentSecretSourceDriver({}),)
        )

        with pytest.raises(ConfigurationError) as raised:
            resolve_redis_urls(settings, secrets)

        assert "MISSING_REDIS_CREDENTIALS" not in str(raised.value)

    @pytest.mark.anyio
    async def test_direct_redis_backend_construction_rejects_unresolved_secrets(
        self,
    ) -> None:
        settings = CachesSettings(
            instances=(
                CacheSettings(
                    backend="redis",
                    url="redis://cache.internal/4",
                    credentials_source="environment",
                    credentials_key="PRIVATE_REDIS_CREDENTIALS",
                    features=(),
                ),
            )
        )

        with pytest.raises(
            ConfigurationError,
            match="connection material must be resolved",
        ):
            await build_caches(settings)

    def test_requires_the_optional_cache_dependency(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        def missing_redis(_: str) -> None:
            raise ImportError("redis is not installed")

        monkeypatch.setattr(
            "wybra.cache.redis_runtime.importlib.import_module", missing_redis
        )

        with pytest.raises(ConfigurationError, match=r"Install wybra\[cache\]"):
            awaitable = RedisCache("redis://cache").get("test", "missing")
            asyncio.run(awaitable)

    @pytest.mark.anyio
    async def test_memory_backend_does_not_import_optional_cache_dependencies(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def unexpected_import(_: str) -> None:
            pytest.fail("Memory cache construction must not import an optional client.")

        redis_runtime = importlib.import_module("wybra.cache.redis_runtime")
        nats_runtime = importlib.import_module("wybra.cache.nats_runtime")
        monkeypatch.setattr(
            redis_runtime,
            "importlib",
            SimpleNamespace(import_module=unexpected_import),
        )
        monkeypatch.setattr(
            nats_runtime,
            "importlib",
            SimpleNamespace(import_module=unexpected_import),
        )

        caches = await build_caches(CachesSettings.load_settings({"cache": {}}))

        assert caches.require("default").backend == "memory"
        await caches.close()

    @pytest.mark.anyio
    async def test_redis_health_failure_closes_client_without_secret_detail(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client = SimpleNamespace(close_count=0)

        async def ping() -> bool:
            raise RuntimeError("redis://user:secret@cache/0")

        async def close() -> None:
            client.close_count += 1

        client.ping = ping
        client.aclose = close
        monkeypatch.setattr(
            "wybra.cache.redis_runtime.importlib.import_module",
            lambda _: SimpleNamespace(
                Redis=SimpleNamespace(
                    from_url=lambda *_args, **_kwargs: client,
                )
            ),
        )
        settings = CachesSettings.load_settings(
            {
                "cache": {
                    "backend": "redis",
                    "url": "redis://user:secret@cache/0",
                }
            }
        )

        with pytest.raises(ConfigurationError) as raised:
            await build_caches(settings)

        assert "secret" not in repr(raised.value)
        assert client.close_count == 1

    @pytest.mark.anyio
    async def test_uses_binary_redis_values(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class FakeRedis:
            def __init__(self) -> None:
                self.values: dict[str, bytes] = {}

            async def get(self, key: str) -> bytes | None:
                return self.values.get(key)

            async def set(self, key: str, value: bytes, *, px: int) -> None:
                assert px == 60_000
                self.values[key] = value

            async def delete(self, key: str) -> None:
                self.values.pop(key, None)

            async def aclose(self) -> None:
                return None

        client = FakeRedis()
        monkeypatch.setattr(
            "wybra.cache.redis_runtime.importlib.import_module",
            lambda _: SimpleNamespace(
                Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
            ),
        )
        cache = RedisCache("redis://cache")

        await cache.set("template", "bytecode", b"compiled", ttl=60)

        assert await cache.get("template", "bytecode") == b"compiled"
        await cache.delete("template", "bytecode")
        assert await cache.get("template", "bytecode") is None

    def test_redis_runtime_rejects_unsafe_direct_namespace(self) -> None:
        runtime = RedisCacheRuntime("redis://cache", "unsafe:namespace")

        with pytest.raises(ValueError, match="namespace must not contain ':'"):
            runtime.key("atomic", "owner", "key")

    @pytest.mark.anyio
    async def test_redis_runtime_close_waits_for_concurrent_callers(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        close_started = asyncio.Event()
        allow_close = asyncio.Event()
        client = SimpleNamespace(close_count=0)

        async def close() -> None:
            client.close_count += 1
            close_started.set()
            await allow_close.wait()

        client.aclose = close
        monkeypatch.setattr(
            "wybra.cache.redis_runtime.importlib.import_module",
            lambda _: SimpleNamespace(
                Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
            ),
        )
        runtime = RedisCacheRuntime("redis://cache")
        runtime.client()

        first = asyncio.create_task(runtime.close())
        await close_started.wait()
        second = asyncio.create_task(runtime.close())
        await asyncio.sleep(0)
        assert second.done() is False
        allow_close.set()
        await asyncio.gather(first, second)

        assert client.close_count == 1


class TestNatsJetStreamCache:
    @pytest.mark.anyio
    async def test_requires_the_optional_cache_dependency(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def missing_nats(_: str) -> None:
            raise ImportError("nats is not installed")

        monkeypatch.setattr(
            "wybra.cache.nats_runtime.importlib.import_module",
            missing_nats,
        )
        settings = CachesSettings.load_settings(
            {
                "cache": {
                    "backend": "nats-jetstream",
                    "servers": ["nats://cache.internal:4222"],
                }
            }
        )

        with pytest.raises(ConfigurationError, match=r"Install wybra\[cache\]"):
            await build_caches(settings)

    @pytest.mark.anyio
    async def test_rejects_a_server_without_per_message_ttl_support(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client = SimpleNamespace(
            connected_server_version=SimpleNamespace(major=2, minor=10, patch=19),
            max_payload=1_048_576,
            close_count=0,
        )

        async def close() -> None:
            client.close_count += 1

        async def connect(*, servers: list[str]) -> SimpleNamespace:
            assert servers == ["nats://cache.internal:4222"]
            return client

        client.close = close
        monkeypatch.setattr(
            "wybra.cache.nats_runtime.importlib.import_module",
            lambda _: SimpleNamespace(connect=connect),
        )
        settings = CachesSettings.load_settings(
            {
                "cache": {
                    "backend": "nats-jetstream",
                    "servers": ["nats://cache.internal:4222"],
                }
            }
        )

        with pytest.raises(ConfigurationError, match="requires NATS server 2.11.0"):
            await build_caches(settings)

        assert client.close_count == 1

    @pytest.mark.anyio
    async def test_rejects_a_server_without_portable_value_capacity(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client = SimpleNamespace(
            connected_server_version=SimpleNamespace(major=2, minor=11, patch=0),
            max_payload=1,
            close_count=0,
        )

        async def close() -> None:
            client.close_count += 1

        async def connect(*, servers: list[str]) -> SimpleNamespace:
            assert servers == ["nats://cache.internal:4222"]
            return client

        client.close = close
        monkeypatch.setattr(
            "wybra.cache.nats_runtime.importlib.import_module",
            lambda _: SimpleNamespace(connect=connect),
        )
        settings = CachesSettings.load_settings(
            {
                "cache": {
                    "backend": "nats-jetstream",
                    "servers": ["nats://cache.internal:4222"],
                }
            }
        )

        with pytest.raises(ConfigurationError, match="payload limit"):
            await build_caches(settings)

        assert client.close_count == 1

    @pytest.mark.anyio
    async def test_retries_failed_startup_cleanup_before_releasing_runtime(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client = SimpleNamespace(
            connected_server_version=SimpleNamespace(major=2, minor=10, patch=19),
            max_payload=1_048_576,
            close_attempts=0,
        )

        async def close() -> None:
            client.close_attempts += 1
            if client.close_attempts == 1:
                raise RuntimeError("transient close failure")

        async def connect(*, servers: list[str]) -> SimpleNamespace:
            assert servers == ["nats://cache.internal:4222"]
            return client

        client.close = close
        monkeypatch.setattr(
            "wybra.cache.nats_runtime.importlib.import_module",
            lambda _: SimpleNamespace(connect=connect),
        )
        settings = CachesSettings.load_settings(
            {
                "cache": {
                    "backend": "nats-jetstream",
                    "servers": ["nats://cache.internal:4222"],
                }
            }
        )

        with pytest.raises(ConfigurationError, match="requires NATS server 2.11.0"):
            await build_caches(settings)

        assert client.close_attempts == 2

    @pytest.mark.parametrize(
        "stream_configuration",
        (
            {"mirror": object()},
            {"republish": object()},
            {"sources": (object(),)},
            {"subject_delete_marker_ttl": 60},
            {"subject_transform": object()},
        ),
        ids=(
            "mirror",
            "republish",
            "sources",
            "subject-delete-marker",
            "subject-transform",
        ),
    )
    @pytest.mark.anyio
    async def test_rejects_stream_topology_that_changes_baseline_semantics(
        self,
        monkeypatch: pytest.MonkeyPatch,
        stream_configuration: dict[str, object],
    ) -> None:
        client = SimpleNamespace(
            connected_server_version=SimpleNamespace(major=2, minor=11, patch=0),
            max_payload=1_048_576,
            close_count=0,
        )
        config = SimpleNamespace(
            **(
                {
                    "name": "WYBRA_CACHE_DEFAULT",
                    "subjects": ("wybra.cache.default.>",),
                    "retention": "limits",
                    "max_msgs_per_subject": 1,
                    "max_msgs": -1,
                    "max_bytes": -1,
                    "max_age": 0,
                    "max_msg_size": -1,
                    "discard_new_per_subject": False,
                    "discard": "old",
                    "no_ack": False,
                    "sealed": False,
                    "deny_delete": False,
                    "mirror": None,
                    "republish": None,
                    "sources": (),
                    "subject_delete_marker_ttl": None,
                    "subject_transform": None,
                    "allow_direct": True,
                    "allow_msg_ttl": True,
                }
                | stream_configuration
            )
        )

        async def close() -> None:
            client.close_count += 1

        async def connect(*, servers: list[str]) -> SimpleNamespace:
            assert servers == ["nats://cache.internal:4222"]
            return client

        async def account_info() -> None:
            return None

        async def stream_info(_name: str) -> SimpleNamespace:
            return SimpleNamespace(config=config)

        class NotFoundError(Exception):
            pass

        def import_module(name: str) -> SimpleNamespace:
            if name == "nats":
                return SimpleNamespace(connect=connect)
            if name == "nats.js.errors":
                return SimpleNamespace(NotFoundError=NotFoundError)
            pytest.fail(f"Unexpected NATS module request: {name}")

        client.close = close
        client.jetstream = lambda: SimpleNamespace(
            account_info=account_info,
            stream_info=stream_info,
        )
        monkeypatch.setattr(
            "wybra.cache.nats_runtime.importlib.import_module",
            import_module,
        )
        settings = CachesSettings.load_settings(
            {
                "cache": {
                    "backend": "nats-jetstream",
                    "servers": ["nats://cache.internal:4222"],
                }
            }
        )

        with pytest.raises(
            ConfigurationError, match="stream configuration is incompatible"
        ):
            await build_caches(settings)

        assert client.close_count == 1

    @pytest.mark.anyio
    async def test_reports_connection_startup_failure_safely(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async def connect(*, servers: list[str]) -> None:
            del servers
            raise RuntimeError("connection refused")

        monkeypatch.setattr(
            "wybra.cache.nats_runtime.importlib.import_module",
            lambda _: SimpleNamespace(connect=connect),
        )
        settings = CachesSettings.load_settings(
            {
                "cache": {
                    "backend": "nats-jetstream",
                    "servers": ["nats://cache.internal:4222"],
                }
            }
        )

        with pytest.raises(ConfigurationError, match="cache backend startup failed"):
            await build_caches(settings)

    @pytest.mark.anyio
    async def test_delegates_byte_values_ttl_and_deletion_to_jetstream(self) -> None:
        class FakeRuntime:
            def __init__(self) -> None:
                self.values: dict[tuple[str, str], bytes] = {}
                self.ttls: dict[tuple[str, str], float] = {}

            async def get(self, owner: str, key: str) -> bytes | None:
                return self.values.get((owner, key))

            async def set(
                self,
                owner: str,
                key: str,
                value: bytes,
                *,
                ttl: float,
            ) -> None:
                self.values[(owner, key)] = value
                self.ttls[(owner, key)] = ttl

            async def delete(self, owner: str, key: str) -> None:
                self.values.pop((owner, key), None)

        runtime = FakeRuntime()
        cache = NatsJetStreamCache.from_runtime(runtime)

        await cache.set("template", "bytecode", b"compiled", ttl=1)

        assert await cache.get("template", "bytecode") == b"compiled"
        assert runtime.ttls == {("template", "bytecode"): 1.0}
        await cache.delete("template", "bytecode")
        assert await cache.get("template", "bytecode") is None

    @pytest.mark.anyio
    async def test_baseline_caches_require_a_minimum_one_second_ttl(self) -> None:
        caches = (
            InMemoryCache(),
            RedisCache("redis://cache.internal/0"),
            NatsJetStreamCache.from_runtime(SimpleNamespace()),
        )

        for cache in caches:
            with pytest.raises(ValueError, match="at least one second"):
                await cache.set("template", "short-lived", b"value", ttl=0.2)

    @pytest.mark.parametrize(
        "ttl",
        (10**400, -(10**400)),
        ids=("huge-positive-integer", "huge-negative-integer"),
    )
    @pytest.mark.anyio
    async def test_baseline_caches_reject_unrepresentable_ttls_before_io(
        self,
        ttl: int,
    ) -> None:
        caches = (
            InMemoryCache(),
            RedisCache("redis://cache.internal/0"),
            NatsJetStreamCache.from_runtime(SimpleNamespace()),
        )

        for cache in caches:
            with pytest.raises(ValueError, match="finite duration"):
                await cache.set("template", "unrepresentable", b"value", ttl=ttl)

            async def factory() -> bytes:
                pytest.fail("An invalid TTL must not invoke its factory.")

            with pytest.raises(ValueError, match="finite duration"):
                await cache.get_or_set(
                    "template",
                    "unrepresentable",
                    ttl=ttl,
                    factory=factory,
                )

    @pytest.mark.anyio
    async def test_baseline_caches_reject_values_above_the_portable_limit(
        self,
    ) -> None:
        caches = (
            InMemoryCache(),
            RedisCache("redis://cache.internal/0"),
            NatsJetStreamCache.from_runtime(SimpleNamespace()),
        )
        oversized = b"x" * (MAX_CACHE_VALUE_BYTES + 1)

        for cache in caches:
            with pytest.raises(ValueError, match="cannot exceed"):
                await cache.set("template", "oversized", oversized, ttl=60)

    @pytest.mark.anyio
    async def test_nats_ttl_limit_is_reported_as_invalid_input(self) -> None:
        class FakeJetStream:
            async def publish(self, *_args: object, **_kwargs: object) -> None:
                pytest.fail("An invalid TTL must not attempt a provider write.")

        runtime = NatsJetStreamRuntime(("nats://cache.internal:4222",), "default")
        runtime._jetstream = FakeJetStream()

        with pytest.raises(ValueError, match="exceeds the NATS JetStream TTL limit"):
            await runtime.set("template", "bytecode", b"compiled", ttl=1e30)

    def test_subject_tokens_do_not_reveal_logical_cache_keys(self) -> None:
        runtime = NatsJetStreamRuntime(("nats://cache.internal:4222",), "default")

        first = runtime.cache_subject("session", "user:42/reset-token")
        second = runtime.cache_subject("session", "user:42/reset-token")

        assert first == second
        assert "session" not in first
        assert "user:42/reset-token" not in first

    @pytest.mark.anyio
    async def test_close_retries_after_a_transient_client_failure(self) -> None:
        class FakeClient:
            close_attempts = 0

            async def close(self) -> None:
                self.close_attempts += 1
                if self.close_attempts == 1:
                    raise RuntimeError("transient close failure")

        runtime = NatsJetStreamRuntime(("nats://cache.internal:4222",), "default")
        client = FakeClient()
        runtime._client = client
        runtime._jetstream = object()

        with pytest.raises(CacheFeatureError, match="shutdown failed"):
            await runtime.close()

        await runtime.close()
        assert client.close_attempts == 2

    @pytest.mark.anyio
    async def test_delete_accepts_a_concurrently_replaced_value(self) -> None:
        class FakeJetStream:
            def __init__(self) -> None:
                self.reads = 0

            async def get_last_msg(
                self,
                stream: str,
                subject: str,
                *,
                direct: bool,
            ) -> SimpleNamespace:
                del stream, subject, direct
                self.reads += 1
                return SimpleNamespace(seq=1 if self.reads == 1 else 2)

            async def delete_msg(self, stream: str, sequence: int) -> None:
                del stream, sequence
                raise RuntimeError("message was replaced")

        runtime = NatsJetStreamRuntime(("nats://cache.internal:4222",), "default")
        runtime._jetstream = FakeJetStream()

        await runtime.delete("template", "bytecode")


class TestCacheModule:
    @pytest.mark.parametrize(
        "modules",
        (
            ("wybra.secrets", "wybra.cache"),
            ("wybra.cache", "wybra.secrets"),
        ),
    )
    @pytest.mark.anyio
    async def test_module_resolves_environment_credentials_before_redis_startup(
        self,
        monkeypatch: pytest.MonkeyPatch,
        modules: tuple[str, str],
    ) -> None:
        received_urls: list[str] = []
        client = SimpleNamespace()

        async def close() -> None:
            return None

        client.aclose = close
        client.ping = _healthy_ping
        monkeypatch.setattr(
            "wybra.cache.redis_runtime.importlib.import_module",
            lambda _: SimpleNamespace(
                Redis=SimpleNamespace(
                    from_url=lambda url, **_kwargs: received_urls.append(url) or client,
                )
            ),
        )
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": modules},
                    "cache": {
                        "backend": "redis",
                        "url": "rediss://cache.internal/4",
                        "features": (),
                        "credentials_source": "environment",
                        "credentials_key": "PRIVATE_REDIS_CREDENTIALS",
                    },
                }
            ),
            environ={"PRIVATE_REDIS_CREDENTIALS": "service:password"},
        )
        try:
            assert received_urls == ["rediss://service:password@cache.internal/4"]
        finally:
            await site.close()

    @pytest.mark.anyio
    async def test_module_registers_default_and_named_cache_capabilities(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.cache",)},
                    "cache": {},
                    "cache.session": {"backend": "memory"},
                }
            ),
        )

        caches = site.require_capability(CachesCapability)
        default = caches.require("default")
        session = caches.require("session", consumer="sessions")

        assert isinstance(default.values, CacheCapability)
        assert site.require_capability(CacheCapability) is default.values
        assert caches.require("session") is session
        assert caches.optional("missing") is None
        assert caches.optional(" session ") is None
        with pytest.raises(CacheNotFoundError, match="Invalid cache name"):
            caches.require(" session ")
        await default.values.set("test", "shared-key", b"default", ttl=60)
        assert await session.values.get("test", "shared-key") is None
        await site.close()

    @pytest.mark.anyio
    async def test_missing_named_cache_error_identifies_name_and_consumer(self) -> None:
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {"app": {"modules": ("wybra.cache",)}, "cache": {}}
            ),
        )
        caches = site.require_capability(CachesCapability)

        with pytest.raises(
            CacheNotFoundError,
            match=r"missing.+sessions",
        ):
            caches.require("missing", consumer="sessions")

        await site.close()

    @pytest.mark.anyio
    async def test_registry_closes_each_named_backend_once(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clients: list[SimpleNamespace] = []

        def from_url(*_args: object, **_kwargs: object) -> SimpleNamespace:
            client = SimpleNamespace(close_count=0)

            async def aclose() -> None:
                client.close_count += 1

            client.aclose = aclose
            client.ping = _healthy_ping
            clients.append(client)
            return client

        monkeypatch.setattr(
            "wybra.cache.redis_runtime.importlib.import_module",
            lambda _: SimpleNamespace(
                Redis=SimpleNamespace(from_url=from_url),
            ),
        )
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.cache",)},
                    "cache": {
                        "backend": "redis",
                        "url": "redis://cache/0",
                        "features": (),
                    },
                    "cache.session": {
                        "backend": "redis",
                        "url": "redis://cache/1",
                        "features": (),
                    },
                }
            ),
        )

        await site.close()

        assert [client.close_count for client in clients] == [1, 1]

    @pytest.mark.anyio
    async def test_registry_closes_started_backends_after_startup_failure(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        first = SimpleNamespace(close_count=0)
        calls = 0

        async def aclose() -> None:
            first.close_count += 1

        first.aclose = aclose
        first.ping = _healthy_ping

        def from_url(*_args: object, **_kwargs: object) -> SimpleNamespace:
            nonlocal calls
            calls += 1
            if calls == 1:
                return first
            raise RuntimeError("second cache failed")

        monkeypatch.setattr(
            "wybra.cache.redis_runtime.importlib.import_module",
            lambda _: SimpleNamespace(
                Redis=SimpleNamespace(from_url=from_url),
            ),
        )

        with pytest.raises(
            SiteCapabilityError,
            match=(
                "module=wybra.cache.*attribute=setup_finalisation.*"
                "error_type=ConfigurationError"
            ),
        ) as raised:
            await start(
                FastAPI(),
                config_source=MappingConfigSource(
                    {
                        "app": {"modules": ("wybra.cache",)},
                        "cache": {
                            "backend": "redis",
                            "url": "redis://cache/0",
                            "features": (),
                        },
                        "cache.session": {
                            "backend": "redis",
                            "url": "redis://cache/1",
                            "features": (),
                        },
                    }
                ),
            )

        assert isinstance(raised.value.__cause__, ConfigurationError)
        assert "health check failed" in str(raised.value.__cause__)
        assert first.close_count == 1

    @pytest.mark.anyio
    async def test_registry_closes_backend_rejected_by_baseline_validation(
        self,
    ) -> None:
        settings = CachesSettings.load_settings({"cache": {}})
        close_count = 0

        async def close() -> None:
            nonlocal close_count
            close_count += 1

        async def factory(_settings: CacheSettings) -> CacheBackend:
            return CacheBackend(SimpleNamespace(), close)

        with pytest.raises(ConfigurationError, match="mandatory cache baseline"):
            await build_caches(settings, factories={"memory": factory})

        assert close_count == 1

    @pytest.mark.anyio
    async def test_backend_configuration_error_identifies_named_cache(self) -> None:
        settings = CachesSettings.load_settings(
            {
                "cache": {},
                "cache.session": {
                    "backend": "redis",
                    "url": "http://cache/1",
                },
            }
        )

        async def memory_factory(_settings: CacheSettings) -> CacheBackend:
            return CacheBackend(InMemoryCache())

        async def redis_factory(_settings: CacheSettings) -> CacheBackend:
            raise ValueError("Redis URL has an unsupported scheme")

        with pytest.raises(
            ConfigurationError,
            match=r"Cache 'session' backend 'redis' configuration failed",
        ):
            await build_caches(
                settings,
                factories={
                    "memory": memory_factory,
                    "redis": redis_factory,
                },
            )

    @pytest.mark.anyio
    async def test_redis_backend_rechecks_the_url_invariant(self) -> None:
        settings = CachesSettings(instances=(invalid_redis_cache_settings(),))

        with pytest.raises(
            ConfigurationError,
            match=r"Cache 'default' backend 'redis' configuration failed",
        ):
            await build_caches(settings)

    @pytest.mark.anyio
    async def test_module_registration_failure_closes_new_registry_backends(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        clients: list[SimpleNamespace] = []

        def from_url(*_args: object, **_kwargs: object) -> SimpleNamespace:
            client = SimpleNamespace(close_count=0)

            async def aclose() -> None:
                client.close_count += 1

            client.aclose = aclose
            client.ping = _healthy_ping
            clients.append(client)
            return client

        monkeypatch.setattr(
            "wybra.cache.redis_runtime.importlib.import_module",
            lambda _: SimpleNamespace(
                Redis=SimpleNamespace(from_url=from_url),
            ),
        )

        with pytest.raises(RuntimeError, match="already provided"):
            await start(
                FastAPI(),
                config_source=MappingConfigSource(
                    {
                        "app": {"modules": ("wybra.cache", "wybra.cache")},
                        "cache": {
                            "backend": "redis",
                            "url": "redis://cache/0",
                            "features": (),
                        },
                    }
                ),
            )

        assert [client.close_count for client in clients] == [1, 1]

    @pytest.mark.anyio
    async def test_registration_rollback_preserves_cancellation(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        class CancellingCaches:
            def require(self, _name: str) -> SimpleNamespace:
                return SimpleNamespace(values=InMemoryCache())

            async def close(self) -> None:
                raise asyncio.CancelledError

        class FailingSite:
            config = {"cache": {}}
            registration_count = 0

            def capability_proxy(self, _capability_type: object) -> object:
                return object()

            def defer_setup_finalisation(self, finaliser: object) -> None:
                self.finaliser = finaliser

            def provide_capability(
                self,
                _capability_type: object,
                _value: object,
            ) -> None:
                self.registration_count += 1
                if self.registration_count == 2:
                    raise RuntimeError("registration failed")

        async def build_cancelling_caches(
            _settings: CachesSettings,
            **_kwargs: object,
        ) -> CancellingCaches:
            return CancellingCaches()

        monkeypatch.setattr(
            "wybra.cache.setup.build_caches",
            build_cancelling_caches,
        )

        site = FailingSite()
        await setup_site(site)

        with pytest.raises(asyncio.CancelledError) as captured:
            await site.finaliser()

        assert isinstance(captured.value.__cause__, RuntimeError)

    @pytest.mark.anyio
    async def test_registry_attempts_every_close_when_one_backend_fails(self) -> None:
        settings = CachesSettings.load_settings(
            {"cache": {}, "cache.session": {"backend": "memory"}}
        )
        close_calls: list[str] = []

        async def factory(settings: CacheSettings) -> CacheBackend:
            async def close() -> None:
                close_calls.append(settings.name)
                if settings.name == "session":
                    raise RuntimeError("close failed")

            return CacheBackend(InMemoryCache(), close)

        caches = await build_caches(settings, factories={"memory": factory})

        with pytest.raises(BaseExceptionGroup, match="shutdown failed"):
            await caches.close()

        assert close_calls == ["session", "default"]

    @pytest.mark.anyio
    async def test_registry_retries_only_failed_backend_cleanup(self) -> None:
        settings = CachesSettings.load_settings(
            {"cache": {}, "cache.session": {"backend": "memory"}}
        )
        close_calls: list[str] = []
        session_attempts = 0

        async def factory(settings: CacheSettings) -> CacheBackend:
            async def close() -> None:
                nonlocal session_attempts
                close_calls.append(settings.name)
                if settings.name == "session":
                    session_attempts += 1
                    if session_attempts == 1:
                        raise RuntimeError("close failed once")

            return CacheBackend(InMemoryCache(), close)

        caches = await build_caches(settings, factories={"memory": factory})

        with pytest.raises(BaseExceptionGroup, match="shutdown failed"):
            await caches.close()
        await caches.close()
        await caches.close()

        assert close_calls == ["session", "default", "session"]

    @pytest.mark.anyio
    async def test_registry_preserves_cancellation_after_closing_every_backend(
        self,
    ) -> None:
        settings = CachesSettings.load_settings(
            {"cache": {}, "cache.session": {"backend": "memory"}}
        )
        close_calls: list[str] = []

        async def factory(settings: CacheSettings) -> CacheBackend:
            async def close() -> None:
                close_calls.append(settings.name)
                if settings.name == "session":
                    raise asyncio.CancelledError

            return CacheBackend(InMemoryCache(), close)

        caches = await build_caches(settings, factories={"memory": factory})

        with pytest.raises(asyncio.CancelledError):
            await caches.close()

        assert close_calls == ["session", "default"]

    @pytest.mark.anyio
    async def test_registry_retries_only_cancelled_backend_cleanup(self) -> None:
        settings = CachesSettings.load_settings(
            {"cache": {}, "cache.session": {"backend": "memory"}}
        )
        close_calls: list[str] = []
        session_attempts = 0

        async def factory(settings: CacheSettings) -> CacheBackend:
            async def close() -> None:
                nonlocal session_attempts
                close_calls.append(settings.name)
                if settings.name == "session":
                    session_attempts += 1
                    if session_attempts == 1:
                        raise asyncio.CancelledError

            return CacheBackend(InMemoryCache(), close)

        caches = await build_caches(settings, factories={"memory": factory})

        with pytest.raises(asyncio.CancelledError):
            await caches.close()
        await caches.close()

        assert close_calls == ["session", "default", "session"]

    @pytest.mark.anyio
    async def test_registry_serialises_concurrent_close_calls(self) -> None:
        settings = CachesSettings.load_settings({"cache": {}})
        started = asyncio.Event()
        release = asyncio.Event()
        close_calls = 0

        async def factory(_settings: CacheSettings) -> CacheBackend:
            async def close() -> None:
                nonlocal close_calls
                close_calls += 1
                started.set()
                await release.wait()

            return CacheBackend(InMemoryCache(), close)

        caches = await build_caches(settings, factories={"memory": factory})
        first = asyncio.create_task(caches.close())
        await started.wait()
        second = asyncio.create_task(caches.close())
        await asyncio.sleep(0)
        release.set()

        await asyncio.gather(first, second)

        assert close_calls == 1

    @pytest.mark.anyio
    async def test_redis_backend_retries_features_before_closing_runtime(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client = SimpleNamespace(close_count=0)
        feature_close_attempts = 0

        async def ping() -> bool:
            return True

        async def close_client() -> None:
            client.close_count += 1

        client.ping = ping
        client.aclose = close_client
        monkeypatch.setattr(
            "wybra.cache.redis_runtime.importlib.import_module",
            lambda _: SimpleNamespace(
                Redis=SimpleNamespace(from_url=lambda *_args, **_kwargs: client)
            ),
        )
        original_close = RedisCacheFeatures.close

        async def fail_features_once(features: RedisCacheFeatures) -> None:
            nonlocal feature_close_attempts
            feature_close_attempts += 1
            if feature_close_attempts == 1:
                raise CacheFeatureError("feature cleanup failed once")
            await original_close(features)

        monkeypatch.setattr(RedisCacheFeatures, "close", fail_features_once)
        caches = await build_caches(
            CachesSettings(
                instances=(
                    CacheSettings(
                        backend="redis",
                        url="redis://cache/0",
                        features=(),
                    ),
                )
            )
        )

        with pytest.raises(BaseExceptionGroup, match="shutdown failed"):
            await caches.close()
        assert client.close_count == 0

        await caches.close()
        await caches.close()

        assert feature_close_attempts == 2
        assert client.close_count == 1

    @pytest.mark.anyio
    async def test_registry_closes_shared_lifecycle_owner_once(self) -> None:
        settings = CachesSettings.load_settings(
            {"cache": {}, "cache.session": {"backend": "memory"}}
        )
        close_count = 0
        owner = object()

        async def close() -> None:
            nonlocal close_count
            close_count += 1

        async def factory(_settings: CacheSettings) -> CacheBackend:
            return CacheBackend(
                InMemoryCache(),
                close,
                lifecycle_owner=owner,
            )

        caches = await build_caches(settings, factories={"memory": factory})

        await caches.close()

        assert close_count == 1

    @pytest.mark.anyio
    async def test_empty_factory_mapping_disables_builtin_factories(self) -> None:
        settings = CachesSettings.load_settings({"cache": {}})

        with pytest.raises(ConfigurationError, match="No cache backend factory"):
            await build_caches(settings, factories={})


class TestTemplateFragmentCache:
    def test_configure_cache_extension_reports_a_missing_extension(self) -> None:
        with pytest.raises(RuntimeError, match="not registered"):
            configure_cache_extension(Environment(), None)

    @pytest.mark.anyio
    async def test_template_module_resolves_cache_capability_at_render_time(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "fragment.html").write_text(
            '{% cache "greeting" ttl=60 %}{{ value }}{% endcache %}',
            encoding="utf-8",
        )
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.template", "wybra.cache")},
                    "app.templates": {"root": str(tmp_path)},
                    "cache": {},
                }
            ),
        )
        templates = site.require_capability(TemplateCapability)

        assert await templates.render_template("fragment.html", {"value": "first"}) == (
            "first"
        )
        assert await templates.render_template(
            "fragment.html", {"value": "second"}
        ) == ("first")
        await site.close()

    @pytest.mark.anyio
    async def test_caches_fragments_when_a_cache_provider_is_available(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "fragment.html").write_text(
            '{% cache "greeting" ttl=60 vary_by=locale %}{{ value }}{% endcache %}',
            encoding="utf-8",
        )
        cache = InMemoryCache()
        templates = DefaultTemplateCapability(
            template_root=tmp_path,
            cache_provider=lambda: _cache_provider(cache),
        )

        assert (
            await templates.render_template(
                "fragment.html", {"locale": "en-AU", "value": "first"}
            )
            == "first"
        )
        assert (
            await templates.render_template(
                "fragment.html", {"locale": "en-AU", "value": "second"}
            )
            == "first"
        )
        assert (
            await templates.render_template(
                "fragment.html", {"locale": "fr", "value": "troisième"}
            )
            == "troisième"
        )

    @pytest.mark.anyio
    @pytest.mark.parametrize(
        ("value", "is_cached"),
        (
            ("x" * MAX_CACHE_VALUE_BYTES, True),
            ("x" * (MAX_CACHE_VALUE_BYTES + 1), False),
            ("é" * ((MAX_CACHE_VALUE_BYTES // 2) + 1), False),
        ),
    )
    async def test_fragment_cache_preserves_oversized_rendering(
        self,
        tmp_path: Path,
        value: str,
        *,
        is_cached: bool,
    ) -> None:
        (tmp_path / "fragment.html").write_text(
            '{% cache "greeting" ttl=60 %}{{ value }}{% endcache %}',
            encoding="utf-8",
        )
        cache = InMemoryCache()
        templates = DefaultTemplateCapability(
            template_root=tmp_path,
            cache_provider=lambda: _cache_provider(cache),
        )

        assert (
            await templates.render_template("fragment.html", {"value": value}) == value
        )
        assert await templates.render_template(
            "fragment.html", {"value": "replacement"}
        ) == (value if is_cached else "replacement")

    @pytest.mark.anyio
    async def test_fragment_cache_shares_concurrent_oversized_rendering(
        self,
        tmp_path: Path,
    ) -> None:
        class ObservedInMemoryCache(InMemoryCache):
            def __init__(self) -> None:
                super().__init__()
                self.waiting_for_fill = asyncio.Event()

            async def _wait_for_fill(
                self,
                completed: asyncio.Event,
                *,
                timeout: float,
            ) -> None:
                self.waiting_for_fill.set()
                await super()._wait_for_fill(completed, timeout=timeout)

        (tmp_path / "fragment.html").write_text(
            '{% cache "greeting" ttl=60 %}{{ render_fragment() }}{% endcache %}',
            encoding="utf-8",
        )
        cache = ObservedInMemoryCache()
        templates = DefaultTemplateCapability(
            template_root=tmp_path,
            cache_provider=lambda: _cache_provider(cache),
        )
        rendered = "x" * (MAX_CACHE_VALUE_BYTES + 1)
        render_started = asyncio.Event()
        release_render = asyncio.Event()
        render_calls = 0

        async def render_fragment() -> str:
            nonlocal render_calls
            render_calls += 1
            render_started.set()
            await release_render.wait()
            return rendered

        templates.environment.globals["render_fragment"] = render_fragment
        first = asyncio.create_task(templates.render_template("fragment.html", {}))
        await asyncio.wait_for(render_started.wait(), timeout=60)
        second = asyncio.create_task(templates.render_template("fragment.html", {}))
        await asyncio.wait_for(cache.waiting_for_fill.wait(), timeout=60)
        release_render.set()

        assert await asyncio.wait_for(asyncio.gather(first, second), timeout=60) == [
            rendered,
            rendered,
        ]
        assert render_calls == 1

    @pytest.mark.anyio
    @pytest.mark.parametrize("ttl", (0.5, float("nan"), float("inf")))
    async def test_fragment_cache_rejects_invalid_ttl_before_cache_resolution(
        self,
        tmp_path: Path,
        ttl: float,
    ) -> None:
        (tmp_path / "fragment.html").write_text(
            '{% cache "greeting" ttl=ttl %}{{ value }}{% endcache %}',
            encoding="utf-8",
        )

        async def unexpected_cache_provider() -> CacheCapability | None:
            pytest.fail("Invalid cache fragment TTLs must not resolve a cache.")

        templates = DefaultTemplateCapability(
            template_root=tmp_path,
            cache_provider=unexpected_cache_provider,
        )

        with pytest.raises(TemplateRuntimeError, match="at least one second"):
            await templates.render_template("fragment.html", {"ttl": ttl, "value": "x"})

    @pytest.mark.anyio
    async def test_isolates_user_scoped_fragments(self, tmp_path: Path) -> None:
        (tmp_path / "fragment.html").write_text(
            '{% cache "greeting" ttl=60 vary_by=request.user.id %}'
            "{{ request.user.name }}{% endcache %}",
            encoding="utf-8",
        )
        cache = InMemoryCache()
        templates = DefaultTemplateCapability(
            template_root=tmp_path,
            cache_provider=lambda: _cache_provider(cache),
        )
        first_request = SimpleNamespace(user=SimpleNamespace(id=1, name="Ada"))
        second_request = SimpleNamespace(user=SimpleNamespace(id=2, name="Grace"))

        assert (
            await templates.render_template("fragment.html", {"request": first_request})
            == "Ada"
        )
        assert (
            await templates.render_template(
                "fragment.html", {"request": second_request}
            )
            == "Grace"
        )

    @pytest.mark.anyio
    async def test_cache_key_helper_normalises_registered_value_types(
        self, tmp_path: Path
    ) -> None:
        class Audience:
            def __init__(self, identifier: int) -> None:
                self.identifier = identifier

        (tmp_path / "fragment.html").write_text(
            '{% cache "greeting" ttl=60 '
            "vary_by=cache_key(audience=audience, locales=locales) %}"
            "{{ value }}{% endcache %}",
            encoding="utf-8",
        )
        cache = InMemoryCache()
        templates = DefaultTemplateCapability(
            template_root=tmp_path,
            cache_provider=lambda: _cache_provider(cache),
        )
        templates.register_cache_key_normaliser(
            Audience,
            lambda value: {"audience_id": value.identifier},
        )

        assert (
            await templates.render_template(
                "fragment.html",
                {"audience": Audience(1), "locales": {"fr", "en-AU"}, "value": "one"},
            )
            == "one"
        )
        assert (
            await templates.render_template(
                "fragment.html",
                {"audience": Audience(1), "locales": {"en-AU", "fr"}, "value": "two"},
            )
            == "one"
        )
        assert (
            await templates.render_template(
                "fragment.html",
                {"audience": Audience(2), "locales": {"en-AU", "fr"}, "value": "three"},
            )
            == "three"
        )

    @pytest.mark.anyio
    async def test_rejects_unsupported_fragment_variation_values(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "fragment.html").write_text(
            '{% cache "greeting" ttl=60 vary_by=audience %}{{ value }}{% endcache %}',
            encoding="utf-8",
        )
        cache = InMemoryCache()
        templates = DefaultTemplateCapability(
            template_root=tmp_path,
            cache_provider=lambda: _cache_provider(cache),
        )

        with pytest.raises(
            TemplateRuntimeError,
            match="must be JSON-compatible or use cache_key",
        ):
            await templates.render_template(
                "fragment.html",
                {"audience": SimpleNamespace(identifier=1), "value": "one"},
            )

    @pytest.mark.anyio
    async def test_fragment_keys_are_isolated_by_template_fingerprint(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "first.html").write_text(
            '{% cache "summary" ttl=60 %}First {{ value }}{% endcache %}',
            encoding="utf-8",
        )
        (tmp_path / "second.html").write_text(
            '{% cache "summary" ttl=60 %}Second {{ value }}{% endcache %}',
            encoding="utf-8",
        )
        cache = InMemoryCache()
        templates = DefaultTemplateCapability(
            template_root=tmp_path,
            cache_provider=lambda: _cache_provider(cache),
        )

        assert await templates.render_template("first.html", {"value": "one"}) == (
            "First one"
        )
        assert await templates.render_template("second.html", {"value": "two"}) == (
            "Second two"
        )

    @pytest.mark.anyio
    async def test_cache_hits_do_not_reload_template_source(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        (tmp_path / "fragment.html").write_text(
            '{% cache "greeting" ttl=60 %}{{ value }}{% endcache %}',
            encoding="utf-8",
        )
        cache = InMemoryCache()
        templates = DefaultTemplateCapability(
            template_root=tmp_path,
            cache_provider=lambda: _cache_provider(cache),
        )
        loader = templates.environment.loader
        assert loader is not None
        calls = 0
        get_source = loader.get_source

        def counted_get_source(*args: object, **kwargs: object) -> object:
            nonlocal calls
            calls += 1
            return get_source(*args, **kwargs)

        monkeypatch.setattr(loader, "get_source", counted_get_source)

        assert await templates.render_template("fragment.html", {"value": "first"}) == (
            "first"
        )
        calls_after_first_render = calls
        assert await templates.render_template(
            "fragment.html", {"value": "second"}
        ) == ("first")
        assert calls == calls_after_first_render

    @pytest.mark.anyio
    async def test_cache_tag_renders_normally_without_a_cache_provider(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "fragment.html").write_text(
            '{% cache "greeting" ttl=60 %}{{ value }}{% endcache %}',
            encoding="utf-8",
        )
        templates = DefaultTemplateCapability(template_root=tmp_path)

        assert (
            await templates.render_template("fragment.html", {"value": "first"})
            == "first"
        )
        assert (
            await templates.render_template("fragment.html", {"value": "second"})
            == "second"
        )

    @pytest.mark.anyio
    async def test_template_module_renders_cache_tag_without_cache_module(
        self, tmp_path: Path
    ) -> None:
        (tmp_path / "fragment.html").write_text(
            '{% cache "greeting" ttl=60 %}{{ value }}{% endcache %}',
            encoding="utf-8",
        )
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.template",)},
                    "app.templates": {"root": str(tmp_path)},
                }
            ),
        )
        templates = site.require_capability(TemplateCapability)

        assert await templates.render_template("fragment.html", {"value": "first"}) == (
            "first"
        )
        assert await templates.render_template(
            "fragment.html", {"value": "second"}
        ) == ("second")
        await site.close()
