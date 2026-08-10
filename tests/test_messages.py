from __future__ import annotations

import importlib
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from fastapi import FastAPI
from starlette.requests import Request

from wybra.cache import (
    AtomicCacheCapability,
    AtomicCacheValue,
    CacheRevision,
    CachesCapability,
    InMemoryAtomicCache,
)
from wybra.cache import (
    module_config as cache_module_config,
)
from wybra.config import ConfigService, ConfigSourceError, MappingConfigSource
from wybra.core.exceptions import ConfigurationError
from wybra.core.resources import PackageResourceSource
from wybra.db import DatabaseCapability
from wybra.db.capabilities import tortoise_transaction
from wybra.db.surfaces import (
    discover_migration_version_locations,
    discover_model_package,
)
from wybra.messages import (
    ERROR_ALERT,
    SUCCESS_ALERT,
    WARNING_ALERT,
    DefaultMessagesCapability,
    InvalidAlertError,
    MessageQueueUnavailableError,
    MessagesCapability,
    MessagesConfigurationError,
    MessagesSettings,
    MessageStorageBackend,
    MessageStorageError,
)
from wybra.messages.config import module_config
from wybra.messages.context import messages_context
from wybra.messages.models import MessageAlert
from wybra.messages.records import AlertRecord
from wybra.messages.storage import (
    QUEUE_ENTRY_ID_KEY,
    REQUEST_ALERTS_RENDERED_ATTRIBUTE,
    SESSION_ALERTS_KEY,
    SESSION_QUEUE_ID_KEY,
    CacheMessagesStorage,
    DatabaseMessagesStorage,
    NamedCacheQueueBackend,
    SessionMessagesStorage,
    storage_from_settings,
)
from wybra.messages.validation import validate_alerts
from wybra.sessions import (
    DatabaseSessionStorage as DatabaseRequestSessionStorage,
)
from wybra.sessions import (
    RequestSession,
    SessionCleanupRegistry,
    SessionRecord,
    create_session_id,
)
from wybra.site import Site, SiteCapabilityError, start
from wybra.template.capabilities import DefaultTemplateCapability
from wybra.template.context import TemplateContext
from wybra.testing import create_test_site, migrated_test_database
from wybra.tools.validation.registry import discover_validation_targets


def _settings(
    values: dict[str, object] | None = None,
    *,
    environ: dict[str, str] | None = None,
) -> MessagesSettings:
    ConfigService.set_runtime_environment({} if environ is None else environ)
    config = ConfigService(
        [
            MappingConfigSource(
                {
                    "wybra.messages": {} if values is None else values,
                }
            )
        ],
        config_defs=(module_config,),
        discover_module_config=False,
    )
    return MessagesSettings.load_settings(config)


def _cache_validation_settings(
    *,
    modules: tuple[str, ...],
    cache_name: str | None = None,
    named_caches: dict[str, dict[str, object]] | None = None,
) -> SimpleNamespace:
    message_values: dict[str, object] = {"storage_backend": "cache"}
    if cache_name is not None:
        message_values["cache_name"] = cache_name
    values: dict[str, dict[str, object]] = {
        "cache": {},
        "wybra.messages": message_values,
    }
    values.update(named_caches or {})
    return SimpleNamespace(
        modules=modules,
        config=ConfigService(
            [MappingConfigSource(values)],
            config_defs=(cache_module_config, module_config),
            discover_module_config=False,
        ),
    )


def _request(session: dict[str, object] | None = None) -> Request:
    app = FastAPI()
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/target",
        "headers": [],
        "query_string": b"",
        "scheme": "http",
        "server": ("testserver", 80),
        "client": ("testclient", 50000),
        "app": app,
    }
    if session is not None:
        scope["session"] = session
    return Request(scope)


def _site(settings: MessagesSettings, capability: DefaultMessagesCapability) -> Site:
    app = FastAPI()
    site = Site(
        app=app,
        config=ConfigService(
            [MappingConfigSource({"wybra.messages": {}})],
            config_defs=(module_config,),
            discover_module_config=False,
        ),
    )
    app.state.site = site
    app.state.messages_settings = settings
    site.provide_capability(MessagesCapability, capability)
    return site


@asynccontextmanager
async def _database_site(
    *,
    modules: tuple[str, ...] = ("wybra.messages",),
) -> AsyncIterator[tuple[Site, DatabaseCapability]]:
    async with migrated_test_database(modules=modules) as database:
        site = create_test_site({"app": {"modules": modules}})
        capability = database.capability()
        site.provide_capability(DatabaseCapability, capability)
        yield site, capability


@pytest.mark.anyio
async def test_messages_module_registers_capability_on_startup() -> None:
    app = FastAPI()

    site = await start(
        app,
        config_source=MappingConfigSource(
            {
                "app": {"modules": ("wybra.messages",)},
                "wybra.messages": {},
            }
        ),
    )

    assert site.has_capability(MessagesCapability) is True
    assert isinstance(site.require_capability(MessagesCapability), MessagesCapability)


def test_messages_settings_defaults_to_session_storage() -> None:
    settings = _settings()

    assert settings.storage_backend is MessageStorageBackend.SESSION
    assert settings.queue_depth == 5


def test_cache_messages_settings_default_to_named_default_cache() -> None:
    settings = _settings({"storage_backend": "cache"})

    assert settings.cache_name is None
    assert settings.resolved_cache_name == "default"


def test_cache_messages_settings_accept_explicit_cache_name() -> None:
    settings = _settings(
        {"storage_backend": "cache", "cache_name": "messages"},
    )

    assert settings.cache_name == "messages"
    assert settings.resolved_cache_name == "messages"


def test_cache_messages_settings_support_cache_name_environment_override() -> None:
    settings = _settings(
        {"storage_backend": "cache"},
        environ={"MESSAGES_CACHE_NAME": "messages"},
    )

    assert settings.cache_name == "messages"
    assert settings.resolved_cache_name == "messages"


def test_cache_messages_settings_reject_invalid_cache_name() -> None:
    with pytest.raises(ConfigSourceError, match="cache_name"):
        _settings({"storage_backend": "cache", "cache_name": "Message Cache"})


def test_named_cache_messages_settings_reject_oversized_queue_bound() -> None:
    with pytest.raises(ConfigurationError, match="atomic cache payload limit"):
        _settings(
            {
                "storage_backend": "cache",
                "queue_depth": 1,
                "message_max_length": 1_100_000,
            }
        )


def test_alert_record_validates_severity_and_message() -> None:
    alert = AlertRecord.create(
        SUCCESS_ALERT,
        "Saved",
        max_message_length=20,
        created_at=1.0,
    )

    assert alert.severity == SUCCESS_ALERT
    assert alert.message == "Saved"
    assert alert.created_at == 1.0

    with pytest.raises(InvalidAlertError, match="severity"):
        AlertRecord.create("notice", "Saved", max_message_length=20)

    with pytest.raises(InvalidAlertError, match="blank"):
        AlertRecord.create(SUCCESS_ALERT, "   ", max_message_length=20)

    with pytest.raises(InvalidAlertError, match="maximum length"):
        AlertRecord.create(SUCCESS_ALERT, "x" * 21, max_message_length=20)


@pytest.mark.anyio
async def test_session_storage_queues_and_pops_alerts_once() -> None:
    settings = _settings()
    capability = DefaultMessagesCapability(settings, SessionMessagesStorage(settings))
    session: dict[str, object] = {}
    request = _request(session)

    await capability.success(request, "Saved")
    await capability.warning(request, "Check this")
    alerts = await capability.consume_alerts(request)
    repeated_alerts = await capability.consume_alerts(request)

    assert [alert.severity for alert in alerts] == [SUCCESS_ALERT, WARNING_ALERT]
    assert [alert.message for alert in alerts] == ["Saved", "Check this"]
    assert repeated_alerts == alerts
    assert "_wybra_messages_alerts" not in session


@pytest.mark.anyio
async def test_session_storage_consume_after_peek_acknowledges_alerts() -> None:
    settings = _settings()
    capability = DefaultMessagesCapability(settings, SessionMessagesStorage(settings))
    session: dict[str, object] = {}
    request = _request(session)

    await capability.success(request, "Saved")
    peeked_alerts = await capability.peek_alerts(request)

    assert [alert.message for alert in peeked_alerts] == ["Saved"]
    assert SESSION_ALERTS_KEY in session

    consumed_alerts = await capability.consume_alerts(request)
    repeated_alerts = await capability.consume_alerts(request)

    assert consumed_alerts == peeked_alerts
    assert repeated_alerts == consumed_alerts
    assert SESSION_ALERTS_KEY not in session


@pytest.mark.anyio
async def test_message_added_after_rendered_alerts_is_not_acknowledged() -> None:
    settings = _settings()
    capability = DefaultMessagesCapability(settings, SessionMessagesStorage(settings))
    session: dict[str, object] = {}
    request = _request(session)

    await capability.success(request, "Rendered")
    alerts = await capability.renderable_alerts(request)

    assert [alert.message for alert in alerts] == ["Rendered"]
    assert getattr(request.state, REQUEST_ALERTS_RENDERED_ATTRIBUTE) is True

    await capability.success(request, "Queued later")

    repeated_alerts = await capability.peek_alerts(request)
    assert [alert.message for alert in repeated_alerts] == ["Queued later"]
    assert not hasattr(request.state, REQUEST_ALERTS_RENDERED_ATTRIBUTE)


@pytest.mark.anyio
async def test_session_storage_requires_request_session_mapping() -> None:
    settings = _settings()
    capability = DefaultMessagesCapability(settings, SessionMessagesStorage(settings))

    with pytest.raises(MessageQueueUnavailableError, match="Wybra sessions"):
        await capability.error(_request(), "Cannot store")


@pytest.mark.anyio
async def test_queue_depth_discards_oldest_session_alerts() -> None:
    settings = _settings({"queue_depth": 2})
    capability = DefaultMessagesCapability(settings, SessionMessagesStorage(settings))
    request = _request({})

    await capability.success(request, "One")
    await capability.warning(request, "Two")
    await capability.error(request, "Three")
    alerts = await capability.consume_alerts(request)

    assert [alert.message for alert in alerts] == ["Two", "Three"]


def test_non_cache_messages_warn_about_ignored_cache_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warning_messages: list[str] = []
    storage_module = importlib.import_module("wybra.messages.storage")

    def record_warning(message: str, *args: object) -> None:
        warning_messages.append(message % args)

    monkeypatch.setattr(storage_module.logger, "warning", record_warning)

    storage = storage_from_settings(
        Site(FastAPI(), ConfigService([], discover_module_config=False)),
        _settings({"storage_backend": "session", "cache_name": "messages"}),
    )

    assert isinstance(storage, SessionMessagesStorage)
    assert any("cache_name is ignored" in message for message in warning_messages)


@pytest.mark.anyio
async def test_named_default_cache_messages_start_and_persist_alerts() -> None:
    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": ("wybra.cache", "wybra.messages")},
                "cache": {},
                "wybra.messages": {"storage_backend": "cache"},
            }
        ),
        environ={},
    )

    try:
        capability = site.require_capability(MessagesCapability)
        assert isinstance(capability, DefaultMessagesCapability)
        assert isinstance(capability.storage, CacheMessagesStorage)
        assert isinstance(capability.storage.backend, NamedCacheQueueBackend)

        session: dict[str, object] = {}
        await capability.success(_request(session), "Cached")

        alerts = await capability.consume_alerts(_request(session))
        assert [alert.message for alert in alerts] == ["Cached"]
    finally:
        await site.close()


@pytest.mark.anyio
async def test_named_cache_messages_start_before_cache_module() -> None:
    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": ("wybra.messages", "wybra.cache")},
                "cache": {},
                "wybra.messages": {"storage_backend": "cache"},
            }
        ),
        environ={},
    )

    try:
        capability = site.require_capability(MessagesCapability)
        assert isinstance(capability, DefaultMessagesCapability)
        assert isinstance(capability.storage, CacheMessagesStorage)
        assert isinstance(capability.storage.backend, NamedCacheQueueBackend)
    finally:
        await site.close()


@pytest.mark.anyio
async def test_named_messages_cache_isolated_from_default_cache() -> None:
    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": ("wybra.cache", "wybra.messages")},
                "cache": {},
                "cache.messages": {"backend": "memory"},
                "wybra.messages": {
                    "storage_backend": "cache",
                    "cache_name": "messages",
                },
            }
        ),
        environ={},
    )

    try:
        capability = site.require_capability(MessagesCapability)
        caches = site.require_capability(CachesCapability)
        session: dict[str, object] = {}
        await capability.success(_request(session), "Isolated")
        queue_id = session[SESSION_QUEUE_ID_KEY]
        assert isinstance(queue_id, str)
        queue_key = f"wybra:messages:{queue_id}"

        default_atomic = caches.require("default").require(AtomicCacheCapability)
        messages_atomic = caches.require("messages").require(AtomicCacheCapability)
        assert await default_atomic.get("messages", queue_key) is None
        assert await messages_atomic.get("messages", queue_key) is not None
    finally:
        await site.close()


@pytest.mark.anyio
async def test_named_cache_acknowledgement_preserves_concurrent_append() -> None:
    settings = _settings({"storage_backend": "cache"})
    storage = CacheMessagesStorage(
        settings,
        NamedCacheQueueBackend(InMemoryAtomicCache()),
    )
    session: dict[str, object] = {}
    rendered_request = _request(session)
    concurrent_request = _request(session)

    await storage.enqueue(
        rendered_request,
        AlertRecord.create(
            SUCCESS_ALERT,
            "Rendered",
            max_message_length=settings.resolved_message_max_length,
        ),
    )
    rendered = await storage.peek(rendered_request, now=0)
    await storage.enqueue(
        concurrent_request,
        AlertRecord.create(
            WARNING_ALERT,
            "Queued concurrently",
            max_message_length=settings.resolved_message_max_length,
        ),
    )
    await storage.acknowledge(rendered_request, now=0)

    remaining = await storage.peek(concurrent_request, now=0)

    assert [alert.message for alert in rendered] == ["Rendered"]
    assert [alert.message for alert in remaining] == ["Queued concurrently"]


@pytest.mark.anyio
async def test_cache_messages_expire_no_later_than_the_request_session() -> None:
    settings = _settings({"storage_backend": "cache", "message_ttl_seconds": 60})
    observed_ttls: list[float] = []

    class RecordingBackend:
        async def append(
            self,
            queue_key: str,
            payload: dict[str, object],
            *,
            queue_depth: int,
            ttl_seconds: float,
        ) -> None:
            del queue_key, payload, queue_depth
            observed_ttls.append(ttl_seconds)

    storage = CacheMessagesStorage(settings, RecordingBackend())
    request = _request()
    request.scope["session"] = RequestSession(expires_at=110.0)
    await storage.enqueue(
        request,
        AlertRecord(
            severity=SUCCESS_ALERT,
            message="Expires with the session",
            created_at=100.0,
        ),
    )

    assert observed_ttls == [10.0]


@pytest.mark.anyio
async def test_cache_messages_use_a_renewed_session_expiry() -> None:
    settings = _settings({"storage_backend": "cache", "message_ttl_seconds": 60})
    observed_ttls: list[float] = []

    class RecordingBackend:
        async def append(
            self,
            queue_key: str,
            payload: dict[str, object],
            *,
            queue_depth: int,
            ttl_seconds: float,
        ) -> None:
            del queue_key, payload, queue_depth
            observed_ttls.append(ttl_seconds)

    storage = CacheMessagesStorage(settings, RecordingBackend())
    request = _request()
    request.scope["session"] = RequestSession(
        expires_at=101.0,
        prospective_expires_at=160.0,
    )
    await storage.enqueue(
        request,
        AlertRecord(
            severity=SUCCESS_ALERT,
            message="Renewed session",
            created_at=100.0,
        ),
    )

    assert observed_ttls == [60.0]


@pytest.mark.anyio
async def test_cache_acknowledgement_preserves_session_queue_expiry() -> None:
    settings = _settings({"storage_backend": "cache", "message_ttl_seconds": 60})
    observed_ttls: list[float | None] = []

    class RecordingBackend:
        async def peek(self, queue_key: str) -> tuple[dict[str, object], ...]:
            del queue_key
            return ()

        async def acknowledge(
            self,
            queue_key: str,
            *,
            observed: tuple[dict[str, object], ...] | None = None,
            ttl_seconds: float | None = None,
        ) -> None:
            del queue_key, observed
            observed_ttls.append(ttl_seconds)

    storage = CacheMessagesStorage(settings, RecordingBackend())
    request = _request(
        RequestSession(
            data={SESSION_QUEUE_ID_KEY: "queue"},
            expires_at=110.0,
        )
    )
    await storage.acknowledge(request, now=100.0)

    assert observed_ttls == [10.0]


@pytest.mark.anyio
async def test_cache_messages_expire_promptly_for_an_expired_session() -> None:
    settings = _settings({"storage_backend": "cache", "message_ttl_seconds": 60})
    observed_ttls: list[float] = []

    class RecordingBackend:
        async def append(
            self,
            queue_key: str,
            payload: dict[str, object],
            *,
            queue_depth: int,
            ttl_seconds: float,
        ) -> None:
            del queue_key, payload, queue_depth
            observed_ttls.append(ttl_seconds)

    storage = CacheMessagesStorage(settings, RecordingBackend())
    request = _request()
    request.scope["session"] = RequestSession(expires_at=100.0)
    await storage.enqueue(
        request,
        AlertRecord(
            severity=SUCCESS_ALERT,
            message="Expired session",
            created_at=100.0,
        ),
    )

    assert observed_ttls == [1.0]


@pytest.mark.anyio
async def test_cache_acknowledgement_snapshots_before_removing_alerts() -> None:
    settings = _settings({"storage_backend": "cache"})
    atomic_backend = NamedCacheQueueBackend(InMemoryAtomicCache())
    queue_key = f"{settings.cache_key_prefix}queue"
    concurrent_payload = {
        QUEUE_ENTRY_ID_KEY: "concurrent",
        "severity": WARNING_ALERT,
        "message": "Queued concurrently",
        "created_at": 2.0,
        "expires_at": 60.0,
    }

    class InterleavingBackend:
        async def peek(self, selected_queue_key: str) -> tuple[dict[str, object], ...]:
            observed = await atomic_backend.peek(selected_queue_key)
            await atomic_backend.append(
                selected_queue_key,
                concurrent_payload,
                queue_depth=settings.resolved_queue_depth,
                ttl_seconds=settings.resolved_message_ttl_seconds,
            )
            return observed

        async def acknowledge(
            self,
            selected_queue_key: str,
            *,
            observed: tuple[dict[str, object], ...] | None = None,
            ttl_seconds: float | None = None,
        ) -> None:
            await atomic_backend.acknowledge(
                selected_queue_key,
                observed=observed,
                ttl_seconds=ttl_seconds,
            )

    await atomic_backend.append(
        queue_key,
        {
            QUEUE_ENTRY_ID_KEY: "observed",
            "severity": SUCCESS_ALERT,
            "message": "Observed",
            "created_at": 1.0,
            "expires_at": 60.0,
        },
        queue_depth=settings.resolved_queue_depth,
        ttl_seconds=settings.resolved_message_ttl_seconds,
    )
    storage = CacheMessagesStorage(settings, InterleavingBackend())

    await storage.acknowledge(
        _request({SESSION_QUEUE_ID_KEY: "queue"}),
        now=0,
    )

    remaining = await atomic_backend.peek(queue_key)
    assert [payload["message"] for payload in remaining] == ["Queued concurrently"]


@pytest.mark.anyio
async def test_cache_messages_storage_uses_backend_atomic_pop() -> None:
    settings = _settings({"storage_backend": "cache"})
    payload = {
        "severity": SUCCESS_ALERT,
        "message": "Popped",
        "created_at": 1.0,
        "expires_at": 60.0,
    }

    class PopOnlyBackend:
        async def pop(self, queue_key: str) -> tuple[dict[str, object], ...]:
            assert queue_key.startswith(settings.cache_key_prefix)
            return (payload,)

        async def peek(self, queue_key: str) -> tuple[dict[str, object], ...]:
            raise AssertionError(f"Unexpected non-atomic peek for {queue_key}.")

    storage = CacheMessagesStorage(settings, PopOnlyBackend())
    alerts = await storage.pop(
        _request({SESSION_QUEUE_ID_KEY: "queue"}),
        now=2.0,
    )

    assert [alert.message for alert in alerts] == ["Popped"]


@pytest.mark.anyio
async def test_named_cache_messages_reject_missing_cache_module() -> None:
    with pytest.raises(SiteCapabilityError) as exc_info:
        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.messages",)},
                    "wybra.messages": {"storage_backend": "cache"},
                }
            ),
            environ={},
        )
    assert isinstance(exc_info.value.__cause__, MessagesConfigurationError)
    assert "wybra.cache" in str(exc_info.value.__cause__)


@pytest.mark.anyio
async def test_named_cache_messages_reject_missing_named_cache() -> None:
    with pytest.raises(SiteCapabilityError) as exc_info:
        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.cache", "wybra.messages")},
                    "cache": {},
                    "wybra.messages": {
                        "storage_backend": "cache",
                        "cache_name": "missing",
                    },
                }
            ),
            environ={},
        )
    assert isinstance(exc_info.value.__cause__, MessagesConfigurationError)
    assert "missing" in str(exc_info.value.__cause__)
    assert "queued request messages" in str(exc_info.value.__cause__)


@pytest.mark.anyio
async def test_named_cache_messages_reject_cache_without_atomic_feature() -> None:
    with pytest.raises(SiteCapabilityError) as exc_info:
        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.cache", "wybra.messages")},
                    "cache": {
                        "backend": "memory",
                        "features": [],
                    },
                    "wybra.messages": {"storage_backend": "cache"},
                }
            ),
            environ={},
        )
    assert isinstance(exc_info.value.__cause__, MessagesConfigurationError)
    error = str(exc_info.value.__cause__)
    assert "default" in error
    assert "AtomicCacheCapability" in error
    assert "queued request messages" in error


@pytest.mark.anyio
async def test_named_atomic_cache_queue_preserves_depth_peek_and_pop() -> None:
    atomic = InMemoryAtomicCache()
    backend = NamedCacheQueueBackend(atomic)
    first = {"severity": SUCCESS_ALERT, "message": "One", "created_at": 1.0}
    second = {"severity": WARNING_ALERT, "message": "Two", "created_at": 2.0}
    third = {"severity": ERROR_ALERT, "message": "Three", "created_at": 3.0}

    await backend.append("alerts", first, queue_depth=2, ttl_seconds=60)
    await backend.append("alerts", second, queue_depth=2, ttl_seconds=60)
    await backend.append("alerts", third, queue_depth=2, ttl_seconds=60)

    assert await backend.peek("alerts") == (second, third)
    assert await backend.peek("alerts") == (second, third)
    assert await backend.pop("alerts") == (second, third)
    assert await backend.peek("alerts") == ()


@pytest.mark.anyio
async def test_named_atomic_cache_queue_uses_owner_prefix_and_ttl() -> None:
    now = 1.0
    atomic = InMemoryAtomicCache(clock=lambda: now)
    backend = NamedCacheQueueBackend(atomic)
    payload = {"severity": SUCCESS_ALERT, "message": "Saved", "created_at": 1.0}

    await backend.append(
        "wybra:messages:queue",
        payload,
        queue_depth=2,
        ttl_seconds=2,
    )

    assert await atomic.get("messages", "wybra:messages:queue") is not None
    now = 4.0
    assert await backend.peek("wybra:messages:queue") == ()


@pytest.mark.anyio
async def test_named_atomic_cache_queue_close_does_not_close_shared_feature() -> None:
    atomic = InMemoryAtomicCache()
    backend = NamedCacheQueueBackend(atomic)
    payload = {"severity": SUCCESS_ALERT, "message": "Saved", "created_at": 1.0}

    await backend.append("alerts", payload, queue_depth=2, ttl_seconds=60)
    await backend.close()

    assert await backend.peek("alerts") == (payload,)


@pytest.mark.anyio
@pytest.mark.parametrize("conflict_operation", ("create", "swap", "delete"))
async def test_named_atomic_cache_queue_retries_revision_conflicts(
    conflict_operation: str,
) -> None:
    atomic = InMemoryAtomicCache()
    existing = {"severity": SUCCESS_ALERT, "message": "Existing", "created_at": 1.0}
    appended = {"severity": WARNING_ALERT, "message": "Appended", "created_at": 2.0}
    if conflict_operation != "create":
        await atomic.create(
            "messages",
            "alerts",
            json.dumps((existing,)).encode(),
            ttl=60,
        )

    class ConflictOnceAtomic:
        conflicted = False

        async def get(self, owner: str, key: str) -> AtomicCacheValue | None:
            return await atomic.get(owner, key)

        async def create(
            self,
            owner: str,
            key: str,
            value: bytes,
            *,
            ttl: float,
        ) -> AtomicCacheValue | None:
            if conflict_operation == "create" and not self.conflicted:
                self.conflicted = True
                return None
            return await atomic.create(owner, key, value, ttl=ttl)

        async def compare_and_swap(
            self,
            owner: str,
            key: str,
            expected: CacheRevision,
            value: bytes,
            *,
            ttl: float,
        ) -> AtomicCacheValue | None:
            if conflict_operation == "swap" and not self.conflicted:
                self.conflicted = True
                return None
            return await atomic.compare_and_swap(
                owner,
                key,
                expected,
                value,
                ttl=ttl,
            )

        async def compare_and_delete(
            self,
            owner: str,
            key: str,
            expected: CacheRevision,
        ) -> bool:
            if conflict_operation == "delete" and not self.conflicted:
                self.conflicted = True
                return False
            return await atomic.compare_and_delete(owner, key, expected)

    backend = NamedCacheQueueBackend(ConflictOnceAtomic())

    if conflict_operation == "delete":
        assert await backend.pop("alerts") == (existing,)
    else:
        await backend.append("alerts", appended, queue_depth=2, ttl_seconds=60)
        expected = (
            (appended,) if conflict_operation == "create" else (existing, appended)
        )
        assert await backend.peek("alerts") == expected


@pytest.mark.anyio
async def test_named_atomic_cache_queue_bounds_contention_retries() -> None:
    class ContendedAtomic:
        create_calls = 0

        async def get(self, owner: str, key: str) -> None:
            return None

        async def create(
            self,
            owner: str,
            key: str,
            value: bytes,
            *,
            ttl: float,
        ) -> None:
            self.create_calls += 1
            return None

    atomic = ContendedAtomic()
    backend = NamedCacheQueueBackend(atomic, max_conflict_attempts=2)

    with pytest.raises(MessageStorageError, match="contention limit"):
        await backend.append(
            "alerts",
            {"severity": SUCCESS_ALERT, "message": "Saved", "created_at": 1.0},
            queue_depth=2,
            ttl_seconds=60,
        )

    assert atomic.create_calls == 2


@pytest.mark.anyio
async def test_named_atomic_cache_queue_reports_readiness_failure() -> None:
    class UnavailableAtomic:
        async def get(self, owner: str, key: str) -> None:
            raise OSError("unavailable")

    backend = NamedCacheQueueBackend(UnavailableAtomic())

    with pytest.raises(MessageStorageError, match="unavailable"):
        await backend.validate()


@pytest.mark.anyio
async def test_named_atomic_cache_queue_ignores_malformed_utf8() -> None:
    atomic = InMemoryAtomicCache()
    await atomic.create("messages", "alerts", b"\xff", ttl=60)
    backend = NamedCacheQueueBackend(atomic)

    assert await backend.peek("alerts") == ()


@pytest.mark.anyio
async def test_database_storage_persists_and_pops_alerts() -> None:
    settings = _settings({"storage_backend": "database"})
    async with _database_site() as (site, _db_capability):
        storage = DatabaseMessagesStorage(
            settings,
            site.capability_proxy(DatabaseCapability),
        )
        capability = DefaultMessagesCapability(settings, storage)
        session: dict[str, object] = {}
        await capability.error(_request(session), "Stored")
        alerts = await capability.consume_alerts(_request(session))
        empty_alerts = await capability.consume_alerts(_request(session))

    assert [alert.severity for alert in alerts] == [ERROR_ALERT]
    assert [alert.message for alert in alerts] == ["Stored"]
    assert empty_alerts == ()


@pytest.mark.anyio
async def test_database_storage_queue_depth_keeps_newest_alerts() -> None:
    settings = _settings({"storage_backend": "database", "queue_depth": 2})
    async with _database_site() as (site, _db_capability):
        capability = DefaultMessagesCapability(
            settings,
            DatabaseMessagesStorage(
                settings,
                site.capability_proxy(DatabaseCapability),
            ),
        )
        session: dict[str, object] = {}
        await capability.success(_request(session), "One")
        await capability.warning(_request(session), "Two")
        await capability.error(_request(session), "Three")
        alerts = await capability.consume_alerts(_request(session))

    assert [alert.message for alert in alerts] == ["Two", "Three"]


@pytest.mark.anyio
async def test_database_storage_removes_alert_queue_when_session_is_deleted() -> None:
    settings = _settings({"storage_backend": "database"})
    async with _database_site(
        modules=("wybra.messages", "wybra.sessions"),
    ) as (site, db_capability):
        messages = DefaultMessagesCapability(
            settings,
            DatabaseMessagesStorage(
                settings,
                site.capability_proxy(DatabaseCapability),
            ),
        )
        cleanup_registry = SessionCleanupRegistry()
        cleanup_registry.register(messages.cleanup_session_data)
        sessions = DatabaseRequestSessionStorage(
            database=site.capability_proxy(DatabaseCapability),
            connection_name="default",
            payload_max_bytes=1024,
            cleanup_registry=cleanup_registry,
        )
        session_data: dict[str, object] = {}
        session_id = create_session_id(now=1.0)
        await messages.error(_request(session_data), "Stored")
        await sessions.save(
            session_id,
            SessionRecord(
                data=dict(session_data),
                created_at=1.0,
                updated_at=1.0,
                expires_at=100.0,
            ),
        )

        assert SESSION_QUEUE_ID_KEY in session_data
        assert await _message_alert_count(db_capability) == 1

        await sessions.delete(session_id)

        assert await _message_alert_count(db_capability) == 0


@pytest.mark.anyio
async def test_database_storage_removes_alert_queue_when_session_expires() -> None:
    settings = _settings({"storage_backend": "database"})
    async with _database_site(
        modules=("wybra.messages", "wybra.sessions"),
    ) as (site, db_capability):
        messages = DefaultMessagesCapability(
            settings,
            DatabaseMessagesStorage(
                settings,
                site.capability_proxy(DatabaseCapability),
            ),
        )
        cleanup_registry = SessionCleanupRegistry()
        cleanup_registry.register(messages.cleanup_session_data)
        sessions = DatabaseRequestSessionStorage(
            database=site.capability_proxy(DatabaseCapability),
            connection_name="default",
            payload_max_bytes=1024,
            cleanup_registry=cleanup_registry,
        )
        session_data: dict[str, object] = {}
        session_id = create_session_id(now=1.0)
        await messages.warning(_request(session_data), "Expired")
        await sessions.save(
            session_id,
            SessionRecord(
                data=dict(session_data),
                created_at=1.0,
                updated_at=1.0,
                expires_at=2.0,
            ),
        )

        assert await _message_alert_count(db_capability) == 1
        assert await sessions.load(session_id, now=3.0) is None

        assert await _message_alert_count(db_capability) == 0


@pytest.mark.anyio
async def test_database_storage_cleanup_removes_expired_alerts() -> None:
    settings = _settings({"storage_backend": "database"})
    async with _database_site() as (site, db_capability):
        storage = DatabaseMessagesStorage(
            settings,
            site.capability_proxy(DatabaseCapability),
        )
        async with tortoise_transaction(
            db_capability, db_capability.database().for_write()
        ) as connection:
            await MessageAlert.create(
                queue_key="queue",
                severity=SUCCESS_ALERT,
                message="Expired",
                created_at=1.0,
                expires_at=2.0,
                using_db=connection,
            )

        assert await _message_alert_count(db_capability) == 1

        await storage.cleanup(now=3.0)

        assert await _message_alert_count(db_capability) == 0


def test_database_storage_exposes_model_and_migration_surface() -> None:
    migration_locations = discover_migration_version_locations("wybra.messages")

    assert discover_model_package("wybra.messages") == "wybra.messages.models"
    assert migration_locations
    assert any(
        path.name == "migrations" and path.joinpath("0001_initial.py").is_file()
        for path in migration_locations
    )


@pytest.mark.anyio
async def test_messages_context_peeks_until_alerts_are_rendered() -> None:
    settings = _settings()
    capability = DefaultMessagesCapability(settings, SessionMessagesStorage(settings))
    site = _site(settings, capability)
    session: dict[str, object] = {}
    request = _request(session)
    request.scope["app"] = site.app

    await capability.success(request, "Saved")

    context = await messages_context(request, TemplateContext())
    repeated_context = await messages_context(request, TemplateContext())

    assert SESSION_ALERTS_KEY in session
    assert bool(context["has_alerts"]) is True
    assert context["messages_enabled"] is True
    assert [alert.message for alert in context["alerts"]] == ["Saved"]
    assert [alert.message for alert in repeated_context["alerts"]] == ["Saved"]
    assert getattr(request.state, REQUEST_ALERTS_RENDERED_ATTRIBUTE) is True

    await capability.acknowledge_alerts(request)

    assert SESSION_ALERTS_KEY not in session


@pytest.mark.anyio
async def test_default_alert_component_escapes_message_text() -> None:
    renderer = DefaultTemplateCapability(
        template_sources=(
            PackageResourceSource(package="wybra.messages", directory="templates"),
        ),
        include_request_context=False,
        cache_size=0,
    )

    content = await renderer.render_template(
        "components/alerts.html",
        {
            "alerts": (
                AlertRecord.create(
                    ERROR_ALERT,
                    "<strong>Unsafe</strong>",
                    max_message_length=100,
                    created_at=1.0,
                ),
            )
        },
    )

    assert "&lt;strong&gt;Unsafe&lt;/strong&gt;" in content
    assert "<strong>Unsafe</strong>" not in content
    assert 'data-alert-severity="error"' in content
    assert 'aria-label="Page notifications"' in content
    assert 'aria-labelledby="wybra-alert-1-heading"' in content
    assert "Error notification" in content
    assert "wybra-visually-hidden" in content


async def _message_alert_count(
    db_capability: DatabaseCapability,
) -> int:
    async with tortoise_transaction(
        db_capability, db_capability.database().for_write()
    ) as connection:
        return await MessageAlert.all(using_db=connection).count()


@pytest.mark.anyio
async def test_widget_layout_renders_alert_component_when_context_exists() -> None:
    renderer = DefaultTemplateCapability(
        template_sources=(
            PackageResourceSource(package="wybra.widgets", directory="templates"),
            PackageResourceSource(package="wybra.messages", directory="templates"),
        ),
        include_request_context=False,
        cache_size=0,
    )

    content = await renderer.render_template(
        "layouts/page.html",
        {
            "asset_url": lambda path: f"/static/{path}",
            "has_alerts": True,
            "messages_enabled": True,
            "page_title": "Page",
            "route_name": "page",
            "theme_attribute": "",
            "alerts": (
                AlertRecord.create(
                    SUCCESS_ALERT,
                    "Saved",
                    max_message_length=100,
                    created_at=1.0,
                ),
            ),
        },
    )

    assert "/static/styles/messages.css" in content
    assert "wybra-alert--success" in content


@pytest.mark.anyio
async def test_widget_layout_omits_alert_component_without_context() -> None:
    renderer = DefaultTemplateCapability(
        template_sources=(
            PackageResourceSource(package="wybra.widgets", directory="templates"),
        ),
        include_request_context=False,
        cache_size=0,
    )

    content = await renderer.render_template(
        "layouts/page.html",
        {
            "asset_url": lambda path: f"/static/{path}",
            "page_title": "Page",
            "route_name": "page",
            "theme_attribute": "",
        },
    )

    assert "styles/messages.css" not in content
    assert "wybra-alert" not in content


def test_alerts_validation_target_is_discovered() -> None:
    targets = discover_validation_targets(("wybra.messages",))

    assert "alerts" in targets


def test_validate_alerts_checks_settings_and_resources() -> None:
    settings = SimpleNamespace(
        modules=("wybra.messages",),
        config=ConfigService(
            [MappingConfigSource({"wybra.messages": {}})],
            config_defs=(module_config,),
            discover_module_config=False,
        ),
    )

    result = validate_alerts(settings)

    assert result.is_ok


def test_validate_alerts_accepts_named_atomic_cache_selection() -> None:
    settings = _cache_validation_settings(
        modules=("wybra.cache", "wybra.messages"),
        cache_name="messages",
        named_caches={"cache.messages": {"backend": "memory"}},
    )

    result = validate_alerts(settings)

    assert result.is_ok
    assert any(
        "cache=messages" in check.description
        and "AtomicCacheCapability" in check.description
        and "verified at startup" in check.description
        for check in result.checks
    )


def test_validate_alerts_rejects_missing_cache_module() -> None:
    result = validate_alerts(_cache_validation_settings(modules=("wybra.messages",)))

    assert result.is_ok is False
    assert any("wybra.cache" in error for error in result.errors)


def test_validate_alerts_rejects_missing_named_cache() -> None:
    result = validate_alerts(
        _cache_validation_settings(
            modules=("wybra.cache", "wybra.messages"),
            cache_name="missing",
        )
    )

    assert result.is_ok is False
    assert any("missing" in error for error in result.errors)
    assert any("queued request messages" in error for error in result.errors)
