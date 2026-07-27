from __future__ import annotations

import importlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from typing import cast

import pytest
from fastapi import FastAPI, Request
from fastapi.responses import Response

from wybra.cache import (
    CachesCapability,
    InMemoryCache,
)
from wybra.cache import (
    module_config as cache_module_config,
)
from wybra.config import ConfigService, ConfigSourceError, MappingConfigSource
from wybra.core.config import RUNTIME_CONFIG_DEF
from wybra.core.exceptions import ConfigurationError
from wybra.db import DatabaseCapability
from wybra.db.capabilities import tortoise_transaction
from wybra.db.surfaces import (
    discover_migration_version_locations,
    discover_model_package,
    migration_version_locations_from_modules,
    model_packages_from_modules,
)
from wybra.events._core import EVT_SESSION, Event, EventsCapability
from wybra.events.sessions import SessionLifecycleEvent
from wybra.services.crypto import (
    ENV_WYBRA_SECRET_KEY,
    SecretEnvelopeService,
    generate_secret_key_entry,
)
from wybra.sessions import (
    CookieSessionStorage,
    DatabaseSessionStorage,
    FileSessionStorage,
    MemorySessionStorage,
    NamedCacheSessionStorage,
    RequestSession,
    SessionCleanupRegistry,
    SessionIdentifierError,
    SessionMiddlewareContext,
    SessionRecord,
    SessionRecordModel,
    SessionsConfigurationError,
    SessionsSettings,
    SessionStorage,
    SessionStorageBackend,
    create_session_id,
    module_config,
    setup_core_sessions,
    validate_session_id,
    validate_sessions,
)
from wybra.sessions.middleware import SESSION_CLEANUP_INTERVAL_SECONDS
from wybra.sessions.setup import session_storage_from_site
from wybra.sessions.storage import CacheSessionStorage, SessionStorageError
from wybra.site import Site, SiteCapabilityError, start, start_site
from wybra.testing import (
    WybraTestClient,
    create_test_site,
    migrated_test_database,
)


def _config(
    *,
    app: dict[str, object] | None = None,
    sessions: dict[str, object] | None = None,
    environ: dict[str, str] | None = None,
) -> ConfigService:
    ConfigService.set_runtime_environment({} if environ is None else environ)
    return ConfigService(
        [
            MappingConfigSource(
                {
                    "app": {"deployment_environment": "local", **(app or {})},
                    "wybra.sessions": sessions or {},
                }
            )
        ],
        config_defs=(RUNTIME_CONFIG_DEF, module_config),
        discover_module_config=False,
    )


def _settings(
    values: dict[str, object] | None = None,
    *,
    app: dict[str, object] | None = None,
    environ: dict[str, str] | None = None,
) -> SessionsSettings:
    return SessionsSettings.load_settings(
        _config(app=app, sessions=values, environ=environ)
    )


def _cache_session_validation_config(
    *,
    modules: tuple[str, ...],
    cache_name: str | None = None,
    named_caches: dict[str, dict[str, object]] | None = None,
) -> ConfigService:
    values: dict[str, dict[str, object]] = {
        "app": {
            "deployment_environment": "local",
            "modules": modules,
        },
        "cache": {},
        "wybra.sessions": {
            "storage_backend": "cache",
            **({} if cache_name is None else {"cache_name": cache_name}),
        },
    }
    values.update(named_caches or {})
    return ConfigService(
        [MappingConfigSource(values)],
        config_defs=(RUNTIME_CONFIG_DEF, cache_module_config, module_config),
        discover_module_config=False,
    )


def _record(
    *,
    data: dict[str, object] | None = None,
    created_at: float = 1.0,
    updated_at: float = 1.0,
    expires_at: float = 60.0,
) -> SessionRecord:
    return SessionRecord(
        data={} if data is None else data,
        created_at=created_at,
        updated_at=updated_at,
        expires_at=expires_at,
    )


@asynccontextmanager
async def _database_site(
    *,
    modules: tuple[str, ...] = ("wybra.sessions",),
) -> AsyncIterator[tuple[Site, DatabaseCapability]]:
    async with migrated_test_database(modules=modules) as database:
        site = create_test_site({"app": {"modules": modules}})
        capability = database.capability()
        site.provide_capability(DatabaseCapability, capability)
        yield site, capability


def test_sessions_settings_defaults_to_cookie_for_local_deployments() -> None:
    settings = _settings()

    assert settings.resolved_storage_backend is SessionStorageBackend.COOKIE
    assert settings.resolved_cookie_secure is False
    assert settings.resolved_lifetime_seconds == 14 * 24 * 60 * 60


def test_sessions_settings_requires_explicit_backend_outside_local() -> None:
    with pytest.raises(ConfigurationError, match="storage_backend"):
        _settings(app={"deployment_environment": "production"})


def test_sessions_settings_rejects_invalid_backend() -> None:
    with pytest.raises(ConfigSourceError, match="storage_backend"):
        _settings({"storage_backend": "unknown"})


def test_sessions_settings_rejects_same_site_none_without_secure() -> None:
    with pytest.raises(ConfigurationError, match="cookie_secure"):
        _settings({"cookie_same_site": "none"})


def test_cache_session_settings_default_to_named_default_cache() -> None:
    settings = _settings({"storage_backend": "cache"})

    assert settings.cache_name is None
    assert settings.resolved_cache_name == "default"
    assert settings.cache_url is None


def test_cache_session_settings_accept_explicit_cache_name() -> None:
    settings = _settings(
        {"storage_backend": "cache", "cache_name": "session"},
    )

    assert settings.cache_name == "session"
    assert settings.resolved_cache_name == "session"


def test_cache_session_settings_support_cache_name_environment_override() -> None:
    settings = _settings(
        {"storage_backend": "cache"},
        environ={"SESSIONS_CACHE_NAME": "session"},
    )

    assert settings.cache_name == "session"
    assert settings.resolved_cache_name == "session"


def test_cache_session_settings_reject_invalid_cache_name() -> None:
    with pytest.raises(ConfigSourceError, match="cache_name"):
        _settings({"storage_backend": "cache", "cache_name": "Session Cache"})


def test_cache_session_settings_reject_name_with_legacy_url() -> None:
    with pytest.raises(ConfigurationError, match="cache_name.*cache_url"):
        _settings(
            {
                "storage_backend": "cache",
                "cache_name": "default",
                "cache_url": "memory://sessions",
            }
        )


def test_session_settings_always_reject_ambiguous_cache_selection() -> None:
    with pytest.raises(ConfigurationError, match="cache_name.*cache_url"):
        _settings(
            {
                "storage_backend": "database",
                "cache_name": "default",
                "cache_url": "memory://sessions",
            }
        )


def test_cache_session_settings_preserve_legacy_url_compatibility() -> None:
    settings = _settings(
        {"storage_backend": "cache", "cache_url": "memory://sessions"},
    )

    assert settings.cache_name is None
    assert settings.cache_url == "memory://sessions"


def test_sessions_settings_resolves_file_directory_against_project_root(
    tmp_path: Path,
) -> None:
    settings = _settings(
        {"storage_backend": "file", "file_directory": "runtime/sessions"},
        app={"project_root": tmp_path},
    )

    assert settings.resolved_file_directory == (tmp_path / "runtime/sessions")


def test_session_ids_are_safe_validated_and_timestamp_ordered() -> None:
    earlier = create_session_id(now=10.0)
    later = create_session_id(now=11.0)

    assert validate_session_id(earlier) == earlier
    assert validate_session_id(later) == later
    assert earlier < later
    assert "/" not in earlier
    assert "\\" not in earlier

    with pytest.raises(SessionIdentifierError):
        validate_session_id("../unsafe")


def test_request_session_tracks_mutation_and_clear_state() -> None:
    session = RequestSession({"existing": "value"}, session_id="session-id")

    session["new"] = "saved"
    assert session.modified is True
    assert session.accessed is True
    assert session.cleared is False

    session.clear()
    assert dict(session) == {}
    assert session.modified is True
    assert session.cleared is True


@pytest.mark.anyio
async def test_memory_storage_saves_copies_and_expires_records() -> None:
    storage = MemorySessionStorage(payload_max_bytes=1024)
    record = _record(data={"value": "saved"}, expires_at=5.0)

    await storage.save("session", record)
    loaded = await storage.load("session", now=2.0)
    expired = await storage.load("session", now=6.0)

    assert loaded == record
    assert loaded is not record
    assert expired is None


@pytest.mark.anyio
async def test_storage_rejects_oversized_payloads() -> None:
    storage = MemorySessionStorage(payload_max_bytes=10)

    with pytest.raises(SessionStorageError, match="payload exceeds"):
        await storage.save("session", _record(data={"value": "too large"}))


@pytest.mark.anyio
async def test_file_storage_writes_loads_deletes_and_cleans_expired_records(
    tmp_path: Path,
) -> None:
    storage = FileSessionStorage(directory=tmp_path, payload_max_bytes=1024)
    active_id = create_session_id(now=1.0)
    expired_id = create_session_id(now=2.0)

    await storage.save(active_id, _record(data={"value": "active"}, expires_at=50.0))
    await storage.save(expired_id, _record(data={"value": "old"}, expires_at=2.0))
    await storage.cleanup(now=10.0)

    assert await storage.load(active_id, now=10.0) == _record(
        data={"value": "active"},
        expires_at=50.0,
    )
    assert await storage.load(expired_id, now=10.0) is None
    assert (tmp_path / f"{active_id}.json").is_file()
    assert not (tmp_path / f"{expired_id}.json").exists()

    await storage.delete(active_id)
    assert await storage.load(active_id, now=10.0) is None


@pytest.mark.anyio
async def test_file_storage_save_wraps_directory_errors(tmp_path: Path) -> None:
    file_path = tmp_path / "sessions"
    file_path.write_text("not a directory", encoding="utf-8")
    storage = FileSessionStorage(directory=file_path, payload_max_bytes=1024)

    with pytest.raises(SessionStorageError, match="directory is not available"):
        await storage.save(create_session_id(now=1.0), _record())


@pytest.mark.anyio
async def test_file_storage_validate_wraps_directory_errors(tmp_path: Path) -> None:
    file_path = tmp_path / "sessions"
    file_path.write_text("not a directory", encoding="utf-8")
    storage = FileSessionStorage(directory=file_path, payload_max_bytes=1024)

    with pytest.raises(SessionStorageError, match="directory is not available"):
        await storage.validate()


def test_cookie_storage_encrypts_and_validates_payloads() -> None:
    storage = CookieSessionStorage(
        service=SecretEnvelopeService.for_testing(),
        payload_max_bytes=1024,
        cookie_payload_max_bytes=4096,
    )
    record = _record(data={"value": "cookie"}, expires_at=20.0)

    cookie_value = storage.dump_cookie("session", record)
    loaded = storage.load_cookie(cookie_value, now=5.0)

    assert loaded == ("session", record)
    assert storage.load_cookie("not-an-envelope", now=5.0) is None
    assert storage.load_cookie(cookie_value, now=25.0) is None


@pytest.mark.anyio
async def test_cache_storage_supports_memory_url() -> None:
    storage = CacheSessionStorage(
        url="memory://sessions",
        key_prefix="test:",
        payload_max_bytes=1024,
    )

    await storage.save("session", _record(data={"value": "cached"}))

    assert await storage.load("session", now=2.0) == _record(data={"value": "cached"})

    await storage.delete("session")
    assert await storage.load("session", now=2.0) is None


@pytest.mark.anyio
async def test_named_cache_storage_round_trips_and_isolates_key_prefixes() -> None:
    cache = InMemoryCache()
    primary = NamedCacheSessionStorage(
        cache=cache,
        key_prefix="primary:",
        payload_max_bytes=1024,
    )
    secondary = NamedCacheSessionStorage(
        cache=cache,
        key_prefix="secondary:",
        payload_max_bytes=1024,
    )
    record = _record(data={"value": "cached"})

    await primary.save("session", record)

    assert await primary.load("session", now=2.0) == record
    assert await secondary.load("session", now=2.0) is None

    await primary.close()
    assert await primary.load("session", now=2.0) == record


@pytest.mark.anyio
async def test_named_cache_storage_deletes_with_session_cleanup() -> None:
    cleaned: list[dict[str, object]] = []
    cleanup_registry = SessionCleanupRegistry()

    async def cleanup(data: object) -> None:
        assert isinstance(data, dict)
        cleaned.append(data)

    cleanup_registry.register(cleanup)
    storage = NamedCacheSessionStorage(
        cache=InMemoryCache(),
        key_prefix="test:",
        payload_max_bytes=1024,
        cleanup_registry=cleanup_registry,
    )
    await storage.save("session", _record(data={"value": "cached"}))

    await storage.delete("session")

    assert cleaned == [{"value": "cached"}]
    assert await storage.load("session", now=2.0) is None


@pytest.mark.anyio
async def test_named_cache_storage_uses_record_lifetime_as_ttl() -> None:
    recorded_ttls: list[float] = []

    class RecordingCache:
        async def get(self, owner: str, key: str) -> bytes | None:
            del owner, key
            return None

        async def set(
            self,
            owner: str,
            key: str,
            value: bytes,
            *,
            ttl: float,
        ) -> None:
            del owner, key, value
            recorded_ttls.append(ttl)

        async def delete(self, owner: str, key: str) -> None:
            del owner, key

        async def get_or_set(self, *args: object, **kwargs: object) -> bytes:
            del args, kwargs
            raise AssertionError("Session storage must not use cache fills.")

    storage = NamedCacheSessionStorage(
        cache=RecordingCache(),
        key_prefix="test:",
        payload_max_bytes=1024,
    )

    await storage.save(
        "session",
        _record(updated_at=10.5, expires_at=70.75),
    )

    assert recorded_ttls == [60.25]


@pytest.mark.anyio
async def test_named_cache_storage_validation_reports_unavailable_cache() -> None:
    class UnavailableCache:
        async def get(self, owner: str, key: str) -> bytes | None:
            del owner, key
            raise RuntimeError("cache unavailable")

        async def set(
            self,
            owner: str,
            key: str,
            value: bytes,
            *,
            ttl: float,
        ) -> None:
            del owner, key, value, ttl
            raise AssertionError("Validation must not mutate the cache.")

        async def delete(self, owner: str, key: str) -> None:
            del owner, key
            raise AssertionError("Validation must not mutate the cache.")

        async def get_or_set(self, *args: object, **kwargs: object) -> bytes:
            del args, kwargs
            raise AssertionError("Validation must not use cache fills.")

    storage = NamedCacheSessionStorage(
        cache=UnavailableCache(),
        key_prefix="test:",
        payload_max_bytes=1024,
    )

    with pytest.raises(SessionStorageError, match="unavailable"):
        await storage.validate()


@pytest.mark.anyio
async def test_database_storage_persists_session_records() -> None:
    async with _database_site() as (site, capability):
        storage = DatabaseSessionStorage(
            database=site.capability_proxy(DatabaseCapability),
            connection_name="default",
            payload_max_bytes=1024,
        )
        await storage.save("session", _record(data={"value": "database"}))
        assert await storage.load("session", now=2.0) == _record(
            data={"value": "database"}
        )
        async with tortoise_transaction(
            capability, capability.database().for_write()
        ) as connection:
            row = await SessionRecordModel.get_or_none(
                id="session",
                using_db=connection,
            )
            assert row is not None
            assert row.data == '{"value":"database"}'
            assert row.created_at == 1.0
            assert row.updated_at == 1.0
            assert row.expires_at == 60.0

        await storage.save(
            "session",
            _record(data={"value": "updated"}, updated_at=3.0, expires_at=80.0),
        )
        assert await storage.load("session", now=4.0) == _record(
            data={"value": "updated"},
            updated_at=3.0,
            expires_at=80.0,
        )

        await storage.delete("session")
        assert await storage.load("session", now=2.0) is None


def test_core_model_and_migration_surfaces_include_sessions() -> None:
    model_packages = model_packages_from_modules(())
    migration_locations = migration_version_locations_from_modules(())
    discovered_locations = discover_migration_version_locations("wybra.sessions")

    assert discover_model_package("wybra.sessions") == "wybra.sessions.models"
    assert "wybra.sessions.models" in model_packages
    assert discovered_locations
    assert discovered_locations[0] in migration_locations


def test_sessions_validation_target_loads_default_settings() -> None:
    settings = SimpleNamespace(
        config=_config(),
        deployment_environment="local",
    )

    result = validate_sessions(settings)

    assert result.is_ok
    assert any("storage_backend=cookie" in check.description for check in result.checks)


def test_sessions_validation_reports_named_cache_selection_safely() -> None:
    settings = SimpleNamespace(
        config=_cache_session_validation_config(
            modules=("wybra.cache",),
            cache_name="session",
            named_caches={"cache.session": {"backend": "memory"}},
        ),
        deployment_environment="local",
    )

    result = validate_sessions(settings)

    assert result.is_ok
    assert any("cache=session" in check.description for check in result.checks)


def test_sessions_validation_rejects_missing_cache_module() -> None:
    settings = SimpleNamespace(
        config=_cache_session_validation_config(modules=()),
        deployment_environment="local",
    )

    result = validate_sessions(settings)

    assert result.is_ok is False
    assert any("wybra.cache" in error for error in result.errors)


def test_sessions_validation_rejects_missing_named_cache() -> None:
    settings = SimpleNamespace(
        config=_cache_session_validation_config(
            modules=("wybra.cache",),
            cache_name="missing",
        ),
        deployment_environment="local",
    )

    result = validate_sessions(settings)

    assert result.is_ok is False
    assert any("missing" in error for error in result.errors)
    assert any("request sessions" in error for error in result.errors)


def test_sessions_validation_accepts_configured_named_cache() -> None:
    settings = SimpleNamespace(
        config=_cache_session_validation_config(
            modules=("wybra.cache",),
            cache_name="session",
            named_caches={"cache.session": {"backend": "memory"}},
        ),
        deployment_environment="local",
    )

    result = validate_sessions(settings)

    assert result.is_ok
    assert any("cache=session" in check.description for check in result.checks)


def test_sessions_validation_retries_cache_provider_import_after_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    discovery_module = importlib.import_module("wybra.cache.discovery")
    import_attempts = 0

    def import_cache_provider(_module_name: str) -> object:
        nonlocal import_attempts
        import_attempts += 1
        if import_attempts == 1:
            raise ImportError("optional dependency unavailable")
        return SimpleNamespace(provides_cache_capability=True)

    monkeypatch.setattr(discovery_module, "import_module", import_cache_provider)
    settings = SimpleNamespace(
        config=_cache_session_validation_config(modules=("custom.cache",)),
        deployment_environment="local",
    )

    first_result = validate_sessions(settings)
    second_result = validate_sessions(settings)

    assert first_result.is_ok is False
    assert second_result.is_ok
    assert import_attempts == 2


def test_sessions_validation_reports_legacy_cache_without_exposing_url() -> None:
    legacy_url = "redis://user:secret@cache.internal/1"
    settings = SimpleNamespace(
        config=_config(sessions={"storage_backend": "cache", "cache_url": legacy_url}),
        deployment_environment="local",
    )

    result = validate_sessions(settings)

    rendered = repr(result)
    assert result.is_ok
    assert "legacy" in rendered
    assert legacy_url not in rendered
    assert "secret" not in rendered


@pytest.mark.anyio
async def test_start_registers_core_session_storage_capability() -> None:
    site = await start(
        FastAPI(),
        config_source=MappingConfigSource({"app": {"modules": ()}}),
        environ={},
    )

    try:
        assert site.has_capability(SessionStorage) is True
        assert isinstance(session_storage_from_site(site), CookieSessionStorage)
    finally:
        await site.close()


@pytest.mark.anyio
async def test_start_uses_default_named_cache_for_sessions() -> None:
    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": ("wybra.cache",)},
                "cache": {},
                "wybra.sessions": {"storage_backend": "cache"},
            }
        ),
        environ={},
    )

    try:
        storage = session_storage_from_site(site)
        caches = site.require_capability(CachesCapability)

        assert isinstance(storage, NamedCacheSessionStorage)
        assert storage.cache is caches.require("default").values
    finally:
        await site.close()


@pytest.mark.anyio
async def test_start_uses_explicit_isolated_named_cache_for_sessions() -> None:
    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": ("wybra.cache",)},
                "cache": {},
                "cache.session": {"backend": "memory"},
                "wybra.sessions": {
                    "storage_backend": "cache",
                    "cache_name": "session",
                },
            }
        ),
        environ={},
    )

    try:
        storage = session_storage_from_site(site)
        caches = site.require_capability(CachesCapability)
        record = _record(data={"value": "isolated"})

        assert isinstance(storage, NamedCacheSessionStorage)
        assert storage.cache is caches.require("session").values

        await storage.save("session-id", record)

        assert (
            await caches.require("default").values.get(
                "sessions",
                "wybra:sessions:session-id",
            )
            is None
        )
        assert (
            await caches.require("session").values.get(
                "sessions",
                "wybra:sessions:session-id",
            )
            is not None
        )
    finally:
        await site.close()


@pytest.mark.anyio
async def test_cache_sessions_require_cache_module_at_startup() -> None:
    with pytest.raises(SessionsConfigurationError, match=r"wybra\.cache"):
        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ()},
                    "wybra.sessions": {"storage_backend": "cache"},
                }
            ),
            environ={},
        )


@pytest.mark.anyio
async def test_cache_sessions_reject_missing_named_cache_at_startup() -> None:
    with pytest.raises(
        SessionsConfigurationError,
        match=r"missing.+request sessions",
    ):
        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ("wybra.cache",)},
                    "cache": {},
                    "wybra.sessions": {
                        "storage_backend": "cache",
                        "cache_name": "missing",
                    },
                }
            ),
            environ={},
        )


@pytest.mark.anyio
async def test_legacy_cache_url_still_starts_with_operator_warnings(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warning_messages: list[str] = []
    storage_module = importlib.import_module("wybra.sessions.storage")

    def record_warning(message: str, *args: object) -> None:
        warning_messages.append(message % args)

    monkeypatch.setattr(storage_module.logger, "warning", record_warning)

    with pytest.warns(DeprecationWarning, match="cache_name"):
        site = await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ()},
                    "wybra.sessions": {
                        "storage_backend": "cache",
                        "cache_url": "memory://sessions",
                    },
                }
            ),
            environ={},
        )

    try:
        assert isinstance(session_storage_from_site(site), CacheSessionStorage)
        assert any("cache_url is deprecated" in message for message in warning_messages)
    finally:
        await site.close()


@pytest.mark.anyio
async def test_non_cache_sessions_warn_about_ignored_cache_selection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    warning_messages: list[str] = []
    storage_module = importlib.import_module("wybra.sessions.storage")

    def record_warning(message: str, *args: object) -> None:
        warning_messages.append(message % args)

    monkeypatch.setattr(storage_module.logger, "warning", record_warning)

    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": ()},
                "wybra.sessions": {
                    "storage_backend": "cookie",
                    "cache_name": "session",
                },
            }
        ),
        environ={},
    )

    try:
        assert any("cache_name is ignored" in message for message in warning_messages)
        assert isinstance(session_storage_from_site(site), CookieSessionStorage)
    finally:
        await site.close()


@pytest.mark.anyio
async def test_session_middleware_persists_request_session_between_requests() -> None:
    app = FastAPI(
        lifespan=start_site(
            config_source=MappingConfigSource({"app": {"modules": ()}}),
            environ={},
        )
    )

    @app.get("/set")
    async def set_session(request: Request) -> dict[str, str]:
        request.session["value"] = "saved"
        return {"value": request.session["value"]}

    @app.get("/get")
    async def get_session(request: Request) -> dict[str, object]:
        return {"value": request.session.get("value")}

    async with WybraTestClient(app) as client:
        set_response = await client.get("/set")
        get_response = await client.get("/get")

    assert set_response.status_code == 200
    assert get_response.json() == {"value": "saved"}
    assert "wybra_session" in set_response.cookies


@pytest.mark.anyio
async def test_session_middleware_clears_session_cookie() -> None:
    app = FastAPI(
        lifespan=start_site(
            config_source=MappingConfigSource({"app": {"modules": ()}}),
            environ={},
        )
    )

    @app.get("/set")
    async def set_session(request: Request) -> dict[str, bool]:
        request.session["value"] = "saved"
        return {"ok": True}

    @app.get("/clear")
    async def clear_session(request: Request) -> dict[str, bool]:
        request.session.clear()
        return {"ok": True}

    @app.get("/get")
    async def get_session(request: Request) -> dict[str, object]:
        return {"value": request.session.get("value")}

    async with WybraTestClient(app) as client:
        await client.get("/set")
        clear_response = await client.get("/clear")
        get_response = await client.get("/get")

    assert clear_response.status_code == 200
    assert get_response.json() == {"value": None}


@pytest.mark.anyio
async def test_session_finalisation_skips_unchanged_sessions() -> None:
    class CountingStorage(MemorySessionStorage):
        def __init__(self) -> None:
            super().__init__(payload_max_bytes=1024)
            self.save_count = 0

        async def save(self, session_id: str, record: SessionRecord) -> None:
            self.save_count += 1
            await super().save(session_id, record)

    storage = CountingStorage()
    context = SessionMiddlewareContext(settings=_settings(), storage=storage)
    session = RequestSession(
        data={"value": "loaded"},
        session_id=create_session_id(now=1.0),
        created_at=1.0,
        expires_at=100.0,
    )

    await context.finalise_response(Response(), session, now=2.0)

    assert storage.save_count == 0


@pytest.mark.anyio
async def test_new_request_session_has_a_prospective_expiry() -> None:
    context = SessionMiddlewareContext(
        settings=_settings({"lifetime_seconds": 60}),
        storage=MemorySessionStorage(payload_max_bytes=1024),
    )
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [],
        }
    )

    session = await context.load_session(request, now=100.0)

    assert session.expires_at == 160.0
    assert session.prospective_expires_at == 160.0


@pytest.mark.anyio
async def test_session_lifecycle_events_exclude_session_data_and_identifiers() -> None:
    observed: list[Event] = []

    async def handler(event: Event) -> None:
        observed.append(event)

    site = await start(
        FastAPI(),
        config_source=MappingConfigSource(
            {
                "app": {"modules": (), "deployment_environment": "local"},
                "wybra.events": {"enabled": True},
            }
        ),
    )
    await site.require_capability(EventsCapability).subscribe(EVT_SESSION, handler)
    context = SessionMiddlewareContext(
        settings=_settings(),
        storage=MemorySessionStorage(payload_max_bytes=1024),
    )
    request = Request(
        {
            "type": "http",
            "method": "GET",
            "path": "/",
            "headers": [],
        }
    )

    try:
        session = await context.load_session(request, now=1.0)
        session["private"] = "not-observable"
        await context.finalise_response(Response(), session, now=2.0)
    finally:
        await site.close()

    session_events = [cast(SessionLifecycleEvent, event) for event in observed]
    assert [(event.operation, event.outcome) for event in session_events] == [
        ("load", "succeeded"),
        ("created", "succeeded"),
    ]
    assert all(isinstance(event, SessionLifecycleEvent) for event in observed)
    assert "not-observable" not in repr(observed)
    assert all(not hasattr(event, "session_id") for event in observed)


@pytest.mark.anyio
async def test_session_cleanup_runs_at_most_once_per_interval() -> None:
    class CountingStorage(MemorySessionStorage):
        def __init__(self) -> None:
            super().__init__(payload_max_bytes=1024)
            self.cleanup_count = 0

        async def cleanup(self, *, now: float) -> None:
            self.cleanup_count += 1
            await super().cleanup(now=now)

    storage = CountingStorage()
    context = SessionMiddlewareContext(settings=_settings(), storage=storage)

    await context.cleanup_expired(now=10.0)
    await context.cleanup_expired(now=20.0)
    await context.cleanup_expired(now=10.0 + SESSION_CLEANUP_INTERVAL_SECONDS)

    assert storage.cleanup_count == 2


@pytest.mark.anyio
async def test_cookie_session_backend_round_trips_through_middleware() -> None:
    app = FastAPI(
        lifespan=start_site(
            config_source=MappingConfigSource(
                {
                    "app": {"modules": ()},
                    "wybra.sessions": {"storage_backend": "cookie"},
                }
            ),
            environ={
                ENV_WYBRA_SECRET_KEY: generate_secret_key_entry(version="current")
            },
        )
    )

    @app.get("/set")
    async def set_session(request: Request) -> dict[str, str]:
        request.session["value"] = "cookie"
        return {"value": request.session["value"]}

    @app.get("/get")
    async def get_session(request: Request) -> dict[str, object]:
        return {"value": request.session.get("value")}

    async with WybraTestClient(app) as client:
        response = await client.get("/set")
        repeated = await client.get("/get")

    assert response.status_code == 200
    assert repeated.json() == {"value": "cookie"}


@pytest.mark.anyio
async def test_starlette_session_middleware_is_rejected() -> None:
    fake_session_middleware = type("SessionMiddleware", (), {})
    fake_session_middleware.__module__ = "starlette.middleware.sessions"
    app = FastAPI()
    app.add_middleware(fake_session_middleware)

    with pytest.raises(SessionsConfigurationError, match="Starlette"):
        await start(
            app,
            config_source=MappingConfigSource({"app": {"modules": ()}}),
            environ={},
        )


@pytest.mark.anyio
async def test_custom_session_storage_can_be_registered_by_module_setup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "custom_sessions.py").write_text(
        "\n".join(
            (
                "from wybra.sessions import MemorySessionStorage, SessionStorage",
                "STORAGE = MemorySessionStorage(payload_max_bytes=1024)",
                "async def setup_site(site):",
                "    site.provide_capability(SessionStorage, STORAGE)",
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    site = await start(
        FastAPI(),
        config_source=MappingConfigSource({"app": {"modules": ("custom_sessions",)}}),
        environ={},
    )

    try:
        custom_module = importlib.import_module("custom_sessions")
        assert session_storage_from_site(site) is custom_module.STORAGE
    finally:
        await site.close()


@pytest.mark.anyio
async def test_invalid_custom_session_storage_is_rejected(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    (tmp_path / "bad_sessions.py").write_text(
        "\n".join(
            (
                "from wybra.sessions import SessionStorage",
                "async def setup_site(site):",
                "    site.provide_capability(SessionStorage, object())",
            )
        ),
        encoding="utf-8",
    )
    monkeypatch.syspath_prepend(str(tmp_path))
    importlib.invalidate_caches()

    with pytest.raises(SiteCapabilityError, match="invalid type"):
        await start(
            FastAPI(),
            config_source=MappingConfigSource({"app": {"modules": ("bad_sessions",)}}),
            environ={},
        )


@pytest.mark.anyio
async def test_non_local_startup_requires_explicit_session_backend() -> None:
    with pytest.raises(ConfigurationError, match="storage_backend"):
        await start(
            FastAPI(),
            config_source=MappingConfigSource(
                {"app": {"modules": (), "deployment_environment": "production"}}
            ),
            environ={},
        )


@pytest.mark.anyio
async def test_setup_core_sessions_is_idempotent() -> None:
    site = Site(
        app=FastAPI(),
        config=_config(),
    )

    await setup_core_sessions(site)
    first_storage = session_storage_from_site(site)
    await setup_core_sessions(site)

    assert session_storage_from_site(site) is first_storage
