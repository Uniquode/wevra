from __future__ import annotations

import asyncio
import importlib
import json
import logging
import uuid
import warnings
from collections.abc import Mapping
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any, ClassVar, Protocol, cast, runtime_checkable

from anyio import Path as AsyncPath

from wybra.cache import CacheCapability, CacheNotFoundError, CachesCapability
from wybra.core.environment import environment_get
from wybra.core.runtime import LOCAL_ENVIRONMENT
from wybra.db import DatabaseCapability
from wybra.db.capabilities import tortoise_transaction
from wybra.services.crypto import (
    ENV_WYBRA_SECRET_KEY,
    ENVELOPE_PREFIX,
    SecretDataError,
    SecretEnvelopeService,
    SecretMaterialMissingError,
)
from wybra.sessions.cleanup import SessionCleanupRegistry
from wybra.sessions.config import SessionStorageBackend
from wybra.sessions.exceptions import (
    SessionsConfigurationError,
    SessionStorageError,
)
from wybra.sessions.models import SessionRecordModel
from wybra.sessions.settings import SessionsSettings
from wybra.site import Site, SiteCapabilityError, SiteCapabilityProxy

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class SessionRecord:
    data: dict[str, Any]
    created_at: float
    updated_at: float
    expires_at: float

    def expired(self, now: float) -> bool:
        return self.expires_at <= now


@runtime_checkable
class SessionStorage(Protocol):
    async def load(self, session_id: str, *, now: float) -> SessionRecord | None: ...

    async def save(self, session_id: str, record: SessionRecord) -> None: ...

    async def delete(self, session_id: str) -> None: ...

    async def validate(self) -> None: ...

    async def cleanup(self, *, now: float) -> None: ...

    async def close(self) -> None: ...


@dataclass(slots=True)
class MemorySessionStorage:
    payload_max_bytes: int
    cleanup_registry: SessionCleanupRegistry | None = None
    _records: dict[str, SessionRecord] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def load(self, session_id: str, *, now: float) -> SessionRecord | None:
        cleanup_data: dict[str, Any] | None = None
        async with self._lock:
            record = self._records.get(session_id)
            if record is None:
                return None
            if record.expired(now):
                self._records.pop(session_id, None)
                cleanup_data = dict(record.data)
                loaded = None
            else:
                loaded = _copy_record(record)
        if cleanup_data is not None:
            await _cleanup_session_data(self.cleanup_registry, cleanup_data)
        return loaded

    async def save(self, session_id: str, record: SessionRecord) -> None:
        _record_json(record, max_bytes=self.payload_max_bytes)
        async with self._lock:
            self._records[session_id] = _copy_record(record)

    async def delete(self, session_id: str) -> None:
        cleanup_data: dict[str, Any] | None = None
        async with self._lock:
            record = self._records.pop(session_id, None)
            if record is not None:
                cleanup_data = dict(record.data)
        if cleanup_data is not None:
            await _cleanup_session_data(self.cleanup_registry, cleanup_data)

    async def validate(self) -> None:
        return None

    async def cleanup(self, *, now: float) -> None:
        cleanup_records: list[dict[str, Any]] = []
        async with self._lock:
            expired_ids = [
                session_id
                for session_id, record in self._records.items()
                if record.expired(now)
            ]
            for session_id in expired_ids:
                record = self._records.pop(session_id, None)
                if record is not None:
                    cleanup_records.append(dict(record.data))
        for cleanup_data in cleanup_records:
            await _cleanup_session_data(self.cleanup_registry, cleanup_data)

    async def close(self) -> None:
        async with self._lock:
            self._records.clear()


@dataclass(frozen=True, slots=True)
class FileSessionStorage:
    directory: AsyncPath
    payload_max_bytes: int
    cleanup_registry: SessionCleanupRegistry | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.directory, AsyncPath):
            object.__setattr__(self, "directory", AsyncPath(self.directory))

    async def load(self, session_id: str, *, now: float) -> SessionRecord | None:
        path = self._path(session_id)
        try:
            raw_payload = await path.read_text(encoding="utf-8")
        except FileNotFoundError:
            return None
        except OSError as exc:
            raise SessionStorageError(
                f"Session file could not be read: {path}"
            ) from exc
        try:
            record = _record_from_json(raw_payload)
        except SessionStorageError:
            return None
        if record.expired(now):
            await path.unlink(missing_ok=True)
            await _cleanup_session_data(self.cleanup_registry, record.data)
            return None
        return record

    async def save(self, session_id: str, record: SessionRecord) -> None:
        payload = _record_json(record, max_bytes=self.payload_max_bytes)
        await self._ensure_directory()
        temporary_path = self.directory / f".{session_id}.{uuid.uuid4().hex}.tmp"
        try:
            await temporary_path.write_text(payload, encoding="utf-8")
            await temporary_path.replace(self._path(session_id))
        except OSError as exc:
            with suppress(OSError):
                await temporary_path.unlink(missing_ok=True)
            raise SessionStorageError("Session file could not be written.") from exc

    async def delete(self, session_id: str) -> None:
        path = self._path(session_id)
        cleanup_data: dict[str, Any] | None = None
        try:
            record = _record_from_json(await path.read_text(encoding="utf-8"))
        except FileNotFoundError, OSError, SessionStorageError:
            record = None
        if record is not None:
            cleanup_data = dict(record.data)
        await path.unlink(missing_ok=True)
        if cleanup_data is not None:
            await _cleanup_session_data(self.cleanup_registry, cleanup_data)

    async def validate(self) -> None:
        await self._ensure_directory()

    async def _ensure_directory(self) -> None:
        try:
            await self.directory.mkdir(parents=True, exist_ok=True)
        except OSError as exc:
            raise SessionStorageError(
                f"Session file directory is not available: {self.directory}"
            ) from exc
        if not await self.directory.is_dir():
            raise SessionStorageError(
                f"Session file directory is not available: {self.directory}"
            )

    async def cleanup(self, *, now: float) -> None:
        if not await self.directory.is_dir():
            return
        cleanup_records: list[dict[str, Any]] = []
        async for path in self.directory.glob("s1_*.json"):
            try:
                record = _record_from_json(await path.read_text(encoding="utf-8"))
            except OSError, SessionStorageError:
                continue
            if record.expired(now):
                await path.unlink(missing_ok=True)
                cleanup_records.append(dict(record.data))
        for cleanup_data in cleanup_records:
            await _cleanup_session_data(self.cleanup_registry, cleanup_data)

    async def close(self) -> None:
        return None

    def _path(self, session_id: str) -> AsyncPath:
        return self.directory / f"{session_id}.json"


@dataclass(frozen=True, slots=True)
class CookieSessionStorage:
    service: SecretEnvelopeService
    payload_max_bytes: int
    cookie_payload_max_bytes: int

    def load_cookie(
        self,
        cookie_value: str,
        *,
        now: float,
    ) -> tuple[str, SessionRecord] | None:
        loaded = self.decode_cookie(cookie_value)
        if loaded is None:
            return None
        _session_id, record = loaded
        if record.expired(now):
            return None
        return loaded

    def decode_cookie(self, value: str) -> tuple[str, SessionRecord] | None:
        if not value.startswith(ENVELOPE_PREFIX):
            return None
        try:
            payload, _version = self.service.decrypt_required(value)
            decoded = json.loads(payload)
        except (
            SecretDataError,
            SecretMaterialMissingError,
            json.JSONDecodeError,
        ):
            return None
        if not isinstance(decoded, Mapping):
            return None
        session_id = decoded.get("id")
        raw_record = decoded.get("record")
        if not isinstance(session_id, str) or not isinstance(raw_record, Mapping):
            return None
        try:
            record = _record_from_mapping(raw_record)
        except SessionStorageError:
            return None
        return session_id, record

    def dump_cookie(self, session_id: str, record: SessionRecord) -> str:
        payload = json.dumps(
            {"id": session_id, "record": _record_payload(record)},
            sort_keys=True,
            separators=(",", ":"),
        )
        _check_size(payload, self.payload_max_bytes)
        encrypted = self.service.encrypt_required(payload)
        _check_size(encrypted, self.cookie_payload_max_bytes)
        return encrypted

    async def load(self, session_id: str, *, now: float) -> SessionRecord | None:
        cookie_value = session_id
        loaded = self.load_cookie(cookie_value, now=now)
        return None if loaded is None else loaded[1]

    async def save(self, session_id: str, record: SessionRecord) -> None:
        self.dump_cookie(session_id, record)

    async def delete(self, session_id: str) -> None:
        return None

    async def validate(self) -> None:
        self.service.current_version_required()

    async def cleanup(self, *, now: float) -> None:
        return None

    async def close(self) -> None:
        return None


@dataclass(frozen=True, slots=True)
class CacheSessionStorage:
    url: str
    key_prefix: str
    payload_max_bytes: int
    cleanup_registry: SessionCleanupRegistry | None = None
    _client: Any = field(default=None, init=False, repr=False, compare=False)
    _memory: MemorySessionStorage | None = field(default=None, init=False, repr=False)

    async def load(self, session_id: str, *, now: float) -> SessionRecord | None:
        if self.url.startswith("memory://"):
            return await self._memory_storage().load(session_id, now=now)
        raw_payload = await self._redis_client().get(self._key(session_id))
        if raw_payload is None:
            return None
        if isinstance(raw_payload, bytes):
            raw_payload = raw_payload.decode("utf-8")
        if not isinstance(raw_payload, str):
            return None
        try:
            record = _record_from_json(raw_payload)
        except SessionStorageError:
            return None
        if record.expired(now):
            await self.delete(session_id)
            return None
        return record

    async def save(self, session_id: str, record: SessionRecord) -> None:
        if self.url.startswith("memory://"):
            await self._memory_storage().save(session_id, record)
            return
        payload = _record_json(record, max_bytes=self.payload_max_bytes)
        await self._redis_client().set(
            self._key(session_id),
            payload,
            ex=max(1, int(record.expires_at - record.updated_at)),
        )

    async def delete(self, session_id: str) -> None:
        if self.url.startswith("memory://"):
            await self._memory_storage().delete(session_id)
            return
        client = self._redis_client()
        raw_payload = await client.get(self._key(session_id))
        cleanup_data = _record_data_from_json(raw_payload)
        await client.delete(self._key(session_id))
        if cleanup_data is not None:
            await _cleanup_session_data(self.cleanup_registry, cleanup_data)

    async def validate(self) -> None:
        if self.url.startswith("memory://"):
            return None
        try:
            await self._redis_client().ping()
        except Exception as exc:  # pragma: no cover - external service
            raise SessionStorageError("Redis session cache is unavailable.") from exc

    async def cleanup(self, *, now: float) -> None:
        if self.url.startswith("memory://"):
            await self._memory_storage().cleanup(now=now)

    async def close(self) -> None:
        if self._memory is not None:
            await self._memory.close()
        client = self._client
        if client is None:
            return
        close = getattr(client, "aclose", None) or getattr(client, "close", None)
        if close is not None:
            result = close()
            if asyncio.iscoroutine(result):
                await result
        object.__setattr__(self, "_client", None)

    def _memory_storage(self) -> MemorySessionStorage:
        if self._memory is None:
            object.__setattr__(
                self,
                "_memory",
                MemorySessionStorage(
                    payload_max_bytes=self.payload_max_bytes,
                    cleanup_registry=self.cleanup_registry,
                ),
            )
        assert self._memory is not None
        return self._memory

    def _redis_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            redis_module = importlib.import_module("redis.asyncio")
        except ImportError as exc:
            raise SessionsConfigurationError(
                "Redis session cache requires the optional redis package."
            ) from exc
        client = redis_module.Redis.from_url(self.url, decode_responses=True)
        object.__setattr__(self, "_client", client)
        return client

    def _key(self, session_id: str) -> str:
        return f"{self.key_prefix}{session_id}"


@dataclass(frozen=True, slots=True)
class NamedCacheSessionStorage:
    cache: CacheCapability = field(repr=False)
    key_prefix: str
    payload_max_bytes: int
    cleanup_registry: SessionCleanupRegistry | None = None
    owner: ClassVar[str] = "sessions"

    async def load(self, session_id: str, *, now: float) -> SessionRecord | None:
        raw_payload = await self.cache.get(self.owner, self._key(session_id))
        record = _record_from_cache_value(raw_payload)
        if record is None:
            return None
        if record.expired(now):
            await self.delete(session_id)
            return None
        return record

    async def save(self, session_id: str, record: SessionRecord) -> None:
        payload = _record_json(record, max_bytes=self.payload_max_bytes).encode("utf-8")
        ttl = record.expires_at - record.updated_at
        if ttl <= 0:
            raise SessionStorageError(
                "Session record expiry must be later than its update time."
            )
        await self.cache.set(
            self.owner,
            self._key(session_id),
            payload,
            ttl=ttl,
        )

    async def delete(self, session_id: str) -> None:
        key = self._key(session_id)
        raw_payload = await self.cache.get(self.owner, key)
        cleanup_data = _record_data_from_json(raw_payload)
        await self.cache.delete(self.owner, key)
        if cleanup_data is not None:
            await _cleanup_session_data(self.cleanup_registry, cleanup_data)

    async def validate(self) -> None:
        try:
            await self.cache.get(
                self.owner,
                self._key("__validation__"),
            )
        except Exception as exc:
            raise SessionStorageError("Named session cache is unavailable.") from exc

    async def cleanup(self, *, now: float) -> None:
        del now

    async def close(self) -> None:
        return None

    def _key(self, session_id: str) -> str:
        return f"{self.key_prefix}{session_id}"


@dataclass(frozen=True, slots=True)
class DatabaseSessionStorage:
    database: SiteCapabilityProxy[DatabaseCapability]
    connection_name: str
    payload_max_bytes: int
    cleanup_registry: SessionCleanupRegistry | None = None

    async def load(self, session_id: str, *, now: float) -> SessionRecord | None:
        cleanup_data: dict[str, Any] | None = None
        database = await self.database.require()
        async with tortoise_transaction(
            database,
            database.database(self.connection_name).for_write(),
        ) as connection:
            row = await SessionRecordModel.get_or_none(
                id=session_id,
                using_db=connection,
            )
            if row is None:
                return None
            record = _record_from_database_row(row)
            if record is None:
                await row.delete(using_db=connection)
                return None
            if row.expires_at <= now:
                cleanup_data = dict(record.data)
                await row.delete(using_db=connection)
                loaded = None
            else:
                loaded = record
        if cleanup_data is not None:
            await _cleanup_session_data(self.cleanup_registry, cleanup_data)
        return loaded

    async def save(self, session_id: str, record: SessionRecord) -> None:
        data = _session_data_json(record.data, max_bytes=self.payload_max_bytes)
        database = await self.database.require()
        async with tortoise_transaction(
            database,
            database.database(self.connection_name).for_write(),
        ) as connection:
            await SessionRecordModel.update_or_create(
                id=session_id,
                using_db=connection,
                defaults={
                    "data": data,
                    "created_at": record.created_at,
                    "updated_at": record.updated_at,
                    "expires_at": record.expires_at,
                },
            )

    async def delete(self, session_id: str) -> None:
        cleanup_data: dict[str, Any] | None = None
        database = await self.database.require()
        async with tortoise_transaction(
            database,
            database.database(self.connection_name).for_write(),
        ) as connection:
            row = await SessionRecordModel.get_or_none(
                id=session_id,
                using_db=connection,
            )
            if row is not None:
                cleanup_data = _session_data_from_json(row.data)
                await row.delete(using_db=connection)
        if cleanup_data is not None:
            await _cleanup_session_data(self.cleanup_registry, cleanup_data)

    async def validate(self) -> None:
        await self.database.require()

    async def cleanup(self, *, now: float) -> None:
        cleanup_records: list[dict[str, Any]] = []
        database = await self.database.require()
        async with tortoise_transaction(
            database,
            database.database(self.connection_name).for_write(),
        ) as connection:
            rows = tuple(
                await SessionRecordModel.filter(expires_at__lte=now)
                .using_db(connection)
                .all()
            )
            for row in rows:
                cleanup_data = _session_data_from_json(row.data)
                if cleanup_data is not None:
                    cleanup_records.append(cleanup_data)
                await row.delete(using_db=connection)
        for cleanup_data in cleanup_records:
            await _cleanup_session_data(self.cleanup_registry, cleanup_data)

    async def close(self) -> None:
        return None


def storage_from_settings(
    site: Site,
    settings: SessionsSettings,
    *,
    cleanup_registry: SessionCleanupRegistry | None = None,
) -> SessionStorage:
    if settings.resolved_storage_backend is not SessionStorageBackend.CACHE:
        ignored_cache_settings = [
            setting
            for setting, value in (
                ("cache_name", settings.cache_name),
                ("cache_url", settings.cache_url),
            )
            if value is not None
        ]
        if ignored_cache_settings:
            logger.warning(
                "wybra.sessions.%s %s ignored unless storage_backend is 'cache'.",
                " and wybra.sessions.".join(ignored_cache_settings),
                "are" if len(ignored_cache_settings) > 1 else "is",
            )
    if settings.resolved_storage_backend is SessionStorageBackend.MEMORY:
        return MemorySessionStorage(
            payload_max_bytes=settings.resolved_payload_max_bytes,
            cleanup_registry=cleanup_registry,
        )
    if settings.resolved_storage_backend is SessionStorageBackend.COOKIE:
        return CookieSessionStorage(
            service=_cookie_secret_service(site, settings),
            payload_max_bytes=settings.resolved_payload_max_bytes,
            cookie_payload_max_bytes=settings.resolved_cookie_payload_max_bytes,
        )
    if settings.resolved_storage_backend is SessionStorageBackend.FILE:
        return FileSessionStorage(
            directory=AsyncPath(settings.resolved_file_directory),
            payload_max_bytes=settings.resolved_payload_max_bytes,
            cleanup_registry=cleanup_registry,
        )
    if settings.resolved_storage_backend is SessionStorageBackend.CACHE:
        if settings.cache_url is not None:
            deprecation_message = (
                "wybra.sessions.cache_url is deprecated; configure wybra.cache "
                "and select it with wybra.sessions.cache_name instead."
            )
            warnings.warn(
                deprecation_message,
                DeprecationWarning,
                stacklevel=2,
            )
            logger.warning(deprecation_message)
            return CacheSessionStorage(
                url=settings.cache_url,
                key_prefix=settings.cache_key_prefix,
                payload_max_bytes=settings.resolved_payload_max_bytes,
                cleanup_registry=cleanup_registry,
            )
        try:
            caches = site.require_capability(CachesCapability)
        except SiteCapabilityError as exc:
            raise SessionsConfigurationError(
                "Cache-backed sessions require the wybra.cache module."
            ) from exc
        try:
            cache = caches.require(
                settings.resolved_cache_name,
                consumer="request sessions",
            )
        except CacheNotFoundError as exc:
            raise SessionsConfigurationError(
                f"Session cache {settings.resolved_cache_name!r} is unavailable: {exc}"
            ) from exc
        return NamedCacheSessionStorage(
            cache=cache.values,
            key_prefix=settings.cache_key_prefix,
            payload_max_bytes=settings.resolved_payload_max_bytes,
            cleanup_registry=cleanup_registry,
        )
    if settings.resolved_storage_backend is SessionStorageBackend.DATABASE:
        return DatabaseSessionStorage(
            database=site.capability_proxy(DatabaseCapability),
            connection_name=settings.database_connection_name,
            payload_max_bytes=settings.resolved_payload_max_bytes,
            cleanup_registry=cleanup_registry,
        )
    raise SessionsConfigurationError("Unsupported sessions storage backend.")


def _cookie_secret_service(
    site: Site,
    settings: SessionsSettings,
) -> SecretEnvelopeService:
    environ = site.config.environ
    if _has_configured_secret_key(environ):
        return SecretEnvelopeService.from_env(environ)
    if settings.deployment_environment == LOCAL_ENVIRONMENT:
        return SecretEnvelopeService.for_testing(version="local")
    return SecretEnvelopeService.from_env(environ)


def _has_configured_secret_key(environ: object | None) -> bool:
    if environ is None:
        return False
    value = environment_get(environ, ENV_WYBRA_SECRET_KEY)
    return isinstance(value, str) and bool(value.strip())


def _record_data_from_json(value: object) -> dict[str, Any] | None:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(value, str):
        return None
    try:
        return dict(_record_from_json(value).data)
    except SessionStorageError:
        return None


def _record_from_cache_value(value: object) -> SessionRecord | None:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if not isinstance(value, str):
        return None
    try:
        return _record_from_json(value)
    except SessionStorageError:
        return None


def _session_data_json(data: Mapping[str, Any], *, max_bytes: int) -> str:
    _validate_data(data)
    try:
        payload = json.dumps(
            dict(data),
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise SessionStorageError("Session data must be JSON serialisable.") from exc
    _check_size(payload, max_bytes)
    return payload


def _session_data_from_json(value: object) -> dict[str, Any] | None:
    if isinstance(value, bytes):
        value = value.decode("utf-8")
    if not isinstance(value, str):
        return None
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return None
    if not isinstance(decoded, Mapping):
        return None
    try:
        _validate_data(cast(Mapping[str, Any], decoded))
    except SessionStorageError:
        return None
    return dict(cast(Mapping[str, Any], decoded))


def _record_from_database_row(row: SessionRecordModel) -> SessionRecord | None:
    data = _session_data_from_json(row.data)
    if data is None:
        return None
    if not isinstance(row.created_at, (int, float)):
        return None
    if not isinstance(row.updated_at, (int, float)):
        return None
    if not isinstance(row.expires_at, (int, float)):
        return None
    return SessionRecord(
        data=data,
        created_at=float(row.created_at),
        updated_at=float(row.updated_at),
        expires_at=float(row.expires_at),
    )


async def _cleanup_session_data(
    cleanup_registry: SessionCleanupRegistry | None,
    data: Mapping[str, Any],
) -> None:
    if cleanup_registry is not None:
        await cleanup_registry.cleanup_session_data(data)


def _copy_record(record: SessionRecord) -> SessionRecord:
    return SessionRecord(
        data=dict(record.data),
        created_at=record.created_at,
        updated_at=record.updated_at,
        expires_at=record.expires_at,
    )


def _record_payload(record: SessionRecord) -> dict[str, Any]:
    _validate_data(record.data)
    return {
        "data": dict(record.data),
        "created_at": record.created_at,
        "updated_at": record.updated_at,
        "expires_at": record.expires_at,
    }


def _record_json(record: SessionRecord, *, max_bytes: int) -> str:
    try:
        payload = json.dumps(
            _record_payload(record),
            sort_keys=True,
            separators=(",", ":"),
        )
    except (TypeError, ValueError) as exc:
        raise SessionStorageError("Session data must be JSON serialisable.") from exc
    _check_size(payload, max_bytes)
    return payload


def _record_from_json(value: str) -> SessionRecord:
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError as exc:
        raise SessionStorageError("Stored session payload is invalid JSON.") from exc
    if not isinstance(decoded, Mapping):
        raise SessionStorageError("Stored session payload must be a mapping.")
    return _record_from_mapping(decoded)


def _record_from_mapping(value: Mapping[object, object]) -> SessionRecord:
    data = value.get("data")
    created_at = value.get("created_at")
    updated_at = value.get("updated_at")
    expires_at = value.get("expires_at")
    if not isinstance(data, Mapping):
        raise SessionStorageError("Stored session data must be a mapping.")
    if not all(isinstance(key, str) for key in data):
        raise SessionStorageError("Stored session data keys must be strings.")
    if not isinstance(created_at, (int, float)):
        raise SessionStorageError("Stored session created timestamp is invalid.")
    if not isinstance(updated_at, (int, float)):
        raise SessionStorageError("Stored session updated timestamp is invalid.")
    if not isinstance(expires_at, (int, float)):
        raise SessionStorageError("Stored session expiry timestamp is invalid.")
    return SessionRecord(
        data=dict(cast(Mapping[str, Any], data)),
        created_at=float(created_at),
        updated_at=float(updated_at),
        expires_at=float(expires_at),
    )


def _validate_data(value: Mapping[str, Any]) -> None:
    if not all(isinstance(key, str) for key in value):
        raise SessionStorageError("Session data keys must be strings.")


def _check_size(value: str, max_bytes: int) -> None:
    size = len(value.encode("utf-8"))
    if size > max_bytes:
        raise SessionStorageError(
            f"Session payload exceeds configured limit: {size} > {max_bytes} bytes."
        )


__all__ = (
    "CacheSessionStorage",
    "CookieSessionStorage",
    "DatabaseSessionStorage",
    "FileSessionStorage",
    "MemorySessionStorage",
    "NamedCacheSessionStorage",
    "SessionRecord",
    "SessionStorage",
    "storage_from_settings",
)
