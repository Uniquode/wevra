from __future__ import annotations

import asyncio
import importlib
import json
import logging
import uuid
import warnings
from collections.abc import Mapping, MutableMapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar, Protocol, cast
from urllib.parse import urlparse

from fastapi import Request
from tortoise.expressions import Q

from wybra.cache import (
    AtomicCacheCapability,
    CacheFeatureError,
    CacheFeatureUnavailableError,
    CacheNotFoundError,
    CachesCapability,
)
from wybra.db import DatabaseCapability
from wybra.db.capabilities import tortoise_transaction
from wybra.messages.config import MessageStorageBackend
from wybra.messages.exceptions import (
    InvalidAlertError,
    MessageQueueUnavailableError,
    MessagesConfigurationError,
    MessageStorageError,
)
from wybra.messages.models import MessageAlert
from wybra.messages.records import AlertPayload, AlertRecord
from wybra.messages.settings import MessagesSettings
from wybra.sessions.state import RequestSession
from wybra.site import Site, SiteCapabilityError, SiteCapabilityProxy

logger = logging.getLogger(__name__)

SESSION_ALERTS_KEY = "_wybra_messages_alerts"
SESSION_QUEUE_ID_KEY = "_wybra_messages_queue_id"
REQUEST_PEEKED_ALERTS_ATTRIBUTE = "wybra_messages_peeked_alerts"
REQUEST_PEEKED_CACHE_PAYLOADS_ATTRIBUTE = "wybra_messages_peeked_cache_payloads"
REQUEST_ALERTS_RENDERED_ATTRIBUTE = "wybra_messages_alerts_rendered"
REQUEST_ALERTS_ACKNOWLEDGED_ATTRIBUTE = "wybra_messages_alerts_acknowledged"
DEFAULT_ATOMIC_QUEUE_CONFLICT_ATTEMPTS = 8
QUEUE_ENTRY_ID_KEY = "_wybra_queue_entry_id"
EXPIRED_SESSION_QUEUE_TTL_SECONDS = 1.0
WYBRA_WARNING_SKIP_PREFIXES = (str(Path(__file__).resolve().parents[1]),)


class MessagesStorage(Protocol):
    async def enqueue(self, request: Request, alert: AlertRecord) -> None: ...

    async def peek(
        self, request: Request, *, now: float
    ) -> tuple[AlertRecord, ...]: ...

    async def acknowledge(self, request: Request, *, now: float) -> None: ...

    async def pop(self, request: Request, *, now: float) -> tuple[AlertRecord, ...]: ...

    async def cleanup_session_data(self, session_data: Mapping[str, Any]) -> None: ...

    async def cleanup(self, *, now: float) -> None: ...

    async def validate(self) -> None: ...


class CacheQueueBackend(Protocol):
    async def append(
        self,
        queue_key: str,
        payload: AlertPayload,
        *,
        queue_depth: int,
        ttl_seconds: float,
    ) -> None: ...

    async def peek(self, queue_key: str) -> tuple[AlertPayload, ...]: ...

    async def acknowledge(
        self,
        queue_key: str,
        *,
        observed: Sequence[Mapping[str, object]] | None = None,
        ttl_seconds: float | None = None,
    ) -> None: ...

    async def pop(self, queue_key: str) -> tuple[AlertPayload, ...]: ...

    async def validate(self) -> None: ...

    async def close(self) -> None: ...


@dataclass(frozen=True, slots=True)
class SessionMessagesStorage:
    settings: MessagesSettings

    async def enqueue(self, request: Request, alert: AlertRecord) -> None:
        session = request_session(request)
        queue = _valid_payloads(
            session.get(SESSION_ALERTS_KEY, ()),
            max_message_length=self.settings.resolved_message_max_length,
            now=None,
        )
        queue.append(_stored_payload(alert, self._expires_at(alert.created_at)))
        session[SESSION_ALERTS_KEY] = queue[-self.settings.resolved_queue_depth :]

    async def peek(self, request: Request, *, now: float) -> tuple[AlertRecord, ...]:
        session = request_session(request)
        raw_queue = session.get(SESSION_ALERTS_KEY, ())
        payloads = _valid_payloads(
            raw_queue,
            max_message_length=self.settings.resolved_message_max_length,
            now=now,
        )
        return _records_from_payloads(
            payloads,
            max_message_length=self.settings.resolved_message_max_length,
        )

    async def acknowledge(self, request: Request, *, now: float) -> None:
        request_session(request).pop(SESSION_ALERTS_KEY, None)

    async def pop(self, request: Request, *, now: float) -> tuple[AlertRecord, ...]:
        alerts = await self.peek(request, now=now)
        await self.acknowledge(request, now=now)
        return alerts

    async def cleanup_session_data(self, session_data: Mapping[str, Any]) -> None:
        return None

    async def cleanup(self, *, now: float) -> None:
        return None

    async def validate(self) -> None:
        return None

    def _expires_at(self, created_at: float) -> float:
        return created_at + self.settings.resolved_message_ttl_seconds


@dataclass(frozen=True, slots=True)
class CacheMessagesStorage:
    settings: MessagesSettings
    backend: CacheQueueBackend

    async def enqueue(self, request: Request, alert: AlertRecord) -> None:
        queue_key = server_side_queue_key(
            request,
            prefix=self.settings.cache_key_prefix,
        )
        ttl_seconds = _queue_ttl_seconds(
            request,
            default_ttl_seconds=self.settings.resolved_message_ttl_seconds,
            now=alert.created_at,
        )
        payload = _stored_payload(
            alert,
            alert.created_at + ttl_seconds,
        )
        payload[QUEUE_ENTRY_ID_KEY] = uuid.uuid4().hex
        await self.backend.append(
            queue_key,
            payload,
            queue_depth=self.settings.resolved_queue_depth,
            ttl_seconds=ttl_seconds,
        )

    async def peek(self, request: Request, *, now: float) -> tuple[AlertRecord, ...]:
        queue_key = optional_server_side_queue_key(
            request,
            prefix=self.settings.cache_key_prefix,
        )
        if queue_key is None:
            return ()
        raw_payloads = await self.backend.peek(queue_key)
        setattr(
            request.state,
            REQUEST_PEEKED_CACHE_PAYLOADS_ATTRIBUTE,
            raw_payloads,
        )
        payloads = _valid_payloads(
            raw_payloads,
            max_message_length=self.settings.resolved_message_max_length,
            now=now,
        )
        return _records_from_payloads(
            payloads,
            max_message_length=self.settings.resolved_message_max_length,
        )

    async def acknowledge(self, request: Request, *, now: float) -> None:
        queue_key = optional_server_side_queue_key(
            request,
            prefix=self.settings.cache_key_prefix,
        )
        if queue_key is not None:
            observed = getattr(
                request.state,
                REQUEST_PEEKED_CACHE_PAYLOADS_ATTRIBUTE,
                None,
            )
            if observed is None:
                observed = await self.backend.peek(queue_key)
            await self.backend.acknowledge(
                queue_key,
                observed=observed,
                ttl_seconds=_queue_ttl_seconds(
                    request,
                    default_ttl_seconds=self.settings.resolved_message_ttl_seconds,
                    now=now,
                ),
            )
            if hasattr(request.state, REQUEST_PEEKED_CACHE_PAYLOADS_ATTRIBUTE):
                delattr(request.state, REQUEST_PEEKED_CACHE_PAYLOADS_ATTRIBUTE)

    async def pop(self, request: Request, *, now: float) -> tuple[AlertRecord, ...]:
        queue_key = optional_server_side_queue_key(
            request,
            prefix=self.settings.cache_key_prefix,
        )
        if queue_key is None:
            return ()
        raw_payloads = await self.backend.pop(queue_key)
        if hasattr(request.state, REQUEST_PEEKED_CACHE_PAYLOADS_ATTRIBUTE):
            delattr(request.state, REQUEST_PEEKED_CACHE_PAYLOADS_ATTRIBUTE)
        return _records_from_payloads(
            _valid_payloads(
                raw_payloads,
                max_message_length=self.settings.resolved_message_max_length,
                now=now,
            ),
            max_message_length=self.settings.resolved_message_max_length,
        )

    async def cleanup_session_data(self, session_data: Mapping[str, Any]) -> None:
        queue_key = server_side_queue_key_from_session_data(
            session_data,
            prefix=self.settings.cache_key_prefix,
        )
        if queue_key is not None:
            await self.backend.acknowledge(queue_key)

    async def cleanup(self, *, now: float) -> None:
        return None

    async def validate(self) -> None:
        await self.backend.validate()

    async def close(self) -> None:
        await self.backend.close()


@dataclass(slots=True)
class InMemoryCacheQueueBackend:
    _queues: dict[str, list[AlertPayload]] = field(default_factory=dict)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    async def append(
        self,
        queue_key: str,
        payload: AlertPayload,
        *,
        queue_depth: int,
        ttl_seconds: float,
    ) -> None:
        async with self._lock:
            queue = list(self._queues.get(queue_key, ()))
            queue.append(payload)
            self._queues[queue_key] = queue[-queue_depth:]

    async def pop(self, queue_key: str) -> tuple[AlertPayload, ...]:
        async with self._lock:
            return tuple(self._queues.pop(queue_key, ()))

    async def peek(self, queue_key: str) -> tuple[AlertPayload, ...]:
        async with self._lock:
            return tuple(self._queues.get(queue_key, ()))

    async def acknowledge(
        self,
        queue_key: str,
        *,
        observed: Sequence[Mapping[str, object]] | None = None,
        ttl_seconds: float | None = None,
    ) -> None:
        del ttl_seconds
        async with self._lock:
            if observed is None:
                self._queues.pop(queue_key, None)
                return
            current = self._queues.get(queue_key)
            if current is None:
                return
            remaining = _remaining_after_acknowledgement(current, observed)
            if remaining:
                self._queues[queue_key] = remaining
            else:
                self._queues.pop(queue_key, None)

    async def validate(self) -> None:
        return None

    async def close(self) -> None:
        async with self._lock:
            self._queues.clear()


@dataclass(frozen=True, slots=True)
class NamedCacheQueueBackend:
    atomic: AtomicCacheCapability = field(repr=False)
    max_conflict_attempts: int = DEFAULT_ATOMIC_QUEUE_CONFLICT_ATTEMPTS
    owner: ClassVar[str] = "messages"

    def __post_init__(self) -> None:
        if (
            isinstance(self.max_conflict_attempts, bool)
            or not isinstance(self.max_conflict_attempts, int)
            or self.max_conflict_attempts <= 0
        ):
            raise ValueError("Atomic queue conflict attempts must be positive.")

    async def append(
        self,
        queue_key: str,
        payload: AlertPayload,
        *,
        queue_depth: int,
        ttl_seconds: float,
    ) -> None:
        try:
            for _attempt in range(self.max_conflict_attempts):
                current = await self.atomic.get(self.owner, queue_key)
                queue = [] if current is None else _payloads_from_json(current.value)
                queue.append(payload)
                encoded = _queue_payload(queue[-queue_depth:])
                if current is None:
                    created = await self.atomic.create(
                        self.owner,
                        queue_key,
                        encoded,
                        ttl=ttl_seconds,
                    )
                    if created is not None:
                        return
                    continue
                swapped = await self.atomic.compare_and_swap(
                    self.owner,
                    queue_key,
                    current.revision,
                    encoded,
                    ttl=ttl_seconds,
                )
                if swapped is not None:
                    return
        except (CacheFeatureError, TypeError, ValueError) as exc:
            raise MessageStorageError(
                "Named messages cache rejected a queue update."
            ) from exc
        raise MessageStorageError(
            "Atomic messages queue update exceeded its contention limit."
        )

    async def peek(self, queue_key: str) -> tuple[AlertPayload, ...]:
        current = await self.atomic.get(self.owner, queue_key)
        if current is None:
            return ()
        return tuple(_payloads_from_json(current.value))

    async def acknowledge(
        self,
        queue_key: str,
        *,
        observed: Sequence[Mapping[str, object]] | None = None,
        ttl_seconds: float | None = None,
    ) -> None:
        if observed is None:
            await self._delete(queue_key, return_payloads=False)
            return
        if ttl_seconds is None:
            raise MessageStorageError(
                "Snapshot acknowledgement requires the messages queue TTL."
            )
        await self._acknowledge_observed(
            queue_key,
            observed=observed,
            ttl_seconds=ttl_seconds,
        )

    async def pop(self, queue_key: str) -> tuple[AlertPayload, ...]:
        return await self._delete(queue_key, return_payloads=True)

    async def validate(self) -> None:
        try:
            await self.atomic.get(self.owner, "__validation__")
        except Exception as exc:
            raise MessageStorageError(
                "Named messages cache atomic feature is unavailable."
            ) from exc

    async def close(self) -> None:
        return None

    async def _delete(
        self,
        queue_key: str,
        *,
        return_payloads: bool,
    ) -> tuple[AlertPayload, ...]:
        for _attempt in range(self.max_conflict_attempts):
            current = await self.atomic.get(self.owner, queue_key)
            if current is None:
                return ()
            payloads = tuple(_payloads_from_json(current.value))
            if await self.atomic.compare_and_delete(
                self.owner,
                queue_key,
                current.revision,
            ):
                return payloads if return_payloads else ()
        raise MessageStorageError(
            "Atomic messages queue deletion exceeded its contention limit."
        )

    async def _acknowledge_observed(
        self,
        queue_key: str,
        *,
        observed: Sequence[Mapping[str, object]],
        ttl_seconds: float,
    ) -> None:
        for _attempt in range(self.max_conflict_attempts):
            current = await self.atomic.get(self.owner, queue_key)
            if current is None:
                return
            payloads = _payloads_from_json(current.value)
            remaining = _remaining_after_acknowledgement(payloads, observed)
            if remaining == payloads:
                return
            if not remaining:
                if await self.atomic.compare_and_delete(
                    self.owner,
                    queue_key,
                    current.revision,
                ):
                    return
                continue
            swapped = await self.atomic.compare_and_swap(
                self.owner,
                queue_key,
                current.revision,
                _queue_payload(remaining),
                ttl=ttl_seconds,
            )
            if swapped is not None:
                return
        raise MessageStorageError(
            "Atomic messages queue acknowledgement exceeded its contention limit."
        )


_REDIS_APPEND_QUEUE_SCRIPT = """
local raw_queue = redis.call('GET', KEYS[1])
local queue = {}
if raw_queue then
  local ok, decoded = pcall(cjson.decode, raw_queue)
  if ok and type(decoded) == 'table' then
    queue = decoded
  end
end

local ok, payload = pcall(cjson.decode, ARGV[1])
if not ok then
  return redis.error_reply('invalid alert payload')
end

table.insert(queue, payload)
local queue_depth = tonumber(ARGV[2])
while #queue > queue_depth do
  table.remove(queue, 1)
end

redis.call('SET', KEYS[1], cjson.encode(queue), 'EX', tonumber(ARGV[3]))
return 1
"""

_REDIS_POP_QUEUE_SCRIPT = """
local raw_queue = redis.call('GET', KEYS[1])
if raw_queue then
  redis.call('DEL', KEYS[1])
end
return raw_queue
"""


@dataclass(slots=True)
class RedisCacheQueueBackend:
    url: str
    _client: Any = field(default=None, init=False, repr=False)

    async def append(
        self,
        queue_key: str,
        payload: AlertPayload,
        *,
        queue_depth: int,
        ttl_seconds: float,
    ) -> None:
        await self._redis_client().eval(
            _REDIS_APPEND_QUEUE_SCRIPT,
            1,
            queue_key,
            json.dumps(payload),
            str(queue_depth),
            str(max(1, int(ttl_seconds))),
        )

    async def pop(self, queue_key: str) -> tuple[AlertPayload, ...]:
        raw_queue = await self._redis_client().eval(
            _REDIS_POP_QUEUE_SCRIPT,
            1,
            queue_key,
        )
        return tuple(_payloads_from_json(raw_queue))

    async def peek(self, queue_key: str) -> tuple[AlertPayload, ...]:
        return tuple(_payloads_from_json(await self._redis_client().get(queue_key)))

    async def acknowledge(
        self,
        queue_key: str,
        *,
        observed: Sequence[Mapping[str, object]] | None = None,
        ttl_seconds: float | None = None,
    ) -> None:
        del observed, ttl_seconds
        await self._redis_client().delete(queue_key)

    async def validate(self) -> None:
        client = self._redis_client()
        try:
            await client.ping()
        except Exception as exc:  # pragma: no cover - depends on external service
            raise MessageStorageError("Redis messages cache is unavailable.") from exc

    async def close(self) -> None:
        client = self._client
        if client is None:
            return
        close = getattr(client, "aclose", None) or getattr(client, "close", None)
        if close is not None:
            result = close()
            if asyncio.iscoroutine(result):
                await result
        self._client = None

    def _redis_client(self) -> Any:
        if self._client is not None:
            return self._client
        try:
            redis_module = importlib.import_module("redis.asyncio")
        except ImportError as exc:
            raise MessagesConfigurationError(
                "Redis messages cache requires the optional redis package."
            ) from exc
        self._client = redis_module.Redis.from_url(self.url, decode_responses=True)
        return self._client


@dataclass(frozen=True, slots=True)
class DatabaseMessagesStorage:
    settings: MessagesSettings
    database: SiteCapabilityProxy[DatabaseCapability]

    async def enqueue(self, request: Request, alert: AlertRecord) -> None:
        queue_key = server_side_queue_key(request, prefix="")
        database = await self.database.require()
        async with tortoise_transaction(
            database,
            database.database(self.settings.database_connection_name).for_write(),
        ) as connection:
            await MessageAlert.create(
                queue_key=queue_key,
                severity=alert.severity,
                message=alert.message,
                created_at=alert.created_at,
                expires_at=alert.created_at
                + self.settings.resolved_message_ttl_seconds,
                using_db=connection,
            )
            await self._trim_queue(connection, queue_key)

    async def peek(self, request: Request, *, now: float) -> tuple[AlertRecord, ...]:
        queue_key = optional_server_side_queue_key(request, prefix="")
        if queue_key is None:
            return ()
        database = await self.database.require()
        async with tortoise_transaction(
            database,
            database.database(self.settings.database_connection_name).for_write(),
        ) as connection:
            rows = tuple(
                await MessageAlert.filter(queue_key=queue_key)
                .filter(Q(expires_at__isnull=True) | Q(expires_at__gt=now))
                .using_db(connection)
                .order_by("id")
                .all()
            )
            if not rows:
                return ()
            return tuple(
                AlertRecord.create(
                    row.severity,
                    row.message,
                    created_at=row.created_at,
                    max_message_length=self.settings.resolved_message_max_length,
                )
                for row in rows
            )

    async def acknowledge(self, request: Request, *, now: float) -> None:
        queue_key = optional_server_side_queue_key(request, prefix="")
        if queue_key is None:
            return
        database = await self.database.require()
        async with tortoise_transaction(
            database,
            database.database(self.settings.database_connection_name).for_write(),
        ) as connection:
            await self._delete_queue(connection, queue_key)

    async def pop(self, request: Request, *, now: float) -> tuple[AlertRecord, ...]:
        alerts = await self.peek(request, now=now)
        await self.acknowledge(request, now=now)
        return alerts

    async def cleanup_session_data(self, session_data: Mapping[str, Any]) -> None:
        queue_key = server_side_queue_key_from_session_data(session_data, prefix="")
        if queue_key is None:
            return
        database = await self.database.require()
        async with tortoise_transaction(
            database,
            database.database(self.settings.database_connection_name).for_write(),
        ) as connection:
            await self._delete_queue(connection, queue_key)

    async def cleanup(self, *, now: float) -> None:
        database = await self.database.require()
        async with tortoise_transaction(
            database,
            database.database(self.settings.database_connection_name).for_write(),
        ) as connection:
            await self._delete_expired(connection, now)

    async def validate(self) -> None:
        await self.database.require()

    async def _delete_expired(self, connection: Any, now: float) -> None:
        await (
            MessageAlert.filter(Q(expires_at__isnull=False), expires_at__lte=now)
            .using_db(connection)
            .delete()
        )

    async def _delete_queue(self, connection: Any, queue_key: str) -> None:
        await MessageAlert.filter(queue_key=queue_key).using_db(connection).delete()

    async def _trim_queue(self, connection: Any, queue_key: str) -> None:
        keep_ids = tuple(
            await MessageAlert.filter(queue_key=queue_key)
            .using_db(connection)
            .order_by("-id")
            .limit(self.settings.resolved_queue_depth)
            .values_list("id", flat=True)
        )
        if keep_ids:
            await (
                MessageAlert.filter(queue_key=queue_key)
                .exclude(id__in=keep_ids)
                .using_db(connection)
                .delete()
            )


def storage_from_settings(site: Site, settings: MessagesSettings) -> MessagesStorage:
    if settings.resolved_storage_backend is not MessageStorageBackend.CACHE:
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
                "wybra.messages.%s %s ignored unless storage_backend is 'cache'.",
                " and wybra.messages.".join(ignored_cache_settings),
                "are" if len(ignored_cache_settings) > 1 else "is",
            )
    if settings.resolved_storage_backend is MessageStorageBackend.SESSION:
        return SessionMessagesStorage(settings)
    if settings.resolved_storage_backend is MessageStorageBackend.CACHE:
        if settings.cache_url is not None:
            deprecation_message = (
                "wybra.messages.cache_url is deprecated; configure wybra.cache "
                "and select it with wybra.messages.cache_name instead."
            )
            warnings.warn(
                deprecation_message,
                DeprecationWarning,
                skip_file_prefixes=WYBRA_WARNING_SKIP_PREFIXES,
            )
            logger.warning(deprecation_message)
            return CacheMessagesStorage(
                settings=settings,
                backend=cache_backend_from_url(settings.cache_url),
            )
        try:
            caches = site.require_capability(CachesCapability)
        except SiteCapabilityError as exc:
            raise MessagesConfigurationError(
                "Cache-backed messages require the wybra.cache module."
            ) from exc
        try:
            cache = caches.require(
                settings.resolved_cache_name,
                consumer="queued request messages",
            )
        except CacheNotFoundError as exc:
            raise MessagesConfigurationError(
                f"Messages cache {settings.resolved_cache_name!r} is unavailable: {exc}"
            ) from exc
        try:
            atomic = cache.require(
                AtomicCacheCapability,
                consumer="queued request messages",
            )
        except CacheFeatureUnavailableError as exc:
            raise MessagesConfigurationError(str(exc)) from exc
        return CacheMessagesStorage(
            settings=settings,
            backend=NamedCacheQueueBackend(atomic),
        )
    if settings.resolved_storage_backend is MessageStorageBackend.DATABASE:
        return DatabaseMessagesStorage(
            settings=settings,
            database=site.capability_proxy(DatabaseCapability),
        )
    raise MessagesConfigurationError("Unsupported messages storage backend.")


def cache_backend_from_url(url: str) -> CacheQueueBackend:
    parsed = urlparse(url)
    if parsed.scheme == "memory":
        return InMemoryCacheQueueBackend()
    if parsed.scheme in {"redis", "rediss"}:
        return RedisCacheQueueBackend(url)
    raise MessagesConfigurationError(
        "wybra.messages.cache_url must use memory://, redis://, or rediss://."
    )


def request_session(request: Request) -> MutableMapping[str, Any]:
    session = request.scope.get("session")
    if session is None:
        raise MessageQueueUnavailableError(
            "Messages session storage requires Wybra sessions middleware to provide "
            "a compatible request.session mapping."
        )
    if not isinstance(session, MutableMapping):
        raise MessageQueueUnavailableError(
            "Messages storage requires request.session to be a mutable mapping."
        )
    return session


def server_side_queue_key(request: Request, *, prefix: str) -> str:
    session = request_session(request)
    value = session.get(SESSION_QUEUE_ID_KEY)
    if isinstance(value, str) and value.strip():
        queue_id = value.strip()
    else:
        queue_id = uuid.uuid4().hex
        session[SESSION_QUEUE_ID_KEY] = queue_id
    return f"{prefix}{queue_id}"


def optional_server_side_queue_key(request: Request, *, prefix: str) -> str | None:
    session = request_session(request)
    return server_side_queue_key_from_session_data(session, prefix=prefix)


def server_side_queue_key_from_session_data(
    session_data: Mapping[str, Any],
    *,
    prefix: str,
) -> str | None:
    value = session_data.get(SESSION_QUEUE_ID_KEY)
    if isinstance(value, str) and value.strip():
        return f"{prefix}{value.strip()}"
    return None


def _queue_ttl_seconds(
    request: Request,
    *,
    default_ttl_seconds: float,
    now: float,
) -> float:
    session = request_session(request)
    if not isinstance(session, RequestSession):
        return float(default_ttl_seconds)
    expires_at = (
        session.prospective_expires_at
        if session.modified and session.prospective_expires_at is not None
        else session.expires_at
    )
    if isinstance(expires_at, bool) or not isinstance(expires_at, int | float):
        return float(default_ttl_seconds)
    remaining = float(expires_at) - now
    if remaining <= 0:
        return EXPIRED_SESSION_QUEUE_TTL_SECONDS
    return min(float(default_ttl_seconds), remaining)


def _stored_payload(alert: AlertRecord, expires_at: float) -> AlertPayload:
    payload = alert.to_payload()
    payload["expires_at"] = expires_at
    return payload


def _valid_payloads(
    raw_queue: object,
    *,
    max_message_length: int,
    now: float | None,
) -> list[AlertPayload]:
    if not isinstance(raw_queue, Sequence) or isinstance(raw_queue, (str, bytes)):
        return []
    payloads: list[AlertPayload] = []
    for raw_payload in raw_queue:
        if not isinstance(raw_payload, Mapping):
            continue
        payload = dict(raw_payload)
        expires_at = payload.get("expires_at")
        if (
            isinstance(expires_at, (int, float))
            and now is not None
            and expires_at <= now
        ):
            continue
        try:
            AlertRecord.from_payload(payload, max_message_length=max_message_length)
        except InvalidAlertError:
            continue
        payloads.append(payload)
    return payloads


def _records_from_payloads(
    payloads: Sequence[Mapping[str, object]],
    *,
    max_message_length: int,
) -> tuple[AlertRecord, ...]:
    return tuple(
        AlertRecord.from_payload(payload, max_message_length=max_message_length)
        for payload in payloads
    )


def _payloads_from_json(value: object) -> list[AlertPayload]:
    if value is None:
        return []
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return []
    if not isinstance(value, str):
        return []
    try:
        decoded = json.loads(value)
    except json.JSONDecodeError:
        return []
    if not isinstance(decoded, list):
        return []
    return [cast(AlertPayload, item) for item in decoded if isinstance(item, dict)]


def _remaining_after_acknowledgement(
    current: Sequence[Mapping[str, object]],
    observed: Sequence[Mapping[str, object]],
) -> list[AlertPayload]:
    observed_ids = {
        entry_id
        for payload in observed
        if isinstance(entry_id := payload.get(QUEUE_ENTRY_ID_KEY), str)
    }
    remaining = [
        dict(payload)
        for payload in current
        if payload.get(QUEUE_ENTRY_ID_KEY) not in observed_ids
    ]
    observed_without_ids = [
        dict(payload) for payload in observed if QUEUE_ENTRY_ID_KEY not in payload
    ]
    if not observed_without_ids:
        return remaining
    overlap = _acknowledged_prefix_length(remaining, observed_without_ids)
    return remaining[overlap:]


def _acknowledged_prefix_length(
    current: Sequence[Mapping[str, object]],
    observed: Sequence[Mapping[str, object]],
) -> int:
    maximum_overlap = min(len(current), len(observed))
    for overlap in range(maximum_overlap, 0, -1):
        if list(current[:overlap]) == list(observed[-overlap:]):
            return overlap
    return 0


def _queue_payload(payloads: Sequence[Mapping[str, object]]) -> bytes:
    try:
        return json.dumps(
            tuple(dict(payload) for payload in payloads),
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise MessageStorageError(
            "Queued alert payloads must be JSON serialisable."
        ) from exc


__all__ = (
    "DEFAULT_ATOMIC_QUEUE_CONFLICT_ATTEMPTS",
    "REQUEST_ALERTS_RENDERED_ATTRIBUTE",
    "REQUEST_ALERTS_ACKNOWLEDGED_ATTRIBUTE",
    "REQUEST_PEEKED_ALERTS_ATTRIBUTE",
    "SESSION_ALERTS_KEY",
    "SESSION_QUEUE_ID_KEY",
    "CacheMessagesStorage",
    "CacheQueueBackend",
    "DatabaseMessagesStorage",
    "InMemoryCacheQueueBackend",
    "MessagesStorage",
    "NamedCacheQueueBackend",
    "RedisCacheQueueBackend",
    "SessionMessagesStorage",
    "cache_backend_from_url",
    "request_session",
    "server_side_queue_key",
    "storage_from_settings",
)
