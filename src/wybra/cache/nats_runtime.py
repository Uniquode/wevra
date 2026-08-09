from __future__ import annotations

import asyncio
import importlib
import logging
from dataclasses import dataclass, field
from decimal import ROUND_CEILING, Decimal
from hashlib import sha256
from typing import Any

from wybra.cache.feature_models import (
    MAX_CACHE_VALUE_BYTES,
    CacheFeatureError,
    validate_cache_ttl,
    validate_cache_value,
)
from wybra.core.exceptions import ConfigurationError

logger = logging.getLogger(__name__)
_MINIMUM_SERVER_VERSION = (2, 11, 0)
_NANOSECONDS_PER_SECOND = 1_000_000_000
_MAX_NATS_TTL_NANOSECONDS = 2**63 - 1
_NATS_HEADER_ALLOWANCE_BYTES = 1_024


@dataclass(slots=True)
class NatsJetStreamRuntime:
    servers: tuple[str, ...] = field(repr=False)
    namespace: str
    _client: Any = field(default=None, init=False, repr=False)
    _jetstream: Any = field(default=None, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)
    _start_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
        repr=False,
    )
    _close_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
        repr=False,
    )

    @property
    def stream_name(self) -> str:
        return f"WYBRA_CACHE_{self.namespace.upper()}"

    @property
    def subject_prefix(self) -> str:
        return f"wybra.cache.{self.namespace}"

    async def health_check(self) -> None:
        await self._start()

    async def get(self, owner: str, key: str) -> bytes | None:
        subject = self.cache_subject(owner, key)
        jetstream = await self._ready_jetstream()
        not_found_error = self._not_found_error()
        try:
            message = await jetstream.get_last_msg(
                self.stream_name,
                subject,
                direct=True,
            )
        except not_found_error:
            return None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("read", exc)
            raise CacheFeatureError("NATS JetStream cache operation failed.") from None
        value = getattr(message, "data", None)
        if not isinstance(value, bytes):
            raise CacheFeatureError(
                "NATS JetStream cache returned an invalid cached value."
            )
        return value

    async def set(self, owner: str, key: str, value: bytes, *, ttl: float) -> None:
        value = validate_cache_value(value)
        ttl_header = _ttl_header(ttl)
        subject = self.cache_subject(owner, key)
        jetstream = await self._ready_jetstream()
        try:
            await jetstream.publish(
                subject,
                value,
                stream=self.stream_name,
                headers={"Nats-TTL": ttl_header},
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("write", exc)
            raise CacheFeatureError("NATS JetStream cache operation failed.") from None

    async def delete(self, owner: str, key: str) -> None:
        subject = self.cache_subject(owner, key)
        jetstream = await self._ready_jetstream()
        not_found_error = self._not_found_error()
        try:
            message = await jetstream.get_last_msg(
                self.stream_name,
                subject,
                direct=True,
            )
        except not_found_error:
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("delete lookup", exc)
            raise CacheFeatureError("NATS JetStream cache operation failed.") from None
        sequence = getattr(message, "seq", None)
        if not isinstance(sequence, int) or sequence <= 0:
            raise CacheFeatureError(
                "NATS JetStream cache returned an invalid cached value."
            )
        try:
            await jetstream.delete_msg(self.stream_name, sequence)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if await self._delete_was_settled(jetstream, subject, sequence):
                return
            _log_failure("delete", exc)
            raise CacheFeatureError("NATS JetStream cache operation failed.") from None

    async def close(self) -> None:
        async with self._close_lock:
            async with self._start_lock:
                if self._closed:
                    return
                client = self._client
                if client is None:
                    self._closed = True
                    return
                try:
                    await client.close()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    _log_failure("shutdown", exc)
                    raise CacheFeatureError(
                        "NATS JetStream cache backend shutdown failed."
                    ) from None
                self._client = None
                self._jetstream = None
                self._closed = True

    def cache_subject(self, owner: str, key: str) -> str:
        owner = _cache_owner(owner)
        key = _cache_key(key)
        cache_key = f"{owner}:{key}".encode()
        return f"{self.subject_prefix}.{sha256(cache_key).hexdigest()}"

    async def _ready_jetstream(self) -> Any:
        await self._start()
        jetstream = self._jetstream
        if jetstream is None:
            raise CacheFeatureError("The NATS JetStream cache backend is closed.")
        return jetstream

    async def _start(self) -> None:
        if self._jetstream is not None:
            return
        if self._closed:
            raise CacheFeatureError("The NATS JetStream cache backend is closed.")
        async with self._start_lock:
            if self._jetstream is not None:
                return
            if self._closed:
                raise CacheFeatureError("The NATS JetStream cache backend is closed.")
            if self._client is not None:
                await self._close_startup_client()
                if self._client is not None:
                    raise CacheFeatureError(
                        "NATS JetStream cache backend startup cleanup is incomplete."
                    )
            client = None
            try:
                nats = importlib.import_module("nats")
                client = await nats.connect(servers=list(self.servers))
                self._client = client
                _validate_server_version(client)
                _validate_server_payload_limit(client)
                jetstream = client.jetstream()
                await jetstream.account_info()
                await self._ensure_stream(jetstream)
            except asyncio.CancelledError:
                await self._close_startup_client()
                raise
            except ImportError as exc:
                await self._close_startup_client()
                raise ConfigurationError(
                    "NATS JetStream cache backend requires the optional cache "
                    "dependency. Install wybra[cache]."
                ) from exc
            except ConfigurationError:
                await self._close_startup_client()
                raise
            except Exception as exc:
                await self._close_startup_client()
                _log_failure("startup", exc)
                raise ConfigurationError(
                    "NATS JetStream cache backend startup failed."
                ) from None
            self._jetstream = jetstream

    async def _close_startup_client(self) -> None:
        client = self._client
        if client is None:
            return
        try:
            await client.close()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _log_failure("startup cleanup", exc)
        else:
            if self._client is client:
                self._client = None

    async def _ensure_stream(self, jetstream: Any) -> None:
        not_found_error = self._not_found_error()
        try:
            stream = await jetstream.stream_info(self.stream_name)
        except not_found_error:
            stream = await self._create_or_read_stream(jetstream)
        _validate_stream_configuration(
            stream,
            name=self.stream_name,
            subject=f"{self.subject_prefix}.>",
        )

    async def _create_or_read_stream(self, jetstream: Any) -> Any:
        api = importlib.import_module("nats.js.api")
        config = api.StreamConfig(
            name=self.stream_name,
            subjects=[f"{self.subject_prefix}.>"],
            retention=api.RetentionPolicy.LIMITS,
            max_msgs_per_subject=1,
            max_msg_size=-1,
            discard=api.DiscardPolicy.OLD,
            no_ack=False,
            allow_direct=True,
            allow_msg_ttl=True,
        )
        try:
            return await jetstream.add_stream(config)
        except asyncio.CancelledError:
            raise
        except Exception as creation_error:
            try:
                return await jetstream.stream_info(self.stream_name)
            except asyncio.CancelledError:
                raise
            except Exception:
                raise creation_error from None

    async def _delete_was_settled(
        self,
        jetstream: Any,
        subject: str,
        sequence: int,
    ) -> bool:
        try:
            current = await jetstream.get_last_msg(
                self.stream_name,
                subject,
                direct=True,
            )
        except self._not_found_error():
            return True
        except asyncio.CancelledError:
            raise
        except Exception:
            return False
        current_sequence = getattr(current, "seq", None)
        return isinstance(current_sequence, int) and current_sequence != sequence

    @staticmethod
    def _not_found_error() -> type[Exception]:
        errors = importlib.import_module("nats.js.errors")
        error_type = getattr(errors, "NotFoundError", None)
        if not isinstance(error_type, type) or not issubclass(error_type, Exception):
            raise ConfigurationError(
                "NATS JetStream cache backend requires a supported nats-py client."
            )
        return error_type


def _validate_server_version(client: Any) -> None:
    version = getattr(client, "connected_server_version", None)
    if version is None:
        raise ConfigurationError(
            "NATS JetStream cache backend could not determine the NATS server version."
        )
    try:
        actual = (int(version.major), int(version.minor), int(version.patch))
    except (AttributeError, TypeError, ValueError) as exc:
        raise ConfigurationError(
            "NATS JetStream cache backend could not determine the NATS server version."
        ) from exc
    if actual < _MINIMUM_SERVER_VERSION:
        required = ".".join(map(str, _MINIMUM_SERVER_VERSION))
        received = ".".join(map(str, actual))
        raise ConfigurationError(
            "NATS JetStream cache backend requires NATS server "
            f"{required} or newer (connected to {received})."
        )


def _validate_server_payload_limit(client: Any) -> None:
    max_payload = getattr(client, "max_payload", None)
    required = MAX_CACHE_VALUE_BYTES + _NATS_HEADER_ALLOWANCE_BYTES
    if not isinstance(max_payload, int) or isinstance(max_payload, bool):
        raise ConfigurationError(
            "NATS JetStream cache backend could not determine the NATS server "
            "payload limit."
        )
    if max_payload < required:
        raise ConfigurationError(
            "NATS JetStream cache backend requires a NATS server payload limit "
            f"of at least {required} bytes."
        )


def _validate_stream_configuration(stream: Any, *, name: str, subject: str) -> None:
    config = getattr(stream, "config", None)
    subjects = tuple(getattr(config, "subjects", ()) or ())
    retention = getattr(config, "retention", None)
    retention_value = getattr(retention, "value", retention)
    discard = getattr(config, "discard", None)
    discard_value = getattr(discard, "value", discard)
    sources = tuple(getattr(config, "sources", ()) or ())
    if (
        getattr(config, "name", None) != name
        or subjects != (subject,)
        or retention_value != "limits"
        or getattr(config, "max_msgs_per_subject", None) != 1
        or getattr(config, "max_msgs", None) != -1
        or getattr(config, "max_bytes", None) != -1
        or getattr(config, "max_age", None) != 0
        or getattr(config, "max_msg_size", None) != -1
        or getattr(config, "discard_new_per_subject", None) is not False
        or discard_value != "old"
        or getattr(config, "no_ack", None) is not False
        or getattr(config, "sealed", None) is not False
        or getattr(config, "deny_delete", None) is not False
        or getattr(config, "mirror", None) is not None
        or getattr(config, "republish", None) is not None
        or sources != ()
        or getattr(config, "subject_delete_marker_ttl", None) not in (None, 0, 0.0)
        or getattr(config, "subject_transform", None) is not None
        or getattr(config, "allow_direct", None) is not True
        or getattr(config, "allow_msg_ttl", None) is not True
    ):
        raise ConfigurationError(
            "NATS JetStream cache stream configuration is incompatible with "
            "the required cache baseline."
        )


def _cache_owner(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Cache owner must be a non-blank string.")
    if ":" in value:
        raise ValueError("Cache owner must not contain ':'.")
    return value.strip()


def _cache_key(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("Cache key must be a non-blank string.")
    return value


def _ttl_header(value: float) -> str:
    ttl = validate_cache_ttl(value)
    nanoseconds = int(
        (Decimal(str(ttl)) * _NANOSECONDS_PER_SECOND).to_integral_value(
            rounding=ROUND_CEILING
        )
    )
    if nanoseconds > _MAX_NATS_TTL_NANOSECONDS:
        raise ValueError("Cache TTL exceeds the NATS JetStream TTL limit.")
    return f"{nanoseconds}ns"


def _log_failure(operation: str, error: Exception) -> None:
    logger.warning(
        "NATS JetStream cache %s failed (%s).",
        operation,
        type(error).__name__,
    )


__all__ = ("NatsJetStreamRuntime",)
