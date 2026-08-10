from __future__ import annotations

import asyncio
import importlib
import logging
from dataclasses import dataclass, field
from datetime import datetime
from decimal import ROUND_CEILING, Decimal
from hashlib import sha256
from time import monotonic
from typing import Any
from uuid import uuid4

from wybra.cache.feature_models import (
    MAX_CACHE_FEATURE_LIMIT,
    MAX_CACHE_VALUE_BYTES,
    MINIMUM_CACHE_TTL_SECONDS,
    CacheFeatureError,
    validate_cache_ttl,
    validate_cache_value,
    validate_positive_finite,
)
from wybra.core.exceptions import ConfigurationError

logger = logging.getLogger(__name__)
_MINIMUM_SERVER_VERSION = (2, 11, 0)
_NANOSECONDS_PER_SECOND = 1_000_000_000
_MAX_NATS_TTL_NANOSECONDS = 2**63 - 1
_NATS_HEADER_ALLOWANCE_BYTES = 1_024
_NATS_ADVANCED_FEATURE_PAYLOAD_BYTES = 128 * 1_024
COORDINATION_COMMAND_MAX_AGE_SECONDS = 60.0
_CACHE_TIME_MINIMUM_REFRESH_SECONDS = 60.0
_CACHE_TIME_TARGET_REFRESH_SECONDS = 300.0
_CACHE_TIME_MAXIMUM_REFRESH_SECONDS = 600.0


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
    _time_provider_at_calibration: float | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _time_monotonic_at_calibration: float | None = field(
        default=None,
        init=False,
        repr=False,
    )
    _time_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
        repr=False,
    )

    @property
    def stream_name(self) -> str:
        return f"WYBRA_CACHE_{self.namespace.upper()}"

    @property
    def coordination_state_stream_name(self) -> str:
        return f"WYBRA_CACHE_STATE_{self.namespace.upper()}"

    @property
    def coordination_command_stream_name(self) -> str:
        return f"WYBRA_CACHE_COMMANDS_{self.namespace.upper()}"

    @property
    def subject_prefix(self) -> str:
        return f"wybra.cache.{self.namespace}"

    @property
    def coordination_state_subject_prefix(self) -> str:
        return f"{self.subject_prefix}.state"

    @property
    def value_subject_prefix(self) -> str:
        return f"{self.subject_prefix}.values"

    @property
    def coordination_command_subject(self) -> str:
        return f"{self.subject_prefix}.commands"

    @property
    def coordination_clock_subject(self) -> str:
        return f"{self.coordination_state_subject_prefix}.clock"

    async def ensure_work_queue_stream(
        self,
        name: str,
        subject: str,
        *,
        maximum_messages: int,
    ) -> None:
        """Ensure a bounded JetStream work queue stream exists."""
        if maximum_messages <= 0:
            raise ValueError("NATS work queue capacity must be positive.")
        jetstream = await self._ready_jetstream()
        not_found_error = self._not_found_error()
        try:
            stream = await jetstream.stream_info(name)
        except not_found_error:
            api = importlib.import_module("nats.js.api")
            configuration = api.StreamConfig(
                name=name,
                subjects=[subject],
                storage=api.StorageType.FILE,
                retention=api.RetentionPolicy.WORK_QUEUE,
                max_msgs=maximum_messages,
                max_msgs_per_subject=-1,
                max_bytes=-1,
                max_age=0,
                max_msg_size=-1,
                discard=api.DiscardPolicy.NEW,
                no_ack=False,
                allow_direct=False,
                allow_msg_ttl=False,
            )
            try:
                stream = await jetstream.add_stream(configuration)
            except asyncio.CancelledError:
                raise
            except Exception as creation_error:
                try:
                    stream = await jetstream.stream_info(name)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise creation_error from None
        _validate_work_queue_stream_configuration(
            stream,
            name=name,
            subject=subject,
            maximum_messages=maximum_messages,
        )

    async def health_check(self, *, advanced_features: bool = False) -> None:
        """Confirm baseline and selected-feature payload capacity."""
        await self._start()
        client = await self.nats_client()
        _validate_server_payload_limit(client, advanced_features=advanced_features)

    async def refresh_time(self) -> float:
        """Return locally cached time calibrated from a JetStream server timestamp."""
        if not self._time_refresh_required():
            return self.cache_time()
        async with self._time_lock:
            if not self._time_refresh_required():
                return self.cache_time()
            await self.ensure_coordination_state_stream()
            jetstream = await self._ready_jetstream()
            try:
                await jetstream.publish(
                    self.coordination_clock_subject,
                    b"clock",
                    stream=self.coordination_state_stream_name,
                )
                message = await jetstream.get_last_msg(
                    self.coordination_state_stream_name,
                    self.coordination_clock_subject,
                    direct=True,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                _log_failure("time calibration", exc)
                raise CacheFeatureError(
                    "NATS JetStream cache time calibration failed."
                ) from None
            timestamp = getattr(message, "time", None)
            if not isinstance(timestamp, datetime):
                raise CacheFeatureError(
                    "NATS JetStream cache returned an invalid server time."
                )
            provider_time = timestamp.timestamp()
            calibrated_at = monotonic()
            self._time_provider_at_calibration = provider_time
            self._time_monotonic_at_calibration = calibrated_at
            return provider_time

    def cache_time(self) -> float:
        """Return the latest calibrated JetStream timestamp without remote I/O."""
        provider_time = self._time_provider_at_calibration
        calibrated_at = self._time_monotonic_at_calibration
        if provider_time is None or calibrated_at is None:
            raise CacheFeatureError(
                "NATS JetStream cache time has not been calibrated."
            )
        elapsed = max(0.0, monotonic() - calibrated_at)
        if elapsed >= _CACHE_TIME_MAXIMUM_REFRESH_SECONDS:
            raise CacheFeatureError(
                "NATS JetStream cache time calibration has expired."
            )
        return provider_time + elapsed

    def _time_refresh_required(self) -> bool:
        calibrated_at = self._time_monotonic_at_calibration
        if calibrated_at is None:
            return True
        age = max(0.0, monotonic() - calibrated_at)
        if age < _CACHE_TIME_MINIMUM_REFRESH_SECONDS:
            return False
        return age >= _CACHE_TIME_TARGET_REFRESH_SECONDS

    async def nats_client(self) -> Any:
        await self._start()
        client = self._client
        if client is None:
            raise CacheFeatureError("The NATS JetStream cache backend is closed.")
        return client

    async def coordination_jetstream(self) -> Any:
        await self._start()
        return await self._ready_jetstream()

    async def pull_subscribe(
        self,
        subject: str,
        *,
        durable: str,
        stream: str,
        ack_wait: float,
        max_deliver: int,
        max_ack_pending: int,
    ) -> Any:
        """Create or validate a private durable pull consumer."""
        jetstream = await self.coordination_jetstream()
        api = importlib.import_module("nats.js.api")
        subscription = await jetstream.pull_subscribe(
            subject,
            durable=durable,
            stream=stream,
            config=api.ConsumerConfig(
                durable_name=durable,
                ack_policy=api.AckPolicy.EXPLICIT,
                ack_wait=ack_wait,
                max_deliver=max_deliver,
                max_ack_pending=max_ack_pending,
                max_waiting=MAX_CACHE_FEATURE_LIMIT,
                deliver_policy=api.DeliverPolicy.ALL,
                replay_policy=api.ReplayPolicy.INSTANT,
                headers_only=False,
            ),
        )
        try:
            info = await jetstream.consumer_info(stream, durable)
            _validate_pull_consumer_configuration(
                info,
                durable=durable,
                subject=subject,
                ack_wait=ack_wait,
                max_deliver=max_deliver,
                max_ack_pending=max_ack_pending,
            )
        except BaseException as error:
            try:
                await subscription.unsubscribe()
            except asyncio.CancelledError:
                if isinstance(error, asyncio.CancelledError):
                    return
                raise
            except Exception as cleanup_error:
                error.add_note(
                    "NATS JetStream consumer cleanup failed "
                    f"({type(cleanup_error).__name__})."
                )
                _log_failure("consumer cleanup", cleanup_error)
            raise
        return subscription

    async def current_subject_messages(
        self,
        stream: str,
        subject: str,
        *,
        headers_only: bool = False,
    ) -> tuple[Any, ...]:
        """Read the current messages matching a private subject filter."""
        jetstream = await self.coordination_jetstream()
        api = importlib.import_module("nats.js.api")
        durable = f"WYBRA_QUERY_{uuid4().hex}"
        subscription = await jetstream.pull_subscribe(
            subject,
            durable=durable,
            stream=stream,
            config=api.ConsumerConfig(
                durable_name=durable,
                ack_policy=api.AckPolicy.EXPLICIT,
                ack_wait=1.0,
                max_deliver=1,
                max_ack_pending=MAX_CACHE_FEATURE_LIMIT,
                inactive_threshold=60.0,
                headers_only=headers_only,
            ),
        )
        consumer_name = durable
        try:
            info = await subscription.consumer_info()
            configured_name = getattr(info, "name", None)
            if isinstance(configured_name, str) and configured_name:
                consumer_name = configured_name
            pending = getattr(info, "num_pending", None)
            if not isinstance(pending, int) or pending < 0:
                raise CacheFeatureError("NATS JetStream consumer state is invalid.")
            if pending == 0:
                return ()
            deadline = asyncio.get_running_loop().time() + 5.0
            messages: list[Any] = []
            while len(messages) < pending:
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise CacheFeatureError(
                        "NATS JetStream filtered subject query was incomplete."
                    )
                try:
                    batch = await subscription.fetch(
                        pending - len(messages), timeout=remaining
                    )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if self.is_timeout_error(exc):
                        raise CacheFeatureError(
                            "NATS JetStream filtered subject query was incomplete."
                        ) from None
                    raise
                if not batch:
                    raise CacheFeatureError(
                        "NATS JetStream filtered subject query was incomplete."
                    )
                messages.extend(batch)
            for message in messages:
                await message.ack()
            await (await self.nats_client()).flush()
            return tuple(messages)
        finally:
            try:
                await subscription.unsubscribe()
            except Exception as exc:
                _log_failure("filtered consumer cleanup", exc)
            finally:
                try:
                    await jetstream.delete_consumer(stream, consumer_name)
                except Exception as exc:
                    _log_failure("filtered consumer deletion", exc)

    async def ensure_coordination_state_stream(self) -> None:
        jetstream = await self._ready_jetstream()
        await self._ensure_coordination_stream(
            jetstream,
            name=self.coordination_state_stream_name,
            subjects=[f"{self.coordination_state_subject_prefix}.>"],
            state=True,
        )

    async def ensure_coordination_streams(self) -> None:
        jetstream = await self._ready_jetstream()
        await self.ensure_coordination_state_stream()
        await self._ensure_coordination_stream(
            jetstream,
            name=self.coordination_command_stream_name,
            subjects=[self.coordination_command_subject],
            state=False,
        )

    async def ensure_replay_stream(
        self,
        name: str,
        subject: str,
        *,
        retention_count: int,
    ) -> None:
        if retention_count <= 0:
            raise ValueError("NATS stream retention must be positive.")
        jetstream = await self._ready_jetstream()
        not_found_error = self._not_found_error()
        try:
            stream = await jetstream.stream_info(name)
        except not_found_error:
            api = importlib.import_module("nats.js.api")
            configuration = api.StreamConfig(
                name=name,
                subjects=[subject],
                storage=api.StorageType.FILE,
                retention=api.RetentionPolicy.LIMITS,
                max_msgs=retention_count,
                max_msgs_per_subject=-1,
                max_bytes=-1,
                max_age=0,
                max_msg_size=-1,
                discard=api.DiscardPolicy.OLD,
                no_ack=False,
                allow_direct=True,
                allow_msg_ttl=False,
            )
            try:
                stream = await jetstream.add_stream(configuration)
            except asyncio.CancelledError:
                raise
            except Exception as creation_error:
                try:
                    stream = await jetstream.stream_info(name)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise creation_error from None
        _validate_replay_stream_configuration(
            stream,
            name=name,
            subject=subject,
            retention_count=retention_count,
        )

    @staticmethod
    def not_found_error() -> type[Exception]:
        """Return the provider's missing-resource error type."""
        return NatsJetStreamRuntime._not_found_error()

    @staticmethod
    def is_timeout_error(error: Exception) -> bool:
        """Return whether an error is the NATS client's receive timeout."""
        errors = importlib.import_module("nats.errors")
        timeout_error = getattr(errors, "TimeoutError", None)
        return isinstance(timeout_error, type) and isinstance(error, timeout_error)

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
        ttl_header = nats_ttl_header(ttl)
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
                except ConnectionResetError:
                    self._client = None
                    self._jetstream = None
                    self._closed = True
                    return
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
        return f"{self.value_subject_prefix}.{sha256(cache_key).hexdigest()}"

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
                _validate_server_payload_limit(client, advanced_features=False)
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
            subject=f"{self.value_subject_prefix}.>",
        )

    async def _ensure_coordination_stream(
        self,
        jetstream: Any,
        *,
        name: str,
        subjects: list[str],
        state: bool,
    ) -> None:
        not_found_error = self._not_found_error()
        try:
            stream = await jetstream.stream_info(name)
        except not_found_error:
            api = importlib.import_module("nats.js.api")
            configuration = api.StreamConfig(
                name=name,
                subjects=subjects,
                storage=api.StorageType.FILE,
                retention=(
                    api.RetentionPolicy.LIMITS
                    if state
                    else api.RetentionPolicy.WORK_QUEUE
                ),
                max_msgs_per_subject=1 if state else -1,
                max_msgs=-1 if state else MAX_CACHE_FEATURE_LIMIT,
                max_bytes=-1,
                max_age=0 if state else COORDINATION_COMMAND_MAX_AGE_SECONDS,
                max_msg_size=-1,
                discard=api.DiscardPolicy.OLD if state else api.DiscardPolicy.NEW,
                no_ack=False,
                allow_direct=state,
                allow_msg_ttl=state,
            )
            try:
                stream = await jetstream.add_stream(configuration)
            except asyncio.CancelledError:
                raise
            except Exception as creation_error:
                try:
                    stream = await jetstream.stream_info(name)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    raise creation_error from None
        _validate_coordination_stream_configuration(
            stream,
            name=name,
            subjects=tuple(subjects),
            state=state,
        )

    async def _create_or_read_stream(self, jetstream: Any) -> Any:
        api = importlib.import_module("nats.js.api")
        config = api.StreamConfig(
            name=self.stream_name,
            subjects=[f"{self.value_subject_prefix}.>"],
            storage=api.StorageType.FILE,
            retention=api.RetentionPolicy.LIMITS,
            max_msgs_per_subject=1,
            max_bytes=-1,
            max_age=0,
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


def _validate_server_payload_limit(client: Any, *, advanced_features: bool) -> None:
    max_payload = getattr(client, "max_payload", None)
    baseline_required = MAX_CACHE_VALUE_BYTES + _NATS_HEADER_ALLOWANCE_BYTES
    required = (
        _NATS_ADVANCED_FEATURE_PAYLOAD_BYTES if advanced_features else baseline_required
    )
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
        not _has_safe_private_stream_configuration(config)
        or getattr(config, "name", None) != name
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


def _validate_coordination_stream_configuration(
    stream: Any,
    *,
    name: str,
    subjects: tuple[str, ...],
    state: bool,
) -> None:
    config = getattr(stream, "config", None)
    retention = getattr(config, "retention", None)
    retention_value = getattr(retention, "value", retention)
    if (
        not _has_safe_private_stream_configuration(
            config,
            max_age=0 if state else COORDINATION_COMMAND_MAX_AGE_SECONDS,
        )
        or getattr(config, "name", None) != name
        or tuple(getattr(config, "subjects", ()) or ()) != subjects
        or retention_value != ("limits" if state else "workqueue")
        or getattr(config, "max_msgs_per_subject", None) != (1 if state else -1)
        or getattr(config, "max_msgs", None)
        != (-1 if state else MAX_CACHE_FEATURE_LIMIT)
        or getattr(config, "max_age", None)
        != (0 if state else COORDINATION_COMMAND_MAX_AGE_SECONDS)
        or getattr(config, "max_msg_size", None) != -1
        or getattr(
            getattr(config, "discard", None), "value", getattr(config, "discard", None)
        )
        != ("old" if state else "new")
        or getattr(config, "no_ack", None) is not False
        or getattr(config, "allow_direct", None) is not state
        or getattr(config, "allow_msg_ttl", None) is not state
    ):
        raise ConfigurationError(
            "NATS JetStream cache coordination stream configuration is incompatible "
            "with the required atomic and lease features."
        )


def _validate_replay_stream_configuration(
    stream: Any,
    *,
    name: str,
    subject: str,
    retention_count: int,
) -> None:
    config = getattr(stream, "config", None)
    retention = getattr(config, "retention", None)
    retention_value = getattr(retention, "value", retention)
    discard = getattr(config, "discard", None)
    discard_value = getattr(discard, "value", discard)
    if (
        not _has_safe_private_stream_configuration(config)
        or getattr(config, "name", None) != name
        or tuple(getattr(config, "subjects", ()) or ()) != (subject,)
        or retention_value != "limits"
        or getattr(config, "max_msgs", None) != retention_count
        or getattr(config, "max_msgs_per_subject", None) != -1
        or getattr(config, "max_msg_size", None) != -1
        or discard_value != "old"
        or getattr(config, "no_ack", None) is not False
        or getattr(config, "allow_direct", None) is not True
    ):
        raise ConfigurationError(
            "NATS JetStream replay stream configuration is incompatible with "
            "the required stream feature."
        )


def _validate_work_queue_stream_configuration(
    stream: Any,
    *,
    name: str,
    subject: str,
    maximum_messages: int,
) -> None:
    config = getattr(stream, "config", None)
    retention = getattr(config, "retention", None)
    retention_value = getattr(retention, "value", retention)
    discard = getattr(config, "discard", None)
    discard_value = getattr(discard, "value", discard)
    if (
        not _has_safe_private_stream_configuration(config)
        or getattr(config, "name", None) != name
        or tuple(getattr(config, "subjects", ()) or ()) != (subject,)
        or retention_value != "workqueue"
        or getattr(config, "max_msgs", None) != maximum_messages
        or getattr(config, "max_msgs_per_subject", None) != -1
        or getattr(config, "max_msg_size", None) != -1
        or discard_value != "new"
        or getattr(config, "no_ack", None) is not False
        or getattr(config, "allow_direct", None) is not False
    ):
        raise ConfigurationError(
            "NATS JetStream work queue configuration is incompatible with the "
            "required queue feature."
        )


def _validate_pull_consumer_configuration(
    consumer: Any,
    *,
    durable: str,
    subject: str,
    ack_wait: float,
    max_deliver: int,
    max_ack_pending: int,
) -> None:
    config = getattr(consumer, "config", None)
    ack_policy = getattr(config, "ack_policy", None)
    ack_policy_value = getattr(ack_policy, "value", ack_policy)
    deliver_policy = getattr(config, "deliver_policy", None)
    deliver_policy_value = getattr(deliver_policy, "value", deliver_policy)
    replay_policy = getattr(config, "replay_policy", None)
    replay_policy_value = getattr(replay_policy, "value", replay_policy)
    consumer_ack_wait = getattr(config, "ack_wait", None)
    if (
        getattr(config, "durable_name", None) != durable
        or getattr(config, "filter_subject", None) != subject
        or ack_policy_value != "explicit"
        or consumer_ack_wait != ack_wait
        or getattr(config, "max_deliver", None) != max_deliver
        or getattr(config, "max_ack_pending", None) != max_ack_pending
        or getattr(config, "max_waiting", None) != MAX_CACHE_FEATURE_LIMIT
        or deliver_policy_value != "all"
        or replay_policy_value != "instant"
        or bool(getattr(config, "headers_only", False))
        or getattr(config, "pause_until", None) is not None
        or tuple(getattr(config, "filter_subjects", ()) or ()) != ()
        or tuple(getattr(config, "backoff", ()) or ()) != ()
    ):
        raise ConfigurationError(
            "NATS JetStream cache consumer configuration is incompatible with "
            "the required cache feature."
        )


def _has_safe_private_stream_configuration(
    config: Any,
    *,
    max_age: float = 0,
) -> bool:
    storage = getattr(config, "storage", None)
    storage_value = getattr(storage, "value", storage)
    retention = getattr(config, "retention", None)
    retention_value = getattr(retention, "value", retention)
    discard = getattr(config, "discard", None)
    discard_value = getattr(discard, "value", discard)
    sources = tuple(getattr(config, "sources", ()) or ())
    return (
        storage_value == "file"
        and retention_value in {"limits", "workqueue"}
        and getattr(config, "max_bytes", None) == -1
        and getattr(config, "max_age", None) == max_age
        and getattr(config, "max_msg_size", None) == -1
        and getattr(config, "discard_new_per_subject", None) is False
        and discard_value in {"old", "new"}
        and getattr(config, "no_ack", None) is False
        and getattr(config, "sealed", None) is False
        and getattr(config, "deny_delete", None) is False
        and getattr(config, "mirror", None) is None
        and getattr(config, "republish", None) is None
        and sources == ()
        and getattr(config, "subject_delete_marker_ttl", None) in (None, 0, 0.0)
        and getattr(config, "subject_transform", None) is None
        and getattr(config, "allow_rollup_hdrs", None) is False
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


def nats_ttl_header(value: float) -> str:
    ttl = validate_cache_ttl(value)
    return _ttl_header(ttl)


def nats_feature_ttl_header(value: float) -> str:
    """Encode an advanced-feature TTL using JetStream's minimum expiry."""
    ttl = validate_positive_finite(value, label="cache feature TTL")
    return _ttl_header(max(ttl, MINIMUM_CACHE_TTL_SECONDS))


def _ttl_header(ttl: float) -> str:
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


__all__ = (
    "COORDINATION_COMMAND_MAX_AGE_SECONDS",
    "NatsJetStreamRuntime",
    "nats_feature_ttl_header",
    "nats_ttl_header",
)
