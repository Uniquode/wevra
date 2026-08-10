from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from hashlib import sha256
from typing import Any
from uuid import uuid4

from wybra.cache.feature_models import (
    DEFAULT_STREAM_MAX_CONSUMERS,
    DEFAULT_STREAM_RETENTION_COUNT,
    CacheConflictError,
    CacheFeatureError,
    CachePositionExpiredError,
    LeaseToken,
    StreamPosition,
    StreamRecord,
    validate_limit,
    validate_payload,
    validate_positive_integer,
    validate_resource,
)
from wybra.cache.nats_coordination import NatsCoordination
from wybra.cache.nats_runtime import NatsJetStreamRuntime, nats_feature_ttl_header

logger = logging.getLogger(__name__)
_MAX_CHECKPOINT_ATTEMPTS = 8
_CONSUMER_LOCK_ATTEMPTS = 16
_CONSUMER_LOCK_RETRY_SECONDS = 0.01
_CONSUMER_LOCK_TTL_SECONDS = 30.0
_FORGOTTEN_CHECKPOINT_TTL_SECONDS = 60.0
_CHECKPOINT_TOMBSTONE = b"0"


@dataclass(frozen=True, slots=True)
class _StreamAddress:
    digest: str
    name: str
    subject: str
    stream_name: str


@dataclass(frozen=True, slots=True)
class _Checkpoint:
    sequence: int
    position: StreamPosition | None


@dataclass(frozen=True, slots=True)
class NatsStreamCache:
    runtime: NatsJetStreamRuntime
    coordination: NatsCoordination
    retention_count: int = DEFAULT_STREAM_RETENTION_COUNT
    max_consumers: int = DEFAULT_STREAM_MAX_CONSUMERS

    def __post_init__(self) -> None:
        validate_positive_integer(self.retention_count, label="stream retention")
        validate_positive_integer(self.max_consumers, label="maximum stream consumers")

    async def append(
        self,
        owner: str,
        stream: str,
        payload: bytes,
        *,
        lease: LeaseToken | None = None,
    ) -> StreamPosition:
        address = _address(self.runtime, owner, stream)
        payload = validate_payload(payload)
        await self.runtime.ensure_replay_stream(
            address.name,
            address.subject,
            retention_count=self.retention_count,
        )
        if lease is None:
            jetstream = await self.runtime.coordination_jetstream()
            try:
                acknowledgement = await jetstream.publish(
                    address.subject,
                    payload,
                    stream=address.name,
                )
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "NATS JetStream stream append failed (%s).", type(exc).__name__
                )
                raise CacheFeatureError(
                    "NATS JetStream stream append failed."
                ) from None
            return _position(getattr(acknowledgement, "seq", None), label="append")
        return StreamPosition(
            await self.coordination.append_stream(
                address.name,
                address.subject,
                payload,
                lease=lease,
            )
        )

    async def read(
        self,
        owner: str,
        stream: str,
        *,
        after: StreamPosition | None = None,
        limit: int = 100,
    ) -> tuple[StreamRecord, ...]:
        address = _address(self.runtime, owner, stream)
        after_value = _position_value(after)
        limit = validate_limit(limit)
        info = await self._stream_info(address.name)
        if info is None:
            return ()
        state = getattr(info, "state", None)
        first = _stream_sequence(getattr(state, "first_seq", None), label="first")
        last = _stream_sequence(getattr(state, "last_seq", None), label="last")
        if last == 0:
            return ()
        if after_value is not None and first > 0 and after_value < first - 1:
            raise CachePositionExpiredError(
                f"Stream position {after_value} is no longer retained."
            )
        start = first if after_value is None else max(first, after_value + 1)
        if start > last:
            return ()
        jetstream = await self.runtime.coordination_jetstream()
        records: list[StreamRecord] = []
        for sequence in range(start, min(last, start + limit - 1) + 1):
            try:
                message = await jetstream.get_msg(
                    address.name, seq=sequence, direct=True
                )
            except asyncio.CancelledError:
                raise
            except self.runtime.not_found_error() as exc:
                current_info = await self._stream_info(address.name)
                current_state = (
                    None
                    if current_info is None
                    else getattr(current_info, "state", None)
                )
                current_first = _stream_sequence(
                    getattr(current_state, "first_seq", None),
                    label="first",
                )
                if sequence < current_first:
                    raise CachePositionExpiredError(
                        f"Stream position {sequence - 1} is no longer retained."
                    ) from exc
                raise CacheFeatureError(
                    "NATS JetStream stream replay is invalid."
                ) from exc
            except Exception as exc:
                logger.warning(
                    "NATS JetStream stream read failed (%s).", type(exc).__name__
                )
                raise CacheFeatureError("NATS JetStream stream read failed.") from None
            payload = getattr(message, "data", None)
            if not isinstance(payload, bytes):
                raise CacheFeatureError("NATS JetStream stream replay is invalid.")
            records.append(
                StreamRecord(address.stream_name, StreamPosition(sequence), payload)
            )
        return tuple(records)

    async def read_consumer(
        self,
        owner: str,
        stream: str,
        consumer: str,
        *,
        limit: int = 100,
    ) -> tuple[StreamRecord, ...]:
        address = _address(self.runtime, owner, stream)
        consumer = validate_resource(consumer, label="stream consumer")
        checkpoint = await self._checkpoint(address, consumer)
        return await self.read(
            owner,
            stream,
            after=None if checkpoint is None else checkpoint.position,
            limit=limit,
        )

    async def acknowledge(
        self,
        owner: str,
        stream: str,
        consumer: str,
        position: StreamPosition,
    ) -> None:
        address = _address(self.runtime, owner, stream)
        consumer = validate_resource(consumer, label="stream consumer")
        position_value = _position_value(position)
        assert position_value is not None
        info = await self._stream_info(address.name)
        if info is None:
            raise CacheConflictError(
                f"Stream position {position_value} does not exist in {stream!r}."
            )
        state = getattr(info, "state", None)
        last = _stream_sequence(getattr(state, "last_seq", None), label="last")
        if position_value > last:
            raise CacheConflictError(
                f"Stream position {position_value} does not exist in {stream!r}."
            )
        async with self._consumer_lock(owner, address):
            for _attempt in range(_MAX_CHECKPOINT_ATTEMPTS):
                checkpoint = await self._checkpoint(
                    address, consumer, include_tombstone=True
                )
                assert checkpoint is not None
                if checkpoint.position is not None and position < checkpoint.position:
                    raise CacheConflictError(
                        f"Stream consumer {consumer!r} cannot move backwards."
                    )
                if checkpoint.position == position:
                    return
                if (
                    checkpoint.position is None
                    and await self._consumer_count(address) >= self.max_consumers
                ):
                    raise CacheFeatureError(
                        "The NATS JetStream stream has reached its configured "
                        "consumer capacity."
                    )
                if await self._write_checkpoint(
                    address, consumer, position, checkpoint.sequence
                ):
                    return
        raise CacheConflictError("NATS JetStream stream acknowledgement conflicted.")

    async def forget_consumer(
        self,
        owner: str,
        stream: str,
        consumer: str,
    ) -> bool:
        address = _address(self.runtime, owner, stream)
        consumer = validate_resource(consumer, label="stream consumer")
        async with self._consumer_lock(owner, address):
            for _attempt in range(_MAX_CHECKPOINT_ATTEMPTS):
                checkpoint = await self._checkpoint(
                    address, consumer, include_tombstone=True
                )
                assert checkpoint is not None
                if checkpoint.position is None:
                    return False
                if await self._write_checkpoint(
                    address, consumer, None, checkpoint.sequence
                ):
                    return True
        raise CacheConflictError("NATS JetStream stream consumer release conflicted.")

    async def _stream_info(self, name: str) -> Any | None:
        jetstream = await self.runtime.coordination_jetstream()
        try:
            return await jetstream.stream_info(name)
        except self.runtime.not_found_error():
            return None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream stream lookup failed (%s).", type(exc).__name__
            )
            raise CacheFeatureError("NATS JetStream stream operation failed.") from None

    async def _checkpoint(
        self,
        address: _StreamAddress,
        consumer: str,
        *,
        include_tombstone: bool = False,
    ) -> _Checkpoint | None:
        await self.runtime.ensure_coordination_state_stream()
        subject = _checkpoint_subject(self.runtime, address, consumer)
        jetstream = await self.runtime.coordination_jetstream()
        try:
            message = await jetstream.get_last_msg(
                self.runtime.coordination_state_stream_name,
                subject,
                direct=True,
            )
        except self.runtime.not_found_error():
            return _Checkpoint(0, None) if include_tombstone else None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream stream checkpoint read failed (%s).", type(exc).__name__
            )
            raise CacheFeatureError(
                "NATS JetStream stream checkpoint is invalid."
            ) from None
        sequence = getattr(message, "seq", None)
        payload = getattr(message, "data", None)
        if (
            not isinstance(sequence, int)
            or sequence <= 0
            or not isinstance(payload, bytes)
        ):
            raise CacheFeatureError("NATS JetStream stream checkpoint is invalid.")
        if payload == _CHECKPOINT_TOMBSTONE:
            return _Checkpoint(sequence, None) if include_tombstone else None
        try:
            position = StreamPosition(int(payload.decode("ascii")))
        except (UnicodeDecodeError, ValueError) as exc:
            raise CacheFeatureError(
                "NATS JetStream stream checkpoint is invalid."
            ) from exc
        return _Checkpoint(sequence, position)

    async def _write_checkpoint(
        self,
        address: _StreamAddress,
        consumer: str,
        position: StreamPosition | None,
        expected: int,
    ) -> bool:
        subject = _checkpoint_subject(self.runtime, address, consumer)
        payload = (
            _CHECKPOINT_TOMBSTONE if position is None else str(position.value).encode()
        )
        headers = {"Nats-Expected-Last-Subject-Sequence": str(expected)}
        if position is None:
            headers["Nats-TTL"] = nats_feature_ttl_header(
                _FORGOTTEN_CHECKPOINT_TTL_SECONDS
            )
        jetstream = await self.runtime.coordination_jetstream()
        try:
            await jetstream.publish(
                subject,
                payload,
                stream=self.runtime.coordination_state_stream_name,
                headers=headers,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _is_wrong_sequence(exc):
                return False
            logger.warning(
                "NATS JetStream stream checkpoint write failed (%s).",
                type(exc).__name__,
            )
            raise CacheFeatureError(
                "NATS JetStream stream checkpoint is invalid."
            ) from None
        return True

    async def _consumer_count(self, address: _StreamAddress) -> int:
        subject = f"{_checkpoint_subject_prefix(self.runtime, address)}.>"
        count = 0
        messages = await self.runtime.current_subject_messages(
            self.runtime.coordination_state_stream_name,
            subject,
        )
        for message in messages:
            payload = getattr(message, "data", None)
            if not isinstance(payload, bytes):
                raise CacheFeatureError("NATS JetStream stream checkpoint is invalid.")
            if payload != _CHECKPOINT_TOMBSTONE:
                count += 1
        return count

    @asynccontextmanager
    async def _consumer_lock(
        self,
        owner: str,
        address: _StreamAddress,
    ) -> AsyncIterator[None]:
        holder = uuid4().hex
        lease: LeaseToken | None = None
        for _attempt in range(_CONSUMER_LOCK_ATTEMPTS):
            lease = await self.coordination.acquire_internal(
                f"stream-consumers-{address.digest}",
                holder,
                ttl=_CONSUMER_LOCK_TTL_SECONDS,
            )
            if lease is not None:
                break
            await asyncio.sleep(_CONSUMER_LOCK_RETRY_SECONDS)
        if lease is None:
            raise CacheConflictError(
                "NATS JetStream stream consumer operation is busy."
            )
        try:
            yield
        finally:
            await self.coordination.release_internal(lease)


def _address(runtime: NatsJetStreamRuntime, owner: str, stream: str) -> _StreamAddress:
    owner = validate_resource(owner, label="cache owner")
    stream = validate_resource(stream, label="stream")
    digest = sha256(f"{owner}\x00{stream}".encode()).hexdigest()
    return _StreamAddress(
        digest=digest,
        name=f"WYBRA_STREAM_{runtime.namespace.upper()}_{digest.upper()}",
        subject=f"{runtime.subject_prefix}.streams.{digest}",
        stream_name=stream,
    )


def _checkpoint_subject(
    runtime: NatsJetStreamRuntime,
    address: _StreamAddress,
    consumer: str,
) -> str:
    digest = sha256(consumer.encode()).hexdigest()
    return f"{_checkpoint_subject_prefix(runtime, address)}.{digest}"


def _checkpoint_subject_prefix(
    runtime: NatsJetStreamRuntime,
    address: _StreamAddress,
) -> str:
    return f"{runtime.coordination_state_subject_prefix}.stream-cursor.{address.digest}"


def _position(value: object, *, label: str) -> StreamPosition:
    if isinstance(value, bool) or not isinstance(value, int):
        raise CacheFeatureError(
            f"NATS JetStream stream {label} returned invalid state."
        )
    try:
        return StreamPosition(value)
    except ValueError as exc:
        raise CacheFeatureError(
            f"NATS JetStream stream {label} returned invalid state."
        ) from exc


def _position_value(position: StreamPosition | None) -> int | None:
    if position is None:
        return None
    if not isinstance(position, StreamPosition):
        raise TypeError("Stream position must be a StreamPosition.")
    return position.value


def _stream_sequence(value: object, *, label: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise CacheFeatureError(f"NATS JetStream stream {label} sequence is invalid.")
    return value


def _is_wrong_sequence(error: Exception) -> bool:
    return getattr(error, "err_code", None) in {10071, 10164}


__all__ = ("NatsStreamCache",)
