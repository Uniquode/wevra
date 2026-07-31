from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, cast

from wybra.cache.feature_models import (
    DEFAULT_STREAM_MAX_CONSUMERS,
    DEFAULT_STREAM_RETENTION_COUNT,
    CacheConflictError,
    CacheFeatureError,
    CachePositionExpiredError,
    StreamPosition,
    StreamRecord,
    validate_limit,
    validate_payload,
    validate_positive_integer,
    validate_resource,
)
from wybra.cache.redis_runtime import RedisCacheRuntime
from wybra.cache.redis_stream_scripts import (
    STREAM_ACKNOWLEDGE_SCRIPT,
    STREAM_APPEND_SCRIPT,
    STREAM_FORGET_CONSUMER_SCRIPT,
    STREAM_READ_SCRIPT,
)


@dataclass(frozen=True, slots=True)
class _StreamKeys:
    stream: str
    sequence: str
    consumers: str
    stream_name: str


@dataclass(frozen=True, slots=True)
class RedisStreamCache:
    """Redis-backed durable stream records with monotonic public positions."""

    runtime: RedisCacheRuntime
    retention_count: int = DEFAULT_STREAM_RETENTION_COUNT
    max_consumers: int = DEFAULT_STREAM_MAX_CONSUMERS

    def __post_init__(self) -> None:
        validate_positive_integer(self.retention_count, label="stream retention")
        validate_positive_integer(
            self.max_consumers,
            label="maximum stream consumers",
        )

    async def append(
        self,
        owner: str,
        stream: str,
        payload: bytes,
    ) -> StreamPosition:
        keys = self._keys(owner, stream)
        payload = validate_payload(payload)

        async def append_record(client: Any) -> object:
            return await client.eval(
                STREAM_APPEND_SCRIPT,
                2,
                keys.stream,
                keys.sequence,
                payload,
                self.retention_count,
            )

        return _provider_position(
            await self.runtime.feature_call(append_record),
            label="append result",
        )

    async def read(
        self,
        owner: str,
        stream: str,
        *,
        after: StreamPosition | None = None,
        limit: int = 100,
    ) -> tuple[StreamRecord, ...]:
        keys = self._keys(owner, stream)
        after_value = _position_value(after)
        limit = validate_limit(limit)

        async def read_records(client: Any) -> object:
            return await client.eval(
                STREAM_READ_SCRIPT,
                1,
                keys.stream,
                "" if after_value is None else after_value,
                limit,
            )

        return _read_result(
            await self.runtime.feature_call(read_records),
            stream_name=keys.stream_name,
            after=after_value,
        )

    async def read_consumer(
        self,
        owner: str,
        stream: str,
        consumer: str,
        *,
        limit: int = 100,
    ) -> tuple[StreamRecord, ...]:
        keys = self._keys(owner, stream)
        consumer = validate_resource(consumer, label="stream consumer")
        limit = validate_limit(limit)

        async def consumer_position(client: Any) -> object:
            return await client.hget(keys.consumers, consumer)

        position = await self.runtime.feature_call(consumer_position)
        after = (
            None
            if position is None
            else _provider_position(position, label="consumer position")
        )
        return await self.read(owner, stream, after=after, limit=limit)

    async def acknowledge(
        self,
        owner: str,
        stream: str,
        consumer: str,
        position: StreamPosition,
    ) -> None:
        keys = self._keys(owner, stream)
        consumer = validate_resource(consumer, label="stream consumer")
        position_value = _position_value(position)
        assert position_value is not None

        async def acknowledge_position(client: Any) -> object:
            return await client.eval(
                STREAM_ACKNOWLEDGE_SCRIPT,
                2,
                keys.sequence,
                keys.consumers,
                consumer,
                position_value,
                self.max_consumers,
            )

        result = _integer(await self.runtime.feature_call(acknowledge_position))
        if result == 0:
            raise CacheConflictError(
                f"Stream position {position_value} does not exist in {stream!r}."
            )
        if result == -1:
            raise CacheConflictError(
                f"Stream consumer {consumer!r} cannot move backwards."
            )
        if result == -2:
            raise CacheFeatureError(
                "The Redis stream cache has reached its configured consumer capacity."
            )
        if result != 1:
            raise CacheFeatureError("Redis stream returned an invalid acknowledgement.")

    async def forget_consumer(
        self,
        owner: str,
        stream: str,
        consumer: str,
    ) -> bool:
        keys = self._keys(owner, stream)
        consumer = validate_resource(consumer, label="stream consumer")

        async def forget_position(client: Any) -> object:
            return await client.eval(
                STREAM_FORGET_CONSUMER_SCRIPT,
                1,
                keys.consumers,
                consumer,
            )

        result = _integer(await self.runtime.feature_call(forget_position))
        if result not in {0, 1}:
            raise CacheFeatureError(
                "Redis stream returned an invalid consumer release."
            )
        return result == 1

    def _keys(self, owner: str, stream: str) -> _StreamKeys:
        owner = validate_resource(owner, label="cache owner")
        stream = validate_resource(stream, label="stream")
        return _StreamKeys(
            stream=self.runtime.key("stream", owner, stream),
            sequence=self.runtime.key("stream-sequence", owner, stream),
            consumers=self.runtime.key("stream-consumer", owner, stream),
            stream_name=stream,
        )


def _position_value(position: StreamPosition | None) -> int | None:
    if position is None:
        return None
    if not isinstance(position, StreamPosition):
        raise TypeError("Stream position must be a StreamPosition.")
    return validate_positive_integer(position.value, label="stream position")


def _records(value: object, *, stream_name: str) -> tuple[StreamRecord, ...]:
    if not isinstance(value, list | tuple):
        raise CacheFeatureError("Redis stream returned invalid records.")
    records: list[StreamRecord] = []
    for entry in value:
        if not isinstance(entry, list | tuple) or len(entry) != 2:
            raise CacheFeatureError("Redis stream returned invalid records.")
        fields = _record_fields(entry[1])
        records.append(
            StreamRecord(
                stream=stream_name,
                position=_provider_position(
                    _field(fields, b"p"),
                    label="record position",
                ),
                payload=_payload(_field(fields, b"d")),
            )
        )
    return tuple(records)


def _read_result(
    value: object,
    *,
    stream_name: str,
    after: int | None,
) -> tuple[StreamRecord, ...]:
    if not isinstance(value, list | tuple) or not value:
        raise CacheFeatureError("Redis stream returned an invalid replay result.")
    status = _integer(value[0])
    if status == 0:
        return ()
    if status == -1:
        assert after is not None
        raise CachePositionExpiredError(
            f"Stream position {after} is no longer retained."
        )
    if status != 1:
        raise CacheFeatureError("Redis stream returned an invalid replay result.")
    return _records(value[1:], stream_name=stream_name)


def _record_fields(value: object) -> Mapping[object, object]:
    if isinstance(value, Mapping):
        return cast(Mapping[object, object], value)
    if not isinstance(value, list | tuple) or len(value) % 2:
        raise CacheFeatureError("Redis stream returned invalid records.")
    return dict(zip(value[::2], value[1::2], strict=True))


def _field(fields: Mapping[object, object], name: bytes) -> object:
    value = fields.get(name)
    if value is None:
        value = fields.get(name.decode("utf-8"))
    if value is None:
        raise CacheFeatureError("Redis stream returned invalid records.")
    return value


def _provider_position(value: object, *, label: str) -> StreamPosition:
    try:
        return StreamPosition(_integer(value))
    except ValueError as exc:
        raise CacheFeatureError(f"Redis stream returned an invalid {label}.") from exc


def _integer(value: object) -> int:
    if isinstance(value, bool):
        raise CacheFeatureError("Redis stream returned an invalid integer.")
    if isinstance(value, bytes):
        try:
            value = value.decode("ascii")
        except UnicodeDecodeError as exc:
            raise CacheFeatureError(
                "Redis stream returned an invalid integer."
            ) from exc
    if isinstance(value, str):
        try:
            value = int(value)
        except ValueError as exc:
            raise CacheFeatureError(
                "Redis stream returned an invalid integer."
            ) from exc
    if not isinstance(value, int):
        raise CacheFeatureError("Redis stream returned an invalid integer.")
    return value


def _payload(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise CacheFeatureError("Redis stream returned an invalid payload.")
    try:
        return validate_payload(value)
    except ValueError as exc:
        raise CacheFeatureError("Redis stream returned an invalid payload.") from exc


__all__ = ("RedisStreamCache",)
