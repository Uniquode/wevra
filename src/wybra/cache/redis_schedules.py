from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from wybra.cache.feature_models import (
    DEFAULT_SCHEDULE_MAX_RECORDS,
    MAX_CACHE_FEATURE_LIMIT,
    CacheConflictError,
    CacheFeatureError,
    CacheRevision,
    FencingToken,
    ScheduleClaim,
    ScheduleCursor,
    ScheduleRecord,
    validate_finite,
    validate_limit,
    validate_payload,
    validate_positive_finite,
    validate_positive_integer,
    validate_resource,
    validate_schedule_values,
)
from wybra.cache.redis_runtime import RedisCacheRuntime
from wybra.cache.redis_schedule_ordering import (
    due_boundary_member,
    due_member,
    encode_due_order,
)
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


@dataclass(frozen=True, slots=True)
class _ScheduleKeys:
    record: str
    due: str
    due_lex: str
    claim: str
    claims: str
    index: str
    revision: str
    fencing: str
    count: str


@dataclass(frozen=True, slots=True)
class RedisScheduleCache:
    """Redis-backed revisioned schedule storage with fenced claims."""

    runtime: RedisCacheRuntime
    max_records: int = DEFAULT_SCHEDULE_MAX_RECORDS

    def __post_init__(self) -> None:
        validate_positive_integer(self.max_records, label="maximum schedule records")
        if self.max_records > MAX_CACHE_FEATURE_LIMIT:
            raise ValueError(
                f"Maximum schedule records cannot exceed {MAX_CACHE_FEATURE_LIMIT}."
            )

    @property
    def maximum_records(self) -> int:
        """Return the bounded capacity required by the schedule feature."""
        return self.max_records

    async def create(
        self,
        owner: str,
        identity: str,
        payload: bytes,
        *,
        next_due_at: float,
        interval_seconds: float | None = None,
    ) -> ScheduleRecord | None:
        keys = self._keys(owner, identity)
        payload, next_due_at, interval_seconds = validate_schedule_values(
            payload,
            next_due_at,
            interval_seconds,
        )

        async def create_record(client: Any) -> object:
            return await client.eval(
                SCHEDULE_CREATE_SCRIPT,
                6,
                keys.record,
                keys.due,
                keys.revision,
                keys.index,
                keys.count,
                keys.due_lex,
                identity,
                next_due_at,
                payload,
                _interval_value(interval_seconds),
                self.max_records,
                due_member(next_due_at, identity),
            )

        result = _script_result(await self.runtime.feature_call(create_record))
        if result[0] == 0:
            return None
        if result[0] == -1:
            raise CacheFeatureError(
                "The Redis schedule store has reached its configured record capacity."
            )
        _require_success(result, length=2, label="create")
        return ScheduleRecord(
            identity=identity,
            revision=CacheRevision(_integer(result[1], label="schedule revision")),
            payload=payload,
            next_due_at=next_due_at,
            interval_seconds=interval_seconds,
        )

    async def update(
        self,
        owner: str,
        identity: str,
        expected: CacheRevision,
        payload: bytes,
        *,
        next_due_at: float,
        interval_seconds: float | None = None,
    ) -> ScheduleRecord | None:
        keys = self._keys(owner, identity)
        expected = _revision(expected)
        payload, next_due_at, interval_seconds = validate_schedule_values(
            payload,
            next_due_at,
            interval_seconds,
        )

        async def update_record(client: Any) -> object:
            return await client.eval(
                SCHEDULE_UPDATE_SCRIPT,
                5,
                keys.record,
                keys.claim,
                keys.due,
                keys.revision,
                keys.due_lex,
                expected.value,
                next_due_at,
                payload,
                _interval_value(interval_seconds),
                identity,
                due_member(next_due_at, identity),
            )

        result = _script_result(await self.runtime.feature_call(update_record))
        if result[0] == 0:
            return None
        _require_success(result, length=2, label="update")
        return ScheduleRecord(
            identity=identity,
            revision=CacheRevision(_integer(result[1], label="schedule revision")),
            payload=payload,
            next_due_at=next_due_at,
            interval_seconds=interval_seconds,
        )

    async def due(
        self,
        owner: str,
        *,
        before: float,
        limit: int = 100,
        after: ScheduleCursor | None = None,
    ) -> tuple[ScheduleRecord, ...]:
        owner = validate_resource(owner, label="cache owner")
        before = validate_finite(before, label="schedule due boundary")
        limit = validate_limit(limit)
        if after is not None and not isinstance(after, ScheduleCursor):
            raise TypeError("Schedule due cursor must be a ScheduleCursor.")
        due_key = self.runtime.key("schedule-due", owner, "records")

        async def due_records(client: Any) -> object:
            return await client.eval(
                SCHEDULE_DUE_SCRIPT,
                4,
                due_key,
                self.runtime.key("schedule-claims", owner, "records"),
                self.runtime.key("schedule-index", owner, "records"),
                self.runtime.key("schedule-due-lex", owner, "records"),
                before,
                limit,
                due_boundary_member(before),
                "" if after is None else due_member(after.next_due_at, after.identity),
            )

        result = await self.runtime.feature_call(due_records)
        if not isinstance(result, list | tuple):
            raise CacheFeatureError("Redis schedule due query returned invalid state.")
        return tuple(_record(entry) for entry in result)

    async def delete(self, owner: str, identity: str) -> bool:
        keys = self._keys(owner, identity)

        async def delete_record(client: Any) -> object:
            return await client.eval(
                SCHEDULE_DELETE_SCRIPT,
                7,
                keys.record,
                keys.due,
                keys.claim,
                keys.claims,
                keys.index,
                keys.count,
                keys.due_lex,
                identity,
            )

        result = _script_result(await self.runtime.feature_call(delete_record))
        if len(result) != 1 or result[0] not in (0, 1):
            raise CacheFeatureError("Redis schedule deletion returned invalid state.")
        return result[0] == 1

    async def claim(
        self,
        owner: str,
        identity: str,
        claimant: str,
        *,
        ttl: float,
    ) -> ScheduleClaim | None:
        keys = self._keys(owner, identity)
        claimant = validate_resource(claimant, label="schedule claimant")
        ttl_ms = self.runtime.ttl_milliseconds(ttl, label="schedule claim TTL")
        token = uuid4().hex

        async def claim_record(client: Any) -> object:
            return await client.eval(
                SCHEDULE_CLAIM_SCRIPT,
                6,
                keys.record,
                keys.claim,
                keys.fencing,
                keys.due,
                keys.claims,
                keys.due_lex,
                claimant,
                token,
                ttl_ms,
            )

        result = _script_result(await self.runtime.feature_call(claim_record))
        if result[0] == 0:
            return None
        _require_success(result, length=8, label="claim")
        record = _record(result[1:6])
        return ScheduleClaim(
            owner=owner,
            record=record,
            claimant=claimant,
            fencing_token=FencingToken(
                _integer(result[6], label="schedule fencing token")
            ),
            expires_at=_number(result[7], label="schedule claim expiry"),
            token=token,
        )

    async def complete(self, claim: ScheduleClaim) -> ScheduleRecord | None:
        claim = _claim(claim)
        keys = self._keys(claim.owner, claim.record.identity)

        async def complete_record(client: Any) -> object:
            return await client.eval(
                SCHEDULE_COMPLETE_SCRIPT,
                8,
                keys.record,
                keys.due,
                keys.claim,
                keys.revision,
                keys.count,
                keys.claims,
                keys.index,
                keys.due_lex,
                claim.claimant,
                claim.token,
                claim.fencing_token.value,
                claim.record.revision.value,
            )

        result = _script_result(await self.runtime.feature_call(complete_record))
        if result[0] == 0:
            raise CacheConflictError("Schedule claim is stale or no longer held.")
        if result[0] == -2:
            raise CacheFeatureError("Recurring schedule cannot advance safely.")
        if len(result) == 2 and result[0] == 1 and result[1] == 0:
            return None
        _require_success(result, length=7, label="completion")
        return _record(result[2:])

    async def discard(self, claim: ScheduleClaim) -> None:
        claim = _claim(claim)
        keys = self._keys(claim.owner, claim.record.identity)

        async def discard_record(client: Any) -> object:
            return await client.eval(
                SCHEDULE_DISCARD_SCRIPT,
                7,
                keys.record,
                keys.due,
                keys.claim,
                keys.claims,
                keys.index,
                keys.count,
                keys.due_lex,
                claim.claimant,
                claim.token,
                claim.fencing_token.value,
                claim.record.revision.value,
                claim.record.identity,
            )

        result = _script_result(await self.runtime.feature_call(discard_record))
        if result[0] == 0:
            raise CacheConflictError("Schedule claim is stale or no longer held.")
        _require_success(result, length=1, label="discard")

    async def advance(
        self,
        claim: ScheduleClaim,
        payload: bytes,
        *,
        next_due_at: float,
    ) -> ScheduleRecord:
        claim = _claim(claim)
        payload, next_due_at, _interval_seconds = validate_schedule_values(
            payload,
            next_due_at,
            None,
        )
        keys = self._keys(claim.owner, claim.record.identity)

        async def advance_record(client: Any) -> object:
            return await client.eval(
                SCHEDULE_ADVANCE_SCRIPT,
                6,
                keys.record,
                keys.due,
                keys.claim,
                keys.revision,
                keys.claims,
                keys.due_lex,
                claim.claimant,
                claim.token,
                claim.fencing_token.value,
                claim.record.revision.value,
                next_due_at,
                payload,
                claim.record.identity,
                due_member(next_due_at, claim.record.identity),
            )

        result = _script_result(await self.runtime.feature_call(advance_record))
        if result[0] == 0:
            raise CacheConflictError("Schedule claim is stale or no longer held.")
        _require_success(result, length=2, label="advance")
        return ScheduleRecord(
            identity=claim.record.identity,
            revision=CacheRevision(_integer(result[1], label="schedule revision")),
            payload=payload,
            next_due_at=next_due_at,
            interval_seconds=claim.record.interval_seconds,
        )

    async def held(self, claim: ScheduleClaim) -> bool:
        claim = _claim(claim)
        keys = self._keys(claim.owner, claim.record.identity)

        async def held_claim(client: Any) -> object:
            return await client.eval(
                SCHEDULE_HELD_SCRIPT,
                2,
                keys.record,
                keys.claim,
                claim.claimant,
                claim.token,
                claim.fencing_token.value,
                claim.record.revision.value,
            )

        result = _script_result(await self.runtime.feature_call(held_claim))
        if len(result) != 1 or result[0] not in (0, 1):
            raise CacheFeatureError(
                "Redis schedule claim check returned invalid state."
            )
        return result[0] == 1

    async def release(self, claim: ScheduleClaim) -> None:
        claim = _claim(claim)
        keys = self._keys(claim.owner, claim.record.identity)

        async def release_claim(client: Any) -> object:
            return await client.eval(
                SCHEDULE_RELEASE_SCRIPT,
                5,
                keys.record,
                keys.claim,
                keys.due,
                keys.claims,
                keys.due_lex,
                claim.claimant,
                claim.token,
                claim.fencing_token.value,
                claim.record.revision.value,
                claim.record.identity,
            )

        result = _script_result(await self.runtime.feature_call(release_claim))
        if result[0] == 0:
            raise CacheConflictError("Schedule claim is stale or no longer held.")
        _require_success(result, length=1, label="claim release")

    def _keys(self, owner: str, identity: str) -> _ScheduleKeys:
        owner = validate_resource(owner, label="cache owner")
        identity = validate_resource(identity, label="schedule identity")
        return _ScheduleKeys(
            record=self.runtime.key("schedule", owner, identity),
            due=self.runtime.key("schedule-due", owner, "records"),
            due_lex=self.runtime.key("schedule-due-lex", owner, "records"),
            claim=self.runtime.key("schedule-claim", owner, identity),
            claims=self.runtime.key("schedule-claims", owner, "records"),
            index=self.runtime.key("schedule-index", owner, "records"),
            revision=self.runtime.sequence_key("schedule-revision"),
            fencing=self.runtime.sequence_key("schedule-fencing"),
            count=self.runtime.sequence_key("schedule-count"),
        )


def _interval_value(interval_seconds: float | None) -> str:
    return "" if interval_seconds is None else repr(interval_seconds)


def _claim(value: ScheduleClaim) -> ScheduleClaim:
    if not isinstance(value, ScheduleClaim):
        raise TypeError("Claim must be a ScheduleClaim.")
    _revision(value.record.revision)
    validate_resource(value.owner, label="cache owner")
    validate_resource(value.record.identity, label="schedule identity")
    validate_resource(value.claimant, label="schedule claimant")
    validate_resource(value.token, label="schedule claim token")
    return value


def _revision(value: CacheRevision) -> CacheRevision:
    if not isinstance(value, CacheRevision):
        raise TypeError("Expected revision must be a CacheRevision.")
    return value


def _script_result(value: object) -> tuple[object, ...]:
    if not isinstance(value, list | tuple) or not value:
        raise CacheFeatureError("Redis schedule operation returned invalid state.")
    return tuple(value)


def _require_success(result: tuple[object, ...], *, length: int, label: str) -> None:
    if result[0] != 1 or len(result) != length:
        raise CacheFeatureError(f"Redis schedule {label} returned invalid state.")


def _record(value: object) -> ScheduleRecord:
    if not isinstance(value, list | tuple) or len(value) != 5:
        raise CacheFeatureError(
            "Redis schedule operation returned invalid record state."
        )
    identity, payload, revision, due_at, interval = value
    return ScheduleRecord(
        identity=_text(identity, label="schedule identity"),
        payload=_payload(payload),
        revision=CacheRevision(_integer(revision, label="schedule revision")),
        next_due_at=_number(due_at, label="schedule due time"),
        interval_seconds=_interval(interval),
    )


def _text(value: object, *, label: str) -> str:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            raise CacheFeatureError(f"Redis {label} is invalid.") from None
    if not isinstance(value, str):
        raise CacheFeatureError(f"Redis {label} is invalid.")
    try:
        return validate_resource(value, label=label)
    except (TypeError, ValueError) as exc:
        raise CacheFeatureError(f"Redis {label} is invalid.") from exc


def _payload(value: object) -> bytes:
    if not isinstance(value, bytes):
        raise CacheFeatureError("Redis schedule payload is invalid.")
    try:
        return validate_payload(value)
    except (TypeError, ValueError) as exc:
        raise CacheFeatureError("Redis schedule payload is invalid.") from exc


def _integer(value: object, *, label: str) -> int:
    if isinstance(value, bytes):
        try:
            value = int(value)
        except ValueError:
            raise CacheFeatureError(f"Redis {label} is invalid.") from None
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise CacheFeatureError(f"Redis {label} is invalid.")
    return value


def _number(value: object, *, label: str) -> float:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            raise CacheFeatureError(f"Redis {label} is invalid.") from None
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            raise CacheFeatureError(f"Redis {label} is invalid.") from None
    if isinstance(value, bool) or not isinstance(value, int | float):
        raise CacheFeatureError(f"Redis {label} is invalid.")
    try:
        return validate_finite(value, label=label)
    except (TypeError, ValueError) as exc:
        raise CacheFeatureError(f"Redis {label} is invalid.") from exc


def _interval(value: object) -> float | None:
    if value in (b"", "", None):
        return None
    try:
        return validate_positive_finite(
            _number(value, label="schedule interval"),
            label="schedule interval",
        )
    except (TypeError, ValueError) as exc:
        raise CacheFeatureError("Redis schedule interval is invalid.") from exc


__all__ = ("RedisScheduleCache", "encode_due_order")
