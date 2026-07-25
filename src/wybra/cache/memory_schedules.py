from __future__ import annotations

import asyncio
import math
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from uuid import uuid4

from wybra.cache.feature_models import (
    CacheConflictError,
    CacheFeatureError,
    CacheRevision,
    FencingToken,
    ScheduleClaim,
    ScheduleRecord,
    validate_finite,
    validate_limit,
    validate_payload,
    validate_positive_finite,
    validate_positive_integer,
    validate_resource,
)

type Clock = Callable[[], float]
type ScheduleKey = tuple[str, str]


@dataclass(slots=True)
class _ClaimState:
    claimant: str
    fencing_token: FencingToken
    expires_at: float
    token: str
    revision: CacheRevision


@dataclass(slots=True)
class InMemoryScheduleCache:
    clock: Clock = field(default=time.time, repr=False)
    max_records: int = 10_000
    _records: dict[ScheduleKey, ScheduleRecord] = field(
        default_factory=dict,
        init=False,
    )
    _claims: dict[ScheduleKey, _ClaimState] = field(default_factory=dict, init=False)
    _next_fencing_token: int = field(default=0, init=False)
    _next_revision: int = field(default=0, init=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)

    def __post_init__(self) -> None:
        validate_positive_integer(
            self.max_records,
            label="maximum schedule records",
        )

    async def create(
        self,
        owner: str,
        identity: str,
        payload: bytes,
        *,
        next_due_at: float,
        interval_seconds: float | None = None,
    ) -> ScheduleRecord | None:
        schedule_key = _schedule_key(owner, identity)
        payload, next_due_at, interval_seconds = _schedule_values(
            payload,
            next_due_at,
            interval_seconds,
        )
        async with self._lock:
            if schedule_key in self._records:
                return None
            if len(self._records) >= self.max_records:
                raise CacheFeatureError(
                    "The in-memory schedule store has reached its configured "
                    "record capacity."
                )
            record = ScheduleRecord(
                identity=identity,
                revision=self._revision(),
                payload=payload,
                next_due_at=next_due_at,
                interval_seconds=interval_seconds,
            )
            self._records[schedule_key] = record
            return record

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
        schedule_key = _schedule_key(owner, identity)
        payload, next_due_at, interval_seconds = _schedule_values(
            payload,
            next_due_at,
            interval_seconds,
        )
        async with self._lock:
            self._recover_claims()
            current = self._records.get(schedule_key)
            if (
                current is None
                or current.revision != expected
                or schedule_key in self._claims
            ):
                return None
            record = ScheduleRecord(
                identity=identity,
                revision=self._revision(),
                payload=payload,
                next_due_at=next_due_at,
                interval_seconds=interval_seconds,
            )
            self._records[schedule_key] = record
            return record

    async def due(
        self,
        owner: str,
        *,
        before: float,
        limit: int = 100,
    ) -> tuple[ScheduleRecord, ...]:
        owner = validate_resource(owner, label="cache owner")
        before = validate_finite(before, label="schedule due boundary")
        limit = validate_limit(limit)
        async with self._lock:
            self._recover_claims()
            records = [
                record
                for schedule_key, record in self._records.items()
                if schedule_key[0] == owner
                and record.next_due_at <= before
                and schedule_key not in self._claims
            ]
            records.sort(key=lambda record: (record.next_due_at, record.identity))
            return tuple(records[:limit])

    async def claim(
        self,
        owner: str,
        identity: str,
        claimant: str,
        *,
        ttl: float,
    ) -> ScheduleClaim | None:
        schedule_key = _schedule_key(owner, identity)
        claimant = validate_resource(claimant, label="schedule claimant")
        ttl = validate_positive_finite(ttl, label="schedule claim TTL")
        async with self._lock:
            self._recover_claims()
            record = self._records.get(schedule_key)
            if (
                record is None
                or record.next_due_at > self.clock()
                or schedule_key in self._claims
            ):
                return None
            self._next_fencing_token += 1
            state = _ClaimState(
                claimant=claimant,
                fencing_token=FencingToken(self._next_fencing_token),
                expires_at=self.clock() + ttl,
                token=uuid4().hex,
                revision=record.revision,
            )
            self._claims[schedule_key] = state
            return _claim_value(owner, record, state)

    async def complete(self, claim: ScheduleClaim) -> ScheduleRecord | None:
        async with self._lock:
            schedule_key, record = self._required_claim(claim)
            if record.interval_seconds is None:
                self._claims.pop(schedule_key, None)
                self._records.pop(schedule_key, None)
                return None
            now = self.clock()
            next_due_at = _next_due_at(record, now)
            updated = ScheduleRecord(
                identity=record.identity,
                revision=self._revision(),
                payload=record.payload,
                next_due_at=next_due_at,
                interval_seconds=record.interval_seconds,
            )
            self._claims.pop(schedule_key, None)
            self._records[schedule_key] = updated
            return updated

    async def release(self, claim: ScheduleClaim) -> None:
        async with self._lock:
            schedule_key, _record = self._required_claim(claim)
            self._claims.pop(schedule_key, None)

    def _required_claim(
        self,
        claim: ScheduleClaim,
    ) -> tuple[ScheduleKey, ScheduleRecord]:
        self._recover_claims()
        schedule_key = _schedule_key(claim.owner, claim.record.identity)
        state = self._claims.get(schedule_key)
        if state is None or state.token != claim.token:
            raise CacheConflictError(
                f"Schedule claim for {claim.record.identity!r} is stale."
            )
        record = self._records.get(schedule_key)
        if (
            record is None
            or state.claimant != claim.claimant
            or state.fencing_token != claim.fencing_token
            or state.revision != claim.record.revision
            or record.revision != claim.record.revision
        ):
            raise CacheConflictError(
                f"Schedule claim for {claim.record.identity!r} is stale."
            )
        return schedule_key, record

    def _recover_claims(self) -> None:
        now = self.clock()
        expired = [
            schedule_key
            for schedule_key, state in self._claims.items()
            if state.expires_at <= now
        ]
        for schedule_key in expired:
            self._claims.pop(schedule_key, None)

    def _revision(self) -> CacheRevision:
        self._next_revision += 1
        return CacheRevision(self._next_revision)


def _schedule_key(owner: str, identity: str) -> ScheduleKey:
    return (
        validate_resource(owner, label="cache owner"),
        validate_resource(identity, label="schedule identity"),
    )


def _schedule_values(
    payload: bytes,
    next_due_at: float,
    interval_seconds: float | None,
) -> tuple[bytes, float, float | None]:
    payload = validate_payload(payload)
    next_due_at = validate_finite(next_due_at, label="schedule due time")
    if interval_seconds is not None:
        interval_seconds = validate_positive_finite(
            interval_seconds,
            label="schedule interval",
        )
    return payload, next_due_at, interval_seconds


def _claim_value(
    owner: str,
    record: ScheduleRecord,
    state: _ClaimState,
) -> ScheduleClaim:
    return ScheduleClaim(
        owner=owner,
        record=record,
        claimant=state.claimant,
        fencing_token=state.fencing_token,
        expires_at=state.expires_at,
        token=state.token,
    )


def _next_due_at(record: ScheduleRecord, now: float) -> float:
    interval = record.interval_seconds
    if interval is None:
        raise CacheFeatureError("A one-time schedule has no next due time.")
    next_due_at = record.next_due_at + interval
    if next_due_at <= now:
        elapsed = now - record.next_due_at
        if math.isfinite(elapsed):
            remainder = math.fmod(elapsed, interval)
            next_due_at = now + (interval - remainder)
        if next_due_at <= now or not math.isfinite(next_due_at):
            next_due_at = math.nextafter(now, math.inf)
    if not math.isfinite(next_due_at):
        raise CacheFeatureError(
            f"Recurring schedule {record.identity!r} cannot advance beyond the "
            "current clock value."
        )
    return next_due_at


__all__ = ("InMemoryScheduleCache",)
