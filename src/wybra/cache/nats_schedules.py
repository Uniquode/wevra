from __future__ import annotations

import asyncio
import base64
import json
import logging
import math
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from hashlib import sha256
from time import monotonic
from typing import Any
from uuid import uuid4

from wybra.cache.feature_models import (
    DEFAULT_SCHEDULE_MAX_RECORDS,
    MAX_CACHE_FEATURE_LIMIT,
    CacheConflictError,
    CacheFeatureError,
    CacheRevision,
    FencingToken,
    LeaseToken,
    ScheduleClaim,
    ScheduleCursor,
    ScheduleRecord,
    validate_finite,
    validate_limit,
    validate_positive_finite,
    validate_positive_integer,
    validate_resource,
    validate_schedule_values,
)
from wybra.cache.nats_coordination import NatsCoordination
from wybra.cache.nats_runtime import NatsJetStreamRuntime, nats_feature_ttl_header

logger = logging.getLogger(__name__)
_LOCK_TTL_SECONDS = 30.0
_LOCK_RETRY_INITIAL_SECONDS = 0.01
_LOCK_RETRY_MAXIMUM_SECONDS = 0.25
_CAPACITY_LOCK_RESOURCE = "records"
_STATE_ATTEMPTS = 16
_REVISION_TTL_SECONDS = 100 * 365 * 24 * 60 * 60
_DELETED_STATE_TTL_SECONDS = 60.0
_STATE_LIVE = "live"
_STATE_CLAIMED = "claimed"
_STATE_DELETED = "deleted"
_SCHEDULE_METADATA_HEADER = "Wybra-Schedule-Metadata"


@dataclass(frozen=True, slots=True)
class _ScheduleAddress:
    owner: str
    identity: str
    owner_digest: str
    subject: str


@dataclass(frozen=True, slots=True)
class _ClaimState:
    claimant: str
    fencing_token: int
    expires_at: float
    token: str


@dataclass(frozen=True, slots=True)
class _ScheduleState:
    status: str
    identity: str
    revision: int
    payload: str
    next_due_at: float
    interval_seconds: float | None
    claim: _ClaimState | None = None


@dataclass(frozen=True, slots=True)
class _StateRecord:
    sequence: int
    state: _ScheduleState


@dataclass(frozen=True, slots=True)
class NatsScheduleCache:
    """Revisioned schedule storage over JetStream's durable state stream."""

    runtime: NatsJetStreamRuntime
    coordination: NatsCoordination
    max_records: int = DEFAULT_SCHEDULE_MAX_RECORDS

    def __post_init__(self) -> None:
        validate_positive_integer(self.max_records, label="maximum schedule records")
        if self.max_records > MAX_CACHE_FEATURE_LIMIT:
            raise ValueError(
                f"Maximum schedule records cannot exceed {MAX_CACHE_FEATURE_LIMIT}."
            )

    @property
    def maximum_records(self) -> int:
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
        address = _address(self.runtime, owner, identity)
        payload, next_due_at, interval_seconds = validate_schedule_values(
            payload,
            next_due_at,
            interval_seconds,
        )
        async with self._capacity_lock():
            async with self._owner_lock(address.owner):
                current = await self._state(address)
                if current is not None and current.state.status != _STATE_DELETED:
                    return None
                if (
                    current is None or current.state.status == _STATE_DELETED
                ) and await self._record_count() >= self.max_records:
                    raise CacheFeatureError(
                        "The NATS JetStream schedule store has reached its configured "
                        "record capacity."
                    )
                state = _ScheduleState(
                    _STATE_LIVE,
                    identity,
                    await self._next_revision(address.owner),
                    _encode_payload(payload),
                    next_due_at,
                    interval_seconds,
                )
                sequence = await self._write_state(
                    address,
                    0 if current is None else current.sequence,
                    state,
                )
                if sequence is None:
                    raise CacheConflictError(
                        "NATS JetStream schedule creation conflicted."
                    )
                return _record(state, sequence)

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
        address = _address(self.runtime, owner, identity)
        expected = _revision(expected)
        payload, next_due_at, interval_seconds = validate_schedule_values(
            payload,
            next_due_at,
            interval_seconds,
        )
        now = await self.runtime.refresh_time()
        async with self._owner_lock(address.owner):
            current = await self._state(address)
            if (
                current is None
                or current.state.revision != expected.value
                or current.state.status == _STATE_DELETED
                or _claim_live(current.state.claim, now)
            ):
                return None
            state = _ScheduleState(
                _STATE_LIVE,
                identity,
                await self._next_revision(address.owner),
                _encode_payload(payload),
                next_due_at,
                interval_seconds,
            )
            sequence = await self._write_state(address, current.sequence, state)
            return None if sequence is None else _record(state, sequence)

    async def delete(self, owner: str, identity: str) -> bool:
        address = _address(self.runtime, owner, identity)
        async with self._owner_lock(address.owner):
            current = await self._state(address)
            if current is None or current.state.status == _STATE_DELETED:
                return False
            deleted = _ScheduleState(
                _STATE_DELETED,
                current.state.identity,
                current.state.revision,
                "",
                0,
                None,
            )
            sequence = await self._write_state(
                address,
                current.sequence,
                deleted,
                ttl=_DELETED_STATE_TTL_SECONDS,
            )
            if sequence is None:
                raise CacheConflictError("NATS JetStream schedule deletion conflicted.")
            if current.state.claim is not None:
                await self._release_committed_claim(address, current.state.claim)
            return True

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
        now = await self.runtime.refresh_time()
        records: list[tuple[int, _ScheduleState]] = []
        for sequence, state in await self._owner_states(owner):
            if (
                state.status == _STATE_DELETED
                or state.next_due_at > before
                or _claim_live(state.claim, now)
            ):
                continue
            if after is not None and (
                state.next_due_at,
                state.identity,
            ) <= (after.next_due_at, after.identity):
                continue
            records.append((sequence, state))
        records.sort(key=lambda entry: (entry[1].next_due_at, entry[1].identity))
        selected: list[ScheduleRecord] = []
        for sequence, metadata in records[:limit]:
            current = await self._state(
                _address(self.runtime, owner, metadata.identity)
            )
            if current is None or current.sequence != sequence:
                continue
            state = current.state
            if (
                state.status == _STATE_DELETED
                or state.next_due_at > before
                or _claim_live(state.claim, now)
            ):
                continue
            selected.append(_record(state, current.sequence))
        return tuple(selected)

    async def claim(
        self,
        owner: str,
        identity: str,
        claimant: str,
        *,
        ttl: float,
    ) -> ScheduleClaim | None:
        address = _address(self.runtime, owner, identity)
        claimant = validate_resource(claimant, label="schedule claimant")
        ttl = validate_positive_finite(ttl, label="schedule claim TTL")
        now = await self.runtime.refresh_time()
        async with self._owner_lock(address.owner):
            current = await self._state(address)
            if (
                current is None
                or current.state.status == _STATE_DELETED
                or current.state.next_due_at > now
                or _claim_live(current.state.claim, now)
            ):
                return None
            lease = await self.coordination.acquire_internal(
                _claim_resource(address),
                claimant,
                ttl=ttl,
            )
            if lease is None:
                return None
            claim = _ClaimState(
                claimant,
                lease.fencing_token.value,
                lease.expires_at,
                lease.token,
            )
            state = _ScheduleState(
                _STATE_CLAIMED,
                current.state.identity,
                current.state.revision,
                current.state.payload,
                current.state.next_due_at,
                current.state.interval_seconds,
                claim,
            )
            try:
                sequence = await self._write_state(address, current.sequence, state)
            except BaseException as error:
                await self._release_failed_claim(lease, error)
                raise
            if sequence is None:
                error = CacheConflictError("NATS JetStream schedule claim conflicted.")
                await self._release_failed_claim(lease, error)
                raise error
            return _claim(address.owner, _record(state, sequence), claim)

    async def complete(self, claim: ScheduleClaim) -> ScheduleRecord | None:
        address, current = await self._required_claim(claim)
        async with self._owner_lock(address.owner):
            address, current = await self._required_claim(claim)
            if current.state.interval_seconds is None:
                deleted = _ScheduleState(
                    _STATE_DELETED,
                    current.state.identity,
                    current.state.revision,
                    "",
                    0,
                    None,
                )
                sequence = await self._write_state(
                    address,
                    current.sequence,
                    deleted,
                    ttl=_DELETED_STATE_TTL_SECONDS,
                )
                if sequence is None:
                    raise CacheConflictError(
                        "NATS JetStream schedule completion conflicted."
                    )
                await self._release_committed_schedule_claim(claim)
                return None
            next_due_at = _next_due_at(
                current.state.next_due_at,
                current.state.interval_seconds,
                await self.runtime.refresh_time(),
            )
            state = _ScheduleState(
                _STATE_LIVE,
                current.state.identity,
                await self._next_revision(address.owner),
                current.state.payload,
                next_due_at,
                current.state.interval_seconds,
            )
            sequence = await self._write_state(address, current.sequence, state)
            if sequence is None:
                raise CacheConflictError(
                    "NATS JetStream schedule completion conflicted."
                )
            await self._release_committed_schedule_claim(claim)
            return _record(state, sequence)

    async def discard(self, claim: ScheduleClaim) -> None:
        address, current = await self._required_claim(claim)
        async with self._owner_lock(address.owner):
            address, current = await self._required_claim(claim)
            deleted = _ScheduleState(
                _STATE_DELETED,
                current.state.identity,
                current.state.revision,
                "",
                0,
                None,
            )
            sequence = await self._write_state(
                address,
                current.sequence,
                deleted,
                ttl=_DELETED_STATE_TTL_SECONDS,
            )
            if sequence is None:
                raise CacheConflictError("NATS JetStream schedule discard conflicted.")
            await self._release_committed_schedule_claim(claim)

    async def held(self, claim: ScheduleClaim) -> bool:
        try:
            self._address_for_claim(claim)
        except TypeError, ValueError:
            return False
        try:
            _address, current = await self._required_claim(claim)
        except CacheConflictError:
            return False
        return _claim_live(current.state.claim, await self.runtime.refresh_time())

    async def advance(
        self,
        claim: ScheduleClaim,
        payload: bytes,
        *,
        next_due_at: float,
    ) -> ScheduleRecord:
        payload, next_due_at, _interval = validate_schedule_values(
            payload,
            next_due_at,
            None,
        )
        address, current = await self._required_claim(claim)
        async with self._owner_lock(address.owner):
            address, current = await self._required_claim(claim)
            state = _ScheduleState(
                _STATE_LIVE,
                current.state.identity,
                await self._next_revision(address.owner),
                _encode_payload(payload),
                next_due_at,
                current.state.interval_seconds,
            )
            sequence = await self._write_state(address, current.sequence, state)
            if sequence is None:
                raise CacheConflictError("NATS JetStream schedule advance conflicted.")
            await self._release_committed_schedule_claim(claim)
            return _record(state, sequence)

    async def release(self, claim: ScheduleClaim) -> None:
        address, current = await self._required_claim(claim)
        async with self._owner_lock(address.owner):
            address, current = await self._required_claim(claim)
            state = _ScheduleState(
                _STATE_LIVE,
                current.state.identity,
                current.state.revision,
                current.state.payload,
                current.state.next_due_at,
                current.state.interval_seconds,
            )
            sequence = await self._write_state(address, current.sequence, state)
            if sequence is None:
                raise CacheConflictError("NATS JetStream schedule release conflicted.")
            await self._release_committed_schedule_claim(claim)

    async def _required_claim(
        self,
        claim: ScheduleClaim,
    ) -> tuple[_ScheduleAddress, _StateRecord]:
        address = self._address_for_claim(claim)
        current = await self._state(address)
        if (
            current is None
            or not _claim_matches(current.state.claim, claim)
            or not _claim_live(current.state.claim, await self.runtime.refresh_time())
        ):
            raise CacheConflictError("Schedule claim is stale or no longer held.")
        return address, current

    async def _state(self, address: _ScheduleAddress) -> _StateRecord | None:
        await self.runtime.ensure_coordination_state_stream()
        jetstream = await self.runtime.coordination_jetstream()
        try:
            message = await jetstream.get_last_msg(
                self.runtime.coordination_state_stream_name,
                address.subject,
                direct=True,
            )
        except self.runtime.not_found_error():
            return None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream schedule state read failed (%s).", type(exc).__name__
            )
            raise CacheFeatureError(
                "NATS JetStream schedule state read failed."
            ) from None
        sequence = getattr(message, "seq", None)
        data = getattr(message, "data", None)
        if (
            not isinstance(sequence, int)
            or sequence <= 0
            or not isinstance(data, bytes)
        ):
            raise CacheFeatureError("NATS JetStream schedule state is invalid.")
        return _StateRecord(sequence, _decode_state(data))

    async def _write_state(
        self,
        address: _ScheduleAddress,
        expected_sequence: int,
        state: _ScheduleState,
        *,
        ttl: float | None = None,
    ) -> int | None:
        jetstream = await self.runtime.coordination_jetstream()
        headers = {
            "Nats-Expected-Last-Subject-Sequence": str(expected_sequence),
            _SCHEDULE_METADATA_HEADER: _encode_metadata(state),
        }
        if ttl is not None:
            headers["Nats-TTL"] = nats_feature_ttl_header(ttl)
        try:
            acknowledgement = await jetstream.publish(
                address.subject,
                _encode_state(state),
                stream=self.runtime.coordination_state_stream_name,
                headers=headers,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _is_sequence_conflict(exc):
                return None
            logger.warning(
                "NATS JetStream schedule state write failed (%s).", type(exc).__name__
            )
            raise CacheFeatureError(
                "NATS JetStream schedule state write failed."
            ) from None
        sequence = getattr(acknowledgement, "seq", None)
        if not isinstance(sequence, int) or sequence <= 0:
            raise CacheFeatureError("NATS JetStream schedule state is invalid.")
        return sequence

    async def _owner_states(
        self,
        owner: str,
    ) -> tuple[tuple[int, _ScheduleState], ...]:
        subject = f"{_owner_subject_prefix(self.runtime, owner)}>"
        records: list[tuple[int, _ScheduleState]] = []
        try:
            messages = await self.runtime.current_subject_messages(
                self.runtime.coordination_state_stream_name,
                subject,
                headers_only=True,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream schedule index read failed (%s).", type(exc).__name__
            )
            raise CacheFeatureError(
                "NATS JetStream schedule index read failed."
            ) from None
        for message in messages:
            sequence = getattr(
                getattr(getattr(message, "metadata", None), "sequence", None),
                "stream",
                None,
            )
            headers = getattr(message, "headers", None)
            if not isinstance(sequence, int) or sequence <= 0:
                raise CacheFeatureError("NATS JetStream schedule state is invalid.")
            records.append((sequence, _decode_metadata(headers)))
        return tuple(records)

    async def _record_count(self) -> int:
        messages = await self.runtime.current_subject_messages(
            self.runtime.coordination_state_stream_name,
            f"{self.runtime.coordination_state_subject_prefix}.schedule.>",
            headers_only=True,
        )
        return sum(
            _decode_metadata(getattr(message, "headers", None)).status != _STATE_DELETED
            for message in messages
        )

    async def _next_revision(self, owner: str) -> int:
        revision = await self.coordination.increment_internal(
            f"schedule-revision-{_owner_digest(owner)}",
            ttl=_REVISION_TTL_SECONDS,
        )
        return revision.value

    @asynccontextmanager
    async def _owner_lock(self, owner: str) -> AsyncIterator[None]:
        async with self._lock(f"schedules-{_owner_digest(owner)}"):
            yield

    @asynccontextmanager
    async def _capacity_lock(self) -> AsyncIterator[None]:
        async with self._lock(_CAPACITY_LOCK_RESOURCE):
            yield

    @asynccontextmanager
    async def _lock(self, resource: str) -> AsyncIterator[None]:
        holder = uuid4().hex
        lease: LeaseToken | None = None
        deadline = monotonic() + _LOCK_TTL_SECONDS
        retry_delay = _LOCK_RETRY_INITIAL_SECONDS
        while True:
            lease = await self.coordination.acquire_internal(
                resource,
                holder,
                ttl=_LOCK_TTL_SECONDS,
            )
            if lease is not None:
                break
            remaining = deadline - monotonic()
            if remaining <= 0:
                raise CacheConflictError("NATS JetStream schedule operation is busy.")
            await asyncio.sleep(min(retry_delay, remaining))
            retry_delay = min(retry_delay * 2, _LOCK_RETRY_MAXIMUM_SECONDS)
        try:
            yield
        finally:
            try:
                await self.coordination.release_internal(lease)
            except CacheFeatureError as exc:
                logger.warning(
                    "NATS JetStream schedule lock cleanup failed (%s).",
                    type(exc).__name__,
                )

    async def _release_committed_schedule_claim(self, claim: ScheduleClaim) -> None:
        await self._release_committed_claim(
            self._address_for_claim(claim),
            _claim_from_schedule(claim),
        )

    async def _release_committed_claim(
        self,
        address: _ScheduleAddress,
        claim: _ClaimState,
    ) -> None:
        try:
            await self._release_claim(address, claim)
        except CacheFeatureError as exc:
            logger.warning(
                "NATS JetStream schedule claim cleanup failed (%s).",
                type(exc).__name__,
            )

    async def _release_failed_claim(
        self,
        lease: LeaseToken,
        error: BaseException,
    ) -> None:
        try:
            await self.coordination.release_internal(lease)
        except asyncio.CancelledError:
            if isinstance(error, asyncio.CancelledError):
                return
            raise
        except Exception as cleanup_error:
            error.add_note(
                "NATS JetStream schedule claim cleanup failed "
                f"({type(cleanup_error).__name__})."
            )
            logger.warning(
                "NATS JetStream schedule claim cleanup failed (%s).",
                type(cleanup_error).__name__,
            )

    async def _release_claim(
        self,
        address: _ScheduleAddress,
        claim: _ClaimState,
    ) -> None:
        lease = LeaseToken(
            address.owner,
            _claim_resource(address),
            claim.claimant,
            FencingToken(claim.fencing_token),
            claim.expires_at,
            claim.token,
        )
        try:
            await self.coordination.release_internal(lease)
        except CacheConflictError:
            if claim.expires_at > await self.runtime.refresh_time():
                raise

    def _address_for_claim(self, claim: ScheduleClaim) -> _ScheduleAddress:
        if not isinstance(claim, ScheduleClaim):
            raise TypeError("Claim must be a ScheduleClaim.")
        return _address(self.runtime, claim.owner, claim.record.identity)


def _address(
    runtime: NatsJetStreamRuntime,
    owner: str,
    identity: str,
) -> _ScheduleAddress:
    owner = validate_resource(owner, label="cache owner")
    identity = validate_resource(identity, label="schedule identity")
    owner_digest = _owner_digest(owner)
    identity_digest = sha256(identity.encode()).hexdigest()
    return _ScheduleAddress(
        owner,
        identity,
        owner_digest,
        f"{runtime.coordination_state_subject_prefix}.schedule.{owner_digest}.{identity_digest}",
    )


def _owner_digest(owner: str) -> str:
    return sha256(validate_resource(owner, label="cache owner").encode()).hexdigest()


def _owner_subject_prefix(runtime: NatsJetStreamRuntime, owner: str) -> str:
    return (
        f"{runtime.coordination_state_subject_prefix}.schedule.{_owner_digest(owner)}."
    )


def _claim_resource(address: _ScheduleAddress) -> str:
    identity_digest = sha256(address.identity.encode()).hexdigest()
    return f"schedule-{address.owner_digest}-{identity_digest}"


def _encode_payload(payload: bytes) -> str:
    return base64.b64encode(payload).decode("ascii")


def _decode_payload(payload: str) -> bytes:
    try:
        return base64.b64decode(payload, validate=True)
    except (TypeError, ValueError) as exc:
        raise CacheFeatureError("NATS JetStream schedule state is invalid.") from exc


def _encode_state(state: _ScheduleState) -> bytes:
    return json.dumps(
        {
            "status": state.status,
            "identity": state.identity,
            "revision": state.revision,
            "payload": state.payload,
            "next_due_at": state.next_due_at,
            "interval_seconds": state.interval_seconds,
            "claim": None
            if state.claim is None
            else {
                "claimant": state.claim.claimant,
                "fencing_token": state.claim.fencing_token,
                "expires_at": state.claim.expires_at,
                "token": state.claim.token,
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _encode_metadata(state: _ScheduleState) -> str:
    return json.dumps(
        {
            "status": state.status,
            "identity": state.identity,
            "revision": state.revision,
            "next_due_at": state.next_due_at,
            "interval_seconds": state.interval_seconds,
            "claim": None
            if state.claim is None
            else {
                "claimant": state.claim.claimant,
                "fencing_token": state.claim.fencing_token,
                "expires_at": state.claim.expires_at,
                "token": state.claim.token,
            },
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def _decode_state(value: bytes) -> _ScheduleState:
    try:
        decoded = json.loads(value)
        if not isinstance(decoded, dict):
            raise TypeError
        claim_value = decoded["claim"]
        claim = None
        if claim_value is not None:
            if not isinstance(claim_value, dict):
                raise TypeError
            claim = _ClaimState(
                claim_value["claimant"],
                claim_value["fencing_token"],
                claim_value["expires_at"],
                claim_value["token"],
            )
        state = _ScheduleState(
            decoded["status"],
            decoded["identity"],
            decoded["revision"],
            decoded["payload"],
            decoded["next_due_at"],
            decoded["interval_seconds"],
            claim,
        )
        _validate_state(state)
        return state
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise CacheFeatureError("NATS JetStream schedule state is invalid.") from exc


def _decode_metadata(headers: Any) -> _ScheduleState:
    if headers is None:
        raise CacheFeatureError("NATS JetStream schedule state is invalid.")
    value = headers.get(_SCHEDULE_METADATA_HEADER)
    if not isinstance(value, str):
        raise CacheFeatureError("NATS JetStream schedule state is invalid.")
    try:
        decoded = json.loads(value)
        if not isinstance(decoded, dict):
            raise TypeError
        claim_value = decoded["claim"]
        claim = None
        if claim_value is not None:
            if not isinstance(claim_value, dict):
                raise TypeError
            claim = _ClaimState(
                claim_value["claimant"],
                claim_value["fencing_token"],
                claim_value["expires_at"],
                claim_value["token"],
            )
        state = _ScheduleState(
            decoded["status"],
            decoded["identity"],
            decoded["revision"],
            "",
            decoded["next_due_at"],
            decoded["interval_seconds"],
            claim,
        )
        _validate_metadata(state)
        return state
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise CacheFeatureError("NATS JetStream schedule state is invalid.") from exc


def _validate_state(state: _ScheduleState) -> None:
    if state.status not in {_STATE_LIVE, _STATE_CLAIMED, _STATE_DELETED}:
        raise ValueError("Schedule status is invalid.")
    validate_resource(state.identity, label="schedule identity")
    validate_positive_integer(state.revision, label="schedule revision")
    if state.status != _STATE_DELETED:
        _decode_payload(state.payload)
        validate_finite(state.next_due_at, label="schedule due time")
        if state.interval_seconds is not None:
            validate_positive_finite(state.interval_seconds, label="schedule interval")
    if state.status == _STATE_CLAIMED and state.claim is None:
        raise ValueError("Schedule claim is invalid.")
    if state.claim is not None:
        validate_resource(state.claim.claimant, label="schedule claimant")
        validate_positive_integer(state.claim.fencing_token, label="fencing token")
        validate_finite(state.claim.expires_at, label="schedule claim expiry")
        validate_resource(state.claim.token, label="schedule claim token")


def _validate_metadata(state: _ScheduleState) -> None:
    if state.status not in {_STATE_LIVE, _STATE_CLAIMED, _STATE_DELETED}:
        raise ValueError("Schedule status is invalid.")
    validate_resource(state.identity, label="schedule identity")
    validate_positive_integer(state.revision, label="schedule revision")
    if state.status != _STATE_DELETED:
        validate_finite(state.next_due_at, label="schedule due time")
        if state.interval_seconds is not None:
            validate_positive_finite(state.interval_seconds, label="schedule interval")
    if state.status == _STATE_CLAIMED and state.claim is None:
        raise ValueError("Schedule claim is invalid.")
    if state.claim is not None:
        validate_resource(state.claim.claimant, label="schedule claimant")
        validate_positive_integer(state.claim.fencing_token, label="fencing token")
        validate_finite(state.claim.expires_at, label="schedule claim expiry")
        validate_resource(state.claim.token, label="schedule claim token")


def _record(state: _ScheduleState, sequence: int) -> ScheduleRecord:
    return ScheduleRecord(
        state.identity,
        CacheRevision(state.revision),
        _decode_payload(state.payload),
        state.next_due_at,
        state.interval_seconds,
    )


def _claim(owner: str, record: ScheduleRecord, state: _ClaimState) -> ScheduleClaim:
    return ScheduleClaim(
        owner,
        record,
        state.claimant,
        FencingToken(state.fencing_token),
        state.expires_at,
        state.token,
    )


def _claim_from_schedule(claim: ScheduleClaim) -> _ClaimState:
    return _ClaimState(
        claim.claimant,
        claim.fencing_token.value,
        claim.expires_at,
        claim.token,
    )


def _claim_matches(state: _ClaimState | None, claim: ScheduleClaim) -> bool:
    return state == _claim_from_schedule(claim)


def _claim_live(claim: _ClaimState | None, now: float) -> bool:
    return claim is not None and claim.expires_at > now


def _next_due_at(previous: float, interval: float, now: float) -> float:
    elapsed = (now - previous) / interval
    if not math.isfinite(elapsed):
        raise CacheFeatureError("Recurring schedule cannot advance safely.")
    missed = max(0, math.floor(elapsed) + 1)
    next_due_at = previous + max(1, missed) * interval
    if not math.isfinite(next_due_at):
        raise CacheFeatureError("Recurring schedule cannot advance safely.")
    if next_due_at <= now:
        return math.nextafter(now, math.inf)
    return next_due_at


def _revision(value: CacheRevision) -> CacheRevision:
    if not isinstance(value, CacheRevision):
        raise TypeError("Expected revision must be a CacheRevision.")
    return value


def _is_sequence_conflict(error: Exception) -> bool:
    return getattr(error, "err_code", None) in {10071, 10164}


__all__ = ("NatsScheduleCache",)
