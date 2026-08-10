from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import datetime
from hashlib import sha256
from time import monotonic
from typing import Any
from uuid import uuid4

from wybra.cache.feature_models import (
    AtomicCacheValue,
    CacheConflictError,
    CacheFeatureError,
    CacheRevision,
    CounterCacheValue,
    FencingToken,
    LeaseToken,
    validate_lease_token,
    validate_payload,
    validate_positive_finite,
    validate_resource,
)
from wybra.cache.nats_runtime import (
    COORDINATION_COMMAND_MAX_AGE_SECONDS,
    NatsJetStreamRuntime,
    nats_feature_ttl_header,
)

logger = logging.getLogger(__name__)
_FRAME_MAGIC = b"WYC1"
_REPLY_TIMEOUT_SECONDS = 10.0
_COORDINATOR_MAX_DELIVERY_ATTEMPTS = 3
_OPERATION_RESULT_TTL_SECONDS = COORDINATION_COMMAND_MAX_AGE_SECONDS * 2
_COORDINATOR_FETCH_TIMEOUT_SECONDS = 0.5
_COORDINATOR_DURABLE_SUFFIX = "COORDINATOR"
_ACTION_ACQUIRE = b"acquire"
_ACTION_RENEW = b"renew"
_ACTION_RELEASE = b"release"
_ACTION_CREATE = b"create"
_ACTION_SWAP = b"swap"
_ACTION_DELETE = b"delete"
_ACTION_INCREMENT = b"increment"
_ACTION_APPEND_STREAM = b"append-stream"
_REPLY_CREATED = b"created"
_REPLY_UPDATED = b"updated"
_REPLY_DELETED = b"deleted"
_REPLY_COUNTER = b"counter"
_REPLY_LEASE = b"lease"
_REPLY_NONE = b"none"
_REPLY_CONFLICT = b"conflict"
_REPLY_FAILURE = b"failure"
_REPLY_STREAM_POSITION = b"stream-position"
_STATE_GUARD = b"guard"
_STATE_LEASE = b"lease"
_STATE_VALUE = b"value"
_STATE_COUNTER = b"counter"
_STATE_TOMBSTONE = b"tombstone"
_STATE_OPERATION = b"operation"
_INTERNAL_LEASE_OWNER = "nats-provider"


@dataclass(frozen=True, slots=True)
class _StateRecord:
    sequence: int
    data: bytes
    timestamp: float


@dataclass(frozen=True, slots=True)
class _LeaseGuard:
    subject: str
    sequence: int


class _OperationReplayRequired(CacheFeatureError):
    """A committed command must redeliver before its outcome is acknowledged."""


@dataclass(slots=True)
class NatsCoordination:
    """Serialise atomic and lease mutations through a private JetStream worker."""

    runtime: NatsJetStreamRuntime
    _start_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _close_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)
    _subscription: Any = field(default=None, init=False, repr=False)
    _worker: asyncio.Task[None] | None = field(default=None, init=False, repr=False)
    _closed: bool = field(default=False, init=False, repr=False)

    async def start(self) -> None:
        if self._worker is not None:
            return
        async with self._start_lock:
            if self._worker is not None:
                return
            if self._closed:
                raise CacheFeatureError("The NATS JetStream cache backend is closed.")
            await self.runtime.ensure_coordination_streams()
            self._subscription = await self.runtime.pull_subscribe(
                self.runtime.coordination_command_subject,
                durable=self._durable_name,
                stream=self.runtime.coordination_command_stream_name,
                ack_wait=_REPLY_TIMEOUT_SECONDS,
                max_deliver=_COORDINATOR_MAX_DELIVERY_ATTEMPTS,
                max_ack_pending=1,
            )
            self._worker = asyncio.create_task(self._run(), name=self._durable_name)

    async def close(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            worker = self._worker
            if worker is not None:
                worker.cancel()
                with suppress(asyncio.CancelledError):
                    await worker
            subscription = self._subscription
            if subscription is not None:
                with suppress(Exception):
                    await subscription.unsubscribe()
            self._worker = None
            self._subscription = None
            self._closed = True

    async def get(self, owner: str, key: str) -> AtomicCacheValue | None:
        owner = validate_resource(owner, label="cache owner")
        key = validate_resource(key, label="cache key")
        current = await self._read_state(self._state_subject("atomic", owner, key))
        if current is None:
            return None
        state = _unpack(current.data)
        if not state:
            raise CacheFeatureError("NATS JetStream atomic state is invalid.")
        if state[0] == _STATE_TOMBSTONE:
            return None
        if await self._feature_state_expired(state, current.timestamp):
            return None
        if state[0] == _STATE_COUNTER:
            raise CacheConflictError("The requested atomic key contains a counter.")
        if len(state) != 4 or state[0] != _STATE_VALUE:
            raise CacheFeatureError("NATS JetStream atomic state is invalid.")
        return AtomicCacheValue(state[2], CacheRevision(current.sequence))

    async def create(
        self,
        owner: str,
        key: str,
        value: bytes,
        *,
        ttl: float,
        lease: LeaseToken | None = None,
    ) -> AtomicCacheValue | None:
        owner = validate_resource(owner, label="cache owner")
        key = validate_resource(key, label="cache key")
        value = validate_payload(value)
        ttl = validate_positive_finite(ttl, label="atomic value TTL")
        reply = await self._request(
            _ACTION_CREATE,
            self._state_subject("atomic", owner, key).encode(),
            value,
            repr(ttl).encode(),
            *self._lease_parts(lease),
        )
        if reply[0] == _REPLY_NONE:
            return None
        if reply[0] != _REPLY_CREATED or len(reply) != 2:
            _raise_reply_error(reply, label="atomic")
        return AtomicCacheValue(value, CacheRevision(_reply_integer(reply[1])))

    async def compare_and_swap(
        self,
        owner: str,
        key: str,
        expected: CacheRevision,
        value: bytes,
        *,
        ttl: float,
        lease: LeaseToken | None = None,
    ) -> AtomicCacheValue | None:
        owner = validate_resource(owner, label="cache owner")
        key = validate_resource(key, label="cache key")
        expected = _revision(expected)
        value = validate_payload(value)
        ttl = validate_positive_finite(ttl, label="atomic value TTL")
        reply = await self._request(
            _ACTION_SWAP,
            self._state_subject("atomic", owner, key).encode(),
            str(expected.value).encode(),
            value,
            repr(ttl).encode(),
            *self._lease_parts(lease),
        )
        if reply[0] == _REPLY_NONE:
            return None
        if reply[0] != _REPLY_UPDATED or len(reply) != 2:
            _raise_reply_error(reply, label="atomic")
        return AtomicCacheValue(value, CacheRevision(_reply_integer(reply[1])))

    async def compare_and_delete(
        self,
        owner: str,
        key: str,
        expected: CacheRevision,
        *,
        lease: LeaseToken | None = None,
    ) -> bool:
        owner = validate_resource(owner, label="cache owner")
        key = validate_resource(key, label="cache key")
        expected = _revision(expected)
        reply = await self._request(
            _ACTION_DELETE,
            self._state_subject("atomic", owner, key).encode(),
            str(expected.value).encode(),
            *self._lease_parts(lease),
        )
        if reply[0] == _REPLY_NONE:
            return False
        if reply[0] != _REPLY_DELETED or len(reply) != 1:
            _raise_reply_error(reply, label="atomic")
        return True

    async def increment(
        self,
        owner: str,
        key: str,
        *,
        amount: int = 1,
        ttl: float,
    ) -> CounterCacheValue:
        owner = validate_resource(owner, label="cache owner")
        key = validate_resource(key, label="cache key")
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise TypeError("Counter amount must be an integer.")
        ttl = validate_positive_finite(ttl, label="counter TTL")
        reply = await self._request(
            _ACTION_INCREMENT,
            self._state_subject("atomic", owner, key).encode(),
            str(amount).encode(),
            repr(ttl).encode(),
        )
        if reply[0] != _REPLY_COUNTER or len(reply) != 3:
            _raise_reply_error(reply, label="counter")
        return CounterCacheValue(
            _integer(reply[1], label="reply"),
            CacheRevision(_reply_integer(reply[2])),
        )

    async def acquire(
        self,
        owner: str,
        resource: str,
        holder: str,
        *,
        ttl: float,
    ) -> LeaseToken | None:
        owner = validate_resource(owner, label="cache owner")
        resource = validate_resource(resource, label="lease resource")
        holder = validate_resource(holder, label="lease holder")
        ttl = validate_positive_finite(ttl, label="lease TTL")
        reply = await self._request(
            _ACTION_ACQUIRE,
            self._state_subject("lease", owner, resource).encode(),
            holder.encode(),
            repr(ttl).encode(),
        )
        if reply[0] == _REPLY_NONE:
            return None
        if reply[0] != _REPLY_LEASE or len(reply) != 4:
            _raise_reply_error(reply, label="lease")
        return _lease_from_reply(owner, resource, holder, reply)

    async def renew(self, lease: LeaseToken, *, ttl: float) -> LeaseToken:
        lease = validate_lease_token(lease)
        ttl = validate_positive_finite(ttl, label="lease TTL")
        reply = await self._request(
            _ACTION_RENEW,
            self._state_subject("lease", lease.owner, lease.resource).encode(),
            lease.holder.encode(),
            lease.token.encode(),
            str(lease.fencing_token.value).encode(),
            repr(ttl).encode(),
        )
        if reply[0] != _REPLY_LEASE or len(reply) != 4:
            _raise_reply_error(reply, label="lease")
        return _lease_from_reply(lease.owner, lease.resource, lease.holder, reply)

    async def release(self, lease: LeaseToken) -> None:
        lease = validate_lease_token(lease)
        reply = await self._request(
            _ACTION_RELEASE,
            self._state_subject("lease", lease.owner, lease.resource).encode(),
            lease.holder.encode(),
            lease.token.encode(),
            str(lease.fencing_token.value).encode(),
        )
        if reply[0] != _REPLY_DELETED or len(reply) != 1:
            _raise_reply_error(reply, label="lease")

    async def acquire_internal(
        self,
        resource: str,
        holder: str,
        *,
        ttl: float,
    ) -> LeaseToken | None:
        """Acquire a provider-reserved coordination lease."""
        resource = validate_resource(resource, label="internal lease resource")
        holder = validate_resource(holder, label="internal lease holder")
        ttl = validate_positive_finite(ttl, label="internal lease TTL")
        reply = await self._request(
            _ACTION_ACQUIRE,
            self._state_subject("internal-lease", b"", resource).encode(),
            holder.encode(),
            repr(ttl).encode(),
        )
        if reply[0] == _REPLY_NONE:
            return None
        if reply[0] != _REPLY_LEASE or len(reply) != 4:
            _raise_reply_error(reply, label="internal lease")
        return _lease_from_reply(_INTERNAL_LEASE_OWNER, resource, holder, reply)

    async def release_internal(self, lease: LeaseToken) -> None:
        """Release a provider-reserved coordination lease."""
        lease = validate_lease_token(lease)
        reply = await self._request(
            _ACTION_RELEASE,
            self._state_subject("internal-lease", b"", lease.resource).encode(),
            lease.holder.encode(),
            lease.token.encode(),
            str(lease.fencing_token.value).encode(),
        )
        if reply[0] != _REPLY_DELETED or len(reply) != 1:
            _raise_reply_error(reply, label="internal lease")

    async def increment_internal(
        self,
        key: str,
        *,
        amount: int = 1,
        ttl: float,
    ) -> CounterCacheValue:
        """Increment a provider-reserved coordination counter."""
        key = validate_resource(key, label="internal counter key")
        if isinstance(amount, bool) or not isinstance(amount, int):
            raise TypeError("Internal counter amount must be an integer.")
        ttl = validate_positive_finite(ttl, label="internal counter TTL")
        reply = await self._request(
            _ACTION_INCREMENT,
            self._state_subject("internal-counter", b"", key).encode(),
            str(amount).encode(),
            repr(ttl).encode(),
        )
        if reply[0] != _REPLY_COUNTER or len(reply) != 3:
            _raise_reply_error(reply, label="internal counter")
        return CounterCacheValue(
            _reply_integer(reply[1]), CacheRevision(_reply_integer(reply[2]))
        )

    async def append_stream(
        self,
        stream_name: str,
        subject: str,
        payload: bytes,
        *,
        lease: LeaseToken | None = None,
    ) -> int:
        payload = validate_payload(payload)
        reply = await self._request(
            _ACTION_APPEND_STREAM,
            stream_name.encode(),
            subject.encode(),
            payload,
            *self._lease_parts(lease),
        )
        if reply[0] != _REPLY_STREAM_POSITION or len(reply) != 2:
            _raise_reply_error(reply, label="stream")
        return _reply_integer(reply[1])

    @property
    def _durable_name(self) -> str:
        return f"WYBRA_{self.runtime.namespace.upper()}_{_COORDINATOR_DURABLE_SUFFIX}"

    @property
    def _guard_subject(self) -> str:
        return f"{self.runtime.coordination_state_subject_prefix}.guard"

    async def _request(self, action: bytes, *parts: bytes) -> tuple[bytes, ...]:
        await self.start()
        client = await self.runtime.nats_client()
        inbox = client.new_inbox()
        subscription = await client.subscribe(inbox)
        operation = uuid4().bytes
        payload = _pack(action, operation, inbox.encode(), *parts)
        try:
            for _attempt in range(_COORDINATOR_MAX_DELIVERY_ATTEMPTS):
                await client.publish(
                    self.runtime.coordination_command_subject,
                    payload,
                )
                await client.flush()
                deadline = monotonic() + _REPLY_TIMEOUT_SECONDS
                while True:
                    remaining = deadline - monotonic()
                    if remaining <= 0:
                        break
                    try:
                        message = await subscription.next_msg(timeout=remaining)
                    except TimeoutError:
                        break
                    if not message.data.startswith(_FRAME_MAGIC):
                        continue
                    reply = _unpack(message.data)
                    if not reply or reply[0] != operation:
                        continue
                    return reply[1:]
            recovered = await self._operation_reply(operation)
            if recovered is not None:
                return recovered
            raise CacheFeatureError("NATS JetStream feature operation timed out.")
        except asyncio.CancelledError:
            raise
        except CacheFeatureError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream feature request failed (%s).", type(exc).__name__
            )
            raise CacheFeatureError(
                "NATS JetStream feature operation failed."
            ) from None
        finally:
            with suppress(Exception):
                await subscription.unsubscribe()

    async def _run(self) -> None:
        subscription = self._subscription
        if subscription is None:
            return
        while True:
            try:
                messages = await subscription.fetch(
                    1, timeout=_COORDINATOR_FETCH_TIMEOUT_SECONDS
                )
            except asyncio.CancelledError:
                raise
            except TimeoutError:
                continue
            except Exception as exc:
                logger.warning(
                    "NATS JetStream feature coordinator receive failed (%s).",
                    type(exc).__name__,
                )
                await asyncio.sleep(_COORDINATOR_FETCH_TIMEOUT_SECONDS)
                continue
            for message in messages:
                operation = _operation_id(message.data)
                reply_to = _reply_subject(message.data)
                try:
                    operation, reply_to, reply = await self._apply(message.data)
                except asyncio.CancelledError:
                    raise
                except _OperationReplayRequired:
                    continue
                except CacheConflictError:
                    reply = (_REPLY_CONFLICT,)
                except CacheFeatureError as exc:
                    logger.warning(
                        "NATS JetStream feature coordinator failed (%s).",
                        type(exc).__name__,
                    )
                    reply = (_REPLY_FAILURE,)
                except Exception as exc:
                    logger.warning(
                        "NATS JetStream feature coordinator failed (%s).",
                        type(exc).__name__,
                    )
                    reply = (_REPLY_FAILURE,)
                if operation is not None and reply_to is not None:
                    try:
                        client = await self.runtime.nats_client()
                        await client.publish(reply_to, _pack(operation, *reply))
                        await client.flush()
                    except asyncio.CancelledError:
                        raise
                    except Exception as exc:
                        logger.warning(
                            "NATS JetStream feature coordinator reply failed (%s).",
                            type(exc).__name__,
                        )
                try:
                    await message.ack()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "NATS JetStream feature coordinator acknowledgement failed "
                        "(%s).",
                        type(exc).__name__,
                    )

    async def _apply(self, payload: bytes) -> tuple[bytes, str, tuple[bytes, ...]]:
        action, operation, reply_value, *parts = _unpack(payload)
        reply_to = _inbox(reply_value)
        previous_reply = await self._operation_reply(operation)
        if previous_reply is not None:
            return operation, reply_to, previous_reply
        guard_sequence = await self._claim_guard(operation)
        if action == _ACTION_ACQUIRE:
            reply = await self._apply_acquire(operation, guard_sequence, parts)
        elif action == _ACTION_RENEW:
            reply = await self._apply_renew(operation, guard_sequence, parts)
        elif action == _ACTION_RELEASE:
            reply = await self._apply_release(operation, guard_sequence, parts)
        elif action == _ACTION_CREATE:
            reply = await self._apply_create(operation, guard_sequence, parts)
        elif action == _ACTION_SWAP:
            reply = await self._apply_swap(operation, guard_sequence, parts)
        elif action == _ACTION_DELETE:
            reply = await self._apply_delete(operation, guard_sequence, parts)
        elif action == _ACTION_INCREMENT:
            reply = await self._apply_increment(operation, guard_sequence, parts)
        elif action == _ACTION_APPEND_STREAM:
            reply = await self._apply_append_stream(operation, parts)
        else:
            raise CacheFeatureError("NATS JetStream feature command is invalid.")
        try:
            await self._write_operation_reply(operation, reply)
        except CacheFeatureError as exc:
            raise _OperationReplayRequired(
                "NATS JetStream feature outcome requires replay."
            ) from exc
        return operation, reply_to, reply

    async def _apply_acquire(
        self, operation: bytes, guard: int, parts: list[bytes]
    ) -> tuple[bytes, ...]:
        if len(parts) != 3:
            raise CacheFeatureError("NATS JetStream lease command is invalid.")
        subject_value, holder, ttl_value = parts
        ttl = _positive_ttl(ttl_value, label="lease TTL")
        subject = self._command_state_subject(subject_value, "lease", "internal-lease")
        current = await self._read_state(subject)
        if current is not None:
            state = _unpack(current.data)
            live = await self._lease_live(state, current.timestamp)
            if state[0] == _STATE_LEASE and state[1] == operation and live:
                return _lease_reply(state, current.timestamp)
            if state[0] == _STATE_LEASE and live:
                return (_REPLY_NONE,)
        record = _pack(
            _STATE_LEASE,
            operation,
            holder,
            uuid4().hex.encode(),
            str(guard).encode(),
            repr(ttl).encode(),
        )
        written = await self._write_state(
            subject, record, ttl=ttl, guard_sequence=guard
        )
        return _lease_reply(_unpack(written.data), written.timestamp)

    async def _apply_renew(
        self, operation: bytes, guard: int, parts: list[bytes]
    ) -> tuple[bytes, ...]:
        if len(parts) != 5:
            raise CacheFeatureError("NATS JetStream lease command is invalid.")
        subject_value, holder, token, fencing_token, ttl_value = parts
        ttl = _positive_ttl(ttl_value, label="lease TTL")
        subject = self._command_state_subject(subject_value, "lease", "internal-lease")
        current = await self._read_state(subject)
        if current is None:
            raise CacheConflictError("Lease is stale or no longer held.")
        state = _unpack(current.data)
        live = await self._lease_live(state, current.timestamp)
        if state[0] == _STATE_LEASE and state[1] == operation and live:
            return _lease_reply(state, current.timestamp)
        if not live:
            raise CacheConflictError("Lease is stale or no longer held.")
        _require_lease_state(state, holder, token, fencing_token)
        record = _pack(
            _STATE_LEASE, operation, holder, token, fencing_token, repr(ttl).encode()
        )
        lease_guard = _LeaseGuard(subject, current.sequence)
        written = await self._write_state(
            subject,
            record,
            ttl=ttl,
            guard_sequence=guard,
            lease_guard=lease_guard,
        )
        return _lease_reply(_unpack(written.data), written.timestamp)

    async def _apply_release(
        self, operation: bytes, guard: int, parts: list[bytes]
    ) -> tuple[bytes, ...]:
        if len(parts) != 4:
            raise CacheFeatureError("NATS JetStream lease command is invalid.")
        subject_value, holder, token, fencing_token = parts
        subject = self._command_state_subject(subject_value, "lease", "internal-lease")
        current = await self._read_state(subject)
        if current is None:
            raise CacheConflictError("Lease is stale or no longer held.")
        state = _unpack(current.data)
        if state[0] == _STATE_TOMBSTONE and len(state) == 2 and state[1] == operation:
            return (_REPLY_DELETED,)
        if not await self._lease_live(state, current.timestamp):
            raise CacheConflictError("Lease is stale or no longer held.")
        _require_lease_state(state, holder, token, fencing_token)
        await self._write_tombstone(
            subject,
            operation,
            guard,
            lease_guard=_LeaseGuard(subject, current.sequence),
        )
        return (_REPLY_DELETED,)

    async def _apply_create(
        self, operation: bytes, guard: int, parts: list[bytes]
    ) -> tuple[bytes, ...]:
        if len(parts) != 7:
            raise CacheFeatureError("NATS JetStream atomic command is invalid.")
        subject_value, value, ttl_value, *lease_values = parts
        ttl = _positive_ttl(ttl_value, label="atomic value TTL")
        subject = self._command_state_subject(subject_value, "atomic")
        current = await self._read_state(subject)
        if current is not None:
            state = _unpack(current.data)
            if await self._feature_state_expired(state, current.timestamp):
                current = None
            elif state[0] == _STATE_VALUE and state[1] == operation:
                return (_REPLY_CREATED, str(current.sequence).encode())
            elif state[0] != _STATE_TOMBSTONE:
                return (_REPLY_NONE,)
        lease_guard = await self._require_lease(lease_values)
        written = await self._write_state(
            subject,
            _pack(_STATE_VALUE, operation, value, repr(ttl).encode()),
            ttl=ttl,
            guard_sequence=guard,
            lease_guard=lease_guard,
        )
        return (_REPLY_CREATED, str(written.sequence).encode())

    async def _apply_swap(
        self, operation: bytes, guard: int, parts: list[bytes]
    ) -> tuple[bytes, ...]:
        if len(parts) != 8:
            raise CacheFeatureError("NATS JetStream atomic command is invalid.")
        subject_value, expected, value, ttl_value, *lease_values = parts
        ttl = _positive_ttl(ttl_value, label="atomic value TTL")
        subject = self._command_state_subject(subject_value, "atomic")
        current = await self._read_state(subject)
        if current is None:
            return (_REPLY_NONE,)
        state = _unpack(current.data)
        if await self._feature_state_expired(state, current.timestamp):
            return (_REPLY_NONE,)
        if state[0] == _STATE_VALUE and state[1] == operation:
            return (_REPLY_UPDATED, str(current.sequence).encode())
        if state[0] == _STATE_COUNTER:
            raise CacheConflictError("The requested atomic key contains a counter.")
        if (
            len(state) != 4
            or state[0] != _STATE_VALUE
            or current.sequence != _command_integer(expected)
        ):
            return (_REPLY_NONE,)
        lease_guard = await self._require_lease(lease_values)
        written = await self._write_state(
            subject,
            _pack(_STATE_VALUE, operation, value, repr(ttl).encode()),
            ttl=ttl,
            guard_sequence=guard,
            lease_guard=lease_guard,
        )
        return (_REPLY_UPDATED, str(written.sequence).encode())

    async def _apply_delete(
        self, operation: bytes, guard: int, parts: list[bytes]
    ) -> tuple[bytes, ...]:
        if len(parts) != 6:
            raise CacheFeatureError("NATS JetStream atomic command is invalid.")
        subject_value, expected, *lease_values = parts
        subject = self._command_state_subject(subject_value, "atomic")
        current = await self._read_state(subject)
        if current is None:
            return (_REPLY_NONE,)
        state = _unpack(current.data)
        if await self._feature_state_expired(state, current.timestamp):
            return (_REPLY_NONE,)
        if state[0] == _STATE_TOMBSTONE and len(state) == 2 and state[1] == operation:
            return (_REPLY_DELETED,)
        if state[0] == _STATE_COUNTER:
            raise CacheConflictError("The requested atomic key contains a counter.")
        if state[0] != _STATE_VALUE or current.sequence != _command_integer(expected):
            return (_REPLY_NONE,)
        lease_guard = await self._require_lease(lease_values)
        await self._write_tombstone(
            subject,
            operation,
            guard,
            lease_guard=lease_guard,
        )
        return (_REPLY_DELETED,)

    async def _apply_increment(
        self, operation: bytes, guard: int, parts: list[bytes]
    ) -> tuple[bytes, ...]:
        if len(parts) != 3:
            raise CacheFeatureError("NATS JetStream counter command is invalid.")
        subject_value, amount_value, ttl_value = parts
        amount = _command_integer(amount_value)
        ttl = _positive_ttl(ttl_value, label="counter TTL")
        subject = self._command_state_subject(
            subject_value, "atomic", "internal-counter"
        )
        current = await self._read_state(subject)
        value = 0
        if current is not None:
            state = _unpack(current.data)
            if await self._feature_state_expired(state, current.timestamp):
                state = ()
            elif state[0] == _STATE_COUNTER and state[1] == operation:
                return (_REPLY_COUNTER, state[2], str(current.sequence).encode())
            if state and state[0] == _STATE_VALUE:
                raise CacheConflictError(
                    "The requested counter key contains an atomic byte value."
                )
            if state and state[0] == _STATE_COUNTER:
                if len(state) != 4:
                    raise CacheFeatureError("NATS JetStream counter state is invalid.")
                value = _command_integer(state[2])
        value += amount
        written = await self._write_state(
            subject,
            _pack(_STATE_COUNTER, operation, str(value).encode(), repr(ttl).encode()),
            ttl=ttl,
            guard_sequence=guard,
        )
        return (_REPLY_COUNTER, str(value).encode(), str(written.sequence).encode())

    async def _apply_append_stream(
        self, operation: bytes, parts: list[bytes]
    ) -> tuple[bytes, ...]:
        if len(parts) != 7:
            raise CacheFeatureError("NATS JetStream stream command is invalid.")
        stream_name, subject, payload, *lease_values = parts
        stream = _stream_name(stream_name)
        stream_subject = _stream_subject(
            subject,
            prefix=f"{self.runtime.subject_prefix}.streams.",
        )
        await self._require_lease(lease_values)
        jetstream = await self.runtime.coordination_jetstream()
        try:
            acknowledgement = await jetstream.publish(
                stream_subject,
                payload,
                stream=stream,
                headers={"Nats-Msg-Id": operation.hex()},
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream stream append failed (%s).", type(exc).__name__
            )
            raise CacheFeatureError("NATS JetStream stream append failed.") from None
        sequence = getattr(acknowledgement, "seq", None)
        if not isinstance(sequence, int) or sequence <= 0:
            raise CacheFeatureError(
                "NATS JetStream stream append returned invalid state."
            )
        return (_REPLY_STREAM_POSITION, str(sequence).encode())

    async def _require_lease(self, values: list[bytes]) -> _LeaseGuard | None:
        if len(values) != 4:
            raise CacheFeatureError("NATS JetStream lease command is invalid.")
        subject_value, holder, token, fencing_token = values
        if not subject_value:
            return None
        subject = self._command_state_subject(subject_value, "lease")
        record = await self._read_state(subject)
        if record is None:
            raise CacheConflictError("Lease is stale or no longer held.")
        state = _unpack(record.data)
        if not await self._lease_live(state, record.timestamp):
            raise CacheConflictError("Lease is stale or no longer held.")
        _require_lease_state(state, holder, token, fencing_token)
        return _LeaseGuard(subject, record.sequence)

    async def _lease_live(self, state: tuple[bytes, ...], timestamp: float) -> bool:
        if len(state) != 6 or state[0] != _STATE_LEASE:
            return False
        return timestamp + _positive_ttl(state[5], label="lease TTL") > (
            await self.runtime.refresh_time()
        )

    async def _feature_state_expired(
        self,
        state: tuple[bytes, ...],
        timestamp: float,
    ) -> bool:
        if not state:
            raise CacheFeatureError("NATS JetStream atomic state is invalid.")
        if state[0] not in {_STATE_VALUE, _STATE_COUNTER}:
            return False
        if len(state) != 4:
            raise CacheFeatureError("NATS JetStream atomic state is invalid.")
        return timestamp + _positive_ttl(state[3], label="feature TTL") <= (
            await self.runtime.refresh_time()
        )

    async def _claim_guard(self, operation: bytes) -> int:
        jetstream = await self.runtime.coordination_jetstream()
        for _attempt in range(8):
            current = await self._read_state(self._guard_subject)
            expected = "0" if current is None else str(current.sequence)
            try:
                acknowledgement = await jetstream.publish(
                    self._guard_subject,
                    _pack(_STATE_GUARD, operation),
                    stream=self.runtime.coordination_state_stream_name,
                    headers={"Nats-Expected-Last-Subject-Sequence": expected},
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                continue
            sequence = getattr(acknowledgement, "seq", None)
            if isinstance(sequence, int) and sequence > 0:
                return sequence
        raise CacheFeatureError(
            "NATS JetStream feature coordinator could not claim work."
        )

    async def _read_state(self, subject: str) -> _StateRecord | None:
        jetstream = await self.runtime.coordination_jetstream()
        try:
            message = await jetstream.get_last_msg(
                self.runtime.coordination_state_stream_name, subject, direct=True
            )
        except self.runtime.not_found_error():
            return None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream feature state read failed (%s).", type(exc).__name__
            )
            raise CacheFeatureError(
                "NATS JetStream feature operation failed."
            ) from None
        sequence = getattr(message, "seq", None)
        timestamp = getattr(message, "time", None)
        data = getattr(message, "data", None)
        if (
            not isinstance(sequence, int)
            or sequence <= 0
            or not isinstance(data, bytes)
            or not isinstance(timestamp, datetime)
        ):
            raise CacheFeatureError("NATS JetStream feature state is invalid.")
        return _StateRecord(sequence, data, timestamp.timestamp())

    async def _write_tombstone(
        self,
        subject: str,
        operation: bytes,
        guard: int,
        *,
        lease_guard: _LeaseGuard | None = None,
    ) -> _StateRecord:
        return await self._write_state(
            subject,
            _pack(_STATE_TOMBSTONE, operation),
            ttl=1.0,
            guard_sequence=guard,
            lease_guard=lease_guard,
        )

    async def _write_state(
        self,
        subject: str,
        data: bytes,
        *,
        ttl: float,
        guard_sequence: int,
        lease_guard: _LeaseGuard | None = None,
    ) -> _StateRecord:
        jetstream = await self.runtime.coordination_jetstream()
        await self.runtime.refresh_time()
        headers = {"Nats-TTL": nats_feature_ttl_header(ttl)}
        if lease_guard is None:
            headers.update(
                {
                    "Nats-Expected-Last-Subject-Sequence": str(guard_sequence),
                    "Nats-Expected-Last-Subject-Sequence-Subject": self._guard_subject,
                }
            )
        else:
            headers.update(_lease_guard_headers(lease_guard))
        try:
            acknowledgement = await jetstream.publish(
                subject,
                data,
                stream=self.runtime.coordination_state_stream_name,
                headers=headers,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream feature state write failed (%s).", type(exc).__name__
            )
            try:
                recovered = await self._read_state(subject)
            except CacheFeatureError:
                raise _OperationReplayRequired(
                    "NATS JetStream feature state write requires replay."
                ) from None
            if recovered is not None and recovered.data == data:
                return recovered
            raise CacheFeatureError(
                "NATS JetStream feature operation failed."
            ) from None
        sequence = getattr(acknowledgement, "seq", None)
        if not isinstance(sequence, int) or sequence <= 0:
            raise CacheFeatureError("NATS JetStream feature state is invalid.")
        return _StateRecord(sequence, data, self.runtime.cache_time())

    async def _operation_reply(self, operation: bytes) -> tuple[bytes, ...] | None:
        current = await self._read_state(self._operation_subject(operation))
        if current is None:
            return None
        state = _unpack(current.data)
        if not state or state[0] != _STATE_OPERATION:
            raise CacheFeatureError("NATS JetStream operation result is invalid.")
        return state[1:]

    async def _write_operation_reply(
        self,
        operation: bytes,
        reply: tuple[bytes, ...],
    ) -> None:
        subject = self._operation_subject(operation)
        data = _pack(_STATE_OPERATION, *reply)
        jetstream = await self.runtime.coordination_jetstream()
        try:
            acknowledgement = await jetstream.publish(
                subject,
                data,
                stream=self.runtime.coordination_state_stream_name,
                headers={
                    "Nats-TTL": nats_feature_ttl_header(_OPERATION_RESULT_TTL_SECONDS)
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream operation result write failed (%s).",
                type(exc).__name__,
            )
            raise CacheFeatureError(
                "NATS JetStream feature operation failed."
            ) from None
        sequence = getattr(acknowledgement, "seq", None)
        if not isinstance(sequence, int) or sequence <= 0:
            raise CacheFeatureError("NATS JetStream operation result is invalid.")

    def _state_subject(self, kind: str, owner: str | bytes, key: str | bytes) -> str:
        owner_value = owner.encode() if isinstance(owner, str) else owner
        key_value = key.encode() if isinstance(key, str) else key
        digest = sha256(
            kind.encode() + b"\0" + owner_value + b"\0" + key_value
        ).hexdigest()
        return f"{self.runtime.coordination_state_subject_prefix}.{kind}.{digest}"

    def _operation_subject(self, operation: bytes) -> str:
        if len(operation) != 16:
            raise CacheFeatureError("NATS JetStream feature command is invalid.")
        return (
            f"{self.runtime.coordination_state_subject_prefix}.operation."
            f"{operation.hex()}"
        )

    def _command_state_subject(self, value: bytes, *kinds: str) -> str:
        try:
            subject = value.decode()
        except UnicodeDecodeError as exc:
            raise CacheFeatureError(
                "NATS JetStream feature command is invalid."
            ) from exc
        prefix = next(
            (
                f"{self.runtime.coordination_state_subject_prefix}.{kind}."
                for kind in kinds
                if subject.startswith(
                    f"{self.runtime.coordination_state_subject_prefix}.{kind}."
                )
            ),
            None,
        )
        if prefix is None:
            raise CacheFeatureError("NATS JetStream feature command is invalid.")
        digest = subject.removeprefix(prefix)
        if len(digest) != 64:
            raise CacheFeatureError("NATS JetStream feature command is invalid.")
        try:
            int(digest, 16)
        except ValueError as exc:
            raise CacheFeatureError(
                "NATS JetStream feature command is invalid."
            ) from exc
        return subject

    def _lease_parts(
        self, lease: LeaseToken | None
    ) -> tuple[bytes, bytes, bytes, bytes]:
        if lease is None:
            return (b"", b"", b"", b"")
        lease = validate_lease_token(lease)
        return (
            self._state_subject("lease", lease.owner, lease.resource).encode(),
            lease.holder.encode(),
            lease.token.encode(),
            str(lease.fencing_token.value).encode(),
        )


def _lease_from_reply(
    owner: str, resource: str, holder: str, reply: tuple[bytes, ...]
) -> LeaseToken:
    return LeaseToken(
        owner=owner,
        resource=resource,
        holder=holder,
        fencing_token=FencingToken(_reply_integer(reply[1])),
        expires_at=_reply_float(reply[2]),
        token=_reply_text(reply[3]),
    )


def _stream_name(value: bytes) -> str:
    try:
        decoded = value.decode("ascii")
    except UnicodeDecodeError as exc:
        raise CacheFeatureError("NATS JetStream stream command is invalid.") from exc
    if not decoded.startswith("WYBRA_STREAM_"):
        raise CacheFeatureError("NATS JetStream stream command is invalid.")
    return decoded


def _stream_subject(value: bytes, *, prefix: str) -> str:
    try:
        subject = value.decode("ascii")
    except UnicodeDecodeError as exc:
        raise CacheFeatureError("NATS JetStream stream command is invalid.") from exc
    digest = subject.removeprefix(prefix)
    if not subject.startswith(prefix) or len(digest) != 64:
        raise CacheFeatureError("NATS JetStream stream command is invalid.")
    try:
        int(digest, 16)
    except ValueError as exc:
        raise CacheFeatureError("NATS JetStream stream command is invalid.") from exc
    return subject


def _lease_reply(state: tuple[bytes, ...], timestamp: float) -> tuple[bytes, ...]:
    if len(state) != 6 or state[0] != _STATE_LEASE:
        raise CacheFeatureError("NATS JetStream lease state is invalid.")
    return (
        _REPLY_LEASE,
        state[4],
        repr(timestamp + _positive_ttl(state[5], label="lease TTL")).encode(),
        state[3],
    )


def _lease_guard_headers(lease_guard: _LeaseGuard) -> dict[str, str]:
    return {
        "Nats-Expected-Last-Subject-Sequence": str(lease_guard.sequence),
        "Nats-Expected-Last-Subject-Sequence-Subject": lease_guard.subject,
    }


def _require_lease_state(
    state: tuple[bytes, ...], holder: bytes, token: bytes, fencing_token: bytes
) -> None:
    if (
        len(state) != 6
        or state[0] != _STATE_LEASE
        or state[2] != holder
        or state[3] != token
        or state[4] != fencing_token
    ):
        raise CacheConflictError("Lease is stale or no longer held.")


def _revision(value: CacheRevision) -> CacheRevision:
    if not isinstance(value, CacheRevision):
        raise TypeError("Expected revision must be a CacheRevision.")
    return value


def _positive_ttl(value: bytes, *, label: str) -> float:
    try:
        return validate_positive_finite(float(value), label=label)
    except (TypeError, ValueError) as exc:
        raise CacheFeatureError("NATS JetStream feature command is invalid.") from exc


def _reply_integer(value: bytes) -> int:
    result = _integer(value, label="reply")
    if result <= 0:
        raise CacheFeatureError("NATS JetStream feature reply is invalid.")
    return result


def _command_integer(value: bytes) -> int:
    return _integer(value, label="command")


def _integer(value: bytes, *, label: str) -> int:
    try:
        return int(value)
    except ValueError as exc:
        raise CacheFeatureError(f"NATS JetStream feature {label} is invalid.") from exc


def _reply_float(value: bytes) -> float:
    try:
        return float(value)
    except ValueError as exc:
        raise CacheFeatureError("NATS JetStream feature reply is invalid.") from exc


def _reply_text(value: bytes) -> str:
    try:
        return value.decode()
    except UnicodeDecodeError as exc:
        raise CacheFeatureError("NATS JetStream feature reply is invalid.") from exc


def _raise_reply_error(reply: tuple[bytes, ...], *, label: str) -> None:
    if reply and reply[0] == _REPLY_CONFLICT:
        if label == "lease":
            raise CacheConflictError("Lease is stale or no longer held.")
        raise CacheConflictError(
            f"NATS JetStream {label} operation conflicts with current state."
        )
    raise CacheFeatureError(f"NATS JetStream {label} operation failed.")


def _operation_id(payload: bytes) -> bytes | None:
    try:
        parts = _unpack(payload)
    except CacheFeatureError:
        return None
    return parts[1] if len(parts) > 1 else None


def _reply_subject(payload: bytes) -> str | None:
    try:
        parts = _unpack(payload)
        return _inbox(parts[2]) if len(parts) > 2 else None
    except CacheFeatureError:
        return None


def _inbox(value: bytes) -> str:
    try:
        subject = value.decode()
    except UnicodeDecodeError as exc:
        raise CacheFeatureError("NATS JetStream feature command is invalid.") from exc
    if not subject.startswith("_INBOX."):
        raise CacheFeatureError("NATS JetStream feature command is invalid.")
    return subject


def _pack(*parts: bytes) -> bytes:
    if len(parts) > 255:
        raise CacheFeatureError("NATS JetStream feature message is invalid.")
    frame = bytearray(_FRAME_MAGIC)
    frame.append(len(parts))
    for part in parts:
        if not isinstance(part, bytes) or len(part) > 2**32 - 1:
            raise CacheFeatureError("NATS JetStream feature message is invalid.")
        frame.extend(len(part).to_bytes(4))
        frame.extend(part)
    return bytes(frame)


def _unpack(value: bytes) -> tuple[bytes, ...]:
    if (
        not isinstance(value, bytes)
        or len(value) < len(_FRAME_MAGIC) + 1
        or not value.startswith(_FRAME_MAGIC)
    ):
        raise CacheFeatureError("NATS JetStream feature message is invalid.")
    count = value[len(_FRAME_MAGIC)]
    offset = len(_FRAME_MAGIC) + 1
    parts: list[bytes] = []
    for _ in range(count):
        if offset + 4 > len(value):
            raise CacheFeatureError("NATS JetStream feature message is invalid.")
        size = int.from_bytes(value[offset : offset + 4])
        offset += 4
        end = offset + size
        if end > len(value):
            raise CacheFeatureError("NATS JetStream feature message is invalid.")
        parts.append(value[offset:end])
        offset = end
    if offset != len(value):
        raise CacheFeatureError("NATS JetStream feature message is invalid.")
    return tuple(parts)


__all__ = ("NatsCoordination",)
