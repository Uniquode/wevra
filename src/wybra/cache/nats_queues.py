from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass, field
from hashlib import sha256
from time import monotonic
from typing import Any
from uuid import uuid4

from wybra.cache.feature_models import (
    DEFAULT_STREAM_RETENTION_COUNT,
    MAX_CACHE_FEATURE_LIMIT,
    CacheConflictError,
    CacheFeatureError,
    CacheWorkQueueRejectedError,
    WorkDelivery,
    WorkIdentity,
    validate_limit,
    validate_non_negative_finite,
    validate_payload,
    validate_positive_finite,
    validate_positive_integer,
    validate_resource,
)
from wybra.cache.nats_runtime import NatsJetStreamRuntime, nats_feature_ttl_header

logger = logging.getLogger(__name__)
_NATIVE_ACK_WAIT_SECONDS = 0.001
_MAX_STATE_ATTEMPTS = 16
_MAX_WORK_ITEMS_PER_QUEUE = MAX_CACHE_FEATURE_LIMIT
_MAX_DEAD_LETTERS_PER_QUEUE = DEFAULT_STREAM_RETENTION_COUNT
_TERMINAL_STATE_TTL_SECONDS = 60.0
_STATE_ACTIVE = "active"
_STATE_READY = "ready"
_STATE_DELAYED = "delayed"
_STATE_RELEASED = "released"
_STATE_ACKNOWLEDGED = "acknowledged"
_STATE_DEAD_PENDING = "dead-pending"
_STATE_DEAD = "dead"
_WORK_ID_HEADER = "Wybra-Work-Identity"
_WORK_MAX_ATTEMPTS_HEADER = "Wybra-Work-Max-Attempts"
_WORK_DUE_AT_HEADER = "Wybra-Work-Due-At"
_WORK_DEAD_HEADER = "Wybra-Work-Dead-Letter"


@dataclass(frozen=True, slots=True)
class _QueueAddress:
    owner: str
    queue: str
    digest: str
    stream_name: str
    subject: str
    durable: str
    dead_stream_name: str
    dead_subject: str


@dataclass(frozen=True, slots=True)
class _StateRecord:
    sequence: int
    state: _WorkState


@dataclass(frozen=True, slots=True)
class _WorkState:
    identity: str
    maximum_attempts: int
    attempt: int
    status: str
    receipt: str | None = None
    deadline: float = 0.0

    def __post_init__(self) -> None:
        validate_resource(self.identity, label="work identity")
        validate_positive_integer(
            self.maximum_attempts,
            label="maximum delivery attempts",
        )
        if self.attempt < 0:
            raise ValueError("Work delivery attempt cannot be negative.")
        if self.status not in {
            _STATE_ACTIVE,
            _STATE_READY,
            _STATE_DELAYED,
            _STATE_RELEASED,
            _STATE_ACKNOWLEDGED,
            _STATE_DEAD_PENDING,
            _STATE_DEAD,
        }:
            raise ValueError("NATS JetStream work state is invalid.")
        if self.receipt is not None:
            validate_resource(self.receipt, label="delivery receipt")
        if not isinstance(self.deadline, int | float):
            raise TypeError("Work deadline must be a number.")


@dataclass(slots=True)
class _LocalDelivery:
    address: _QueueAddress
    message: Any = field(repr=False)
    delivery: WorkDelivery


@dataclass(slots=True)
class NatsWorkQueue:
    """JetStream work queues with provider-private conditional receipts."""

    runtime: NatsJetStreamRuntime
    _deliveries: dict[str, _LocalDelivery] = field(default_factory=dict, init=False)
    _ensure_locks: dict[str, asyncio.Lock] = field(default_factory=dict, init=False)
    _closed: bool = field(default=False, init=False)
    _close_lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False)

    async def publish(
        self,
        owner: str,
        queue: str,
        payload: bytes,
        *,
        delay: float = 0,
        max_attempts: int = 3,
    ) -> WorkIdentity:
        address = _address(self.runtime, owner, queue)
        payload = validate_payload(payload)
        delay = validate_non_negative_finite(delay, label="work delay")
        max_attempts = validate_positive_integer(
            max_attempts,
            label="maximum delivery attempts",
        )
        self._require_open()
        await self._ensure_queue(address)
        identity = WorkIdentity(uuid4().hex)
        due_at = (await self.runtime.refresh_time()) + delay
        jetstream = await self.runtime.coordination_jetstream()
        try:
            await jetstream.publish(
                address.subject,
                payload,
                stream=address.stream_name,
                headers={
                    "Nats-Msg-Id": identity.value,
                    _WORK_ID_HEADER: identity.value,
                    _WORK_MAX_ATTEMPTS_HEADER: str(max_attempts),
                    _WORK_DUE_AT_HEADER: repr(due_at),
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream work publication failed (%s).", type(exc).__name__
            )
            if _is_queue_capacity_error(exc):
                raise CacheWorkQueueRejectedError(
                    f"Queue {queue!r} has reached its configured item capacity."
                ) from None
            raise CacheFeatureError("NATS JetStream work publication failed.") from None
        return identity

    async def reserve(
        self,
        owner: str,
        queue: str,
        consumer: str,
        *,
        visibility_timeout: float,
        wait_timeout: float = 0,
    ) -> WorkDelivery | None:
        address = _address(self.runtime, owner, queue)
        validate_resource(consumer, label="queue consumer")
        visibility_timeout = validate_positive_finite(
            visibility_timeout,
            label="visibility timeout",
        )
        wait_timeout = validate_non_negative_finite(
            wait_timeout,
            label="queue wait timeout",
        )
        self._require_open()
        await self._ensure_queue(address)
        deadline = monotonic() + wait_timeout
        unavailable_sequences: set[int] = set()
        while True:
            remaining = deadline - monotonic()
            if wait_timeout == 0:
                remaining = 0.01
            elif remaining <= 0:
                return None
            message = await self._next_message(address, remaining)
            if message is None:
                return None
            sequence = _message_sequence(message)
            if wait_timeout == 0 and sequence in unavailable_sequences:
                return None
            delivery = await self._reserve_message(
                address,
                message,
                visibility_timeout=visibility_timeout,
            )
            if delivery is not None:
                return delivery
            unavailable_sequences.add(sequence)

    async def renew(
        self,
        delivery: WorkDelivery,
        *,
        visibility_timeout: float,
    ) -> WorkDelivery:
        visibility_timeout = validate_positive_finite(
            visibility_timeout,
            label="visibility timeout",
        )
        local = self._required_delivery(delivery)
        now = await self.runtime.refresh_time()
        for _attempt in range(_MAX_STATE_ATTEMPTS):
            current = await self._state(local.address, delivery.identity.value)
            if current is None or not _owns(current.state, delivery):
                self._deliveries.pop(delivery.receipt, None)
                raise _stale_delivery(delivery)
            updated = _WorkState(
                identity=current.state.identity,
                maximum_attempts=current.state.maximum_attempts,
                attempt=current.state.attempt,
                status=_STATE_ACTIVE,
                receipt=delivery.receipt,
                deadline=now + visibility_timeout,
            )
            if await self._write_state(local.address, current.sequence, updated):
                await local.message.in_progress()
                return WorkDelivery(
                    queue=delivery.queue,
                    identity=delivery.identity,
                    payload=delivery.payload,
                    attempt=delivery.attempt,
                    visible_until=updated.deadline,
                    receipt=delivery.receipt,
                )
        raise CacheConflictError("NATS JetStream delivery renewal conflicted.")

    async def acknowledge(self, delivery: WorkDelivery) -> None:
        local = self._required_delivery(delivery)
        await self._settle(delivery, local, action=_STATE_ACKNOWLEDGED, delay=0)
        await local.message.ack()
        await self._flush()
        self._deliveries.pop(delivery.receipt, None)

    async def reject(self, delivery: WorkDelivery, *, delay: float = 0) -> None:
        delay = validate_non_negative_finite(delay, label="retry delay")
        local = self._required_delivery(delivery)
        state = await self._settle(delivery, local, action=_STATE_DELAYED, delay=delay)
        if state.status == _STATE_DEAD_PENDING:
            await self._write_dead_letter(local.address, delivery, state)
            await self._mark_dead(local.address, delivery.identity.value, state)
            await local.message.term()
            await self._flush()
            self._deliveries.pop(delivery.receipt, None)
            return
        remaining_delay = state.deadline - await self.runtime.refresh_time()
        if remaining_delay <= 0:
            await local.message.nak()
        else:
            await local.message.nak(delay=remaining_delay)
        await self._flush()
        self._deliveries.pop(delivery.receipt, None)

    async def dead_letter(self, delivery: WorkDelivery) -> None:
        local = self._required_delivery(delivery)
        state = await self._settle(
            delivery,
            local,
            action=_STATE_DEAD_PENDING,
            delay=0,
        )
        await self._write_dead_letter(local.address, delivery, state)
        await self._mark_dead(local.address, delivery.identity.value, state)
        await local.message.term()
        await self._flush()
        self._deliveries.pop(delivery.receipt, None)

    async def dead_letters(
        self,
        owner: str,
        queue: str,
        *,
        limit: int = 100,
    ) -> tuple[WorkDelivery, ...]:
        address = _address(self.runtime, owner, queue)
        limit = validate_limit(limit)
        try:
            info = await (await self.runtime.coordination_jetstream()).stream_info(
                address.dead_stream_name
            )
        except self.runtime.not_found_error():
            return ()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream dead-letter lookup failed (%s).", type(exc).__name__
            )
            raise CacheFeatureError(
                "NATS JetStream dead-letter lookup failed."
            ) from None
        stream_state = getattr(info, "state", None)
        first = getattr(stream_state, "first_seq", 0)
        last = getattr(stream_state, "last_seq", 0)
        if not isinstance(first, int) or not isinstance(last, int) or last == 0:
            return ()
        jetstream = await self.runtime.coordination_jetstream()
        deliveries: list[WorkDelivery] = []
        for sequence in range(first, last + 1):
            if len(deliveries) >= limit:
                break
            try:
                message = await jetstream.get_msg(
                    address.dead_stream_name,
                    seq=sequence,
                    direct=True,
                )
            except self.runtime.not_found_error():
                continue
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.warning(
                    "NATS JetStream dead-letter read failed (%s).", type(exc).__name__
                )
                raise CacheFeatureError(
                    "NATS JetStream dead-letter read failed."
                ) from None
            delivery = _dead_delivery(queue, message)
            if delivery is not None:
                deliveries.append(delivery)
        return tuple(deliveries)

    async def close(self) -> None:
        async with self._close_lock:
            if self._closed:
                return
            self._closed = True
            deliveries = tuple(self._deliveries.values())
            self._deliveries.clear()
            for local in deliveries:
                try:
                    if await self._return_to_ready(local):
                        await local.message.nak()
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    logger.warning(
                        "NATS JetStream delivery release failed (%s).",
                        type(exc).__name__,
                    )

    async def _ensure_queue(self, address: _QueueAddress) -> None:
        lock = self._ensure_locks.setdefault(address.digest, asyncio.Lock())
        async with lock:
            await self.runtime.ensure_work_queue_stream(
                address.stream_name,
                address.subject,
                maximum_messages=_MAX_WORK_ITEMS_PER_QUEUE,
            )

    async def _next_message(
        self,
        address: _QueueAddress,
        timeout: float,
    ) -> Any | None:
        try:
            subscription = await self.runtime.pull_subscribe(
                address.subject,
                durable=address.durable,
                stream=address.stream_name,
                ack_wait=_NATIVE_ACK_WAIT_SECONDS,
                max_deliver=-1,
                max_ack_pending=_MAX_WORK_ITEMS_PER_QUEUE,
            )
            try:
                messages = await subscription.fetch(1, timeout=max(0.001, timeout))
            finally:
                await subscription.unsubscribe()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self.runtime.is_timeout_error(exc):
                return None
            logger.warning(
                "NATS JetStream work reservation failed (%s).", type(exc).__name__
            )
            raise CacheFeatureError("NATS JetStream work reservation failed.") from None
        return messages[0] if messages else None

    async def _reserve_message(
        self,
        address: _QueueAddress,
        message: Any,
        *,
        visibility_timeout: float,
    ) -> WorkDelivery | None:
        try:
            identity, maximum_attempts, due_at = _message_metadata(message)
        except CacheFeatureError:
            await message.term()
            await self._flush()
            return None
        payload = getattr(message, "data", None)
        if not isinstance(payload, bytes):
            await message.term()
            await self._flush()
            return None
        now = await self.runtime.refresh_time()
        for _attempt in range(_MAX_STATE_ATTEMPTS):
            current = await self._state(address, identity.value)
            if current is None:
                state = _WorkState(
                    identity=identity.value,
                    maximum_attempts=maximum_attempts,
                    attempt=0,
                    status=_STATE_DELAYED if due_at > now else _STATE_READY,
                    deadline=due_at if due_at > now else 0,
                )
                if not await self._write_state(address, 0, state):
                    continue
                current = await self._state(address, identity.value)
                if current is None:
                    raise CacheFeatureError("NATS JetStream work state is unavailable.")
            state = current.state
            if state.status in {_STATE_ACKNOWLEDGED, _STATE_DEAD}:
                await message.ack()
                await self._flush()
                return None
            if state.status == _STATE_DEAD_PENDING:
                await self._complete_pending_dead_letter(
                    address, message, state, payload
                )
                return None
            if state.status == _STATE_DELAYED and state.deadline > now:
                await message.nak(delay=max(0.001, state.deadline - now))
                await self._flush()
                return None
            if state.status == _STATE_ACTIVE and state.deadline > now:
                await message.nak(delay=max(0.001, state.deadline - now))
                await self._flush()
                return None
            next_attempt = (
                state.attempt if state.status == _STATE_RELEASED else state.attempt + 1
            )
            if next_attempt > state.maximum_attempts:
                pending = _WorkState(
                    identity=state.identity,
                    maximum_attempts=state.maximum_attempts,
                    attempt=state.attempt,
                    status=_STATE_DEAD_PENDING,
                )
                if not await self._write_state(address, current.sequence, pending):
                    continue
                await self._complete_pending_dead_letter(
                    address, message, pending, payload
                )
                return None
            receipt = uuid4().hex
            active = _WorkState(
                identity=state.identity,
                maximum_attempts=state.maximum_attempts,
                attempt=next_attempt,
                status=_STATE_ACTIVE,
                receipt=receipt,
                deadline=now + visibility_timeout,
            )
            if not await self._write_state(address, current.sequence, active):
                continue
            delivery = WorkDelivery(
                queue=address.queue,
                identity=identity,
                payload=payload,
                attempt=active.attempt,
                visible_until=active.deadline,
                receipt=receipt,
            )
            self._deliveries[receipt] = _LocalDelivery(address, message, delivery)
            return delivery
        raise CacheConflictError("NATS JetStream work reservation conflicted.")

    async def _settle(
        self,
        delivery: WorkDelivery,
        local: _LocalDelivery,
        *,
        action: str,
        delay: float,
    ) -> _WorkState:
        now = await self.runtime.refresh_time()
        for _attempt in range(_MAX_STATE_ATTEMPTS):
            current = await self._state(local.address, delivery.identity.value)
            if current is None or not _owns(current.state, delivery):
                self._deliveries.pop(delivery.receipt, None)
                raise _stale_delivery(delivery)
            terminal = (
                action == _STATE_DELAYED
                and current.state.attempt >= current.state.maximum_attempts
            )
            updated = _WorkState(
                identity=current.state.identity,
                maximum_attempts=current.state.maximum_attempts,
                attempt=current.state.attempt,
                status=_STATE_DEAD_PENDING if terminal else action,
                deadline=now + delay
                if action == _STATE_DELAYED and not terminal
                else 0,
            )
            if await self._write_state(
                local.address,
                current.sequence,
                updated,
                ttl=(
                    _TERMINAL_STATE_TTL_SECONDS
                    if updated.status == _STATE_ACKNOWLEDGED
                    else None
                ),
            ):
                return updated
        raise CacheConflictError("NATS JetStream delivery settlement conflicted.")

    async def _return_to_ready(self, local: _LocalDelivery) -> bool:
        for _attempt in range(_MAX_STATE_ATTEMPTS):
            current = await self._state(local.address, local.delivery.identity.value)
            if current is None or not _owns(current.state, local.delivery):
                return False
            ready = _WorkState(
                identity=current.state.identity,
                maximum_attempts=current.state.maximum_attempts,
                attempt=current.state.attempt,
                status=_STATE_RELEASED,
            )
            if await self._write_state(local.address, current.sequence, ready):
                return True
        raise CacheConflictError("NATS JetStream delivery release conflicted.")

    async def _state(
        self,
        address: _QueueAddress,
        identity: str,
    ) -> _StateRecord | None:
        await self.runtime.ensure_coordination_state_stream()
        subject = _state_subject(self.runtime, address, identity)
        jetstream = await self.runtime.coordination_jetstream()
        try:
            message = await jetstream.get_last_msg(
                self.runtime.coordination_state_stream_name,
                subject,
                direct=True,
            )
        except self.runtime.not_found_error():
            return None
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream work state read failed (%s).", type(exc).__name__
            )
            raise CacheFeatureError("NATS JetStream work state read failed.") from None
        sequence = getattr(message, "seq", None)
        data = getattr(message, "data", None)
        if (
            not isinstance(sequence, int)
            or sequence <= 0
            or not isinstance(data, bytes)
        ):
            raise CacheFeatureError("NATS JetStream work state is invalid.")
        return _StateRecord(sequence, _decode_state(data))

    async def _write_state(
        self,
        address: _QueueAddress,
        expected_sequence: int,
        state: _WorkState,
        *,
        ttl: float | None = None,
    ) -> bool:
        subject = _state_subject(self.runtime, address, state.identity)
        jetstream = await self.runtime.coordination_jetstream()
        headers = {
            "Nats-Expected-Last-Subject-Sequence": str(expected_sequence),
        }
        if ttl is not None:
            headers["Nats-TTL"] = nats_feature_ttl_header(ttl)
        try:
            await jetstream.publish(
                subject,
                _encode_state(state),
                stream=self.runtime.coordination_state_stream_name,
                headers=headers,
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if _is_sequence_conflict(exc):
                return False
            logger.warning(
                "NATS JetStream work state write failed (%s).", type(exc).__name__
            )
            raise CacheFeatureError("NATS JetStream work state write failed.") from None
        return True

    async def _write_dead_letter(
        self,
        address: _QueueAddress,
        delivery: WorkDelivery,
        state: _WorkState,
    ) -> None:
        await self.runtime.ensure_replay_stream(
            address.dead_stream_name,
            address.dead_subject,
            retention_count=_MAX_DEAD_LETTERS_PER_QUEUE,
        )
        jetstream = await self.runtime.coordination_jetstream()
        try:
            await jetstream.publish(
                address.dead_subject,
                delivery.payload,
                stream=address.dead_stream_name,
                headers={
                    "Nats-Msg-Id": f"dead-{delivery.identity.value}",
                    _WORK_ID_HEADER: delivery.identity.value,
                    _WORK_MAX_ATTEMPTS_HEADER: str(state.maximum_attempts),
                    _WORK_DEAD_HEADER: str(state.attempt),
                },
            )
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream dead-letter publication failed (%s).",
                type(exc).__name__,
            )
            raise CacheFeatureError(
                "NATS JetStream dead-letter publication failed."
            ) from None

    async def _mark_dead(
        self,
        address: _QueueAddress,
        identity: str,
        pending: _WorkState,
    ) -> None:
        for _attempt in range(_MAX_STATE_ATTEMPTS):
            current = await self._state(address, identity)
            if current is None:
                raise CacheFeatureError("NATS JetStream work state is unavailable.")
            if current.state.status == _STATE_DEAD:
                return
            if current.state != pending:
                raise CacheConflictError("NATS JetStream delivery is stale.")
            terminal = _WorkState(
                identity=current.state.identity,
                maximum_attempts=current.state.maximum_attempts,
                attempt=current.state.attempt,
                status=_STATE_DEAD,
            )
            if await self._write_state(
                address,
                current.sequence,
                terminal,
                ttl=_TERMINAL_STATE_TTL_SECONDS,
            ):
                return
        raise CacheConflictError("NATS JetStream dead-letter settlement conflicted.")

    async def _complete_pending_dead_letter(
        self,
        address: _QueueAddress,
        message: Any,
        state: _WorkState,
        payload: bytes,
    ) -> None:
        delivery = WorkDelivery(
            queue=address.queue,
            identity=WorkIdentity(state.identity),
            payload=payload,
            attempt=state.attempt,
            visible_until=0,
            receipt=f"dead-{state.identity}",
        )
        await self._write_dead_letter(address, delivery, state)
        await self._mark_dead(address, state.identity, state)
        await message.term()
        await self._flush()

    async def _flush(self) -> None:
        client = await self.runtime.nats_client()
        try:
            await client.flush()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning(
                "NATS JetStream work settlement flush failed (%s).",
                type(exc).__name__,
            )
            raise CacheFeatureError(
                "NATS JetStream work settlement confirmation failed."
            ) from None

    def _required_delivery(self, delivery: WorkDelivery) -> _LocalDelivery:
        if not isinstance(delivery, WorkDelivery):
            raise TypeError("Delivery must be a WorkDelivery.")
        local = self._deliveries.get(delivery.receipt)
        if local is None or local.address.queue != delivery.queue:
            raise _stale_delivery(delivery)
        return local

    def _require_open(self) -> None:
        if self._closed:
            raise CacheFeatureError("The NATS JetStream work queue is closed.")


def _address(runtime: NatsJetStreamRuntime, owner: str, queue: str) -> _QueueAddress:
    owner = validate_resource(owner, label="cache owner")
    queue = validate_resource(queue, label="queue")
    digest = sha256(f"{owner}\0{queue}".encode()).hexdigest()
    prefix = runtime.namespace.upper()
    return _QueueAddress(
        owner=owner,
        queue=queue,
        digest=digest,
        stream_name=f"WYBRA_WORK_{prefix}_{digest}",
        subject=f"{runtime.subject_prefix}.work.{digest}",
        durable=f"WYBRA_WORKER_{prefix}_{digest}",
        dead_stream_name=f"WYBRA_DEAD_{prefix}_{digest}",
        dead_subject=f"{runtime.subject_prefix}.dead.{digest}",
    )


def _state_subject(
    runtime: NatsJetStreamRuntime,
    address: _QueueAddress,
    identity: str,
) -> str:
    digest = sha256(f"{address.digest}\0{identity}".encode()).hexdigest()
    return f"{runtime.coordination_state_subject_prefix}.work.{digest}"


def _encode_state(state: _WorkState) -> bytes:
    return json.dumps(
        {
            "identity": state.identity,
            "maximum_attempts": state.maximum_attempts,
            "attempt": state.attempt,
            "status": state.status,
            "receipt": state.receipt,
            "deadline": state.deadline,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


def _decode_state(value: bytes) -> _WorkState:
    try:
        decoded = json.loads(value)
        if not isinstance(decoded, dict):
            raise TypeError
        identity = decoded["identity"]
        maximum_attempts = decoded["maximum_attempts"]
        attempt = decoded["attempt"]
        status = decoded["status"]
        receipt = decoded["receipt"]
        deadline = decoded["deadline"]
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise CacheFeatureError("NATS JetStream work state is invalid.") from exc
    if receipt is not None and not isinstance(receipt, str):
        raise CacheFeatureError("NATS JetStream work state is invalid.")
    try:
        return _WorkState(
            identity=identity,
            maximum_attempts=maximum_attempts,
            attempt=attempt,
            status=status,
            receipt=receipt,
            deadline=deadline,
        )
    except (TypeError, ValueError) as exc:
        raise CacheFeatureError("NATS JetStream work state is invalid.") from exc


def _message_metadata(message: Any) -> tuple[WorkIdentity, int, float]:
    headers = getattr(message, "headers", None)
    if headers is None:
        raise CacheFeatureError("NATS JetStream work message is invalid.")
    identity = _header(headers, _WORK_ID_HEADER)
    maximum_attempts = _header_integer(headers, _WORK_MAX_ATTEMPTS_HEADER)
    due_at = _header_float(headers, _WORK_DUE_AT_HEADER)
    try:
        return (
            WorkIdentity(identity),
            validate_positive_integer(
                maximum_attempts,
                label="maximum delivery attempts",
            ),
            validate_non_negative_finite(due_at, label="work due time"),
        )
    except (TypeError, ValueError) as exc:
        raise CacheFeatureError("NATS JetStream work message is invalid.") from exc


def _dead_delivery(queue: str, message: Any) -> WorkDelivery | None:
    headers = getattr(message, "headers", None)
    payload = getattr(message, "data", None)
    if headers is None or not isinstance(payload, bytes):
        return None
    try:
        identity = WorkIdentity(_header(headers, _WORK_ID_HEADER))
        attempt = _header_integer(headers, _WORK_DEAD_HEADER)
    except CacheFeatureError:
        return None
    return WorkDelivery(
        queue=queue,
        identity=identity,
        payload=payload,
        attempt=attempt,
        visible_until=0,
        receipt=f"dead-{identity.value}",
    )


def _header(headers: Any, name: str) -> str:
    value = headers.get(name)
    if not isinstance(value, str) or not value:
        raise CacheFeatureError("NATS JetStream work message is invalid.")
    return value


def _header_integer(headers: Any, name: str) -> int:
    try:
        return int(_header(headers, name))
    except ValueError as exc:
        raise CacheFeatureError("NATS JetStream work message is invalid.") from exc


def _header_float(headers: Any, name: str) -> float:
    try:
        return float(_header(headers, name))
    except ValueError as exc:
        raise CacheFeatureError("NATS JetStream work message is invalid.") from exc


def _message_sequence(message: Any) -> int:
    sequence = getattr(
        getattr(getattr(message, "metadata", None), "sequence", None),
        "stream",
        None,
    )
    if not isinstance(sequence, int) or sequence <= 0:
        raise CacheFeatureError("NATS JetStream work message is invalid.")
    return sequence


def _owns(state: _WorkState, delivery: WorkDelivery) -> bool:
    return (
        state.status == _STATE_ACTIVE
        and state.receipt == delivery.receipt
        and state.identity == delivery.identity.value
        and state.attempt == delivery.attempt
    )


def _stale_delivery(delivery: WorkDelivery) -> CacheConflictError:
    return CacheConflictError(
        f"Delivery {delivery.identity.value!r} is stale or no longer reserved."
    )


def _is_sequence_conflict(error: Exception) -> bool:
    return getattr(error, "err_code", None) in {10071, 10164}


def _is_queue_capacity_error(error: Exception) -> bool:
    return getattr(error, "err_code", None) == 10077


__all__ = ("NatsWorkQueue",)
