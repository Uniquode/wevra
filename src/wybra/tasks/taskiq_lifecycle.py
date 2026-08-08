"""Secret-safe Taskiq lifecycle storage over Wybra cache features."""

from __future__ import annotations

import json
import logging
import time
from asyncio import CancelledError, Event, create_task, sleep
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager, suppress
from contextvars import ContextVar
from dataclasses import dataclass, field, replace
from math import isfinite, log
from random import random
from typing import Any
from uuid import UUID, uuid4

from taskiq import SimpleRetryMiddleware, SmartRetryMiddleware, TaskiqMiddleware
from taskiq.exceptions import NoResultError, SendTaskError
from taskiq.message import TaskiqMessage
from taskiq.result import TaskiqResult

from wybra.cache import (
    MAX_CACHE_FEATURE_LIMIT,
    AtomicCacheCapability,
    CacheConflictError,
    CachePositionExpiredError,
    CacheTimeCapability,
    CacheWorkQueueRejectedError,
    LeaseCacheCapability,
    LeaseToken,
    StreamCacheCapability,
    StreamPosition,
)
from wybra.tasks.lifecycle import (
    TERMINAL_TASK_STATES,
    TaskLifecycleError,
    TaskLifecycleEvent,
    TaskLifecycleKind,
    TaskProgressError,
    TaskState,
    TaskStatus,
    TaskStatusProjection,
)
from wybra.tasks.models import TaskExecutionContext
from wybra.tasks.taskiq_protocol import (
    DELIVERY_ATTEMPT_LABEL,
    DELIVERY_IDENTITY_LABEL,
    DELIVERY_RECEIPT_LABEL,
    TASK_CAUSATION_ID_LABEL,
    TASK_CORRELATION_ID_LABEL,
    TASK_IDEMPOTENCY_KEY_LABEL,
    TASK_NAME_LABEL,
    TASK_QUEUE_LABEL,
    TASK_RETRY_BACKOFF_MULTIPLIER_LABEL,
    TASK_RETRY_INITIAL_DELAY_LABEL,
    TASK_RETRY_JITTER_SECONDS_LABEL,
    TASK_RETRY_MAXIMUM_DELAY_LABEL,
    TASK_SCHEMA_VERSION_LABEL,
)

_LABEL = "_wybra_task_lifecycle"
_ENVELOPE_VERSION = 1
_LOGGER = logging.getLogger(__name__)
DEFAULT_ACTIVE_STATUS_TIMEOUT_SECONDS = 86_400.0
DEFAULT_LIFECYCLE_LEASE_TTL_SECONDS = 30.0
DEFAULT_LIFECYCLE_LEASE_WAIT_TIMEOUT_SECONDS = 30.0
DEFAULT_PROJECTION_CAS_MAX_ATTEMPTS = 10
DEFAULT_REPLAY_CONFLICT_MAX_ATTEMPTS = 5
_TERMINAL_STATES = TERMINAL_TASK_STATES
_ENVELOPE_FIELDS = frozenset({"version", "event"})
_EVENT_FIELDS = frozenset(
    {
        "attempt",
        "causation_id",
        "correlation_id",
        "delivery_attempt",
        "delivery_identity",
        "error_type",
        "kind",
        "occurred_at",
        "progress",
        "queue",
        "schema_version",
        "task_id",
        "task_name",
        "worker_id",
    }
)
_STATUS_ENVELOPE_FIELDS = frozenset({"version", "position", "status"})
_STATUS_FIELDS = frozenset(
    {
        "attempt",
        "causation_id",
        "correlation_id",
        "delivery_attempt",
        "delivery_identity",
        "error_type",
        "progress",
        "queue",
        "schema_version",
        "state",
        "submitted_at",
        "task_id",
        "task_name",
        "terminal_at",
        "updated_at",
        "worker_id",
    }
)
_METADATA_FIELDS = frozenset(
    {
        "attempt",
        "causation_id",
        "correlation_id",
        "delivery_attempt",
        "delivery_identity",
        "queue",
        "retry_pending",
        "retry_origin_attempt",
        "retry_origin_delivery_attempt",
        "retry_origin_delivery_identity",
        "retry_origin_worker_id",
        "schema_version",
        "submitted",
        "task_id",
        "task_name",
        "terminal_error_type",
        "terminal_pending",
        "version",
        "worker_id",
    }
)
_CURRENT_EXECUTION_CONTEXT: ContextVar[TaskExecutionContext | None] = ContextVar(
    "wybra_taskiq_execution_context",
    default=None,
)


@dataclass(frozen=True, slots=True)
class TaskiqLifecyclePolicy:
    """Names and retention for one isolated Taskiq lifecycle store."""

    owner: str
    stream: str
    status_retention_seconds: float
    queue: str
    active_status_timeout_seconds: float = DEFAULT_ACTIVE_STATUS_TIMEOUT_SECONDS
    lifecycle_lease_ttl_seconds: float = DEFAULT_LIFECYCLE_LEASE_TTL_SECONDS
    lifecycle_lease_wait_timeout_seconds: float = (
        DEFAULT_LIFECYCLE_LEASE_WAIT_TIMEOUT_SECONDS
    )
    projection_cas_max_attempts: int = DEFAULT_PROJECTION_CAS_MAX_ATTEMPTS
    worker_id: str | None = None
    schema_version: int = 1
    replay_page_limit: int = 100

    def __post_init__(self) -> None:
        for label, value in (
            ("Owner", self.owner),
            ("Stream", self.stream),
            ("Queue", self.queue),
        ):
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{label} must be a non-blank string.")
        if self.worker_id is not None and (
            not isinstance(self.worker_id, str) or not self.worker_id.strip()
        ):
            raise ValueError("Worker ID must be a non-blank string when configured.")
        if (
            isinstance(self.schema_version, bool)
            or not isinstance(self.schema_version, int)
            or self.schema_version < 1
        ):
            raise ValueError("Schema version must be a positive integer.")
        if (
            isinstance(self.replay_page_limit, bool)
            or not isinstance(self.replay_page_limit, int)
            or not 1 <= self.replay_page_limit <= MAX_CACHE_FEATURE_LIMIT
        ):
            raise ValueError(
                "Replay page limit must be a positive integer no greater than "
                f"{MAX_CACHE_FEATURE_LIMIT}."
            )
        if (
            isinstance(self.projection_cas_max_attempts, bool)
            or not isinstance(self.projection_cas_max_attempts, int)
            or self.projection_cas_max_attempts < 1
        ):
            raise ValueError("Projection CAS attempts must be a positive integer.")
        if (
            isinstance(self.status_retention_seconds, bool)
            or not isinstance(self.status_retention_seconds, int | float)
            or not isfinite(self.status_retention_seconds)
            or self.status_retention_seconds <= 0
        ):
            raise ValueError("Status retention must be a positive finite number.")
        for label, value in (
            ("Active status timeout", self.active_status_timeout_seconds),
            ("Lifecycle lease TTL", self.lifecycle_lease_ttl_seconds),
            (
                "Lifecycle lease wait timeout",
                self.lifecycle_lease_wait_timeout_seconds,
            ),
        ):
            if (
                isinstance(value, bool)
                or not isinstance(value, int | float)
                or not isfinite(value)
                or value <= 0
            ):
                raise ValueError(f"{label} must be a positive finite number.")
        object.__setattr__(self, "owner", self.owner.strip())
        object.__setattr__(self, "stream", self.stream.strip())
        object.__setattr__(self, "queue", self.queue.strip())
        if self.worker_id is not None:
            object.__setattr__(self, "worker_id", self.worker_id.strip())


@dataclass(slots=True)
class CacheTaskLifecycle:
    """Append lifecycle facts and maintain a revisioned status projection."""

    streams: StreamCacheCapability
    atomic: AtomicCacheCapability
    leases: LeaseCacheCapability
    policy: TaskiqLifecyclePolicy
    cache_time: CacheTimeCapability
    _position: StreamPosition | None = field(default=None, init=False, repr=False)
    _pending_dead_letters: set[UUID] = field(
        default_factory=set,
        init=False,
        repr=False,
    )
    _pending_failed_attempts: set[UUID] = field(
        default_factory=set,
        init=False,
        repr=False,
    )

    async def record(self, event: TaskLifecycleEvent) -> TaskStatus:
        async with self._projection_lease(event.task_id) as (
            lease_ref,
            lease_lost,
        ):
            event = replace(event, occurred_at=await self.cache_time.refresh())
            status = await self._record(
                event,
                lease_ref=lease_ref,
                lease_lost=lease_lost,
            )
            self._track_repair_candidate(event, status)
            return status

    async def record_repair(
        self,
        event: TaskLifecycleEvent,
        *,
        preserve_occurred_at: bool = False,
    ) -> TaskStatus:
        async with self._projection_lease(event.task_id) as (
            lease_ref,
            lease_lost,
        ):
            if preserve_occurred_at:
                await self.cache_time.refresh()
            else:
                event = replace(event, occurred_at=await self.cache_time.refresh())
            return await self._record(
                event,
                lease_ref=lease_ref,
                lease_lost=lease_lost,
                allow_abandoned_repair=True,
            )

    async def _record(
        self,
        event: TaskLifecycleEvent,
        *,
        lease_ref: list[LeaseToken],
        lease_lost: Event,
        allow_abandoned_repair: bool = False,
    ) -> TaskStatus:
        existing = await self._projected_status(event.task_id)
        recovered_position: StreamPosition | None = None
        refresh_abandoned_start = False
        if (
            not allow_abandoned_repair
            and existing is not None
            and _is_abandoned_active_status(
                existing,
                event.occurred_at,
                self.policy,
            )
        ):
            refresh_abandoned_start = event.kind is TaskLifecycleKind.STARTED
            existing = None
        if existing is None and event.kind is not TaskLifecycleKind.SUBMITTED:
            existing, recovered_position = await self._status_from_stream(event.task_id)
            if (
                not allow_abandoned_repair
                and existing is not None
                and _is_abandoned_active_status(
                    existing,
                    event.occurred_at,
                    self.policy,
                )
            ):
                if event.kind is TaskLifecycleKind.STARTED:
                    refresh_abandoned_start = True
                else:
                    existing = None
                    recovered_position = None
        if (
            existing is not None
            and not refresh_abandoned_start
            and _is_duplicate_start(existing, event)
        ):
            if recovered_position is None:
                return existing
        try:
            _apply_event(existing, event)
        except TaskLifecycleError:
            if existing is not None and _is_idempotent_event(existing, event):
                if recovered_position is not None:
                    if existing.state in _TERMINAL_STATES:
                        return existing
                    return await self._persist(
                        event,
                        recovered_position,
                        lease_ref=lease_ref,
                        lease_lost=lease_lost,
                    )
                return existing
            raise
        _require_lease(lease_lost)
        position = await self.streams.append(
            self.policy.owner,
            self.policy.stream,
            _encode_event(event),
            lease=lease_ref[0],
        )
        _require_lease(lease_lost)
        return await self._persist(
            event,
            position,
            lease_ref=lease_ref,
            lease_lost=lease_lost,
            apply_target_event=True,
        )

    @asynccontextmanager
    async def _projection_lease(
        self,
        task_id: UUID,
    ) -> AsyncIterator[tuple[list[LeaseToken], Event]]:
        lease = await self._acquire_projection_lease(task_id)
        lost = Event()
        lease_ref = [lease]
        renewal = create_task(self._renew_projection_lease(lease_ref, lost))
        try:
            yield lease_ref, lost
        finally:
            renewal.cancel()
            with suppress(CancelledError):
                try:
                    await renewal
                except Exception:
                    _LOGGER.warning("Task lifecycle projection lease renewal failed.")
            try:
                await self.leases.release(lease_ref[0])
            except Exception:
                _LOGGER.warning("Task lifecycle projection lease release failed.")

    async def _acquire_projection_lease(self, task_id: UUID) -> LeaseToken:
        resource = f"lifecycle:{self.policy.stream}:{task_id}"
        holder = uuid4().hex
        deadline = time.monotonic() + self.policy.lifecycle_lease_wait_timeout_seconds
        delay = 0.01
        while True:
            lease = await self.leases.acquire(
                self.policy.owner,
                resource,
                holder,
                ttl=self.policy.lifecycle_lease_ttl_seconds,
            )
            if lease is not None:
                return lease
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise CacheConflictError(
                    "Timed out waiting for task lifecycle projection."
                )
            await sleep(min(delay, remaining))
            delay = min(delay * 2, 0.25)

    async def _renew_projection_lease(
        self,
        lease_ref: list[LeaseToken],
        lost: Event,
    ) -> None:
        while True:
            try:
                await sleep(self.policy.lifecycle_lease_ttl_seconds / 3)
                lease_ref[0] = await self.leases.renew(
                    lease_ref[0],
                    ttl=self.policy.lifecycle_lease_ttl_seconds,
                )
            except Exception:
                lost.set()
                _LOGGER.warning("Task lifecycle projection lease renewal failed.")
                return

    async def status(self, task_id: UUID) -> TaskStatus | None:
        status = await self._projected_status(task_id)
        if status is not None:
            if status.state not in _TERMINAL_STATES and _is_abandoned_active_status(
                status,
                await self.cache_time.refresh(),
                self.policy,
            ):
                return None
            return _public_status(status)
        status, _position = await self._status_from_stream(task_id)
        if status is None or status.state in _TERMINAL_STATES:
            return None
        now = await self.cache_time.refresh()
        if _is_abandoned_active_status(status, now, self.policy):
            return None
        return _public_status(status)

    async def _projected_status(self, task_id: UUID) -> TaskStatus | None:
        snapshot = await self.atomic.get(self.policy.owner, _status_key(task_id))
        if snapshot is None:
            return None
        status, position = _decode_stored_status(snapshot.value)
        recovered, _position = await self._status_from_stream(
            task_id,
            status=status,
            after=position,
        )
        return recovered

    async def recovered_status(self, task_id: UUID) -> TaskStatus | None:
        status = await self._projected_status(task_id)
        if status is not None:
            return status
        status, _position = await self._status_from_stream(task_id)
        return status

    async def lifecycle(self, task_id: UUID) -> tuple[TaskLifecycleEvent, ...]:
        events: list[TaskLifecycleEvent] = []
        position: StreamPosition | None = None
        restarted = False
        while True:
            try:
                records = await self.streams.read(
                    self.policy.owner,
                    self.policy.stream,
                    after=position,
                    limit=self.policy.replay_page_limit,
                )
            except CachePositionExpiredError:
                if restarted:
                    raise TaskLifecycleError(
                        "Task lifecycle history changed during replay."
                    ) from None
                events.clear()
                position = None
                restarted = True
                continue
            if not records:
                return tuple(events)
            for record in records:
                try:
                    event = _decode_event(record.payload)
                except TaskLifecycleError, ValueError:
                    continue
                if event.task_id == task_id:
                    events.append(_public_event(event))
            position = records[-1].position
            if len(records) < self.policy.replay_page_limit:
                return tuple(events)

    async def replay(self) -> None:
        await self.cache_time.refresh()
        restarted = False
        while True:
            try:
                records = await self.streams.read(
                    self.policy.owner,
                    self.policy.stream,
                    after=self._position,
                    limit=self.policy.replay_page_limit,
                )
            except CachePositionExpiredError:
                if restarted:
                    raise TaskLifecycleError(
                        "Task lifecycle stream changed during replay."
                    ) from None
                self._position = None
                restarted = True
                continue
            if not records:
                break
            for record in records:
                try:
                    event = _decode_event(record.payload)
                except TaskLifecycleError, ValueError:
                    self._position = record.position
                    continue
                try:
                    status = await self._replay_event(event, record.position)
                except TaskLifecycleError, ValueError:
                    self._position = record.position
                    continue
                self._track_repair_candidate(event, status)
                self._position = record.position
            if len(records) < self.policy.replay_page_limit:
                break
        await self.reconcile_abandoned()

    async def _replay_event(
        self,
        event: TaskLifecycleEvent,
        position: StreamPosition,
    ) -> TaskStatus:
        """Persist one replayed event, waiting for a concurrent projection writer."""

        delay = 0.01
        for attempt in range(DEFAULT_REPLAY_CONFLICT_MAX_ATTEMPTS):
            try:
                async with self._projection_lease(event.task_id) as (
                    lease_ref,
                    lease_lost,
                ):
                    await self.cache_time.refresh()
                    _require_lease(lease_lost)
                    return await self._persist(
                        event,
                        position,
                        lease_ref=lease_ref,
                        lease_lost=lease_lost,
                    )
            except CacheConflictError:
                if attempt + 1 == DEFAULT_REPLAY_CONFLICT_MAX_ATTEMPTS:
                    _LOGGER.warning(
                        "Task lifecycle replay could not acquire a projection lease "
                        "after %s attempts.",
                        DEFAULT_REPLAY_CONFLICT_MAX_ATTEMPTS,
                    )
                    raise
                await sleep(delay)
                delay = min(delay * 2, 1.0)
        raise AssertionError("Replay conflict retries must either return or raise.")

    async def reconcile_abandoned(self) -> None:
        """Repair bounded locally observed retry hand-offs after abandonment."""
        now = await self.cache_time.refresh()
        for task_id in tuple(self._pending_failed_attempts):
            status = await self.recovered_status(task_id)
            if status is None or not _is_abandoned_failed_attempt(
                status,
                now,
                self.policy,
            ):
                continue
            self._pending_dead_letters.add(task_id)
            await self.record_repair(_failed_event(status))
            self._pending_failed_attempts.discard(task_id)
        for task_id in tuple(self._pending_dead_letters):
            status = await self.recovered_status(task_id)
            if (
                status is not None
                and status.state is TaskState.FAILED
                and self._retention_ttl(status) > 0
            ):
                await self.record_repair(
                    _dead_letter_event(status),
                    preserve_occurred_at=True,
                )
            self._pending_dead_letters.discard(task_id)

    def _track_repair_candidate(
        self,
        event: TaskLifecycleEvent,
        status: TaskStatus,
    ) -> None:
        if status.state is TaskState.FAILED:
            self._pending_dead_letters.add(status.task_id)
        else:
            self._pending_dead_letters.discard(status.task_id)
        if event.kind is TaskLifecycleKind.ATTEMPT_FAILED:
            self._pending_failed_attempts.add(status.task_id)
        else:
            self._pending_failed_attempts.discard(status.task_id)

    async def _persist(
        self,
        event: TaskLifecycleEvent,
        position: StreamPosition,
        *,
        lease_ref: list[LeaseToken],
        lease_lost: Event,
        apply_target_event: bool = False,
    ) -> TaskStatus:
        key = _status_key(event.task_id)
        for attempt in range(self.policy.projection_cas_max_attempts):
            _require_lease(lease_lost)
            current = await self.atomic.get(self.policy.owner, key)
            _require_lease(lease_lost)
            previous = None
            previous_position = None
            if current is not None:
                previous, previous_position = _decode_stored_status(current.value)
            if previous_position is not None and position <= previous_position:
                if previous is not None:
                    return previous
                raise TaskLifecycleError("Task lifecycle snapshot is invalid.")
            target = position
            status = await self._project_through(
                event.task_id,
                status=previous,
                after=previous_position,
                target=target,
                lease_lost=lease_lost,
                target_event=event if apply_target_event else None,
            )
            _require_lease(lease_lost)
            if status is None:
                if previous is not None:
                    return previous
                raise TaskLifecycleError(
                    "Task lifecycle stream has no valid task state."
                )
            if (
                current is not None
                and previous == status
                and previous_position == target
            ):
                return status
            ttl = self._retention_ttl(status)
            if ttl == 0:
                if current is None:
                    return status
                deleted = await self.atomic.compare_and_delete(
                    self.policy.owner,
                    key,
                    current.revision,
                    lease=lease_ref[0],
                )
                if deleted:
                    return status
                await self._retry_projection_write(attempt, lease_lost=lease_lost)
                continue
            payload = _encode_status(status, target)
            if current is None:
                created = await self.atomic.create(
                    self.policy.owner,
                    key,
                    payload,
                    ttl=ttl,
                    lease=lease_ref[0],
                )
                if created is not None:
                    return status
                await self._retry_projection_write(attempt, lease_lost=lease_lost)
                continue
            updated = await self.atomic.compare_and_swap(
                self.policy.owner,
                key,
                current.revision,
                payload,
                ttl=ttl,
                lease=lease_ref[0],
            )
            if updated is not None:
                return status
            await self._retry_projection_write(attempt, lease_lost=lease_lost)
        raise CacheConflictError("Task lifecycle projection could not be persisted.")

    async def _retry_projection_write(self, attempt: int, *, lease_lost: Event) -> None:
        if attempt + 1 < self.policy.projection_cas_max_attempts:
            await sleep(min(0.01 * (2**attempt), 0.25))
            _require_lease(lease_lost)

    async def _project_through(
        self,
        task_id: UUID,
        *,
        status: TaskStatus | None,
        after: StreamPosition | None,
        target: StreamPosition,
        lease_lost: Event,
        target_event: TaskLifecycleEvent | None,
    ) -> TaskStatus | None:
        projection = TaskStatusProjection(retention_seconds=None)
        if status is not None:
            projection.restore(status)
        position = after
        while True:
            _require_lease(lease_lost)
            try:
                records = await self.streams.read(
                    self.policy.owner,
                    self.policy.stream,
                    after=position,
                    limit=self.policy.replay_page_limit,
                )
            except CachePositionExpiredError:
                if position is None:
                    raise TaskLifecycleError(
                        "Task lifecycle stream position expired."
                    ) from None
                position = None
                projection = TaskStatusProjection(retention_seconds=None)
                if status is not None:
                    projection.restore(status)
                continue
            _require_lease(lease_lost)
            if not records:
                return _project_target_event(
                    projection,
                    task_id,
                    target_event,
                )
            for record in records:
                if record.position > target:
                    return _project_target_event(
                        projection,
                        task_id,
                        target_event,
                    )
                position = record.position
                if record.position == target:
                    target_event = None
                try:
                    stream_event = _decode_event(record.payload)
                    if stream_event.task_id == task_id:
                        projection.apply(stream_event)
                except TaskLifecycleError, ValueError:
                    continue
            if len(records) < self.policy.replay_page_limit:
                return _project_target_event(
                    projection,
                    task_id,
                    target_event,
                )

    async def _status_from_stream(
        self,
        task_id: UUID,
        *,
        status: TaskStatus | None = None,
        after: StreamPosition | None = None,
    ) -> tuple[TaskStatus | None, StreamPosition | None]:
        projection = TaskStatusProjection(retention_seconds=None)
        if status is not None:
            projection.restore(status)
        position = after
        restarted = False
        while True:
            try:
                records = await self.streams.read(
                    self.policy.owner,
                    self.policy.stream,
                    after=position,
                    limit=self.policy.replay_page_limit,
                )
            except CachePositionExpiredError:
                if restarted:
                    raise TaskLifecycleError(
                        "Task lifecycle stream changed during recovery."
                    ) from None
                position = None
                projection = TaskStatusProjection(retention_seconds=None)
                if status is not None:
                    projection.restore(status)
                restarted = True
                continue
            if not records:
                return projection.status(task_id), position
            for record in records:
                position = record.position
                try:
                    stream_event = _decode_event(record.payload)
                    if stream_event.task_id == task_id:
                        projection.apply(stream_event)
                except TaskLifecycleError, ValueError:
                    continue
            if len(records) < self.policy.replay_page_limit:
                return projection.status(task_id), position

    def _retention_ttl(self, status: TaskStatus) -> float:
        retention = self.policy.status_retention_seconds
        if status.state not in _TERMINAL_STATES:
            retention = self.policy.active_status_timeout_seconds
        retention_anchor = (
            status.terminal_at if status.terminal_at is not None else status.updated_at
        )
        elapsed = max(0, self.cache_time.now() - retention_anchor)
        return max(0, retention - elapsed)


class CacheTaskiqLifecycleMiddleware(TaskiqMiddleware):
    """Map Taskiq hooks to provider-neutral, secret-safe lifecycle facts."""

    def __init__(self, lifecycle: CacheTaskLifecycle) -> None:
        super().__init__()
        self._lifecycle = lifecycle

    async def pre_send(self, message: TaskiqMessage) -> TaskiqMessage:
        retries = _retries(message)
        if retries == 0 and _LABEL in message.labels:
            raise ValueError("Taskiq submission must not supply lifecycle metadata.")
        metadata = _metadata(message, self._lifecycle.policy)
        if retries == 0 and not metadata["submitted"]:
            await self._lifecycle.record(
                _event_from_metadata(metadata, TaskLifecycleKind.SUBMITTED)
            )
            metadata["submitted"] = True
        elif retries:
            if not metadata["submitted"]:
                raise ValueError("Taskiq retry is missing lifecycle metadata.")
            if not metadata["retry_pending"] or metadata["attempt"] != retries:
                raise ValueError("Taskiq retry lifecycle metadata is invalid.")
            metadata["attempt"] = retries + 1
            metadata["delivery_attempt"] = None
            metadata["delivery_identity"] = None
            metadata["worker_id"] = None
            for label in (
                DELIVERY_ATTEMPT_LABEL,
                DELIVERY_IDENTITY_LABEL,
                DELIVERY_RECEIPT_LABEL,
            ):
                message.labels.pop(label, None)
        message.labels[_LABEL] = _encode_metadata(metadata)
        return message

    async def post_send(self, message: TaskiqMessage) -> None:
        metadata = _metadata(message, self._lifecycle.policy)
        if not _retries(message) or not metadata["retry_pending"]:
            return
        await self._record_retry_scheduled(metadata)

    async def submission_rejected(self, message: TaskiqMessage) -> None:
        """Terminalise a submission that the cache queue definitively rejected."""
        if _retries(message):
            return
        metadata = _metadata(message, self._lifecycle.policy)
        status = await self._lifecycle.recovered_status(UUID(metadata["task_id"]))
        if status is None:
            return
        if status.state is TaskState.SUBMITTED:
            status = await self._lifecycle.record(
                _failed_event(status, error_type="CacheWorkQueueRejectedError")
            )
        if status.state is TaskState.FAILED:
            await self._lifecycle.record(_dead_letter_event(status))

    async def submission_aborted(self, task_id: UUID) -> None:
        """Repair a submission that failed before the broker hand-off began."""

        status = await self._lifecycle.recovered_status(task_id)
        if status is None or status.state in _TERMINAL_STATES:
            return
        if status.state is not TaskState.SUBMITTED:
            return
        status = await self._lifecycle.record_repair(
            _failed_event(status, error_type="TaskLifecyclePrePublicationFailure")
        )
        if status.state is TaskState.FAILED:
            await self._lifecycle.record_repair(_dead_letter_event(status))

    async def delivery_exhausted(self, message: TaskiqMessage) -> None:
        """Terminalise a delivery after the cache exhausts its visibility attempts."""
        metadata = _metadata(message, self._lifecycle.policy)
        status = await self._lifecycle.recovered_status(UUID(metadata["task_id"]))
        if status is None:
            return
        if status.state in _TERMINAL_STATES and status.state is not TaskState.FAILED:
            return
        if status.state is TaskState.FAILED:
            await self._lifecycle.record_repair(
                _dead_letter_event(status),
                preserve_occurred_at=True,
            )
            return
        delivery_attempt = _delivery_attempt(message)
        delivery_identity = _delivery_identity(message)
        pending_retry = _is_pending_retry_delivery(status, metadata)
        if status.state is TaskState.RETRY_SCHEDULED:
            if not pending_retry:
                return
        elif metadata["retry_pending"] and pending_retry:
            pass
        elif status.attempt != metadata["attempt"] or (
            status.state is TaskState.RUNNING
            and (
                delivery_attempt is None
                or delivery_identity is None
                or status._delivery_identity != delivery_identity
                or (
                    status.delivery_attempt is not None
                    and delivery_attempt < status.delivery_attempt
                )
            )
        ):
            return
        failed = await self._lifecycle.record_repair(
            _failed_event(status, error_type="CacheWorkQueueDeliveryExhausted")
        )
        await self._lifecycle.record_repair(_dead_letter_event(failed))

    async def pre_execute(self, message: TaskiqMessage) -> TaskiqMessage:
        await self._lifecycle.reconcile_abandoned()
        metadata = _metadata(message, self._lifecycle.policy)
        metadata["worker_id"] = self._lifecycle.policy.worker_id
        metadata["delivery_attempt"] = _delivery_attempt(message)
        metadata["delivery_identity"] = _delivery_identity(message)
        if metadata["retry_pending"] and _retries(message):
            await self._record_retry_scheduled(metadata)
        metadata["retry_pending"] = False
        _clear_retry_origin(metadata)
        message.labels[_LABEL] = _encode_metadata(metadata)
        await self._lifecycle.record(
            _event_from_metadata(metadata, TaskLifecycleKind.STARTED)
        )
        _CURRENT_EXECUTION_CONTEXT.set(self._execution_context(metadata, message))
        return message

    async def is_obsolete_retry_delivery(self, message: TaskiqMessage) -> bool:
        """Return whether a cache delivery has been superseded or terminalised."""
        metadata = _metadata(message, self._lifecycle.policy)
        status = await self._lifecycle.recovered_status(UUID(message.task_id))
        if status is None:
            return False
        if status.state is TaskState.FAILED:
            status = await self._lifecycle.record_repair(
                _dead_letter_event(status),
                preserve_occurred_at=True,
            )
        if status.state in _TERMINAL_STATES:
            return True
        if _retries(message) == 0:
            return status.state is TaskState.RETRY_SCHEDULED or (
                status.attempt > metadata["attempt"]
                or _is_later_delivery(
                    status,
                    _delivery_attempt(message),
                    _delivery_identity(message),
                )
            )
        origin_attempt = metadata["retry_origin_attempt"]
        origin_delivery = metadata["retry_origin_delivery_attempt"]
        origin_worker = metadata["retry_origin_worker_id"]
        if metadata["retry_pending"] and _valid_retry_origin(
            retry_pending=True,
            attempt=origin_attempt,
            delivery_attempt=origin_delivery,
            delivery_identity=metadata["retry_origin_delivery_identity"],
            worker_id=origin_worker,
        ):
            return not (
                status.attempt == origin_attempt
                and status.delivery_attempt == origin_delivery
                and status._delivery_identity
                == metadata["retry_origin_delivery_identity"]
                and status.worker_id == origin_worker
                and status.state in {TaskState.RUNNING, TaskState.RETRY_SCHEDULED}
            )
        return not (
            status.state is TaskState.RETRY_SCHEDULED
            and status.attempt == metadata["attempt"] - 1
        )

    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
        exception: BaseException,
    ) -> None:
        metadata = _metadata(message, self._lifecycle.policy)
        if isinstance(exception, NoResultError):
            return
        error_type = type(exception).__name__
        if self._will_retry(message, exception):
            await self._lifecycle.record(
                _event_from_metadata(
                    metadata,
                    TaskLifecycleKind.ATTEMPT_FAILED,
                    error_type=error_type,
                )
            )
            metadata["retry_origin_attempt"] = metadata["attempt"]
            metadata["retry_origin_delivery_attempt"] = metadata["delivery_attempt"]
            metadata["retry_origin_delivery_identity"] = metadata["delivery_identity"]
            metadata["retry_origin_worker_id"] = metadata["worker_id"]
            metadata["retry_pending"] = True
            message.labels[_LABEL] = _encode_metadata(metadata)
            return
        self._defer_terminal_failure(metadata, error_type)
        message.labels[_LABEL] = _encode_metadata(metadata)

    async def result_skipped(self, message: TaskiqMessage) -> None:
        """Record successful completion when a handler intentionally omits a result."""

        metadata = _metadata(message, self._lifecycle.policy)
        await self._lifecycle.record(
            _event_from_metadata(metadata, TaskLifecycleKind.SUCCEEDED)
        )

    async def retry_handoff_rejected(
        self,
        message: TaskiqMessage,
        error: SendTaskError,
    ) -> None:
        """Record a terminal outcome after a definitively rejected retry."""
        if not isinstance(error.__cause__, CacheWorkQueueRejectedError):
            return
        metadata = _metadata(message, self._lifecycle.policy)
        if not metadata["retry_pending"]:
            return
        status = await self._lifecycle.recovered_status(UUID(metadata["task_id"]))
        if (
            status is None
            or status.state is not TaskState.RUNNING
            or status.attempt != metadata["retry_origin_attempt"]
            or status.delivery_attempt != metadata["retry_origin_delivery_attempt"]
            or status._delivery_identity != metadata["retry_origin_delivery_identity"]
            or status.worker_id != metadata["retry_origin_worker_id"]
        ):
            return
        failed = await self._lifecycle.record(
            _failed_event(
                status,
                error_type=status.error_type or type(error.__cause__).__name__,
            )
        )
        await self._lifecycle.record(_dead_letter_event(failed))

    async def startup(self) -> None:
        self._validate_retry_middleware_order()
        await self._lifecycle.replay()

    async def post_execute(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        del message, result
        _CURRENT_EXECUTION_CONTEXT.set(None)

    async def post_save(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
    ) -> None:
        """Record a terminal lifecycle state after Taskiq stores its result."""

        metadata = _metadata(message, self._lifecycle.policy)
        if not result.is_err:
            await self._lifecycle.record(
                _event_from_metadata(metadata, TaskLifecycleKind.SUCCEEDED)
            )
            return
        if not metadata["terminal_pending"]:
            return
        error_type = metadata["terminal_error_type"]
        if error_type is None:
            raise ValueError("Taskiq terminal lifecycle metadata is invalid.")
        await self._lifecycle.record(
            _event_from_metadata(
                metadata,
                TaskLifecycleKind.FAILED,
                error_type=error_type,
            )
        )
        await self._lifecycle.record(
            _event_from_metadata(
                metadata,
                TaskLifecycleKind.DEAD_LETTERED,
                error_type=error_type,
            )
        )
        metadata["terminal_pending"] = False
        metadata["terminal_error_type"] = None
        message.labels[_LABEL] = _encode_metadata(metadata)

    def execution_context(self) -> TaskExecutionContext:
        """Return the context established for the Taskiq callback in progress."""
        context = _CURRENT_EXECUTION_CONTEXT.get()
        if context is None:
            raise RuntimeError("No Taskiq execution context is active.")
        return context

    def _execution_context(
        self,
        metadata: Mapping[str, Any],
        message: TaskiqMessage,
    ) -> TaskExecutionContext:
        async def report(progress: Mapping[str, object]) -> None:
            await self._lifecycle.record(
                _event_from_metadata(
                    metadata,
                    TaskLifecycleKind.PROGRESS,
                    progress=progress,
                )
            )

        return TaskExecutionContext(
            task_id=UUID(metadata["task_id"]),
            task_name=metadata["task_name"],
            schema_version=metadata["schema_version"],
            attempt=metadata["attempt"],
            correlation_id=UUID(metadata["correlation_id"]),
            causation_id=(
                UUID(metadata["causation_id"])
                if metadata["causation_id"] is not None
                else None
            ),
            idempotency_key=_optional_label_string(
                message.labels.get(TASK_IDEMPOTENCY_KEY_LABEL),
                label="Task idempotency key",
            ),
            queue=metadata["queue"],
            worker_id=metadata["worker_id"],
            progress_reporter=report,
        )

    @staticmethod
    def _defer_terminal_failure(metadata: dict[str, Any], error_type: str) -> None:
        metadata["terminal_pending"] = True
        metadata["terminal_error_type"] = error_type

    def _validate_retry_middleware_order(self) -> None:
        """Require lifecycle errors to run before Taskiq decides to retry."""
        if self.broker is None:
            return
        middlewares = self.broker.middlewares
        lifecycle_index = middlewares.index(self)
        retry_middlewares = [
            middleware
            for middleware in middlewares
            if isinstance(middleware, (SimpleRetryMiddleware, SmartRetryMiddleware))
        ]
        if len(retry_middlewares) > 1:
            raise RuntimeError(
                "CacheTaskiqLifecycleMiddleware supports one Taskiq retry middleware."
            )
        if retry_middlewares and not isinstance(
            retry_middlewares[0],
            (CacheTaskiqSimpleRetryMiddleware, CacheTaskiqSmartRetryMiddleware),
        ):
            raise RuntimeError(
                "CacheTaskiqLifecycleMiddleware requires a cache-aware Taskiq retry "
                "middleware."
            )
        if (
            retry_middlewares
            and isinstance(retry_middlewares[0], SmartRetryMiddleware)
            and retry_middlewares[0].schedule_source is not None
        ):
            raise RuntimeError(
                "CacheTaskiqLifecycleMiddleware does not support SmartRetryMiddleware "
                "scheduled retries."
            )
        if (
            retry_middlewares
            and middlewares.index(retry_middlewares[0]) > lifecycle_index
        ):
            raise RuntimeError(
                "CacheTaskiqLifecycleMiddleware must be installed after "
                "Taskiq retry middleware."
            )

    def _will_retry(self, message: TaskiqMessage, exception: BaseException) -> bool:
        retry_middleware = self._retry_middleware()
        if retry_middleware is None or isinstance(
            exception,
            (NoResultError, TaskProgressError),
        ):
            return False
        allowed_types = retry_middleware.types_of_exceptions
        if allowed_types is not None and not isinstance(
            exception,
            tuple(allowed_types),
        ):
            return False
        retry_on_error = message.labels.get("retry_on_error")
        if isinstance(retry_on_error, str):
            retry_on_error = retry_on_error.lower() == "true"
        if retry_on_error is None:
            retry_on_error = retry_middleware.default_retry_label
        if not retry_on_error:
            return False
        retries = _retries(message) + 1
        max_retries = int(
            message.labels.get("max_retries", retry_middleware.default_retry_count)
        )
        return retries < max_retries

    def _retry_middleware(self) -> SimpleRetryMiddleware | SmartRetryMiddleware | None:
        if self.broker is None:
            return None
        retry_middlewares = [
            middleware
            for middleware in self.broker.middlewares
            if isinstance(middleware, (SimpleRetryMiddleware, SmartRetryMiddleware))
        ]
        if len(retry_middlewares) != 1:
            return None
        return retry_middlewares[0]

    async def _record_retry_scheduled(self, metadata: Mapping[str, Any]) -> None:
        retry_metadata = dict(metadata)
        retry_metadata["attempt"] = retry_metadata["retry_origin_attempt"]
        retry_metadata["delivery_attempt"] = retry_metadata[
            "retry_origin_delivery_attempt"
        ]
        retry_metadata["delivery_identity"] = retry_metadata[
            "retry_origin_delivery_identity"
        ]
        retry_metadata["worker_id"] = retry_metadata["retry_origin_worker_id"]
        task_id = UUID(retry_metadata["task_id"])
        status = await self._lifecycle.recovered_status(task_id)
        if (
            status is None
            or status.state is TaskState.RETRY_SCHEDULED
            or status.attempt != retry_metadata["attempt"]
            or status.state is not TaskState.RUNNING
            or status.delivery_attempt != retry_metadata["delivery_attempt"]
            or status._delivery_identity != retry_metadata["delivery_identity"]
            or status.worker_id != retry_metadata["worker_id"]
        ):
            return
        await self._lifecycle.record(
            _event_from_metadata(retry_metadata, TaskLifecycleKind.RETRY_SCHEDULED)
        )


class CacheTaskiqSimpleRetryMiddleware(SimpleRetryMiddleware):
    """Observe definitive cache rejection from Taskiq's standard retry policy."""

    def __init__(
        self,
        lifecycle: CacheTaskiqLifecycleMiddleware,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._lifecycle = lifecycle

    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
        exception: BaseException,
    ) -> None:
        if isinstance(exception, TaskProgressError):
            return
        try:
            await super().on_error(message, result, exception)
        except SendTaskError as error:
            await self._lifecycle.retry_handoff_rejected(message, error)
            raise


class CacheTaskiqSmartRetryMiddleware(SmartRetryMiddleware):
    """Observe definitive cache rejection from Taskiq's standard retry policy."""

    def __init__(
        self,
        lifecycle: CacheTaskiqLifecycleMiddleware,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self._lifecycle = lifecycle

    async def on_error(
        self,
        message: TaskiqMessage,
        result: TaskiqResult[Any],
        exception: BaseException,
    ) -> None:
        if isinstance(exception, TaskProgressError):
            return
        try:
            await super().on_error(message, result, exception)
        except SendTaskError as error:
            await self._lifecycle.retry_handoff_rejected(message, error)
            raise

    def make_delay(self, message: TaskiqMessage, retries: int) -> float:
        """Apply the declared Wybra retry policy to the Taskiq retry count."""
        delay = _retry_label_number(
            message,
            TASK_RETRY_INITIAL_DELAY_LABEL,
            default=self.default_delay,
            minimum=0,
        )
        multiplier = _retry_label_number(
            message,
            TASK_RETRY_BACKOFF_MULTIPLIER_LABEL,
            default=1,
            minimum=1,
        )
        maximum_delay = _optional_retry_label_number(
            message,
            TASK_RETRY_MAXIMUM_DELAY_LABEL,
            minimum=0,
        )
        if retries > 1 and delay > 0 and multiplier > 1:
            exponent = retries - 1
            if maximum_delay is not None:
                cap_exponent = (log(maximum_delay) - log(delay)) / log(multiplier)
                if exponent >= cap_exponent:
                    delay = maximum_delay
                else:
                    delay *= multiplier**exponent
            else:
                delay *= multiplier**exponent
        jitter = _retry_label_number(
            message,
            TASK_RETRY_JITTER_SECONDS_LABEL,
            default=0,
            minimum=0,
        )
        delay += jitter * random()
        return min(delay, maximum_delay) if maximum_delay is not None else delay


def _metadata(message: TaskiqMessage, policy: TaskiqLifecyclePolicy) -> dict[str, Any]:
    existing = message.labels.get(_LABEL)
    if existing is None:
        task_id = UUID(message.task_id)
        task_name = _label_string(
            message.labels.get(TASK_NAME_LABEL, message.task_name),
            label="Task name",
        )
        schema_version = _positive_label_int(
            message.labels.get(TASK_SCHEMA_VERSION_LABEL, policy.schema_version),
            label="Task schema version",
        )
        queue = _label_string(
            message.labels.get(TASK_QUEUE_LABEL, policy.queue),
            label="Task queue",
        )
        correlation_id = _uuid_label(
            message.labels.get(TASK_CORRELATION_ID_LABEL, str(task_id)),
            label="Task correlation ID",
        )
        causation_id = _optional_uuid_label(
            message.labels.get(TASK_CAUSATION_ID_LABEL),
            label="Task causation ID",
        )
        return {
            "version": _ENVELOPE_VERSION,
            "task_id": str(task_id),
            "task_name": task_name,
            "schema_version": schema_version,
            "queue": queue,
            "correlation_id": correlation_id,
            "causation_id": causation_id,
            "attempt": _retries(message) + 1,
            "delivery_attempt": None,
            "delivery_identity": None,
            "worker_id": None,
            "submitted": False,
            "retry_pending": False,
            "retry_origin_attempt": None,
            "retry_origin_delivery_attempt": None,
            "retry_origin_delivery_identity": None,
            "retry_origin_worker_id": None,
            "terminal_pending": False,
            "terminal_error_type": None,
        }
    if isinstance(existing, str):
        try:
            existing = json.loads(existing)
        except json.JSONDecodeError as exc:
            raise ValueError("Taskiq lifecycle metadata is invalid.") from exc
    if (
        not isinstance(existing, dict)
        or set(existing) != _METADATA_FIELDS
        or existing.get("version") != _ENVELOPE_VERSION
    ):
        raise ValueError("Taskiq lifecycle metadata is invalid.")
    metadata = dict(existing)
    _validate_metadata(metadata, message, policy)
    return metadata


def _encode_metadata(metadata: Mapping[str, Any]) -> str:
    return json.dumps(metadata, separators=(",", ":"), sort_keys=True)


def _retries(message: TaskiqMessage) -> int:
    value = message.labels.get("_retries", 0)
    if isinstance(value, str) and value.isdecimal():
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError("Taskiq retry metadata is invalid.")
    return value


def _delivery_attempt(message: TaskiqMessage) -> int | None:
    value = message.labels.get(DELIVERY_ATTEMPT_LABEL)
    if value is None:
        return None
    if isinstance(value, str) and value.isdecimal():
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError("Taskiq delivery metadata is invalid.")
    return value


def _delivery_identity(message: TaskiqMessage) -> str | None:
    value = message.labels.get(DELIVERY_IDENTITY_LABEL)
    if value is None:
        return None
    return _label_string(value, label="Taskiq delivery identity")


def _validate_metadata(
    metadata: Mapping[str, Any],
    message: TaskiqMessage,
    policy: TaskiqLifecyclePolicy,
) -> None:
    try:
        task_id = UUID(metadata["task_id"])
        UUID(metadata["correlation_id"])
        causation_id = metadata["causation_id"]
        if causation_id is not None:
            UUID(causation_id)
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Taskiq lifecycle metadata is invalid.") from exc
    attempt = metadata.get("attempt")
    delivery_attempt = metadata.get("delivery_attempt")
    delivery_identity = metadata.get("delivery_identity")
    worker_id = metadata.get("worker_id")
    retry_origin_attempt = metadata.get("retry_origin_attempt")
    retry_origin_delivery_attempt = metadata.get("retry_origin_delivery_attempt")
    retry_origin_delivery_identity = metadata.get("retry_origin_delivery_identity")
    retry_origin_worker_id = metadata.get("retry_origin_worker_id")
    terminal_pending = metadata.get("terminal_pending")
    terminal_error_type = metadata.get("terminal_error_type")
    if (
        task_id != UUID(message.task_id)
        or not isinstance(metadata.get("task_name"), str)
        or not metadata["task_name"].strip()
        or isinstance(metadata.get("schema_version"), bool)
        or not isinstance(metadata.get("schema_version"), int)
        or metadata["schema_version"] < 1
        or not isinstance(metadata.get("queue"), str)
        or not metadata["queue"].strip()
        or isinstance(attempt, bool)
        or not isinstance(attempt, int)
        or attempt < 1
        or (
            delivery_attempt is not None
            and (
                isinstance(delivery_attempt, bool)
                or not isinstance(delivery_attempt, int)
                or delivery_attempt < 1
            )
        )
        or not _valid_optional_delivery_identity(delivery_identity)
        or not isinstance(metadata.get("submitted"), bool)
        or not isinstance(metadata.get("retry_pending"), bool)
        or not isinstance(terminal_pending, bool)
        or (
            terminal_pending
            and (not isinstance(terminal_error_type, str) or not terminal_error_type)
        )
        or (not terminal_pending and terminal_error_type is not None)
        or (
            worker_id is not None
            and (not isinstance(worker_id, str) or not worker_id.strip())
        )
        or not _valid_retry_origin(
            retry_pending=metadata.get("retry_pending"),
            attempt=retry_origin_attempt,
            delivery_attempt=retry_origin_delivery_attempt,
            delivery_identity=retry_origin_delivery_identity,
            worker_id=retry_origin_worker_id,
        )
    ):
        raise ValueError("Taskiq lifecycle metadata is invalid.")


def _label_string(value: object, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} is invalid.")
    return value.strip()


def _optional_label_string(value: object, *, label: str) -> str | None:
    if value is None:
        return None
    return _label_string(value, label=label)


def _positive_label_int(value: object, *, label: str) -> int:
    if isinstance(value, str) and value.isdecimal():
        value = int(value)
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(f"{label} is invalid.")
    return value


def _uuid_label(value: object, *, label: str) -> str:
    try:
        return str(UUID(_label_string(value, label=label)))
    except ValueError as exc:
        raise ValueError(f"{label} is invalid.") from exc


def _optional_uuid_label(value: object, *, label: str) -> str | None:
    if value is None:
        return None
    return _uuid_label(value, label=label)


def _retry_label_number(
    message: TaskiqMessage,
    label: str,
    *,
    default: float,
    minimum: float,
) -> float:
    return _retry_number(
        message.labels.get(label, default),
        label=label,
        minimum=minimum,
    )


def _optional_retry_label_number(
    message: TaskiqMessage,
    label: str,
    *,
    minimum: float,
) -> float | None:
    value = message.labels.get(label)
    if value is None:
        return None
    return _retry_number(value, label=label, minimum=minimum)


def _retry_number(value: object, *, label: str, minimum: float) -> float:
    if isinstance(value, bool) or not isinstance(value, str | int | float):
        raise ValueError(f"Taskiq retry {label} is invalid.")
    try:
        parsed = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ValueError(f"Taskiq retry {label} is invalid.") from exc
    if not isfinite(parsed) or parsed < minimum:
        raise ValueError(f"Taskiq retry {label} is invalid.")
    return parsed


def _valid_retry_origin(
    *,
    retry_pending: object,
    attempt: object,
    delivery_attempt: object,
    delivery_identity: object,
    worker_id: object,
) -> bool:
    if retry_pending is False:
        return (
            attempt is None
            and delivery_attempt is None
            and delivery_identity is None
            and worker_id is None
        )
    return (
        isinstance(attempt, int)
        and not isinstance(attempt, bool)
        and attempt >= 1
        and _valid_optional_delivery_attempt(delivery_attempt)
        and _valid_optional_delivery_identity(delivery_identity)
        and _valid_optional_worker_id(worker_id)
    )


def _valid_optional_delivery_attempt(value: object) -> bool:
    return value is None or (
        not isinstance(value, bool) and isinstance(value, int) and value >= 1
    )


def _valid_optional_delivery_identity(value: object) -> bool:
    return value is None or (isinstance(value, str) and bool(value.strip()))


def _valid_optional_worker_id(value: object) -> bool:
    return value is None or (isinstance(value, str) and bool(value.strip()))


def _clear_retry_origin(metadata: dict[str, Any]) -> None:
    metadata["retry_origin_attempt"] = None
    metadata["retry_origin_delivery_attempt"] = None
    metadata["retry_origin_delivery_identity"] = None
    metadata["retry_origin_worker_id"] = None


def _is_pending_retry_delivery(
    status: TaskStatus,
    metadata: Mapping[str, Any],
) -> bool:
    """Return whether a queued retry still belongs to the current lifecycle state."""
    return (
        metadata["retry_pending"]
        and status.attempt == metadata["retry_origin_attempt"]
        and status.delivery_attempt == metadata["retry_origin_delivery_attempt"]
        and status._delivery_identity == metadata["retry_origin_delivery_identity"]
        and status.worker_id == metadata["retry_origin_worker_id"]
    )


def _event_from_metadata(
    metadata: Mapping[str, Any],
    kind: TaskLifecycleKind,
    *,
    error_type: str | None = None,
    progress: Mapping[str, object] | None = None,
) -> TaskLifecycleEvent:
    try:
        return TaskLifecycleEvent(
            kind=kind,
            task_id=UUID(metadata["task_id"]),
            task_name=metadata["task_name"],
            schema_version=metadata["schema_version"],
            queue=metadata["queue"],
            correlation_id=UUID(metadata["correlation_id"]),
            causation_id=UUID(metadata["causation_id"])
            if metadata["causation_id"]
            else None,
            attempt=metadata["attempt"],
            delivery_attempt=metadata["delivery_attempt"],
            _delivery_identity=metadata["delivery_identity"],
            worker_id=metadata["worker_id"],
            error_type=error_type,
            progress=progress,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Taskiq lifecycle metadata is invalid.") from exc


def _encode_event(event: TaskLifecycleEvent) -> bytes:
    return _encode({"version": _ENVELOPE_VERSION, "event": _event_data(event)})


def _decode_event(payload: bytes) -> TaskLifecycleEvent:
    data = _decode(payload, fields=_ENVELOPE_FIELDS).get("event")
    if not isinstance(data, dict):
        raise ValueError("Task lifecycle envelope is invalid.")
    try:
        _validate_event_data(data)
        return TaskLifecycleEvent(
            kind=TaskLifecycleKind(data["kind"]),
            task_id=UUID(data["task_id"]),
            task_name=data["task_name"],
            schema_version=data["schema_version"],
            queue=data["queue"],
            correlation_id=UUID(data["correlation_id"]),
            causation_id=UUID(data["causation_id"]) if data["causation_id"] else None,
            attempt=data["attempt"],
            delivery_attempt=data["delivery_attempt"],
            _delivery_identity=data["delivery_identity"],
            worker_id=data["worker_id"],
            progress=data["progress"],
            error_type=data["error_type"],
            occurred_at=data["occurred_at"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Task lifecycle envelope is invalid.") from exc


def _encode_status(status: TaskStatus, position: StreamPosition) -> bytes:
    return _encode(
        {
            "version": _ENVELOPE_VERSION,
            "position": position.value,
            "status": _status_data(status),
        }
    )


def _public_status(status: TaskStatus) -> TaskStatus:
    """Return status data without cache-private delivery provenance."""

    return replace(status, _delivery_identity=None)


def _public_event(event: TaskLifecycleEvent) -> TaskLifecycleEvent:
    """Return lifecycle data without cache-private delivery provenance."""

    return replace(event, _delivery_identity=None)


def _decode_stored_status(payload: bytes) -> tuple[TaskStatus, StreamPosition]:
    envelope = _decode(payload, fields=_STATUS_ENVELOPE_FIELDS)
    data = envelope.get("status")
    position = envelope.get("position")
    if not isinstance(data, dict) or isinstance(position, bool):
        raise ValueError("Task status envelope is invalid.")
    try:
        _validate_status_data(data)
        stream_position = StreamPosition(position)
        status = TaskStatus(
            task_id=UUID(data["task_id"]),
            task_name=data["task_name"],
            schema_version=data["schema_version"],
            state=TaskState(data["state"]),
            queue=data["queue"],
            correlation_id=UUID(data["correlation_id"]),
            causation_id=UUID(data["causation_id"]) if data["causation_id"] else None,
            attempt=data["attempt"],
            submitted_at=data["submitted_at"],
            updated_at=data["updated_at"],
            worker_id=data["worker_id"],
            progress=data["progress"],
            error_type=data["error_type"],
            delivery_attempt=data["delivery_attempt"],
            _delivery_identity=data["delivery_identity"],
            terminal_at=data["terminal_at"],
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError("Task status envelope is invalid.") from exc
    return status, stream_position


def _validate_event_data(data: Mapping[str, object]) -> None:
    if set(data) != _EVENT_FIELDS:
        raise ValueError("Task lifecycle envelope is invalid.")
    _validate_identity_data(data)
    _validate_attempt_data(data)
    _validate_optional_delivery_attempt(data.get("delivery_attempt"))
    _validate_optional_string(data.get("delivery_identity"))
    _validate_optional_string(data.get("worker_id"))
    _validate_optional_string(data.get("error_type"))
    progress = data.get("progress")
    if progress is not None and not isinstance(progress, Mapping):
        raise ValueError("Task lifecycle envelope is invalid.")
    occurred_at = data.get("occurred_at")
    if (
        isinstance(occurred_at, bool)
        or not isinstance(occurred_at, int | float)
        or not isfinite(occurred_at)
    ):
        raise ValueError("Task lifecycle envelope is invalid.")


def _validate_status_data(data: Mapping[str, object]) -> None:
    if set(data) != _STATUS_FIELDS:
        raise ValueError("Task status envelope is invalid.")
    _validate_identity_data(data)
    _validate_attempt_data(data)
    _validate_optional_delivery_attempt(data.get("delivery_attempt"))
    _validate_optional_string(data.get("delivery_identity"))
    _validate_optional_string(data.get("worker_id"))
    _validate_optional_string(data.get("error_type"))
    progress = data.get("progress")
    if progress is not None and not isinstance(progress, Mapping):
        raise ValueError("Task status envelope is invalid.")
    for value in (
        data.get("submitted_at"),
        data.get("updated_at"),
        data.get("terminal_at"),
    ):
        if value is None:
            continue
        if (
            isinstance(value, bool)
            or not isinstance(value, int | float)
            or not isfinite(value)
        ):
            raise ValueError("Task status envelope is invalid.")


def _validate_identity_data(data: Mapping[str, object]) -> None:
    for key in ("task_name", "queue"):
        value = data.get(key)
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Task lifecycle envelope is invalid.")
    schema_version = data.get("schema_version")
    if (
        isinstance(schema_version, bool)
        or not isinstance(schema_version, int)
        or schema_version < 1
    ):
        raise ValueError("Task lifecycle envelope is invalid.")


def _validate_attempt_data(data: Mapping[str, object]) -> None:
    attempt = data.get("attempt")
    if isinstance(attempt, bool) or not isinstance(attempt, int) or attempt < 1:
        raise ValueError("Task lifecycle envelope is invalid.")


def _validate_optional_string(value: object) -> None:
    if value is not None and (not isinstance(value, str) or not value.strip()):
        raise ValueError("Task lifecycle envelope is invalid.")


def _validate_optional_delivery_attempt(value: object) -> None:
    if value is not None and (
        isinstance(value, bool) or not isinstance(value, int) or value < 1
    ):
        raise ValueError("Task lifecycle envelope is invalid.")


def _event_data(event: TaskLifecycleEvent) -> dict[str, object]:
    return {
        "kind": event.kind.value,
        "task_id": str(event.task_id),
        "task_name": event.task_name,
        "schema_version": event.schema_version,
        "queue": event.queue,
        "correlation_id": str(event.correlation_id),
        "causation_id": str(event.causation_id) if event.causation_id else None,
        "attempt": event.attempt,
        "delivery_attempt": event.delivery_attempt,
        "delivery_identity": event._delivery_identity,
        "worker_id": event.worker_id,
        "progress": dict(event.progress) if event.progress is not None else None,
        "error_type": event.error_type,
        "occurred_at": event.occurred_at,
    }


def _status_data(status: TaskStatus) -> dict[str, object]:
    return {
        "task_id": str(status.task_id),
        "task_name": status.task_name,
        "schema_version": status.schema_version,
        "state": status.state.value,
        "queue": status.queue,
        "correlation_id": str(status.correlation_id),
        "causation_id": str(status.causation_id) if status.causation_id else None,
        "attempt": status.attempt,
        "delivery_attempt": status.delivery_attempt,
        "delivery_identity": status._delivery_identity,
        "submitted_at": status.submitted_at,
        "updated_at": status.updated_at,
        "worker_id": status.worker_id,
        "progress": dict(status.progress) if status.progress is not None else None,
        "error_type": status.error_type,
        "terminal_at": status.terminal_at,
    }


def _encode(value: Mapping[str, object]) -> bytes:
    return json.dumps(value, separators=(",", ":"), sort_keys=True).encode()


def _decode(payload: bytes, *, fields: frozenset[str]) -> dict[str, Any]:
    try:
        value = json.loads(payload)
    except (TypeError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError("Task lifecycle envelope is invalid.") from exc
    if (
        not isinstance(value, dict)
        or set(value) != fields
        or value.get("version") != _ENVELOPE_VERSION
    ):
        raise ValueError("Task lifecycle envelope is invalid.")
    return value


def _status_key(task_id: UUID) -> str:
    return f"status:{task_id}"


def _require_lease(lease_lost: Event) -> None:
    if lease_lost.is_set():
        raise CacheConflictError("Task lifecycle projection lease was lost.")


def _project_target_event(
    projection: TaskStatusProjection,
    task_id: UUID,
    target_event: TaskLifecycleEvent | None,
) -> TaskStatus | None:
    if target_event is None:
        return projection.status(task_id)
    if target_event.task_id != task_id:
        return projection.status(task_id)
    return projection.apply(target_event)


def _dead_letter_event(status: TaskStatus) -> TaskLifecycleEvent:
    return TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.DEAD_LETTERED,
        task_id=status.task_id,
        task_name=status.task_name,
        schema_version=status.schema_version,
        queue=status.queue,
        correlation_id=status.correlation_id,
        causation_id=status.causation_id,
        attempt=status.attempt,
        delivery_attempt=status.delivery_attempt,
        _delivery_identity=status._delivery_identity,
        worker_id=status.worker_id,
        error_type=status.error_type,
        occurred_at=status.updated_at,
    )


def _failed_event(
    status: TaskStatus,
    *,
    error_type: str | None = None,
) -> TaskLifecycleEvent:
    return TaskLifecycleEvent.new(
        kind=TaskLifecycleKind.FAILED,
        task_id=status.task_id,
        task_name=status.task_name,
        schema_version=status.schema_version,
        queue=status.queue,
        correlation_id=status.correlation_id,
        causation_id=status.causation_id,
        attempt=status.attempt,
        delivery_attempt=status.delivery_attempt,
        _delivery_identity=status._delivery_identity,
        worker_id=status.worker_id,
        error_type=status.error_type if error_type is None else error_type,
    )


def _is_abandoned_failed_attempt(
    status: TaskStatus,
    now: float,
    policy: TaskiqLifecyclePolicy,
) -> bool:
    return (
        status.state is TaskState.RUNNING
        and status.error_type is not None
        and now - status.updated_at >= policy.active_status_timeout_seconds
    )


def _is_abandoned_active_status(
    status: TaskStatus,
    now: float,
    policy: TaskiqLifecyclePolicy,
) -> bool:
    return (
        status.state not in _TERMINAL_STATES
        and now - status.updated_at >= policy.active_status_timeout_seconds
    )


def _is_later_delivery(
    status: TaskStatus,
    delivery_attempt: object,
    delivery_identity: object,
) -> bool:
    if (
        status._delivery_identity is not None
        and status._delivery_identity != delivery_identity
    ):
        return True
    return (
        isinstance(delivery_attempt, int)
        and not isinstance(delivery_attempt, bool)
        and status.delivery_attempt is not None
        and status.delivery_attempt > delivery_attempt
    )


def _is_idempotent_event(status: TaskStatus, event: TaskLifecycleEvent) -> bool:
    if not _matches_identity(status, event) or event.attempt != status.attempt:
        return False
    if status.state.value == event.kind.value:
        return True
    if status.state is TaskState.RUNNING and event.kind is TaskLifecycleKind.PROGRESS:
        return (
            status.worker_id == event.worker_id
            and status.delivery_attempt == event.delivery_attempt
            and status._delivery_identity == event._delivery_identity
        )
    return _is_duplicate_start(status, event)


def _is_duplicate_start(status: TaskStatus, event: TaskLifecycleEvent) -> bool:
    return (
        status.state is TaskState.RUNNING
        and event.kind is TaskLifecycleKind.STARTED
        and _matches_identity(status, event)
        and event.attempt == status.attempt
        and event.delivery_attempt == status.delivery_attempt
        and event._delivery_identity == status._delivery_identity
        and status.worker_id == event.worker_id
    )


def _apply_event(
    status: TaskStatus | None,
    event: TaskLifecycleEvent,
) -> TaskStatus:
    projection = TaskStatusProjection(retention_seconds=None)
    if status is not None:
        projection.restore(status)
    return projection.apply(event)


def _matches_identity(status: TaskStatus, event: TaskLifecycleEvent) -> bool:
    return (
        event.task_name == status.task_name
        and event.schema_version == status.schema_version
        and event.queue == status.queue
        and event.correlation_id == status.correlation_id
        and event.causation_id == status.causation_id
    )


__all__ = (
    "CacheTaskLifecycle",
    "CacheTaskiqLifecycleMiddleware",
    "CacheTaskiqSimpleRetryMiddleware",
    "CacheTaskiqSmartRetryMiddleware",
    "TaskiqLifecyclePolicy",
)
