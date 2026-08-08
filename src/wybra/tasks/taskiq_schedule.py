"""Taskiq schedule source backed by Wybra's fenced schedule capability."""

from __future__ import annotations

import asyncio
import json
import math
from collections import OrderedDict
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from logging import getLogger
from uuid import UUID, uuid4, uuid5

import pytz
from croniter import croniter
from taskiq import ScheduledTask
from taskiq.abc.schedule_source import ScheduleSource
from taskiq.exceptions import ScheduledTaskCancelledError

from wybra.cache import (
    MAX_CACHE_FEATURE_LIMIT,
    CacheConflictError,
    CacheRevision,
    CacheTimeCapability,
    ScheduleCacheCapability,
    ScheduleClaim,
    ScheduleCursor,
)
from wybra.utils.safety import truncate_safe_string

_DEFAULT_OWNER = "taskiq-scheduler"
_ENVELOPE_VERSION = 1
_MINUTE_SECONDS = 60.0
_PENDING_HELD_CHECK_LIMIT = 100
_DISPATCH_TIME = datetime(1970, 1, 1, tzinfo=UTC)
_DISPATCH_NAMESPACE = UUID("3e30ddfc-9f33-4b42-9073-b53e00fbb66a")
_logger = getLogger(__name__)


class _InvalidScheduleEnvelopeError(ValueError):
    """Raised without durable payload content for invalid adapter state."""


class _RefreshIntervalMismatchError(ValueError):
    """Raised when a persisted interval cannot meet the local source cadence."""


class _UnsupportedScheduleEnvelopeError(ValueError):
    """Raised without durable payload content for an unsupported schema version."""


@dataclass(frozen=True, slots=True)
class TaskiqSchedulePolicy:
    """Controls durable cache-backed Taskiq scheduling."""

    claimant: str
    claim_ttl_seconds: float
    owner: str = _DEFAULT_OWNER
    due_limit: int = 100
    scan_page_limit: int = 100
    scan_limit: int = 1_000
    timezone: str = "UTC"
    catch_up_limit: int = 1
    source_refresh_interval_seconds: int = 60

    def __post_init__(self) -> None:
        _validate_policy_resource(self.claimant, label="Schedule claimant")
        _validate_policy_resource(self.owner, label="Schedule owner")
        if (
            isinstance(self.claim_ttl_seconds, bool)
            or not isinstance(self.claim_ttl_seconds, int | float)
            or not math.isfinite(self.claim_ttl_seconds)
            or self.claim_ttl_seconds <= 0
        ):
            raise ValueError("Schedule claim TTL must be positive.")
        _validate_limit(self.due_limit, label="Schedule due limit")
        _validate_limit(self.scan_page_limit, label="Schedule scan page limit")
        _validate_limit(self.scan_limit, label="Schedule scan limit")
        _validate_limit(self.catch_up_limit, label="Catch-up limit")
        _validate_source_refresh_interval(self.source_refresh_interval_seconds)
        _timezone(self.timezone)


@dataclass(frozen=True, slots=True)
class _Envelope:
    task: ScheduledTask
    timezone: str
    catch_up_limit: int
    pending_due_at: tuple[float, ...] = ()


@dataclass(frozen=True, slots=True)
class _PendingDispatch:
    claim: ScheduleClaim
    envelope: _Envelope
    handoff_in_flight: bool = False


class CacheTaskiqScheduleSource(ScheduleSource):
    """Adapt durable cache schedules to Taskiq's schedule-source protocol."""

    def __init__(
        self,
        schedules: ScheduleCacheCapability,
        *,
        policy: TaskiqSchedulePolicy,
        cache_time: CacheTimeCapability,
    ) -> None:
        _validate_schedule_capacity(schedules.maximum_records)
        self._schedules = schedules
        self._policy = policy
        self._cache_time = cache_time
        self._pending: dict[str, _PendingDispatch] = {}
        self._pending_prune_cursor = 0
        self._refreshing: dict[str, ScheduleClaim] = {}
        self._deferred_revisions: OrderedDict[str, CacheRevision] = OrderedDict()
        self._scan_cursor: ScheduleCursor | None = None
        self._state_lock = asyncio.Lock()
        self._idle = asyncio.Event()
        self._idle.set()
        self._active_operations = 0
        self._closing = False

    async def _begin_operation(self, *, allow_closing: bool = False) -> bool:
        async with self._state_lock:
            if self._closing and not allow_closing:
                return False
            self._active_operations += 1
            self._idle.clear()
            return True

    async def _end_operation(self) -> None:
        completion = asyncio.create_task(self._decrement_operation())
        await asyncio.shield(completion)

    async def _decrement_operation(self) -> None:
        async with self._state_lock:
            self._active_operations -= 1
            if self._active_operations == 0:
                self._idle.set()

    async def _is_closing(self) -> bool:
        async with self._state_lock:
            return self._closing

    async def add_schedule(
        self,
        schedule: ScheduledTask,
        *,
        timezone: str | None = None,
        catch_up_limit: int | None = None,
    ) -> None:
        _validate_schedule(schedule)
        _validate_schedule_refresh_interval(schedule, self._policy)
        zone_name = self._policy.timezone if timezone is None else timezone
        _timezone(zone_name)
        limit = (
            self._policy.catch_up_limit if catch_up_limit is None else catch_up_limit
        )
        _validate_limit(limit, label="Catch-up limit")
        envelope = _Envelope(schedule, zone_name, limit)
        now = await self._cache_time.refresh() if schedule.time is None else None
        record = await self._schedules.create(
            self._policy.owner,
            schedule.schedule_id,
            _encode(envelope),
            next_due_at=_initial_due_at(
                schedule,
                zone_name,
                now=now,
            ),
            interval_seconds=_interval_seconds(schedule),
        )
        if record is None:
            raise ValueError(f"Schedule {schedule.schedule_id!r} already exists.")
        self._scan_cursor = None

    async def delete_schedule(self, schedule_id: str) -> None:
        await self._schedules.delete(self._policy.owner, schedule_id)
        self._pending = {
            dispatch_id: pending
            for dispatch_id, pending in self._pending.items()
            if pending.claim.record.identity != schedule_id
        }
        self._deferred_revisions.pop(schedule_id, None)
        self._scan_cursor = None

    async def get_schedules(self) -> list[ScheduledTask]:
        if not await self._begin_operation():
            return []
        try:
            return await self._get_schedules()
        finally:
            await self._end_operation()

    async def _get_schedules(self) -> list[ScheduledTask]:
        ready: list[ScheduledTask] = []
        staged: list[str] = []
        refreshing: dict[str, ScheduleClaim] = {}
        try:
            return await self._refresh_schedules(ready, staged, refreshing)
        except asyncio.CancelledError as error:
            cleanup_failures = await self._release_refreshing(refreshing)
            cleanup_failures.extend(await self._release_staged(staged))
            if cleanup_failures:
                error.add_note(
                    f"{len(cleanup_failures)} schedule claim release "
                    "operation(s) failed during cancellation."
                )
            raise
        except Exception as error:
            cleanup_failures = await self._release_refreshing(refreshing)
            cleanup_failures.extend(await self._release_staged(staged))
            if cleanup_failures:
                raise ExceptionGroup(
                    "Taskiq schedule refresh abort cleanup failed.",
                    [error, *cleanup_failures],
                ) from error
            raise

    async def _refresh_schedules(
        self,
        ready: list[ScheduledTask],
        staged: list[str],
        refreshing: dict[str, ScheduleClaim],
    ) -> list[ScheduledTask]:
        if await self._prune_stale_pending():
            self._scan_cursor = None
        now = await self._cache_time.refresh()
        cursor = self._scan_cursor
        scanned = 0
        incompatible_count = 0
        unsupported_count = 0
        incompatible_identities: list[str] = []
        unsupported_identities: list[str] = []
        advanced_identities: set[str] = set()
        preserve_scan_cursor = True
        exhausted_continuation = False
        wrapped_scan = False
        while len(ready) < self._policy.due_limit and scanned < self._policy.scan_limit:
            page_limit = min(
                self._policy.scan_page_limit,
                self._policy.scan_limit - scanned,
            )
            records = await self._schedules.due(
                self._policy.owner,
                before=now,
                limit=page_limit,
                after=cursor,
            )
            if not records:
                if cursor is not None and not wrapped_scan:
                    cursor = None
                    wrapped_scan = True
                    continue
                exhausted_continuation = cursor is not None
                break
            for record in records:
                if len(ready) >= self._policy.due_limit:
                    break
                cursor = ScheduleCursor(record.next_due_at, record.identity)
                scanned += 1
                if record.identity in advanced_identities:
                    continue
                if self._deferred_revisions.get(record.identity) == record.revision:
                    self._deferred_revisions.move_to_end(record.identity)
                    continue
                claim = await self._schedules.claim(
                    self._policy.owner,
                    record.identity,
                    self._policy.claimant,
                    ttl=self._policy.claim_ttl_seconds,
                )
                if claim is None:
                    preserve_scan_cursor = False
                    continue
                self._track_refreshing(claim, refreshing)
                if await self._is_closing():
                    return []
                try:
                    envelope = _decode(
                        claim.record.payload,
                        identity=claim.record.identity,
                        next_due_at=claim.record.next_due_at,
                        interval_seconds=claim.record.interval_seconds,
                    )
                    _validate_schedule_refresh_interval(envelope.task, self._policy)
                    self._deferred_revisions.pop(claim.record.identity, None)
                    if envelope.task.cron is not None:
                        try:
                            prepared = await self._prepare_cron_claim(
                                claim, envelope, now
                            )
                        except CacheConflictError:
                            self._untrack_refreshing(claim, refreshing)
                            if not self._deferred_revisions:
                                preserve_scan_cursor = False
                            continue
                        if prepared is None:
                            advanced_identities.add(claim.record.identity)
                            self._untrack_refreshing(claim, refreshing)
                            if not self._deferred_revisions:
                                preserve_scan_cursor = False
                            continue
                        claim, envelope = prepared
                    dispatch = self._add_pending(claim, envelope, ready)
                    staged.append(dispatch.schedule_id)
                    self._untrack_refreshing(claim, refreshing)
                    if not self._deferred_revisions:
                        preserve_scan_cursor = False
                except _RefreshIntervalMismatchError:
                    released, cleanup_failures = await self._release_claim(claim)
                    if released:
                        self._untrack_refreshing(claim, refreshing)
                    if cleanup_failures:
                        raise ExceptionGroup(
                            "Taskiq schedule deferral cleanup failed.",
                            cleanup_failures,
                        ) from None
                    self._remember_deferred(claim)
                    incompatible_count += 1
                    incompatible_identities.append(claim.record.identity)
                    continue
                except _UnsupportedScheduleEnvelopeError:
                    released, cleanup_failures = await self._release_claim(claim)
                    if released:
                        self._untrack_refreshing(claim, refreshing)
                    if cleanup_failures:
                        raise ExceptionGroup(
                            "Taskiq schedule deferral cleanup failed.",
                            cleanup_failures,
                        ) from None
                    self._remember_deferred(claim)
                    unsupported_count += 1
                    unsupported_identities.append(claim.record.identity)
                    continue
                except _InvalidScheduleEnvelopeError:
                    self._deferred_revisions.pop(claim.record.identity, None)
                    try:
                        await self._schedules.discard(claim)
                    except CacheConflictError:
                        self._untrack_refreshing(claim, refreshing)
                        continue
                    except Exception as discard_error:
                        released, cleanup_failures = await self._release_claim(claim)
                        if released:
                            self._untrack_refreshing(claim, refreshing)
                        cleanup_failures.extend(await self._release_staged(staged))
                        if cleanup_failures:
                            raise ExceptionGroup(
                                "Taskiq schedule processing and invalid-envelope "
                                "cleanup failed.",
                                [discard_error, *cleanup_failures],
                            ) from discard_error
                        raise
                    else:
                        self._untrack_refreshing(claim, refreshing)
                    preserve_scan_cursor = False
                    _logger.warning(
                        "Discarded invalid Taskiq schedule envelope for %s.",
                        _diagnostic_identity(claim.record.identity),
                    )
                    continue
                except Exception as error:
                    released, cleanup_failures = await self._release_claim(claim)
                    if released:
                        self._untrack_refreshing(claim, refreshing)
                    cleanup_failures.extend(await self._release_staged(staged))
                    if cleanup_failures:
                        raise ExceptionGroup(
                            "Taskiq schedule refresh and claim cleanup failed.",
                            [error, *cleanup_failures],
                        ) from error
                    raise
        self._scan_cursor = (
            cursor if preserve_scan_cursor and not exhausted_continuation else None
        )
        if incompatible_count:
            _logger.warning(
                "Deferred %d Taskiq schedules incompatible with the source refresh "
                "interval: %s.",
                incompatible_count,
                _diagnostic_identities(incompatible_identities),
            )
        if unsupported_count:
            _logger.warning(
                "Deferred %d Taskiq schedules with an unsupported envelope "
                "version: %s.",
                unsupported_count,
                _diagnostic_identities(unsupported_identities),
            )
        return ready

    async def pre_send(self, task: ScheduledTask) -> None:
        if not await self._begin_operation():
            raise ScheduledTaskCancelledError
        try:
            pending = self._pending.get(task.schedule_id)
            if pending is None:
                raise ScheduledTaskCancelledError
            if not await self._schedules.held(pending.claim):
                self._pending.pop(task.schedule_id, None)
                self._scan_cursor = None
                raise ScheduledTaskCancelledError
            if await self._is_closing():
                raise ScheduledTaskCancelledError
            self._pending[task.schedule_id] = replace(pending, handoff_in_flight=True)
        finally:
            await self._end_operation()

    async def post_send(self, task: ScheduledTask) -> None:
        if not await self._begin_operation(allow_closing=True):
            return
        try:
            pending = self._pending.get(task.schedule_id)
            if pending is None:
                return
            claim = pending.claim
            envelope = pending.envelope
            if not await self._schedules.held(claim):
                self._pending.pop(task.schedule_id, None)
                self._scan_cursor = None
                return
            remaining = envelope.pending_due_at[1:]
            try:
                if remaining:
                    await self._schedules.advance(
                        claim,
                        _encode(replace(envelope, pending_due_at=remaining)),
                        next_due_at=remaining[0],
                    )
                elif envelope.task.cron is not None:
                    await self._schedules.advance(
                        claim,
                        _encode(replace(envelope, pending_due_at=())),
                        next_due_at=_next_cron_due_at(
                            envelope.task.cron or "",
                            envelope.pending_due_at[-1],
                            envelope.timezone,
                        ),
                    )
                else:
                    await self._schedules.complete(claim)
            except CacheConflictError:
                pass
            self._pending.pop(task.schedule_id, None)
        finally:
            await self._end_operation()

    async def shutdown(self) -> None:
        async with self._state_lock:
            self._closing = True
        await self._idle.wait()
        pending = tuple(self._pending.items())
        refreshing = tuple(self._refreshing.items())
        failures: list[Exception] = []
        cancellation: asyncio.CancelledError | None = None
        for dispatch_id, pending_dispatch in pending:
            if pending_dispatch.handoff_in_flight:
                continue
            try:
                await self._schedules.release(pending_dispatch.claim)
            except CacheConflictError:
                self._pending.pop(dispatch_id, None)
            except Exception as exc:
                failures.append(exc)
            except asyncio.CancelledError as error:
                cancellation = error
            else:
                self._pending.pop(dispatch_id, None)
        for token, claim in refreshing:
            try:
                await self._schedules.release(claim)
            except CacheConflictError:
                self._refreshing.pop(token, None)
            except Exception as exc:
                failures.append(exc)
            except asyncio.CancelledError as error:
                cancellation = error
            else:
                self._refreshing.pop(token, None)
        if cancellation is not None:
            if failures:
                cancellation.add_note(
                    f"{len(failures)} schedule claim release operation(s) failed "
                    "during cancellation."
                )
            raise cancellation
        if failures:
            raise ExceptionGroup("Taskiq schedule claim release failed.", failures)

    def _add_pending(
        self,
        claim: ScheduleClaim,
        envelope: _Envelope,
        ready: list[ScheduledTask],
    ) -> ScheduledTask:
        task = _dispatch_task(envelope.task, claim)
        self._pending = {
            dispatch_id: pending
            for dispatch_id, pending in self._pending.items()
            if pending.claim.record.identity != claim.record.identity
        }
        self._pending[task.schedule_id] = _PendingDispatch(claim, envelope)
        ready.append(task)
        return task

    async def _prune_stale_pending(self) -> bool:
        pending = tuple(self._pending.items())
        if not pending:
            self._pending_prune_cursor = 0
            return False
        start = self._pending_prune_cursor % len(pending)
        limit = min(_PENDING_HELD_CHECK_LIMIT, len(pending))
        removed = False
        for offset in range(limit):
            dispatch_id, pending_entry = pending[(start + offset) % len(pending)]
            claim = pending_entry.claim
            if (
                not await self._schedules.held(claim)
                and self._pending.get(dispatch_id) is pending_entry
            ):
                self._pending.pop(dispatch_id, None)
                removed = True
        self._pending_prune_cursor = (
            (start + limit) % len(self._pending) if self._pending else 0
        )
        return removed

    async def _release_claim(
        self, claim: ScheduleClaim
    ) -> tuple[bool, list[Exception]]:
        claim_released = False
        cleanup_failures: list[Exception] = []
        try:
            await self._schedules.release(claim)
        except CacheConflictError:
            claim_released = True
        except Exception as error:
            cleanup_failures.append(error)
        else:
            claim_released = True
        return claim_released, cleanup_failures

    async def _release_staged(self, staged: list[str]) -> list[Exception]:
        cleanup_failures: list[Exception] = []
        for dispatch_id in staged:
            pending = self._pending.get(dispatch_id)
            if pending is None:
                continue
            try:
                await self._schedules.release(pending.claim)
            except CacheConflictError:
                self._pending.pop(dispatch_id, None)
                continue
            except Exception as cleanup_error:
                cleanup_failures.append(cleanup_error)
            else:
                self._pending.pop(dispatch_id, None)
        return cleanup_failures

    def _track_refreshing(
        self,
        claim: ScheduleClaim,
        refreshing: dict[str, ScheduleClaim],
    ) -> None:
        self._refreshing[claim.token] = claim
        refreshing[claim.token] = claim

    def _untrack_refreshing(
        self,
        claim: ScheduleClaim,
        refreshing: dict[str, ScheduleClaim],
    ) -> None:
        self._refreshing.pop(claim.token, None)
        refreshing.pop(claim.token, None)

    async def _release_refreshing(
        self,
        refreshing: dict[str, ScheduleClaim],
    ) -> list[Exception]:
        cleanup_failures: list[Exception] = []
        for token, claim in tuple(refreshing.items()):
            released, failures = await self._release_claim(claim)
            if released:
                self._refreshing.pop(token, None)
                refreshing.pop(token, None)
            cleanup_failures.extend(failures)
        return cleanup_failures

    def _remember_deferred(self, claim: ScheduleClaim) -> None:
        self._deferred_revisions[claim.record.identity] = claim.record.revision
        self._deferred_revisions.move_to_end(claim.record.identity)
        if len(self._deferred_revisions) > MAX_CACHE_FEATURE_LIMIT:
            self._deferred_revisions.popitem(last=False)

    async def _prepare_cron_claim(
        self, claim: ScheduleClaim, envelope: _Envelope, now: float
    ) -> tuple[ScheduleClaim, _Envelope] | None:
        if envelope.pending_due_at:
            return claim, envelope
        matches = _matching_due_times(
            envelope.task.cron or "",
            claim.record.next_due_at,
            now,
            envelope.timezone,
            envelope.catch_up_limit,
        )
        if not matches:
            await self._schedules.advance(
                claim,
                _encode(envelope),
                next_due_at=_next_cron_due_at(
                    envelope.task.cron or "",
                    now,
                    envelope.timezone,
                ),
            )
            return None
        if claim.record.next_due_at < matches[0]:
            await self._schedules.advance(
                claim,
                _encode(replace(envelope, pending_due_at=matches)),
                next_due_at=matches[0],
            )
            return None
        return claim, replace(envelope, pending_due_at=matches)


def _encode(envelope: _Envelope) -> bytes:
    return json.dumps(
        {
            "version": _ENVELOPE_VERSION,
            "task": envelope.task.model_dump(mode="json"),
            "timezone": envelope.timezone,
            "catch_up_limit": envelope.catch_up_limit,
            "pending_due_at": envelope.pending_due_at,
        },
        separators=(",", ":"),
    ).encode()


def _decode(
    payload: bytes,
    *,
    identity: str,
    next_due_at: float,
    interval_seconds: float | None,
) -> _Envelope:
    try:
        value = json.loads(payload)
        if not isinstance(value, dict):
            raise ValueError
        version = value["version"]
        if isinstance(version, bool) or not isinstance(version, int) or version < 1:
            raise ValueError
    except KeyError, TypeError, ValueError:
        raise _InvalidScheduleEnvelopeError(
            "Taskiq schedule envelope is invalid."
        ) from None
    if version != _ENVELOPE_VERSION:
        raise _UnsupportedScheduleEnvelopeError(
            "Taskiq schedule envelope version is unsupported."
        )
    try:
        task = ScheduledTask.model_validate(value["task"])
        _validate_schedule(task)
        if task.schedule_id != identity:
            raise ValueError
        if interval_seconds != _interval_seconds(task):
            raise ValueError
        timezone = value["timezone"]
        _timezone(timezone)
        if task.time is not None and _initial_due_at(task, timezone) != next_due_at:
            raise ValueError
        if task.cron is not None and not _matches_cron_due_at(
            task.cron,
            next_due_at,
            timezone,
        ):
            raise ValueError
        catch_up_limit = value["catch_up_limit"]
        _validate_limit(catch_up_limit, label="Catch-up limit")
        pending_due_at = _pending_due_times(
            value.get("pending_due_at", ()),
            task=task,
            catch_up_limit=catch_up_limit,
            next_due_at=next_due_at,
            timezone=timezone,
        )
    except KeyError, TypeError, ValueError, OverflowError, OSError:
        pass
    else:
        return _Envelope(task, timezone, catch_up_limit, pending_due_at)
    raise _InvalidScheduleEnvelopeError("Taskiq schedule envelope is invalid.")


def _pending_due_times(
    value: object,
    *,
    task: ScheduledTask,
    catch_up_limit: int,
    next_due_at: float,
    timezone: str,
) -> tuple[float, ...]:
    if not isinstance(value, list):
        raise ValueError
    pending_due_at: list[float] = []
    for due_at in value:
        if (
            isinstance(due_at, bool)
            or not isinstance(due_at, int | float)
            or not math.isfinite(due_at)
        ):
            raise ValueError
        pending_due_at.append(float(due_at))
    if not pending_due_at:
        return ()
    if (
        task.cron is None
        or len(pending_due_at) > catch_up_limit
        or pending_due_at[0] != next_due_at
        or any(
            earlier >= later
            for earlier, later in zip(
                pending_due_at,
                pending_due_at[1:],
                strict=False,
            )
        )
    ):
        raise ValueError
    cron = task.cron
    assert cron is not None
    previous = None
    for due_at in pending_due_at:
        if not _matches_cron_due_at(cron, due_at, timezone):
            raise ValueError
        if previous is not None:
            expected = croniter(cron, previous).get_next(datetime).astimezone(UTC)
            if due_at != expected.timestamp():
                raise ValueError
        previous = datetime.fromtimestamp(due_at, UTC).astimezone(_timezone(timezone))
    return tuple(pending_due_at)


def _timezone(name: object):
    timezone = None
    if not isinstance(name, str) or not name.strip():
        raise ValueError("Schedule timezone must be a valid IANA timezone.")
    try:
        timezone = pytz.timezone(name)
    except pytz.UnknownTimeZoneError:
        pass
    if timezone is None:
        raise ValueError("Schedule timezone must be a valid IANA timezone.")
    return timezone


def _matches_cron_due_at(expression: str, due_at: float, timezone: str) -> bool:
    current = datetime.fromtimestamp(due_at, UTC).astimezone(_timezone(timezone))
    return (
        current.second == 0
        and current.microsecond == 0
        and croniter.match(expression, current)
    )


def _validate_schedule(schedule: ScheduledTask) -> None:
    schedule_kinds = sum(
        value is not None for value in (schedule.time, schedule.interval, schedule.cron)
    )
    if schedule_kinds != 1:
        raise ValueError(
            "Taskiq schedule must specify exactly one of time, interval, or cron."
        )
    if schedule.cron_offset is not None:
        raise ValueError(
            "Taskiq cron_offset is not supported; configure an IANA timezone instead."
        )
    if schedule.cron is not None:
        cron_fields = schedule.cron.split()
        if len(cron_fields) != 5 or not croniter.is_valid(schedule.cron):
            raise ValueError(
                "Taskiq cron schedule must be a valid five-field expression."
            )


def _validate_policy_resource(value: object, *, label: str) -> None:
    if not isinstance(value, str) or not value.strip() or value != value.strip():
        raise ValueError(
            f"{label} must be a non-blank string without surrounding whitespace."
        )


def _validate_limit(value: int, *, label: str) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= MAX_CACHE_FEATURE_LIMIT
    ):
        raise ValueError(
            f"{label} must be a positive integer no greater than "
            f"{MAX_CACHE_FEATURE_LIMIT}."
        )


def _validate_schedule_capacity(value: int) -> None:
    if (
        isinstance(value, bool)
        or not isinstance(value, int)
        or not 1 <= value <= MAX_CACHE_FEATURE_LIMIT
    ):
        raise ValueError(
            "Schedule capability maximum_records must be a positive integer no "
            f"greater than {MAX_CACHE_FEATURE_LIMIT}."
        )


def _validate_source_refresh_interval(value: int) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise ValueError(
            "Taskiq source refresh interval must be a positive whole number of seconds."
        )


def _validate_schedule_refresh_interval(
    schedule: ScheduledTask,
    policy: TaskiqSchedulePolicy,
) -> None:
    if schedule.cron is not None:
        return
    interval_seconds = _interval_seconds(schedule)
    if (
        interval_seconds is not None
        and interval_seconds < policy.source_refresh_interval_seconds
    ):
        raise _RefreshIntervalMismatchError(
            "Taskiq schedule interval must not be shorter than the configured "
            "source refresh interval."
        )


def _dispatch_task(task: ScheduledTask, claim: ScheduleClaim) -> ScheduledTask:
    task_id = task.task_id if task.time is not None else None
    if task_id is None:
        task_id = str(
            uuid5(
                _DISPATCH_NAMESPACE,
                "\0".join(
                    (
                        task.schedule_id,
                        str(claim.record.revision.value),
                        repr(claim.record.next_due_at),
                    )
                ),
            )
        )
    return task.model_copy(
        update={
            "task_id": task_id,
            "schedule_id": f"{task.schedule_id}:dispatch:{uuid4().hex}",
            "cron": None,
            "interval": None,
            "time": _DISPATCH_TIME,
        }
    )


def _initial_due_at(
    schedule: ScheduledTask,
    timezone: str,
    *,
    now: float | None = None,
) -> float:
    if schedule.time is not None:
        value = schedule.time
        if value.tzinfo is None:
            localised = None
            try:
                localised = _timezone(timezone).localize(value, is_dst=None)
            except pytz.AmbiguousTimeError, pytz.NonExistentTimeError:
                pass
            if localised is None:
                raise ValueError(
                    "Naive schedule time is ambiguous or non-existent in its timezone; "
                    "use an aware datetime."
                )
            value = localised
        return value.astimezone(UTC).timestamp()
    if schedule.interval is not None:
        if now is None:
            raise ValueError("Interval schedules require a cache time value.")
        return now
    if now is None:
        raise ValueError("Cron schedules require a cache time value.")
    return _next_cron_due_at(schedule.cron or "", now, timezone)


def _interval_seconds(schedule: ScheduledTask) -> float | None:
    if schedule.cron is not None:
        return _MINUTE_SECONDS
    if schedule.interval is None:
        return None
    return (
        schedule.interval.total_seconds()
        if isinstance(schedule.interval, timedelta)
        else float(schedule.interval)
    )


def _next_tick(now: float) -> float:
    return (math.floor(now / _MINUTE_SECONDS) + 1) * _MINUTE_SECONDS


def _next_cron_due_at(expression: str, after: float, timezone: str) -> float:
    current = datetime.fromtimestamp(after, UTC).astimezone(_timezone(timezone))
    return croniter(expression, current).get_next(datetime).astimezone(UTC).timestamp()


def _matching_due_times(
    expression: str, start: float, now: float, timezone: str, limit: int
) -> tuple[float, ...]:
    zone = _timezone(timezone)
    cursor = datetime.fromtimestamp(_next_tick(now), UTC).astimezone(zone)
    iterator = croniter(expression, cursor)
    matches: list[float] = []
    for _ in range(limit):
        due = iterator.get_prev(datetime).astimezone(UTC).timestamp()
        if due < start:
            break
        matches.append(due)
    return tuple(reversed(matches))


def _diagnostic_identity(identity: str) -> str:
    return truncate_safe_string(identity, maximum_length=100)


def _diagnostic_identities(identities: list[str]) -> str:
    sample = ", ".join(_diagnostic_identity(identity) for identity in identities[:5])
    return sample or "none"


__all__ = ("CacheTaskiqScheduleSource", "TaskiqSchedulePolicy")
