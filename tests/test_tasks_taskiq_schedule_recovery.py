from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import AsyncGenerator
from datetime import UTC, datetime

import pytest
from taskiq import ScheduledTask
from taskiq.abc.broker import AsyncBroker
from taskiq.exceptions import ScheduledTaskCancelledError, SendTaskError
from taskiq.message import BrokerMessage
from taskiq.scheduler.scheduler import TaskiqScheduler

from wybra.cache import CacheTimeCapability, InMemoryCacheTime, InMemoryScheduleCache
from wybra.tasks.taskiq_schedule import (
    CacheTaskiqScheduleSource as _CacheTaskiqScheduleSource,
)
from wybra.tasks.taskiq_schedule import TaskiqSchedulePolicy


class Clock:
    def __init__(self, value: float) -> None:
        self.value = value

    def __call__(self) -> float:
        return self.value


def CacheTaskiqScheduleSource(
    schedules: object,
    *,
    policy: TaskiqSchedulePolicy,
    cache_time: CacheTimeCapability | None = None,
) -> _CacheTaskiqScheduleSource:
    return _CacheTaskiqScheduleSource(
        schedules,
        policy=policy,
        cache_time=cache_time or InMemoryCacheTime(schedules.clock),
    )


class FailingBroker(AsyncBroker):
    async def kick(self, message: BrokerMessage) -> None:
        del message
        raise RuntimeError("broker unavailable")

    async def listen(self) -> AsyncGenerator[bytes]:
        if False:
            yield b""


class RecordingBroker(AsyncBroker):
    def __init__(self) -> None:
        super().__init__()
        self.messages: list[BrokerMessage] = []

    async def kick(self, message: BrokerMessage) -> None:
        self.messages.append(message)

    async def listen(self) -> AsyncGenerator[bytes]:
        if False:
            yield b""


class BlockingBroker(RecordingBroker):
    def __init__(self) -> None:
        super().__init__()
        self.started = asyncio.Event()
        self.unblock = asyncio.Event()

    async def kick(self, message: BrokerMessage) -> None:
        self.started.set()
        await self.unblock.wait()
        await super().kick(message)


class DeleteAfterHeldSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def held(self, claim):
        result = await self._schedules.held(claim)
        await self._schedules.delete(claim.owner, claim.record.identity)
        return result


class FailingAdvanceSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def advance(self, *_args: object, **_kwargs: object) -> None:
        raise RuntimeError("advance unavailable")


class DeleteDuringAdvanceSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def advance(self, claim, *args: object, **kwargs: object):
        await self._schedules.delete(claim.owner, claim.record.identity)
        return await self._schedules.advance(claim, *args, **kwargs)


class FailingCompleteSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def complete(self, *_args: object, **_kwargs: object) -> None:
        raise RuntimeError("complete unavailable")


class FailingDiscardSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def discard(self, *_args: object, **_kwargs: object) -> None:
        raise RuntimeError("discard unavailable")


class CountingHeldSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules
        self.held_calls = 0

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def held(self, claim):
        self.held_calls += 1
        return await self._schedules.held(claim)


class CountingAdvanceSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules
        self.advance_calls = 0

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def advance(self, *args: object, **kwargs: object):
        self.advance_calls += 1
        return await self._schedules.advance(*args, **kwargs)


class BlockingAdvanceSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules
        self.started = asyncio.Event()
        self.unblock = asyncio.Event()

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def advance(self, *args: object, **kwargs: object):
        self.started.set()
        await self.unblock.wait()
        return await self._schedules.advance(*args, **kwargs)


class BlockingHeldSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules
        self.started = asyncio.Event()
        self.unblock = asyncio.Event()

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def held(self, claim):
        result = await self._schedules.held(claim)
        self.started.set()
        await self.unblock.wait()
        return result


class BlockingClaimSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules
        self.started = asyncio.Event()
        self.unblock = asyncio.Event()

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def claim(self, *args: object, **kwargs: object):
        claim = await self._schedules.claim(*args, **kwargs)
        self.started.set()
        await self.unblock.wait()
        return claim


class RecordingDueSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules
        self.limits: list[int] = []
        self.pages: list[tuple[str, ...]] = []

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def due(self, *args: object, **kwargs: object):
        self.limits.append(kwargs["limit"])
        records = await self._schedules.due(*args, **kwargs)
        self.pages.append(tuple(record.identity for record in records))
        return records


class FailingRefreshSchedules:
    def __init__(self, schedules: InMemoryScheduleCache, *, operation: str) -> None:
        self._schedules = schedules
        self._operation = operation
        self._due_calls = 0

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def due(self, *args: object, **kwargs: object):
        self._due_calls += 1
        if self._operation == "due" and self._due_calls == 2:
            raise RuntimeError("due unavailable")
        return await self._schedules.due(*args, **kwargs)

    async def claim(self, *args: object, **kwargs: object):
        if self._operation == "claim" and args[1] == "zzz-failure":
            raise RuntimeError("claim unavailable")
        return await self._schedules.claim(*args, **kwargs)


class BlockingSecondDueSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules
        self._due_calls = 0
        self.started = asyncio.Event()
        self.unblock = asyncio.Event()

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def due(self, *args: object, **kwargs: object):
        self._due_calls += 1
        if self._due_calls == 2:
            self.started.set()
            await self.unblock.wait()
        return await self._schedules.due(*args, **kwargs)


class BlockingDueSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules
        self.started = asyncio.Event()
        self.unblock = asyncio.Event()

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def due(self, *args: object, **kwargs: object):
        self.started.set()
        await self.unblock.wait()
        return await self._schedules.due(*args, **kwargs)


class FailingSelectedReleaseSchedules:
    def __init__(self, schedules: InMemoryScheduleCache, *, identity: str) -> None:
        self._schedules = schedules
        self._identity = identity

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def release(self, claim):
        if claim.record.identity == self._identity:
            raise RuntimeError("release unavailable")
        await self._schedules.release(claim)


class FailFirstSelectedReleaseSchedules:
    def __init__(self, schedules: InMemoryScheduleCache, *, identity: str) -> None:
        self._schedules = schedules
        self._identity = identity
        self._failed = False

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def release(self, claim):
        if claim.record.identity == self._identity and not self._failed:
            self._failed = True
            raise RuntimeError("release unavailable")
        await self._schedules.release(claim)


class FailingDeleteSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def delete(self, *_args: object, **_kwargs: object) -> bool:
        raise RuntimeError("delete unavailable")


class CancelFirstReleaseSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules
        self.release_calls = 0

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def release(self, *args: object, **kwargs: object) -> None:
        self.release_calls += 1
        if self.release_calls == 1:
            raise asyncio.CancelledError
        await self._schedules.release(*args, **kwargs)


class CancelAndFailReleaseSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules
        self.release_calls = 0

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def release(self, *args: object, **kwargs: object) -> None:
        self.release_calls += 1
        if self.release_calls == 1:
            raise asyncio.CancelledError
        raise RuntimeError("release unavailable")


def _one_time_schedule(schedule_id: str = "once") -> ScheduledTask:
    return ScheduledTask(
        task_name="tests.once",
        labels={},
        args=[],
        kwargs={},
        time=datetime(1970, 1, 1, 0, 16, 40, tzinfo=UTC),
        schedule_id=schedule_id,
    )


@pytest.mark.anyio
@pytest.mark.parametrize("schedule_kind", ("one-time", "cron"))
async def test_invalid_envelope_does_not_starve_later_due_schedule(
    caplog: pytest.LogCaptureFixture,
    schedule_kind: str,
) -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            due_limit=1,
        ),
    )
    good = (
        _one_time_schedule("zzz-good")
        if schedule_kind == "one-time"
        else ScheduledTask(
            task_name="tests.cron",
            labels={},
            args=[],
            kwargs={},
            cron="* * * * *",
            schedule_id="zzz-good",
        )
    )
    await source.add_schedule(good)
    due_at = clock() if schedule_kind == "one-time" else clock() + 20
    invalid_payload = (
        b'{"version":1,"task":{"task_name":"TOP_SECRET","labels":{},"args":[],"kwargs":{},'
        b'"time":"1970-01-01T00:16:40Z","schedule_id":"aaa-invalid"},'
        b'"timezone":"UTC","catch_up_limit":0}'
    )
    await schedules.create(
        "taskiq-scheduler",
        "aaa-invalid",
        invalid_payload,
        next_due_at=due_at,
        interval_seconds=60,
    )
    clock.value = due_at

    with caplog.at_level(logging.WARNING, logger="wybra.tasks.taskiq_schedule"):
        ready = await source.get_schedules()

    assert [task.task_name for task in ready] == [good.task_name]
    assert "TOP_SECRET" not in caplog.text
    assert "Discarded invalid Taskiq schedule envelope for aaa-invalid." in caplog.text
    assert (
        await schedules.create(
            "taskiq-scheduler",
            "aaa-invalid",
            b"replacement",
            next_due_at=due_at,
        )
        is not None
    )


def test_invalid_timezone_diagnostic_omits_raw_value() -> None:
    raw_timezone = "TOP_SECRET_" + "x" * 10_000

    with pytest.raises(ValueError, match="valid IANA timezone") as error:
        TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            timezone=raw_timezone,
        )

    assert raw_timezone not in str(error.value)
    assert error.value.__cause__ is None
    assert error.value.__context__ is None


@pytest.mark.anyio
async def test_schedule_envelope_uses_the_current_schema_version() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )

    await source.add_schedule(_one_time_schedule())

    record = (await schedules.due("taskiq-scheduler", before=clock()))[0]
    assert json.loads(record.payload)["version"] == 1


@pytest.mark.anyio
async def test_unsupported_envelope_version_does_not_starve_or_delete(
    caplog: pytest.LogCaptureFixture,
) -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            due_limit=1,
        ),
    )
    unsupported = _one_time_schedule("aaa-unsupported")
    payload = json.dumps(
        {
            "version": 2,
            "task": unsupported.model_dump(mode="json"),
            "timezone": "UTC",
            "catch_up_limit": 1,
            "pending_due_at": [],
            "private": "TOP_SECRET",
        }
    ).encode()
    record = await schedules.create(
        "taskiq-scheduler",
        unsupported.schedule_id,
        payload,
        next_due_at=clock(),
    )
    assert record is not None
    await source.add_schedule(_one_time_schedule("zzz-good"))

    with caplog.at_level(logging.WARNING, logger="wybra.tasks.taskiq_schedule"):
        ready = await source.get_schedules()

    assert [task.task_name for task in ready] == ["tests.once"]
    retained = {
        item.identity: item
        for item in await schedules.due(
            "taskiq-scheduler",
            before=clock(),
            limit=2,
        )
    }
    assert retained[unsupported.schedule_id].revision == record.revision
    assert retained[unsupported.schedule_id].next_due_at == record.next_due_at
    assert "unsupported envelope version" in caplog.text
    assert unsupported.schedule_id in caplog.text
    assert "TOP_SECRET" not in caplog.text


@pytest.mark.anyio
@pytest.mark.parametrize("version", (0, -1))
async def test_non_positive_envelope_version_is_discarded(
    version: int,
) -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    task = _one_time_schedule("invalid-version")
    await schedules.create(
        "taskiq-scheduler",
        task.schedule_id,
        json.dumps(
            {
                "version": version,
                "task": task.model_dump(mode="json"),
                "timezone": "UTC",
                "catch_up_limit": 1,
                "pending_due_at": [],
            }
        ).encode(),
        next_due_at=clock(),
    )

    assert await source.get_schedules() == []
    assert await schedules.create(
        "taskiq-scheduler",
        task.schedule_id,
        b"replacement",
        next_due_at=clock(),
    )


@pytest.mark.anyio
async def test_interval_requires_a_compatible_source_refresh_interval() -> None:
    task = ScheduledTask(
        task_name="tests.interval",
        labels={},
        args=[],
        kwargs={},
        interval=5,
        schedule_id="short-interval",
    )
    default_source = CacheTaskiqScheduleSource(
        InMemoryScheduleCache(),
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )

    with pytest.raises(ValueError, match="source refresh interval"):
        await default_source.add_schedule(task)

    fast_source = CacheTaskiqScheduleSource(
        InMemoryScheduleCache(),
        policy=TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=1,
        ),
    )
    await fast_source.add_schedule(task)


@pytest.mark.anyio
@pytest.mark.parametrize("due_limit", (1, 2))
async def test_incompatible_persisted_intervals_do_not_delay_later_due_work(
    due_limit: int,
) -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    fast_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="fast-scheduler",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=1,
        ),
    )
    for index in range(due_limit):
        await fast_source.add_schedule(
            ScheduledTask(
                task_name="tests.fast",
                labels={},
                args=[],
                kwargs={},
                interval=5,
                schedule_id=f"aaa-fast-{index}",
            )
        )
    await fast_source.add_schedule(_one_time_schedule("zzz-good"))
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            due_limit=due_limit,
        ),
    )
    original = {
        record.identity: record
        for record in await schedules.due(
            "taskiq-scheduler",
            before=clock(),
            limit=due_limit + 1,
        )
    }

    ready = await source.get_schedules()
    retained = {
        record.identity: record
        for record in await schedules.due(
            "taskiq-scheduler",
            before=clock(),
            limit=due_limit + 1,
        )
    }
    for index in range(due_limit):
        identity = f"aaa-fast-{index}"
        assert retained[identity].revision == original[identity].revision
        assert retained[identity].next_due_at == original[identity].next_due_at
    assert [task.task_name for task in ready] == ["tests.once"]


@pytest.mark.anyio
async def test_incompatible_source_does_not_defer_work_from_compatible_source() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    fast_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="fast-scheduler",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=1,
        ),
    )
    slow_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="slow-scheduler",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=60,
            due_limit=1,
        ),
    )
    task = ScheduledTask(
        task_name="tests.fast",
        labels={},
        args=[],
        kwargs={},
        interval=5,
        schedule_id="fast",
    )
    await fast_source.add_schedule(task)
    original = (await schedules.due("taskiq-scheduler", before=clock()))[0]

    assert await slow_source.get_schedules() == []
    ready = await fast_source.get_schedules()

    assert [item.task_name for item in ready] == [task.task_name]
    claim = fast_source._pending[ready[0].schedule_id].claim
    assert claim.record.revision == original.revision
    assert claim.record.next_due_at == original.next_due_at


@pytest.mark.anyio
async def test_deferred_revision_is_reconsidered_after_remote_replacement() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    peer = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="peer", claim_ttl_seconds=30),
    )
    unsupported = _one_time_schedule("replaced")
    await schedules.create(
        "taskiq-scheduler",
        unsupported.schedule_id,
        json.dumps({"version": 2}).encode(),
        next_due_at=clock(),
    )

    assert await source.get_schedules() == []
    assert await schedules.delete("taskiq-scheduler", unsupported.schedule_id)
    await peer.add_schedule(unsupported)

    assert [task.task_name for task in await source.get_schedules()] == [
        unsupported.task_name
    ]


@pytest.mark.anyio
async def test_recovered_schedule_emission_keeps_a_stable_task_id() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    task = ScheduledTask(
        task_name="tests.interval",
        labels={},
        args=[],
        kwargs={},
        interval=60,
        schedule_id="recurring",
    )
    await source.add_schedule(task)

    first = (await source.get_schedules())[0]
    clock.value += 31
    recovered = (await source.get_schedules())[0]

    assert first.task_id is not None
    assert recovered.task_id == first.task_id
    await source.post_send(recovered)
    clock.value = 1_060
    next_occurrence = (await source.get_schedules())[0]
    assert next_occurrence.task_id != first.task_id


@pytest.mark.anyio
async def test_recurring_schedule_uses_distinct_ids_for_distinct_occurrences() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    task = ScheduledTask(
        task_name="tests.interval",
        labels={},
        args=[],
        kwargs={},
        task_id="template-id",
        interval=60,
        schedule_id="recurring",
    )
    await source.add_schedule(task)

    first = (await source.get_schedules())[0]
    await source.post_send(first)
    clock.value = 1_060
    second = (await source.get_schedules())[0]

    assert first.task_id != task.task_id
    assert second.task_id != first.task_id


@pytest.mark.anyio
async def test_record_recurrence_mismatch_is_discarded() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    task = _one_time_schedule("mismatch")
    payload = json.dumps(
        {
            "version": 1,
            "task": task.model_dump(mode="json"),
            "timezone": "UTC",
            "catch_up_limit": 1,
            "pending_due_at": [],
        }
    ).encode()
    await schedules.create(
        "taskiq-scheduler",
        task.schedule_id,
        payload,
        next_due_at=clock(),
        interval_seconds=60,
    )

    assert await source.get_schedules() == []
    assert await schedules.create(
        "taskiq-scheduler",
        task.schedule_id,
        b"replacement",
        next_due_at=clock(),
    )


@pytest.mark.anyio
async def test_cron_record_with_a_non_matching_due_time_is_discarded() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    task = ScheduledTask(
        task_name="tests.cron",
        labels={},
        args=[],
        kwargs={},
        cron="0 * * * *",
        schedule_id="invalid-cron-due",
    )
    payload = json.dumps(
        {
            "version": 1,
            "task": task.model_dump(mode="json"),
            "timezone": "UTC",
            "catch_up_limit": 1,
            "pending_due_at": [],
        }
    ).encode()
    await schedules.create(
        "taskiq-scheduler",
        task.schedule_id,
        payload,
        next_due_at=clock(),
        interval_seconds=60,
    )

    assert await source.get_schedules() == []
    assert await schedules.create(
        "taskiq-scheduler",
        task.schedule_id,
        b"replacement",
        next_due_at=clock(),
    )


@pytest.mark.anyio
@pytest.mark.parametrize("kind", ("identity", "one-time", "cron"))
async def test_semantically_inconsistent_schedule_envelope_is_discarded(
    kind: str,
) -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    identity = "stored"
    task = _one_time_schedule(identity)
    interval_seconds = None
    pending_due_at: list[float] = []
    if kind == "identity":
        task = _one_time_schedule("embedded")
    elif kind == "one-time":
        task.time = datetime(1970, 1, 1, 0, 16, 41, tzinfo=UTC)
    else:
        identity = "cron"
        task = ScheduledTask(
            task_name="tests.cron",
            labels={},
            args=[],
            kwargs={},
            cron="0 * * * *",
            schedule_id=identity,
        )
        interval_seconds = 60
        pending_due_at = [clock()]
    payload = json.dumps(
        {
            "version": 1,
            "task": task.model_dump(mode="json"),
            "timezone": "UTC",
            "catch_up_limit": 1,
            "pending_due_at": pending_due_at,
        }
    ).encode()
    await schedules.create(
        "taskiq-scheduler",
        identity,
        payload,
        next_due_at=clock(),
        interval_seconds=interval_seconds,
    )

    assert await source.get_schedules() == []
    assert await schedules.create(
        "taskiq-scheduler",
        identity,
        b"replacement",
        next_due_at=clock(),
    )


@pytest.mark.anyio
async def test_failed_refresh_releases_earlier_staged_claims() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        FailingAdvanceSchedules(schedules),
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    await source.add_schedule(
        ScheduledTask(
            task_name="tests.good",
            labels={},
            args=[],
            kwargs={},
            time=datetime(1970, 1, 1, 0, 17, tzinfo=UTC),
            schedule_id="aaa-good",
        )
    )
    await source.add_schedule(
        ScheduledTask(
            task_name="tests.cron",
            labels={},
            args=[],
            kwargs={},
            cron="* * * * *",
            schedule_id="zzz-cron",
        )
    )
    clock.value = 1_200

    with pytest.raises(RuntimeError, match="advance unavailable"):
        await source.get_schedules()

    assert source._pending == {}
    assert await schedules.claim("taskiq-scheduler", "aaa-good", "other", ttl=30)


@pytest.mark.anyio
async def test_failed_invalid_envelope_discard_releases_earlier_staged_claims() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    await source.add_schedule(_one_time_schedule("aaa-good"))
    await schedules.create(
        "taskiq-scheduler",
        "zzz-invalid",
        b"not-json",
        next_due_at=clock(),
    )
    source._schedules = FailingDiscardSchedules(schedules)

    with pytest.raises(RuntimeError, match="discard unavailable"):
        await source.get_schedules()

    assert source._pending == {}
    assert source._refreshing == {}
    assert await schedules.claim("taskiq-scheduler", "aaa-good", "other", ttl=30)


@pytest.mark.anyio
async def test_deferring_later_schedule_preserves_earlier_ready_dispatch() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    fast_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="fast-scheduler",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=1,
        ),
    )
    slow_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="slow-scheduler",
            claim_ttl_seconds=30,
            due_limit=2,
        ),
    )
    await slow_source.add_schedule(_one_time_schedule("aaa-good"))
    await fast_source.add_schedule(
        ScheduledTask(
            task_name="tests.fast",
            labels={},
            args=[],
            kwargs={},
            interval=5,
            schedule_id="zzz-incompatible",
        )
    )

    ready = await slow_source.get_schedules()

    assert [task.task_name for task in ready] == ["tests.once"]
    await slow_source.pre_send(ready[0])


@pytest.mark.anyio
@pytest.mark.parametrize("operation", ("due", "claim"))
async def test_aborted_refresh_releases_staged_schedules_for_a_peer(
    operation: str,
) -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        FailingRefreshSchedules(schedules, operation=operation),
        policy=TaskiqSchedulePolicy(
            claimant="scheduler-a",
            claim_ttl_seconds=30,
            due_limit=2,
            scan_page_limit=1,
        ),
    )
    peer = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-b", claim_ttl_seconds=30),
    )
    await source.add_schedule(_one_time_schedule("aaa-good"))
    if operation == "claim":
        await source.add_schedule(_one_time_schedule("zzz-failure"))

    with pytest.raises(RuntimeError, match=f"{operation} unavailable"):
        await source.get_schedules()

    ready = await peer.get_schedules()

    assert [task.task_name for task in ready] == ["tests.once"] * (
        2 if operation == "claim" else 1
    )


@pytest.mark.anyio
async def test_cancelled_refresh_releases_staged_schedules_for_a_peer() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    blocking = BlockingSecondDueSchedules(schedules)
    source = CacheTaskiqScheduleSource(
        blocking,
        policy=TaskiqSchedulePolicy(
            claimant="scheduler-a",
            claim_ttl_seconds=30,
            due_limit=2,
            scan_page_limit=1,
        ),
    )
    peer = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-b", claim_ttl_seconds=30),
    )
    await source.add_schedule(_one_time_schedule("once"))

    refreshing = asyncio.create_task(source.get_schedules())
    await blocking.started.wait()
    refreshing.cancel()

    with pytest.raises(asyncio.CancelledError):
        await refreshing

    assert len(await peer.get_schedules()) == 1


@pytest.mark.anyio
async def test_failed_deferral_release_still_releases_earlier_staged_schedule() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    fast_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="fast-scheduler",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=1,
        ),
    )
    source = CacheTaskiqScheduleSource(
        FailingSelectedReleaseSchedules(schedules, identity="zzz-incompatible"),
        policy=TaskiqSchedulePolicy(
            claimant="scheduler-a",
            claim_ttl_seconds=30,
            due_limit=2,
        ),
    )
    peer = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-b", claim_ttl_seconds=30),
    )
    await source.add_schedule(_one_time_schedule("aaa-good"))
    await fast_source.add_schedule(
        ScheduledTask(
            task_name="tests.fast",
            labels={},
            args=[],
            kwargs={},
            interval=5,
            schedule_id="zzz-incompatible",
        )
    )

    with pytest.raises(ExceptionGroup):
        await source.get_schedules()

    ready = await peer.get_schedules()

    assert [task.schedule_id.split(":", 1)[0] for task in ready] == ["aaa-good"]


@pytest.mark.anyio
async def test_aborted_deferral_retries_the_current_claim_release() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    fast_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="fast-scheduler",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=1,
        ),
    )
    source = CacheTaskiqScheduleSource(
        FailFirstSelectedReleaseSchedules(schedules, identity="incompatible"),
        policy=TaskiqSchedulePolicy(claimant="scheduler-a", claim_ttl_seconds=30),
    )
    peer = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="scheduler-b",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=1,
        ),
    )
    await fast_source.add_schedule(
        ScheduledTask(
            task_name="tests.fast",
            labels={},
            args=[],
            kwargs={},
            interval=5,
            schedule_id="incompatible",
        )
    )

    with pytest.raises(ExceptionGroup):
        await source.get_schedules()

    assert len(await peer.get_schedules()) == 1


@pytest.mark.anyio
async def test_scan_budget_rotates_past_deferred_schedules() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    fast_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="fast-scheduler",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=1,
        ),
    )
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            due_limit=1,
            scan_page_limit=1,
            scan_limit=2,
        ),
    )
    for index in range(3):
        await fast_source.add_schedule(
            ScheduledTask(
                task_name="tests.fast",
                labels={},
                args=[],
                kwargs={},
                interval=5,
                schedule_id=f"aaa-incompatible-{index}",
            )
        )
    await source.add_schedule(_one_time_schedule("zzz-good"))

    assert await source.get_schedules() == []

    ready = await source.get_schedules()

    assert [task.task_name for task in ready] == ["tests.once"]


@pytest.mark.anyio
async def test_deferred_backlog_does_not_repeatedly_delay_recurring_work() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    fast_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="fast-scheduler",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=1,
        ),
    )
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            due_limit=1,
            scan_page_limit=1,
            scan_limit=2,
        ),
    )
    for index in range(4):
        await fast_source.add_schedule(
            ScheduledTask(
                task_name="tests.fast",
                labels={},
                args=[],
                kwargs={},
                interval=5,
                schedule_id=f"aaa-incompatible-{index}",
            )
        )
    recurring = ScheduledTask(
        task_name="tests.recurring",
        labels={},
        args=[],
        kwargs={},
        interval=60,
        schedule_id="zzz-recurring",
    )
    await source.add_schedule(recurring)

    assert await source.get_schedules() == []
    assert await source.get_schedules() == []
    first = (await source.get_schedules())[0]
    await source.post_send(first)

    clock.value += 60
    ready = await source.get_schedules()

    assert [task.task_name for task in ready] == [recurring.task_name]


@pytest.mark.anyio
async def test_exhausted_deferred_cursor_wraps_for_a_peer_added_schedule() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            scan_limit=1,
        ),
    )
    incompatible_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="fast-scheduler",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=1,
        ),
    )
    await incompatible_source.add_schedule(
        ScheduledTask(
            task_name="tests.fast",
            labels={},
            args=[],
            kwargs={},
            interval=5,
            schedule_id="incompatible",
        )
    )

    assert await source.get_schedules() == []

    await incompatible_source.add_schedule(_one_time_schedule("aaa-new-work"))

    assert [task.task_name for task in await source.get_schedules()] == ["tests.once"]


@pytest.mark.anyio
async def test_exhausted_deferred_cursor_wraps_for_an_expired_peer_claim() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    compatible_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="compatible-scheduler",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=1,
        ),
    )
    await source.add_schedule(_one_time_schedule("aaa-recovered"))
    crashed_claim = await schedules.claim(
        "taskiq-scheduler",
        "aaa-recovered",
        "crashed-scheduler",
        ttl=30,
    )
    assert crashed_claim is not None
    await compatible_source.add_schedule(
        ScheduledTask(
            task_name="tests.incompatible",
            labels={},
            args=[],
            kwargs={},
            interval=5,
            schedule_id="zzz-incompatible",
        )
    )

    assert await source.get_schedules() == []

    clock.value += 31
    assert [task.task_name for task in await source.get_schedules()] == ["tests.once"]


@pytest.mark.anyio
async def test_cancelled_operation_cleanup_does_not_block_shutdown() -> None:
    schedules = BlockingDueSchedules(InMemoryScheduleCache())
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    refreshing = asyncio.create_task(source.get_schedules())
    await schedules.started.wait()
    await source._state_lock.acquire()
    schedules.unblock.set()
    refreshing.cancel()
    await asyncio.sleep(0)
    assert not refreshing.done()
    refreshing.cancel()

    with pytest.raises(asyncio.CancelledError):
        await refreshing

    source._state_lock.release()
    await asyncio.sleep(0)
    assert source._active_operations == 0
    assert source._idle.is_set()
    await source.shutdown()


@pytest.mark.anyio
async def test_schedule_refresh_pages_past_deferred_records() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    recording = RecordingDueSchedules(schedules)
    fast_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="fast-scheduler",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=1,
        ),
    )
    source = CacheTaskiqScheduleSource(
        recording,
        policy=TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            due_limit=1,
            scan_page_limit=1,
        ),
    )
    for index in range(2):
        await fast_source.add_schedule(
            ScheduledTask(
                task_name="tests.fast",
                labels={},
                args=[],
                kwargs={},
                interval=5,
                schedule_id=f"aaa-incompatible-{index}",
            )
        )
    await source.add_schedule(_one_time_schedule("zzz-good"))

    ready = await source.get_schedules()

    assert [task.task_name for task in ready] == ["tests.once"]
    assert recording.limits == [1, 1, 1]


@pytest.mark.anyio
async def test_refresh_prunes_remote_deletion_after_failed_enqueue() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    await source.add_schedule(_one_time_schedule())
    dispatch = (await source.get_schedules())[0]

    with pytest.raises(SendTaskError, match="Cannot send"):
        await TaskiqScheduler(FailingBroker(), [source]).on_ready(source, dispatch)
    await schedules.delete("taskiq-scheduler", "once")

    assert await source.get_schedules() == []
    assert source._pending == {}


@pytest.mark.anyio
async def test_cancelled_cron_refresh_releases_its_tracked_claim() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    blocking = BlockingAdvanceSchedules(schedules)
    source = CacheTaskiqScheduleSource(
        blocking,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    await source.add_schedule(
        ScheduledTask(
            task_name="tests.cron",
            labels={},
            args=[],
            kwargs={},
            cron="* * * * *",
            schedule_id="cron",
        )
    )
    clock.value = 1_200

    refresh = asyncio.create_task(source.get_schedules())
    await blocking.started.wait()
    refresh.cancel()
    with pytest.raises(asyncio.CancelledError):
        await refresh

    recovered = await schedules.claim("taskiq-scheduler", "cron", "other", ttl=30)
    assert recovered is not None


@pytest.mark.anyio
async def test_shutdown_cancels_pre_send_before_releasing_its_claim() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    blocking = BlockingHeldSchedules(schedules)
    source = CacheTaskiqScheduleSource(
        blocking,
        policy=TaskiqSchedulePolicy(claimant="scheduler-a", claim_ttl_seconds=30),
    )
    peer = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-b", claim_ttl_seconds=30),
    )
    await source.add_schedule(_one_time_schedule())
    dispatch = (await source.get_schedules())[0]

    sending = asyncio.create_task(source.pre_send(dispatch))
    await blocking.started.wait()
    stopping = asyncio.create_task(source.shutdown())
    await asyncio.sleep(0)
    blocking.unblock.set()

    with pytest.raises(ScheduledTaskCancelledError):
        await sending
    await stopping
    assert len(await peer.get_schedules()) == 1


@pytest.mark.anyio
async def test_shutdown_waits_for_refresh_claim_and_returns_no_dispatch() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    blocking = BlockingClaimSchedules(schedules)
    source = CacheTaskiqScheduleSource(
        blocking,
        policy=TaskiqSchedulePolicy(claimant="scheduler-a", claim_ttl_seconds=30),
    )
    peer = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-b", claim_ttl_seconds=30),
    )
    await source.add_schedule(_one_time_schedule())

    refreshing = asyncio.create_task(source.get_schedules())
    await blocking.started.wait()
    stopping = asyncio.create_task(source.shutdown())
    await asyncio.sleep(0)
    blocking.unblock.set()

    assert await refreshing == []
    await stopping
    assert len(await peer.get_schedules()) == 1


@pytest.mark.anyio
async def test_shutdown_retains_cancelled_release_for_a_later_retry() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    for schedule_id in ("first", "second"):
        await source.add_schedule(_one_time_schedule(schedule_id))
    assert len(await source.get_schedules()) == 2
    source._schedules = CancelFirstReleaseSchedules(schedules)

    with pytest.raises(asyncio.CancelledError):
        await source.shutdown()

    assert len(source._pending) == 1
    await source.shutdown()
    assert source._pending == {}


@pytest.mark.anyio
async def test_shutdown_preserves_cancellation_when_another_release_fails() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    for schedule_id in ("first", "second"):
        await source.add_schedule(_one_time_schedule(schedule_id))
    assert len(await source.get_schedules()) == 2
    source._schedules = CancelAndFailReleaseSchedules(schedules)

    with pytest.raises(asyncio.CancelledError) as error:
        await source.shutdown()

    assert "release operation" in "\n".join(error.value.__notes__)
    assert len(source._pending) == 2


@pytest.mark.anyio
async def test_failed_delete_keeps_a_valid_staged_dispatch() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    await source.add_schedule(_one_time_schedule())
    dispatch = (await source.get_schedules())[0]
    source._schedules = FailingDeleteSchedules(schedules)

    with pytest.raises(RuntimeError, match="delete unavailable"):
        await source.delete_schedule("once")

    await source.pre_send(dispatch)


@pytest.mark.anyio
async def test_refresh_bounds_remote_pending_claim_checks() -> None:
    clock = Clock(1_000)
    schedules = CountingHeldSchedules(InMemoryScheduleCache(clock))
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            due_limit=101,
        ),
    )
    for index in range(101):
        await source.add_schedule(_one_time_schedule(f"schedule-{index:03d}"))

    assert len(await source.get_schedules()) == 101
    schedules.held_calls = 0

    assert await source.get_schedules() == []
    assert schedules.held_calls == 100


@pytest.mark.anyio
async def test_refresh_rotates_bounded_pending_claim_checks() -> None:
    clock = Clock(1_000)
    schedules = CountingHeldSchedules(InMemoryScheduleCache(clock))
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            due_limit=101,
        ),
    )
    for index in range(101):
        await source.add_schedule(_one_time_schedule(f"schedule-{index:03d}"))

    assert len(await source.get_schedules()) == 101
    await schedules.delete("taskiq-scheduler", "schedule-100")
    schedules.held_calls = 0

    assert await source.get_schedules() == []
    assert schedules.held_calls == 100
    assert any(
        claim.record.identity == "schedule-100"
        for pending in source._pending.values()
        if (claim := pending.claim)
    )
    schedules.held_calls = 0

    assert await source.get_schedules() == []
    assert schedules.held_calls == 100
    assert all(
        claim.record.identity != "schedule-100"
        for pending in source._pending.values()
        if (claim := pending.claim)
    )


@pytest.mark.anyio
async def test_failed_taskiq_enqueue_recovers_without_retaining_stale_dispatch() -> (
    None
):
    clock = Clock(1_000)
    source = CacheTaskiqScheduleSource(
        InMemoryScheduleCache(clock),
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    await source.add_schedule(_one_time_schedule())
    first = (await source.get_schedules())[0]

    with pytest.raises(SendTaskError, match="Cannot send"):
        await TaskiqScheduler(FailingBroker(), [source]).on_ready(source, first)

    clock.value += 31
    second = (await source.get_schedules())[0]

    with pytest.raises(ScheduledTaskCancelledError):
        await source.pre_send(first)
    await source.pre_send(second)


@pytest.mark.anyio
async def test_taskiq_scheduler_sends_one_shot_dispatch_and_settles_claim() -> None:
    clock = Clock(1_000)
    source = CacheTaskiqScheduleSource(
        InMemoryScheduleCache(clock),
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    await source.add_schedule(_one_time_schedule("stable"))
    dispatch = (await source.get_schedules())[0]
    claim = source._pending[dispatch.schedule_id].claim
    broker = RecordingBroker()

    await TaskiqScheduler(broker, [source]).on_ready(source, dispatch)

    message = broker.messages[0]
    assert message.labels["schedule_id"] == dispatch.schedule_id
    assert claim.token not in message.labels["schedule_id"]
    assert await source.get_schedules() == []


@pytest.mark.anyio
async def test_expired_claim_recovers_on_another_schedule_source() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    first_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-a", claim_ttl_seconds=30),
    )
    second_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-b", claim_ttl_seconds=30),
    )
    await first_source.add_schedule(_one_time_schedule())
    first = (await first_source.get_schedules())[0]

    assert await second_source.get_schedules() == []
    clock.value += 31
    second = (await second_source.get_schedules())[0]

    with pytest.raises(ScheduledTaskCancelledError):
        await first_source.pre_send(first)
    await second_source.post_send(second)


@pytest.mark.anyio
async def test_post_send_ignores_claim_lost_after_final_check() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    await source.add_schedule(_one_time_schedule())
    dispatch = (await source.get_schedules())[0]
    source._schedules = DeleteAfterHeldSchedules(schedules)

    await source.post_send(dispatch)


@pytest.mark.anyio
async def test_deleting_a_cron_schedule_during_advance_is_a_normal_cancellation() -> (
    None
):
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    await source.add_schedule(
        ScheduledTask(
            task_name="tests.cron",
            labels={},
            args=[],
            kwargs={},
            cron="* * * * *",
            schedule_id="cron",
        )
    )
    clock.value = 1_200
    source._schedules = DeleteDuringAdvanceSchedules(schedules)

    assert await source.get_schedules() == []
    assert source._refreshing == {}


@pytest.mark.anyio
async def test_slow_cron_handoff_leaves_missed_runs_for_coalescing() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=300),
    )
    await source.add_schedule(
        ScheduledTask(
            task_name="tests.cron",
            labels={},
            args=[],
            kwargs={},
            cron="* * * * *",
            schedule_id="cron",
        )
    )
    clock.value = 1_200
    assert await source.get_schedules() == []
    dispatch = (await source.get_schedules())[0]

    clock.value = 1_440
    await source.post_send(dispatch)

    records = await schedules.due("taskiq-scheduler", before=clock())
    assert [record.next_due_at for record in records] == [1_260]


@pytest.mark.anyio
async def test_shutdown_keeps_a_claim_during_an_in_flight_broker_handoff() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    first = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-a", claim_ttl_seconds=30),
    )
    second = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-b", claim_ttl_seconds=30),
    )
    await first.add_schedule(_one_time_schedule())
    dispatch = (await first.get_schedules())[0]
    broker = BlockingBroker()
    send = asyncio.create_task(
        TaskiqScheduler(broker, [first]).on_ready(first, dispatch)
    )
    await broker.started.wait()

    await first.shutdown()

    assert await second.get_schedules() == []
    broker.unblock.set()
    await send
    assert first._pending == {}


@pytest.mark.anyio
async def test_failed_post_send_settlement_retains_the_in_flight_claim() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-a", claim_ttl_seconds=30),
    )
    other = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-b", claim_ttl_seconds=30),
    )
    await source.add_schedule(_one_time_schedule())
    dispatch = (await source.get_schedules())[0]
    source._schedules = FailingCompleteSchedules(schedules)

    with pytest.raises(RuntimeError, match="complete unavailable"):
        await TaskiqScheduler(RecordingBroker(), [source]).on_ready(source, dispatch)

    assert source._pending[dispatch.schedule_id].handoff_in_flight
    await source.shutdown()
    assert await other.get_schedules() == []


@pytest.mark.anyio
@pytest.mark.parametrize(
    "local_time",
    (
        datetime(2026, 4, 5, 2, 30),
        datetime(2026, 10, 4, 2, 30),
    ),
)
async def test_naive_dst_transition_time_is_rejected(local_time: datetime) -> None:
    source = CacheTaskiqScheduleSource(
        InMemoryScheduleCache(),
        policy=TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            timezone="Australia/Melbourne",
        ),
    )
    task = _one_time_schedule()
    task.time = local_time

    with pytest.raises(ValueError, match="ambiguous or non-existent"):
        await source.add_schedule(task)


@pytest.mark.anyio
async def test_dispatch_identity_does_not_expose_claim_token() -> None:
    clock = Clock(1_000)
    source = CacheTaskiqScheduleSource(
        InMemoryScheduleCache(clock),
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    await source.add_schedule(_one_time_schedule("stable"))

    dispatch = (await source.get_schedules())[0]
    claim = source._pending[dispatch.schedule_id].claim

    assert dispatch.schedule_id.startswith("stable:dispatch:")
    assert claim.token not in dispatch.schedule_id
