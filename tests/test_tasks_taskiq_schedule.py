from __future__ import annotations

from datetime import UTC, datetime, timedelta

import pytest
from taskiq import ScheduledTask
from taskiq.cli.scheduler.run import is_time_task_now
from taskiq.exceptions import ScheduledTaskCancelledError

from wybra.cache import (
    MAX_CACHE_FEATURE_LIMIT,
    CacheTimeCapability,
    InMemoryCacheTime,
    InMemoryScheduleCache,
)
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


class CountingAdvanceSchedules:
    def __init__(self, schedules: InMemoryScheduleCache) -> None:
        self._schedules = schedules
        self.advance_calls = 0

    def __getattr__(self, name: str):
        return getattr(self._schedules, name)

    async def advance(self, *args: object, **kwargs: object):
        self.advance_calls += 1
        return await self._schedules.advance(*args, **kwargs)


def _schedule(*, cron: str, schedule_id: str = "hourly") -> ScheduledTask:
    return ScheduledTask(
        task_name="tests.hourly",
        labels={},
        args=[],
        kwargs={},
        cron=cron,
        schedule_id=schedule_id,
    )


def _dispatch_task(
    schedules: list[ScheduledTask],
    original: ScheduledTask,
) -> ScheduledTask:
    assert len(schedules) == 1
    dispatch = schedules[0]
    assert dispatch.task_name == original.task_name
    assert dispatch.labels == original.labels
    assert dispatch.args == original.args
    assert dispatch.kwargs == original.kwargs
    assert dispatch.schedule_id.startswith(f"{original.schedule_id}:")
    assert dispatch.cron is None
    assert dispatch.interval is None
    assert dispatch.time is not None
    assert dispatch.time == datetime(1970, 1, 1, tzinfo=dispatch.time.tzinfo)
    return dispatch


def test_schedule_source_requires_cache_time() -> None:
    with pytest.raises(TypeError, match="cache_time"):
        _CacheTaskiqScheduleSource(
            InMemoryScheduleCache(),
            policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
        )


@pytest.mark.anyio
async def test_interval_schedule_uses_the_selected_cache_time() -> None:
    provider_clock = Clock(1_000)
    schedules = InMemoryScheduleCache(provider_clock)
    policy = TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30)
    task = ScheduledTask(
        task_name="tests.interval",
        labels={},
        args=[],
        kwargs={},
        interval=timedelta(minutes=1),
        schedule_id="interval",
    )
    producer = CacheTaskiqScheduleSource(
        schedules,
        policy=policy,
        cache_time=InMemoryCacheTime(provider_clock),
    )

    await producer.add_schedule(task)
    provider_clock.value = 1_001
    consumer = CacheTaskiqScheduleSource(
        schedules,
        policy=policy,
        cache_time=InMemoryCacheTime(provider_clock),
    )

    assert (
        _dispatch_task(await consumer.get_schedules(), task).task_name == task.task_name
    )


@pytest.mark.anyio
async def test_cron_schedule_coalesces_an_outage_to_latest_matching_run() -> None:
    clock = Clock(3_600)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    task = _schedule(cron="0 * * * *")
    await source.add_schedule(task)

    clock.value = 10_800
    assert await source.get_schedules() == []
    ready = _dispatch_task(await source.get_schedules(), task)

    assert ready.time is not None
    assert is_time_task_now(ready.time, datetime.fromtimestamp(clock(), UTC))
    await source.post_send(ready)
    clock.value = 10_860
    assert await source.get_schedules() == []


@pytest.mark.anyio
async def test_cron_schedule_retains_only_the_configured_recent_missed_runs() -> None:
    clock = Clock(3_600)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            catch_up_limit=2,
        ),
    )
    task = _schedule(cron="0 * * * *")
    await source.add_schedule(task)

    clock.value = 14_400
    assert await source.get_schedules() == []
    first = _dispatch_task(await source.get_schedules(), task)
    await source.post_send(first)
    second = _dispatch_task(await source.get_schedules(), task)
    await source.post_send(second)

    clock.value = 14_460
    assert await source.get_schedules() == []


@pytest.mark.anyio
async def test_one_time_schedule_converts_naive_local_time_to_utc() -> None:
    clock = Clock(0)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    task = ScheduledTask(
        task_name="tests.melbourne",
        labels={},
        args=[],
        kwargs={},
        time=datetime(2026, 1, 1, 9),
        schedule_id="melbourne",
    )

    await source.add_schedule(task, timezone="Australia/Melbourne")
    records = await schedules.due("taskiq-scheduler", before=1_800_000_000)

    assert records[0].next_due_at == 1_767_218_400


@pytest.mark.anyio
async def test_deleting_claimed_schedule_cancels_it_before_broker_send() -> None:
    clock = Clock(1_000)
    source = CacheTaskiqScheduleSource(
        InMemoryScheduleCache(clock),
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    task = ScheduledTask(
        task_name="tests.once",
        labels={},
        args=[],
        kwargs={},
        time=datetime(1970, 1, 1, 0, 16, 40),
        schedule_id="once",
    )
    await source.add_schedule(task)

    dispatch = _dispatch_task(await source.get_schedules(), task)
    await source.delete_schedule(task.schedule_id)

    with pytest.raises(ScheduledTaskCancelledError):
        await source.pre_send(dispatch)


@pytest.mark.anyio
async def test_remote_deletion_cancels_a_claimed_schedule_before_broker_send() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    policy = TaskiqSchedulePolicy(claimant="scheduler-a", claim_ttl_seconds=30)
    source = CacheTaskiqScheduleSource(schedules, policy=policy)
    deleting_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-b", claim_ttl_seconds=30),
    )
    task = ScheduledTask(
        task_name="tests.once",
        labels={},
        args=[],
        kwargs={},
        time=datetime(1970, 1, 1, 0, 16, 40),
        schedule_id="remote-once",
    )
    await source.add_schedule(task)

    dispatch = _dispatch_task(await source.get_schedules(), task)
    await deleting_source.delete_schedule(task.schedule_id)

    with pytest.raises(ScheduledTaskCancelledError):
        await source.pre_send(dispatch)


@pytest.mark.anyio
async def test_interval_schedule_dispatches_then_advances_to_its_next_run() -> None:
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
        schedule_id="interval",
    )
    await source.add_schedule(task)

    dispatch = _dispatch_task(await source.get_schedules(), task)
    await source.post_send(dispatch)

    assert await schedules.due("taskiq-scheduler", before=clock()) == ()


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("time_value", "interval", "cron"),
    (
        (datetime(1970, 1, 1), None, "0 * * * *"),
        (None, 60, "0 * * * *"),
    ),
)
async def test_schedule_rejects_ambiguous_taskiq_schedule_kinds(
    time_value: datetime | None,
    interval: int | None,
    cron: str | None,
) -> None:
    source = CacheTaskiqScheduleSource(
        InMemoryScheduleCache(),
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    task = ScheduledTask(
        task_name="tests.ambiguous",
        labels={},
        args=[],
        kwargs={},
        time=time_value,
        interval=interval,
        cron=cron,
    )

    with pytest.raises(ValueError, match="exactly one"):
        await source.add_schedule(task)


@pytest.mark.anyio
async def test_schedule_rejects_taskiq_cron_offset() -> None:
    source = CacheTaskiqScheduleSource(
        InMemoryScheduleCache(),
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    task = ScheduledTask(
        task_name="tests.offset",
        labels={},
        args=[],
        kwargs={},
        cron="0 * * * *",
        cron_offset=timedelta(hours=10),
    )

    with pytest.raises(ValueError, match="IANA timezone"):
        await source.add_schedule(task)


@pytest.mark.anyio
@pytest.mark.parametrize(
    "cron",
    ("", "not a cron", "* * * * * *", "* * * * * * *"),
)
async def test_schedule_rejects_invalid_or_non_five_field_cron(cron: str) -> None:
    source = CacheTaskiqScheduleSource(
        InMemoryScheduleCache(),
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    task = _schedule(cron=cron)

    with pytest.raises(ValueError, match="valid five-field"):
        await source.add_schedule(task)


@pytest.mark.parametrize("field", ("due_limit", "catch_up_limit"))
def test_schedule_policy_rejects_limits_above_cache_bound(field: str) -> None:
    with pytest.raises(ValueError, match="no greater"):
        TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            **{field: MAX_CACHE_FEATURE_LIMIT + 1},
        )


def test_schedule_policy_accepts_refresh_intervals_above_one_minute() -> None:
    TaskiqSchedulePolicy(
        claimant="scheduler",
        claim_ttl_seconds=30,
        source_refresh_interval_seconds=61,
    )


@pytest.mark.anyio
async def test_cron_schedule_accepts_a_refresh_interval_above_one_minute() -> None:
    source = CacheTaskiqScheduleSource(
        InMemoryScheduleCache(),
        policy=TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            source_refresh_interval_seconds=61,
        ),
    )

    await source.add_schedule(_schedule(cron="0 0 * * *"))


@pytest.mark.anyio
async def test_sparse_cron_waits_for_its_next_actual_occurrence() -> None:
    clock = Clock(0)
    schedules = InMemoryScheduleCache(clock)
    counting = CountingAdvanceSchedules(schedules)
    source = CacheTaskiqScheduleSource(
        counting,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    task = _schedule(cron="0 0 * * *", schedule_id="daily")
    await source.add_schedule(task)

    clock.value = 60
    assert await source.get_schedules() == []
    assert counting.advance_calls == 0
    clock.value = 86_400
    dispatch = _dispatch_task(await source.get_schedules(), task)

    assert counting.advance_calls == 0
    await source.post_send(dispatch)
    assert counting.advance_calls == 1


@pytest.mark.parametrize("owner", ("", "   "))
def test_schedule_policy_rejects_blank_owner(owner: str) -> None:
    with pytest.raises(ValueError, match="owner.*non-blank"):
        TaskiqSchedulePolicy(
            claimant="scheduler",
            claim_ttl_seconds=30,
            owner=owner,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    (("owner", " scheduler"), ("claimant", "scheduler ")),
)
def test_schedule_policy_rejects_surrounding_whitespace(
    field: str,
    value: str,
) -> None:
    with pytest.raises(ValueError, match="non-blank"):
        policy = {"claimant": "scheduler", "claim_ttl_seconds": 30}
        policy[field] = value
        TaskiqSchedulePolicy(**policy)


@pytest.mark.anyio
async def test_schedule_policy_owner_isolates_schedule_sets() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    first = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="first-scheduler",
            claim_ttl_seconds=30,
            owner="first-set",
        ),
    )
    second = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(
            claimant="second-scheduler",
            claim_ttl_seconds=30,
            owner="second-set",
        ),
    )
    first_task = ScheduledTask(
        task_name="tests.first",
        labels={},
        args=[],
        kwargs={},
        time=datetime.fromtimestamp(clock(), UTC),
        schedule_id="shared",
    )
    second_task = first_task.model_copy(update={"task_name": "tests.second"})

    await first.add_schedule(first_task)
    await second.add_schedule(second_task)
    await first.delete_schedule(first_task.schedule_id)

    assert await first.get_schedules() == []
    ready = await second.get_schedules()
    assert [task.task_name for task in ready] == [second_task.task_name]


@pytest.mark.anyio
async def test_invalid_schedule_envelope_is_discarded_without_aborting_poll() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler", claim_ttl_seconds=30),
    )
    await schedules.create(
        "taskiq-scheduler",
        "corrupt",
        b"not-json",
        next_due_at=clock(),
    )

    assert await source.get_schedules() == []

    assert await schedules.create(
        "taskiq-scheduler",
        "corrupt",
        b"replacement",
        next_due_at=clock(),
    )


@pytest.mark.anyio
async def test_deletion_between_send_hooks_does_not_fail_settlement() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-a", claim_ttl_seconds=30),
    )
    deleting_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-b", claim_ttl_seconds=30),
    )
    task = ScheduledTask(
        task_name="tests.once",
        labels={},
        args=[],
        kwargs={},
        time=datetime(1970, 1, 1, 0, 16, 40),
        schedule_id="between-hooks",
    )
    await source.add_schedule(task)
    dispatch = _dispatch_task(await source.get_schedules(), task)

    await source.pre_send(dispatch)
    await deleting_source.delete_schedule(task.schedule_id)
    await source.post_send(dispatch)


@pytest.mark.anyio
async def test_shutdown_releases_live_claims_after_a_stale_claim() -> None:
    clock = Clock(1_000)
    schedules = InMemoryScheduleCache(clock)
    source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-a", claim_ttl_seconds=30),
    )
    deleting_source = CacheTaskiqScheduleSource(
        schedules,
        policy=TaskiqSchedulePolicy(claimant="scheduler-b", claim_ttl_seconds=30),
    )
    for schedule_id in ("first", "second"):
        await source.add_schedule(
            ScheduledTask(
                task_name=f"tests.{schedule_id}",
                labels={},
                args=[],
                kwargs={},
                time=datetime(1970, 1, 1, 0, 16, 40),
                schedule_id=schedule_id,
            )
        )

    assert len(await source.get_schedules()) == 2
    await deleting_source.delete_schedule("first")
    await source.shutdown()

    assert (
        await schedules.claim("taskiq-scheduler", "second", "other", ttl=30) is not None
    )
