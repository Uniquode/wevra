from __future__ import annotations

import asyncio
import logging
from asyncio import TimeoutError, sleep, wait_for
from uuid import uuid4

import pytest
from taskiq import AckableMessage, BrokerMessage, NoResultError, TaskiqMessage
from taskiq.acks import AcknowledgeType

from wybra.cache import (
    MAX_CACHE_FEATURE_PAYLOAD_BYTES,
    CacheFeatureError,
    CacheWorkQueueRejectedError,
    InMemoryAtomicCache,
    InMemoryCacheFeatures,
    InMemoryLeaseCache,
    InMemoryStreamCache,
    InMemoryWorkQueue,
)
from wybra.tasks import (
    RetryPolicy,
    TaskRegistrationError,
    TaskSubmissionError,
    current_task_context,
    task,
)
from wybra.tasks.lifecycle import TaskLifecycleKind, TaskState
from wybra.tasks.taskiq_broker import CacheTaskiqBroker, TaskiqBrokerPolicy
from wybra.tasks.taskiq_capabilities import CacheTaskiqTasksCapability
from wybra.tasks.taskiq_lifecycle import (
    CacheTaskiqLifecycleMiddleware,
    CacheTaskiqSmartRetryMiddleware,
    CacheTaskLifecycle,
    TaskiqLifecyclePolicy,
)
from wybra.tasks.taskiq_protocol import (
    TASK_RETRY_INITIAL_DELAY_LABEL,
    TASK_VISIBILITY_TIMEOUT_LABEL,
)
from wybra.tasks.taskiq_receiver import CacheTaskiqReceiver


def _capability(
    *,
    worker_concurrency: int = 1,
    work_queue: InMemoryWorkQueue | None = None,
    maximum_delivery_attempts: int = 3,
    wait_timeout_seconds: float = 1,
    active_status_timeout_seconds: float = 86_400,
    retry: bool = False,
    worker_shutdown_grace_seconds: float = 30,
) -> tuple[CacheTaskiqTasksCapability, CacheTaskiqBroker]:
    streams = InMemoryStreamCache()
    atomic = InMemoryAtomicCache()
    leases = InMemoryLeaseCache()
    features = InMemoryCacheFeatures(
        atomic=atomic,
        leases=leases,
        streams=streams,
    )
    lifecycle = CacheTaskLifecycle(
        streams,
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            worker_id="worker-1",
            status_retention_seconds=60,
            active_status_timeout_seconds=active_status_timeout_seconds,
        ),
        features.time,
    )
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    broker = CacheTaskiqBroker(
        InMemoryWorkQueue() if work_queue is None else work_queue,
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=0.1,
            wait_timeout_seconds=wait_timeout_seconds,
            maximum_delivery_attempts=maximum_delivery_attempts,
        ),
        on_publication_rejected=middleware.submission_rejected,
        on_delivery_exhausted=middleware.delivery_exhausted,
    )
    if retry:
        broker.add_middlewares(
            CacheTaskiqSmartRetryMiddleware(
                middleware,
                default_retry_count=2,
                default_retry_label=True,
                default_delay=0,
                no_result_on_retry=True,
            )
        )
    broker.add_middlewares(middleware)
    return (
        CacheTaskiqTasksCapability(
            broker=broker,
            lifecycle_store=lifecycle,
            lifecycle_middleware=middleware,
            worker_concurrency=worker_concurrency,
            worker_shutdown_grace_seconds=worker_shutdown_grace_seconds,
        ),
        broker,
    )


class _RejectingWorkQueue(InMemoryWorkQueue):
    async def publish(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        raise CacheWorkQueueRejectedError("Queue capacity exhausted.")


class _FailFirstLifecycleProjection(InMemoryAtomicCache):
    def __init__(self) -> None:
        super().__init__()
        self._fail_next_create = True

    async def create(self, *args: object, **kwargs: object) -> object:
        if self._fail_next_create:
            self._fail_next_create = False
            raise CacheFeatureError("Lifecycle projection unavailable.")
        return await super().create(*args, **kwargs)


class _AcceptedThenUnavailableWorkQueue(InMemoryWorkQueue):
    async def publish(self, *args: object, **kwargs: object):
        await super().publish(*args, **kwargs)
        raise CacheFeatureError("The cache response was lost.")


@pytest.mark.anyio
async def test_cache_taskiq_capability_hides_provider_submission_errors() -> None:
    capability, _broker = _capability(work_queue=_RejectingWorkQueue())

    @task(name="tests.provider_submission_error")
    async def operation() -> None:
        return None

    capability.register(operation)

    with pytest.raises(TaskSubmissionError) as raised:
        await capability.submit(operation, operation.payload())

    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True
    assert raised.value.acceptance_unknown is False


@pytest.mark.anyio
async def test_oversized_submission_is_definitively_rejected() -> None:
    capability, _broker = _capability()

    @task(name="tests.oversized_submission")
    async def operation(value: str) -> None:
        return None

    capability.register(operation)

    with pytest.raises(TaskSubmissionError) as raised:
        await capability.submit(
            operation,
            operation.payload(value="x" * MAX_CACHE_FEATURE_PAYLOAD_BYTES),
        )

    assert raised.value.acceptance_unknown is False
    status = await capability.status(raised.value.task_id)
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED


@pytest.mark.anyio
@pytest.mark.parametrize("error_type", (CacheFeatureError, ValueError))
async def test_definitive_rejection_remains_known_when_lifecycle_repair_fails(
    error_type: type[Exception],
) -> None:
    capability, broker = _capability(work_queue=_RejectingWorkQueue())

    @task(name="tests.rejected_submission_lifecycle_repair_failure")
    async def operation() -> None:
        return None

    async def fail_reconciliation(_message: TaskiqMessage) -> None:
        raise error_type("Lifecycle projection unavailable.")

    broker._on_publication_rejected = fail_reconciliation
    capability.register(operation)

    with pytest.raises(TaskSubmissionError) as raised:
        await capability.submit(operation, operation.payload())

    assert raised.value.acceptance_unknown is False


@pytest.mark.anyio
async def test_cache_taskiq_capability_hides_lifecycle_submission_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capability, _broker = _capability()

    @task(name="tests.lifecycle_submission_error")
    async def operation() -> None:
        return None

    async def fail_pre_send(_message: TaskiqMessage) -> TaskiqMessage:
        raise CacheFeatureError("secret lifecycle detail")

    capability.register(operation)
    monkeypatch.setattr(capability.lifecycle_middleware, "pre_send", fail_pre_send)

    with pytest.raises(TaskSubmissionError) as raised:
        await capability.submit(operation, operation.payload())

    assert raised.value.__cause__ is None
    assert raised.value.__suppress_context__ is True
    assert raised.value.acceptance_unknown is False


@pytest.mark.anyio
async def test_pre_publication_lifecycle_failure_is_terminalised() -> None:
    streams = InMemoryStreamCache()
    atomic = _FailFirstLifecycleProjection()
    leases = InMemoryLeaseCache()
    features = InMemoryCacheFeatures(atomic=atomic, leases=leases, streams=streams)
    lifecycle = CacheTaskLifecycle(
        streams,
        atomic,
        leases,
        TaskiqLifecyclePolicy(
            owner="task-lifecycle",
            stream="events",
            queue="default",
            worker_id="worker-1",
            status_retention_seconds=60,
        ),
        features.time,
    )
    middleware = CacheTaskiqLifecycleMiddleware(lifecycle)
    broker = CacheTaskiqBroker(
        InMemoryWorkQueue(),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=0.1,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
        on_publication_rejected=middleware.submission_rejected,
        on_delivery_exhausted=middleware.delivery_exhausted,
    )
    broker.add_middlewares(middleware)
    capability = CacheTaskiqTasksCapability(
        broker=broker,
        lifecycle_store=lifecycle,
        lifecycle_middleware=middleware,
    )

    @task(name="tests.pre_publication_lifecycle_failure")
    async def operation() -> None:
        return None

    capability.register(operation)

    with pytest.raises(TaskSubmissionError) as raised:
        await capability.submit(operation, operation.payload())

    assert raised.value.acceptance_unknown is False
    status = await capability.status(raised.value.task_id)
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert (
        await broker._work_queue.reserve(
            "taskiq-broker",
            "default",
            "probe",
            visibility_timeout=0.1,
        )
        is None
    )


@pytest.mark.anyio
async def test_cache_taskiq_capability_exposes_unknown_submission_outcome() -> None:
    capability, _broker = _capability(work_queue=_AcceptedThenUnavailableWorkQueue())

    @task(name="tests.unknown_submission_outcome")
    async def operation() -> None:
        return None

    capability.register(operation)

    with pytest.raises(TaskSubmissionError) as raised:
        await capability.submit(operation, operation.payload())

    error = raised.value
    assert error.acceptance_unknown is True
    assert await capability.status(error.task_id) is not None


def test_cache_taskiq_capability_installs_secret_safe_kicker_filter() -> None:
    CacheTaskiqBroker(
        InMemoryWorkQueue(),
        queue="default",
        consumer="worker-1",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=0.1,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=3,
        ),
    )
    logger = logging.getLogger("taskiq")
    secret_record = logger.makeRecord(
        logger.name,
        logging.DEBUG,
        "/site-packages/taskiq/kicker.py",
        1,
        "kicking task with secret argument",
        (),
        None,
    )
    ordinary_record = logger.makeRecord(
        logger.name,
        logging.INFO,
        "/site-packages/taskiq/kicker.py",
        1,
        "task accepted",
        (),
        None,
    )

    assert logger.filter(secret_record) is False
    assert logger.filter(ordinary_record)


@pytest.mark.anyio
async def test_cache_taskiq_capability_rejects_task_outside_runtime_registry() -> None:
    capability, _broker = _capability()

    @task(name="tests.unregistered_task")
    async def operation() -> None:
        return None

    with pytest.raises(TaskRegistrationError, match="configured modules"):
        await capability.submit(operation, operation.payload())


@pytest.mark.anyio
async def test_cache_taskiq_capability_rejects_different_definition_with_identity() -> (
    None
):
    capability, _broker = _capability()

    @task(name="tests.definition_identity")
    async def registered(value: int) -> None:
        return None

    @task(name="tests.definition_identity")
    async def replacement(value: str) -> None:
        return None

    capability.register(registered)

    with pytest.raises(TaskRegistrationError, match="does not match"):
        await capability.submit(replacement, replacement.payload("unexpected"))


@pytest.mark.anyio
async def test_cache_taskiq_capability_executes_registered_task_with_context() -> None:
    capability, broker = _capability()

    @task(
        name="tests.taskiq_capability",
        visibility_timeout_seconds=1,
    )
    async def operation(value: str) -> str:
        context = current_task_context()
        assert context is not None
        assert context.task_name == "tests.taskiq_capability"
        return value.upper()

    capability.register(operation)
    handle = await capability.submit(operation, operation.payload("wybra"))
    listener = broker.listen()
    received = await anext(listener)
    assert isinstance(received, AckableMessage)
    task_message = broker.formatter.loads(message=received.data)
    assert float(task_message.labels[TASK_VISIBILITY_TIMEOUT_LABEL]) == 1

    acknowledged = False
    original_acknowledge = received.ack

    async def acknowledge() -> None:
        nonlocal acknowledged
        acknowledged = True
        await original_acknowledge()

    received = AckableMessage(data=received.data, ack=acknowledge)
    receiver = CacheTaskiqReceiver(broker, validate_params=False, max_async_tasks=1)
    await receiver.callback(received)

    status = await handle.status()
    assert status is not None
    assert status.state is TaskState.SUCCEEDED
    assert acknowledged is True
    await listener.aclose()


@pytest.mark.anyio
async def test_initial_retry_delay_does_not_delay_submission() -> None:
    capability, broker = _capability(retry=True)

    @task(
        name="tests.initial_retry_delay",
        retry=RetryPolicy(max_attempts=2, initial_delay_seconds=10),
    )
    async def operation() -> None:
        return None

    capability.register(operation)
    await capability.submit(operation, operation.payload())
    listener = broker.listen()
    received = await anext(listener)
    message = broker.formatter.loads(message=received.data)

    assert "delay" not in message.labels
    assert float(message.labels[TASK_RETRY_INITIAL_DELAY_LABEL]) == 10
    await received.ack()
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_receiver_excludes_task_data_from_diagnostics(
    caplog: pytest.LogCaptureFixture,
) -> None:
    capability, broker = _capability()
    secret = "AUDIT_SECRET_ARGUMENT"

    @task(name="tests.secret_safe_receiver")
    async def operation(value: str) -> None:
        raise RuntimeError(f"failure:{value}")

    capability.register(operation)
    with caplog.at_level(logging.DEBUG):
        await capability.submit(operation, operation.payload(secret))
    assert secret not in caplog.text
    listener = broker.listen()
    received = await anext(listener)
    assert isinstance(received, AckableMessage)

    with caplog.at_level(logging.DEBUG):
        await capability.receiver(validate_params=False).callback(received)

    assert secret not in caplog.text
    assert "RuntimeError" in caplog.text
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_receiver_completes_task_that_skips_result() -> None:
    capability, broker = _capability()
    acknowledged = False

    @task(name="tests.no_result")
    async def operation() -> None:
        raise NoResultError

    capability.register(operation)
    handle = await capability.submit(operation, operation.payload())
    listener = broker.listen()
    received = await anext(listener)
    assert isinstance(received, AckableMessage)
    original_acknowledge = received.ack

    async def acknowledge() -> None:
        nonlocal acknowledged
        acknowledged = True
        await original_acknowledge()

    await capability.receiver(validate_params=False).callback(
        AckableMessage(data=received.data, ack=acknowledge)
    )

    status = await handle.status()
    assert status is not None
    assert status.state is TaskState.SUCCEEDED
    assert acknowledged is True
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_receiver_does_not_treat_retry_signal_as_success() -> None:
    capability, broker = _capability(retry=True)
    attempts = 0

    @task(name="tests.retry_signal", retry=RetryPolicy(max_attempts=2))
    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError

    capability.register(operation)
    handle = await capability.submit(operation, operation.payload())
    listener = broker.listen()
    receiver = capability.receiver(validate_params=False)
    await receiver.callback(await anext(listener))

    scheduled = await handle.status()
    assert scheduled is not None
    assert scheduled.state is TaskState.RETRY_SCHEDULED

    await receiver.callback(await anext(listener))

    completed = await handle.status()
    assert completed is not None
    assert completed.state is TaskState.SUCCEEDED
    assert attempts == 2
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_receiver_does_not_retry_progress_metadata_failure() -> None:
    capability, broker = _capability(retry=True)
    attempts = 0

    @task(name="tests.progress_failure", retry=RetryPolicy(max_attempts=2))
    async def operation() -> None:
        nonlocal attempts
        attempts += 1
        context = current_task_context()
        assert context is not None
        await context.report_progress({"invalid": object()})

    capability.register(operation)
    handle = await capability.submit(operation, operation.payload())
    listener = broker.listen()

    await capability.receiver(validate_params=False).callback(await anext(listener))

    status = await handle.status()
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert status.error_type == "TaskProgressError"
    assert attempts == 1
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_receiver_leaves_cancelled_delivery_for_recovery() -> None:
    capability, broker = _capability()
    started = asyncio.Event()
    acknowledged = False

    @task(name="tests.cancelled_delivery")
    async def operation() -> None:
        started.set()
        await asyncio.Event().wait()

    capability.register(operation)
    handle = await capability.submit(operation, operation.payload())
    listener = broker.listen()
    received = await anext(listener)
    assert isinstance(received, AckableMessage)
    original_acknowledge = received.ack

    async def acknowledge() -> None:
        nonlocal acknowledged
        acknowledged = True
        await original_acknowledge()

    execution = asyncio.create_task(
        capability.receiver(validate_params=False).callback(
            AckableMessage(data=received.data, ack=acknowledge)
        )
    )
    await started.wait()
    execution.cancel()

    with pytest.raises(asyncio.CancelledError):
        await execution

    status = await handle.status()
    assert status is not None
    assert status.state is TaskState.RUNNING
    assert acknowledged is False
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_receiver_propagates_simultaneous_renewal_failure() -> None:
    _, broker = _capability()
    receiver = CacheTaskiqReceiver(broker, validate_params=False, max_async_tasks=1)
    renewal_started = asyncio.Event()
    operation_finished = asyncio.Event()
    renewals = 0

    async def renew(_receipt: str, *, visibility_timeout: float) -> None:
        del visibility_timeout
        nonlocal renewals
        renewals += 1
        if renewals == 1:
            return
        renewal_started.set()
        await operation_finished.wait()
        raise RuntimeError("renewal failed")

    async def operation() -> None:
        await renewal_started.wait()
        operation_finished.set()

    with pytest.raises(RuntimeError, match="renewal failed"):
        await receiver._run_with_delivery_renewal(
            receipt="receipt",
            renew=renew,
            visibility_timeout=0.1,
            operation=operation,
        )


@pytest.mark.anyio
async def test_cache_taskiq_receiver_cancels_callbacks_left_after_taskiq_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capability, broker = _capability(worker_shutdown_grace_seconds=0.01)
    started = asyncio.Event()
    cancelled = asyncio.Event()
    acknowledged = False

    @task(name="tests.shutdown_cancelled_delivery")
    async def operation() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    capability.register(operation)
    handle = await capability.submit(operation, operation.payload())
    listener = broker.listen()
    received = await anext(listener)
    assert isinstance(received, AckableMessage)
    original_acknowledge = received.ack

    async def acknowledge() -> None:
        nonlocal acknowledged
        acknowledged = True
        await original_acknowledge()

    receiver = capability.receiver(validate_params=False)
    receiver.run_startup = False
    execution = asyncio.create_task(
        receiver.callback(AckableMessage(data=received.data, ack=acknowledge))
    )
    await started.wait()

    async def block_shutdown(
        _receiver: object,
        finish_event: asyncio.Event,
    ) -> None:
        await finish_event.wait()
        await asyncio.Event().wait()

    monkeypatch.setattr("taskiq.receiver.Receiver.listen", block_shutdown)
    shutdown_requested = asyncio.Event()
    shutdown_requested.set()
    await receiver.listen(shutdown_requested)

    assert execution.cancelled()
    assert cancelled.is_set()
    status = await handle.status()
    assert status is not None
    assert status.state is TaskState.RUNNING
    assert acknowledged is False
    await listener.aclose()


def test_cache_taskiq_receiver_uses_configured_worker_concurrency() -> None:
    capability, _broker = _capability(worker_concurrency=2)

    receiver = capability.receiver(validate_params=False)

    assert receiver.sem is not None
    assert receiver.sem._value == 2
    assert receiver.validate_params is False


def test_cache_taskiq_receiver_uses_configured_shutdown_grace() -> None:
    capability, _broker = _capability(worker_shutdown_grace_seconds=45)

    receiver = capability.receiver(validate_params=False)

    assert receiver.wait_tasks_timeout == 45


@pytest.mark.anyio
async def test_cache_taskiq_receiver_honours_zero_cancellation_drain(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capability, _broker = _capability()
    receiver = capability.receiver(validate_params=False)
    receiver.wait_tasks_timeout = 0
    callback = asyncio.create_task(asyncio.Event().wait())
    receiver._active_callbacks.add(callback)
    captured_timeout: float | None = None
    original_wait = asyncio.wait

    async def capture_wait(
        futures: object,
        *,
        timeout: float | None = None,
        return_when: object = asyncio.ALL_COMPLETED,
    ) -> tuple[set[asyncio.Future[object]], set[asyncio.Future[object]]]:
        nonlocal captured_timeout
        captured_timeout = timeout
        return await original_wait(futures, timeout=timeout, return_when=return_when)

    monkeypatch.setattr("wybra.tasks.taskiq_receiver.asyncio.wait", capture_wait)

    await receiver._cancel_active_callbacks()

    assert captured_timeout == 0


@pytest.mark.anyio
async def test_cache_taskiq_receiver_marks_delegated_startup_as_worker(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capability, broker = _capability()
    receiver = capability.receiver(validate_params=False)
    receiver.run_startup = False
    delegated = False

    async def listen(_receiver: object, _finish_event: asyncio.Event) -> None:
        nonlocal delegated
        delegated = True
        assert broker.is_worker_process is True

    monkeypatch.setattr("taskiq.receiver.Receiver.listen", listen)

    await receiver.listen(asyncio.Event())

    assert delegated is True


@pytest.mark.parametrize(
    "invalid_kwargs",
    (
        {"validate_params": True},
        {"executor": object()},
    ),
)
def test_cache_taskiq_receiver_rejects_unsupported_taskiq_execution_options(
    invalid_kwargs: dict[str, object],
) -> None:
    capability, _broker = _capability()

    with pytest.raises(ValueError, match="does not support"):
        capability.receiver(**invalid_kwargs)


@pytest.mark.parametrize(
    "acknowledgement",
    (AcknowledgeType.WHEN_RECEIVED, AcknowledgeType.WHEN_EXECUTED),
)
def test_cache_taskiq_receiver_requires_acknowledgement_after_persistence(
    acknowledgement: AcknowledgeType,
) -> None:
    _capability_instance, broker = _capability()

    with pytest.raises(ValueError, match="after task result"):
        CacheTaskiqReceiver(
            broker,
            ack_type=acknowledgement,
        )


@pytest.mark.anyio
async def test_cache_taskiq_receiver_leaves_unknown_tasks_for_recovery(
    caplog: pytest.LogCaptureFixture,
) -> None:
    capability, broker = _capability()
    acknowledged = False

    async def acknowledge() -> None:
        nonlocal acknowledged
        acknowledged = True

    untrusted_identifier = "secret-in-unknown-delivery"
    message = TaskiqMessage(
        task_id=untrusted_identifier,
        task_name=untrusted_identifier,
        labels={},
        args=[],
        kwargs={},
    )
    received = AckableMessage(
        data=broker.formatter.dumps(message).message,
        ack=acknowledge,
    )

    with caplog.at_level(logging.WARNING):
        await capability.receiver(validate_params=False).callback(received)

    assert acknowledged is False
    assert "unknown task" in caplog.text
    assert untrusted_identifier not in caplog.text


@pytest.mark.anyio
async def test_cache_taskiq_receiver_does_not_resolve_global_tasks(
    caplog: pytest.LogCaptureFixture,
) -> None:
    capability, broker = _capability()
    external = CacheTaskiqBroker(
        InMemoryWorkQueue(),
        queue="external",
        consumer="external-worker",
        policy=TaskiqBrokerPolicy(
            visibility_timeout_seconds=0.1,
            wait_timeout_seconds=1,
            maximum_delivery_attempts=1,
        ),
    )

    @external.task(task_name="tests.external_task")
    async def external_task() -> None:
        raise AssertionError("A site-local worker must not execute global tasks.")

    message = TaskiqMessage(
        task_id=str(uuid4()),
        task_name=external_task.task_name,
        labels={},
        args=[],
        kwargs={},
    )
    received = AckableMessage(
        data=broker.formatter.dumps(message).message,
        ack=lambda: None,
    )

    with caplog.at_level(logging.WARNING):
        await capability.receiver(validate_params=False).callback(received)

    assert "unknown task" in caplog.text
    assert "missing its cache receipt" not in caplog.text


@pytest.mark.anyio
async def test_cache_taskiq_receiver_leaves_missing_receipt_unacknowledged(
    caplog: pytest.LogCaptureFixture,
) -> None:
    capability, broker = _capability()
    acknowledged = False

    @task(name="tests.missing_receipt")
    async def operation() -> None:
        raise AssertionError("Message without a receipt must not execute.")

    capability.register(operation)

    async def acknowledge() -> None:
        nonlocal acknowledged
        acknowledged = True

    message = TaskiqMessage(
        task_id=str(uuid4()),
        task_name="wybra.tests.missing_receipt.__v1",
        labels={},
        args=[],
        kwargs={},
    )
    received = AckableMessage(
        data=broker.formatter.dumps(message).message,
        ack=acknowledge,
    )

    with caplog.at_level(logging.WARNING):
        await capability.receiver(validate_params=False).callback(received)

    assert acknowledged is False
    assert message.task_id in caplog.text


@pytest.mark.anyio
@pytest.mark.parametrize("delivery_kind", ("malformed", "unknown"))
async def test_worker_prefetch_relinquishes_unexecutable_delivery(
    delivery_kind: str,
) -> None:
    work_queue = InMemoryWorkQueue()
    capability, broker = _capability(
        work_queue=work_queue,
        maximum_delivery_attempts=2,
    )
    broker.is_worker_process = True
    if delivery_kind == "malformed":
        payload = b"not-a-taskiq-message"
    else:
        payload = broker.formatter.dumps(
            TaskiqMessage(
                task_id=str(uuid4()),
                task_name="tests.unknown_task",
                labels={},
                args=[],
                kwargs={},
            )
        ).message
    await broker.kick(
        BrokerMessage(
            task_id="poisoned-delivery",
            task_name="tests.envelope",
            message=payload,
            labels={},
        )
    )
    listener = broker.listen()
    first = await anext(listener)
    assert isinstance(first, AckableMessage)
    assert broker._prefetch_renewals

    await capability.receiver(validate_params=False).callback(first)

    assert broker._prefetch_renewals == {}
    await sleep(0.11)
    redelivered = await anext(listener)
    assert isinstance(redelivered, AckableMessage)
    await listener.aclose()


@pytest.mark.anyio
@pytest.mark.parametrize("visibility_timeout", ("invalid", "-1", "0.01"))
async def test_worker_prefetch_relinquishes_invalid_visibility_metadata(
    visibility_timeout: str,
) -> None:
    work_queue = InMemoryWorkQueue()
    capability, broker = _capability(
        work_queue=work_queue,
        maximum_delivery_attempts=2,
    )
    broker.is_worker_process = True

    @task(name="tests.invalid_visibility")
    async def operation() -> None:
        raise AssertionError("Invalid visibility metadata must not execute.")

    capability.register(operation)
    payload = broker.formatter.dumps(
        TaskiqMessage(
            task_id=str(uuid4()),
            task_name="wybra.tests.invalid_visibility.__v1",
            labels={TASK_VISIBILITY_TIMEOUT_LABEL: visibility_timeout},
            args=[],
            kwargs={},
        )
    ).message
    await broker.kick(
        BrokerMessage(
            task_id="invalid-visibility",
            task_name="tests.envelope",
            message=payload,
            labels={},
        )
    )
    listener = broker.listen()
    received = await anext(listener)
    assert isinstance(received, AckableMessage)
    assert broker._prefetch_renewals

    with pytest.raises(ValueError, match="visibility timeout"):
        await capability.receiver(validate_params=False).callback(received)

    assert broker._prefetch_renewals == {}
    await sleep(0.11)
    redelivered = await anext(listener)
    assert isinstance(redelivered, AckableMessage)
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_receiver_cancels_blocked_startup_on_shutdown(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capability, broker = _capability()
    started = asyncio.Event()
    cancelled = False

    async def blocked_startup() -> None:
        nonlocal cancelled
        assert broker.is_worker_process is True
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled = True
            raise

    monkeypatch.setattr(broker, "startup", blocked_startup)
    shutdown_requested = asyncio.Event()
    running = asyncio.create_task(
        capability.receiver(validate_params=False).listen(shutdown_requested),
    )

    await started.wait()
    shutdown_requested.set()
    await running

    assert cancelled is True


@pytest.mark.anyio
async def test_result_persistence_failure_leaves_delivery_active_for_recovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capability, broker = _capability()

    @task(name="tests.result_persistence")
    async def operation() -> str:
        return "complete"

    capability.register(operation)
    handle = await capability.submit(operation, operation.payload())
    listener = broker.listen()
    received = await anext(listener)
    assert isinstance(received, AckableMessage)

    async def fail_to_store_result(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError("result store unavailable")

    monkeypatch.setattr(broker.result_backend, "set_result", fail_to_store_result)

    await capability.receiver(validate_params=False).callback(received)

    status = await handle.status()
    assert status is not None
    assert status.state is TaskState.RUNNING
    assert broker._outstanding_deliveries
    await listener.aclose()


@pytest.mark.anyio
async def test_cache_taskiq_receiver_renews_while_checking_obsolete_delivery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capability, broker = _capability()

    @task(name="tests.obsolete_delivery")
    async def operation() -> None:
        raise AssertionError("Obsolete delivery must not execute.")

    capability.register(operation)
    await capability.submit(operation, operation.payload())
    listener = broker.listen()
    received = await anext(listener)
    assert isinstance(received, AckableMessage)

    async def slow_obsolete(_message: TaskiqMessage) -> bool:
        await sleep(0.25)
        return True

    renewals = 0
    original_renew = broker.renew_delivery

    async def count_renewals(*args: object, **kwargs: object) -> None:
        nonlocal renewals
        renewals += 1
        await original_renew(*args, **kwargs)

    monkeypatch.setattr(
        capability.lifecycle_middleware,
        "is_obsolete_retry_delivery",
        slow_obsolete,
    )
    monkeypatch.setattr(broker, "renew_delivery", count_renewals)

    await capability.receiver(validate_params=False).callback(received)

    assert renewals >= 3
    await listener.aclose()


@pytest.mark.anyio
async def test_definitively_rejected_submission_is_dead_lettered() -> None:
    capability, broker = _capability(work_queue=_RejectingWorkQueue())
    task_id = uuid4()
    message = TaskiqMessage(
        task_id=str(task_id),
        task_name="tests.rejected_submission",
        labels={},
        args=[],
        kwargs={},
    )
    await capability.lifecycle_middleware.pre_send(message)

    with pytest.raises(CacheWorkQueueRejectedError, match="capacity"):
        await broker.kick(broker.formatter.dumps(message))

    status = await capability.status(task_id)
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert status.error_type == "CacheWorkQueueRejectedError"
    assert [event.kind for event in await capability.lifecycle(task_id)] == [
        TaskLifecycleKind.SUBMITTED,
        TaskLifecycleKind.FAILED,
        TaskLifecycleKind.DEAD_LETTERED,
    ]


@pytest.mark.anyio
async def test_delivery_exhaustion_is_reconciled_to_terminal_lifecycle() -> None:
    work_queue = InMemoryWorkQueue()
    capability, broker = _capability(
        work_queue=work_queue,
        maximum_delivery_attempts=1,
        wait_timeout_seconds=0.01,
    )

    @task(name="tests.exhausted_delivery")
    async def operation() -> None:
        raise AssertionError("Abandoned delivery must not execute.")

    capability.register(operation)
    handle = await capability.submit(operation, operation.payload())
    listener = broker.listen()
    received = await anext(listener)
    assert isinstance(received, AckableMessage)
    started = broker.formatter.loads(message=received.data)
    started.parse_labels()
    await capability.lifecycle_middleware.pre_execute(started)

    status = await handle.status()
    assert status is not None
    assert status.state is TaskState.RUNNING

    await sleep(0.15)

    with pytest.raises(TimeoutError):
        await wait_for(anext(listener), timeout=0.2)

    status = await handle.status()
    assert status is not None
    assert status.state is TaskState.DEAD_LETTERED
    assert status.error_type == "CacheWorkQueueDeliveryExhausted"
    assert not hasattr(status, "delivery_identity")
    assert status._delivery_identity is None
    assert all(
        not hasattr(event, "delivery_identity")
        for event in await capability.lifecycle(handle.task_id)
    )
    assert all(
        event._delivery_identity is None
        for event in await capability.lifecycle(handle.task_id)
    )
    await listener.aclose()
