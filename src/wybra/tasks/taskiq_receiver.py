"""Cache-aware Taskiq receiver with renewable delivery settlement."""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import Awaitable, Callable, Coroutine
from time import monotonic
from typing import Any, cast
from uuid import UUID

from taskiq import AckableMessage, NoResultError, TaskiqMiddleware, TaskiqResult
from taskiq.acks import AcknowledgeType
from taskiq.message import TaskiqMessage
from taskiq.receiver import Receiver

from wybra.tasks.taskiq_broker import CacheTaskiqBroker
from wybra.tasks.taskiq_lifecycle import CacheTaskiqLifecycleMiddleware
from wybra.tasks.taskiq_protocol import DELIVERY_RECEIPT_LABEL

_LOGGER = logging.getLogger(__name__)
_MAXIMUM_CANCELLATION_DRAIN_SECONDS = 1.0


class CacheTaskiqReceiver(Receiver):
    """Acknowledge obsolete cache-backed retry deliveries before execution."""

    def __init__(
        self,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        if self.ack_time is not AcknowledgeType.WHEN_SAVED:
            raise ValueError(
                "CacheTaskiqReceiver requires acknowledgement after task result "
                "and lifecycle persistence."
            )
        lifecycle_middlewares = [
            middleware
            for middleware in self.broker.middlewares
            if isinstance(middleware, CacheTaskiqLifecycleMiddleware)
        ]
        if len(lifecycle_middlewares) != 1:
            raise RuntimeError(
                "CacheTaskiqReceiver requires exactly one lifecycle middleware."
            )
        self._lifecycle = lifecycle_middlewares[0]
        if not isinstance(self.broker, CacheTaskiqBroker):
            raise RuntimeError("CacheTaskiqReceiver requires CacheTaskiqBroker.")
        self._cache_broker = self.broker
        self._active_callbacks: set[asyncio.Task[Any]] = set()

    async def callback(
        self,
        message: bytes | AckableMessage,
        raise_err: bool = False,
    ) -> None:
        """Execute one delivery and retain it until worker shutdown settles it."""
        callback = asyncio.current_task()
        if callback is None:
            await self._callback(message, raise_err)
            return
        self._active_callbacks.add(callback)
        try:
            await self._callback(message, raise_err)
        finally:
            self._active_callbacks.discard(callback)

    async def _callback(
        self,
        message: bytes | AckableMessage,
        raise_err: bool = False,
    ) -> None:
        """Execute one delivery without exposing task content in diagnostics.

        ``raise_err`` preserves Taskiq's result-persistence contract: it raises
        only when a completed task result cannot be saved. Handler failures are
        represented through retry and lifecycle middleware.
        """
        broker_receipt = (
            self._cache_broker.delivery_receipt(message)
            if isinstance(message, AckableMessage)
            else None
        )
        task_message = self._task_message(message)
        if task_message is None:
            _LOGGER.warning("Taskiq delivery could not be parsed; skipping execution.")
            await self._relinquish_prefetch_delivery(broker_receipt)
            return
        task = self.broker.local_task_registry.get(task_message.task_name)
        if task is None:
            _LOGGER.warning(
                "Taskiq delivery references an unknown task for task %s; leaving it "
                "unacknowledged for visibility recovery.",
                _diagnostic_task_id(task_message.task_id),
            )
            await self._relinquish_prefetch_delivery(broker_receipt)
            return
        if not isinstance(message, AckableMessage):
            await self._execute_delivery(task_message, task.original_func, raise_err)
            return
        labelled_receipt = task_message.labels.get(DELIVERY_RECEIPT_LABEL)
        if not isinstance(labelled_receipt, str):
            _LOGGER.warning(
                "Taskiq delivery for task %s (%s) is missing its cache receipt; "
                "leaving it unacknowledged for visibility recovery.",
                task.task_name,
                _diagnostic_task_id(task_message.task_id),
            )
            await self._relinquish_prefetch_delivery(broker_receipt)
            return
        if broker_receipt is not None and labelled_receipt != broker_receipt:
            _LOGGER.warning(
                "Taskiq delivery for task %s (%s) has an invalid cache receipt; "
                "leaving it unacknowledged for visibility recovery.",
                task.task_name,
                _diagnostic_task_id(task_message.task_id),
            )
            await self._relinquish_prefetch_delivery(broker_receipt)
            return
        receipt = broker_receipt or labelled_receipt

        async def process_delivery() -> None:
            if await self._lifecycle.is_obsolete_retry_delivery(task_message):
                await _maybe_await(message.ack())
                return
            if await self._execute_delivery(
                task_message,
                task.original_func,
                raise_err,
            ):
                await _maybe_await(message.ack())

        try:
            visibility_timeout = self._cache_broker.delivery_visibility_timeout(
                task_message.labels
            )
        except ValueError:
            await self._relinquish_prefetch_delivery(broker_receipt)
            raise
        await self._run_with_delivery_renewal(
            receipt=receipt,
            renew=self._cache_broker.renew_delivery,
            visibility_timeout=visibility_timeout,
            operation=process_delivery,
        )

    async def _relinquish_prefetch_delivery(self, receipt: str | None) -> None:
        if receipt is not None:
            await self._cache_broker.relinquish_delivery(receipt)

    async def listen(self, finish_event: asyncio.Event) -> None:
        """Start interruptibly, then let Taskiq drain on graceful shutdown."""

        self.broker.is_worker_process = True
        if not self.run_startup:
            try:
                await self._listen_with_bounded_shutdown(finish_event)
            finally:
                await self._cancel_active_callbacks()
            return
        startup = asyncio.create_task(
            self.broker.startup(),
            name="wybra-taskiq-broker-startup",
        )
        shutdown_waiter = asyncio.create_task(
            finish_event.wait(),
            name="wybra-taskiq-startup-shutdown-waiter",
        )
        try:
            done, _pending = await asyncio.wait(
                (startup, shutdown_waiter),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if startup not in done:
                await _cancel_task(startup)
                return
            await startup
        finally:
            if not shutdown_waiter.done():
                await _cancel_task(shutdown_waiter)
        original_run_startup = self.run_startup
        self.run_startup = False
        try:
            await self._listen_with_bounded_shutdown(finish_event)
        finally:
            try:
                await self._cancel_active_callbacks()
            finally:
                self.run_startup = original_run_startup

    async def _listen_with_bounded_shutdown(
        self,
        finish_event: asyncio.Event,
    ) -> None:
        """Bound Taskiq shutdown even when its runner is waiting for capacity."""
        listener = asyncio.create_task(
            super().listen(finish_event),
            name="wybra-taskiq-receiver-listen",
        )
        shutdown_waiter = asyncio.create_task(
            finish_event.wait(),
            name="wybra-taskiq-receiver-shutdown-waiter",
        )
        try:
            done, _pending = await asyncio.wait(
                (listener, shutdown_waiter),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if listener in done:
                await listener
                return
            try:
                await asyncio.wait_for(
                    asyncio.shield(listener),
                    timeout=self.wait_tasks_timeout,
                )
            except TimeoutError:
                await self._cancel_active_callbacks()
                await _cancel_task(listener)
            else:
                await listener
        finally:
            if not shutdown_waiter.done():
                await _cancel_task(shutdown_waiter)
            if not listener.done():
                await _cancel_task(listener)

    async def _cancel_active_callbacks(self) -> None:
        """Cancel callbacks that outlive Taskiq's bounded graceful drain."""
        current = asyncio.current_task()
        callbacks = tuple(
            callback
            for callback in self._active_callbacks
            if callback is not current and not callback.done()
        )
        for callback in callbacks:
            callback.cancel()
        if callbacks:
            _done, pending = await asyncio.wait(
                callbacks,
                timeout=min(
                    self.wait_tasks_timeout or _MAXIMUM_CANCELLATION_DRAIN_SECONDS,
                    _MAXIMUM_CANCELLATION_DRAIN_SECONDS,
                ),
            )
            if pending:
                _LOGGER.warning(
                    "Taskiq callbacks ignored cancellation during worker shutdown; "
                    "their deliveries remain available for recovery."
                )

    def _task_message(self, message: bytes | AckableMessage) -> TaskiqMessage | None:
        try:
            message_data = (
                message.data if isinstance(message, AckableMessage) else message
            )
            task_message = self.broker.formatter.loads(message=message_data)
            task_message.parse_labels()
        except Exception:
            return None
        return task_message

    async def _execute_delivery(
        self,
        message: TaskiqMessage,
        target: Callable[..., Any],
        raise_err: bool,
    ) -> bool:
        for middleware in self.broker.middlewares:
            if middleware.__class__.pre_execute is not TaskiqMiddleware.pre_execute:
                message = await _maybe_await(middleware.pre_execute(message))
        result, intentionally_skipped_result = await self._run_registered_task(
            message,
            target,
        )
        for middleware in reversed(self.broker.middlewares):
            if middleware.__class__.post_execute is not TaskiqMiddleware.post_execute:
                await _maybe_await(middleware.post_execute(message, result))
        if isinstance(result.error, NoResultError):
            if intentionally_skipped_result:
                await self._lifecycle.result_skipped(message)
            return True
        try:
            await self.broker.result_backend.set_result(message.task_id, result)
            for middleware in reversed(self.broker.middlewares):
                if middleware.__class__.post_save is not TaskiqMiddleware.post_save:
                    await _maybe_await(middleware.post_save(message, result))
        except Exception as error:
            _LOGGER.error(
                "Taskiq result persistence failed for task %s: %s.",
                message.task_id,
                type(error).__name__,
            )
            if raise_err:
                raise
            return False
        return True

    async def _run_registered_task(
        self,
        message: TaskiqMessage,
        target: Callable[..., Any],
    ) -> tuple[TaskiqResult[Any], bool]:
        started_at = monotonic()
        returned: object | None = None
        error: BaseException | None = None
        try:
            returned = await _maybe_await(target(*message.args, **message.kwargs))
        except Exception as exc:
            error = exc
            _LOGGER.error(
                "Taskiq task %s failed for task %s: %s.",
                message.task_name,
                message.task_id,
                type(exc).__name__,
            )
        intentionally_skipped_result = isinstance(error, NoResultError)
        result = TaskiqResult(
            is_err=error is not None,
            return_value=returned,
            execution_time=round(monotonic() - started_at, 2),
            error=error,
            labels=message.labels,
        )
        if error is not None:
            for middleware in reversed(self.broker.middlewares):
                if middleware.__class__.on_error is not TaskiqMiddleware.on_error:
                    await _maybe_await(middleware.on_error(message, result, error))
        return result, intentionally_skipped_result

    async def _run_with_delivery_renewal(
        self,
        *,
        receipt: str,
        renew: Callable[..., Awaitable[None]],
        visibility_timeout: float,
        operation: Callable[[], Coroutine[object, object, None]],
    ) -> None:
        await renew(receipt, visibility_timeout=visibility_timeout)
        execution = asyncio.create_task(
            operation(),
            name="wybra-taskiq-delivery-execution",
        )
        renewal = asyncio.create_task(
            self._renew_until_settled(
                receipt,
                renew=renew,
                visibility_timeout=visibility_timeout,
            ),
            name="wybra-taskiq-delivery-renewal",
        )
        try:
            done, _pending = await asyncio.wait(
                (execution, renewal),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if execution in done:
                if renewal in done:
                    execution_result, renewal_result = await asyncio.gather(
                        execution,
                        renewal,
                        return_exceptions=True,
                    )
                    if isinstance(execution_result, BaseException):
                        raise execution_result
                    if isinstance(renewal_result, BaseException):
                        raise renewal_result
                else:
                    await execution
                return
            if renewal in done:
                renewal.result()
            await execution
        finally:
            for task in (execution, renewal):
                if not task.done():
                    task.cancel()
            _done, pending = await asyncio.wait(
                (execution, renewal),
                timeout=_MAXIMUM_CANCELLATION_DRAIN_SECONDS,
            )
            if pending:
                _LOGGER.warning(
                    "Taskiq delivery cancellation cleanup exceeded its bounded drain; "
                    "execution may continue and the delivery remains recoverable "
                    "under at-least-once semantics."
                )

    async def _renew_until_settled(
        self,
        receipt: str,
        *,
        renew: Callable[..., Awaitable[None]],
        visibility_timeout: float,
    ) -> None:
        interval = max(0.05, visibility_timeout / 2)
        while True:
            await asyncio.sleep(interval)
            await renew(receipt, visibility_timeout=visibility_timeout)


__all__ = ("CacheTaskiqReceiver",)


async def _maybe_await[T](value: T | Awaitable[T]) -> T:
    if inspect.isawaitable(value):
        return cast(T, await value)
    return cast(T, value)


async def _cancel_task[T](task: asyncio.Task[T]) -> None:
    if not task.done():
        task.cancel()
    await asyncio.wait((task,), timeout=_MAXIMUM_CANCELLATION_DRAIN_SECONDS)


def _diagnostic_task_id(value: object) -> str:
    """Return a valid task identifier for correlation without logging raw input."""

    if not isinstance(value, str):
        return "<invalid>"
    try:
        return str(UUID(value))
    except ValueError:
        return "<invalid>"
