from __future__ import annotations

import asyncio
import logging
from collections import OrderedDict
from collections.abc import AsyncGenerator, Awaitable, Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar, Token
from dataclasses import dataclass, field
from math import isfinite
from time import monotonic
from uuid import uuid4

from taskiq import AckableMessage, AsyncBroker, BrokerMessage, TaskiqMessage

from wybra.cache import (
    MAX_CACHE_FEATURE_LIMIT,
    MAX_CACHE_FEATURE_PAYLOAD_BYTES,
    CacheConflictError,
    CacheFeatureError,
    CacheWorkQueueRejectedError,
    WorkDelivery,
    WorkQueueCacheCapability,
)
from wybra.tasks.config import (
    MAX_TASK_DELIVERY_ATTEMPTS,
    MINIMUM_TASK_VISIBILITY_TIMEOUT_SECONDS,
)
from wybra.tasks.lifecycle import TaskLifecycleError
from wybra.tasks.taskiq_protocol import (
    DELIVERY_ATTEMPT_LABEL,
    DELIVERY_IDENTITY_LABEL,
    DELIVERY_RECEIPT_LABEL,
    TASK_VISIBILITY_TIMEOUT_LABEL,
)

_TASKIQ_WORK_QUEUE_OWNER = "taskiq-broker"
_MINIMUM_DEAD_LETTER_OBSERVATION_INTERVAL_SECONDS = 0.1
_MAXIMUM_DEAD_LETTER_OBSERVATION_INTERVAL_SECONDS = 1.0
_PUBLICATION_RECONCILIATION_MAX_ATTEMPTS = 3
_CANCELLATION_JOIN_TIMEOUT_SECONDS = 1.0
_LOGGER = logging.getLogger(__name__)
_TASKIQ_LOGGER = logging.getLogger("taskiq")


class _SecretSafeTaskiqKickerFilter(logging.Filter):
    """Suppress Taskiq debug records that include task arguments."""

    def filter(self, record: logging.LogRecord) -> bool:
        return not (record.levelno < logging.INFO and record.module == "kicker")


_SECRET_SAFE_KICKER_FILTER = _SecretSafeTaskiqKickerFilter()


def _install_secret_safe_kicker_filter() -> None:
    if _SECRET_SAFE_KICKER_FILTER not in _TASKIQ_LOGGER.filters:
        _TASKIQ_LOGGER.addFilter(_SECRET_SAFE_KICKER_FILTER)


@dataclass(slots=True)
class _BrokerLifecycle:
    shutdown_requested: asyncio.Event = field(default_factory=asyncio.Event)


@dataclass(frozen=True, slots=True)
class _OutstandingDelivery:
    delivery: WorkDelivery


@dataclass(slots=True)
class _PublicationAttempt:
    task_id: str
    reached_broker: bool = False


@dataclass(frozen=True, slots=True)
class TaskiqBrokerPolicy:
    """Delivery controls for the cache-backed Taskiq broker."""

    visibility_timeout_seconds: float
    wait_timeout_seconds: float
    maximum_delivery_attempts: int

    def __post_init__(self) -> None:
        _validate_positive_finite(
            self.visibility_timeout_seconds,
            label="Visibility timeout",
        )
        if self.visibility_timeout_seconds < MINIMUM_TASK_VISIBILITY_TIMEOUT_SECONDS:
            raise ValueError(
                "Visibility timeout must be at least "
                f"{MINIMUM_TASK_VISIBILITY_TIMEOUT_SECONDS} seconds."
            )
        _validate_positive_finite(self.wait_timeout_seconds, label="Wait timeout")
        if (
            isinstance(self.maximum_delivery_attempts, bool)
            or not isinstance(self.maximum_delivery_attempts, int)
            or not 1 <= self.maximum_delivery_attempts <= MAX_TASK_DELIVERY_ATTEMPTS
        ):
            raise ValueError(
                "Maximum delivery attempts must be between one and "
                f"{MAX_TASK_DELIVERY_ATTEMPTS}."
            )


class CacheTaskiqBroker(AsyncBroker):
    """Adapt Taskiq broker messages to a durable Wybra work queue."""

    def __init__(
        self,
        work_queue: WorkQueueCacheCapability,
        *,
        queue: str,
        consumer: str,
        policy: TaskiqBrokerPolicy,
        on_publication_rejected: Callable[[TaskiqMessage], Awaitable[None]]
        | None = None,
        on_delivery_exhausted: Callable[[TaskiqMessage], Awaitable[None]] | None = None,
    ) -> None:
        super().__init__()
        _install_secret_safe_kicker_filter()
        self._work_queue = work_queue
        self._queue = _require_non_blank(queue, label="Queue")
        self._consumer = _require_non_blank(consumer, label="Consumer")
        self._policy = policy
        self._on_publication_rejected = on_publication_rejected
        self._on_delivery_exhausted = on_delivery_exhausted
        self._lifecycle = _BrokerLifecycle()
        self._lifecycle_lock = asyncio.Lock()
        self._outstanding_deliveries: dict[str, _OutstandingDelivery] = {}
        self._prefetch_renewals: dict[str, asyncio.Task[None]] = {}
        self._observed_dead_letters: OrderedDict[tuple[str, int], None] = OrderedDict()
        self._next_dead_letter_observation_at = 0.0
        self._dead_letter_observation_interval_seconds = min(
            _MAXIMUM_DEAD_LETTER_OBSERVATION_INTERVAL_SECONDS,
            max(
                _MINIMUM_DEAD_LETTER_OBSERVATION_INTERVAL_SECONDS,
                policy.wait_timeout_seconds,
            ),
        )
        self._publication_attempt: ContextVar[_PublicationAttempt | None] = ContextVar(
            "wybra_taskiq_publication_attempt",
            default=None,
        )

    async def startup(self) -> None:
        async with self._lifecycle_lock:
            self._lifecycle.shutdown_requested.set()
            lifecycle = _BrokerLifecycle()
            try:
                await super().startup()
            except BaseException:
                lifecycle.shutdown_requested.set()
                raise
            self._lifecycle = lifecycle

    @property
    def queue(self) -> str:
        """Return the only queue this broker instance can consume."""
        return self._queue

    async def shutdown(self) -> None:
        async with self._lifecycle_lock:
            self._lifecycle.shutdown_requested.set()
            await super().shutdown()

    async def kick(self, message: BrokerMessage) -> None:
        try:
            if len(message.message) > MAX_CACHE_FEATURE_PAYLOAD_BYTES:
                raise CacheWorkQueueRejectedError(
                    "Task message exceeds the configured cache payload limit."
                )
            publication = self._publication_attempt.get()
            if publication is not None and publication.task_id == message.task_id:
                publication.reached_broker = True
            await self._work_queue.publish(
                _TASKIQ_WORK_QUEUE_OWNER,
                self._queue,
                message.message,
                delay=_retry_delay(message),
                max_attempts=self._policy.maximum_delivery_attempts,
            )
        except CacheWorkQueueRejectedError:
            if self._on_publication_rejected is not None:
                try:
                    task_message = self.formatter.loads(message=message.message)
                    task_message.parse_labels()
                    await self._reconcile_rejected_publication(task_message)
                except Exception as error:
                    _LOGGER.warning(
                        "Task lifecycle could not immediately reconcile a rejected "
                        "submission: %s.",
                        type(error).__name__,
                    )
            raise

    @contextmanager
    def track_publication(self, task_id: str) -> Iterator[_PublicationAttempt]:
        """Track whether one submission reached this broker's queue hand-off."""

        attempt = _PublicationAttempt(task_id=task_id)
        token: Token[_PublicationAttempt | None] = self._publication_attempt.set(
            attempt
        )
        try:
            yield attempt
        finally:
            self._publication_attempt.reset(token)

    async def _reconcile_rejected_publication(self, message: TaskiqMessage) -> None:
        """Record a definitive rejection before returning it to the submitter."""

        assert self._on_publication_rejected is not None
        for attempt in range(_PUBLICATION_RECONCILIATION_MAX_ATTEMPTS):
            try:
                await self._on_publication_rejected(message)
                return
            except CacheConflictError, CacheFeatureError:
                if attempt + 1 == _PUBLICATION_RECONCILIATION_MAX_ATTEMPTS:
                    raise
                await asyncio.sleep(0.01 * (2**attempt))

    def listen(self) -> AsyncGenerator[bytes | AckableMessage]:
        return self._listen(self._lifecycle)

    async def _listen(
        self,
        lifecycle: _BrokerLifecycle,
    ) -> AsyncGenerator[AckableMessage]:
        shutdown = asyncio.create_task(lifecycle.shutdown_requested.wait())
        try:
            while (
                self._lifecycle is lifecycle
                and not lifecycle.shutdown_requested.is_set()
            ):
                await self._observe_delivery_exhaustion(force=False)
                delivery = await self._reserve_or_stop(shutdown)
                if (
                    delivery is not None
                    and self._lifecycle is lifecycle
                    and not lifecycle.shutdown_requested.is_set()
                ):
                    visibility_timeout = self._policy.visibility_timeout_seconds
                    receipt = uuid4().hex
                    self._outstanding_deliveries[receipt] = _OutstandingDelivery(
                        delivery=delivery,
                    )
                    if self.is_worker_process:
                        self._prefetch_renewals[receipt] = asyncio.create_task(
                            self._renew_prefetched_delivery(
                                receipt, visibility_timeout
                            ),
                            name="wybra-taskiq-prefetch-renewal",
                        )
                    yield AckableMessage(
                        data=self._delivery_payload(delivery, receipt),
                        ack=_DeliveryAcknowledgement(self, receipt),
                    )
        finally:
            await self._cancel_prefetch_renewals()
            await _cancel_task(shutdown)

    async def _observe_delivery_exhaustion(self, *, force: bool = True) -> None:
        if self._on_delivery_exhausted is None:
            return
        now = monotonic()
        if not force and now < self._next_dead_letter_observation_at:
            return
        if not force:
            self._next_dead_letter_observation_at = (
                now + self._dead_letter_observation_interval_seconds
            )
        try:
            deliveries = await self._work_queue.dead_letters(
                _TASKIQ_WORK_QUEUE_OWNER,
                self._queue,
                limit=MAX_CACHE_FEATURE_LIMIT,
            )
        except (CacheConflictError, CacheFeatureError) as error:
            _LOGGER.warning(
                "Cache work dead-letter observation could not read the queue; it "
                "will be retried: %s.",
                type(error).__name__,
            )
            return
        for delivery in deliveries:
            identity = (delivery.identity.value, delivery.attempt)
            if identity in self._observed_dead_letters:
                continue
            try:
                message = self.formatter.loads(message=delivery.payload)
                message.parse_labels()
            except Exception:
                _LOGGER.warning(
                    "Cache work dead-letter payload is not a valid Taskiq message."
                )
                self._remember_dead_letter(identity)
                continue
            message.labels[DELIVERY_ATTEMPT_LABEL] = delivery.attempt
            message.labels[DELIVERY_IDENTITY_LABEL] = delivery.identity.value
            try:
                await self._on_delivery_exhausted(message)
            except CacheConflictError:
                _LOGGER.warning(
                    "Cache work dead-letter lifecycle state is temporarily "
                    "contended; it will be retried."
                )
                continue
            except CacheFeatureError:
                _LOGGER.warning(
                    "Cache work dead-letter lifecycle observation failed; it will "
                    "be retried."
                )
                continue
            except TaskLifecycleError, ValueError:
                _LOGGER.warning("Cache work dead-letter lifecycle metadata is invalid.")
            except Exception as error:
                _LOGGER.warning(
                    "Cache work dead-letter lifecycle observation failed "
                    "unexpectedly: %s.",
                    type(error).__name__,
                )
                continue
            self._remember_dead_letter(identity)

    def _remember_dead_letter(self, identity: tuple[str, int]) -> None:
        self._observed_dead_letters[identity] = None
        self._observed_dead_letters.move_to_end(identity)
        if len(self._observed_dead_letters) > MAX_CACHE_FEATURE_LIMIT:
            self._observed_dead_letters.popitem(last=False)

    async def _reserve_or_stop(
        self,
        shutdown: asyncio.Task[bool],
    ) -> WorkDelivery | None:
        reservation = asyncio.create_task(
            self._work_queue.reserve(
                _TASKIQ_WORK_QUEUE_OWNER,
                self._queue,
                self._consumer,
                visibility_timeout=self._policy.visibility_timeout_seconds,
                wait_timeout=self._policy.wait_timeout_seconds,
            )
        )
        try:
            completed, _ = await asyncio.wait(
                (reservation, shutdown),
                return_when=asyncio.FIRST_COMPLETED,
            )
            if shutdown in completed:
                await _cancel_task(reservation)
                return None
            return reservation.result()
        except BaseException:
            await _cancel_task(reservation)
            raise

    async def _acknowledge(self, receipt: str) -> None:
        await self._stop_prefetch_renewal(receipt)
        delivery = self._delivery_for_receipt(receipt)
        try:
            await self._work_queue.acknowledge(delivery)
        except CacheConflictError:
            self._outstanding_deliveries.pop(receipt, None)
            raise
        self._outstanding_deliveries.pop(receipt, None)

    async def renew_delivery(
        self,
        receipt: str,
        *,
        visibility_timeout: float,
    ) -> None:
        """Renew one locally issued cache delivery by its opaque adapter receipt."""
        _validate_positive_finite(visibility_timeout, label="Visibility timeout")
        await self._stop_prefetch_renewal(receipt)
        outstanding = self._outstanding_for_receipt(receipt)
        try:
            renewed = await self._work_queue.renew(
                outstanding.delivery,
                visibility_timeout=visibility_timeout,
            )
        except CacheConflictError:
            self._outstanding_deliveries.pop(receipt, None)
            raise
        if self._outstanding_deliveries.get(receipt) is not outstanding:
            raise CacheConflictError("Delivery is stale or no longer reserved.")
        self._outstanding_deliveries[receipt] = _OutstandingDelivery(
            delivery=renewed,
        )

    def delivery_receipt(self, message: AckableMessage) -> str | None:
        """Return the broker-issued receipt bound to one acknowledgement message."""

        acknowledgement = message.ack
        if (
            isinstance(acknowledgement, _DeliveryAcknowledgement)
            and acknowledgement.broker is self
        ):
            return acknowledgement.receipt
        return None

    async def relinquish_delivery(self, receipt: str) -> None:
        """Stop prefetch renewal and allow an unacknowledged delivery to recover."""

        await self._stop_prefetch_renewal(receipt)

    def delivery_visibility_timeout(self, labels: dict[str, object]) -> float:
        value = labels.get(
            TASK_VISIBILITY_TIMEOUT_LABEL,
            self._policy.visibility_timeout_seconds,
        )
        if isinstance(value, bool) or not isinstance(value, str | int | float):
            raise ValueError("Task visibility timeout must be a positive number.")
        try:
            timeout = float(value)
        except (TypeError, ValueError, OverflowError) as error:
            raise ValueError(
                "Task visibility timeout must be a positive number."
            ) from error
        _validate_visibility_timeout(timeout)
        return max(self._policy.visibility_timeout_seconds, timeout)

    async def _renew_prefetched_delivery(
        self,
        receipt: str,
        visibility_timeout: float,
    ) -> None:
        try:
            while self._prefetch_renewals.get(receipt) is asyncio.current_task():
                await asyncio.sleep(max(0.05, visibility_timeout / 2))
                outstanding = self._outstanding_for_receipt(receipt)
                try:
                    renewed = await self._work_queue.renew(
                        outstanding.delivery,
                        visibility_timeout=visibility_timeout,
                    )
                except CacheConflictError:
                    self._outstanding_deliveries.pop(receipt, None)
                    raise
                if self._outstanding_deliveries.get(receipt) is outstanding:
                    self._outstanding_deliveries[receipt] = _OutstandingDelivery(
                        delivery=renewed,
                    )
        except CacheConflictError, CacheFeatureError:
            return
        finally:
            if self._prefetch_renewals.get(receipt) is asyncio.current_task():
                self._prefetch_renewals.pop(receipt, None)

    async def _stop_prefetch_renewal(self, receipt: str) -> None:
        renewal = self._prefetch_renewals.pop(receipt, None)
        if renewal is not None and renewal is not asyncio.current_task():
            await _cancel_task(renewal)

    async def _cancel_prefetch_renewals(self) -> None:
        renewals = tuple(self._prefetch_renewals.values())
        self._prefetch_renewals.clear()
        for renewal in renewals:
            renewal.cancel()
        if renewals:
            await asyncio.wait(renewals, timeout=_CANCELLATION_JOIN_TIMEOUT_SECONDS)

    def _delivery_payload(self, delivery: WorkDelivery, receipt: str) -> bytes:
        try:
            message = self.formatter.loads(message=delivery.payload)
        except Exception:
            _LOGGER.warning(
                "Taskiq work delivery cannot carry cache acknowledgement metadata; "
                "it will remain unacknowledged for visibility recovery."
            )
            return delivery.payload
        message.labels[DELIVERY_ATTEMPT_LABEL] = delivery.attempt
        message.labels[DELIVERY_IDENTITY_LABEL] = delivery.identity.value
        message.labels[DELIVERY_RECEIPT_LABEL] = receipt
        return self.formatter.dumps(message).message

    def _delivery_for_receipt(self, receipt: str) -> WorkDelivery:
        return self._outstanding_for_receipt(receipt).delivery

    def _outstanding_for_receipt(self, receipt: str) -> _OutstandingDelivery:
        outstanding = self._outstanding_deliveries.get(receipt)
        if outstanding is None:
            raise CacheConflictError("Delivery is stale or no longer reserved.")
        return outstanding


async def _cancel_task[T](task: asyncio.Task[T]) -> None:
    if not task.done():
        task.cancel()
    await asyncio.wait((task,), timeout=_CANCELLATION_JOIN_TIMEOUT_SECONDS)


@dataclass(frozen=True, slots=True)
class _DeliveryAcknowledgement:
    broker: CacheTaskiqBroker
    receipt: str

    async def __call__(self) -> None:
        await self.broker._acknowledge(self.receipt)


def _validate_positive_finite(value: object, *, label: str) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, int | float)
        or not isfinite(value)
        or value <= 0
    ):
        raise ValueError(f"{label} must be a positive finite number.")
    return float(value)


def _validate_visibility_timeout(value: object) -> None:
    timeout = _validate_positive_finite(value, label="Task visibility timeout")
    if timeout < MINIMUM_TASK_VISIBILITY_TIMEOUT_SECONDS:
        raise ValueError(
            "Task visibility timeout must be at least "
            f"{MINIMUM_TASK_VISIBILITY_TIMEOUT_SECONDS} seconds."
        )


def _require_non_blank(value: str, *, label: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{label} must be a non-blank string.")
    return value.strip()


def _retry_delay(message: BrokerMessage) -> float:
    value = message.labels.get("delay", 0)
    if isinstance(value, bool):
        raise ValueError("Taskiq retry delay must be a non-negative finite number.")
    try:
        delay = float(value)
    except TypeError, ValueError, OverflowError:
        delay = None
    if delay is None:
        raise ValueError("Taskiq retry delay must be a non-negative finite number.")
    if not isfinite(delay) or delay < 0:
        raise ValueError("Taskiq retry delay must be a non-negative finite number.")
    return delay


__all__ = (
    "CacheTaskiqBroker",
    "TaskiqBrokerPolicy",
)
