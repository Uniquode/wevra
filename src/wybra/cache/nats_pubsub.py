from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from hashlib import sha256
from typing import Any

from wybra.cache.feature_models import (
    CacheFeatureError,
    validate_payload,
    validate_positive_finite,
    validate_resource,
)
from wybra.cache.lifecycle import close_all, raise_cleanup_errors
from wybra.cache.nats_runtime import NatsJetStreamRuntime

logger = logging.getLogger(__name__)

type SubscriptionCloser = Callable[[], Awaitable[None]]


class _SubscriptionClosed(CacheFeatureError):
    pass


@dataclass(slots=True, eq=False)
class NatsPubSubSubscription:
    _runtime: NatsJetStreamRuntime = field(repr=False)
    _handle: Any = field(repr=False)
    _subject: str = field(repr=False)
    _close_callback: SubscriptionCloser = field(repr=False)
    _closed: bool = field(default=False, init=False)
    _released: bool = field(default=False, init=False)
    _close_event: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    _read_idle: asyncio.Event = field(default_factory=asyncio.Event, init=False)
    _close_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
        repr=False,
    )
    _receive_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
        repr=False,
    )

    def __post_init__(self) -> None:
        self._read_idle.set()

    def __aiter__(self) -> AsyncIterator[bytes]:
        return self

    async def __anext__(self) -> bytes:
        try:
            return await self.receive()
        except _SubscriptionClosed as exc:
            raise StopAsyncIteration from exc

    async def receive(self, *, timeout: float | None = None) -> bytes:
        if timeout is not None:
            timeout = validate_positive_finite(timeout, label="subscription timeout")
        deadline = (
            None if timeout is None else asyncio.get_running_loop().time() + timeout
        )
        await self._acquire_receive_lock(deadline)
        try:
            self._require_open()
            message = await self._read_or_close(_remaining_timeout(deadline))
            data = getattr(message, "data", None)
            if not isinstance(data, bytes):
                raise CacheFeatureError("NATS pub/sub receive returned invalid state.")
            return validate_payload(data)
        finally:
            self._receive_lock.release()

    async def close(self) -> None:
        async with self._close_lock:
            if self._released:
                return
            self._closed = True
            self._close_event.set()
            await self._read_idle.wait()
            await self._close_callback()
            self._released = True

    async def _acquire_receive_lock(self, deadline: float | None) -> None:
        if deadline is None:
            await self._receive_lock.acquire()
            return
        remaining = _remaining_timeout(deadline)
        assert remaining is not None
        if remaining <= 0:
            raise TimeoutError("NATS pub/sub receive timed out.")
        try:
            await asyncio.wait_for(self._receive_lock.acquire(), timeout=remaining)
        except TimeoutError:
            raise TimeoutError("NATS pub/sub receive timed out.") from None

    async def _read_or_close(self, timeout: float | None) -> Any:
        self._read_idle.clear()
        read = asyncio.create_task(self._handle.next_msg(timeout=timeout))
        closed = asyncio.create_task(self._close_event.wait())
        try:
            done, _pending = await asyncio.wait(
                (read, closed),
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if read in done or read.done():
                try:
                    return await read
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    if self._runtime.is_timeout_error(exc):
                        raise TimeoutError("NATS pub/sub receive timed out.") from None
                    logger.warning(
                        "NATS pub/sub receive failed (%s).", type(exc).__name__
                    )
                    raise CacheFeatureError("NATS pub/sub receive failed.") from None
            read.cancel()
            await asyncio.gather(read, return_exceptions=True)
            if closed in done or closed.done():
                raise _SubscriptionClosed("The pub/sub subscription is closed.")
            raise TimeoutError("NATS pub/sub receive timed out.")
        finally:
            for task in (read, closed):
                if not task.done():
                    task.cancel()
            try:
                await asyncio.gather(read, closed, return_exceptions=True)
            finally:
                self._read_idle.set()

    def _require_open(self) -> None:
        if self._closed:
            raise _SubscriptionClosed("The pub/sub subscription is closed.")


@dataclass(slots=True)
class NatsPubSubCache:
    runtime: NatsJetStreamRuntime
    _subscriptions: set[NatsPubSubSubscription] = field(
        default_factory=set,
        init=False,
        repr=False,
    )
    _closed: bool = field(default=False, init=False, repr=False)
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock, init=False, repr=False)
    _close_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
        repr=False,
    )

    async def publish(self, owner: str, topic: str, payload: bytes) -> None:
        async with self._lock:
            self._require_open()
        subject = _subject(self.runtime, owner, topic)
        payload = validate_payload(payload)
        client = await self.runtime.nats_client()
        try:
            await client.publish(subject, payload)
            await client.flush()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("NATS pub/sub publish failed (%s).", type(exc).__name__)
            raise CacheFeatureError("NATS pub/sub publish failed.") from None

    async def subscribe(self, owner: str, topic: str) -> NatsPubSubSubscription:
        subject = _subject(self.runtime, owner, topic)
        async with self._lock:
            self._require_open()
        client = await self.runtime.nats_client()
        try:
            handle = await client.subscribe(subject)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.warning("NATS pub/sub subscribe failed (%s).", type(exc).__name__)
            raise CacheFeatureError("NATS pub/sub subscribe failed.") from None

        subscription: NatsPubSubSubscription

        async def remove() -> None:
            try:
                await handle.unsubscribe()
            finally:
                async with self._lock:
                    self._subscriptions.discard(subscription)

        subscription = NatsPubSubSubscription(
            self.runtime,
            handle,
            subject,
            remove,
        )
        async with self._lock:
            closed = self._closed
            if not closed:
                self._subscriptions.add(subscription)
        if closed:
            await remove()
            raise CacheFeatureError("The NATS pub/sub cache is closed.")
        try:
            await client.flush()
        except asyncio.CancelledError as error:
            await _remove_after_failed_activation(remove, error)
            raise
        except Exception as exc:
            await _remove_after_failed_activation(remove, exc)
            logger.warning("NATS pub/sub subscribe failed (%s).", type(exc).__name__)
            raise CacheFeatureError("NATS pub/sub subscribe failed.") from None
        return subscription

    async def close(self) -> None:
        async with self._close_lock:
            async with self._lock:
                self._closed = True
                subscriptions = tuple(self._subscriptions)
            errors = await close_all(
                subscription.close for subscription in subscriptions
            )
            raise_cleanup_errors("NATS pub/sub subscription cleanup failed.", errors)

    def _require_open(self) -> None:
        if self._closed:
            raise CacheFeatureError("The NATS pub/sub cache is closed.")


def _remaining_timeout(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return max(0, deadline - asyncio.get_running_loop().time())


def _subject(runtime: NatsJetStreamRuntime, owner: str, topic: str) -> str:
    owner = validate_resource(owner, label="cache owner")
    topic = validate_resource(topic, label="topic")
    digest = sha256(f"{owner}\x00{topic}".encode()).hexdigest()
    return f"{runtime.subject_prefix}.pubsub.{digest}"


async def _remove_after_failed_activation(
    remove: SubscriptionCloser,
    error: BaseException,
) -> None:
    try:
        await remove()
    except asyncio.CancelledError:
        if isinstance(error, asyncio.CancelledError):
            return
        raise
    except Exception as cleanup_error:
        error.add_note(
            f"NATS pub/sub activation cleanup failed ({type(cleanup_error).__name__})."
        )
        logger.warning(
            "NATS pub/sub activation cleanup failed (%s).",
            type(cleanup_error).__name__,
        )


__all__ = ("NatsPubSubCache", "NatsPubSubSubscription")
