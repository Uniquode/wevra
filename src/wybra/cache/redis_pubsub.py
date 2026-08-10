from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from typing import Any

from wybra.cache.feature_models import (
    CacheFeatureError,
    validate_payload,
    validate_positive_finite,
    validate_resource,
)
from wybra.cache.lifecycle import close_all, raise_cleanup_errors
from wybra.cache.redis_runtime import RedisCacheRuntime

type SubscriptionCloser = Callable[[], Awaitable[None]]


class _SubscriptionClosed(CacheFeatureError):
    pass


@dataclass(slots=True, eq=False)
class RedisPubSubSubscription:
    runtime: RedisCacheRuntime = field(repr=False)
    handle: Any = field(repr=False)
    channel: str = field(repr=False)
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
            while True:
                message_timeout = _remaining_timeout(deadline)
                message = await self._read_or_close(message_timeout)
                if message is not None:
                    return _message_payload(message, self.channel)
                if deadline is not None:
                    remaining = _remaining_timeout(deadline)
                    assert remaining is not None
                    if remaining <= 0:
                        raise TimeoutError("Redis pub/sub receive timed out.")
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
            raise TimeoutError("Redis pub/sub receive timed out.")
        try:
            await asyncio.wait_for(self._receive_lock.acquire(), timeout=remaining)
        except TimeoutError:
            raise TimeoutError("Redis pub/sub receive timed out.") from None

    async def _read_or_close(self, timeout: float | None) -> object:
        self._read_idle.clear()
        read = asyncio.create_task(
            self.runtime.subscription_call(
                lambda: self.handle.get_message(
                    ignore_subscribe_messages=True,
                    timeout=timeout,
                )
            )
        )
        closed = asyncio.create_task(self._close_event.wait())
        try:
            done, _pending = await asyncio.wait(
                (read, closed),
                timeout=timeout,
                return_when=asyncio.FIRST_COMPLETED,
            )
            if read in done or read.done():
                return await read
            read.cancel()
            await asyncio.gather(read, return_exceptions=True)
            if closed in done or closed.done():
                raise _SubscriptionClosed("The pub/sub subscription is closed.")
            raise TimeoutError("Redis pub/sub receive timed out.")
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
class RedisPubSubCache:
    runtime: RedisCacheRuntime
    _subscriptions: set[RedisPubSubSubscription] = field(
        default_factory=set,
        init=False,
        repr=False,
    )
    _activations: set[asyncio.Task[RedisPubSubSubscription]] = field(
        default_factory=set,
        init=False,
        repr=False,
    )
    _pending_handles: dict[int, Any] = field(
        default_factory=dict,
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
        self._require_open()
        channel = _channel(self.runtime, owner, topic)
        payload = validate_payload(payload)

        async def publish_message(client: Any) -> object:
            return await client.publish(channel, payload)

        await self.runtime.feature_call(publish_message)

    async def subscribe(
        self,
        owner: str,
        topic: str,
    ) -> RedisPubSubSubscription:
        channel = _channel(self.runtime, owner, topic)
        async with self._lock:
            self._require_open()
            activation = asyncio.create_task(
                self._activate_subscription(channel),
                name="wybra-cache-pubsub-subscribe",
            )
            self._activations.add(activation)
        try:
            return await asyncio.shield(activation)
        except asyncio.CancelledError as cancellation:
            current = asyncio.current_task()
            caller_cancelled = current is not None and current.cancelling() > 0
            if caller_cancelled:
                activation.cancel()
                result = await asyncio.gather(activation, return_exceptions=True)
                if result and isinstance(result[0], RedisPubSubSubscription):
                    try:
                        await result[0].close()
                    except BaseException as cleanup_error:
                        cancellation.add_note(
                            "Redis pub/sub subscription cleanup after caller "
                            f"cancellation failed ({type(cleanup_error).__name__})."
                        )
                raise cancellation
            if self._closed:
                raise CacheFeatureError("The Redis pub/sub cache is closed.") from None
            raise
        finally:
            async with self._lock:
                self._activations.discard(activation)

    async def close(self) -> None:
        async with self._close_lock:
            cancellation: asyncio.CancelledError | None = None
            async with self._lock:
                self._closed = True
                activations = tuple(self._activations)
            for activation in activations:
                activation.cancel()
            if activations:
                try:
                    await asyncio.gather(*activations, return_exceptions=True)
                except asyncio.CancelledError as exc:
                    cancellation = exc
                    await asyncio.gather(*activations, return_exceptions=True)

            async with self._lock:
                subscriptions = tuple(self._subscriptions)
                pending_handles = tuple(self._pending_handles.values())
            try:
                errors = await close_all(
                    (
                        *(subscription.close for subscription in subscriptions),
                        *(
                            lambda handle=handle: self._release_pending_handle(handle)
                            for handle in pending_handles
                        ),
                    )
                )
            except asyncio.CancelledError as exc:
                cancellation = cancellation or exc
                errors = []
            if cancellation is not None:
                if errors:
                    cancellation.add_note(
                        f"{len(errors)} Redis pub/sub cleanup error(s) also occurred."
                    )
                raise cancellation
            raise_cleanup_errors("Redis pub/sub subscription cleanup failed.", errors)

    async def _activate_subscription(
        self,
        channel: str,
    ) -> RedisPubSubSubscription:
        handle: Any = None
        try:
            handle = await self.runtime.open_pubsub()
            async with self._lock:
                self._pending_handles[id(handle)] = handle
                self._require_open()
            await self.runtime.subscribe_pubsub(handle, channel)

            subscription: RedisPubSubSubscription

            async def close_subscription() -> None:
                await self.runtime.subscription_call(handle.aclose)
                async with self._lock:
                    self._subscriptions.discard(subscription)

            subscription = RedisPubSubSubscription(
                self.runtime,
                handle,
                channel,
                close_subscription,
            )
            async with self._lock:
                self._require_open()
                self._pending_handles.pop(id(handle), None)
                self._subscriptions.add(subscription)
            return subscription
        except BaseException as activation_error:
            if handle is not None:
                async with self._lock:
                    self._pending_handles.setdefault(id(handle), handle)
                try:
                    await self._release_pending_handle(handle)
                except BaseException as cleanup_error:
                    _annotate_cleanup_error(activation_error, cleanup_error)
                    if isinstance(
                        cleanup_error, asyncio.CancelledError
                    ) and not isinstance(activation_error, asyncio.CancelledError):
                        raise cleanup_error from activation_error
            raise
        finally:
            current = asyncio.current_task()
            if current is not None:
                async with self._lock:
                    self._activations.discard(current)

    async def _release_pending_handle(self, handle: Any) -> None:
        await self.runtime.subscription_call(handle.aclose)
        async with self._lock:
            if self._pending_handles.get(id(handle)) is handle:
                self._pending_handles.pop(id(handle), None)

    def _require_open(self) -> None:
        if self._closed:
            raise CacheFeatureError("The Redis pub/sub cache is closed.")


def _remaining_timeout(deadline: float | None) -> float | None:
    if deadline is None:
        return None
    return max(0.0, deadline - asyncio.get_running_loop().time())


def _annotate_cleanup_error(
    operation_error: BaseException,
    cleanup_error: BaseException,
) -> None:
    operation_error.add_note(
        "Redis pub/sub activation cleanup also failed "
        f"({type(cleanup_error).__name__}); cache shutdown will retry."
    )


def _channel(runtime: RedisCacheRuntime, owner: str, topic: str) -> str:
    return runtime.key(
        "pub-sub",
        validate_resource(owner, label="cache owner"),
        validate_resource(topic, label="topic"),
    )


def _message_payload(value: object, channel: str) -> bytes:
    if not isinstance(value, Mapping):
        raise CacheFeatureError("Redis pub/sub receive returned invalid state.")
    message_type = value.get("type")
    received_channel = value.get("channel")
    payload = value.get("data")
    if (
        message_type not in ("message", b"message")
        or received_channel not in (channel, channel.encode())
        or not isinstance(payload, bytes)
    ):
        raise CacheFeatureError("Redis pub/sub receive returned invalid state.")
    try:
        return validate_payload(payload)
    except (TypeError, ValueError) as exc:
        raise CacheFeatureError(
            "Redis pub/sub receive returned invalid state."
        ) from exc


__all__ = ("RedisPubSubCache", "RedisPubSubSubscription")
