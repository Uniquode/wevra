from __future__ import annotations

from asyncio import CancelledError
from collections.abc import Awaitable, Callable, Iterable

type CacheCloser = Callable[[], Awaitable[None]]


async def close_all(closers: Iterable[CacheCloser]) -> list[Exception]:
    """Run every closer, preserving cancellation after cleanup completes."""
    errors: list[Exception] = []
    cancellation: CancelledError | None = None
    for close in closers:
        try:
            await close()
        except CancelledError as exc:
            cancellation = cancellation or exc
        except Exception as exc:
            errors.append(exc)
    if cancellation is not None:
        if errors:
            cancellation.add_note(
                f"{len(errors)} cache cleanup error(s) also occurred."
            )
        raise cancellation
    return errors


def raise_cleanup_errors(message: str, errors: list[Exception]) -> None:
    """Raise collected cleanup errors without hiding an individual cause."""
    if errors:
        raise ExceptionGroup(message, errors)


__all__ = ("CacheCloser", "close_all", "raise_cleanup_errors")
