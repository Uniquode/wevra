from __future__ import annotations

from dataclasses import dataclass

from wybra.cache.redis_runtime import RedisCacheRuntime


@dataclass(frozen=True, slots=True)
class RedisCacheTime:
    """Provider-calibrated time backed by one Redis runtime."""

    runtime: RedisCacheRuntime

    async def refresh(self) -> float:
        return await self.runtime.refresh_time()

    def now(self) -> float:
        return self.runtime.cache_time()


__all__ = ("RedisCacheTime",)
