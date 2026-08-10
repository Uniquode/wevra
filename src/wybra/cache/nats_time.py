from __future__ import annotations

from dataclasses import dataclass

from wybra.cache.nats_runtime import NatsJetStreamRuntime


@dataclass(frozen=True, slots=True)
class NatsCacheTime:
    """Provider-calibrated time backed by one JetStream runtime."""

    runtime: NatsJetStreamRuntime

    async def refresh(self) -> float:
        return await self.runtime.refresh_time()

    def now(self) -> float:
        return self.runtime.cache_time()


__all__ = ("NatsCacheTime",)
