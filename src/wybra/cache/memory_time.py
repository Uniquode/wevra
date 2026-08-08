from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import dataclass, field

from wybra.cache.feature_models import validate_finite

type Clock = Callable[[], float]


@dataclass(frozen=True, slots=True)
class InMemoryCacheTime:
    """Deterministic process-local cache time."""

    clock: Clock = field(default=time.time, repr=False)

    async def refresh(self) -> float:
        return self.now()

    def now(self) -> float:
        return validate_finite(self.clock(), label="cache time")


__all__ = ("InMemoryCacheTime",)
