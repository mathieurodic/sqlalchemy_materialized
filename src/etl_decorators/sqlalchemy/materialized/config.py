from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class _MaterializedConfig:
    in_transaction: bool = True
    depends_on: tuple[str, ...] = ()
    validate: bool = True
    retry_on: (
        type[Exception]
        | tuple[type[Exception], ...]
        | Callable[[Exception], bool]
    ) = ()
    retry_max: int = 3
    retry_factor: float = 2.0
    retry_interval: float = 1.0
