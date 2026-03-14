from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable


@dataclass(frozen=True)
class _MaterializedConfig:
    in_transaction: bool = True
    depends_on: tuple[str, ...] = ()
    validate: bool = True

