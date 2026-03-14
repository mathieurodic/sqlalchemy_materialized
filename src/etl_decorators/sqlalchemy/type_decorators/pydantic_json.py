"""Backward-compatible entrypoint for Pydantic JSON SQLAlchemy types.

Historically this module contained the full implementation. It is now split
into smaller modules:

- :mod:`._pydantic_json_core` (mutable container + model instrumentation)
- :mod:`.pydantic_json_types` (TypeDecorators)
- :mod:`._pydantic_json_tracking` (SQLAlchemy event listeners / dirty tracking)

Importing this module registers the tracking listeners (idempotently).
"""

from __future__ import annotations

from ._pydantic_json_tracking import setup_pydantic_json_tracking
from .pydantic_json_types import PydanticJSON, PydanticJSONList


setup_pydantic_json_tracking()


__all__ = [
    "PydanticJSON",
    "PydanticJSONList",
]
