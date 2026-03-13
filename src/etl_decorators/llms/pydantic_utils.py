"""Pydantic interoperability helpers.

This subpackage has an optional dependency on `pydantic`.

At import time we try to import :class:`pydantic.BaseModel`. If it is not
available we fall back to `object` so type-checking still works and we can
raise a nice runtime error when users attempt to use structured output.
"""

from __future__ import annotations


try:  # pragma: no cover (import is exercised in integration, tests monkeypatch)
    from pydantic import BaseModel
except Exception:  # pragma: no cover
    BaseModel = object  # type: ignore[assignment]


__all__ = ["BaseModel"]
