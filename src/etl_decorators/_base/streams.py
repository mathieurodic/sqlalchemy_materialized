"""Internal helpers for decorators operating on (async) iterables.

This module is **internal** (under :mod:`etl_decorators._base`).

Several decorators are designed to work on streams returned by user functions
(``Iterable`` / ``AsyncIterable``). They all need the same small runtime
classification:

- detect whether an object is an ``AsyncIterable`` (has ``__aiter__``)
- detect whether an object is an ``Iterable`` (has ``__iter__``)
- raise consistent, helpful error messages when the payload is not iterable

Keeping this logic here reduces boilerplate in individual streaming
decorators.
"""

from __future__ import annotations

from collections.abc import AsyncIterable, Iterable
from typing import Any, TypeGuard


def _is_async_iterable(obj: object) -> TypeGuard[AsyncIterable[Any]]:
    # Prefer attribute check to `isinstance(..., AsyncIterable)` because:
    # - many async generator objects satisfy the protocol but may not register
    #   with `collections.abc.AsyncIterable` in all environments.
    return hasattr(obj, "__aiter__")


def _is_iterable(obj: object) -> TypeGuard[Iterable[Any]]:
    # Do not treat strings/bytes specially here; the streaming decorators are
    # explicitly about iterators/streams.
    return hasattr(obj, "__iter__")


def _require_stream(obj: Any, *, fn_name: str | None = None) -> Iterable[Any] | AsyncIterable[Any]:
    """Return *obj* if it is an iterable or async iterable.

    Raises:
        TypeError: if the object is neither ``Iterable`` nor ``AsyncIterable``.
    """

    if _is_async_iterable(obj):
        return obj
    if _is_iterable(obj):
        return obj

    prefix = f"{fn_name}(): " if fn_name else ""
    raise TypeError(
        prefix
        + "expected an Iterable or AsyncIterable return value, "
        + f"got {type(obj).__name__}"
    )
