"""Internal helpers to implement decorators consistently.

This project exposes multiple independent decorators, but many share the same
mechanics:

- Support both ``@decorator`` and ``@decorator(...)`` calling conventions
- Support decorating both sync and async functions
- Preserve wrapped function metadata (``__name__``, ``__doc__``, etc.)

This module provides small base classes to reduce boilerplate in individual
decorator implementations.

It is intentionally **internal** (leading underscore) and not part of the
public API.
"""

from __future__ import annotations

import inspect
from abc import ABC
from functools import update_wrapper
from typing import Any, Callable, Generic, ParamSpec, TypeVar


P = ParamSpec("P")
R = TypeVar("R")
S = TypeVar("S")


class DecoratorCommonBase(ABC):
    """Common ancestor for internal decorator helper classes."""


class DecoratorBase(DecoratorCommonBase, Generic[P, R, S]):
    """Base class for decorators that post-process a function result.

    Subclasses implement :meth:`process_result` and (optionally)
    :meth:`process_exception`.

    Note: this class does *not* implement ``__call__`` on purpose.
    Public decorators in this project are functions, and should stay functions.
    """

    def decorate(self, fn: Callable[P, Any]) -> Callable[P, R]:
        is_async = inspect.iscoroutinefunction(fn)

        if is_async:

            async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[misc]
                state = await self.before_call_async(fn, args, kwargs)
                try:
                    result = await fn(*args, **kwargs)
                except Exception as e:
                    maybe = await self.process_exception_async(fn, e, args, kwargs, state)
                    if maybe is _RAISE:
                        raise
                    return maybe  # type: ignore[return-value]
                return await self.process_result_async(fn, result, args, kwargs, state)

        else:

            def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[misc]
                state = self.before_call(fn, args, kwargs)
                try:
                    result = fn(*args, **kwargs)
                except Exception as e:
                    maybe = self.process_exception(fn, e, args, kwargs, state)
                    if maybe is _RAISE:
                        raise
                    return maybe  # type: ignore[return-value]
                return self.process_result(fn, result, args, kwargs, state)

        update_wrapper(wrapped, fn)
        return wrapped

    def process_result(
        self,
        fn: Callable[P, Any],
        result: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        state: S,
    ) -> R:
        raise NotImplementedError

    async def process_result_async(
        self,
        fn: Callable[P, Any],
        result: Any,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        state: S,
    ) -> R:
        # Most decorators do the same processing in sync and async cases.
        return self.process_result(fn, result, args, kwargs, state)

    def before_call(
        self,
        fn: Callable[P, Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> S:
        return None  # type: ignore[return-value]

    async def before_call_async(
        self,
        fn: Callable[P, Any],
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
    ) -> S:
        return self.before_call(fn, args, kwargs)

    def process_exception(
        self,
        fn: Callable[P, Any],
        exc: Exception,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        state: S,
    ) -> Any:
        return _RAISE

    async def process_exception_async(
        self,
        fn: Callable[P, Any],
        exc: Exception,
        args: tuple[Any, ...],
        kwargs: dict[str, Any],
        state: S,
    ) -> Any:
        return self.process_exception(fn, exc, args, kwargs, state)


class _Raise:
    pass


_RAISE = _Raise()


Fn = TypeVar("Fn", bound=Callable[..., Any])


class OptionalFnDecoratorBase(DecoratorCommonBase):
    """Base helper for decorators supporting ``fn=None``.

    Many decorators in this repo implement the same pattern:

    .. code-block:: python

        def deco(fn=None, *, option=...):
            def _decorate(f):
                ...
            if fn is None:
                return _decorate
            return _decorate(fn)

    This helper standardizes that logic.
    """

    def bind_optional(self, fn: Fn | None, decorator: Callable[[Fn], Any]):
        if fn is None:
            return decorator
        return decorator(fn)
