"""Timeout decorator.

This module provides :func:`etl_decorators.resilience.timeout`.

- Async functions are timed out using :func:`asyncio.wait_for`.
- Sync functions are timed out using a background thread.

Limitations (sync):
    When a timeout occurs, the underlying function keeps running in the
    background thread (it cannot be forcefully stopped in pure Python).
"""

from __future__ import annotations

import asyncio
import inspect
import threading
from functools import update_wrapper
from typing import Any, Callable, ParamSpec, TypeVar, overload

from etl_decorators._base.decorators import OptionalFnDecoratorBase


P = ParamSpec("P")
R = TypeVar("R")


def _raise_timeout(
    *,
    timeout_error: type[Exception],
    message: str | None,
) -> None:
    msg = message or "Timed out"
    raise timeout_error(msg)


@overload
def timeout(fn: Callable[P, R]) -> Callable[P, R]: ...


@overload
def timeout(
    fn: None = None,
    *,
    seconds: float,
    timeout_error: type[Exception] = TimeoutError,
    message: str | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def timeout(
    fn: Callable[P, R] | None = None,
    *,
    seconds: float,
    timeout_error: type[Exception] = TimeoutError,
    message: str | None = None,
):
    """Enforce a maximum execution time.

    Parameters:
        seconds: Timeout in seconds. Must be > 0.
        timeout_error: Exception class to raise on timeout.
        message: Optional message for the timeout exception.
    """

    if seconds <= 0:
        raise ValueError("timeout(seconds=...): seconds must be > 0")
    if not (isinstance(timeout_error, type) and issubclass(timeout_error, Exception)):
        raise TypeError("timeout(timeout_error=...): must be an Exception subclass")

    binder = OptionalFnDecoratorBase()

    def _decorate(f: Callable[P, R]) -> Callable[P, R]:
        is_async = inspect.iscoroutinefunction(f)

        if is_async:

            async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[misc]
                try:
                    return await asyncio.wait_for(
                        f(*args, **kwargs),  # type: ignore[misc]
                        timeout=seconds,
                    )
                except asyncio.TimeoutError:
                    _raise_timeout(timeout_error=timeout_error, message=message)

        else:

            def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[misc]
                result_box: dict[str, Any] = {}
                exc_box: dict[str, BaseException] = {}

                def runner() -> None:
                    try:
                        result_box["result"] = f(*args, **kwargs)
                    except BaseException as e:
                        exc_box["exc"] = e

                t = threading.Thread(target=runner, daemon=True)
                t.start()
                t.join(timeout=seconds)
                if t.is_alive():
                    _raise_timeout(timeout_error=timeout_error, message=message)

                if "exc" in exc_box:
                    raise exc_box["exc"]
                return result_box["result"]  # type: ignore[no-any-return]

        update_wrapper(wrapped, f)
        return wrapped

    return binder.bind_optional(fn, _decorate)
