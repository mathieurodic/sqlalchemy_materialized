"""Retry decorator.

This module provides :func:`etl_decorators.resilience.retry`, a small
general-purpose retry decorator with exponential backoff.

Design goals:

- Works for both sync and async callables.
- Explicit, typed configuration.
- Minimal side effects (it only sleeps between attempts).
"""

from __future__ import annotations

import asyncio
import inspect
import random
import time
import logging
from functools import update_wrapper
from typing import Any, Callable, ParamSpec, TypeVar, overload

from etl_decorators._base.decorators import OptionalFnDecoratorBase


logger = logging.getLogger(__name__)

P = ParamSpec("P")
R = TypeVar("R")

RetryOn = type[BaseException] | tuple[type[BaseException], ...]
RetryIf = Callable[[BaseException], bool]
OnRetryHook = Callable[[BaseException, int, float], Any]


def _is_fatal_exception(exc: BaseException) -> bool:
    """Return True if *exc* should never be retried."""

    # BaseException subclasses used for control flow / cancellation.
    if isinstance(exc, (KeyboardInterrupt, SystemExit, GeneratorExit)):
        return True
    # Async cancellation should not be swallowed.
    try:
        if isinstance(exc, asyncio.CancelledError):
            return True
    except Exception:  # pragma: no cover
        # Extremely defensive: if asyncio.CancelledError isn't available for
        # some reason, don't fail classification.
        pass
    return False


def _validate_config(
    *,
    retry_on: RetryOn | None,
    retry_if: RetryIf | None,
    max_attempts: int,
    interval: float,
    factor: float,
    jitter: float,
) -> None:
    if retry_on is None and retry_if is None:
        raise ValueError(
            "retry(...): you must provide at least one of retry_on=... or retry_if=..."
        )

    if retry_on is not None:
        if isinstance(retry_on, type):
            if not issubclass(retry_on, BaseException):
                raise TypeError("retry(retry_on=...): must be an exception class")
        elif isinstance(retry_on, tuple):
            if not retry_on:
                raise TypeError(
                    "retry(retry_on=...): tuple must contain at least one exception class"
                )
            for t in retry_on:
                if not (isinstance(t, type) and issubclass(t, BaseException)):
                    raise TypeError(
                        "retry(retry_on=...): tuple items must be exception classes"
                    )
        else:
            raise TypeError(
                "retry(retry_on=...): must be an exception class or a tuple of exception classes"
            )

    if retry_if is not None and not callable(retry_if):
        raise TypeError("retry(retry_if=...): must be callable")

    if max_attempts < 1:
        raise ValueError("retry(max_attempts=...): must be >= 1")
    if interval < 0:
        raise ValueError("retry(interval=...): must be >= 0")
    if factor < 1:
        raise ValueError("retry(factor=...): must be >= 1")
    if jitter < 0:
        raise ValueError("retry(jitter=...): must be >= 0")


def _should_retry(
    exc: BaseException,
    *,
    retry_on: RetryOn | None,
    retry_if: RetryIf | None,
) -> bool:
    if _is_fatal_exception(exc):
        return False

    if retry_if is not None:
        return bool(retry_if(exc))

    if retry_on is None:
        return False

    if isinstance(retry_on, type):
        return isinstance(exc, retry_on)
    return isinstance(exc, retry_on)


def _compute_delay(
    attempt: int,
    *,
    interval: float,
    factor: float,
    jitter: float,
) -> float:
    """Compute exponential backoff delay for the *next* sleep.

    attempt is 1-indexed.
    """

    delay = interval * (factor ** (attempt - 1))
    if delay <= 0:
        return 0.0

    if jitter > 0:
        # Multiplicative jitter (fraction of delay).
        lo = max(0.0, 1.0 - jitter)
        hi = 1.0 + jitter
        delay *= random.uniform(lo, hi)
    return delay


@overload
def retry(fn: Callable[P, R]) -> Callable[P, R]: ...


@overload
def retry(
    fn: None = None,
    *,
    retry_on: RetryOn | None = None,
    retry_if: RetryIf | None = None,
    max_attempts: int = 3,
    interval: float = 1.0,
    factor: float = 2.0,
    jitter: float = 0.0,
    on_retry: OnRetryHook | None = None,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def retry(
    fn: Callable[P, R] | None = None,
    *,
    retry_on: RetryOn | None = None,
    retry_if: RetryIf | None = None,
    max_attempts: int = 3,
    interval: float = 1.0,
    factor: float = 2.0,
    jitter: float = 0.0,
    on_retry: OnRetryHook | None = None,
):
    """Retry the decorated callable on transient failures.

    Parameters:
        retry_on: Exception class or tuple of exception classes to retry on.
        retry_if: Predicate deciding whether to retry for a caught exception.
            If provided, it takes precedence over ``retry_on``.
        max_attempts: Total number of attempts (1 means no retries).
        interval: Initial backoff delay in seconds.
        factor: Exponential backoff multiplier (>= 1).
        jitter: Optional multiplicative jitter factor. If ``jitter=0.1``, delay
            is multiplied by a random value in ``[0.9, 1.1]``.
        on_retry: Optional hook called before sleeping:
            ``on_retry(exc, attempt, sleep_seconds)``.

    Notes:
        - Works for both sync and async callables.
        - Never retries ``KeyboardInterrupt``, ``SystemExit``, ``GeneratorExit``
          nor ``asyncio.CancelledError``.
    """

    _validate_config(
        retry_on=retry_on,
        retry_if=retry_if,
        max_attempts=max_attempts,
        interval=interval,
        factor=factor,
        jitter=jitter,
    )

    binder = OptionalFnDecoratorBase()

    def _decorate(f: Callable[P, R]) -> Callable[P, R]:
        is_async = inspect.iscoroutinefunction(f)

        if is_async:

            async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[misc]
                last_exc: BaseException | None = None
                for attempt in range(1, max_attempts + 1):
                    try:
                        return await f(*args, **kwargs)  # type: ignore[misc]
                    except BaseException as e:
                        last_exc = e
                        if attempt >= max_attempts or not _should_retry(
                            e, retry_on=retry_on, retry_if=retry_if
                        ):
                            raise

                        delay = _compute_delay(
                            attempt,
                            interval=interval,
                            factor=factor,
                            jitter=jitter,
                        )
                        logger.warning("Retry in %ss after error: %s, %s", delay, e.__class__.__name__, str(e))

                        if on_retry is not None:
                            on_retry(e, attempt, delay)
                        if delay > 0:
                            await asyncio.sleep(delay)

                # Unreachable, but helps type-checkers.
                assert last_exc is not None  # pragma: no cover
                raise last_exc  # pragma: no cover

        else:

            def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[misc]
                last_exc: BaseException | None = None
                for attempt in range(1, max_attempts + 1):
                    try:
                        return f(*args, **kwargs)
                    except BaseException as e:
                        last_exc = e
                        if attempt >= max_attempts or not _should_retry(
                            e, retry_on=retry_on, retry_if=retry_if
                        ):
                            raise

                        delay = _compute_delay(
                            attempt,
                            interval=interval,
                            factor=factor,
                            jitter=jitter,
                        )
                        logger.warning("Retry in %ss after error: %s, %s", delay, e.__class__.__name__, str(e))
                        
                        if on_retry is not None:
                            on_retry(e, attempt, delay)
                        if delay > 0:
                            time.sleep(delay)

                # Unreachable, but helps type-checkers.
                assert last_exc is not None  # pragma: no cover
                raise last_exc  # pragma: no cover

        update_wrapper(wrapped, f)
        return wrapped

    return binder.bind_optional(fn, _decorate)
