"""Core :func:`etl_decorators.logging.log` decorator implementation."""

from __future__ import annotations

import inspect
import logging
import time
from functools import wraps
from typing import Any, Callable, ParamSpec, TypeVar, overload


P = ParamSpec("P")
R = TypeVar("R")


_DEFAULT_MAX_REPR = 200


def _safe_repr(value: object, *, max_len: int = _DEFAULT_MAX_REPR) -> str:
    """Best-effort repr that never raises and truncates long values."""

    try:
        out = repr(value)
    except Exception:
        out = f"<unreprable {type(value).__name__}>"

    if max_len > 0 and len(out) > max_len:
        if max_len <= 3:
            return out[:max_len]
        return out[: max_len - 3] + "..."
    return out


def _format_bound_arguments(bound: inspect.BoundArguments, *, max_repr: int) -> str:
    parts: list[str] = []
    for k, v in bound.arguments.items():
        parts.append(f"{k}={_safe_repr(v, max_len=max_repr)}")
    return ", ".join(parts)


def _resolve_level(level: str | int) -> int:
    if isinstance(level, int):
        return level

    name = level.strip().upper()
    if not name:
        raise ValueError("log(level=...): level must be a non-empty string or an int")

    if name.isdigit():
        return int(name)

    mapping = {
        "CRITICAL": logging.CRITICAL,
        "FATAL": logging.FATAL,
        "ERROR": logging.ERROR,
        "WARNING": logging.WARNING,
        "WARN": logging.WARNING,
        "INFO": logging.INFO,
        "DEBUG": logging.DEBUG,
        "NOTSET": logging.NOTSET,
    }
    if name not in mapping:
        raise ValueError(
            "log(level=...): invalid level string. "
            f"Expected one of {sorted(mapping)!r}, received {level!r}"
        )

    return mapping[name]


@overload
def log(fn: Callable[P, R]) -> Callable[P, R]: ...


@overload
def log(
    fn: None = None,
    *,
    level: str | int = "DEBUG",
    with_result: bool = True,
    with_arguments: bool = True,
    with_duration: bool = True,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def log(
    fn: Callable[P, R] | None = None,
    *,
    level: str | int = "DEBUG",
    with_result: bool = True,
    with_arguments: bool = True,
    with_duration: bool = True,
):
    """Log execution of the decorated callable.

    This is a lightweight wrapper around stdlib :mod:`logging`.

    Successful calls emit **two log lines**:
    - a *start* line, before calling the function, and
    - an *end* line, after completion.

    Exceptions are logged (with traceback) and then re-raised.

    Parameters:
        level: logging level (e.g. "DEBUG", "INFO") or an integer.
        with_result: include a representation of the return value.
        with_arguments: include a representation of bound args/kwargs.
        with_duration: include wall-clock duration (seconds).
    """

    resolved_level = _resolve_level(level)
    max_repr = _DEFAULT_MAX_REPR

    def _decorate(f: Callable[P, R]) -> Callable[P, R]:
        logger = logging.getLogger(getattr(f, "__module__", __name__))
        # Prefer a short, stable name for log messages.
        # __qualname__ includes <locals> noise when functions are defined inside
        # other functions (common in tests and notebooks).
        name = getattr(f, "__name__", "<callable>")
        sig = inspect.signature(f)
        is_async = inspect.iscoroutinefunction(f)

        if is_async:

            @wraps(f)
            async def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[misc]
                bound_str = ""
                if with_arguments:
                    bound = sig.bind_partial(*args, **kwargs)
                    bound.apply_defaults()
                    bound_str = _format_bound_arguments(bound, max_repr=max_repr)

                if with_arguments:
                    logger.log(resolved_level, "start %s(%s)", name, bound_str)
                else:
                    logger.log(resolved_level, "start %s", name)

                start = time.perf_counter()
                try:
                    result = await f(*args, **kwargs)
                except Exception as e:
                    elapsed = time.perf_counter() - start
                    msg_parts = [f"failed {name}"]
                    if with_duration:
                        msg_parts.append(f"duration={elapsed:.6f}s")
                    msg_parts.append(f"exc={e.__class__.__name__}: {e}")
                    logger.log(resolved_level, " ".join(msg_parts), exc_info=True)
                    raise

                elapsed = time.perf_counter() - start
                msg_parts = [f"end {name}"]
                if with_duration:
                    msg_parts.append(f"duration={elapsed:.6f}s")
                if with_result:
                    msg_parts.append(f"result={_safe_repr(result, max_len=max_repr)}")
                logger.log(resolved_level, " ".join(msg_parts))
                return result

        else:

            @wraps(f)
            def wrapped(*args: P.args, **kwargs: P.kwargs) -> R:  # type: ignore[misc]
                bound_str = ""
                if with_arguments:
                    bound = sig.bind_partial(*args, **kwargs)
                    bound.apply_defaults()
                    bound_str = _format_bound_arguments(bound, max_repr=max_repr)

                if with_arguments:
                    logger.log(resolved_level, "start %s(%s)", name, bound_str)
                else:
                    logger.log(resolved_level, "start %s", name)

                start = time.perf_counter()
                try:
                    result = f(*args, **kwargs)
                except Exception as e:
                    elapsed = time.perf_counter() - start
                    msg_parts = [f"failed {name}"]
                    if with_duration:
                        msg_parts.append(f"duration={elapsed:.6f}s")
                    msg_parts.append(f"exc={e.__class__.__name__}: {e}")
                    logger.log(resolved_level, " ".join(msg_parts), exc_info=True)
                    raise

                elapsed = time.perf_counter() - start
                msg_parts = [f"end {name}"]
                if with_duration:
                    msg_parts.append(f"duration={elapsed:.6f}s")
                if with_result:
                    msg_parts.append(f"result={_safe_repr(result, max_len=max_repr)}")
                logger.log(resolved_level, " ".join(msg_parts))
                return result

        return wrapped

    if fn is None:
        return _decorate
    return _decorate(fn)
