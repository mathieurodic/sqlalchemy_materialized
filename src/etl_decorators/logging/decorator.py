"""Core :func:`etl_decorators.logging.log` decorator implementation."""

from __future__ import annotations

import inspect
import logging
import secrets
import time
from typing import Any, Callable, ParamSpec, TypeVar, overload

from etl_decorators._base.decorators import DecoratorBase, OptionalFnDecoratorBase, _RAISE


P = ParamSpec("P")
R = TypeVar("R")


_DEFAULT_MAX_REPR = 200


def _generate_run_id() -> str:
    """Generate a short identifier for a single decorated invocation.

    Currently returns 8 lowercase hex characters.
    """

    return secrets.token_hex(4)


def _action_prefix(action: str, run_id: str) -> str:
    # action is padded for alignment: START / DONE / ERROR
    return f"[{action:<5} {run_id}]"


def _format_location(fn: Callable[..., Any]) -> str:
    path = inspect.getsourcefile(fn) or "<unknown>"
    filename = path.rsplit("/", 1)[-1]
    lineno = getattr(getattr(fn, "__code__", None), "co_firstlineno", None)
    if lineno is None:
        return f"@{filename}:?"
    return f"@{filename}:{lineno}"


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
    with_location: bool = True,
) -> Callable[[Callable[P, R]], Callable[P, R]]: ...


def log(
    fn: Callable[P, R] | None = None,
    *,
    level: str | int = "DEBUG",
    with_result: bool = True,
    with_arguments: bool = True,
    with_duration: bool = True,
    with_location: bool = True,
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
        with_location: include source file + line number where the decorated function is defined.
    """

    resolved_level = _resolve_level(level)
    max_repr = _DEFAULT_MAX_REPR

    class _LogDecorator(
        DecoratorBase[P, R, tuple[logging.Logger, str, inspect.Signature, float, str, str]]
    ):
        def before_call(
            self,
            f: Callable[P, Any],
            args: tuple[Any, ...],
            kwargs: dict[str, Any],
        ) -> tuple[logging.Logger, str, inspect.Signature, float, str, str]:
            logger = logging.getLogger(getattr(f, "__module__", __name__))
            name = getattr(f, "__name__", "<callable>")
            sig = inspect.signature(f)
            run_id = _generate_run_id()
            location = _format_location(f) if with_location else ""

            prefix = _action_prefix("START", run_id)
            loc = f" {location}" if location else ""

            if with_arguments:
                bound = sig.bind_partial(*args, **kwargs)
                bound.apply_defaults()
                bound_str = _format_bound_arguments(bound, max_repr=max_repr)
                logger.log(resolved_level, "%s%s %s(%s)", prefix, loc, name, bound_str)
            else:
                logger.log(resolved_level, "%s%s %s", prefix, loc, name)

            start = time.perf_counter()
            return (logger, name, sig, start, run_id, location)

        def process_result(
            self,
            _fn: Callable[P, Any],
            result: Any,
            _args: tuple[Any, ...],
            _kwargs: dict[str, Any],
            state: tuple[logging.Logger, str, inspect.Signature, float, str, str],
        ) -> R:
            logger, name, _sig, start, run_id, location = state
            elapsed = time.perf_counter() - start

            prefix = _action_prefix("DONE", run_id)
            head_parts = [prefix]
            if location:
                head_parts.append(location)
            head_parts.append(name)

            details: list[str] = []
            if with_duration:
                details.append(f"duration={elapsed:.6f}s")
            if with_result:
                details.append(f"result={_safe_repr(result, max_len=max_repr)}")

            if details:
                logger.log(resolved_level, f"{' '.join(head_parts)}: {', '.join(details)}")
            else:
                logger.log(resolved_level, " ".join(head_parts))
            return result

        def process_exception(
            self,
            _fn: Callable[P, Any],
            exc: Exception,
            _args: tuple[Any, ...],
            _kwargs: dict[str, Any],
            state: tuple[logging.Logger, str, inspect.Signature, float, str, str],
        ) -> Any:
            logger, name, _sig, start, run_id, location = state
            elapsed = time.perf_counter() - start

            prefix = _action_prefix("ERROR", run_id)
            head_parts = [prefix]
            if location:
                head_parts.append(location)
            head_parts.append(name)

            details: list[str] = []
            if with_duration:
                details.append(f"duration={elapsed:.6f}s")
            details.append(f"exc={exc.__class__.__name__}: {exc}")

            logger.log(
                resolved_level,
                f"{' '.join(head_parts)}: {', '.join(details)}",
                exc_info=True,
            )
            return _RAISE

    binder = OptionalFnDecoratorBase()

    def _decorate(f: Callable[P, R]) -> Callable[P, R]:
        return _LogDecorator().decorate(f)

    return binder.bind_optional(fn, _decorate)
