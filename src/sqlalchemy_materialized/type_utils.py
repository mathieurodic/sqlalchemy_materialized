from __future__ import annotations

import types
import typing
from typing import Any, get_args, get_origin


def unwrap_optional(t: Any) -> tuple[Any, bool]:
    """Unwrap Optional[T] / T | None annotations.

    Parameters
    ----------
    t:
        A type annotation.

    Returns
    -------
    (inner, is_optional)
        - inner: the unwrapped type when `t` is Optional[T]
        - is_optional: True when `t` contains NoneType

    Notes
    -----
    This function is intentionally strict:
    - Optional[T] where the non-None side is a single type is supported.
    - Any other Union (e.g. Union[int, str], Union[int, str, None]) raises.
    """

    origin = get_origin(t)
    if origin is None:
        return t, False

    # Only unions can represent Optional / T | None.
    # For any other parametrized annotation (e.g. list[int]), keep as-is.
    if origin not in (types.UnionType, typing.Union):
        # `typing.Union` has origin == types.UnionType starting in recent Python,
        # but keep this as a conservative check: if it's not a union, it's not
        # an Optional.
        return t, False

    args = list(get_args(t))
    none_type = type(None)
    has_none = any(a is none_type for a in args)
    if not has_none:
        # Not an Optional/nullable union.
        raise TypeError(f"Unsupported type annotation (non-Optional union): {t!r}")

    non_none = [a for a in args if a is not none_type]
    if len(non_none) != 1:
        # Union[None, A, B] etc.
        raise TypeError(f"Unsupported Optional/Union annotation: {t!r}")

    inner = non_none[0]
    # NOTE: `inner` can be a parametrized typing object, e.g. `list[int]`.
    # We accept it so that callers can handle containers themselves.
    return inner, True
