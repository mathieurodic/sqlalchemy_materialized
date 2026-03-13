from __future__ import annotations

from typing import Any, Callable

from .config import _MaterializedConfig
from .descriptor import _MaterializedPropertyDescriptor


def materialized_property(
    fn: Callable[..., Any] | None = None,
    *,
    in_transaction: bool = True,
    depends_on: tuple[str, ...] = (),
    validate: bool = True,
    retry_on: (
        type[Exception]
        | tuple[type[Exception], ...]
        | Callable[[Exception], bool]
    ) = (),
    retry_max: int = 3,
    retry_factor: float = 2.0,
    retry_interval: float = 1.0,
):
    """Create a materialized property.

    Supports:
    - @materialized_property
    - @materialized_property(in_transaction=False)
    - @materialized_property(retry_on=SomeError, retry_max=3)
    - @materialized_property(retry_on=(SomeError,), retry_max=3)
    - value = materialized_property(compute)
    """

    if retry_max < 1:
        raise ValueError("materialized_property(...): retry_max must be >= 1")
    if retry_interval < 0:
        raise ValueError("materialized_property(...): retry_interval must be >= 0")

    # Validate retry_on early so should_retry() can be simple and predictable.
    if isinstance(retry_on, tuple):
        for t in retry_on:
            if not (isinstance(t, type) and issubclass(t, Exception)):
                raise TypeError(
                    "materialized_property(...): retry_on tuple items must be Exception subclasses"
                )
    elif isinstance(retry_on, type) and issubclass(retry_on, Exception):
        pass
    elif callable(retry_on):
        pass
    else:
        raise TypeError(
            "materialized_property(...): retry_on must be an Exception subclass, a tuple of Exception subclasses, or a predicate"
        )

    config = _MaterializedConfig(
        in_transaction=in_transaction,
        depends_on=depends_on,
        validate=validate,
        retry_on=retry_on,
        retry_max=retry_max,
        retry_factor=retry_factor,
        retry_interval=retry_interval,
    )

    if fn is None:

        def wrapper(f):
            return _MaterializedPropertyDescriptor(f, config)

        return wrapper

    return _MaterializedPropertyDescriptor(fn, config)
