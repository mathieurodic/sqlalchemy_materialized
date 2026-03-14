from __future__ import annotations

from typing import Any, Callable

from etl_decorators._base.decorators import OptionalFnDecoratorBase

from .config import _MaterializedConfig
from .descriptor import _MaterializedPropertyDescriptor


def materialized_property(
    fn: Callable[..., Any] | None = None,
    *,
    in_transaction: bool = True,
    depends_on: tuple[str, ...] = (),
    validate: bool = True,
):
    """Create a materialized property.

    Supports:
    - @materialized_property
    - @materialized_property(in_transaction=False)
    - value = materialized_property(compute)
    """

    config = _MaterializedConfig(
        in_transaction=in_transaction,
        depends_on=depends_on,
        validate=validate,
    )

    binder = OptionalFnDecoratorBase()

    def _decorate(f: Callable[..., Any]):
        return _MaterializedPropertyDescriptor(f, config)

    return binder.bind_optional(fn, _decorate)
