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
    autosave: bool = False,
    autocommit: bool | None = None,
):
    """Create a materialized property.

    Supports:
    - @materialized_property
    - @materialized_property(in_transaction=False)
    - value = materialized_property(compute)
    """

    # Backwards/ergonomic aliasing:
    # - `autosave` is the public flag name (requested by the task)
    # - `autocommit` is a clearer synonym; if provided it overrides autosave
    effective_autocommit = autosave if autocommit is None else autocommit

    config = _MaterializedConfig(
        in_transaction=in_transaction,
        depends_on=depends_on,
        validate=validate,
        autocommit=effective_autocommit,
    )

    binder = OptionalFnDecoratorBase()

    def _decorate(f: Callable[..., Any]):
        return _MaterializedPropertyDescriptor(f, config)

    return binder.bind_optional(fn, _decorate)
