"""Materialized properties.

This subpackage contains the implementation of
:func:`etl_decorators.sqlalchemy.materialized_property`.

Only the subpackage-level import is considered stable:

    from etl_decorators.sqlalchemy import materialized_property
"""

from .decorator import materialized_property

__all__ = [
    "materialized_property",
]
