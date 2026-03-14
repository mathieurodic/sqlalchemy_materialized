"""SQLAlchemy-related decorators.

This subpackage requires SQLAlchemy (and some helpers may require pydantic).

Install with:

    pip install etl-decorators[sqlalchemy]

Only the main decorator is exported at this level:

    from etl_decorators.sqlalchemy import materialized_property
"""

from .materialized import materialized_property
from .orm import JSON, field
from .orm.as_model import as_model

__all__ = [
    "JSON",
    "as_model",
    "field",
    "materialized_property",
]
