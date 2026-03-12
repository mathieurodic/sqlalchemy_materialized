from datetime import datetime, time
from typing import Any, get_args, get_origin

import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.exc import NoInspectionAvailable
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm.properties import MappedColumn

from .pydantic_json import PydanticJSON, PydanticJSONList
from .type_utils import unwrap_optional


TYPE_MAP = {
    int: sa.Integer,
    float: sa.Float,
    str: sa.String,
    bool: sa.Boolean,
    datetime: sa.DateTime,
    time: sa.Time,
}


def make_sa_column(name: str, t: Any) -> MappedColumn:
    """Create a SQLAlchemy (ORM) column from a Python type.

    Parameters
    ----------
    name:
        Database column name (often the name of the hybrid property).
    t:
        Python type (often coming from the return annotation).

    Returns
    -------
    sqlalchemy.orm.properties.MappedColumn
        Result of `mapped_column(...)`.
    """
    # Light normalization of annotations (handles Optional[T])
    # NOTE: unwrap_optional is strict and raises on other unions.
    inner, _is_optional = unwrap_optional(t)
    t = inner

    # 0) list[...] case: store as JSON.
    # - list[BaseModelSubclass] => validated list via PydanticJSONList
    # - any other list[...] => raw JSON
    origin = get_origin(t)
    if origin is list:
        args = list(get_args(t))
        item_t = args[0] if args else Any

        # Pydantic list => validated serialization/deserialization
        try:
            from pydantic import BaseModel
        except Exception:  # pragma: no cover
            BaseModel = None  # type: ignore[assignment]

        if (
            BaseModel is not None
            and isinstance(item_t, type)
            and issubclass(item_t, BaseModel)
        ):
            return mapped_column(name, PydanticJSONList(item_t), nullable=True)

        return mapped_column(name, sa.JSON, nullable=True)

    # 1) "SQLAlchemy Declarative model" case: create a FK to its PK.
    try:
        mapper = sa_inspect(t)
    except NoInspectionAvailable:
        mapper = None

    if mapper is not None and hasattr(mapper, "primary_key"):
        pk_cols = list(mapper.primary_key)
        if len(pk_cols) != 1:
            raise ValueError(
                "make_sa_column only supports single-column primary keys "
                f"(composite PK detected for {t!r})."
            )

        pk_col = pk_cols[0]
        target_table = pk_col.table.name
        target_pk = pk_col.name
        # Defensive copy of the PK type to avoid sharing a TypeEngine instance.
        pk_type = pk_col.type.copy() if hasattr(pk_col.type, "copy") else pk_col.type
        return mapped_column(
            name,
            pk_type,
            sa.ForeignKey(f"{target_table}.{target_pk}"),
            nullable=True,
        )

    # 2) "Pydantic BaseModel" case: JSON storage
    try:
        from pydantic import BaseModel
    except Exception:  # pragma: no cover
        BaseModel = None  # type: ignore[assignment]

    if BaseModel is not None and isinstance(t, type) and issubclass(t, BaseModel):
        return mapped_column(name, PydanticJSON(t), nullable=True)

    # 3) "Standard Python type" case: mapping via TYPE_MAP, fallback to String.
    sa_type = TYPE_MAP.get(t, sa.String)()
    return mapped_column(name, sa_type, nullable=True)