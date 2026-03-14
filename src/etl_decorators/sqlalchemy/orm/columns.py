from datetime import date, datetime, time
from typing import Any, get_args, get_origin

import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.exc import NoInspectionAvailable
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm.properties import MappedColumn

from ..type_decorators.pydantic_json import PydanticJSON, PydanticJSONList
from ..utils.typing import unwrap_optional


TYPE_MAP = {
    int: sa.Integer,
    float: sa.Float,
    str: sa.String,
    bool: sa.Boolean,
    date: sa.Date,
    datetime: lambda: sa.DateTime(timezone=True),
    time: sa.Time,
}


def _is_sa_type(t: Any) -> bool:
    """Return True if t looks like a SQLAlchemy type or type instance."""

    # sa.String / sa.JSON are classes (subclasses of TypeEngine).
    if isinstance(t, type) and issubclass(t, sa.types.TypeEngine):
        return True

    # PydanticJSON(Payload) etc are instances.
    if isinstance(t, sa.types.TypeEngine):
        return True

    return False


def make_sa_column(
    name: str,
    t: Any,
    *,
    nullable: bool | None = None,
    **mapped_column_kwargs: Any,
) -> MappedColumn:
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

    if nullable is None:
        nullable = _is_optional

    # - Allow callers to pass SQLAlchemy types directly.
    #   Examples:
    #   - details: sa.JSON
    #   - payload: PydanticJSON(Payload)
    if _is_sa_type(t):
        sa_type = t() if isinstance(t, type) else t
        return mapped_column(name, sa_type, nullable=nullable, **mapped_column_kwargs)

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
            return mapped_column(
                name,
                PydanticJSONList(item_t),
                nullable=nullable,
                **mapped_column_kwargs,
            )

        return mapped_column(name, sa.JSON, nullable=nullable, **mapped_column_kwargs)

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
            nullable=nullable,
            **mapped_column_kwargs,
        )

    # 2) "Pydantic BaseModel" case: JSON storage
    try:
        from pydantic import BaseModel
    except Exception:  # pragma: no cover
        BaseModel = None  # type: ignore[assignment]

    if BaseModel is not None and isinstance(t, type) and issubclass(t, BaseModel):
        return mapped_column(
            name,
            PydanticJSON(t),
            nullable=nullable,
            **mapped_column_kwargs,
        )

    # 3) "Standard Python type" case: mapping via TYPE_MAP.
    if t in TYPE_MAP:
        maker = TYPE_MAP[t]
        return mapped_column(
            name,
            maker(),
            nullable=nullable,
            **mapped_column_kwargs,
        )

    raise TypeError(
        "make_sa_column: unsupported python type annotation. "
        f"name={name!r}, type={t!r}. "
        f"Supported scalar types: {sorted([k.__name__ for k in TYPE_MAP.keys() if hasattr(k, '__name__')])}. "
        "If you need custom storage, use an explicit SQLAlchemy type / TypeDecorator in your model."
    )
