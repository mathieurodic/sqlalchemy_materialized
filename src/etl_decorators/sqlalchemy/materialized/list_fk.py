from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import relationship

if TYPE_CHECKING:  # pragma: no cover
    from .descriptor import _MaterializedPropertyDescriptor


def inject_list_fk_storage(descriptor: "_MaterializedPropertyDescriptor", owner: type) -> None:
    assert descriptor._prop_name is not None
    cache_attr = descriptor.cache_attr
    target_cls = descriptor._list_item_type

    # The association table uses column-level ForeignKey, which is enough
    # for SQLAlchemy to infer the correct type of the FK column even though
    # __set_name__ runs before mappers are fully configured.
    owner_table = getattr(owner, "__tablename__", owner.__name__.lower())
    target_table = getattr(target_cls, "__tablename__", target_cls.__name__.lower())
    assoc_table_name = (
        f"__materialized__{owner_table}__{descriptor._prop_name}__{target_table}"
    )

    # Find the owner PK database column name. At __set_name__ time the
    # InstrumentedAttribute isn't configured yet, but mapped_column has
    # already created a Column with name/key.
    owner_pk_col_name: str | None = None
    for k, v in owner.__dict__.items():
        col = getattr(v, "column", None)
        if col is not None and getattr(col, "primary_key", False):
            # At __set_name__ time, SQLAlchemy may not have assigned a final
            # name to the Column yet (col.name can be None). In that case,
            # the default DB column name will match the attribute name.
            owner_pk_col_name = col.name or getattr(col, "key", None) or k
            break
    if owner_pk_col_name is None:
        raise RuntimeError(
            "materialized_property: cannot create association table for list[MappedClass]; "
            f"no primary key detected on {owner!r}"
        )

    # Target PK column name & type: use SQLAlchemy inspection (works at
    # class creation time).
    mapper = sa_inspect(target_cls)
    pk_cols = list(mapper.primary_key)
    if len(pk_cols) != 1:
        raise ValueError(
            "materialized_property only supports single-column primary keys "
            f"(composite PK detected for {target_cls!r})."
        )
    target_pk_col = pk_cols[0]
    target_pk_col_name = target_pk_col.name

    assoc_table = sa.Table(
        assoc_table_name,
        owner.metadata,
        sa.Column(
            "owner_id",
            sa.ForeignKey(f"{owner_table}.{owner_pk_col_name}"),
            primary_key=True,
        ),
        sa.Column("pos", sa.Integer, primary_key=True, nullable=False),
        sa.Column(
            "target_id",
            sa.ForeignKey(f"{target_table}.{target_pk_col_name}"),
            nullable=False,
        ),
    )

    assoc_cls_name = f"__MaterializedAssoc__{owner.__name__}__{descriptor._prop_name}"
    assoc_cls = type(assoc_cls_name, (), {})
    # Map the association class
    owner.registry.map_imperatively(
        assoc_cls,
        assoc_table,
        properties={
            "target": relationship(target_cls),
        },
    )
    descriptor._list_assoc_cls = assoc_cls

    # Relationship holding association objects.
    descriptor._list_assoc_attr = cache_attr
    setattr(
        owner,
        cache_attr,
        relationship(
            assoc_cls,
            cascade="all, delete-orphan",
            order_by=assoc_table.c.pos,
        ),
    )
