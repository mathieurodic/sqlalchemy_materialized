from __future__ import annotations

from typing import TYPE_CHECKING

import sqlalchemy as sa
from sqlalchemy.orm import relationship

from .o2m_collection import _MaterializedO2MList


if TYPE_CHECKING:  # pragma: no cover
    from .descriptor import _MaterializedPropertyDescriptor


def inject_list_o2m_fk_storage(
    descriptor: "_MaterializedPropertyDescriptor",
    owner: type,
) -> None:
    """Inject one-to-many storage for list[MappedClass].

    At class construction time (`__set_name__`), we inject the required mapped
    attributes (FK column + relationships) without forcing mapper
    configuration.

    Note: we set up `back_populates` directly on both relationship() calls.
    SQLAlchemy will resolve them during mapper configuration.
    """

    assert descriptor._prop_name is not None

    prop_name = descriptor._prop_name
    child_cls = descriptor._list_item_type
    if child_cls is None:
        raise RuntimeError(
            "materialized_property: internal error (missing list item type)"
        )

    # Ensure child is mapped and has a single-column PK.
    try:
        child_mapper = sa.inspect(child_cls)
    except Exception as e:
        raise TypeError(
            "materialized_property: list[...] return annotation must be a mapped class; "
            f"got: {child_cls!r}"
        ) from e

    pk_cols = list(getattr(child_mapper, "primary_key", []) or [])
    if len(pk_cols) != 1:
        raise ValueError(
            "materialized_property only supports single-column primary keys "
            f"(composite PK detected for {child_cls!r})."
        )

    owner_name = owner.__name__.lower()
    fk_attr = f"{owner_name}_id"
    child_owner_attr = owner_name

    # Determine FK target based on owner's PK column name.
    owner_table = getattr(owner, "__tablename__", owner.__name__.lower())
    owner_pk_col_name: str | None = None
    for k, v in owner.__dict__.items():
        col = getattr(v, "column", None)
        if col is not None and getattr(col, "primary_key", False):
            owner_pk_col_name = col.name or getattr(col, "key", None) or k
            break
    if owner_pk_col_name is None:
        raise RuntimeError(
            "materialized_property: cannot inject one-to-many FK storage for list[MappedClass]; "
            f"no primary key detected on {owner!r}"
        )

    fk_target = f"{owner_table}.{owner_pk_col_name}"

    # 1) Inject child FK column.
    if not hasattr(child_cls, fk_attr):
        setattr(
            child_cls,
            fk_attr,
            sa.orm.mapped_column(sa.ForeignKey(fk_target), nullable=True),
        )

    # 2) Inject relationships.
    #
    # We set back_populates on both sides up-front. Even though we're adding
    # these attributes during class construction, SQLAlchemy resolves
    # back_populates during mapper configuration (later), so both sides will be
    # present by then.
    setattr(
        child_cls,
        child_owner_attr,
        relationship(owner, back_populates=prop_name),
    )

    # Always override the owner attribute: at __set_name__ time it's still the
    # descriptor instance, but we want the public attribute to be a
    # relationship.
    setattr(
        owner,
        prop_name,
        relationship(
            child_cls,
            back_populates=child_owner_attr,
            cascade="all, delete-orphan",
            collection_class=_MaterializedO2MList,
            lazy="noload",
        ),
    )

    # Configure collection materializer behavior.
    # These attributes are class-level defaults which will be used by each
    # instance's collection.
    rel = getattr(owner, prop_name)
    try:
        # Use staticmethod to avoid Python binding the function to the
        # collection instance when accessed via `self._compute_fn`.
        rel.collection_class._compute_fn = staticmethod(descriptor.fn)  # type: ignore[attr-defined]
        rel.collection_class._computed_at_attr = descriptor.computed_at_attr  # type: ignore[attr-defined]
        rel.collection_class._prop_name = prop_name  # type: ignore[attr-defined]
        rel.collection_class._child_cls = child_cls  # type: ignore[attr-defined]
        rel.collection_class._child_owner_attr = child_owner_attr  # type: ignore[attr-defined]
        rel.collection_class._in_transaction = descriptor.config.in_transaction  # type: ignore[attr-defined]
        rel.collection_class._validate_value = staticmethod(descriptor._validate_list_fk_value)  # type: ignore[attr-defined]
        rel.collection_class._normalize_list_to_instances = staticmethod(descriptor._normalize_list_fk_to_instances)  # type: ignore[attr-defined]
        rel.collection_class._materializing_guard_attr = f"_{descriptor.fn.__name__}__materializing"  # type: ignore[attr-defined]
    except Exception:
        # Defensive: don't break mapping if we can't set this for some reason.
        pass

    # No mapper events required.


# Backwards compat import path within the package.
inject_list_fk_storage = inject_list_o2m_fk_storage
