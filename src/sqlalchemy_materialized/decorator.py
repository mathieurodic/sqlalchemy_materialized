from __future__ import annotations

from contextlib import nullcontext
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Callable, get_args, get_origin

import sqlalchemy as sa
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import object_session, relationship

from .columns import make_sa_column
from .type_utils import unwrap_optional


def _is_mapped_class(t: type) -> bool:
    try:
        mapper = sa_inspect(t)
    except Exception:
        return False
    return hasattr(mapper, "primary_key")


def _pk_id_from_instance(obj) -> object:
    insp = sa_inspect(obj)
    ident = getattr(insp, "identity", None)
    if not ident:
        raise ValueError(
            "Unable to extract identity (PK) from the given instance. "
            "Is it persisted / flushed?"
        )
    if len(ident) != 1:
        raise ValueError(
            "materialized_property only supports single-column primary keys "
            f"(composite PK detected for {obj!r})."
        )
    return ident[0]


def _require_session(obj) -> sa.orm.Session:
    session = object_session(obj)
    if session is None:
        raise RuntimeError(
            "materialized_property: object must be attached to a Session "
            "(detached instances are not supported)."
        )
    return session


@dataclass(frozen=True)
class _MaterializedConfig:
    in_transaction: bool = True


class _MaterializedPropertyDescriptor:
    def __init__(self, fn: Callable[..., Any], config: _MaterializedConfig):
        self.fn = fn
        self.config = config

        # Internal attribute names are derived from the compute function name.
        # This avoids collisions with the public API.
        self.cache_attr: str = f"_{fn.__name__}"
        self.computed_at_attr: str = f"_{fn.__name__}__computed_at"

        # We only know the public property name in __set_name__.
        self._prop_name: str | None = None
        self._list_assoc_attr: str | None = None
        self._list_assoc_cls: type | None = None

        return_ann = fn.__annotations__.get("return")
        if return_ann is None:
            raise TypeError(
                f"materialized_property {fn.__name__} must have a return annotation"
            )

        try:
            # Normalize Optional[T] / T | None so FK detection and isinstance() work.
            unwrapped, _is_optional = unwrap_optional(return_ann)
        except TypeError as e:
            raise TypeError(
                f"Unsupported return annotation for materialized_property {fn.__name__}: {return_ann!r}"
            ) from e

        self.return_type = unwrapped
        self._is_list = get_origin(unwrapped) is list
        if self._is_list:
            args = list(get_args(unwrapped))
            self._list_item_type = args[0] if args else object
            self._is_fk = _is_mapped_class(self._list_item_type)
        else:
            self._list_item_type = None
            self._is_fk = _is_mapped_class(self.return_type)

    def __set_name__(self, owner, name):
        self._prop_name = name

        # 1) list[MappedClass] is stored via association table + relationship
        #    (not a JSON column).
        if self._is_list and self._is_fk:
            self._inject_list_fk_storage(owner)
        else:
            # Inject the backing DB column into the class
            col = make_sa_column(name, self.return_type)
            setattr(owner, self.cache_attr, col)

        # 2) computed_at column (timezone-aware)
        computed_at_col = sa.orm.mapped_column(
            f"{name}__computed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        )
        setattr(owner, self.computed_at_attr, computed_at_col)

        # 3) Build and inject the hybrid property, replacing ourselves
        setattr(owner, name, self._make_hybrid(owner))

    def _inject_list_fk_storage(self, owner):
        assert self._prop_name is not None
        cache_attr = self.cache_attr
        target_cls = self._list_item_type

        # The association table uses column-level ForeignKey, which is enough
        # for SQLAlchemy to infer the correct type of the FK column even though
        # __set_name__ runs before mappers are fully configured.
        owner_table = getattr(owner, "__tablename__", owner.__name__.lower())
        target_table = getattr(target_cls, "__tablename__", target_cls.__name__.lower())
        assoc_table_name = f"__materialized__{owner_table}__{self._prop_name}__{target_table}"

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

        assoc_cls_name = f"__MaterializedAssoc__{owner.__name__}__{self._prop_name}"
        assoc_cls = type(assoc_cls_name, (), {})
        # Map the association class
        owner.registry.map_imperatively(
            assoc_cls,
            assoc_table,
            properties={
                "target": relationship(target_cls),
            },
        )
        self._list_assoc_cls = assoc_cls

        # Relationship holding association objects.
        self._list_assoc_attr = cache_attr
        setattr(
            owner,
            cache_attr,
            relationship(
                assoc_cls,
                cascade="all, delete-orphan",
                order_by=assoc_table.c.pos,
            ),
        )

        # Note: for list[MappedClass], the cached value is stored in the
        # association relationship (self._list_assoc_attr). We intentionally do
        # not create a backing column.

    def _make_hybrid(self, owner):
        cache_attr = self.cache_attr
        computed_at_attr = self.computed_at_attr
        compute_fn = self.fn
        in_transaction = self.config.in_transaction
        is_fk = self._is_fk
        is_list = self._is_list
        target_cls = self._list_item_type if is_list else self.return_type
        list_assoc_attr = self._list_assoc_attr
        list_assoc_cls = self._list_assoc_cls
        is_list_fk = bool(is_list and is_fk and list_assoc_attr and list_assoc_cls)

        def normalize_to_id(value):
            if not is_fk:
                return value

            if is_list:
                if value is None:
                    return None
                if not isinstance(value, list):
                    raise TypeError(
                        "materialized_property: expected a list for list[...] return type, "
                        f"received: {type(value)!r}"
                    )
                out = []
                for item in value:
                    if item is None:
                        raise TypeError(
                            "materialized_property: list[MappedClass] does not accept None items"
                        )
                    if isinstance(item, target_cls):
                        out.append(_pk_id_from_instance(item))
                    else:
                        out.append(item)
                return out

            if value is None:
                return None
            if isinstance(value, target_cls):
                return _pk_id_from_instance(value)
            return value

        def resolve_fk_scalar(self, ident):
            if ident is None:
                return None
            session = _require_session(self)
            out = session.get(target_cls, ident)
            return out

        def normalize_list_fk_to_instances(self, value) -> list[Any]:
            """Accept list[instance|id] and return list[instances] preserving order."""
            if value is None:
                return []
            if not isinstance(value, list):
                raise TypeError(
                    "materialized_property: expected a list for list[...] return type, "
                    f"received: {type(value)!r}"
                )

            session = _require_session(self)
            out: list[Any] = []
            for item in value:
                if item is None:
                    raise TypeError(
                        "materialized_property: list[MappedClass] does not accept None items"
                    )
                if isinstance(item, target_cls):
                    # Ensure identity exists early.
                    _pk_id_from_instance(item)
                    out.append(item)
                    continue

                inst = session.get(target_cls, item)
                if inst is None:
                    raise RuntimeError(
                        "materialized_property: unable to resolve identifier from list[MappedClass] "
                        f"(id={item!r} not found)"
                    )
                out.append(inst)
            return out

        @hybrid_property
        def prop(self):
            computed_at = getattr(self, computed_at_attr)
            if computed_at is None:
                session = _require_session(self)

                if is_list_fk:
                    old_cache = list(getattr(self, list_assoc_attr))
                else:
                    old_cache = getattr(self, cache_attr)
                old_computed_at = getattr(self, computed_at_attr)

                try:
                    if in_transaction:
                        cm = session.begin_nested()
                    else:
                        cm = nullcontext()

                    with cm:
                        computed = compute_fn(self)

                        if is_list_fk:
                            targets = normalize_list_fk_to_instances(self, computed)
                            assoc_rows = [
                                list_assoc_cls(pos=i, target=t) for i, t in enumerate(targets)
                            ]
                            setattr(self, list_assoc_attr, assoc_rows)
                            setattr(self, computed_at_attr, datetime.now(timezone.utc))
                            session.flush()
                        else:
                            normalized = normalize_to_id(computed)
                            setattr(self, cache_attr, normalized)
                            setattr(self, computed_at_attr, datetime.now(timezone.utc))
                            session.flush()
                except Exception:
                    # Rollback DB side effects is handled by begin_nested()'s context manager.
                    # We also restore the in-memory attributes so the property remains "not computed".
                    setattr(self, cache_attr, old_cache)
                    setattr(self, computed_at_attr, old_computed_at)
                    raise

            # Fast path for list[MappedClass]
            if is_list_fk:
                # Even if already materialized, we require a Session for FK-like
                # materialized properties. This keeps the API consistent.
                _require_session(self)
                return [row.target for row in getattr(self, list_assoc_attr)]

            # Fast path for non-FK
            if not is_fk:
                return getattr(self, cache_attr)

            if is_list:
                return getattr(self, cache_attr)

            # FK scalar
            return resolve_fk_scalar(self, getattr(self, cache_attr))

        @prop.expression
        def prop(cls):  # type: ignore[no-redef]
            if is_list:
                raise NotImplementedError(
                    "materialized_property: SQL expression is not supported for list[...] return types"
                )
            # SQL expression for filtering/ordering: use the stored/materialized
            # value directly.
            #
            # Notes:
            # - This does NOT trigger computation. Rows with NULL in the backing
            #   column will behave like any other NULL in SQL.
            # - For FK return types, the stored value is the foreign key id.
            return getattr(cls, cache_attr)

        @prop.setter
        def prop(self, value):
            if is_list_fk:
                targets = normalize_list_fk_to_instances(self, value)
                assoc_rows = [
                    list_assoc_cls(pos=i, target=t) for i, t in enumerate(targets)
                ]
                setattr(self, list_assoc_attr, assoc_rows)
                setattr(self, computed_at_attr, datetime.now(timezone.utc))
            else:
                setattr(self, cache_attr, normalize_to_id(value))
                setattr(self, computed_at_attr, datetime.now(timezone.utc))

        @prop.deleter
        def prop(self):
            session = _require_session(self)
            if is_list_fk:
                setattr(self, list_assoc_attr, [])
            else:
                setattr(self, cache_attr, None)
            setattr(self, computed_at_attr, None)
            session.flush()

        return prop


def materialized_property(
    fn: Callable[..., Any] | None = None,
    *,
    in_transaction: bool = True,
):
    """Create a materialized property.

    Supports:
    - @materialized_property
    - @materialized_property(in_transaction=False)
    - value = materialized_property(compute)
    """

    config = _MaterializedConfig(in_transaction=in_transaction)

    if fn is None:
        def wrapper(f):
            return _MaterializedPropertyDescriptor(f, config)

        return wrapper

    return _MaterializedPropertyDescriptor(fn, config)
