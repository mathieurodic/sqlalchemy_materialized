from sqlalchemy.ext.hybrid import hybrid_property
import sqlalchemy as sa
from datetime import datetime, timezone
from sqlalchemy import inspect as sa_inspect
from sqlalchemy.orm import object_session
from typing import get_args, get_origin

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


class materialized_property:
    def __init__(self, fn):
        self.fn = fn
        self.cache_attr = f"_{fn.__name__}"
        self.computed_at_attr = f"_{fn.__name__}__computed_at"
        return_ann = fn.__annotations__.get("return")
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
        # Inject the backing DB column into the class
        col = make_sa_column(name, self.return_type)
        setattr(owner, self.cache_attr, col)

        # Inject the computed-at column into the class
        computed_at_col = sa.orm.mapped_column(
            f"{name}__computed_at",
            sa.DateTime,
            nullable=True,
        )
        setattr(owner, self.computed_at_attr, computed_at_col)

        # Build and inject the hybrid property, replacing ourselves
        setattr(owner, name, self._make_hybrid())

    def _make_hybrid(self):
        cache_attr = self.cache_attr
        computed_at_attr = self.computed_at_attr
        compute_fn = self.fn
        is_fk = self._is_fk
        is_list = self._is_list
        target_cls = self._list_item_type if is_list else self.return_type

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
            # If we receive a model instance, store its PK.
            if isinstance(value, target_cls):
                return _pk_id_from_instance(value)
            # Otherwise, assume it's already an identifier (PK).
            return value

        def resolve_from_id(self, ident):
            if not is_fk:
                return ident
            if ident is None:
                return None
            session = object_session(self)
            if session is None:
                if is_list:
                    raise RuntimeError(
                        "materialized_property: cannot resolve list[MappedClass] without a session "
                        "(detached instance)."
                    )
                # scalar FK: keep backwards compatible behavior
                return ident

            if is_list:
                if not isinstance(ident, list):
                    raise TypeError(
                        "materialized_property: backing value should be a list of identifiers, "
                        f"received: {type(ident)!r}"
                    )
                return [session.get(target_cls, i) for i in ident]

            return session.get(target_cls, ident)

        @hybrid_property
        def prop(self):
            computed_at = getattr(self, computed_at_attr)
            if computed_at is None:
                session = object_session(self)
                if session is None:
                    raise RuntimeError(
                        "materialized_property: cannot compute the property without a session "
                        "(detached instance)."
                    )

                old_cache = getattr(self, cache_attr)
                old_computed_at = getattr(self, computed_at_attr)

                try:
                    with session.begin_nested():
                        computed = compute_fn(self)
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
            return resolve_from_id(self, getattr(self, cache_attr))

        @prop.expression
        def prop(cls):  # type: ignore[no-redef]
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
            setattr(self, cache_attr, normalize_to_id(value))
            setattr(self, computed_at_attr, datetime.now(timezone.utc))

        @prop.deleter
        def prop(self):
            setattr(self, cache_attr, None)
            setattr(self, computed_at_attr, None)
            session = object_session(self)
            if session:
                session.flush()

        return prop
