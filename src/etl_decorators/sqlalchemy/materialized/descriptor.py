from __future__ import annotations

from contextlib import nullcontext
from datetime import datetime, timezone
import logging
from typing import Any, Callable, get_args, get_origin, get_type_hints

import sqlalchemy as sa
from sqlalchemy.ext.hybrid import hybrid_property

from ..orm.columns import make_sa_column
from ..utils.typing import unwrap_optional
from .config import _MaterializedConfig
from .depends_on import setup_dependency_invalidation
from .helpers import _is_mapped_class, _pk_id_from_instance, _require_session
from .list_fk import inject_list_fk_storage


logger = logging.getLogger(__name__)


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

        # We need the return type, but on Python modules using
        # `from __future__ import annotations`, the return annotation is stored
        # as a string (e.g. "int") until resolved.
        #
        # We therefore compute the return type lazily in __set_name__, once we
        # know the owning class and can pass it as localns.
        self.return_type: Any | None = None
        self._is_optional: bool | None = None
        self._is_list: bool | None = None
        self._list_item_type: Any | None = None
        self._is_fk: bool | None = None

        # Preserve historical behavior: missing return annotations and invalid
        # unions should fail as soon as the decorator is applied.
        return_ann = fn.__annotations__.get("return")
        if return_ann is None:
            raise TypeError(
                f"materialized_property {fn.__name__} must have a return annotation"
            )

        # If the annotation is already a concrete typing object (i.e. not a
        # string forward-ref), we can validate it eagerly.
        if not isinstance(return_ann, str):
            self._set_return_type_from_annotation(return_ann)

    def _set_return_type_from_annotation(self, return_ann: Any) -> None:
        try:
            # Normalize Optional[T] / T | None so FK detection and isinstance() work.
            unwrapped, is_optional = unwrap_optional(return_ann)
        except TypeError as e:
            raise TypeError(
                f"Unsupported return annotation for materialized_property {self.fn.__name__}: {return_ann!r}"
            ) from e

        self.return_type = unwrapped
        self._is_optional = is_optional
        self._is_list = get_origin(unwrapped) is list
        if self._is_list:
            args = list(get_args(unwrapped))
            self._list_item_type = args[0] if args else object
            self._is_fk = _is_mapped_class(self._list_item_type)
        else:
            self._list_item_type = None
            self._is_fk = _is_mapped_class(self.return_type)

    def _resolve_return_type(self, owner: type) -> None:
        # Attempt to resolve forward refs / string annotations.
        try:
            hints = get_type_hints(self.fn, globalns=getattr(self.fn, "__globals__", None), localns=vars(owner))
            return_ann = hints.get("return")
        except Exception:
            return_ann = self.fn.__annotations__.get("return")

        if return_ann is None:
            raise TypeError(
                f"materialized_property {self.fn.__name__} must have a return annotation"
            )

        self._set_return_type_from_annotation(return_ann)

    def __set_name__(self, owner, name):
        # Resolve return type now that we know the owning class (needed for
        # string annotations when `from __future__ import annotations` is used).
        if self.return_type is None:
            self._resolve_return_type(owner)

        self._prop_name = name

        # 1) list[MappedClass] is stored via association table + relationship
        #    (not a JSON column).
        if self._is_list and self._is_fk:
            self._inject_list_fk_storage(owner)
        else:
            # Inject the backing DB column into the class
            # materialized_property backing columns must start nullable because
            # they are NULL until computed.
            col = make_sa_column(name, self.return_type, nullable=True)
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

        # 4) Optional automatic invalidation when dependencies change
        setup_dependency_invalidation(self, owner)

    def _inject_list_fk_storage(self, owner):
        inject_list_fk_storage(self, owner)

    def _make_hybrid(self, owner):
        cache_attr = self.cache_attr
        computed_at_attr = self.computed_at_attr
        compute_fn = self.fn
        in_transaction = self.config.in_transaction
        validate = self.config.validate
        is_fk = self._is_fk
        is_list = self._is_list
        is_optional = self._is_optional
        target_cls = self._list_item_type if is_list else self.return_type
        list_item_type = self._list_item_type
        list_assoc_attr = self._list_assoc_attr
        list_assoc_cls = self._list_assoc_cls
        is_list_fk = bool(is_list and is_fk and list_assoc_attr and list_assoc_cls)

        prop_name = self._prop_name or cache_attr

        def _pk_python_type(mapped_cls: type) -> type | None:
            """Best-effort extraction of PK python type for a mapped class."""
            try:
                mapper = sa.inspect(mapped_cls)
                pk_cols = list(mapper.primary_key)
                if len(pk_cols) != 1:
                    return None
                pk_col = pk_cols[0]
                typ = getattr(pk_col, "type", None)
                if typ is None:
                    return None
                # May raise NotImplementedError for some custom types.
                return typ.python_type  # type: ignore[attr-defined]
            except Exception:
                return None

        fk_pk_py_type: type | None = _pk_python_type(target_cls) if is_fk else None

        def _is_valid_identifier(v: object) -> bool:
            """Return True if v looks like an acceptable FK identifier.

            We intentionally always allow non-bool ints (per project requirement),
            even if the DB pk python type is not int.
            """
            if isinstance(v, bool):
                # bool is a subclass of int; almost never a valid PK identifier.
                return False

            if fk_pk_py_type is int:
                return isinstance(v, int)

            if fk_pk_py_type is not None and isinstance(fk_pk_py_type, type):
                try:
                    if isinstance(v, fk_pk_py_type):
                        return True
                except Exception:
                    pass

            return isinstance(v, int)

        def _raise_type(msg: str) -> None:
            raise TypeError(f"materialized_property {prop_name}: {msg}")

        def validate_value(value: Any) -> None:
            """Strict runtime validation for both compute outputs and setter inputs."""
            if not validate:
                return

            if value is None:
                if is_optional:
                    return
                _raise_type("None is not allowed (return annotation is not Optional)")

            if is_list:
                if not isinstance(value, list):
                    _raise_type(
                        f"expected a list for list[...] return type, received: {type(value)!r}"
                    )

                # FK-ish list: accept instances or identifiers.
                if is_fk:
                    for item in value:
                        if item is None:
                            _raise_type("list[...] does not accept None items")

                        if isinstance(item, target_cls):
                            # Fail early for transient instances.
                            _pk_id_from_instance(item)
                            continue

                        if _is_valid_identifier(item):
                            continue

                        _raise_type(
                            "list items must be mapped instances or identifiers; "
                            f"received: {type(item)!r}"
                        )
                    return

                # Non-FK list: enforce element type when it's a real Python type.
                item_t = list_item_type
                if item_t in (Any, object) or not isinstance(item_t, type):
                    return
                for item in value:
                    if item is None:
                        _raise_type("list[...] does not accept None items")
                    if not isinstance(item, item_t):
                        _raise_type(
                            f"list items must be {item_t!r}, received: {type(item)!r}"
                        )
                return

            # Scalar
            if is_fk:
                if isinstance(value, target_cls):
                    _pk_id_from_instance(value)
                    return
                if _is_valid_identifier(value):
                    return
                _raise_type(
                    "expected a mapped instance or identifier, "
                    f"received: {type(value)!r}"
                )
                # _raise_type always raises; keep this block explicit for
                # readability.

            # Non-FK scalar
            rt = self.return_type
            if rt in (Any, object) or not isinstance(rt, type):
                return
            if not isinstance(value, rt):
                _raise_type(f"expected {rt!r}, received: {type(value)!r}")

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
                        validate_value(computed)

                        if is_list_fk:
                            targets = normalize_list_fk_to_instances(self, computed)
                            assoc_rows = [
                                list_assoc_cls(pos=i, target=t)
                                for i, t in enumerate(targets)
                            ]
                            setattr(self, list_assoc_attr, assoc_rows)
                            setattr(self, computed_at_attr, datetime.now(timezone.utc))
                            session.flush()
                        else:
                            normalized = normalize_to_id(computed)
                            setattr(self, cache_attr, normalized)
                            setattr(self, computed_at_attr, datetime.now(timezone.utc))
                            session.flush()
                except Exception as e:
                    logger.error(
                        "materialized_property compute failed (%s): %s",
                        e.__class__.__name__,
                        str(e),
                    )
                    logger.debug(
                        "materialized_property compute traceback",
                        exc_info=True,
                    )

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
            validate_value(value)
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
