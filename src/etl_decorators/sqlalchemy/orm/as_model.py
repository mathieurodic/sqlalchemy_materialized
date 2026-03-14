from __future__ import annotations

import inspect
import sys
from typing import Any, Callable, get_type_hints

import sqlalchemy as sa
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Mapped

from ..materialized.helpers import _is_mapped_class
from ..utils.typing import unwrap_optional
from .columns import make_sa_column
from .field import _Field, _MISSING


def _default_tablename(cls: type) -> str:
    return f"{cls.__name__.lower()}s"


def _plural_attr_name(cls: type) -> str:
    # Keep it intentionally simple and predictable.
    return f"{cls.__name__.lower()}s"


def _resolve_hints(cls: type) -> dict[str, Any]:
    return _resolve_hints_with_extra(cls)


def _resolve_hints_with_extra(cls: type, extra_localns: dict[str, Any] | None = None) -> dict[str, Any]:
    """Resolve `__annotations__`, best-effort.

    In particular, this supports `from __future__ import annotations` and
    classes defined in local scopes (e.g. inside a function), by merging the
    caller frame locals into `localns`.
    """

    module = sys.modules.get(cls.__module__)
    globalns = vars(module) if module is not None else {}
    localns: dict[str, Any] = {}
    if extra_localns:
        localns.update(extra_localns)
    localns.update(dict(vars(cls)))
    return get_type_hints(cls, globalns=globalns, localns=localns)


def as_model(Base: type, tablename: str | None = None):
    """Decorator turning a plain annotated class into a SQLAlchemy model.

    See `doc/as_model.md` for user documentation.
    """

    def _decorate(cls: type):
        frame = inspect.currentframe()
        caller_locals = frame.f_back.f_locals if frame and frame.f_back else None
        hints = _resolve_hints_with_extra(cls, dict(caller_locals) if caller_locals else None)

        mapped_annotations: dict[str, Any] = {}

        # Start from original namespace so methods/helpers carry over.
        namespace: dict[str, Any] = {
            k: v
            for k, v in cls.__dict__.items()
            if k not in ("__dict__", "__weakref__", "__annotations__")
        }

        namespace["__tablename__"] = tablename or _default_tablename(cls)
        namespace["__allow_unmapped__"] = True
        namespace["__module__"] = cls.__module__

        # Always add an integer PK unless the user already defined one.
        if "id" not in namespace:
            mapped_annotations["id"] = Mapped[int]
            namespace["id"] = sa.orm.mapped_column(sa.Integer, primary_key=True)

        defaults: dict[str, Any] = {}
        factories: dict[str, Callable[..., Any]] = {}
        required_relationships: set[str] = set()

        def _field_info(name: str) -> tuple[Any, dict[str, Any]]:
            """Return (default_marker/value, column_kwargs)."""
            v = cls.__dict__.get(name, _MISSING)

            if isinstance(v, _Field):
                if v.default is not _MISSING:
                    defaults[name] = v.default
                if v.default_factory is not None:
                    factories[name] = v.default_factory
                return _MISSING, dict(v.column_kwargs or {})

            if v is not _MISSING:
                defaults[name] = v
            return _MISSING, {}

        # First pass: inject columns + forward relationships.
        rel_specs: list[tuple[str, type, bool, dict[str, Any]]] = []
        for name, ann in hints.items():
            # Skip private-ish names.
            if name.startswith("_"):
                continue

            # Determine Optional-ness early.
            inner, is_optional = unwrap_optional(ann)

            _unused, col_kwargs = _field_info(name)

            # Relationship to mapped class.
            if isinstance(inner, type) and _is_mapped_class(inner):
                fk_attr = f"{name}_id"
                mapped_annotations[fk_attr] = (
                    Mapped[int | None] if is_optional else Mapped[int]
                )
                namespace[fk_attr] = make_sa_column(
                    fk_attr,
                    inner,
                    nullable=is_optional,
                    **col_kwargs,
                )

                reverse_attr = _plural_attr_name(cls)
                namespace[name] = relationship(
                    inner,
                    back_populates=reverse_attr,
                )
                rel_specs.append((name, inner, is_optional, {"reverse": reverse_attr}))
                if not is_optional:
                    required_relationships.add(name)
                continue

            # Scalar/list/pydantic/sqlalchemy type column.
            mapped_annotations[name] = Mapped[ann]
            namespace[name] = make_sa_column(
                name,
                ann,
                nullable=None,  # infer from Optional[T]
                **col_kwargs,
            )

        namespace["__annotations__"] = {
            **getattr(cls, "__annotations__", {}),
            **mapped_annotations,
        }

        # Create the mapped class.
        model_cls = type(cls.__name__, (Base,), namespace)

        # Second pass: inject reverse relationships onto targets.
        for name, target_cls, _is_optional, spec in rel_specs:
            reverse_attr = spec["reverse"]
            if hasattr(target_cls, reverse_attr):
                continue

            setattr(
                target_cls,
                reverse_attr,
                relationship(
                    model_cls,
                    back_populates=name,
                ),
            )

        # Wrap __init__ to apply defaults and enforce required relationships.
        base_init = getattr(model_cls, "__init__", None)

        def _call_factory(factory: Callable[..., Any], self):
            try:
                sig = inspect.signature(factory)
                params = [
                    p
                    for p in sig.parameters.values()
                    if p.kind
                    in (
                        inspect.Parameter.POSITIONAL_ONLY,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                    )
                ]
                if len(params) == 0:
                    return factory()
                if len(params) == 1:
                    return factory(self)
                raise TypeError
            except (TypeError, ValueError):
                # Fallback: attempt 0-arg then 1-arg.
                try:
                    return factory()
                except TypeError:
                    return factory(self)

        def __init__(self, **kwargs):  # type: ignore[no-redef]
            if base_init is not None:
                base_init(self, **kwargs)
            else:  # pragma: no cover
                super(model_cls, self).__init__(**kwargs)

            # Apply explicit defaults/factories only when the caller didn't
            # provide the value.
            for k, v in defaults.items():
                if k not in kwargs and getattr(self, k, _MISSING) in (_MISSING, None):
                    setattr(self, k, v)

            for k, f in factories.items():
                if k not in kwargs and getattr(self, k, _MISSING) in (_MISSING, None):
                    setattr(self, k, _call_factory(f, self))

            # Enforce non-Optional relationships.
            for rel_name in required_relationships:
                if getattr(self, rel_name, None) is None:
                    raise TypeError(
                        f"{model_cls.__name__}.__init__: '{rel_name}' is required (annotation is not Optional)"
                    )

        model_cls.__init__ = __init__  # type: ignore[assignment]

        # Ensure the decorated name resolves to the mapped class.
        return model_cls

    return _decorate


__all__ = [
    "as_model",
]
