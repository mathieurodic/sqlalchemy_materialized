from __future__ import annotations

import weakref
from typing import Any

from pydantic import BaseModel
from sqlalchemy.ext.mutable import Mutable
from sqlalchemy.orm.attributes import flag_modified


_PARENT_BY_MODEL: "weakref.WeakKeyDictionary[BaseModel, tuple[Any, str]]" = (
    weakref.WeakKeyDictionary()
)


class _MutablePydanticModelList(Mutable, list):
    """Mutable list of pydantic models.

    The dirty-tracking model relies on SQLAlchemy's `Mutable` support to mark
    parent ORM attributes as modified on list-level operations.
    """

    @classmethod
    def coerce(cls, key, value):  # noqa: ANN001
        if value is None:
            return None
        if isinstance(value, _MutablePydanticModelList):
            return value
        if isinstance(value, list):
            out = _MutablePydanticModelList()
            out.extend(value)
            return out
        return value

    def _wrap(self, item: Any):
        # Keep items as real BaseModel instances.
        return item

    def append(self, item: Any) -> None:  # type: ignore[override]
        super().append(self._wrap(item))
        self.changed()

    def extend(self, items) -> None:  # noqa: ANN001
        super().extend([self._wrap(i) for i in items])
        self.changed()

    def __setitem__(self, idx, item):  # noqa: ANN001
        super().__setitem__(idx, self._wrap(item))
        self.changed()

    def insert(self, idx: int, item: Any) -> None:
        super().insert(idx, self._wrap(item))
        self.changed()

    def unwrap(self) -> list[BaseModel]:
        return list(self)


def _instrument_model_cls(model_cls: type[BaseModel]) -> None:
    """Install a `__setattr__` hook that dirties the owning ORM attribute.

    This modifies the model class in-place, but only once.
    """

    if getattr(model_cls, "__etl_decorators_sa_instrumented__", False):
        return

    orig_setattr = model_cls.__setattr__

    def __setattr__(self, name: str, value: Any) -> None:  # noqa: ANN001
        orig_setattr(self, name, value)
        parent = getattr(self, "_sa_mutable_parent", None)
        if parent is None:
            # Fallback for models that reject arbitrary attributes.
            try:
                parent = _PARENT_BY_MODEL.get(self)
            except TypeError:
                parent = None
        if parent is None:
            return
        try:
            obj, attr = parent
            flag_modified(obj, attr)
        except Exception:
            # Best-effort; if SQLAlchemy isn't configured / object detached.
            pass

    model_cls.__setattr__ = __setattr__  # type: ignore[assignment]
    setattr(model_cls, "__etl_decorators_sa_instrumented__", True)


def _attach_parent(value: BaseModel, obj: Any, attr: str) -> None:
    """Attach parent pointer used by the setattr hook."""

    try:
        object.__setattr__(value, "_sa_mutable_parent", (obj, attr))
    except Exception:
        # Some BaseModel configs may disallow setattr; ignore.
        pass

    # Also keep a weakref-based mapping as a robust fallback.
    try:
        _PARENT_BY_MODEL[value] = (obj, attr)
    except TypeError:
        # If the value can't be weakref'd (rare), skip.
        pass


def _attach_parent_list(values: list[BaseModel], obj: Any, attr: str) -> None:
    for v in values:
        _attach_parent(v, obj, attr)
