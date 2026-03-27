from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Callable, Union

if TYPE_CHECKING:  # pragma: no cover
    import numpy as np


Embedding = Union[list[float], tuple[float, ...], "np.ndarray"]


class _Missing:
    pass


_MISSING = _Missing()


@dataclass(frozen=True, slots=True)
class _Field:
    default: Any = _MISSING
    default_factory: Callable[..., Any] | None = None
    column_kwargs: dict[str, Any] | None = None
    index_embedding_using: Callable[[str], Embedding] | None = None


def field(
    *,
    default: Any = _MISSING,
    default_factory: Callable[..., Any] | None = None,
    index_embedding_using: Callable[[str], Embedding] | None = None,
    **column_kwargs: Any,
) -> _Field:
    """Declare per-field configuration for `as_model`.

    Parameters
    ----------
    default:
        Default value used when the user does not pass this field to `__init__`.
    default_factory:
        Callable used to create a default value.

        The callable may accept either:
        - 0 arguments, or
        - 1 argument (self)

        The resulting value is assigned in the generated model's `__init__`.
    **column_kwargs:
        Extra keyword arguments forwarded to SQLAlchemy `mapped_column(...)`.
        This can be used for `index=True`, `unique=True`, `comment=...`,
        `server_default=...`, etc.

    index_embedding_using:
        Optional callable enabling vector indexing for this field.

        When provided, `as_model` (SQLite only for now) will:
        - create a sqlite-vec virtual table
        - keep it updated on INSERT and UPDATE
        - expose `Model.<field>.similarity_with(text)` for ordering/filtering
    """

    if default is not _MISSING and default_factory is not None:
        raise TypeError("field(): 'default' and 'default_factory' are mutually exclusive")

    return _Field(
        default=default,
        default_factory=default_factory,
        column_kwargs=column_kwargs or None,
        index_embedding_using=index_embedding_using,
    )
