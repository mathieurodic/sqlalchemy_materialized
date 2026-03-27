from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

from .dialects.sqlite import install_sqlite_vector_indexing


@dataclass(frozen=True, slots=True)
class VectorIndexSpec:
    model_cls: type
    column_name: str
    embedder: Callable[[str], object]


def register_vector_index(*, model_cls: type, column_name: str, embedder: Callable[[str], object]) -> None:
    """Register vector indexing for a given mapped model column.

    This installs SQLAlchemy event hooks (DDL + DML) on the model.
    """

    # Dialect dispatch is intentionally minimal for now.
    install_sqlite_vector_indexing(VectorIndexSpec(model_cls, column_name, embedder))


__all__ = [
    "VectorIndexSpec",
    "register_vector_index",
]
