from __future__ import annotations

from typing import Callable

import sqlalchemy as sa

from .dialects.sqlite import sqlite_similarity_expr


class VectorIndexedString(sa.TypeDecorator):
    """A String column type with a `.similarity_with(text)` comparator method.

    This does **not** change how the value is stored (still TEXT/VARCHAR). It
    only adds a comparator hook and stores configuration used by the
    `vector_indexing` event listeners.
    """

    impl = sa.String
    cache_ok = True

    def __init__(self, *, embedder: Callable[[str], object]):
        super().__init__()
        self.embedder = embedder
        # Filled later by as_model once the mapped class exists.
        self._vec_table: str | None = None

    class comparator_factory(sa.String.comparator_factory):
        def similarity_with(self, text: str, *, k: int = 50):
            """Return a SQL expression yielding a similarity score.

            The score is in `[0, 1]` (approximately) for cosine distance via
            `1.0 - distance`.
            """

            col = self.expr
            typ: VectorIndexedString = col.type  # type: ignore[assignment]
            if typ._vec_table is None:  # pragma: no cover
                raise RuntimeError(
                    "vector_indexing: internal error (vec table name not configured)"
                )

            pk_cols = list(col.table.primary_key.columns)
            if len(pk_cols) != 1:
                raise ValueError(
                    "vector_indexing: only single-column primary keys are supported"
                )
            pk_col = pk_cols[0]

            return sqlite_similarity_expr(
                source_col=col,
                pk_col=pk_col,
                vec_table=typ._vec_table,
                embedder=typ.embedder,
                query=text,
                k=k,
            )


__all__ = [
    "VectorIndexedString",
]
