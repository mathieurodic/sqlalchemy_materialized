"""Vector indexing integration for `etl_decorators.sqlalchemy`.

Currently only supports SQLite via the `sqlite-vec` extension.
"""

from .registry import register_vector_index

__all__ = [
    "register_vector_index",
]
