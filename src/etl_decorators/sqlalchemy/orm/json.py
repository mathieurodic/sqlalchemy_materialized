"""Project-level JSON type.

This is mostly a convenience alias so user code can annotate with `JSON`
without importing SQLAlchemy directly.
"""

from __future__ import annotations

import sqlalchemy as sa


# Public alias
JSON = sa.JSON

__all__ = [
    "JSON",
]
