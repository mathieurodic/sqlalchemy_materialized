"""Stream / iterator helpers.

This subpackage provides decorators that transform iterables returned by ETL
steps.

The main entrypoints are:

- :func:`etl_decorators.streams.batch`
- :func:`etl_decorators.streams.transform`
- :func:`etl_decorators.streams.keep`
- :func:`etl_decorators.streams.dedupe`
"""

from __future__ import annotations

from .batch import batch
from .dedupe import dedupe
from .keep import keep
from .transform import transform


__all__ = [
    "batch",
    "transform",
    "keep",
    "dedupe",
]
