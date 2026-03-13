"""CSV parsing helpers.

This subpackage provides a small decorator to turn CSV outputs into iterables
of typed dictionaries.

Optional dependency:

    pip install etl-decorators[csv]

The main entrypoint is :func:`etl_decorators.csv.parse_csv`.
"""

from __future__ import annotations

# Public API
from .decorator import parse_csv

# Expose internals for unit tests (mirrors html subpackage style)
from .payload import _read_csv_payload
from .dialect import _sniff_csv_dialect
from .scalar import _parse_scalar


__all__ = [
    "parse_csv",
    # Expose internals for unit tests (mirrors html subpackage style)
    "_read_csv_payload",
    "_sniff_csv_dialect",
    "_parse_scalar",
]

