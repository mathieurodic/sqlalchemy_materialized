"""Logging helpers.

This subpackage provides a small decorator to standardize logging around ETL
steps.

The main entrypoint is :func:`etl_decorators.logging.log`.
"""

from __future__ import annotations

from .decorator import log


__all__ = [
    "log",
]
