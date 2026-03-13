"""HTML parsing helpers.

This subpackage provides small helpers used in ETL pipelines dealing with HTML.

Optional dependency:

    pip install etl-decorators[html]

The main entrypoint is :func:`etl_decorators.html.parse_html`.
"""

from __future__ import annotations

# Public API
from .decorator import parse_html

# Expose internals for unit tests (mirrors csv/html style in this repo)
from .payload import _read_html_payload


__all__ = [
    "parse_html",
    "_read_html_payload",
]
