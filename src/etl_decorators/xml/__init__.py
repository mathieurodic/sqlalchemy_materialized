"""XML parsing helpers.

This subpackage provides small helpers used in ETL pipelines dealing with XML.

Optional dependency:

    pip install etl-decorators[xml]

The main entrypoint is :func:`etl_decorators.xml.parse_xml`.
"""

from __future__ import annotations

# Public API
from .decorator import parse_xml

# Expose internals for unit tests (mirrors csv/html style in this repo)
from .payload import _read_xml_payload

__all__ = [
    "parse_xml",
    "_read_xml_payload",
]
