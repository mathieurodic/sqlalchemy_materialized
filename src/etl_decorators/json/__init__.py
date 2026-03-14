"""JSON parsing helpers.

This subpackage provides small decorators to normalize JSON-shaped payloads.

Unlike some other subpackages, this one uses only the Python standard library
(`json`) and is part of the base package (no optional extra).

Public API:

- :func:`etl_decorators.json.parse_json`
- :func:`etl_decorators.json.parse_jsonl`
"""

from __future__ import annotations

# Public API
from .parse_json import parse_json
from .parse_jsonl import parse_jsonl

# Expose internals for unit tests (mirrors csv/html subpackages style)
from .payload import _read_json_payload, _iter_jsonl_payload

__all__ = [
    "parse_json",
    "parse_jsonl",
    # Expose internals for unit tests
    "_read_json_payload",
    "_iter_jsonl_payload",
]
