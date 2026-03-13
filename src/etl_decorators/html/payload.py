"""Payload normalization for :pymod:`etl_decorators.html`.

The :func:`etl_decorators.html.parse_html` decorator supports returning:
- file path (str/pathlib.Path)
- raw HTML string
- file-like object with .read() returning str or bytes
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


def _read_html_payload(payload: Any) -> str:
    """Normalize function output to a HTML string."""

    # 1) pathlib.Path
    if isinstance(payload, Path):
        return payload.read_text(encoding="utf-8", errors="replace")

    # 2) str (either a file path or HTML)
    if isinstance(payload, str):
        p = Path(payload)
        if p.exists() and p.is_file():
            return p.read_text(encoding="utf-8", errors="replace")
        return payload

    # 3) file-like
    read = getattr(payload, "read", None)
    if callable(read):
        data = read()
        if isinstance(data, bytes):
            return data.decode("utf-8", errors="replace")
        if isinstance(data, str):
            return data
        raise TypeError(
            "parse_html-decorated function returned a file-like object whose read() "
            f"returned {type(data)!r}; expected str or bytes"
        )

    raise TypeError(
        "parse_html-decorated function must return a file path, an HTML string, "
        "or a file-like object with .read(). "
        f"Received: {type(payload)!r}"
    )
