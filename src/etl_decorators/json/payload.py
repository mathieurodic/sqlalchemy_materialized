"""Payload normalization for :pymod:`etl_decorators.json`.

The JSON decorators accept multiple kinds of return values:

- file path (str/pathlib.Path)
- raw JSON / JSONL string
- file-like object

This module provides normalization helpers similar to
:mod:`etl_decorators.csv.payload` and :mod:`etl_decorators.html.payload`.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from typing import Any, TextIO, cast


def _read_json_payload(payload: Any) -> tuple[str, str | None]:
    """Normalize a payload to a text string.

    Supported inputs:
    - file path (str/pathlib.Path)
    - raw JSON string
    - file-like object with `.read()` returning str or bytes

    Returns:
        (text, source_name)

        `source_name` is used only for better error messages.
    """

    # 1) pathlib.Path
    if isinstance(payload, Path):
        return payload.read_text(encoding="utf-8", errors="replace"), str(payload)

    # 2) str (either a file path or JSON content)
    if isinstance(payload, str):
        p = Path(payload)
        if p.exists() and p.is_file():
            return p.read_text(encoding="utf-8", errors="replace"), str(p)
        return payload, None

    # 3) file-like
    read = getattr(payload, "read", None)
    if callable(read):
        data = read()
        if isinstance(data, bytes):
            return data.decode("utf-8", errors="replace"), getattr(payload, "name", None)
        if isinstance(data, str):
            return data, getattr(payload, "name", None)
        raise TypeError(
            "parse_json-decorated function returned a file-like object whose read() "
            f"returned {type(data)!r}; expected str or bytes"
        )

    raise TypeError(
        "parse_json-decorated function must return a file path, a JSON string, "
        "or a file-like object with .read(). "
        f"Received: {type(payload)!r}"
    )


def _iter_jsonl_payload(payload: Any) -> tuple[Iterator[str], str | None]:
    """Normalize a payload to an iterator of text lines.

    Supported inputs:
    - file path (str/pathlib.Path)
    - raw JSONL string (split with `splitlines()`)
    - text/binary file-like object

    This helper aims to be *lazy* for file paths and file-like objects.

    Returns:
        (lines_iterator, source_name)

    Notes:
        When a file path is provided, this function opens the file and the
        returned iterator keeps it open until exhaustion.
    """

    # 1) pathlib.Path
    if isinstance(payload, Path):
        f = payload.open("r", encoding="utf-8", errors="replace")
        return _iter_and_close(f), str(payload)

    # 2) str (either a file path or JSONL content)
    if isinstance(payload, str):
        p = Path(payload)
        if p.exists() and p.is_file():
            f = p.open("r", encoding="utf-8", errors="replace")
            return _iter_and_close(f), str(p)

        # in-memory string: already loaded => splitlines is fine
        return (ln for ln in payload.splitlines()), None

    # 3) file-like
    # Prefer iterating lines to preserve laziness; do NOT .read() the whole file.
    if hasattr(payload, "__iter__"):
        name = getattr(payload, "name", None)

        def _line_iter() -> Iterator[str]:
            for ln in payload:
                if isinstance(ln, bytes):
                    yield ln.decode("utf-8", errors="replace")
                elif isinstance(ln, str):
                    yield ln
                else:
                    raise TypeError(
                        "parse_jsonl-decorated function returned a file-like object "
                        "iterating non str/bytes lines; "
                        f"got {type(ln)!r}"
                    )

        return _line_iter(), cast(str | None, name)

    # Fallback: file-like with .read() but no iterator
    read = getattr(payload, "read", None)
    if callable(read):
        # We can't be fully lazy, but keep behavior defined.
        data = read()
        if isinstance(data, bytes):
            text = data.decode("utf-8", errors="replace")
            return (ln for ln in text.splitlines()), getattr(payload, "name", None)
        if isinstance(data, str):
            return (ln for ln in data.splitlines()), getattr(payload, "name", None)
        raise TypeError(
            "parse_jsonl-decorated function returned a file-like object whose read() "
            f"returned {type(data)!r}; expected str or bytes"
        )

    raise TypeError(
        "parse_jsonl-decorated function must return a file path, a JSONL string, "
        "or a file-like object. "
        f"Received: {type(payload)!r}"
    )


def _iter_and_close(f: TextIO) -> Iterator[str]:
    try:
        for ln in f:
            yield ln
    finally:
        try:
            f.close()
        except Exception:
            # best-effort close
            pass
