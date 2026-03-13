"""Payload normalization helpers.

The :func:`etl_decorators.csv.parse_csv` decorator accepts multiple kinds of
return values (path, raw CSV text, file-like). This module normalizes them into
CSV text.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any


def _read_csv_payload(payload: Any) -> tuple[str, str | None]:
    """Normalize a payload to a UTF-8 CSV string.

    Supported inputs:
    - file path (str/pathlib.Path)
    - raw CSV string
    - file-like object with .read() returning str or bytes

    Returns:
        (text, source_name)
        source_name is used only for better error messages.
    """

    # 1) pathlib.Path
    if isinstance(payload, Path):
        return payload.read_text(encoding="utf-8", errors="replace"), str(payload)

    # 2) str (either a file path or csv content)
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
            "parse_csv-decorated function returned a file-like object whose read() "
            f"returned {type(data)!r}; expected str or bytes"
        )

    raise TypeError(
        "parse_csv-decorated function must return a file path, a CSV string, "
        "or a file-like object with .read(). "
        f"Received: {type(payload)!r}"
    )
