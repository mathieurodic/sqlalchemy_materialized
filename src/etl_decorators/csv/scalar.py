"""Scalar type inference.

`pandas.read_csv` may keep values as strings (object dtype), especially when a
column contains mixed types. This module provides a small best-effort coercion
layer applied per-cell.
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any


_TRUE = {"true", "t", "yes", "y", "1"}
_FALSE = {"false", "f", "no", "n", "0"}


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DT_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(?:\.\d{1,6})?)?$")
_DT_ISO_Z_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?Z$"
)


def _parse_scalar(value: Any, *, auto_datetime: bool) -> Any:
    """Best-effort conversion for a scalar value.

    This is applied after pandas has read the CSV. It mainly covers:
    - pandas may keep columns as object (string) when values are mixed
    - datetimes aren't inferred consistently without parse_dates
    """

    if value is None:
        return None

    # numpy/pandas missing values often stringify to 'nan'/'NaT'; keep None.
    try:
        # pandas uses NaN where NaN != NaN
        if value != value:  # noqa: PLR0124
            return None
    except Exception:
        pass

    # If pandas already gave a non-string type, keep it.
    if not isinstance(value, str):
        return value

    s = value.strip()
    if s == "":
        return None

    low = s.lower()
    if low in _TRUE:
        return True
    if low in _FALSE:
        return False

    # int/float
    try:
        if re.fullmatch(r"[-+]?\d+", s):
            return int(s)
    except Exception:
        pass
    try:
        # covers scientific notation too
        if re.fullmatch(r"[-+]?(?:\d+\.\d*|\d*\.\d+|\d+)(?:[eE][-+]?\d+)?", s):
            return float(s)
    except Exception:
        pass

    if auto_datetime:
        # Quick heuristics to avoid turning arbitrary strings into datetimes.
        # We accept ISO-like formats.
        try:
            if _DATE_RE.match(s):
                return datetime.fromisoformat(s)
            if _DT_RE.match(s):
                return datetime.fromisoformat(s.replace(" ", "T", 1))
            if _DT_ISO_Z_RE.match(s):
                # Python's fromisoformat doesn't accept trailing 'Z'
                return datetime.fromisoformat(s[:-1] + "+00:00")
        except Exception:
            # If parsing fails, fall back to raw string.
            return value

    return value
