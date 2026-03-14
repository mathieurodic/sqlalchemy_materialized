"""Row iteration helpers."""

from __future__ import annotations

from typing import Any, Iterator

from .scalar import _parse_scalar


def _iter_dict_rows(
    df: Any,
    *,
    auto_datetime: bool,
) -> Iterator[dict[str, Any]]:
    """Yield rows from a DataFrame-like object as dictionaries.

    We purposely avoid `DataFrame.to_dict("records")` to not materialize the
    full list.
    """

    columns = list(getattr(df, "columns"))
    for _idx, row in getattr(df, "iterrows")():
        out: dict[str, Any] = {}
        for col in columns:
            out[str(col)] = _parse_scalar(row[col], auto_datetime=auto_datetime)
        yield out


def _iter_tuple_rows(
    df: Any,
    *,
    auto_datetime: bool,
) -> Iterator[tuple[Any, ...]]:
    """Yield rows from a DataFrame-like object as tuples.

    We use `itertuples(index=False, name=None)` which is generally faster than
    `iterrows()` and does not allocate intermediate dicts.
    """

    it = getattr(df, "itertuples")(index=False, name=None)
    for row in it:
        # Row is already a tuple, but may contain numpy scalar types.
        yield tuple(_parse_scalar(v, auto_datetime=auto_datetime) for v in row)
