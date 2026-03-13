"""CSV parsing helpers.

This subpackage provides a small decorator to turn CSV outputs into iterables
of typed dictionaries.

Optional dependency:

    pip install etl-decorators[csv]

The main entrypoint is :func:`etl_decorators.csv.parse_csv`.
"""

from __future__ import annotations

import csv as _csv
import inspect
import io
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Iterable, Iterator, Mapping, ParamSpec, overload


P = ParamSpec("P")


def _require_pandas():
    try:
        import pandas  # noqa: F401
    except Exception as e:  # pragma: no cover
        raise RuntimeError(
            "pandas is required for etl_decorators.csv. Install with: "
            "pip install etl-decorators[csv]"
        ) from e


def _read_csv_payload(payload: Any) -> tuple[str, str | None]:
    """Normalize function output to a CSV string.

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


_TRUE = {"true", "t", "yes", "y", "1"}
_FALSE = {"false", "f", "no", "n", "0"}


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_DT_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2}(?:\.\d{1,6})?)?$"
)
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


@dataclass(frozen=True)
class _DialectParams:
    delimiter: str | None
    quotechar: str | None
    doublequote: bool | None
    quoting: int | None


def _sniff_csv_dialect(
    text: str,
    *,
    delimiter: str | None,
    quotechar: str | None,
    doublequote: bool | None,
    quoting: int | None,
) -> _DialectParams:
    """Return effective csv dialect parameters.

    If `delimiter` is None, use csv.Sniffer on a small sample.
    Explicit parameters always win over sniffed values.
    """

    if delimiter is not None:
        # user forced delimiter: don't sniff
        return _DialectParams(
            delimiter=delimiter,
            quotechar=quotechar,
            doublequote=doublequote,
            quoting=quoting,
        )

    sample = text[:64_000]
    try:
        dialect = _csv.Sniffer().sniff(sample, delimiters=[",", ";", "\t", "|", ":"])
    except Exception:
        # fallback to comma
        dialect = _csv.get_dialect("excel")

    return _DialectParams(
        delimiter=getattr(dialect, "delimiter", ","),
        quotechar=quotechar if quotechar is not None else getattr(dialect, "quotechar", '"'),
        doublequote=doublequote
        if doublequote is not None
        else getattr(dialect, "doublequote", True),
        quoting=quoting if quoting is not None else getattr(dialect, "quoting", _csv.QUOTE_MINIMAL),
    )


def _iter_dict_rows(
    df: Any,
    *,
    auto_datetime: bool,
) -> Iterator[dict[str, Any]]:
    # DataFrame.to_dict("records") exists but materializes the whole list.
    columns = list(getattr(df, "columns"))
    for _idx, row in getattr(df, "iterrows")():
        out: dict[str, Any] = {}
        for col in columns:
            out[str(col)] = _parse_scalar(row[col], auto_datetime=auto_datetime)
        yield out


@overload
def parse_csv(fn: Callable[P, Any]) -> Callable[P, Iterator[dict[str, Any]]]: ...


@overload
def parse_csv(
    fn: None = None,
    *,
    delimiter: str | None = None,
    quotechar: str | None = None,
    doublequote: bool | None = None,
    quoting: int | None = None,
    auto_datetime: bool = True,
    chunksize: int | None = None,
    pandas_kwargs: Mapping[str, Any] | None = None,
) -> Callable[[Callable[P, Any]], Callable[P, Iterator[dict[str, Any]]]]: ...


def parse_csv(
    fn: Callable[P, Any] | None = None,
    *,
    delimiter: str | None = None,
    quotechar: str | None = None,
    doublequote: bool | None = None,
    quoting: int | None = None,
    auto_datetime: bool = True,
    chunksize: int | None = None,
    pandas_kwargs: Mapping[str, Any] | None = None,
):
    """Decorator that parses CSV outputs into an iterator of dict rows.

    The decorated function can return:
    - a file path (str/pathlib.Path)
    - a CSV string
    - a file-like resource (implements `.read()`)

    The decorated callable returns an iterator of dicts, one per row, where keys
    are taken from the header row.

    Parameters:
        delimiter: force a delimiter (when None, delimiter is sniffed).
        quotechar/doublequote/quoting: CSV quoting options. Explicit values
            override sniffed ones.
        auto_datetime: when True, tries to convert ISO-like date/datetime
            strings to `datetime`.
        chunksize: if provided, uses pandas chunked reading and yields rows
            incrementally.
        pandas_kwargs: additional keyword args forwarded to `pandas.read_csv`.
    """

    def _decorate(f: Callable[P, Any]):
        is_async = inspect.iscoroutinefunction(f)

        if is_async:

            async def wrapped(*args: P.args, **kwargs: P.kwargs):  # type: ignore[misc]
                payload = await f(*args, **kwargs)
                return _process_payload(
                    payload,
                    delimiter=delimiter,
                    quotechar=quotechar,
                    doublequote=doublequote,
                    quoting=quoting,
                    auto_datetime=auto_datetime,
                    chunksize=chunksize,
                    pandas_kwargs=pandas_kwargs,
                )

        else:

            def wrapped(*args: P.args, **kwargs: P.kwargs):  # type: ignore[misc]
                payload = f(*args, **kwargs)
                return _process_payload(
                    payload,
                    delimiter=delimiter,
                    quotechar=quotechar,
                    doublequote=doublequote,
                    quoting=quoting,
                    auto_datetime=auto_datetime,
                    chunksize=chunksize,
                    pandas_kwargs=pandas_kwargs,
                )

        # Preserve basic metadata.
        wrapped.__name__ = getattr(f, "__name__", wrapped.__name__)
        wrapped.__qualname__ = getattr(f, "__qualname__", wrapped.__qualname__)
        wrapped.__doc__ = getattr(f, "__doc__", None)
        wrapped.__module__ = getattr(f, "__module__", None)
        wrapped.__wrapped__ = f

        return wrapped

    if fn is None:
        return _decorate
    return _decorate(fn)


def _process_payload(
    payload: Any,
    *,
    delimiter: str | None,
    quotechar: str | None,
    doublequote: bool | None,
    quoting: int | None,
    auto_datetime: bool,
    chunksize: int | None,
    pandas_kwargs: Mapping[str, Any] | None,
) -> Iterator[dict[str, Any]]:
    _require_pandas()
    import pandas as pd

    text, source_name = _read_csv_payload(payload)
    if not text.strip():
        # empty input => empty iterator
        return iter(())

    dialect = _sniff_csv_dialect(
        text,
        delimiter=delimiter,
        quotechar=quotechar,
        doublequote=doublequote,
        quoting=quoting,
    )

    kwargs: dict[str, Any] = dict(pandas_kwargs or {})
    kwargs.setdefault("sep", dialect.delimiter)
    if dialect.quotechar is not None:
        kwargs.setdefault("quotechar", dialect.quotechar)
    if dialect.doublequote is not None:
        kwargs.setdefault("doublequote", dialect.doublequote)
    if dialect.quoting is not None:
        kwargs.setdefault("quoting", dialect.quoting)
    if chunksize is not None:
        kwargs.setdefault("chunksize", chunksize)

    # Always parse from an in-memory file-like; it handles both content + file.
    # In case the original payload was a path string we already read it.
    buf = io.StringIO(text)

    try:
        reader_or_df = pd.read_csv(buf, **kwargs)
    except Exception as e:
        loc = f" ({source_name})" if source_name else ""
        raise RuntimeError(f"Unable to parse CSV{loc}: {e}") from e

    # pd.read_csv returns either DataFrame or TextFileReader (when chunksize).
    if hasattr(reader_or_df, "__iter__") and not hasattr(reader_or_df, "columns"):
        def _gen() -> Iterator[dict[str, Any]]:
            for chunk in reader_or_df:
                yield from _iter_dict_rows(chunk, auto_datetime=auto_datetime)

        return _gen()

    return _iter_dict_rows(reader_or_df, auto_datetime=auto_datetime)


__all__ = [
    "parse_csv",
    # Expose internals for unit tests (mirrors html subpackage style)
    "_read_csv_payload",
    "_sniff_csv_dialect",
]
