"""Core :func:`etl_decorators.csv.parse_csv` decorator implementation."""

from __future__ import annotations

import inspect
import io
from typing import Any, Callable, Iterator, Mapping, ParamSpec, overload

from .deps import _require_pandas
from .dialect import _sniff_csv_dialect
from .payload import _read_csv_payload
from .rows import _iter_dict_rows


P = ParamSpec("P")


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
