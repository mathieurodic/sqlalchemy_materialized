"""Core :func:`etl_decorators.csv.parse_csv` decorator implementation."""

from __future__ import annotations

import io
from typing import Any, Callable, Iterator, Literal, Mapping, ParamSpec, overload

from etl_decorators._base.decorators import DecoratorBase, OptionalFnDecoratorBase

from .deps import _require_pandas
from .dialect import _sniff_csv_dialect
from .payload import _read_csv_payload
from .rows import _iter_dict_rows, _iter_tuple_rows


P = ParamSpec("P")


@overload
def parse_csv(
    fn: Callable[P, Any],
    *,
    delimiter: str | None = None,
    quotechar: str | None = None,
    doublequote: bool | None = None,
    quoting: int | None = None,
    as_dict: Literal[True] = True,
    skip_header_rows: int = 1,
    auto_datetime: bool = True,
    chunksize: int | None = None,
    pandas_kwargs: Mapping[str, Any] | None = None,
) -> Callable[P, Iterator[dict[str, Any]]]: ...


@overload
def parse_csv(
    fn: Callable[P, Any],
    *,
    delimiter: str | None = None,
    quotechar: str | None = None,
    doublequote: bool | None = None,
    quoting: int | None = None,
    as_dict: Literal[False],
    skip_header_rows: int = 1,
    auto_datetime: bool = True,
    chunksize: int | None = None,
    pandas_kwargs: Mapping[str, Any] | None = None,
) -> Callable[P, Iterator[tuple[Any, ...]]]: ...


@overload
def parse_csv(
    fn: None = None,
    *,
    delimiter: str | None = None,
    quotechar: str | None = None,
    doublequote: bool | None = None,
    quoting: int | None = None,
    as_dict: Literal[True] = True,
    skip_header_rows: int = 1,
    auto_datetime: bool = True,
    chunksize: int | None = None,
    pandas_kwargs: Mapping[str, Any] | None = None,
) -> Callable[[Callable[P, Any]], Callable[P, Iterator[dict[str, Any]]]]: ...


@overload
def parse_csv(
    fn: None = None,
    *,
    delimiter: str | None = None,
    quotechar: str | None = None,
    doublequote: bool | None = None,
    quoting: int | None = None,
    as_dict: Literal[False],
    skip_header_rows: int = 1,
    auto_datetime: bool = True,
    chunksize: int | None = None,
    pandas_kwargs: Mapping[str, Any] | None = None,
) -> Callable[[Callable[P, Any]], Callable[P, Iterator[tuple[Any, ...]]]]: ...


def parse_csv(
    fn: Callable[P, Any] | None = None,
    *,
    delimiter: str | None = None,
    quotechar: str | None = None,
    doublequote: bool | None = None,
    quoting: int | None = None,
    as_dict: bool = True,
    skip_header_rows: int = 1,
    auto_datetime: bool = True,
    chunksize: int | None = None,
    pandas_kwargs: Mapping[str, Any] | None = None,
):
    """Decorator that parses CSV outputs into an iterator of rows.

    The decorated function can return:
    - a file path (str/pathlib.Path)
    - a CSV string
    - a file-like resource (implements `.read()`)

    When `as_dict=True` (default), yields dicts (one per row) and the keys are
    taken from a header row.

    When `as_dict=False`, yields tuples (one per row) and no header is used.

    Parameters:
        delimiter: force a delimiter (when None, delimiter is sniffed).
        quotechar/doublequote/quoting: CSV quoting options. Explicit values
            override sniffed ones.
        as_dict: when True (default), yields dict rows using a header row.
            When False, yields tuples.
        skip_header_rows: controls where data rows start (0-based). See Notes:
            - when `as_dict=True`, the header row is at index `skip_header_rows-1`
              and yielded data rows start at index `skip_header_rows`.
            - when `as_dict=False`, the first yielded row is at index
              `skip_header_rows`.
        auto_datetime: when True, tries to convert ISO-like date/datetime
            strings to `datetime`.
        chunksize: if provided, uses pandas chunked reading and yields rows
            incrementally.
        pandas_kwargs: additional keyword args forwarded to `pandas.read_csv`.
    """

    class _ParseCSVDecorator(DecoratorBase[P, Iterator[Any], None]):
        def process_result(
            self,
            _fn: Callable[P, Any],
            result: Any,
            _args: tuple[Any, ...],
            _kwargs: dict[str, Any],
            _state: None,
        ) -> Iterator[Any]:
            return _process_payload(
                result,
                delimiter=delimiter,
                quotechar=quotechar,
                doublequote=doublequote,
                quoting=quoting,
                as_dict=as_dict,
                skip_header_rows=skip_header_rows,
                auto_datetime=auto_datetime,
                chunksize=chunksize,
                pandas_kwargs=pandas_kwargs,
            )

    binder = OptionalFnDecoratorBase()

    def _decorate(f: Callable[P, Any]):
        return _ParseCSVDecorator().decorate(f)

    return binder.bind_optional(fn, _decorate)


def _process_payload(
    payload: Any,
    *,
    delimiter: str | None,
    quotechar: str | None,
    doublequote: bool | None,
    quoting: int | None,
    as_dict: bool,
    skip_header_rows: int,
    auto_datetime: bool,
    chunksize: int | None,
    pandas_kwargs: Mapping[str, Any] | None,
) -> Iterator[dict[str, Any]] | Iterator[tuple[Any, ...]]:
    _require_pandas()
    import pandas as pd

    text, source_name = _read_csv_payload(payload)
    if not text.strip():
        # empty input => empty iterator
        return iter(())

    if skip_header_rows < 0:
        raise ValueError("skip_header_rows must be >= 0")
    if as_dict and skip_header_rows < 1:
        raise ValueError(
            "skip_header_rows must be >= 1 when as_dict=True (a header row is required)"
        )

    # Sniff from a sample that starts at the header row (dict mode) or first
    # returned row (tuple mode) so preamble lines do not confuse the sniffer.
    # We don't want to allocate too much; a few lines are enough.
    lines = text.splitlines(keepends=True)
    sniff_start = (skip_header_rows - 1) if as_dict else skip_header_rows
    sniff_start = max(sniff_start, 0)
    sniff_text = "".join(lines[sniff_start : sniff_start + 20]) or text

    dialect = _sniff_csv_dialect(
        sniff_text,
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

    if as_dict:
        header_row = skip_header_rows - 1
        if header_row > 0:
            # Skip any preamble lines before the header row.
            kwargs.setdefault("skiprows", range(0, header_row))
            # `header` is relative to the *remaining* rows after `skiprows`.
            kwargs.setdefault("header", 0)
        else:
            kwargs.setdefault("header", 0)
    else:
        # No header row; return tuples.
        kwargs.setdefault("header", None)
        if skip_header_rows > 0:
            kwargs.setdefault("skiprows", range(0, skip_header_rows))

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

        def _gen() -> Iterator[dict[str, Any]] | Iterator[tuple[Any, ...]]:
            for chunk in reader_or_df:
                if as_dict:
                    yield from _iter_dict_rows(chunk, auto_datetime=auto_datetime)
                else:
                    yield from _iter_tuple_rows(chunk, auto_datetime=auto_datetime)

        return _gen()

    if as_dict:
        return _iter_dict_rows(reader_or_df, auto_datetime=auto_datetime)
    return _iter_tuple_rows(reader_or_df, auto_datetime=auto_datetime)
