# `etl_decorators.csv.parse_csv`

`etl_decorators.csv.parse_csv` is a small decorator that turns a function
returning CSV (or a reference to CSV) into an **iterator of typed dictionaries**.

Each yielded item corresponds to a CSV row, with keys taken from the **header
row**.

## Installation

```bash
pip install etl-decorators[csv]
```

## Basic usage (return an iterator of row dicts)

```python
from etl_decorators.csv import parse_csv


@parse_csv
def get_csv() -> str:
    return "name;age\nAlice;30\nBob;41\n"


rows = list(get_csv())
assert rows == [
    {"name": "Alice", "age": 30},
    {"name": "Bob", "age": 41},
]

```

## Returning tuples instead of dicts

If you set `as_dict=False`, the decorator yields **tuples** instead of dicts.
This can be useful when:

- your CSV has no header row
- you want positional data

```python
from etl_decorators.csv import parse_csv


@parse_csv(as_dict=False)
def get_csv() -> str:
    return "a,b\n1,2\n3,4\n"


rows = list(get_csv())
assert rows == [
    (1, 2),
    (3, 4),
]
```

By default, `skip_header_rows=1`, so with `as_dict=False` the first row is
skipped (which is commonly the header row, even though it is not used).

If you want to include the first row as data, use `skip_header_rows=0`.

## Skipping preamble/header rows (`skip_header_rows`)

`skip_header_rows` controls where **data** starts (0-based index):

- when `as_dict=True` (default):
  - the header row is at index `skip_header_rows - 1`
  - yielded data rows start at index `skip_header_rows`
- when `as_dict=False`:
  - yielded data rows start at index `skip_header_rows`

Example: one preamble line, then header, then data.

```python
from etl_decorators.csv import parse_csv


@parse_csv(skip_header_rows=2)
def get_csv() -> str:
    return "THIS IS A REPORT\nx,y\n1,2\n"


assert list(get_csv()) == [{"x": 1, "y": 2}]

```

The decorated callable accepts three kinds of return values:

- **file path**: `str` or `pathlib.Path`
- **CSV content**: `str`
- **file-like resource**: any object with `.read()` returning `str` or `bytes`

## Delimiter detection and overrides

When `delimiter=None` (default), the decorator tries to sniff the delimiter
from the beginning of the CSV.

You can force CSV parameters:

```python
import csv
from etl_decorators.csv import parse_csv


@parse_csv(delimiter=";", quoting=csv.QUOTE_MINIMAL)
def get_csv() -> str:
    return "a;b\n1;2\n"
```

Supported CSV-related parameters:

- `delimiter: str | None`
- `quotechar: str | None`
- `doublequote: bool | None`
- `quoting: int | None` (use `csv.QUOTE_*` constants)

## Type inference

The decorator aims to return useful Python types:

- `int`, `float`, `bool`
- `datetime` (when `auto_datetime=True`, default) for ISO-like values such as
  `YYYY-MM-DD` or `YYYY-MM-DD HH:MM:SS`

If a value cannot be safely converted, it is kept as a string.

## Chunked reading

If you pass `chunksize=...`, the decorator will parse the CSV in chunks via
`pandas.read_csv(..., chunksize=...)` and will yield rows incrementally.

```python
from etl_decorators.csv import parse_csv


@parse_csv(chunksize=1000)
def get_big_csv() -> str:
    return "a,b\n" + "\n".join(["1,2"] * 100_000)


for row in get_big_csv():
    ...
```

## Notes

- Optional dependencies are imported lazily; if pandas is missing, the decorator
  raises a clear `RuntimeError` telling you to install `etl-decorators[csv]`.
