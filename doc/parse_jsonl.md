# `etl_decorators.json.parse_jsonl`

`etl_decorators.json.parse_jsonl` is a small decorator that turns a function
returning JSONL (newline-delimited JSON) into an iterator of Python values.

## Installation

This decorator is part of the base package:

```bash
pip install etl-decorators
```

## Basic usage

```python
from etl_decorators.json import parse_jsonl


@parse_jsonl
def events() -> str:
    return '{"id": 1}\n{"id": 2}\n'


items = list(events())
assert items == [{"id": 1}, {"id": 2}]
```

## Accepted return values

The decorated callable may return:

- **file path**: `str` or `pathlib.Path`
- **JSONL content**: `str`
- **file-like resource**: any object yielding lines (`for line in f`) or with
  `.read()`

For file paths and iterables, the decorator aims to be **lazy** (it does not
read the whole file into memory).

## Return type

The decorator returns an `Iterator[dict | list]`.

Each non-empty line must be a JSON object or array.

## Blank lines

Blank / whitespace-only lines are skipped.

## Errors

JSON decoding errors are raised as `ValueError` including:

- line number in the JSONL stream
- optional source info (path / file name), when available
