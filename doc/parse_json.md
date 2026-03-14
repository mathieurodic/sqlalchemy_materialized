# `etl_decorators.json.parse_json`

`etl_decorators.json.parse_json` is a small decorator that turns a function
returning JSON (or a reference to JSON) into a parsed Python value.

## Installation

This decorator is part of the base package:

```bash
pip install etl-decorators
```

## Basic usage

```python
from etl_decorators.json import parse_json


@parse_json
def payload() -> str:
    return '{"a": 1, "b": [1, 2]}'


data = payload()
assert data == {"a": 1, "b": [1, 2]}
```

## Accepted return values

The decorated callable may return:

- **file path**: `str` or `pathlib.Path`
- **JSON content**: `str`
- **file-like resource**: any object with `.read()` returning `str` or `bytes`

## Return type

The decorator returns:

- `dict` (JSON object), or
- `list` (JSON array)

If the JSON is valid but not an object/array (e.g. a string literal), the
decorator raises `ValueError`.

## Errors

JSON decoding errors are raised as `ValueError` with:

- optional source info (path / file name), when available
- line/column information
