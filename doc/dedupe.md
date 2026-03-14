# `etl_decorators.streams.dedupe`

`etl_decorators.streams.dedupe` removes duplicates from a stream.

It keeps an in-memory `set` of seen keys for the lifetime of the iterator.
This is simple and fast, but memory usage grows with the number of unique
items.

## Basic usage

```python
from etl_decorators.streams import dedupe


@dedupe
def xs():
    yield from [1, 1, 2, 2, 3]


assert list(xs()) == [1, 2, 3]
```

## Deduping unhashable items

If items are unhashable (e.g. dicts), provide a `key=`:

```python
from etl_decorators.streams import dedupe


@dedupe(key=lambda row: row["id"])
def rows():
    yield from [
        {"id": 1, "x": "a"},
        {"id": 1, "x": "b"},
        {"id": 2, "x": "c"},
    ]


assert list(rows()) == [
    {"id": 1, "x": "a"},
    {"id": 2, "x": "c"},
]
```
