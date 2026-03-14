# `etl_decorators.streams.batch`

`etl_decorators.streams.batch` turns an item stream into a stream of batches.

It supports both:

- sync iterables (`for x in ...`)
- async iterables (`async for x in ...`)

## Basic usage

```python
from etl_decorators.streams import batch


@batch(size=3)
def rows():
    yield from range(8)


assert list(rows()) == [
    [0, 1, 2],
    [3, 4, 5],
    [6, 7],
]
```

## Dropping the final incomplete batch

```python
from etl_decorators.streams import batch


@batch(size=3, drop_last=True)
def rows():
    yield from range(8)


assert list(rows()) == [
    [0, 1, 2],
    [3, 4, 5],
]
```

## Async usage

```python
import asyncio
from etl_decorators.streams import batch


@batch(size=2)
async def rows():
    async def gen():
        for i in range(5):
            yield i
    return gen()


async def main():
    out = []
    async for b in await rows():
        out.append(b)
    assert out == [[0, 1], [2, 3], [4]]


asyncio.run(main())
```
