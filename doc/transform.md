# `etl_decorators.streams.transform`

`etl_decorators.streams.transform` lazily applies a mapper function to items yielded
from a stream.

It supports sync streams and async streams.

## Basic usage

```python
from etl_decorators.streams import transform


@transform(lambda x: x + 1)
def xs():
    yield from range(3)


assert list(xs()) == [1, 2, 3]
```

## Async streams and async mappers

When the decorated function returns an `AsyncIterable`, the mapper can be:

- synchronous (`def mapper(x): ...`), or
- asynchronous (`async def mapper(x): ...`) / return an awaitable.

```python
import asyncio
from etl_decorators.streams import transform


async def plus_one(x: int) -> int:
    await asyncio.sleep(0)
    return x + 1


@transform(plus_one)
async def xs_async():
    async def gen():
        for i in range(3):
            yield i
    return gen()


async def main():
    out = []
    async for v in await xs_async():
        out.append(v)
    assert out == [1, 2, 3]


asyncio.run(main())
```
