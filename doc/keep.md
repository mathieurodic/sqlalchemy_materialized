# `etl_decorators.streams.keep`

`etl_decorators.streams.keep` lazily filters items yielded from a stream.

It supports sync streams and async streams.

## Basic usage

```python
from etl_decorators.streams import keep


@keep(lambda x: x % 2 == 0)
def xs():
    yield from range(6)


assert list(xs()) == [0, 2, 4]
```

## Async streams and async predicates

When the decorated function returns an `AsyncIterable`, the predicate can be:

- synchronous (`def pred(x): ...`), or
- asynchronous (`async def pred(x): ...`) / return an awaitable.

```python
import asyncio
from etl_decorators.streams import keep


async def is_even(x: int) -> bool:
    await asyncio.sleep(0)
    return x % 2 == 0


@keep(is_even)
async def xs_async():
    async def gen():
        for i in range(6):
            yield i

    return gen()


async def main():
    out = []
    async for v in await xs_async():
        out.append(v)
    assert out == [0, 2, 4]


asyncio.run(main())
```
