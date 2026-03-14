import asyncio

import pytest


def test_batch_exact_multiples():
    from etl_decorators.streams import batch

    @batch(size=2)
    def xs():
        yield from range(4)

    assert list(xs()) == [[0, 1], [2, 3]]


def test_batch_with_remainder_and_drop_last_false():
    from etl_decorators.streams import batch

    @batch(size=3)
    def xs():
        yield from range(8)

    assert list(xs()) == [[0, 1, 2], [3, 4, 5], [6, 7]]


def test_batch_with_remainder_and_drop_last_true():
    from etl_decorators.streams import batch

    @batch(size=3, drop_last=True)
    def xs():
        yield from range(8)

    assert list(xs()) == [[0, 1, 2], [3, 4, 5]]


def test_batch_validates_size():
    from etl_decorators.streams import batch

    with pytest.raises(ValueError, match="size must be > 0"):

        @batch(size=0)
        def _xs():
            yield 1


def test_batch_async_iterable():
    from etl_decorators.streams import batch

    @batch(size=2)
    async def xs_async():
        async def gen():
            for i in range(5):
                yield i

        return gen()

    async def main():
        out = []
        async for b in await xs_async():
            out.append(b)
        return out

    assert asyncio.run(main()) == [[0, 1], [2, 3], [4]]
