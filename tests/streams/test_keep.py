import asyncio

import pytest


def test_keep_is_lazy_and_filters_values():
    from etl_decorators.streams import keep

    seen: list[int] = []

    def pred(x: int) -> bool:
        seen.append(x)
        return x % 2 == 0

    @keep(pred)
    def xs():
        yield from range(5)

    it = xs()
    assert seen == []  # lazy: nothing consumed yet
    assert next(it) == 0
    assert seen == [0]
    assert list(it) == [2, 4]
    # predicate gets evaluated on every item
    assert seen == [0, 1, 2, 3, 4]


def test_keep_predicate_exception_bubbles():
    from etl_decorators.streams import keep

    def pred(_x: int) -> bool:
        raise ValueError("boom")

    @keep(pred)
    def xs():
        yield from range(3)

    it = xs()
    with pytest.raises(ValueError, match="boom"):
        next(it)


def test_keep_async_iterable_with_async_predicate():
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
        return out

    assert asyncio.run(main()) == [0, 2, 4]
