import asyncio

import pytest


def test_map_is_lazy_and_maps_values():
    from etl_decorators.streams import transform

    seen: list[int] = []

    def mapper(x: int) -> int:
        seen.append(x)
        return x + 1

    @transform(mapper)
    def xs():
        yield from range(3)

    it = xs()
    assert seen == []  # lazy: nothing consumed yet
    assert next(it) == 1
    assert seen == [0]
    assert list(it) == [2, 3]
    assert seen == [0, 1, 2]


def test_map_mapper_exception_bubbles():
    from etl_decorators.streams import transform

    def mapper(_x: int) -> int:
        raise ValueError("boom")

    @transform(mapper)
    def xs():
        yield from range(3)

    it = xs()
    with pytest.raises(ValueError, match="boom"):
        next(it)


def test_map_async_iterable_with_async_mapper():
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
        return out

    assert asyncio.run(main()) == [1, 2, 3]
