import asyncio


def test_dedupe_basic_sync():
    from etl_decorators.streams import dedupe

    @dedupe
    def xs():
        yield from [1, 1, 2, 2, 3]

    assert list(xs()) == [1, 2, 3]


def test_dedupe_unhashable_items_with_key():
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


def test_dedupe_async_iterable():
    from etl_decorators.streams import dedupe

    @dedupe
    async def xs_async():
        async def gen():
            for v in [1, 1, 2, 3, 3]:
                yield v

        return gen()

    async def main():
        out = []
        async for v in await xs_async():
            out.append(v)
        return out

    assert asyncio.run(main()) == [1, 2, 3]
