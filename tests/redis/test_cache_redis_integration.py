"""Optional Redis integration tests.

These tests hit a real Redis instance.

They are skipped unless the following environment variable is set:

- ETL_DECORATORS_TESTS_REDIS_URL
"""

from __future__ import annotations

import asyncio


def test_redis_cache_sync_hit_miss(redis_cache_integration):
    calls = {"n": 0}

    @redis_cache_integration(read_ttl=60, write_ttl=60, serialization="pickle")
    def f(x: int) -> dict:
        calls["n"] += 1
        return {"x": x, "calls": calls["n"]}

    out1 = f(1)
    out2 = f(1)
    out3 = f(2)

    assert out1 == {"x": 1, "calls": 1}
    assert out2 == {"x": 1, "calls": 1}
    assert out3 == {"x": 2, "calls": 2}


def test_redis_cache_sync_refresh(redis_cache_integration):
    calls = {"n": 0}

    @redis_cache_integration(read_ttl=60, write_ttl=60, serialization="pickle")
    def f(x: int) -> int:
        calls["n"] += 1
        return x + calls["n"]

    assert f(1) == 2  # calls=1
    assert f(1) == 2  # cached
    assert f(1, refresh=True) == 3  # recompute (calls=2)
    assert f(1) == 3  # cached new value


def test_redis_cache_sync_bypass(redis_cache_integration):
    calls = {"n": 0}

    @redis_cache_integration(read_ttl=60, write_ttl=60, serialization="pickle")
    def f(x: int) -> int:
        calls["n"] += 1
        return x + calls["n"]

    assert f(1) == 2
    assert f(1, cache=False) == 3  # bypass
    assert f(1) == 2  # still cached from first call


def test_redis_cache_async_hit_miss(redis_cache_integration):
    calls = {"n": 0}

    @redis_cache_integration(read_ttl=60, write_ttl=60, serialization="pickle")
    async def f(x: int) -> int:
        calls["n"] += 1
        await asyncio.sleep(0)
        return x + calls["n"]

    async def run():
        assert await f(1) == 2
        assert await f(1) == 2
        assert await f(2) == 4

    asyncio.run(run())
