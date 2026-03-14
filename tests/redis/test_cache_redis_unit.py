from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest


def test_cache_sync_hit_miss(redis_cache):
    calls = {"n": 0}

    @redis_cache(read_ttl=60, write_ttl=60, serialization="pickle")
    def f(x: int) -> dict:
        calls["n"] += 1
        return {"x": x, "calls": calls["n"]}

    assert f(1) == {"x": 1, "calls": 1}
    assert f(1) == {"x": 1, "calls": 1}
    assert f(2) == {"x": 2, "calls": 2}


def test_cache_direct_decorator_form_sync(redis_cache):
    """Cover `RedisCache.__call__(fn=...)` (no parentheses) code path."""

    calls = {"n": 0}

    @redis_cache
    def f(x: int) -> int:
        calls["n"] += 1
        return x + calls["n"]

    assert f(1) == 2
    assert f(1) == 2


def test_cache_sync_refresh(redis_cache):
    calls = {"n": 0}

    @redis_cache(read_ttl=60, write_ttl=60, serialization="pickle")
    def f(x: int) -> int:
        calls["n"] += 1
        return x + calls["n"]

    assert f(1) == 2
    assert f(1) == 2
    assert f(1, refresh=True) == 3
    assert f(1) == 3


def test_cache_sync_bypass(redis_cache):
    calls = {"n": 0}

    @redis_cache(read_ttl=60, write_ttl=60, serialization="pickle")
    def f(x: int) -> int:
        calls["n"] += 1
        return x + calls["n"]

    assert f(1) == 2
    assert f(1, cache=False) == 3
    assert f(1) == 2


def test_cache_sync_expires_refreshed_on_read(redis_cache):
    # Use a tiny read_ttl and longer write_ttl; after a read, ttl should
    # roughly match the read ttl.
    @redis_cache(read_ttl=1, write_ttl=60, serialization="pickle")
    def f(x: int) -> int:
        return x + 1

    _ = f(1)

    # The cache key is computed from the wrapped callable (the one passed to
    # RedisCache._decorate), which is available via __wrapped__.
    orig = getattr(f, "__wrapped__", f)
    key = redis_cache._key(code_hash="testhash", fn=orig, args=(1,), kwargs={})
    client = redis_cache._sync_client()
    ttl1 = client.ttl(key)
    assert ttl1 > 0

    _ = f(1)  # hit => expire to read_ttl
    ttl2 = client.ttl(key)
    assert ttl2 in (0, 1) or ttl2 <= 2


def test_cache_fail_open_false_raises_on_redis_errors(redis_cache, monkeypatch):
    def broken_client():
        raise RuntimeError("boom")

    object.__setattr__(redis_cache, "_sync_client", broken_client)

    @redis_cache(read_ttl=60, write_ttl=60, serialization="pickle", fail_open=False)
    def f(x: int) -> int:
        return x + 1

    with pytest.raises(RuntimeError, match="boom"):
        f(1)


def test_cache_bool_options_type_validation(redis_cache):
    @redis_cache(read_ttl=60, write_ttl=60, serialization="pickle")
    def f(x: int) -> int:
        return x + 1

    with pytest.raises(TypeError):
        f(1, cache="no")

    with pytest.raises(TypeError):
        f(1, refresh="yes")


def test_cache_ttl_timedelta_and_rounding(redis_cache):
    @redis_cache(read_ttl=timedelta(seconds=0.1), write_ttl=timedelta(seconds=0.1))
    def f(x: int) -> int:
        return x + 1

    assert f(1) == 2


def test_cache_invalid_ttl_raises(redis_cache):
    with pytest.raises(ValueError):
        @redis_cache(read_ttl=0, write_ttl=1)
        def f(x: int) -> int:
            return x

    with pytest.raises(ValueError):
        @redis_cache(read_ttl=1, write_ttl=-1)
        def g(x: int) -> int:
            return x


def test_cache_invalid_serialization_raises(redis_cache):
    with pytest.raises(ValueError):
        @redis_cache(serialization="nope")  # type: ignore[arg-type]
        def f(x: int) -> int:
            return x


def test_cache_write_ttl_none_uses_set(redis_cache, monkeypatch):
    client = redis_cache._sync_client()
    calls = {"set": 0, "setex": 0}

    real_set = client.set
    real_setex = client.setex

    def set_spy(*args, **kwargs):
        calls["set"] += 1
        return real_set(*args, **kwargs)

    def setex_spy(*args, **kwargs):
        calls["setex"] += 1
        return real_setex(*args, **kwargs)

    monkeypatch.setattr(client, "set", set_spy)
    monkeypatch.setattr(client, "setex", setex_spy)

    @redis_cache(read_ttl=1, write_ttl=None)
    def f(x: int) -> int:
        return x + 1

    assert f(1) == 2
    assert calls["set"] == 1
    assert calls["setex"] == 0


def test_cache_read_ttl_none_does_not_expire(redis_cache, monkeypatch):
    client = redis_cache._sync_client()
    calls = {"expire": 0}
    real_expire = client.expire

    def expire_spy(*args, **kwargs):
        calls["expire"] += 1
        return real_expire(*args, **kwargs)

    monkeypatch.setattr(client, "expire", expire_spy)

    @redis_cache(read_ttl=None, write_ttl=60)
    def f(x: int) -> int:
        return x + 1

    assert f(1) == 2
    assert f(1) == 2
    assert calls["expire"] == 0


def test_cache_fail_open_true_swallows_redis_errors(redis_cache, monkeypatch):
    # fail-open: if redis read fails, compute; if write fails, still return.
    class Broken:
        def get(self, *a, **k):
            raise RuntimeError("read boom")

        def setex(self, *a, **k):
            raise RuntimeError("write boom")

        def set(self, *a, **k):
            raise RuntimeError("write boom")

        def expire(self, *a, **k):
            raise RuntimeError("expire boom")

    object.__setattr__(redis_cache, "_sync_client", lambda: Broken())

    calls = {"n": 0}

    @redis_cache(fail_open=True, read_ttl=1, write_ttl=1)
    def f(x: int) -> int:
        calls["n"] += 1
        return x + calls["n"]

    assert f(1) == 2
    assert f(1) == 3


def test_cache_async_hit_miss(redis_cache):
    calls = {"n": 0}

    @redis_cache(read_ttl=60, write_ttl=60, serialization="pickle")
    async def f(x: int) -> int:
        calls["n"] += 1
        await asyncio.sleep(0)
        return x + calls["n"]

    async def run():
        assert await f(1) == 2
        assert await f(1) == 2
        assert await f(2) == 4

    asyncio.run(run())


def test_cache_async_fail_open_false_raises(redis_cache):
    class BrokenAsync:
        async def get(self, *a, **k):
            raise RuntimeError("boom")

    object.__setattr__(redis_cache, "_async_client", lambda: BrokenAsync())

    @redis_cache(fail_open=False)
    async def f(x: int) -> int:
        return x + 1

    async def run():
        with pytest.raises(RuntimeError, match="boom"):
            await f(1)

    asyncio.run(run())


def test_cache_async_bypass_kw_cache_false_skips_redis(redis_cache):
    calls = {"n": 0}

    @redis_cache
    async def f(x: int) -> int:
        calls["n"] += 1
        return x + calls["n"]

    async def run():
        assert await f(1, cache=False) == 2
        assert await f(1, cache=False) == 3

    asyncio.run(run())


def test_cache_async_refresh_kw_is_parsed(redis_cache):
    calls = {"n": 0}

    @redis_cache
    async def f(x: int) -> int:
        calls["n"] += 1
        return x + calls["n"]

    async def run():
        # Prime cache
        assert await f(1) == 2
        # refresh=True should skip reads and recompute
        assert await f(1, refresh=True) == 3

    asyncio.run(run())


def test_cache_async_fail_open_true_swallows_read_errors(redis_cache):
    """Cover the async `except: cached=None` branch when fail_open=True."""

    class BrokenReadOKWrite:
        async def get(self, *a, **k):
            raise RuntimeError("read boom")

        async def expire(self, *a, **k):
            return True

        async def setex(self, *a, **k):
            return True

        async def set(self, *a, **k):
            return True

    object.__setattr__(redis_cache, "_async_client", lambda: BrokenReadOKWrite())

    calls = {"n": 0}

    @redis_cache(fail_open=True, read_ttl=1, write_ttl=1)
    async def f(x: int) -> int:
        calls["n"] += 1
        return x + calls["n"]

    async def run():
        assert await f(1) == 2
        assert await f(1) == 3

    asyncio.run(run())


def test_cache_async_fail_open_false_raises_on_write_errors_and_write_ttl_none_uses_set(redis_cache):
    class BrokenWrite:
        async def get(self, *a, **k):
            return None

        async def expire(self, *a, **k):
            return True

        async def setex(self, *a, **k):
            raise RuntimeError("write boom")

        async def set(self, *a, **k):
            raise RuntimeError("write boom")

    object.__setattr__(redis_cache, "_async_client", lambda: BrokenWrite())

    @redis_cache(fail_open=False, write_ttl=None)
    async def f(x: int) -> int:
        return x + 1

    async def run():
        with pytest.raises(RuntimeError, match="write boom"):
            await f(1)

    asyncio.run(run())


def test_cache_fail_open_false_raises_on_sync_write_errors(redis_cache):
    class BrokenWrite:
        def get(self, *a, **k):
            return None

        def expire(self, *a, **k):
            return True

        def setex(self, *a, **k):
            raise RuntimeError("write boom")

        def set(self, *a, **k):
            raise RuntimeError("write boom")

    object.__setattr__(redis_cache, "_sync_client", lambda: BrokenWrite())

    @redis_cache(fail_open=False, write_ttl=1)
    def f(x: int) -> int:
        return x + 1

    with pytest.raises(RuntimeError, match="write boom"):
        f(1)
