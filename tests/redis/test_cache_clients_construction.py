from __future__ import annotations


def test_sync_and_async_client_construction_smoke(monkeypatch):
    # Cover the client construction code paths without needing a real Redis.
    from etl_decorators.redis.cache import RedisCache

    class DummyRedis:
        def __init__(self, kind: str):
            self.kind = kind

        @classmethod
        def from_url(cls, url: str, **kwargs):
            return DummyRedis(f"sync:{url}")

    class DummyAsyncRedis:
        @classmethod
        def from_url(cls, url: str, **kwargs):
            return DummyRedis(f"async:{url}")

    # Ensure deps check passes.
    monkeypatch.setattr("etl_decorators.redis.cache._require_redis", lambda: None)

    import sys
    import types

    # Patch imports inside methods by patching sys.modules with real module
    # objects so `import redis.asyncio as redis_async` works.
    redis_mod = types.ModuleType("redis")
    redis_mod.Redis = DummyRedis

    redis_async_mod = types.ModuleType("redis.asyncio")
    redis_async_mod.Redis = DummyAsyncRedis

    # Make it visible as a submodule too.
    redis_mod.asyncio = redis_async_mod

    monkeypatch.setitem(sys.modules, "redis", redis_mod)
    monkeypatch.setitem(sys.modules, "redis.asyncio", redis_async_mod)

    cache = RedisCache(url="redis://x")
    assert cache._sync_client().kind == "sync:redis://x"
    assert cache._async_client().kind == "async:redis://x"
