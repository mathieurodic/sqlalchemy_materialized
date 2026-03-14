from __future__ import annotations

import builtins

import pytest


def test_require_redis_missing_dependency_raises_runtimeerror(monkeypatch):
    from etl_decorators.redis.deps import _require_redis

    real_import = builtins.__import__

    def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
        if name == "redis":
            raise ImportError("no redis")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(RuntimeError) as e:
        _require_redis()

    assert "pip install etl-decorators[redis]" in str(e.value)
