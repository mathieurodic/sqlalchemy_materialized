from __future__ import annotations

import functools

from etl_decorators.redis.hashing import function_code_hash


def _wraps_passthrough(fn):
    @functools.wraps(fn)
    def wrapped(*args, **kwargs):
        return fn(*args, **kwargs)

    return wrapped


def test_function_code_hash_changes_with_wrapped_chain():
    def foo(x: int = 42) -> int:
        return x + 1

    h1 = function_code_hash(foo)
    h2 = function_code_hash(_wraps_passthrough(foo))

    assert h1 != h2


def test_function_code_hash_same_for_same_object():
    def foo(x: int = 42) -> int:
        return x + 1

    assert function_code_hash(foo) == function_code_hash(foo)


def test_function_code_hash_no_source_fallback(monkeypatch):
    # Force inspect.getsource() failure to exercise the no-source path.
    import inspect

    def boom(_fn):
        raise OSError("no source")

    monkeypatch.setattr(inspect, "getsource", boom)

    def foo():
        return 1

    assert isinstance(function_code_hash(foo), str)


def test_function_code_hash_handles_wrapped_object_without_code_object():
    class NoCode:
        __module__ = "m"
        __qualname__ = "Q"
        __wrapped__ = None

    assert isinstance(function_code_hash(NoCode()), str)
