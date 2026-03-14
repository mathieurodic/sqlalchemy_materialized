import asyncio
import importlib

import pytest


class TransientError(RuntimeError):
    pass


def test_retry_on_retry_hook_is_called(monkeypatch):
    from etl_decorators.resilience import retry
    retry_mod = importlib.import_module("etl_decorators.resilience.retry")

    monkeypatch.setattr(retry_mod.time, "sleep", lambda _s: None)

    events: list[tuple[str, int, float]] = []

    def on_retry(exc: BaseException, attempt: int, sleep: float) -> None:
        events.append((exc.__class__.__name__, attempt, sleep))

    calls = {"n": 0}

    @retry(retry_on=TransientError, max_attempts=2, interval=0.1, factor=1.0, on_retry=on_retry)
    def f() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise TransientError("x")
        return 1

    assert f() == 1
    assert events and events[0][0] == "TransientError" and events[0][1] == 1


def test_retry_jitter_branch(monkeypatch):
    from etl_decorators.resilience import retry
    retry_mod = importlib.import_module("etl_decorators.resilience.retry")

    sleeps: list[float] = []
    monkeypatch.setattr(retry_mod.time, "sleep", lambda s: sleeps.append(s))

    # Make jitter deterministic.
    monkeypatch.setattr(retry_mod.random, "uniform", lambda lo, hi: (lo + hi) / 2)

    calls = {"n": 0}

    @retry(retry_on=TransientError, max_attempts=2, interval=1.0, factor=2.0, jitter=0.5)
    def f() -> int:
        calls["n"] += 1
        raise TransientError("boom")

    with pytest.raises(TransientError):
        f()

    # attempt=1 => base delay 1.0 => jitter multiplier average of [0.5, 1.5] = 1.0
    assert sleeps == [1.0]


def test_retry_never_retries_keyboard_interrupt(monkeypatch):
    from etl_decorators.resilience import retry
    retry_mod = importlib.import_module("etl_decorators.resilience.retry")

    monkeypatch.setattr(
        retry_mod.time,
        "sleep",
        lambda _s: (_ for _ in ()).throw(AssertionError("should not sleep")),
    )

    calls = {"n": 0}

    @retry(retry_on=BaseException, max_attempts=3)
    def f() -> None:
        calls["n"] += 1
        raise KeyboardInterrupt()

    with pytest.raises(KeyboardInterrupt):
        f()
    assert calls["n"] == 1


def test_retry_sync_retries_then_succeeds(monkeypatch):
    from etl_decorators.resilience import retry
    retry_mod = importlib.import_module("etl_decorators.resilience.retry")

    sleeps: list[float] = []
    monkeypatch.setattr(retry_mod.time, "sleep", lambda s: sleeps.append(s))

    calls = {"n": 0}

    @retry(retry_on=TransientError, max_attempts=5, interval=1.0, factor=2.0)
    def f() -> int:
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientError("try again")
        return 42

    assert f() == 42
    assert calls["n"] == 3
    assert sleeps == [1.0, 2.0]


def test_retry_sync_predicate(monkeypatch):
    from etl_decorators.resilience import retry
    retry_mod = importlib.import_module("etl_decorators.resilience.retry")

    sleeps: list[float] = []
    monkeypatch.setattr(retry_mod.time, "sleep", lambda s: sleeps.append(s))

    calls = {"n": 0}

    def predicate(exc: BaseException) -> bool:
        return isinstance(exc, ValueError) and str(exc) == "transient"

    @retry(retry_if=predicate, max_attempts=3, interval=0.5, factor=3.0)
    def f() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise ValueError("transient")
        return 7

    assert f() == 7
    assert calls["n"] == 2
    assert sleeps == [0.5]


def test_retry_sync_does_not_retry_unmatched(monkeypatch):
    from etl_decorators.resilience import retry
    retry_mod = importlib.import_module("etl_decorators.resilience.retry")

    monkeypatch.setattr(
        retry_mod.time,
        "sleep",
        lambda s: (_ for _ in ()).throw(AssertionError("should not sleep")),
    )

    calls = {"n": 0}

    @retry(retry_on=TransientError, max_attempts=3)
    def f() -> int:
        calls["n"] += 1
        raise KeyError("no")

    with pytest.raises(KeyError, match="no"):
        f()
    assert calls["n"] == 1


def test_retry_sync_max_attempts_limits(monkeypatch):
    from etl_decorators.resilience import retry
    retry_mod = importlib.import_module("etl_decorators.resilience.retry")

    sleeps: list[float] = []
    monkeypatch.setattr(retry_mod.time, "sleep", lambda s: sleeps.append(s))

    calls = {"n": 0}

    @retry(retry_on=TransientError, max_attempts=3, interval=1.0, factor=2.0)
    def f() -> int:
        calls["n"] += 1
        raise TransientError("always")

    with pytest.raises(TransientError, match="always"):
        f()

    assert calls["n"] == 3
    assert sleeps == [1.0, 2.0]


def test_retry_interval_0_does_not_sleep(monkeypatch):
    from etl_decorators.resilience import retry
    retry_mod = importlib.import_module("etl_decorators.resilience.retry")

    monkeypatch.setattr(
        retry_mod.time,
        "sleep",
        lambda s: (_ for _ in ()).throw(AssertionError("should not sleep")),
    )

    calls = {"n": 0}

    @retry(retry_on=TransientError, max_attempts=2, interval=0.0)
    def f() -> int:
        calls["n"] += 1
        if calls["n"] == 1:
            raise TransientError("transient")
        return 9

    assert f() == 9
    assert calls["n"] == 2


def test_retry_async_retries_then_succeeds(monkeypatch):
    from etl_decorators.resilience import retry
    retry_mod = importlib.import_module("etl_decorators.resilience.retry")

    sleeps: list[float] = []

    async def fake_sleep(s: float) -> None:
        sleeps.append(s)

    monkeypatch.setattr(retry_mod.asyncio, "sleep", fake_sleep)

    calls = {"n": 0}

    @retry(retry_on=TransientError, max_attempts=4, interval=0.25, factor=2.0)
    async def f() -> int:
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientError("try again")
        return 1

    assert asyncio.run(f()) == 1
    assert calls["n"] == 3
    assert sleeps == [0.25, 0.5]


def test_retry_async_on_retry_is_called_and_then_raises(monkeypatch):
    from etl_decorators.resilience import retry
    retry_mod = importlib.import_module("etl_decorators.resilience.retry")

    sleeps: list[float] = []

    async def fake_sleep(s: float) -> None:
        sleeps.append(s)

    monkeypatch.setattr(retry_mod.asyncio, "sleep", fake_sleep)

    events: list[tuple[str, int, float]] = []

    def on_retry(exc: BaseException, attempt: int, sleep: float) -> None:
        events.append((exc.__class__.__name__, attempt, sleep))

    @retry(
        retry_on=TransientError,
        max_attempts=2,
        interval=0.1,
        factor=1.0,
        on_retry=on_retry,
    )
    async def always_fail() -> None:
        raise TransientError("boom")

    with pytest.raises(TransientError, match="boom"):
        asyncio.run(always_fail())

    # 2 attempts total => 1 retry sleep/hook
    assert sleeps == [0.1]
    assert events == [("TransientError", 1, 0.1)]


def test_retry_requires_policy():
    from etl_decorators.resilience import retry

    def f() -> int:
        return 1

    with pytest.raises(ValueError, match="provide at least one"):
        retry(f)  # type: ignore[misc]
