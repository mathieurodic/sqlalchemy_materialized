import asyncio
import time

import pytest


def test_timeout_sync_raises():
    from etl_decorators.resilience import timeout

    @timeout(seconds=0.05)
    def slow() -> None:
        time.sleep(0.2)

    with pytest.raises(TimeoutError):
        slow()


def test_timeout_sync_preserves_return_value():
    from etl_decorators.resilience import timeout

    @timeout(seconds=0.5)
    def fast() -> int:
        return 1

    assert fast() == 1


def test_timeout_sync_propagates_exception_from_wrapped_function():
    from etl_decorators.resilience import timeout

    class Boom(RuntimeError):
        pass

    @timeout(seconds=0.5)
    def bad() -> None:
        raise Boom("kaboom")

    with pytest.raises(Boom, match="kaboom"):
        bad()


def test_timeout_sync_seconds_validation():
    from etl_decorators.resilience import timeout

    with pytest.raises(ValueError, match="seconds must be > 0"):
        timeout(seconds=0)(lambda: None)  # type: ignore[misc]


def test_timeout_sync_custom_error_and_message():
    from etl_decorators.resilience import timeout

    class MyTimeout(RuntimeError):
        pass

    @timeout(seconds=0.05, timeout_error=MyTimeout, message="nope")
    def slow() -> None:
        time.sleep(0.2)

    with pytest.raises(MyTimeout, match="nope"):
        slow()


def test_timeout_timeout_error_validation():
    from etl_decorators.resilience import timeout

    with pytest.raises(TypeError, match="Exception subclass"):
        timeout(seconds=1, timeout_error=123)(lambda: None)  # type: ignore[misc]


def test_timeout_async_raises():
    from etl_decorators.resilience import timeout

    @timeout(seconds=0.05)
    async def slow() -> None:
        await asyncio.sleep(0.2)

    with pytest.raises(TimeoutError):
        asyncio.run(slow())


def test_timeout_async_custom_error_and_message():
    from etl_decorators.resilience import timeout

    class MyTimeout(RuntimeError):
        pass

    @timeout(seconds=0.05, timeout_error=MyTimeout, message="async nope")
    async def slow() -> None:
        await asyncio.sleep(0.2)

    with pytest.raises(MyTimeout, match="async nope"):
        asyncio.run(slow())


def test_timeout_async_preserves_return_value():
    from etl_decorators.resilience import timeout

    @timeout(seconds=0.5)
    async def fast() -> int:
        return 2

    assert asyncio.run(fast()) == 2
