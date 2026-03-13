import asyncio
import logging

import pytest


def _messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records]


def test_log_sync_start_and_end_includes_args_result_duration(caplog: pytest.LogCaptureFixture):
    from etl_decorators.logging import log

    caplog.set_level("DEBUG")

    @log(level="INFO", with_arguments=True, with_result=True, with_duration=True)
    def step(a: int, b: str = "x") -> str:
        return f"{a}:{b}"

    out = step(1, b="y")
    assert out == "1:y"

    msgs = _messages(caplog)

    assert any(m.startswith("start step(") and "a=1" in m and "b='y'" in m for m in msgs)
    assert any(m.startswith("end step") and "duration=" in m and "result='1:y'" in m for m in msgs)

    # the decorator uses logger.log(INFO)
    assert any(r.levelno == logging.INFO for r in caplog.records)


def test_log_with_arguments_false_does_not_print_parens(caplog: pytest.LogCaptureFixture):
    from etl_decorators.logging import log

    caplog.set_level("DEBUG")

    @log(level="DEBUG", with_arguments=False, with_result=False, with_duration=False)
    def step(a: int) -> int:
        return a + 1

    assert step(1) == 2
    msgs = _messages(caplog)
    assert any(m == "start step" for m in msgs)
    assert any(m == "end step" for m in msgs)


def test_log_exception_is_logged_and_reraised(caplog: pytest.LogCaptureFixture):
    from etl_decorators.logging import log

    caplog.set_level("DEBUG")

    @log(level="WARNING", with_arguments=False, with_result=True, with_duration=True)
    def step() -> int:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        step()

    msgs = _messages(caplog)
    assert any(m.startswith("start step") for m in msgs)
    assert any(m.startswith("failed step") and "exc=ValueError: boom" in m for m in msgs)
    assert any(r.exc_info is not None for r in caplog.records if "failed step" in r.getMessage())


def test_log_async_start_and_end(caplog: pytest.LogCaptureFixture):
    from etl_decorators.logging import log

    caplog.set_level("DEBUG")

    @log(level=10, with_arguments=True, with_result=True, with_duration=True)
    async def step(x: int) -> int:
        return x + 1

    assert asyncio.run(step(1)) == 2

    msgs = _messages(caplog)
    assert any(m.startswith("start step(") and "x=1" in m for m in msgs)
    assert any(m.startswith("end step") and "result=2" in m for m in msgs)


def test_log_bare_decorator_form_uses_defaults(caplog: pytest.LogCaptureFixture):
    """Covers `@log` usage without parentheses (fn passed directly)."""

    from etl_decorators.logging import log

    caplog.set_level("DEBUG")

    @log
    def step(x: int) -> int:
        return x + 1

    assert step(1) == 2
    msgs = _messages(caplog)
    assert any(m.startswith("start step(") and "x=1" in m for m in msgs)
    assert any(m.startswith("end step") and "result=2" in m for m in msgs)


def test_log_level_supports_numeric_string(caplog: pytest.LogCaptureFixture):
    """Covers level='10' parsing branch."""

    from etl_decorators.logging import log

    caplog.set_level("DEBUG")

    @log(level="10", with_arguments=False, with_result=False, with_duration=False)
    def step() -> None:
        return None

    step()
    assert any(r.levelno == 10 for r in caplog.records)


def test_log_level_validation_invalid_string_raises():
    from etl_decorators.logging import log

    with pytest.raises(ValueError, match="invalid level string"):

        @log(level="NOPE")
        def _step():
            return 1


def test_log_level_validation_empty_string_raises():
    from etl_decorators.logging import log

    with pytest.raises(ValueError, match="non-empty"):

        @log(level="   ")
        def _step():
            return 1


def test_log_truncates_large_result_repr(caplog: pytest.LogCaptureFixture):
    """Covers `_safe_repr` truncation in normal decorator flow."""

    from etl_decorators.logging import log

    caplog.set_level("DEBUG")

    @log(level="INFO", with_arguments=False, with_result=True, with_duration=False)
    def step() -> str:
        return "x" * 10_000

    step()
    msgs = _messages(caplog)
    # result should be present and truncated (contains ...)
    end_msgs = [m for m in msgs if m.startswith("end step")]
    assert end_msgs and "result=" in end_msgs[0]
    assert "..." in end_msgs[0]


def test_safe_repr_defensive_branches_are_covered():
    """Cover defensive code paths in internal helpers."""

    from etl_decorators.logging.decorator import _safe_repr

    class BadRepr:
        def __repr__(self) -> str:  # noqa: D401
            raise RuntimeError("nope")

    assert "unreprable" in _safe_repr(BadRepr())

    # max_len <= 3 special-casing
    assert _safe_repr("abcdef", max_len=3) == "'ab"  # repr('abcdef') starts with quotes


def test_log_async_with_arguments_false(caplog: pytest.LogCaptureFixture):
    from etl_decorators.logging import log

    caplog.set_level("DEBUG")

    @log(level="DEBUG", with_arguments=False, with_result=False, with_duration=False)
    async def step(x: int) -> int:
        return x + 1

    assert asyncio.run(step(1)) == 2
    msgs = _messages(caplog)
    assert any(m == "start step" for m in msgs)
    assert any(m == "end step" for m in msgs)


def test_log_async_exception_is_logged_and_reraised(caplog: pytest.LogCaptureFixture):
    from etl_decorators.logging import log

    caplog.set_level("DEBUG")

    @log(level="WARNING", with_arguments=False, with_result=True, with_duration=True)
    async def step() -> int:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        asyncio.run(step())

    msgs = _messages(caplog)
    assert any(m.startswith("failed step") and "exc=ValueError: boom" in m for m in msgs)
    assert any(r.exc_info is not None for r in caplog.records if "failed step" in r.getMessage())
