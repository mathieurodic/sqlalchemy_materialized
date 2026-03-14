import asyncio
import logging
import re

import pytest


def _messages(caplog: pytest.LogCaptureFixture) -> list[str]:
    return [r.getMessage() for r in caplog.records]


def _freeze_perf_counter(
    monkeypatch: pytest.MonkeyPatch,
    *,
    start: float = 100.0,
    end: float = 100.123456,
) -> None:
    """Make duration logging deterministic.

    The `log` decorator calls `time.perf_counter()` exactly twice per invocation:
    once in `before_call`, then once in `process_result`/`process_exception`.
    """

    import etl_decorators.logging.decorator as log_decorator_module

    it = iter([start, end])

    def _fake_perf_counter() -> float:
        return next(it)

    monkeypatch.setattr(log_decorator_module.time, "perf_counter", _fake_perf_counter)


def _freeze_run_id(monkeypatch: pytest.MonkeyPatch, *, run_id: str = "f4b09ae5") -> None:
    import etl_decorators.logging.decorator as log_decorator_module

    monkeypatch.setattr(log_decorator_module, "_generate_run_id", lambda: run_id)


def _defined_location(fn) -> str:
    """Return the @file:line location for the originally-decorated callable."""

    original = getattr(fn, "__wrapped__", fn)
    return f"@{original.__code__.co_filename.rsplit('/', 1)[-1]}:{original.__code__.co_firstlineno}"


def test_log_sync_start_and_end_includes_args_result_duration(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    from etl_decorators.logging import log

    _freeze_perf_counter(monkeypatch)
    _freeze_run_id(monkeypatch)
    caplog.set_level("DEBUG", logger=__name__)
    caplog.clear()

    @log(level="INFO", with_arguments=True, with_result=True, with_duration=True)
    def step(a: int, b: str = "x") -> str:
        return f"{a}:{b}"

    out = step(1, b="y")
    assert out == "1:y"

    loc = _defined_location(step)

    assert len(caplog.records) == 2
    assert [r.levelno for r in caplog.records] == [logging.INFO, logging.INFO]
    assert [r.name for r in caplog.records] == [__name__, __name__]
    assert _messages(caplog) == [
        f"[START f4b09ae5] {loc} step(a=1, b='y')",
        f"[DONE  f4b09ae5] {loc} step: duration=0.123456s, result='1:y'",
    ]


def test_log_with_arguments_false_does_not_print_parens(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    from etl_decorators.logging import log

    _freeze_run_id(monkeypatch)
    caplog.set_level("DEBUG", logger=__name__)
    caplog.clear()

    @log(level="DEBUG", with_arguments=False, with_result=False, with_duration=False)
    def step(a: int) -> int:
        return a + 1

    assert step(1) == 2
    loc = _defined_location(step)

    assert len(caplog.records) == 2
    assert [r.levelno for r in caplog.records] == [logging.DEBUG, logging.DEBUG]
    assert [r.name for r in caplog.records] == [__name__, __name__]
    assert _messages(caplog) == [
        f"[START f4b09ae5] {loc} step",
        f"[DONE  f4b09ae5] {loc} step",
    ]


def test_log_with_location_false_does_not_print_location(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    from etl_decorators.logging import log

    _freeze_run_id(monkeypatch)
    caplog.set_level("DEBUG", logger=__name__)
    caplog.clear()

    @log(level="DEBUG", with_arguments=False, with_result=False, with_duration=False, with_location=False)
    def step() -> None:
        return None

    step()

    assert _messages(caplog) == [
        "[START f4b09ae5] step",
        "[DONE  f4b09ae5] step",
    ]


def test_log_exception_is_logged_and_reraised(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    from etl_decorators.logging import log

    _freeze_perf_counter(monkeypatch)
    _freeze_run_id(monkeypatch)
    caplog.set_level("DEBUG", logger=__name__)
    caplog.clear()

    @log(level="WARNING", with_arguments=False, with_result=True, with_duration=True)
    def step() -> int:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        step()

    loc = _defined_location(step)

    assert len(caplog.records) == 2
    start_rec, failed_rec = caplog.records
    assert start_rec.levelno == logging.WARNING
    assert failed_rec.levelno == logging.WARNING
    assert start_rec.name == __name__
    assert failed_rec.name == __name__
    assert _messages(caplog) == [
        f"[START f4b09ae5] {loc} step",
        f"[ERROR f4b09ae5] {loc} step: duration=0.123456s, exc=ValueError: boom",
    ]
    assert start_rec.exc_info is None
    assert failed_rec.exc_info is not None


def test_log_async_start_and_end(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    from etl_decorators.logging import log

    _freeze_perf_counter(monkeypatch)
    _freeze_run_id(monkeypatch)
    caplog.set_level("DEBUG", logger=__name__)
    caplog.clear()

    @log(level=10, with_arguments=True, with_result=True, with_duration=True)
    async def step(x: int) -> int:
        return x + 1

    assert asyncio.run(step(1)) == 2

    loc = _defined_location(step)

    assert len(caplog.records) == 2
    assert [r.levelno for r in caplog.records] == [10, 10]
    assert [r.name for r in caplog.records] == [__name__, __name__]
    assert _messages(caplog) == [
        f"[START f4b09ae5] {loc} step(x=1)",
        f"[DONE  f4b09ae5] {loc} step: duration=0.123456s, result=2",
    ]


def test_log_bare_decorator_form_uses_defaults(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    """Covers `@log` usage without parentheses (fn passed directly)."""

    from etl_decorators.logging import log

    _freeze_perf_counter(monkeypatch)
    _freeze_run_id(monkeypatch)
    caplog.set_level("DEBUG", logger=__name__)
    caplog.clear()

    @log
    def step(x: int) -> int:
        return x + 1

    assert step(1) == 2

    loc = _defined_location(step)
    assert len(caplog.records) == 2
    assert [r.levelno for r in caplog.records] == [logging.DEBUG, logging.DEBUG]
    assert [r.name for r in caplog.records] == [__name__, __name__]
    assert _messages(caplog) == [
        f"[START f4b09ae5] {loc} step(x=1)",
        f"[DONE  f4b09ae5] {loc} step: duration=0.123456s, result=2",
    ]


def test_log_level_supports_numeric_string(caplog: pytest.LogCaptureFixture):
    """Covers level='10' parsing branch."""

    from etl_decorators.logging import log

    caplog.set_level("DEBUG", logger=__name__)
    caplog.clear()

    @log(level="10", with_arguments=False, with_result=False, with_duration=False)
    def step() -> None:
        return None

    step()
    loc = _defined_location(step)
    assert len(caplog.records) == 2
    assert [r.levelno for r in caplog.records] == [10, 10]
    assert [r.name for r in caplog.records] == [__name__, __name__]
    # run_id is random here; we assert its structure and alignment, but keep exact message equality
    # by extracting it from the first record.
    run_id = caplog.records[0].getMessage().split()[1].rstrip("]")
    assert re.fullmatch(r"[0-9a-f]{8}", run_id)
    assert _messages(caplog) == [
        f"[START {run_id}] {loc} step",
        f"[DONE  {run_id}] {loc} step",
    ]


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


def test_log_nested_calls_generate_distinct_run_ids(caplog: pytest.LogCaptureFixture):
    """Run ids should pair START/DONE per invocation, even when nested."""

    from etl_decorators.logging import log

    caplog.set_level("DEBUG", logger=__name__)
    caplog.clear()

    @log(level="INFO", with_arguments=False, with_result=False, with_duration=False, with_location=False)
    def inner() -> str:
        return "ok"

    @log(level="INFO", with_arguments=False, with_result=False, with_duration=False, with_location=False)
    def outer() -> str:
        return inner()

    assert outer() == "ok"

    msgs = _messages(caplog)
    assert len(msgs) == 4

    # Expected order for nested sync calls:
    # outer START -> inner START -> inner DONE -> outer DONE
    prefix_re = re.compile(r"^\[(START|DONE|ERROR)\s+([0-9a-f]{8})\]")
    parsed = [prefix_re.match(m) for m in msgs]
    assert all(p is not None for p in parsed)

    outer_start_action, outer_id = parsed[0].group(1), parsed[0].group(2)  # type: ignore[union-attr]
    inner_start_action, inner_id = parsed[1].group(1), parsed[1].group(2)  # type: ignore[union-attr]
    inner_done_action, inner_done_id = parsed[2].group(1), parsed[2].group(2)  # type: ignore[union-attr]
    outer_done_action, outer_done_id = parsed[3].group(1), parsed[3].group(2)  # type: ignore[union-attr]

    assert (outer_start_action, inner_start_action, inner_done_action, outer_done_action) == (
        "START",
        "START",
        "DONE",
        "DONE",
    )

    assert outer_id == outer_done_id
    assert inner_id == inner_done_id
    assert outer_id != inner_id

    # And the messages should match the parsed ids (alignment on DONE is important).
    assert msgs == [
        f"[START {outer_id}] outer",
        f"[START {inner_id}] inner",
        f"[DONE  {inner_id}] inner",
        f"[DONE  {outer_id}] outer",
    ]


def test_log_truncates_large_result_repr(caplog: pytest.LogCaptureFixture):
    """Covers `_safe_repr` truncation in normal decorator flow."""

    from etl_decorators.logging import log
    from etl_decorators.logging.decorator import _safe_repr

    caplog.set_level("DEBUG", logger=__name__)
    caplog.clear()

    @log(level="INFO", with_arguments=False, with_result=True, with_duration=False)
    def step() -> str:
        return "x" * 10_000

    step()
    loc = _defined_location(step)
    assert len(caplog.records) == 2
    assert [r.levelno for r in caplog.records] == [logging.INFO, logging.INFO]
    assert [r.name for r in caplog.records] == [__name__, __name__]
    run_id = caplog.records[0].getMessage().split()[1].rstrip("]")
    assert re.fullmatch(r"[0-9a-f]{8}", run_id)
    assert _messages(caplog) == [
        f"[START {run_id}] {loc} step",
        f"[DONE  {run_id}] {loc} step: result={_safe_repr('x' * 10_000)}",
    ]


def test_safe_repr_defensive_branches_are_covered():
    """Cover defensive code paths in internal helpers."""

    from etl_decorators.logging.decorator import _safe_repr

    class BadRepr:
        def __repr__(self) -> str:  # noqa: D401
            raise RuntimeError("nope")

    assert _safe_repr(BadRepr()) == "<unreprable BadRepr>"

    # max_len <= 3 special-casing
    assert _safe_repr("abcdef", max_len=3) == "'ab"  # repr('abcdef') starts with quotes


def test_log_async_with_arguments_false(caplog: pytest.LogCaptureFixture):
    from etl_decorators.logging import log

    caplog.set_level("DEBUG", logger=__name__)
    caplog.clear()

    @log(level="DEBUG", with_arguments=False, with_result=False, with_duration=False)
    async def step(x: int) -> int:
        return x + 1

    assert asyncio.run(step(1)) == 2
    loc = _defined_location(step)
    assert len(caplog.records) == 2
    assert [r.levelno for r in caplog.records] == [logging.DEBUG, logging.DEBUG]
    assert [r.name for r in caplog.records] == [__name__, __name__]
    run_id = caplog.records[0].getMessage().split()[1].rstrip("]")
    assert re.fullmatch(r"[0-9a-f]{8}", run_id)
    assert _messages(caplog) == [
        f"[START {run_id}] {loc} step",
        f"[DONE  {run_id}] {loc} step",
    ]


def test_log_async_exception_is_logged_and_reraised(
    caplog: pytest.LogCaptureFixture,
    monkeypatch: pytest.MonkeyPatch,
):
    from etl_decorators.logging import log

    _freeze_perf_counter(monkeypatch)
    _freeze_run_id(monkeypatch)
    caplog.set_level("DEBUG", logger=__name__)
    caplog.clear()

    @log(level="WARNING", with_arguments=False, with_result=True, with_duration=True)
    async def step() -> int:
        raise ValueError("boom")

    with pytest.raises(ValueError, match="boom"):
        asyncio.run(step())

    loc = _defined_location(step)

    assert len(caplog.records) == 2
    start_rec, failed_rec = caplog.records
    assert start_rec.levelno == logging.WARNING
    assert failed_rec.levelno == logging.WARNING
    assert start_rec.name == __name__
    assert failed_rec.name == __name__
    assert _messages(caplog) == [
        f"[START f4b09ae5] {loc} step",
        f"[ERROR f4b09ae5] {loc} step: duration=0.123456s, exc=ValueError: boom",
    ]
    assert start_rec.exc_info is None
    assert failed_rec.exc_info is not None


def test_generate_run_id_shape_and_charset():
    from etl_decorators.logging.decorator import _generate_run_id

    run_id = _generate_run_id()
    assert len(run_id) == 8
    assert re.fullmatch(r"[0-9a-f]{8}", run_id)
