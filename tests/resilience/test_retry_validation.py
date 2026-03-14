import pytest


def test_retry_validate_config_retry_on_class_not_exception_raises():
    from etl_decorators.resilience import retry

    class NotAnException:  # noqa: N801 - test-only
        pass

    with pytest.raises(TypeError, match="exception class"):
        retry(retry_on=NotAnException)(lambda: None)  # type: ignore[misc]


def test_retry_is_fatal_exception_cancelled_error():
    retry_mod = __import__("etl_decorators.resilience.retry", fromlist=["_is_fatal_exception"])

    # asyncio.CancelledError should never be retried.
    import asyncio

    assert retry_mod._is_fatal_exception(asyncio.CancelledError()) is True


def test_retry_should_retry_tuple_and_none_branch():
    retry_mod = __import__("etl_decorators.resilience.retry", fromlist=["_should_retry"])

    class A(RuntimeError):
        pass

    class B(RuntimeError):
        pass

    # The public decorator forbids retry_on=None and retry_if=None together, but
    # _should_retry still needs to behave defensively.
    assert (
        retry_mod._should_retry(A("x"), retry_on=None, retry_if=None) is False
    )

    # tuple retry_on branch
    assert retry_mod._should_retry(A("x"), retry_on=(A, B), retry_if=None) is True
    assert (
        retry_mod._should_retry(ValueError("x"), retry_on=(A, B), retry_if=None)
        is False
    )


def test_retry_validate_config_retry_on_tuple_empty_raises():
    from etl_decorators.resilience import retry

    with pytest.raises(TypeError, match="tuple must contain"):
        retry(retry_on=())(lambda: None)  # type: ignore[misc]


def test_retry_validate_config_retry_on_tuple_item_not_exception_raises():
    from etl_decorators.resilience import retry

    with pytest.raises(TypeError, match="tuple items"):
        retry(retry_on=(ValueError, 123))(lambda: None)  # type: ignore[misc]


def test_retry_validate_config_retry_on_invalid_type_raises():
    from etl_decorators.resilience import retry

    with pytest.raises(TypeError, match="retry_on"):
        retry(retry_on=123)(lambda: None)  # type: ignore[misc]


def test_retry_validate_config_retry_if_not_callable_raises():
    from etl_decorators.resilience import retry

    with pytest.raises(TypeError, match="retry_if"):
        retry(retry_if=123)(lambda: None)  # type: ignore[misc]


def test_retry_validate_config_interval_factor_jitter_constraints():
    from etl_decorators.resilience import retry

    with pytest.raises(ValueError, match="max_attempts"):
        retry(retry_on=Exception, max_attempts=0)(lambda: None)

    with pytest.raises(ValueError, match="interval"):
        retry(retry_on=Exception, interval=-1)(lambda: None)

    with pytest.raises(ValueError, match="factor"):
        retry(retry_on=Exception, factor=0.9)(lambda: None)

    with pytest.raises(ValueError, match="jitter"):
        retry(retry_on=Exception, jitter=-0.1)(lambda: None)
