import pytest


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
