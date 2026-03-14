import pytest


def test_require_stream_raises_typeerror_without_fn_name():
    from etl_decorators._base.streams import _require_stream

    with pytest.raises(TypeError, match=r"expected an Iterable or AsyncIterable return value, got int"):
        _require_stream(123)


def test_require_stream_raises_typeerror_with_fn_name_prefix():
    from etl_decorators._base.streams import _require_stream

    with pytest.raises(TypeError, match=r"f\(\): expected an Iterable or AsyncIterable return value, got int"):
        _require_stream(123, fn_name="f")