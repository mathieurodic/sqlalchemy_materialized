import asyncio
import io

import pytest


def test_parse_json_from_string():
    from etl_decorators.json import parse_json

    @parse_json
    def payload():
        return '{"a": 1, "b": [1, 2]}'

    assert payload() == {"a": 1, "b": [1, 2]}


def test_parse_json_from_path(tmp_path):
    from etl_decorators.json import parse_json

    p = tmp_path / "x.json"
    p.write_text('{"x": 1}', encoding="utf-8")

    @parse_json
    def payload():
        return p

    assert payload() == {"x": 1}


def test_parse_json_from_str_path(tmp_path):
    from etl_decorators.json import parse_json

    p = tmp_path / "x.json"
    p.write_text('[{"x": 1}]', encoding="utf-8")

    @parse_json
    def payload():
        return str(p)

    assert payload() == [{"x": 1}]


def test__read_json_payload_filelike_bytesio_decodes_utf8():
    from etl_decorators.json import _read_json_payload

    text, name = _read_json_payload(io.BytesIO('{"a": "café"}'.encode("utf-8")))
    assert "café" in text
    assert name is None


def test__read_json_payload_filelike_stringio():
    from etl_decorators.json import _read_json_payload

    text, name = _read_json_payload(io.StringIO('{"a": 1}'))
    assert text == '{"a": 1}'
    assert name is None


def test__read_json_payload_filelike_invalid_return_type_raises():
    from etl_decorators.json import _read_json_payload

    class Bad:
        def read(self):
            return 123

    with pytest.raises(TypeError, match=r"expected str or bytes"):
        _read_json_payload(Bad())


def test__read_json_payload_invalid_payload_type_raises():
    from etl_decorators.json import _read_json_payload

    with pytest.raises(TypeError, match=r"must return a file path"):
        _read_json_payload(123)


def test_parse_json_invalid_json_raises_valueerror_and_mentions_source(tmp_path):
    from etl_decorators.json import parse_json

    p = tmp_path / "bad.json"
    p.write_text("{", encoding="utf-8")

    @parse_json
    def payload():
        return p

    with pytest.raises(ValueError, match=r"Unable to parse JSON \(.*bad\.json\):"):
        payload()


def test_parse_json_valid_but_scalar_raises_valueerror():
    from etl_decorators.json import parse_json

    @parse_json
    def payload():
        return '"hello"'

    with pytest.raises(ValueError, match=r"expects a JSON object or array"):
        payload()


def test_parse_json_async_wrapper():
    from etl_decorators.json import parse_json

    @parse_json
    async def payload():
        return '{"a": 1}'

    out = asyncio.run(payload())
    assert out == {"a": 1}
