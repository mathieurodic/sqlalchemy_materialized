import asyncio
import io

import pytest


def test_parse_jsonl_from_string_skips_blank_lines():
    from etl_decorators.json import parse_jsonl

    @parse_jsonl
    def payload():
        return '{"id": 1}\n\n  \n{"id": 2}\n'

    assert list(payload()) == [{"id": 1}, {"id": 2}]


def test_parse_jsonl_from_path_is_lazy_enough(tmp_path):
    """Not a strict laziness proof, but ensures path input works."""

    from etl_decorators.json import parse_jsonl

    p = tmp_path / "x.jsonl"
    p.write_text('{"id": 1}\n{"id": 2}\n', encoding="utf-8")

    @parse_jsonl
    def payload():
        return p

    it = payload()
    assert next(it) == {"id": 1}
    assert next(it) == {"id": 2}


def test__iter_jsonl_payload_filelike_iterates_lines_and_decodes_bytes():
    from etl_decorators.json import _iter_jsonl_payload

    lines, src = _iter_jsonl_payload(io.BytesIO(b'{"id": 1}\n'))
    assert src is None
    # BytesIO is iterable but yields bytes
    assert list(lines) == ['{"id": 1}\n']


def test__iter_jsonl_payload_from_str_path(tmp_path):
    from etl_decorators.json import _iter_jsonl_payload

    p = tmp_path / "x.jsonl"
    p.write_text('{"id": 1}\n', encoding="utf-8")

    lines, src = _iter_jsonl_payload(str(p))
    assert src == str(p)
    assert list(lines) == ['{"id": 1}\n']


def test__iter_jsonl_payload_iterable_yielding_str_lines():
    from etl_decorators.json import _iter_jsonl_payload

    class StrIterable:
        def __iter__(self):
            yield '{"id": 1}\n'

    lines, src = _iter_jsonl_payload(StrIterable())
    assert src is None
    assert list(lines) == ['{"id": 1}\n']


def test__iter_and_close_swallows_close_errors():
    from etl_decorators.json.payload import _iter_and_close

    class FauxFile:
        def __iter__(self):
            yield "a\n"

        def close(self):
            raise RuntimeError("close failed")

    assert list(_iter_and_close(FauxFile())) == ["a\n"]


def test__iter_jsonl_payload_filelike_iterates_invalid_line_type_raises():
    from etl_decorators.json import _iter_jsonl_payload

    class BadIterable:
        def __iter__(self):
            yield 1

    lines, src = _iter_jsonl_payload(BadIterable())
    assert src is None
    with pytest.raises(TypeError, match=r"iterating non str/bytes"):
        list(lines)


def test__iter_jsonl_payload_readonly_filelike_bytes_fallback():
    from etl_decorators.json import _iter_jsonl_payload

    class ReadOnly:
        def read(self):
            return b'{"id": 1}\n'

    lines, src = _iter_jsonl_payload(ReadOnly())
    assert src is None
    assert list(lines) == ['{"id": 1}']


def test__iter_jsonl_payload_readonly_filelike_str_fallback():
    from etl_decorators.json import _iter_jsonl_payload

    class ReadOnly:
        def read(self):
            return '{"id": 1}\n'

    lines, src = _iter_jsonl_payload(ReadOnly())
    assert src is None
    assert list(lines) == ['{"id": 1}']


def test__iter_jsonl_payload_readonly_filelike_invalid_read_return_type_raises():
    from etl_decorators.json import _iter_jsonl_payload

    class ReadOnly:
        def read(self):
            return 123

    with pytest.raises(TypeError, match=r"expected str or bytes"):
        _iter_jsonl_payload(ReadOnly())


def test__iter_jsonl_payload_invalid_payload_type_raises():
    from etl_decorators.json import _iter_jsonl_payload

    with pytest.raises(TypeError, match=r"must return a file path"):
        _iter_jsonl_payload(object())


def test_parse_jsonl_invalid_line_raises_valueerror_with_line_number():
    from etl_decorators.json import parse_jsonl

    @parse_jsonl
    def payload():
        return '{"id": 1}\n{\n{"id": 2}\n'

    with pytest.raises(ValueError, match=r"at line 2"):
        list(payload())


def test_parse_jsonl_valid_but_scalar_line_raises():
    from etl_decorators.json import parse_jsonl

    @parse_jsonl
    def payload():
        return '"hello"\n'

    with pytest.raises(ValueError, match=r"expects each line to be a JSON object or array"):
        list(payload())


def test_parse_jsonl_async_wrapper():
    from etl_decorators.json import parse_jsonl

    @parse_jsonl
    async def payload():
        return '{"id": 1}\n'

    out = asyncio.run(payload())
    assert list(out) == [{"id": 1}]
