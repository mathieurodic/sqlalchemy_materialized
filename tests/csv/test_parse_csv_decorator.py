import asyncio
import io
import builtins
from datetime import datetime

import pytest

import pandas as pd

from etl_decorators.csv import _parse_scalar, _read_csv_payload, parse_csv
from etl_decorators.csv import dialect as dialect_mod
from etl_decorators.csv import scalar as scalar_mod


def test_parse_csv_missing_pandas_raises(monkeypatch: pytest.MonkeyPatch):
    @parse_csv
    def get_csv():
        return "a,b\n1,2\n"

    # Make the test deterministic by simulating a missing optional dependency.
    # We still run the full test suite in an environment where pandas exists.
    real_import = builtins.__import__

    def _fake_import(name, globals=None, locals=None, fromlist=(), level=0):  # noqa: ANN001
        if name == "pandas":
            raise ImportError("pandas missing")
        return real_import(name, globals, locals, fromlist, level)

    monkeypatch.setattr(builtins, "__import__", _fake_import)

    with pytest.raises(RuntimeError, match="pandas is required"):
        list(get_csv())


def test__read_csv_payload_supports_string_path_and_filelike(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")

    text, src = _read_csv_payload(str(p))
    assert "a,b" in text
    assert src and src.endswith("x.csv")

    text2, src2 = _read_csv_payload(io.BytesIO(b"a,b\n1,2\n"))
    assert text2.startswith("a,b")
    assert src2 is None


def test__read_csv_payload_path_object(tmp_path):
    p = tmp_path / "x.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")

    text, src = _read_csv_payload(p)
    assert text.startswith("a,b")
    assert src and src.endswith("x.csv")


def test__read_csv_payload_filelike_str_and_invalid_type():
    assert _read_csv_payload(io.StringIO("a,b\n1,2\n"))[0].startswith("a,b")

    class Bad:
        def read(self):
            return 123

    with pytest.raises(TypeError, match="expected str or bytes"):
        _read_csv_payload(Bad())


def test__read_csv_payload_invalid_payload_type_raises():
    with pytest.raises(TypeError, match="must return a file path"):
        _read_csv_payload(123)


def test__parse_scalar_more_branches(monkeypatch: pytest.MonkeyPatch):
    assert _parse_scalar(None, auto_datetime=True) is None
    assert _parse_scalar(float("nan"), auto_datetime=True) is None
    assert _parse_scalar(12, auto_datetime=True) == 12  # non-string passthrough
    assert _parse_scalar("", auto_datetime=True) is None
    assert _parse_scalar("false", auto_datetime=True) is False
    assert _parse_scalar("2026-03-13 12:34:56Z", auto_datetime=True).tzinfo is not None

    # Cover the defensive "value != value" exception path.
    class _Weird:
        def __ne__(self, other):  # noqa: ANN001
            raise TypeError("no compare")

    w = _Weird()
    assert _parse_scalar(w, auto_datetime=True) is w  # non-string passthrough

    # Force datetime parsing to fail to cover fallback-to-string branch.
    class _FakeDatetime:
        @staticmethod
        def fromisoformat(_s: str):
            raise ValueError("boom")

    # Patch the module-level `datetime` symbol (not datetime.datetime.fromisoformat).
    monkeypatch.setattr(scalar_mod, "datetime", _FakeDatetime)
    assert _parse_scalar("2026-03-13", auto_datetime=True) == "2026-03-13"


def test__parse_scalar_covers_regex_exception_branches(monkeypatch: pytest.MonkeyPatch):
    """Cover defensive except blocks around re.fullmatch.

    Those are not normally triggered, but we keep them for robustness.
    """

    def _raise(*_a, **_k):
        raise RuntimeError("re broken")

    monkeypatch.setattr(scalar_mod.re, "fullmatch", _raise)
    assert scalar_mod._parse_scalar("123", auto_datetime=False) == "123"


def test__sniff_csv_dialect_fallback_when_sniffer_fails(monkeypatch: pytest.MonkeyPatch):
    def _sniff_fail(self, sample, delimiters=None):
        raise RuntimeError("nope")

    monkeypatch.setattr(dialect_mod._csv.Sniffer, "sniff", _sniff_fail)
    d = dialect_mod._sniff_csv_dialect(
        "a,b\n1,2\n",
        delimiter=None,
        quotechar=None,
        doublequote=None,
        quoting=None,
    )
    assert d.delimiter == ","


def test_parse_csv_empty_input_returns_empty_iterator():
    @parse_csv
    def get_csv():
        return "\n\n"

    assert list(get_csv()) == []


def test_parse_csv_read_csv_error_is_wrapped_and_mentions_path(monkeypatch: pytest.MonkeyPatch, tmp_path):
    def _raise(_buf, **_kwargs):
        raise ValueError("bad csv")

    monkeypatch.setattr(pd, "read_csv", _raise)

    p = tmp_path / "bad.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")

    @parse_csv
    def get_csv():
        return p

    with pytest.raises(RuntimeError, match=r"Unable to parse CSV \(.*bad\.csv\):"):
        list(get_csv())


def test_parse_csv_with_pandas_delimiter_sniff_and_types():
    @parse_csv
    def get_csv():
        # semicolon delimiter should be sniffed
        return "i;f;b;d;dt\n1;1.5;true;2026-03-13;2026-03-13 12:34:56\n"

    rows = list(get_csv())
    assert rows == [
        {
            "i": 1,
            "f": 1.5,
            "b": True,
            "d": datetime(2026, 3, 13),
            "dt": datetime(2026, 3, 13, 12, 34, 56),
        }
    ]


def test_parse_csv_force_delimiter():
    @parse_csv(delimiter=":")
    def get_csv():
        return "a:b\n1:2\n"

    assert list(get_csv()) == [{"a": 1, "b": 2}]


def test_parse_csv_chunksize_yields_rows_incrementally():
    @parse_csv(chunksize=2)
    def get_csv():
        return "a,b\n1,2\n3,4\n5,6\n"

    it = get_csv()
    assert next(it) == {"a": 1, "b": 2}
    assert next(it) == {"a": 3, "b": 4}
    assert next(it) == {"a": 5, "b": 6}


def test_parse_csv_as_tuple_default_skips_header_row():
    @parse_csv(as_dict=False)
    def get_csv():
        return "a,b\n1,2\n3,4\n"

    assert list(get_csv()) == [(1, 2), (3, 4)]


def test_parse_csv_as_tuple_skip_header_rows_zero_includes_first_row_as_data():
    @parse_csv(as_dict=False, skip_header_rows=0)
    def get_csv():
        return "a,b\n1,2\n"

    assert list(get_csv()) == [("a", "b"), (1, 2)]


def test_parse_csv_skip_header_rows_preamble_and_custom_header_row():
    @parse_csv(skip_header_rows=2)
    def get_csv():
        # row 0 is preamble, row 1 is header, data starts at row 2
        return "THIS IS A REPORT\nx,y\n1,2\n"

    assert list(get_csv()) == [{"x": 1, "y": 2}]


def test_parse_csv_skip_header_rows_zero_raises_in_dict_mode():
    @parse_csv(skip_header_rows=0)
    def get_csv():
        return "a,b\n1,2\n"

    with pytest.raises(ValueError, match="skip_header_rows must be >= 1"):
        list(get_csv())


def test_parse_csv_async_wrapper(monkeypatch: pytest.MonkeyPatch):
    @parse_csv
    async def get_csv():
        return "a,b\n1,2\n"

    rows = asyncio.run(get_csv())
    assert list(rows) == [{"a": 1, "b": 2}]
