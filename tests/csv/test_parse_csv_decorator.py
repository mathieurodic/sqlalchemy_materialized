import asyncio
import io
import sys
import types
from datetime import datetime

import pytest


def _install_fake_pandas(monkeypatch: pytest.MonkeyPatch):
    """Install a tiny fake pandas module.

    We only implement what `etl_decorators.csv.parse_csv` uses:
    - pandas.read_csv(file_like, **kwargs)
    - returns DataFrame-like with .columns + .iterrows()
    - and a chunked variant when chunksize is provided.
    """

    pd = types.ModuleType("pandas")

    class FakeDataFrame:
        def __init__(self, columns, records):
            self.columns = columns
            self._records = records

        def iterrows(self):
            for i, rec in enumerate(self._records):
                yield i, rec

    class FakeTextFileReader:
        def __init__(self, chunks):
            self._chunks = chunks

        def __iter__(self):
            return iter(self._chunks)

    def read_csv(buf, **kwargs):
        text = buf.getvalue()
        sep = kwargs.get("sep", ",")

        # extremely small CSV parser: assumes header + no escaped seps
        lines = [ln for ln in text.splitlines() if ln.strip()]
        header = lines[0].split(sep)
        rows = []
        for ln in lines[1:]:
            parts = ln.split(sep)
            rec = {h: parts[idx] if idx < len(parts) else "" for idx, h in enumerate(header)}
            rows.append(rec)

        chunksize = kwargs.get("chunksize")
        if chunksize:
            chunks = []
            for i in range(0, len(rows), int(chunksize)):
                chunks.append(FakeDataFrame(header, rows[i : i + int(chunksize)]))
            return FakeTextFileReader(chunks)

        return FakeDataFrame(header, rows)

    pd.read_csv = read_csv
    monkeypatch.setitem(sys.modules, "pandas", pd)


def test_parse_csv_missing_pandas_raises():
    from etl_decorators.csv import parse_csv

    @parse_csv
    def get_csv():
        return "a,b\n1,2\n"

    with pytest.raises(RuntimeError, match="pandas is required"):
        list(get_csv())


def test__read_csv_payload_supports_string_path_and_filelike(tmp_path):
    from etl_decorators.csv import _read_csv_payload

    p = tmp_path / "x.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")

    text, src = _read_csv_payload(str(p))
    assert "a,b" in text
    assert src and src.endswith("x.csv")

    text2, src2 = _read_csv_payload(io.BytesIO(b"a,b\n1,2\n"))
    assert text2.startswith("a,b")
    assert src2 is None


def test__read_csv_payload_path_object(tmp_path):
    from etl_decorators.csv import _read_csv_payload

    p = tmp_path / "x.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")

    text, src = _read_csv_payload(p)
    assert text.startswith("a,b")
    assert src and src.endswith("x.csv")


def test__read_csv_payload_filelike_str_and_invalid_type():
    from etl_decorators.csv import _read_csv_payload

    assert _read_csv_payload(io.StringIO("a,b\n1,2\n"))[0].startswith("a,b")

    class Bad:
        def read(self):
            return 123

    with pytest.raises(TypeError, match="expected str or bytes"):
        _read_csv_payload(Bad())


def test__read_csv_payload_invalid_payload_type_raises():
    from etl_decorators.csv import _read_csv_payload

    with pytest.raises(TypeError, match="must return a file path"):
        _read_csv_payload(123)


def test__parse_scalar_more_branches(monkeypatch: pytest.MonkeyPatch):
    from etl_decorators.csv import _parse_scalar

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
    from etl_decorators.csv import scalar as scalar_mod

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

    from etl_decorators.csv import scalar as scalar_mod

    def _raise(*_a, **_k):
        raise RuntimeError("re broken")

    monkeypatch.setattr(scalar_mod.re, "fullmatch", _raise)
    assert scalar_mod._parse_scalar("123", auto_datetime=False) == "123"


def test__sniff_csv_dialect_fallback_when_sniffer_fails(monkeypatch: pytest.MonkeyPatch):
    from etl_decorators.csv import dialect as dialect_mod

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


def test_parse_csv_empty_input_returns_empty_iterator(monkeypatch: pytest.MonkeyPatch):
    _install_fake_pandas(monkeypatch)

    from etl_decorators.csv import parse_csv

    @parse_csv
    def get_csv():
        return "\n\n"

    assert list(get_csv()) == []


def test_parse_csv_read_csv_error_is_wrapped_and_mentions_path(monkeypatch: pytest.MonkeyPatch, tmp_path):
    _install_fake_pandas(monkeypatch)

    import pandas as pd

    def _raise(_buf, **_kwargs):
        raise ValueError("bad csv")

    monkeypatch.setattr(pd, "read_csv", _raise)

    from etl_decorators.csv import parse_csv

    p = tmp_path / "bad.csv"
    p.write_text("a,b\n1,2\n", encoding="utf-8")

    @parse_csv
    def get_csv():
        return p

    with pytest.raises(RuntimeError, match=r"Unable to parse CSV \(.*bad\.csv\):"):
        list(get_csv())


def test_parse_csv_with_fake_pandas_delimiter_sniff_and_types(monkeypatch: pytest.MonkeyPatch):
    _install_fake_pandas(monkeypatch)

    from etl_decorators.csv import parse_csv

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


def test_parse_csv_force_delimiter(monkeypatch: pytest.MonkeyPatch):
    _install_fake_pandas(monkeypatch)

    from etl_decorators.csv import parse_csv

    @parse_csv(delimiter=":")
    def get_csv():
        return "a:b\n1:2\n"

    assert list(get_csv()) == [{"a": 1, "b": 2}]


def test_parse_csv_chunksize_yields_rows_incrementally(monkeypatch: pytest.MonkeyPatch):
    _install_fake_pandas(monkeypatch)

    from etl_decorators.csv import parse_csv

    @parse_csv(chunksize=2)
    def get_csv():
        return "a,b\n1,2\n3,4\n5,6\n"

    it = get_csv()
    assert next(it) == {"a": 1, "b": 2}
    assert next(it) == {"a": 3, "b": 4}
    assert next(it) == {"a": 5, "b": 6}


def test_parse_csv_async_wrapper(monkeypatch: pytest.MonkeyPatch):
    _install_fake_pandas(monkeypatch)

    from etl_decorators.csv import parse_csv

    @parse_csv
    async def get_csv():
        return "a,b\n1,2\n"

    rows = asyncio.run(get_csv())
    assert list(rows) == [{"a": 1, "b": 2}]
