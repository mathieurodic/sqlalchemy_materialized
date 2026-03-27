import asyncio
import builtins
import io
import sys

import pytest


def _purge_modules(prefix: str) -> None:
    """Remove a module and its submodules from sys.modules.

    This is important when simulating missing optional dependencies: if the
    module is already imported, patching __import__ alone would not exercise
    our lazy-import error paths.
    """

    for name in list(sys.modules.keys()):
        if name == prefix or name.startswith(prefix + "."):
            del sys.modules[name]


class _MissingDep:
    """Context manager to simulate ImportError for a given top-level module."""

    def __init__(self, monkeypatch: pytest.MonkeyPatch, module: str):
        self._monkeypatch = monkeypatch
        self._module = module

    def __enter__(self):
        _purge_modules(self._module)

        real_import = builtins.__import__

        def fake_import(name, globals=None, locals=None, fromlist=(), level=0):
            if name == self._module:
                raise ImportError(f"no {self._module}")
            return real_import(name, globals, locals, fromlist, level)

        self._monkeypatch.setattr(builtins, "__import__", fake_import)
        return self

    def __exit__(self, exc_type, exc, tb):
        # monkeypatch fixture restores __import__ automatically
        return False


def test__read_html_payload_filelike_stringio():
    from etl_decorators.html import _read_html_payload

    assert _read_html_payload(io.StringIO("<p>hi</p>")) == "<p>hi</p>"


def test__read_html_payload_filelike_invalid_return_type_raises():
    from etl_decorators.html import _read_html_payload

    class Bad:
        def read(self):
            return 123

    with pytest.raises(TypeError, match="expected str or bytes"):
        _read_html_payload(Bad())


def test__read_html_payload_invalid_payload_type_raises():
    from etl_decorators.html import _read_html_payload

    with pytest.raises(TypeError, match="must return a file path"):
        _read_html_payload(123)


def test__read_html_payload_from_path_object(tmp_path):
    from etl_decorators.html import _read_html_payload

    p = tmp_path / "page.html"
    p.write_text("<p>ok</p>", encoding="utf-8")
    assert _read_html_payload(p) == "<p>ok</p>"


def test__read_html_payload_from_str_path(tmp_path):
    from etl_decorators.html import _read_html_payload

    p = tmp_path / "page.html"
    p.write_text("<p>ok</p>", encoding="utf-8")
    assert _read_html_payload(str(p)) == "<p>ok</p>"


def test__read_html_payload_filelike_bytes_decodes_utf8():
    from etl_decorators.html import _read_html_payload

    assert _read_html_payload(io.BytesIO("café".encode("utf-8"))) == "café"


def test_require_bs4_missing_dependency_raises_runtimeerror(monkeypatch: pytest.MonkeyPatch):
    from etl_decorators.html.deps import _require_bs4

    with _MissingDep(monkeypatch, "bs4"):
        with pytest.raises(RuntimeError) as e:
            _require_bs4()

    assert "pip install etl-decorators[html]" in str(e.value)


def test_require_markdownify_missing_dependency_raises_runtimeerror(monkeypatch: pytest.MonkeyPatch):
    from etl_decorators.html.deps import _require_markdownify

    with _MissingDep(monkeypatch, "markdownify"):
        with pytest.raises(RuntimeError) as e:
            _require_markdownify()

    assert "pip install etl-decorators[html]" in str(e.value)


def test_parse_html_from_html_string_returns_soup():
    from etl_decorators.html import parse_html

    @parse_html
    def get_html():
        return "<html><body><h1>Hello</h1></body></html>"

    soup = get_html()
    assert soup.__class__.__name__ == "BeautifulSoup"
    assert soup.select_one("h1").get_text(strip=True) == "Hello"


def test_parse_html_from_path(tmp_path):
    from etl_decorators.html import parse_html

    p = tmp_path / "page.html"
    p.write_text("<div class='x'>ok</div>", encoding="utf-8")

    @parse_html
    def get_path():
        return str(p)

    soup = get_path()
    assert soup.select_one(".x").get_text(strip=True) == "ok"


def test_parse_html_from_filelike_bytesio():
    from etl_decorators.html import parse_html

    @parse_html
    def get_f():
        return io.BytesIO(b"<p>hi</p>")

    soup = get_f()
    assert soup.select_one("p").get_text(strip=True) == "hi"


def test_parse_html_extract_single_tag():
    from etl_decorators.html import parse_html

    @parse_html(extract=".item")
    def get_html():
        return "<div><span class='item'>a</span><span class='item'>b</span></div>"

    tag = get_html()
    assert tag.name == "span"
    assert tag.get_text(strip=True) == "a"


def test_parse_html_extract_missing_returns_none():
    from etl_decorators.html import parse_html

    @parse_html(extract=".missing")
    def get_html():
        return "<div><span>ok</span></div>"

    assert get_html() is None


def test_parse_html_extract_collection():
    from etl_decorators.html import parse_html

    @parse_html(extract=".item", extract_as_collection=True)
    def get_html():
        return "<div><span class='item'>a</span><span class='item'>b</span></div>"

    tags = get_html()
    assert isinstance(tags, list)
    assert [t.get_text(strip=True) for t in tags] == ["a", "b"]


def test_parse_html_extract_collection_missing_returns_empty_list():
    from etl_decorators.html import parse_html

    @parse_html(extract=".missing", extract_as_collection=True)
    def get_html():
        return "<div><span>ok</span></div>"

    assert get_html() == []


def test_parse_html_extract_tuple_first_match_per_selector():
    from etl_decorators.html import parse_html

    @parse_html(extract=("h1", ".item", ".missing"))
    def get_html():
        return "<div><h1>T</h1><span class='item'>a</span><span class='item'>b</span></div>"

    res = get_html()
    assert isinstance(res, list)
    assert res[0] is not None and res[0].name == "h1"
    assert res[1] is not None and res[1].name == "span"
    assert res[2] is None


def test_parse_html_extract_tuple_collection_per_selector():
    from etl_decorators.html import parse_html

    @parse_html(extract=(".item", "h1", ".missing"), extract_as_collection=True)
    def get_html():
        return "<div><h1>T</h1><span class='item'>a</span><span class='item'>b</span></div>"

    res = get_html()
    assert isinstance(res, list)
    assert len(res) == 3
    assert [t.get_text(strip=True) for t in res[0]] == ["a", "b"]
    assert [t.get_text(strip=True) for t in res[1]] == ["T"]
    assert res[2] == []


def test_parse_html_convert_to_markdown_full_doc():
    from etl_decorators.html import parse_html

    @parse_html(convert_to_markdown=True)
    def get_html():
        return "<h1>Title</h1><p>Hello <b>world</b></p>"

    md = get_html()
    assert isinstance(md, str)
    assert "Title" in md
    assert "world" in md


def test_parse_html_convert_to_markdown_fragment_single():
    from etl_decorators.html import parse_html

    @parse_html(extract="p", convert_to_markdown=True)
    def get_html():
        return "<div><p>Hello <b>world</b></p><p>Other</p></div>"

    md = get_html()
    assert isinstance(md, str)
    assert "Hello" in md
    assert "world" in md
    assert "Other" not in md


def test_parse_html_convert_to_markdown_fragment_collection():
    from etl_decorators.html import parse_html

    @parse_html(extract="p", extract_as_collection=True, convert_to_markdown=True)
    def get_html():
        return "<div><p>A</p><p>B</p></div>"

    md_list = get_html()
    assert isinstance(md_list, list)
    assert "A" in md_list[0]
    assert "B" in md_list[1]


def test_parse_html_convert_to_markdown_tuple_first_match_per_selector():
    from etl_decorators.html import parse_html

    @parse_html(extract=("h1", "p", ".missing"), convert_to_markdown=True)
    def get_html():
        return "<div><h1>Title</h1><p>Hello <b>world</b></p></div>"

    md_list = get_html()
    assert isinstance(md_list, list)
    assert "Title" in (md_list[0] or "")
    assert "Hello" in (md_list[1] or "")
    assert md_list[2] is None


def test_parse_html_convert_to_markdown_tuple_collection_per_selector():
    from etl_decorators.html import parse_html

    @parse_html(extract=("p", "h1", ".missing"), extract_as_collection=True, convert_to_markdown=True)
    def get_html():
        return "<div><h1>Title</h1><p>A</p><p>B</p></div>"

    md_lists = get_html()
    assert isinstance(md_lists, list)
    assert md_lists[0][0].strip() != ""
    assert "A" in md_lists[0][0]
    assert "B" in md_lists[0][1]
    assert "Title" in md_lists[1][0]
    assert md_lists[2] == []


def test_parse_html_async_wrapper():
    from etl_decorators.html import parse_html

    @parse_html(extract="h1")
    async def get_html():
        return "<h1>Hello</h1>"

    tag = asyncio.run(get_html())
    assert tag.name == "h1"
    assert tag.get_text(strip=True) == "Hello"
