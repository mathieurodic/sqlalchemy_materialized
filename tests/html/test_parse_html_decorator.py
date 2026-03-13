import io
import sys
import types
import asyncio

import pytest


def _install_fake_bs4(monkeypatch: pytest.MonkeyPatch):
    """Install a tiny fake bs4 module for coverage tests.

    This allows exercising the decorator logic without requiring the optional
    dependency to be installed in the test environment.
    """

    bs4 = types.ModuleType("bs4")

    class _Tag:
        def __init__(self, name: str, text: str, html: str):
            self.name = name
            self._text = text
            self._html = html

        def get_text(self, strip: bool = False):
            return self._text.strip() if strip else self._text

        def __str__(self):
            return self._html

    class BeautifulSoup:
        def __init__(self, html: str, parser: str):
            self._html = html
            self._parser = parser

        def __str__(self):
            return self._html

        def select(self, selector: str):
            # Very small selector surface area just for unit tests.
            if selector == "p":
                # two <p>...</p> blocks
                if "<p>" not in self._html:
                    return []
                parts = self._html.split("<p>")[1:]
                out = []
                for part in parts:
                    txt = part.split("</p>")[0]
                    out.append(_Tag("p", txt, f"<p>{txt}</p>"))
                return out

            if selector == "h1":
                if "<h1>" not in self._html:
                    return []
                txt = self._html.split("<h1>", 1)[1].split("</h1>", 1)[0]
                return [_Tag("h1", txt, f"<h1>{txt}</h1>")]

            if selector == ".item":
                if "class='item'" not in self._html and 'class="item"' not in self._html:
                    return []
                # naive: split by occurrences of ">" after class attr
                # produce 2 spans when present in fixtures.
                out = []
                # handle single/double quotes in our test fixtures
                for marker in ["class='item'>", 'class="item">']:
                    if marker in self._html:
                        parts = self._html.split(marker)[1:]
                        for part in parts:
                            txt = part.split("</", 1)[0]
                            out.append(_Tag("span", txt, f"<span class=\"item\">{txt}</span>"))
                return out

            # anything else: no matches in our fixtures
            return []

        def select_one(self, selector: str):
            matches = self.select(selector)
            return matches[0] if matches else None

    bs4.BeautifulSoup = BeautifulSoup
    monkeypatch.setitem(sys.modules, "bs4", bs4)


def _install_fake_markdownify(monkeypatch: pytest.MonkeyPatch):
    md = types.ModuleType("markdownify")

    def markdownify(html: str) -> str:
        return f"MD:{html}"

    md.markdownify = markdownify
    monkeypatch.setitem(sys.modules, "markdownify", md)


def _has_html_deps() -> bool:
    try:
        import bs4  # noqa: F401

        return True
    except Exception:
        return False


def _has_markdownify() -> bool:
    try:
        import markdownify  # noqa: F401

        return True
    except Exception:
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


def test_parse_html_with_fake_deps_markdown_full_doc(monkeypatch: pytest.MonkeyPatch):
    """Covers convert_to_markdown=True branch without real deps installed."""

    _install_fake_bs4(monkeypatch)
    _install_fake_markdownify(monkeypatch)

    from etl_decorators.html import parse_html

    @parse_html(convert_to_markdown=True)
    def get_html():
        return "<h1>Title</h1><p>Hello</p>"

    out = get_html()
    assert out.startswith("MD:")
    assert "Title" in out


def test_parse_html_with_fake_deps_markdown_fragment_collection(monkeypatch: pytest.MonkeyPatch):
    _install_fake_bs4(monkeypatch)
    _install_fake_markdownify(monkeypatch)

    from etl_decorators.html import parse_html

    @parse_html(extract="p", extract_as_collection=True, convert_to_markdown=True)
    def get_html():
        return "<div><p>A</p><p>B</p></div>"

    out = get_html()
    assert out == ["MD:<p>A</p>", "MD:<p>B</p>"]


def test_parse_html_with_fake_deps_markdown_fragment_single(monkeypatch: pytest.MonkeyPatch):
    """Covers non-collection + convert_to_markdown=True extraction branch."""

    _install_fake_bs4(monkeypatch)
    _install_fake_markdownify(monkeypatch)

    from etl_decorators.html import parse_html

    @parse_html(extract="h1", convert_to_markdown=True)
    def get_html():
        return "<div><h1>Hello</h1><h1>Other</h1></div>"

    out = get_html()
    assert out == "MD:<h1>Hello</h1>"


def test_parse_html_with_fake_bs4_non_markdown_paths(monkeypatch: pytest.MonkeyPatch):
    """Covers soup/matches/none branches of _process_payload without markdown."""

    _install_fake_bs4(monkeypatch)

    from etl_decorators.html import parse_html

    @parse_html
    def full_doc():
        return "<h1>Hello</h1>"

    soup = full_doc()
    assert soup.__class__.__name__ == "BeautifulSoup"

    @parse_html(extract="p", extract_as_collection=True)
    def coll_empty():
        return "<div>No paragraphs</div>"

    assert coll_empty() == []

    @parse_html(extract=".missing")
    def missing():
        return "<div>No match</div>"

    assert missing() is None


def test_parse_html_async_wrapper_with_fake_bs4(monkeypatch: pytest.MonkeyPatch):
    """Covers inspect.iscoroutinefunction -> async wrapper branch."""

    _install_fake_bs4(monkeypatch)

    from etl_decorators.html import parse_html

    @parse_html(extract="h1")
    async def get_html():
        return "<h1>Hello</h1>"

    tag = asyncio.run(get_html())
    assert tag.name == "h1"
    assert tag.get_text(strip=True) == "Hello"


def test_parse_html_from_html_string_returns_soup():
    from etl_decorators.html import parse_html

    @parse_html
    def get_html():
        return "<html><body><h1>Hello</h1></body></html>"

    if not _has_html_deps():
        with pytest.raises(RuntimeError, match="beautifulsoup4 is required"):
            get_html()
        return

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

    if not _has_html_deps():
        with pytest.raises(RuntimeError, match="beautifulsoup4 is required"):
            get_path()
        return

    soup = get_path()
    assert soup.select_one(".x").get_text(strip=True) == "ok"


def test_parse_html_from_filelike_bytesio():
    from etl_decorators.html import parse_html

    @parse_html
    def get_f():
        return io.BytesIO(b"<p>hi</p>")

    if not _has_html_deps():
        with pytest.raises(RuntimeError, match="beautifulsoup4 is required"):
            get_f()
        return

    soup = get_f()
    assert soup.select_one("p").get_text(strip=True) == "hi"


def test_parse_html_extract_single_tag():
    from etl_decorators.html import parse_html

    @parse_html(extract=".item")
    def get_html():
        return "<div><span class='item'>a</span><span class='item'>b</span></div>"

    if not _has_html_deps():
        with pytest.raises(RuntimeError, match="beautifulsoup4 is required"):
            get_html()
        return

    tag = get_html()
    assert tag.name == "span"
    assert tag.get_text(strip=True) == "a"


def test_parse_html_extract_missing_returns_none():
    from etl_decorators.html import parse_html

    @parse_html(extract=".missing")
    def get_html():
        return "<div><span>ok</span></div>"

    if not _has_html_deps():
        with pytest.raises(RuntimeError, match="beautifulsoup4 is required"):
            get_html()
        return

    assert get_html() is None


def test_parse_html_extract_collection():
    from etl_decorators.html import parse_html

    @parse_html(extract=".item", extract_as_collection=True)
    def get_html():
        return "<div><span class='item'>a</span><span class='item'>b</span></div>"

    if not _has_html_deps():
        with pytest.raises(RuntimeError, match="beautifulsoup4 is required"):
            get_html()
        return

    tags = get_html()
    assert isinstance(tags, list)
    assert [t.get_text(strip=True) for t in tags] == ["a", "b"]


def test_parse_html_extract_collection_missing_returns_empty_list():
    from etl_decorators.html import parse_html

    @parse_html(extract=".missing", extract_as_collection=True)
    def get_html():
        return "<div><span>ok</span></div>"

    if not _has_html_deps():
        with pytest.raises(RuntimeError, match="beautifulsoup4 is required"):
            get_html()
        return

    assert get_html() == []


def test_parse_html_convert_to_markdown_full_doc():
    from etl_decorators.html import parse_html

    @parse_html(convert_to_markdown=True)
    def get_html():
        return "<h1>Title</h1><p>Hello <b>world</b></p>"

    if not _has_html_deps():
        with pytest.raises(RuntimeError, match="beautifulsoup4 is required"):
            get_html()
        return
    if not _has_markdownify():
        with pytest.raises(RuntimeError, match="markdownify is required"):
            get_html()
        return

    md = get_html()
    assert isinstance(md, str)
    assert "Title" in md
    assert "world" in md


def test_parse_html_convert_to_markdown_fragment_single():
    from etl_decorators.html import parse_html

    @parse_html(extract="p", convert_to_markdown=True)
    def get_html():
        return "<div><p>Hello <b>world</b></p><p>Other</p></div>"

    if not _has_html_deps():
        with pytest.raises(RuntimeError, match="beautifulsoup4 is required"):
            get_html()
        return
    if not _has_markdownify():
        with pytest.raises(RuntimeError, match="markdownify is required"):
            get_html()
        return

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

    if not _has_html_deps():
        with pytest.raises(RuntimeError, match="beautifulsoup4 is required"):
            get_html()
        return
    if not _has_markdownify():
        with pytest.raises(RuntimeError, match="markdownify is required"):
            get_html()
        return

    md_list = get_html()
    assert isinstance(md_list, list)
    assert "A" in md_list[0]
    assert "B" in md_list[1]
