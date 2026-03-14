import asyncio
import io

import pytest


def test__read_xml_payload_filelike_stringio():
    from etl_decorators.xml import _read_xml_payload

    text, name = _read_xml_payload(io.StringIO("<root/>"))
    assert text == "<root/>"
    assert name is None


def test__read_xml_payload_filelike_bytesio_decodes_and_uses_name():
    from etl_decorators.xml import _read_xml_payload

    bio = io.BytesIO(b"<root/>")
    # mimic real file objects which typically have a .name attribute
    bio.name = "source.xml"  # type: ignore[attr-defined]

    text, name = _read_xml_payload(bio)
    assert text == "<root/>"
    assert name == "source.xml"


def test__read_xml_payload_filelike_invalid_return_type_raises():
    from etl_decorators.xml import _read_xml_payload

    class Bad:
        def read(self):
            return 123

    with pytest.raises(TypeError, match="expected str or bytes"):
        _read_xml_payload(Bad())


def test__read_xml_payload_invalid_payload_type_raises():
    from etl_decorators.xml import _read_xml_payload

    with pytest.raises(TypeError, match="must return a file path"):
        _read_xml_payload(123)


def test__read_xml_payload_from_path_object(tmp_path):
    from etl_decorators.xml import _read_xml_payload

    p = tmp_path / "page.xml"
    p.write_text("<root/>", encoding="utf-8")
    text, name = _read_xml_payload(p)
    assert text == "<root/>"
    assert name.endswith("page.xml")


def test__read_xml_payload_from_str_path(tmp_path):
    from etl_decorators.xml import _read_xml_payload

    p = tmp_path / "page.xml"
    p.write_text("<root/>", encoding="utf-8")
    text, name = _read_xml_payload(str(p))
    assert text == "<root/>"
    assert name.endswith("page.xml")


def test_parse_xml_extract_none_returns_root_element():
    from etl_decorators.xml import parse_xml

    @parse_xml
    def payload():
        return """<root><item>a</item></root>"""

    root = payload()
    assert root.tag == "root"


def test_parse_xml_parse_error_includes_source_name():
    from etl_decorators.xml import parse_xml

    class F:
        name = "source.xml"

        def read(self):
            # invalid XML
            return b"<root"

    @parse_xml
    def payload():
        return F()

    with pytest.raises(ValueError, match=r"Unable to parse XML \(source\.xml\):"):
        payload()


def test_parse_xml_integration_xpath_collection_text():
    from etl_decorators.xml import parse_xml

    @parse_xml(extract="//item", extract_as_collection=True)
    def payload():
        return """<root><item>a</item><item>b</item></root>"""

    assert payload() == ["a", "b"]


def test_parse_xml_integration_xpath_elements_output():
    from etl_decorators.xml import parse_xml

    @parse_xml(extract="//item", extract_output="elements")
    def payload():
        return """<root><item>a</item><item>b</item></root>"""

    elem = payload()
    assert elem.tag == "item"


def test_parse_xml_integration_xpath_scalar_string_expression():
    from etl_decorators.xml import parse_xml

    @parse_xml(extract="string(//item[1])")
    def payload():
        return """<root><item>a</item><item>b</item></root>"""

    assert payload() == "a"


def test_parse_xml_integration_css_collection_text():
    from etl_decorators.xml import parse_xml

    @parse_xml(extract="item", extract_mode="css", extract_as_collection=True)
    def payload():
        return """<root><item>a</item><item>b</item></root>"""

    assert payload() == ["a", "b"]


def test_parse_xml_integration_css_collection_text_auto_mode_heuristic():
    """Covers the auto-mode heuristic falling back to CSS selectors."""

    from etl_decorators.xml import parse_xml

    # No XPath-looking tokens in selector => _resolve_extract_mode returns "css".
    @parse_xml(extract="item", extract_as_collection=True)
    def payload():
        return """<root><item>a</item><item>b</item></root>"""

    assert payload() == ["a", "b"]


def test_parse_xml_missing_selector_returns_none():
    from etl_decorators.xml import parse_xml

    @parse_xml(extract="//missing")
    def payload():
        return """<root><item>a</item></root>"""

    assert payload() is None


def test_normalize_extracted_none_branch_is_covered_directly():
    from etl_decorators.xml.decorator import _normalize_extracted

    assert _normalize_extracted(None) == []


def test_parse_xml_async_wrapper():
    from etl_decorators.xml import parse_xml

    @parse_xml(extract="//item")
    async def payload():
        return """<root><item>a</item><item>b</item></root>"""

    out = asyncio.run(payload())
    assert out == "a"
