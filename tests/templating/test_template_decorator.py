import asyncio
import io
from pathlib import Path

import pytest


def test__read_template_payload_from_str_returns_str():
    from etl_decorators.templating import _read_template_payload

    assert _read_template_payload("Hello {{ name }}") == "Hello {{ name }}"


def test__read_template_payload_from_path_object(tmp_path: Path):
    from etl_decorators.templating import _read_template_payload

    p = tmp_path / "x.j2"
    p.write_text("Hi {{ who }}", encoding="utf-8")
    assert _read_template_payload(p) == "Hi {{ who }}"


def test__read_template_payload_from_str_path(tmp_path: Path):
    from etl_decorators.templating import _read_template_payload

    p = tmp_path / "x.j2"
    p.write_text("Hi {{ who }}", encoding="utf-8")
    assert _read_template_payload(str(p)) == "Hi {{ who }}"


def test__read_template_payload_filelike_bytes_decodes_utf8():
    from etl_decorators.templating import _read_template_payload

    assert _read_template_payload(io.BytesIO("café".encode("utf-8"))) == "café"


def test__read_template_payload_filelike_invalid_return_type_raises():
    from etl_decorators.templating import _read_template_payload

    class Bad:
        def read(self):
            return 123

    with pytest.raises(TypeError, match="expected str or bytes"):
        _read_template_payload(Bad())


def test_template_renders_from_string():
    from etl_decorators.templating import template

    @template
    def prompt(name: str):
        return "Hello {{ name }}"

    assert prompt("Ada") == "Hello Ada"


def test_template_renders_from_filelike():
    from etl_decorators.templating import template

    @template
    def prompt(name: str):
        return io.StringIO("Hello {{ name }}")

    assert prompt("Ada") == "Hello Ada"


def test_template_renders_from_path(tmp_path: Path):
    from etl_decorators.templating import template

    p = tmp_path / "hello.j2"
    p.write_text("Hello {{ name }}", encoding="utf-8")

    @template
    def prompt(name: str):
        return p

    assert prompt("Ada") == "Hello Ada"


def test_template_missing_var_raises():
    from etl_decorators.templating import template

    @template
    def prompt():
        return "Hello {{ missing }}"

    # Jinja2 StrictUndefined raises UndefinedError
    with pytest.raises(Exception):
        prompt()


def test_template_method_self_is_just_a_param():
    from etl_decorators.templating import template

    class Greeter:
        def __init__(self, who: str):
            self.who = who

        @property
        def slow(self):
            raise RuntimeError("property should not be accessed implicitly")

        @template
        def msg(self, punct: str = "!"):
            # The decorator rewrites `self.` -> `self_.` inside Jinja blocks.
            return "Hello {{ self.who }}{{ punct }}"

    g = Greeter("Ada")
    assert g.msg() == "Hello Ada!"


def test_template_async_wrapper():
    from etl_decorators.templating import template

    @template
    async def prompt(name: str):
        return "Hello {{ name }}"

    assert asyncio.run(prompt("Ada")) == "Hello Ada"
