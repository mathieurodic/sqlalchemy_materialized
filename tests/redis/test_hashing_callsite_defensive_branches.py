from __future__ import annotations

import textwrap

import pytest

from etl_decorators.redis.hashing import callsite_code_hash


def test_callsite_code_hash_raises_on_invalid_start_line(tmp_path):
    p = tmp_path / "m.py"
    p.write_text("def f():\n    return 1\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match=r"Invalid start line"):
        callsite_code_hash(filename=str(p), start_lineno=0)


def test_callsite_code_hash_raises_on_start_line_out_of_range(tmp_path):
    p = tmp_path / "m.py"
    p.write_text("def f():\n    return 1\n", encoding="utf-8")

    with pytest.raises(RuntimeError, match=r"out of range"):
        callsite_code_hash(filename=str(p), start_lineno=999)


def test_callsite_code_hash_raises_on_reached_eof_after_blanks(tmp_path):
    p = tmp_path / "m.py"
    p.write_text("\n\n   \n", encoding="utf-8")

    with pytest.raises(RuntimeError, match=r"reached EOF"):
        callsite_code_hash(filename=str(p), start_lineno=1)


def test_callsite_code_hash_raises_when_missing_def_after_decorators(tmp_path):
    src = textwrap.dedent(
        """
        # END
        @x
        @y
        """
    )
    p = tmp_path / "m.py"
    p.write_text(src, encoding="utf-8")

    end = 1 + src.splitlines().index("# END")
    with pytest.raises(RuntimeError, match=r"missing def"):
        callsite_code_hash(filename=str(p), start_lineno=end + 1)


def test_callsite_code_hash_raises_on_def_indentation_mismatch(tmp_path):
    # This branch is subtle to hit because the parser only *recognizes* a def
    # line when `indent == base_indent`. We exploit a tab-indented def line:
    # - base_indent is computed with lstrip(" ") so a leading tab is treated as
    #   indent=0.
    # - with base_indent coming from the space-indented decorator line (4), the
    #   tab-indented def triggers `indent < base_indent` => break, then the
    #   indentation mismatch check fires.
    src = "# END\nclass C:\n    @deco\n\tdef f():\n        return 1\n"
    p = tmp_path / "m.py"
    p.write_text(src, encoding="utf-8")
    # Start at the indented decorator line.
    start_lineno = 1 + src.splitlines().index("    @deco")

    with pytest.raises(RuntimeError, match=r"def indentation mismatch"):
        callsite_code_hash(filename=str(p), start_lineno=start_lineno)


def test_callsite_code_hash_ignores_comments_and_trailing_ws(tmp_path):
    # Same code semantics, different comments/whitespace.
    # Include a blank line after a decorator to exercise that parsing branch.
    src1 = "# END\n\n@deco\n\ndef f():\n    x = 1  # comment\n    return x\n"
    src2 = "# END\n\n@deco\n\ndef f():\n    x = 1\t\n    return x\n"

    p1 = tmp_path / "a.py"
    p2 = tmp_path / "b.py"
    p1.write_text(src1, encoding="utf-8")
    p2.write_text(src2, encoding="utf-8")

    end1 = 1 + src1.splitlines().index("# END")
    end2 = 1 + src2.splitlines().index("# END")

    h1 = callsite_code_hash(filename=str(p1), start_lineno=end1 + 1)
    h2 = callsite_code_hash(filename=str(p2), start_lineno=end2 + 1)
    assert h1 == h2


def test_callsite_code_hash_raises_when_source_file_missing(tmp_path):
    missing = tmp_path / "nope.py"
    with pytest.raises(RuntimeError, match=r"Unable to read source file"):
        callsite_code_hash(filename=str(missing), start_lineno=1)


def test_callsite_code_hash_supports_async_def(tmp_path):
    src = "# END\nasync def f():\n    return 1\n"
    p = tmp_path / "m.py"
    p.write_text(src, encoding="utf-8")
    end = 1 + src.splitlines().index("# END")

    h = callsite_code_hash(filename=str(p), start_lineno=end + 1)
    assert isinstance(h, str)
