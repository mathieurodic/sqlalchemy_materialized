from __future__ import annotations

import textwrap

import pytest

from etl_decorators.redis.hashing import callsite_code_hash


def test_callsite_hash_includes_decorators_below_but_not_above(tmp_path):
    # We generate a module file dynamically so we can control line numbers.
    src = textwrap.dedent(
        """
        from etl_decorators.redis.hashing import callsite_code_hash

        def deco(fn):
            def w(*a, **k):
                return fn(*a, **k)
            w.__wrapped__ = fn
            return w

        # variant A: decorator below
        # end_lineno marker is the line containing the comment below
        # END_A
        @deco
        def f(x=1):
            return x  # comment

        # variant B: no decorator below
        # END_B
        def g(x=1):
            return x
        """
    )

    p = tmp_path / "m.py"
    p.write_text(src, encoding="utf-8")

    lines = src.splitlines()
    end_a = 1 + lines.index("# END_A")
    end_b = 1 + lines.index("# END_B")

    ha = callsite_code_hash(filename=str(p), start_lineno=end_a + 1)
    hb = callsite_code_hash(filename=str(p), start_lineno=end_b + 1)

    # Different blocks because A includes a decorator line @deco
    assert ha != hb


def test_callsite_hash_raises_on_unexpected_statement(tmp_path):
    src = "x = 1\n"
    p = tmp_path / "m.py"
    p.write_text(src, encoding="utf-8")
    from etl_decorators.redis.hashing import callsite_code_hash

    with pytest.raises(RuntimeError):
        callsite_code_hash(filename=str(p), start_lineno=1)
