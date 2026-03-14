from __future__ import annotations

import pytest


def test_serde_json_pydantic_roundtrip_when_model_resolves():
    pydantic = pytest.importorskip("pydantic")
    from pydantic import BaseModel

    from etl_decorators.redis.serde import dumps, loads

    class M(BaseModel):
        x: int

    m = M(x=1)

    raw = dumps(m, serialization="json")
    out = loads(raw, serialization="json")

    assert isinstance(out, M)
    assert out == m


def test_serde_json_pydantic_generated_model_fallback_when_unresolvable(monkeypatch):
    pydantic = pytest.importorskip("pydantic")
    from pydantic import BaseModel

    from etl_decorators.redis import serde

    class M(BaseModel):
        x: int

    m = M(x=2)
    raw = serde.dumps(m, serialization="json")

    # Force resolution to fail so we exercise the generated-model path.
    monkeypatch.setattr(serde, "_resolve_model_class", lambda info: None)
    monkeypatch.setattr(serde, "_find_model_by_name_and_schema", lambda name, schema: None)

    out = serde.loads(raw, serialization="json")
    assert isinstance(out, BaseModel)
    assert out.model_dump() == {"x": 2}


def test_serde_pickle_roundtrip():
    from etl_decorators.redis.serde import dumps, loads

    value = {"a": [1, 2], "b": ("x", None)}
    raw = dumps(value, serialization="pickle")
    out = loads(raw, serialization="pickle")
    assert out == value


def test_serde_json_pydantic_envelope_returned_when_pydantic_missing(monkeypatch):
    pydantic = pytest.importorskip("pydantic")
    from pydantic import BaseModel

    from etl_decorators.redis import serde

    class M(BaseModel):
        x: int

    raw = serde.dumps(M(x=1), serialization="json")

    # Pretend pydantic isn't installed at load time.
    monkeypatch.setattr(serde, "_maybe_pydantic_base_model", lambda: None)
    out = serde.loads(raw, serialization="json")
    assert isinstance(out, dict)
    assert out.get("__etl_decorators__") == "pydantic_v2"


def test_serde_json_plain_roundtrip_dict():
    from etl_decorators.redis.serde import dumps, loads

    raw = dumps({"a": 1, "b": ["x", None]}, serialization="json")
    assert loads(raw, serialization="json") == {"a": 1, "b": ["x", None]}


def test_serde_dumps_unsupported_serialization_raises():
    from etl_decorators.redis.serde import dumps

    with pytest.raises(ValueError, match=r"Unsupported serialization"):
        dumps({"a": 1}, serialization="nope")  # type: ignore[arg-type]


def test_serde_loads_unsupported_serialization_raises():
    from etl_decorators.redis.serde import loads

    with pytest.raises(ValueError, match=r"Unsupported serialization"):
        loads(b"{}", serialization="nope")  # type: ignore[arg-type]


def test_serde_json_envelope_uses_find_model_by_name_and_schema(monkeypatch):
    pytest.importorskip("pydantic")
    from pydantic import BaseModel

    from etl_decorators.redis import serde

    class M(BaseModel):
        x: int

    raw = serde.dumps(M(x=1), serialization="json")

    # Force import-based resolution to fail so we go through name+schema lookup.
    monkeypatch.setattr(serde, "_resolve_model_class", lambda info: None)

    called = {"n": 0}

    def finder(name, schema):
        called["n"] += 1
        return M

    monkeypatch.setattr(serde, "_find_model_by_name_and_schema", finder)

    out = serde.loads(raw, serialization="json")
    assert called["n"] >= 1
    assert isinstance(out, M)
    assert out.x == 1


def test_serde_find_model_by_name_and_schema_skips_broken_subclasses(monkeypatch):
    pytest.importorskip("pydantic")
    from pydantic import BaseModel

    from etl_decorators.redis import serde

    # The code only catches failures of `sub.__subclasses__()` (not the initial
    # BaseModel.__subclasses__() call). So we simulate a broken subclass in the
    # stack instead.

    class BrokenSub(BaseModel):
        @classmethod
        def __subclasses__(cls):
            raise RuntimeError("boom")

    monkeypatch.setattr(serde, "_iter_pydantic_subclasses", lambda _cls: [BrokenSub])

    # Should not raise; it should just ignore the broken subclass and return None.
    assert serde._find_model_by_name_and_schema("M", {}) is None
