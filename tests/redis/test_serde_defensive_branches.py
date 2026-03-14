from __future__ import annotations

import json
import sys
import types

import pytest


def test_resolve_model_class_returns_none_when_pydantic_missing(monkeypatch):
    from etl_decorators.redis import serde

    monkeypatch.setattr(serde, "_maybe_pydantic_base_model", lambda: None)
    assert serde._resolve_model_class({"module": "m", "qualname": "Q"}) is None


def test_resolve_model_class_returns_none_when_module_or_qualname_missing(monkeypatch):
    from etl_decorators.redis import serde

    class Base:
        pass

    monkeypatch.setattr(serde, "_maybe_pydantic_base_model", lambda: Base)
    assert serde._resolve_model_class({"module": "m"}) is None
    assert serde._resolve_model_class({"qualname": "Q"}) is None


def test_resolve_model_class_success_and_non_subclass_fallback(monkeypatch):
    from etl_decorators.redis import serde

    class Base:
        pass

    # Create a throwaway module with a nested class.
    mod = types.ModuleType("_etl_decorators_test_mod")

    class M(Base):
        pass

    class NotModel:
        pass

    mod.M = M
    mod.NotModel = NotModel
    mod.value = 123
    sys.modules[mod.__name__] = mod

    monkeypatch.setattr(serde, "_maybe_pydantic_base_model", lambda: Base)

    assert serde._resolve_model_class({"module": mod.__name__, "qualname": "M"}) is M
    assert serde._resolve_model_class({"module": mod.__name__, "qualname": "NotModel"}) is None
    assert serde._resolve_model_class({"module": mod.__name__, "qualname": "value"}) is None


def test_iter_pydantic_subclasses_swallows_subclass_introspection_errors():
    from etl_decorators.redis.serde import _iter_pydantic_subclasses

    class Base:
        pass

    class Broken(Base):
        @classmethod
        def __subclasses__(cls):
            raise RuntimeError("boom")

    # Should not raise.
    subs = _iter_pydantic_subclasses(Base)
    assert Broken in subs


def test_find_model_by_name_and_schema_returns_none_when_pydantic_missing(monkeypatch):
    from etl_decorators.redis import serde

    monkeypatch.setattr(serde, "_maybe_pydantic_base_model", lambda: None)
    assert serde._find_model_by_name_and_schema("M", {}) is None


def test_find_model_by_name_and_schema_continues_on_schema_errors(monkeypatch):
    """Covers the try/except in `_find_model_by_name_and_schema`."""

    from etl_decorators.redis import serde

    class Base:
        pass

    Bad = type(
        "M",
        (Base,),
        {"model_json_schema": classmethod(lambda cls: (_ for _ in ()).throw(RuntimeError("boom")))},
    )

    monkeypatch.setattr(serde, "_maybe_pydantic_base_model", lambda: Base)
    monkeypatch.setattr(serde, "_iter_pydantic_subclasses", lambda _cls: [Bad])

    assert serde._find_model_by_name_and_schema("M", {}) is None


def test_schemas_compatible_returns_false_on_eq_errors():
    from etl_decorators.redis.serde import _schemas_compatible

    class EqBoom:
        def __eq__(self, other):  # noqa: ANN001
            raise RuntimeError("boom")

    assert _schemas_compatible(EqBoom(), EqBoom()) is False


def test_best_effort_model_from_schema_returns_none_when_base_model_missing(monkeypatch):
    from etl_decorators.redis import serde

    monkeypatch.setattr(serde, "_maybe_pydantic_base_model", lambda: None)
    assert serde._best_effort_model_from_schema("M", {"type": "object"}) is None


def test_best_effort_model_from_schema_exercises_map_type_fallbacks(monkeypatch):
    """Hit `map_type` branches that return Any for odd schemas."""

    pytest.importorskip("pydantic")
    from etl_decorators.redis import serde

    schema = {
        "title": "M",
        "type": "object",
        "properties": {
            "nondict": "oops",  # not a dict -> Any
            "union": {"type": ["string", "integer"]},  # ambiguous union -> Any
            "unknown": {"type": "null"},  # unknown -> Any
        },
        # none required => all optional => we also exercise optionality logic
        "required": [],
    }

    Model = serde._best_effort_model_from_schema("M", schema)
    assert Model is not None


def test_best_effort_model_from_schema_optional_union_failure_sets_any(monkeypatch):
    """Cover the `except: py_t = Any` branch when `py_t | None` fails."""

    pytest.importorskip("pydantic")
    import pydantic

    from etl_decorators.redis import serde

    class BadAny:
        pass

    # Ensure `BadAny | None` fails.
    monkeypatch.setattr(serde, "Any", BadAny())

    # Make create_model tolerate our weird annotation.
    monkeypatch.setattr(pydantic, "create_model", lambda *a, **k: object())

    schema = {
        "title": "M",
        "type": "object",
        "properties": {"x": {"type": "string"}},
        # x not required => optional => triggers `py_t | None` attempt
        "required": [],
    }

    assert serde._best_effort_model_from_schema("M", schema) is not None


def test_best_effort_model_from_schema_returns_none_when_create_model_raises(monkeypatch):
    pytest.importorskip("pydantic")
    import pydantic

    from etl_decorators.redis import serde

    monkeypatch.setattr(pydantic, "create_model", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))

    schema = {
        "title": "M",
        "type": "object",
        "properties": {"x": {"type": "string"}},
        "required": ["x"],
    }

    assert serde._best_effort_model_from_schema("M", schema) is None


def test_loads_envelope_model_validate_error_falls_back_to_obj(monkeypatch):
    """Cover the `except Exception: pass` around model_validate."""

    pytest.importorskip("pydantic")
    from pydantic import BaseModel

    from etl_decorators.redis import serde

    class Broken(BaseModel):
        x: int

        @classmethod
        def model_validate(cls, _payload):  # noqa: ANN001
            raise RuntimeError("boom")

    env = {
        "__etl_decorators__": "pydantic_v2",
        "model": {"module": "m", "qualname": "Q", "name": "Broken", "schema": None},
        "data": {"x": 1},
    }
    raw = json.dumps(env).encode("utf-8")

    monkeypatch.setattr(serde, "_maybe_pydantic_base_model", lambda: BaseModel)
    monkeypatch.setattr(serde, "_resolve_model_class", lambda info: Broken)
    monkeypatch.setattr(serde, "_schemas_compatible", lambda a, b: True)

    out = serde.loads(raw, serialization="json")
    assert isinstance(out, dict)


def test_loads_envelope_generated_model_validate_error_returns_obj(monkeypatch):
    """Cover the `except: return obj` branch for generated model validate."""

    pytest.importorskip("pydantic")
    from pydantic import BaseModel

    from etl_decorators.redis import serde

    env = {
        "__etl_decorators__": "pydantic_v2",
        "model": {"module": None, "qualname": None, "name": "M", "schema": {"type": "object"}},
        "data": {"x": 1},
    }
    raw = json.dumps(env).encode("utf-8")

    monkeypatch.setattr(serde, "_maybe_pydantic_base_model", lambda: BaseModel)
    monkeypatch.setattr(serde, "_resolve_model_class", lambda info: None)
    monkeypatch.setattr(serde, "_find_model_by_name_and_schema", lambda name, schema: None)

    class Gen:
        @classmethod
        def model_validate(cls, _payload):  # noqa: ANN001
            raise RuntimeError("boom")

    monkeypatch.setattr(serde, "_best_effort_model_from_schema", lambda name, schema: Gen)

    out = serde.loads(raw, serialization="json")
    assert isinstance(out, dict)
