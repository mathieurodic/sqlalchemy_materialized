from __future__ import annotations

from unittest.mock import MagicMock

from pydantic import BaseModel

from etl_decorators.sqlalchemy.type_decorators import _pydantic_json_core as core


class Payload(BaseModel):
    value: int


def test_mutable_pydantic_model_list_coerce_and_mutations():
    assert core._MutablePydanticModelList.coerce("x", None) is None

    original = core._MutablePydanticModelList()
    assert core._MutablePydanticModelList.coerce("x", original) is original

    coerced = core._MutablePydanticModelList.coerce("x", [Payload(value=1)])
    assert isinstance(coerced, core._MutablePydanticModelList)

    assert core._MutablePydanticModelList.coerce("x", "raw") == "raw"

    changed_calls: list[str] = []

    def record_changed():
        changed_calls.append("changed")

    coerced.changed = record_changed  # type: ignore[assignment]
    coerced.append(Payload(value=2))
    coerced.extend([Payload(value=3)])
    coerced[0] = Payload(value=4)
    coerced.insert(0, Payload(value=5))
    assert len(changed_calls) == 4
    assert [p.value for p in coerced.unwrap()][:2] == [5, 4]


def test_instrument_model_cls_and_attach_parent_fallbacks(monkeypatch):
    core._PARENT_BY_MODEL.clear()

    class Model(BaseModel):
        value: int

    core._instrument_model_cls(Model)
    # second call should early-return without changing behavior
    core._instrument_model_cls(Model)

    payload = Model(value=1)
    parent = object()
    core._attach_parent(payload, parent, "payload")
    payload.value = 2

    failing_map = MagicMock()
    failing_map.get.side_effect = TypeError("no weakref")
    monkeypatch.setattr(core, "_PARENT_BY_MODEL", failing_map)
    object.__setattr__(payload, "_sa_mutable_parent", None)
    payload.value = 3

    class BadWeakRef(BaseModel):
        value: int
        __hash__ = None

    bad_payload = BadWeakRef(value=1)
    core._attach_parent(bad_payload, parent, "payload")


def test_attach_parent_handles_setattr_failure(monkeypatch):
    core._PARENT_BY_MODEL.clear()

    class Model(BaseModel):
        value: int

    payload = Model(value=1)

    def fail_setattr(*_args, **_kwargs):
        raise RuntimeError("nope")

    class FakeObject:
        __setattr__ = staticmethod(fail_setattr)

    monkeypatch.setattr(core, "object", FakeObject, raising=False)
    core._attach_parent(payload, object(), "payload")
    assert "_sa_mutable_parent" not in payload.__dict__