from __future__ import annotations

import pytest


def test_generated_model_supports_nullable_and_arrays_and_nested_objects():
    pytest.importorskip("pydantic")

    from etl_decorators.redis import serde

    schema = {
        "title": "M",
        "type": "object",
        "properties": {
            "x": {"type": "integer"},
            "z": {"type": ["null", "number"]},
            "arr": {"type": "array", "items": {"type": "boolean"}},
            "obj": {
                "type": "object",
                "properties": {"a": {"type": "string"}},
                "required": ["a"],
            },
        },
        "required": ["x"],
    }

    Model = serde._best_effort_model_from_schema("M", schema)
    assert Model is not None

    m = Model.model_validate({"x": 1, "z": None, "arr": [True, False], "obj": {"a": "ok"}})
    assert m.model_dump()["x"] == 1
    assert m.model_dump()["z"] is None
