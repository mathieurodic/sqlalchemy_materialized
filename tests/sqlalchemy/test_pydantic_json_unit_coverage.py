import json

import sqlalchemy as sa
from pydantic import BaseModel


class Payload(BaseModel):
    a: int
    b: str


def test_pydantic_json_unit_optional_normalization():
    """Covers Optional[T] normalization (Union[T, None])."""
    from etl_decorators.sqlalchemy.orm.columns import make_sa_column

    col = make_sa_column("x", int | None)
    assert isinstance(col.column.type, sa.Integer)


def test_make_sa_column_normalizes_optional_pydantic_model():
    from etl_decorators.sqlalchemy.orm.columns import make_sa_column
    from etl_decorators.sqlalchemy.type_decorators.pydantic_json import PydanticJSON

    col = make_sa_column("payload", Payload | None)
    assert isinstance(col.column.type, PydanticJSON)
    assert col.column.type.model_cls is Payload


def test_pydantic_json_init_raises_for_non_basemodel():
    from etl_decorators.sqlalchemy.type_decorators.pydantic_json import PydanticJSON

    try:
        PydanticJSON(int)  # type: ignore[arg-type]
        raise AssertionError("Expected TypeError")
    except TypeError as e:
        assert "BaseModel" in str(e)


def test_pydantic_json_process_bind_param_accepts_dict_and_str_and_rejects_other():
    from etl_decorators.sqlalchemy.type_decorators.pydantic_json import PydanticJSON

    typ = PydanticJSON(Payload)

    out_dict = typ.process_bind_param({"a": 1, "b": "x"}, dialect=None)
    assert out_dict == {"a": 1, "b": "x"}

    out_str = typ.process_bind_param('{"a": 2, "b": "y"}', dialect=None)
    assert out_str == {"a": 2, "b": "y"}

    try:
        typ.process_bind_param(object(), dialect=None)
        raise AssertionError("Expected TypeError")
    except TypeError:
        pass


def test_pydantic_json_process_result_value_none_str_and_fallback():
    from etl_decorators.sqlalchemy.type_decorators.pydantic_json import PydanticJSON

    typ = PydanticJSON(Payload)

    assert typ.process_result_value(None, dialect=None) is None

    # `str` branch => model_validate_json
    m = typ.process_result_value('{"a": 3, "b": "z"}', dialect=None)
    assert isinstance(m, Payload)
    assert m.a == 3
    assert m.b == "z"

    # fallback: provide a non-dict/list/str object that is still acceptable to model_validate
    m2 = typ.process_result_value(Payload(a=4, b="w"), dialect=None)
    assert isinstance(m2, Payload)
    assert m2.a == 4
    assert m2.b == "w"


def test_pydantic_json_bind_param_string_must_be_valid_json():
    """Covers the ValueError raised on invalid JSON input."""
    from etl_decorators.sqlalchemy.type_decorators.pydantic_json import PydanticJSON

    typ = PydanticJSON(Payload)

    try:
        typ.process_bind_param("{not json}", dialect=None)
        raise AssertionError("Expected ValueError")
    except ValueError:
        pass


def test_pydantic_json_bind_and_manual_json_dump_is_stable():
    """Robustness test: ensure the output is JSON-serializable."""
    from etl_decorators.sqlalchemy.type_decorators.pydantic_json import PydanticJSON

    typ = PydanticJSON(Payload)
    out = typ.process_bind_param(Payload(a=10, b="ok"), dialect=None)
    assert json.dumps(out) == '{"a": 10, "b": "ok"}'


def test_pydantic_json_list_init_raises_for_non_basemodel():
    from etl_decorators.sqlalchemy.type_decorators.pydantic_json import PydanticJSONList

    try:
        PydanticJSONList(int)  # type: ignore[arg-type]
        raise AssertionError("Expected TypeError")
    except TypeError as e:
        assert "BaseModel" in str(e)


def test_pydantic_json_list_process_bind_param_happy_paths_and_errors():
    from etl_decorators.sqlalchemy.type_decorators.pydantic_json import PydanticJSONList

    typ = PydanticJSONList(Payload)

    # non-list => TypeError
    try:
        typ.process_bind_param({"a": 1, "b": "x"}, dialect=None)  # type: ignore[arg-type]
        raise AssertionError("Expected TypeError")
    except TypeError:
        pass

    # list with None item => TypeError
    try:
        typ.process_bind_param([None], dialect=None)
        raise AssertionError("Expected TypeError")
    except TypeError:
        pass

    # invalid item type => TypeError
    try:
        typ.process_bind_param([object()], dialect=None)
        raise AssertionError("Expected TypeError")
    except TypeError:
        pass

    out_models = typ.process_bind_param([Payload(a=1, b="x")], dialect=None)
    assert out_models == [{"a": 1, "b": "x"}]

    out_dicts = typ.process_bind_param([{"a": 2, "b": "y"}], dialect=None)
    assert out_dicts == [{"a": 2, "b": "y"}]

    out_strs = typ.process_bind_param(['{"a": 3, "b": "z"}'], dialect=None)
    assert out_strs == [{"a": 3, "b": "z"}]

    # invalid json string => ValueError
    try:
        typ.process_bind_param(["{not json}"], dialect=None)
        raise AssertionError("Expected ValueError")
    except ValueError:
        pass


def test_pydantic_json_list_process_result_value_branches():
    from etl_decorators.sqlalchemy.type_decorators.pydantic_json import PydanticJSONList

    typ = PydanticJSONList(Payload)

    assert typ.process_result_value(None, dialect=None) is None

    out_list = typ.process_result_value([{"a": 1, "b": "x"}], dialect=None)
    assert isinstance(out_list, list)
    assert isinstance(out_list[0], Payload)
    assert out_list[0].a == 1
    assert out_list[0].b == "x"

    out_str = typ.process_result_value('[{"a": 2, "b": "y"}]', dialect=None)
    assert [x.a for x in out_str] == [2]

    # json string not an array => ValueError
    try:
        typ.process_result_value('{"a": 3, "b": "z"}', dialect=None)
        raise AssertionError("Expected ValueError")
    except ValueError:
        pass

    # fallback => list with 1 element
    out_fallback = typ.process_result_value({"a": 4, "b": "w"}, dialect=None)
    assert isinstance(out_fallback, list)
    assert out_fallback[0].a == 4
