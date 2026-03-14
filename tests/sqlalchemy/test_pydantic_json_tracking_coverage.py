from __future__ import annotations

from unittest.mock import MagicMock

import sqlalchemy as sa
from pydantic import BaseModel

from etl_decorators.sqlalchemy.type_decorators import _pydantic_json_tracking as tracking
from etl_decorators.sqlalchemy.type_decorators.pydantic_json_types import (
    PydanticJSON,
    PydanticJSONList,
)


class Payload(BaseModel):
    value: int


def test_attach_parents_for_instance_handles_bad_column(monkeypatch):
    class BadProp:
        key = "payload"

        @property
        def columns(self):
            raise RuntimeError("boom")

    class DummyMapper:
        column_attrs = [BadProp()]

    monkeypatch.setattr(sa, "inspect", lambda _cls: DummyMapper())
    tracking._attach_parents_for_instance(object())


def test_setup_attribute_set_listeners_and_set_callbacks(monkeypatch):
    listeners: list[tuple[object, str, object]] = []

    def capture_listen(target, event, fn, **_kwargs):
        listeners.append((target, event, fn))

    monkeypatch.setattr(sa.event, "listen", capture_listen)

    tracking._setup_attribute_set_listeners()
    on_mapper = listeners[-1][2]

    mapper = MagicMock(non_primary=True)
    on_mapper(mapper, object())

    attr = object()
    prop = MagicMock()
    prop.key = "payload"
    prop.columns = [MagicMock(type=PydanticJSON(Payload))]

    mapper = MagicMock(non_primary=False, column_attrs=[prop])
    on_mapper(mapper, MagicMock(payload=attr))

    set_listener = listeners[-1][2]
    parent_obj = object()
    target = MagicMock(obj=lambda: parent_obj)
    value = Payload(value=1)

    called: dict[str, int] = {"parent": 0, "list": 0}

    def record_parent(*_args, **_kwargs):
        called["parent"] += 1

    def record_parent_list(*_args, **_kwargs):
        called["list"] += 1

    monkeypatch.setattr(tracking, "_attach_parent", record_parent)
    monkeypatch.setattr(tracking, "_attach_parent_list", record_parent_list)
    set_listener(target, value, None, None)

    prop_list = MagicMock()
    prop_list.key = "payloads"
    prop_list.columns = [MagicMock(type=PydanticJSONList(Payload))]
    mapper = MagicMock(non_primary=False, column_attrs=[prop_list])
    on_mapper(mapper, MagicMock(payloads=attr))

    list_listener = listeners[-1][2]
    list_listener(target, [Payload(value=2)], None, None)

    list_listener(target, "not-a-list", None, None)

    assert called == {"parent": 1, "list": 1}


def test_setup_pydantic_json_tracking_idempotent(monkeypatch):
    tracking._TRACKING_SETUP_DONE = False
    setup_calls: list[str] = []

    def record_setup():
        setup_calls.append("setup")

    monkeypatch.setattr(tracking, "_setup_attribute_set_listeners", record_setup)
    monkeypatch.setattr(tracking._MutablePydanticModelList, "associate_with", lambda *_args: None)

    listeners: list[tuple[object, str, object]] = []

    def capture_listens_for(target, event):
        def decorator(fn):
            listeners.append((target, event, fn))
            return fn

        return decorator

    monkeypatch.setattr(sa.event, "listens_for", capture_listens_for)

    tracking.setup_pydantic_json_tracking()
    tracking.setup_pydantic_json_tracking()

    assert setup_calls == ["setup"]

    load_listener = next(fn for _t, event, fn in listeners if event == "load")
    attach_calls: list[str] = []

    def record_attach(target):
        attach_calls.append(str(target))

    monkeypatch.setattr(tracking, "_attach_parents_for_instance", record_attach)
    load_listener("target", None)
    assert attach_calls == ["target"]