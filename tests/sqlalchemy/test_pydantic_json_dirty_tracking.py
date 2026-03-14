import json

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column

from etl_decorators.sqlalchemy.type_decorators.pydantic_json import (
    PydanticJSON,
    PydanticJSONList,
)


class Payload(BaseModel):
    a: int
    b: str = "x"


def test_pydantic_json_dirty_tracking_scalar_model_field_set_marks_dirty_and_persists():
    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "pyd_json_dirty_scalar"

        id: Mapped[int] = mapped_column(primary_key=True)
        payload: Mapped[Payload] = mapped_column(PydanticJSON(Payload), nullable=True)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model(payload=Payload(a=1))
        session.add(m)
        session.flush()

        session.expire(m)
        assert m.payload.a == 1

        # In-place mutation must dirty the attribute
        m.payload.a = 2
        assert session.is_modified(m)
        session.flush()

        raw_text = session.execute(
            sa.select(sa.cast(Model.__table__.c.payload, sa.Text)).where(Model.id == m.id)
        ).scalar_one()
        assert json.loads(raw_text) == {"a": 2, "b": "x"}


def test_pydantic_json_dirty_tracking_list_item_field_set_marks_dirty_and_persists():
    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "pyd_json_dirty_list"

        id: Mapped[int] = mapped_column(primary_key=True)
        payloads: Mapped[list[Payload]] = mapped_column(
            PydanticJSONList(Payload), nullable=True
        )

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model(payloads=[Payload(a=1), Payload(a=10)])
        session.add(m)
        session.flush()

        session.expire(m)
        assert [p.a for p in m.payloads] == [1, 10]

        m.payloads[0].a = 2
        assert session.is_modified(m)
        session.flush()

        raw_text = session.execute(
            sa.select(sa.cast(Model.__table__.c.payloads, sa.Text)).where(Model.id == m.id)
        ).scalar_one()
        assert json.loads(raw_text)[0]["a"] == 2
