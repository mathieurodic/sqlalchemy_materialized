import json

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class Payload(BaseModel):
    a: int
    b: str


def test_pydantic_json_column_manual_roundtrip_sqlite():
    from etl_decorators.sqlalchemy.type_decorators.pydantic_json import PydanticJSON

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "model_pydantic_manual"

        id: Mapped[int] = mapped_column(primary_key=True)
        payload: Mapped[Payload] = mapped_column(PydanticJSON(Payload), nullable=True)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model(payload=Payload(a=1, b="x"))
        session.add(m)
        session.flush()

        session.expire(m)
        assert isinstance(m.payload, Payload)
        assert m.payload.a == 1
        assert m.payload.b == "x"

        # Check the raw value stored in DB.
        # Under SQLite, `sa.JSON` is stored as TEXT. A CAST AS JSON can yield
        # surprising results (e.g. 0), so we cast to TEXT and parse.
        raw_text = session.execute(
            sa.select(sa.cast(Model.__table__.c.payload, sa.Text)).where(Model.id == m.id)
        ).scalar_one()
        assert isinstance(raw_text, str)
        assert json.loads(raw_text) == {"a": 1, "b": "x"}


def test_make_sa_column_detects_pydantic_return_type_via_materialized_property():
    from etl_decorators.sqlalchemy.type_decorators.pydantic_json import PydanticJSON
    from etl_decorators.sqlalchemy import materialized_property

    class Base(DeclarativeBase):
        pass

    def compute(self) -> Payload:
        return Payload(a=self.base, b="ok")

    class Model(Base):
        __tablename__ = "model_pydantic_auto"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        base: Mapped[int] = mapped_column(sa.Integer)
        payload = materialized_property(compute)

    # The injected column should be our TypeDecorator
    assert isinstance(Model.__table__.c.payload.type, PydanticJSON)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model(base=42)
        session.add(m)
        session.flush()

        # Access => compute + flush + caching
        p = m.payload
        assert isinstance(p, Payload)
        assert p.a == 42
        assert p.b == "ok"

        session.expire(m)
        p2 = m.payload
        assert isinstance(p2, Payload)
        assert p2.a == 42
        assert p2.b == "ok"
