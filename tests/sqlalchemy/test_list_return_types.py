import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


def test_materialized_property_list_int_maps_to_json_column():
    from etl_decorators.sqlalchemy import materialized_property

    def compute(self) -> list[int]:
        return [1, 2, self.base]

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "model_list_int"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        base: Mapped[int] = mapped_column(sa.Integer)
        value = materialized_property(compute)

    assert isinstance(Model.__table__.c.value.type, sa.JSON)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        m = Model(base=3)
        session.add(m)
        session.flush()
        assert m.value == [1, 2, 3]


class Payload(BaseModel):
    a: int


def test_materialized_property_list_pydantic_roundtrip_sqlite():
    from etl_decorators.sqlalchemy.type_decorators.pydantic_json import PydanticJSONList
    from etl_decorators.sqlalchemy import materialized_property

    def compute(self) -> list[Payload]:
        return [Payload(a=self.base), Payload(a=self.base + 1)]

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "model_list_pydantic"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        base: Mapped[int] = mapped_column(sa.Integer)
        payloads = materialized_property(compute)

    assert isinstance(Model.__table__.c.payloads.type, PydanticJSONList)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        m = Model(base=10)
        session.add(m)
        session.flush()

        p = m.payloads
        assert [x.a for x in p] == [10, 11]

        session.expire(m)
        p2 = m.payloads
        assert [x.a for x in p2] == [10, 11]


def test_materialized_property_list_mappedclass_resolves_in_session_but_raises_detached():
    from etl_decorators.sqlalchemy import materialized_property

    class Base(DeclarativeBase):
        pass

    class Author(Base):
        __tablename__ = "author_list"

        id: Mapped[int] = mapped_column(primary_key=True)

    def compute_authors(self) -> list[Author]:
        # returning ids is allowed
        return self.author_ids  # type: ignore[return-value]

    class Post(Base):
        __tablename__ = "post_list"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        # JSON column holding the ids used by compute
        author_ids: Mapped[list[int]] = mapped_column(sa.JSON)
        authors = materialized_property(compute_authors)

    # backing storage is an association table (no authors column on post_list)
    assert "authors" not in Post.__table__.c

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        a1 = Author()
        a2 = Author()
        session.add_all([a1, a2])
        session.flush()

        p = Post(author_ids=[a1.id, a2.id])
        session.add(p)
        session.flush()

        resolved = p.authors
        assert [a.id for a in resolved] == [a1.id, a2.id]

        session.expunge(p)
        import pytest

        with pytest.raises(RuntimeError, match="detached"):
            _ = p.authors
