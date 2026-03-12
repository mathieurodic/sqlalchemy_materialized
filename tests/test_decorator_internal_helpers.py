import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


def test_pk_id_from_instance_raises_when_instance_has_no_identity():
    from sqlalchemy_materialized.decorator import _pk_id_from_instance

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "pk_helper_not_flushed"

        id: Mapped[int] = mapped_column(primary_key=True)

    # No session/flush => no identity
    m = Model()
    try:
        _pk_id_from_instance(m)
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "Unable to extract identity" in str(e)


def test_pk_id_from_instance_raises_when_composite_pk():
    from sqlalchemy_materialized.decorator import _pk_id_from_instance

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "pk_helper_composite"

        id1: Mapped[int] = mapped_column(primary_key=True)
        id2: Mapped[int] = mapped_column(primary_key=True)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model(id1=1, id2=2)
        session.add(m)
        session.flush()

        try:
            _pk_id_from_instance(m)
            raise AssertionError("Expected ValueError")
        except ValueError as e:
            assert "composite PK" in str(e)
