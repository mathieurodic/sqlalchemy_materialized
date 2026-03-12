import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


def test_integration_sqlite_flush_persists_computed_value_and_delete_sets_null():
    """Integration test with a real Session/flush to ensure the value is stored in DB."""
    from sqlalchemy_materialized.decorator import materialized_property

    calls = {"compute": 0}

    def compute(self) -> int:
        calls["compute"] += 1
        return self.base * 2

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "model_integration"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        base: Mapped[int] = mapped_column(sa.Integer)
        value = materialized_property(compute)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model(base=10)
        session.add(m)
        session.flush()

        # First access computes and flushes; value should be persisted in DB.
        assert m.value == 20
        assert calls["compute"] == 1

        # Expire local state & ensure DB has the cached value
        session.expire(m)
        row_value = session.execute(sa.select(Model.__table__.c.value).where(Model.id == m.id)).scalar_one()
        assert row_value == 20

        row_computed_at = session.execute(
            sa.select(Model.__table__.c.value__computed_at).where(Model.id == m.id)
        ).scalar_one()
        assert row_computed_at is not None

        # Delete should set column to NULL, reset computed_at to NULL, and flush
        del m.value
        session.expire(m)
        row_value2 = session.execute(sa.select(Model.__table__.c.value).where(Model.id == m.id)).scalar_one()
        assert row_value2 is None

        row_computed_at2 = session.execute(
            sa.select(Model.__table__.c.value__computed_at).where(Model.id == m.id)
        ).scalar_one()
        assert row_computed_at2 is None

        # Access after delete recomputes
        assert m.value == 20
        assert calls["compute"] == 2


def test_integration_sqlite_materialized_value_can_be_used_in_filter_and_order_by():
    """`Model.value` should be usable as a SQL expression.

    The SQL expression uses the stored/materialized value (backing column) and
    does not trigger computation.
    """

    from sqlalchemy_materialized.decorator import materialized_property

    calls = {"compute": 0}

    def compute(self) -> int:
        calls["compute"] += 1
        return self.base * 2

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "model_integration_querying"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        base: Mapped[int] = mapped_column(sa.Integer)
        value = materialized_property(compute)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m1 = Model(base=5)
        m2 = Model(base=10)
        session.add_all([m1, m2])
        session.flush()

        # Materialize only m2
        assert m2.value == 20
        assert calls["compute"] == 1

        # Filtering via SQL should work and should not compute m1
        rows = session.execute(sa.select(Model.id).where(Model.value == 20)).scalars().all()
        assert rows == [m2.id]
        assert calls["compute"] == 1

        # Ordering works (NULLs first in SQLite)
        ids = session.execute(sa.select(Model.id).order_by(Model.value.asc(), Model.id.asc())).scalars().all()
        assert ids == [m1.id, m2.id]
        assert calls["compute"] == 1


def test_integration_sqlite_compute_runs_in_savepoint_and_rolls_back_side_effects_on_error():
    """If compute_fn creates DB side effects and then errors, everything should rollback."""
    from sqlalchemy_materialized.decorator import materialized_property

    class Base(DeclarativeBase):
        pass

    class SideEffect(Base):
        __tablename__ = "side_effect"
        id: Mapped[int] = mapped_column(primary_key=True)
        note: Mapped[str] = mapped_column(sa.String)

    class Model(Base):
        __tablename__ = "model_integration_savepoint"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        base: Mapped[int] = mapped_column(sa.Integer)

        def compute(self) -> int:
            session = sa.orm.object_session(self)
            assert session is not None
            session.add(SideEffect(note="created in compute"))
            session.flush()
            raise RuntimeError("boom")

        value = materialized_property(compute)

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    import pytest

    with Session(engine) as session:
        m = Model(base=10)
        session.add(m)
        session.flush()

        with pytest.raises(RuntimeError, match="boom"):
            _ = m.value

        # Side effects rolled back
        assert session.execute(sa.select(sa.func.count(SideEffect.id))).scalar_one() == 0

        # Cache value and computed_at not persisted
        row_value = session.execute(sa.select(Model.__table__.c.value).where(Model.id == m.id)).scalar_one()
        assert row_value is None

        row_computed_at = session.execute(
            sa.select(Model.__table__.c.value__computed_at).where(Model.id == m.id)
        ).scalar_one()
        assert row_computed_at is None
