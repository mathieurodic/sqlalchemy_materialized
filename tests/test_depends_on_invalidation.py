import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column, relationship


def test_depends_on_column_change_invalidates_and_recomputes_on_next_access():
    from sqlalchemy_materialized.decorator import materialized_property

    calls = {"compute": 0}

    def compute(self) -> int:
        calls["compute"] += 1
        return self.base * 2

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "model_depends_on_column"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        base: Mapped[int] = mapped_column(sa.Integer)
        value = materialized_property(compute, depends_on=("base",))

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model(base=10)
        session.add(m)
        session.flush()

        assert m.value == 20
        assert calls["compute"] == 1
        assert getattr(m, "_compute__computed_at") is not None

        # Change dependency => should invalidate (no recompute yet)
        m.base = 11
        assert getattr(m, "_compute__computed_at") is None

        # Next access recomputes
        assert m.value == 22
        assert calls["compute"] == 2


def test_depends_on_relationship_collection_change_invalidates_and_recomputes():
    from sqlalchemy_materialized.decorator import materialized_property

    calls = {"compute": 0}

    class Base(DeclarativeBase):
        pass

    class Child(Base):
        __tablename__ = "child_depends_on"

        id: Mapped[int] = mapped_column(primary_key=True)
        parent_id: Mapped[int] = mapped_column(sa.ForeignKey("parent_depends_on.id"))

    class Parent(Base):
        __tablename__ = "parent_depends_on"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        children: Mapped[list[Child]] = relationship(Child, cascade="all, delete-orphan")

        def compute_children_count(self) -> int:
            calls["compute"] += 1
            return len(self.children)

        children_count = materialized_property(
            compute_children_count,
            depends_on=("children",),
        )

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        p = Parent()
        session.add(p)
        session.flush()

        assert p.children_count == 0
        assert calls["compute"] == 1

        # Append to collection => invalidate only
        p.children.append(Child())
        assert getattr(p, "_compute_children_count__computed_at") is None

        # Next access recomputes
        assert p.children_count == 1
        assert calls["compute"] == 2


def test_depends_on_invalidates_without_flush_side_effects():
    """Changing a dependency should not implicitly flush.

    Invalidation is in-memory only. This test ensures DB still contains the
    previous persisted value until an explicit flush.
    """

    from sqlalchemy_materialized.decorator import materialized_property

    def compute(self) -> int:
        return self.base * 2

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "model_depends_on_no_flush"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        base: Mapped[int] = mapped_column(sa.Integer)
        value = materialized_property(compute, depends_on=("base",))

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model(base=10)
        session.add(m)
        session.flush()

        assert m.value == 20
        session.expire(m)
        assert (
            session.execute(
                sa.select(Model.__table__.c.value).where(Model.id == m.id)
            ).scalar_one()
            == 20
        )

        # Change dependency => invalidation clears in-memory cache, but should not flush.
        m.base = 11

        # DB should still contain old value
        session.expire(m)
        assert (
            session.execute(
                sa.select(Model.__table__.c.value).where(Model.id == m.id)
            ).scalar_one()
            == 20
        )
