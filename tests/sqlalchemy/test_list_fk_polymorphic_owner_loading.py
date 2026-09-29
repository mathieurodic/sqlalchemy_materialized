import sqlalchemy as sa

from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


def test_list_fk_materialized_property_loads_children_for_polymorphic_owner_type():
    """Regression test.

    When list[MappedClass] materialized_property is stored as an injected
    one-to-many relationship, loading persisted children must work even if the
    owner instance is a polymorphic subclass.

    Prior bug: loader derived FK name from `type(owner).__name__`, resulting in
    looking for e.g. `child.subclass_id` instead of the injected `child.base_id`.
    """

    from etl_decorators.sqlalchemy import materialized_property

    class Base(DeclarativeBase):
        pass

    class Child(Base):
        __tablename__ = "poly_owner_child"
        id: Mapped[int] = mapped_column(primary_key=True)

    class Owner(Base):
        __tablename__ = "poly_owner"
        id: Mapped[int] = mapped_column(primary_key=True)
        type: Mapped[str] = mapped_column(sa.String)

        __mapper_args__ = {
            "polymorphic_on": "type",
            "polymorphic_identity": "owner",
        }

        def compute_children(self) -> list[Child]:
            # Computation returns existing mapped instances (already flushed).
            # This is enough to persist the relationship + set computed_at.
            session = sa.orm.object_session(self)
            assert session is not None
            return session.query(Child).order_by(Child.id).all()

        children = materialized_property(compute_children)

    class OwnerSub(Owner):
        __mapper_args__ = {"polymorphic_identity": "sub"}

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    # First session: compute and persist.
    with Session(engine) as session:
        c1, c2 = Child(), Child()
        session.add_all([c1, c2])
        session.flush()

        o = OwnerSub(type="sub")
        session.add(o)
        session.flush()

        assert [c.id for c in list(o.children)] == [c1.id, c2.id]
        assert getattr(o, "_compute_children__computed_at") is not None
        session.commit()

    # Second session: should NOT recompute, should load children from DB.
    with Session(engine) as session:
        o2 = session.query(Owner).one()
        # Ensure we still have a polymorphic subclass instance.
        assert isinstance(o2, OwnerSub)
        assert getattr(o2, "_compute_children__computed_at") is not None
        assert [c.id for c in list(o2.children)] == [1, 2]
