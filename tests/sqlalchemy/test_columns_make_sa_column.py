from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def test_make_sa_column_raises_for_composite_pk_model():
    from etl_decorators.sqlalchemy.orm.columns import make_sa_column

    class Base(DeclarativeBase):
        pass

    class Composite(Base):
        __tablename__ = "composite_pk"

        id1: Mapped[int] = mapped_column(primary_key=True)
        id2: Mapped[int] = mapped_column(primary_key=True)

    try:
        make_sa_column("x", Composite)
        raise AssertionError("Expected ValueError")
    except ValueError as e:
        assert "composite PK" in str(e)
