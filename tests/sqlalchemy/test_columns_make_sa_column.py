import enum

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def test_make_sa_column_str_enum_produces_sa_enum_column():
    from etl_decorators.sqlalchemy.orm.columns import make_sa_column

    class Color(str, enum.Enum):
        red = "red"
        green = "green"
        blue = "blue"

    col = make_sa_column("color", Color, nullable=False)
    assert isinstance(col.column.type, sa.Enum)
    assert col.column.type.native_enum is False
    assert col.column.nullable is False


def test_make_sa_column_optional_enum_produces_nullable_column():
    from etl_decorators.sqlalchemy.orm.columns import make_sa_column

    class Status(str, enum.Enum):
        active = "active"
        inactive = "inactive"

    col = make_sa_column("status", Status | None, nullable=None)
    assert isinstance(col.column.type, sa.Enum)
    assert col.column.nullable is True


def test_make_sa_column_float_enum_produces_sa_enum_column():
    from etl_decorators.sqlalchemy.orm.columns import make_sa_column

    class Score(float, enum.Enum):
        low = 1.0
        high = 5.0

    col = make_sa_column("score", Score, nullable=False)
    assert isinstance(col.column.type, sa.Enum)
    assert col.column.type.native_enum is False


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
