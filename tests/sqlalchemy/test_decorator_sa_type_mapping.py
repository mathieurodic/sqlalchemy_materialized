import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


def test_sa_type_mapping_from_return_annotation_int():
    from etl_decorators.sqlalchemy import materialized_property

    def compute(self) -> int:
        return 123

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "model_int"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        value = materialized_property(compute)

    # __set_name__ should have run at class creation time
    assert isinstance(Model.__dict__["value"], sa.ext.hybrid.hybrid_property)
    # backing attribute injected (the Column is stored on the class)
    assert hasattr(Model, "_compute")
    assert hasattr(Model, "_compute__computed_at")

    # ensure SQLA column type matches return annotation
    assert isinstance(Model.__table__.c.value.type, sa.Integer)
    assert isinstance(Model.__table__.c.value__computed_at.type, sa.DateTime)


def test_sa_type_mapping_fallback_to_string_for_unknown_return_type():
    import pytest

    from etl_decorators.sqlalchemy import materialized_property

    def compute(self) -> dict:
        return {"a": 1}

    class Base(DeclarativeBase):
        pass

    with pytest.raises(TypeError, match="unsupported"):
        class Model(Base):
            __tablename__ = "model_fallback"
            __allow_unmapped__ = True

            id: Mapped[int] = mapped_column(primary_key=True)
            value = materialized_property(compute)
