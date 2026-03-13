import sqlalchemy as sa
import pytest
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class TransientError(RuntimeError):
    """Used for retry tests."""


def test_retry_on_tuple_retries_then_succeeds(monkeypatch):
    from etl_decorators.sqlalchemy import materialized_property

    sleeps: list[float] = []
    monkeypatch.setattr(
        "etl_decorators.sqlalchemy.materialized.descriptor.time.sleep",
        lambda s: sleeps.append(s),
    )

    calls = {"compute": 0}

    def compute(self) -> int:
        calls["compute"] += 1
        if calls["compute"] < 3:
            raise TransientError("try again")
        return 42

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "retry_tuple_success"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        value = materialized_property(
            compute,
            retry_on=(TransientError,),
            retry_max=5,
            retry_interval=1.0,
            retry_factor=2.0,
        )

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model()
        session.add(m)
        session.flush()

        assert m.value == 42
        assert calls["compute"] == 3

    # exponential backoff: 1.0, 2.0 between attempt 1->2 and 2->3
    assert sleeps == [1.0, 2.0]


def test_retry_on_single_exception_class_form(monkeypatch):
    """`retry_on` also accepts a single exception class (not only a tuple)."""

    from etl_decorators.sqlalchemy import materialized_property

    sleeps: list[float] = []
    monkeypatch.setattr(
        "etl_decorators.sqlalchemy.materialized.descriptor.time.sleep",
        lambda s: sleeps.append(s),
    )

    calls = {"compute": 0}

    def compute(self) -> int:
        calls["compute"] += 1
        if calls["compute"] == 1:
            raise TransientError("transient")
        return 1

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "retry_single_class"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        value = materialized_property(
            compute,
            retry_on=TransientError,
            retry_max=3,
            retry_interval=0.25,
            retry_factor=2.0,
        )

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model()
        session.add(m)
        session.flush()
        assert m.value == 1

    assert calls["compute"] == 2
    assert sleeps == [0.25]


def test_retry_on_callable_predicate(monkeypatch):
    from etl_decorators.sqlalchemy import materialized_property

    sleeps: list[float] = []
    monkeypatch.setattr(
        "etl_decorators.sqlalchemy.materialized.descriptor.time.sleep",
        lambda s: sleeps.append(s),
    )

    calls = {"compute": 0}

    def compute(self) -> int:
        calls["compute"] += 1
        if calls["compute"] == 1:
            raise ValueError("transient")
        return 7

    def predicate(exc: Exception) -> bool:
        return isinstance(exc, ValueError) and str(exc) == "transient"

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "retry_predicate"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        value = materialized_property(
            compute,
            retry_on=predicate,
            retry_max=3,
            retry_interval=0.5,
            retry_factor=3.0,
        )

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model()
        session.add(m)
        session.flush()

        assert m.value == 7
        assert calls["compute"] == 2

    assert sleeps == [0.5]


def test_retry_logs_error_and_debug_traceback(caplog):
    """Exceptions should be logged at error (summary) and debug (traceback)."""

    from etl_decorators.sqlalchemy import materialized_property

    caplog.set_level("DEBUG")

    def compute(self) -> int:
        raise TransientError("boom")

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "retry_logs"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        value = materialized_property(
            compute,
            retry_on=TransientError,
            retry_max=1,  # fail fast
        )

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model()
        session.add(m)
        session.flush()

        with pytest.raises(TransientError, match="boom"):
            _ = m.value

    # error summary
    assert any(
        r.levelname == "ERROR"
        and "compute failed" in r.getMessage()
        and "TransientError" in r.getMessage()
        and "boom" in r.getMessage()
        for r in caplog.records
    )

    # debug traceback (exc_info set)
    assert any(
        r.levelname == "DEBUG" and r.exc_info is not None for r in caplog.records
    )


def test_retry_does_not_retry_on_unmatched_exception(monkeypatch):
    from etl_decorators.sqlalchemy import materialized_property

    monkeypatch.setattr(
        "etl_decorators.sqlalchemy.materialized.descriptor.time.sleep",
        lambda s: (_ for _ in ()).throw(AssertionError("should not sleep")),
    )

    calls = {"compute": 0}

    def compute(self) -> int:
        calls["compute"] += 1
        raise KeyError("no retry")

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "retry_unmatched"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        value = materialized_property(
            compute,
            retry_on=(TransientError,),
            retry_max=5,
        )

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model()
        session.add(m)
        session.flush()

        with pytest.raises(KeyError, match="no retry"):
            _ = m.value

    assert calls["compute"] == 1


def test_retry_max_limits_attempts(monkeypatch):
    from etl_decorators.sqlalchemy import materialized_property

    sleeps: list[float] = []
    monkeypatch.setattr(
        "etl_decorators.sqlalchemy.materialized.descriptor.time.sleep",
        lambda s: sleeps.append(s),
    )

    calls = {"compute": 0}

    def compute(self) -> int:
        calls["compute"] += 1
        raise TransientError("always")

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "retry_max"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        value = materialized_property(
            compute,
            retry_on=(TransientError,),
            retry_max=3,
            retry_interval=1.0,
            retry_factor=2.0,
        )

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model()
        session.add(m)
        session.flush()

        with pytest.raises(TransientError, match="always"):
            _ = m.value

    assert calls["compute"] == 3
    # sleep only between attempts 1->2 and 2->3
    assert sleeps == [1.0, 2.0]


def test_retry_interval_0_does_not_sleep(monkeypatch):
    from etl_decorators.sqlalchemy import materialized_property

    monkeypatch.setattr(
        "etl_decorators.sqlalchemy.materialized.descriptor.time.sleep",
        lambda s: (_ for _ in ()).throw(AssertionError("should not sleep")),
    )

    calls = {"compute": 0}

    def compute(self) -> int:
        calls["compute"] += 1
        if calls["compute"] == 1:
            raise TransientError("transient")
        return 9

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "retry_interval_0"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        value = materialized_property(
            compute,
            retry_on=(TransientError,),
            retry_max=2,
            retry_interval=0.0,
        )

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model()
        session.add(m)
        session.flush()

        assert m.value == 9
        assert calls["compute"] == 2


def test_retry_arg_validation():
    from etl_decorators.sqlalchemy import materialized_property

    def compute(self) -> int:
        return 1

    with pytest.raises(ValueError, match="retry_max must be >= 1"):
        materialized_property(compute, retry_max=0)

    with pytest.raises(ValueError, match="retry_interval must be >= 0"):
        materialized_property(compute, retry_interval=-1)

    with pytest.raises(TypeError, match="retry_on tuple items"):
        materialized_property(compute, retry_on=(123,))  # type: ignore[arg-type]

    with pytest.raises(TypeError, match="retry_on must be"):
        materialized_property(compute, retry_on=123)  # type: ignore[arg-type]
