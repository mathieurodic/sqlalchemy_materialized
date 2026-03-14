from __future__ import annotations

import sqlalchemy as sa
from sqlalchemy.orm import DeclarativeBase, relationship

from etl_decorators.sqlalchemy import as_model, field
from etl_decorators.sqlalchemy.orm.as_model import _resolve_hints


def test_as_model_hints_reverse_attr_and_factory_branches():
    mapper_registry = sa.orm.registry()

    class Base(DeclarativeBase):
        registry = mapper_registry

    class Example:
        value: int

    assert _resolve_hints(Example)["value"] is int

    @as_model(Base)
    class User:
        name: str

    User.applications = relationship("Application", back_populates="user")

    def bad_factory(self, other=None):
        return "ok"

    class BrokenCallable:
        def __call__(self):
            return "broken"

        @property
        def __signature__(self):
            raise ValueError("boom")

    @as_model(Base)
    class Application:
        user: User
        age: int
        zero_arg: str = field(default_factory=lambda: "zero")
        bad: str = field(default_factory=bad_factory)
        broken: str = field(default_factory=BrokenCallable())

    assert isinstance(Application.__table__.c.age.type, sa.Integer)
    assert hasattr(User, "applications")

    app = Application(user=User(name="x"))
    assert app.zero_arg == "zero"
    assert app.bad == "ok"
    assert app.broken == "broken"

    mapper_registry.configure()
    mapper_registry.dispose()