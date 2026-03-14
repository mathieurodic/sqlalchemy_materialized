import sys
import types

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy.orm import DeclarativeBase, Mapped, Session, mapped_column


class Summary(BaseModel):
    summary: str


def _install_fake_litellm(monkeypatch, *, parsed: dict):
    """Install a fake `litellm` module returning a structured response."""

    mod = types.SimpleNamespace()
    calls = {"completion": []}

    def completion(**kwargs):
        calls["completion"].append(kwargs)
        return {"choices": [{"message": {"parsed": parsed}}]}

    mod.completion = completion
    monkeypatch.setitem(sys.modules, "litellm", mod)
    return calls


def test_materialized_property_on_top_of_llm_structured_uses_pydantic_json_storage(
    monkeypatch,
):
    """LLM + materialized_property should be stackable.

    We expect:
    - `@llm` infers `return_type` from the annotated return type (Summary)
    - the decorated callable advertises `-> Summary` at runtime so
      materialized_property stores it in a JSON column via PydanticJSON
    - first access triggers an LLM call + flush
    - second access reads materialized value without calling the LLM again
    """

    calls = _install_fake_litellm(monkeypatch, parsed={"summary": "hi"})

    from etl_decorators.llms import LLM
    from etl_decorators.sqlalchemy import materialized_property

    llm = LLM(model="fake")

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "llm_mat_prop"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)

        @materialized_property
        @llm
        def summary(self) -> Summary:  # type: ignore[return-value]
            return "Return a summary"  # prompt

    # Ensure mapper is configured so we can inspect columns.
    sa.orm.configure_mappers()

    # Column should exist and be backed by the PydanticJSON type decorator.
    # (We check by name to avoid importing SQLAlchemy internals.)
    col = Model.__table__.c.summary
    assert col.type.__class__.__name__ == "PydanticJSON"

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model()
        session.add(m)
        session.flush()

        out1 = m.summary
        assert isinstance(out1, Summary)
        assert out1.summary == "hi"
        assert len(calls["completion"]) == 1

        # Second access should NOT call the LLM again.
        out2 = m.summary
        assert isinstance(out2, Summary)
        assert out2.summary == "hi"
        assert len(calls["completion"]) == 1


def test_template_llm_materialized_property_pipeline(monkeypatch):
    """Optional composition: template -> llm -> materialized_property."""

    calls = _install_fake_litellm(monkeypatch, parsed={"summary": "templated"})

    from etl_decorators.llms import LLM
    from etl_decorators.sqlalchemy import materialized_property
    from etl_decorators.templating import template

    llm = LLM(model="fake")

    class Base(DeclarativeBase):
        pass

    class Model(Base):
        __tablename__ = "tpl_llm_mat_prop"
        __allow_unmapped__ = True

        id: Mapped[int] = mapped_column(primary_key=True)
        name: Mapped[str] = mapped_column(sa.String)

        @materialized_property
        @llm
        @template
        def summary(self) -> Summary:  # type: ignore[return-value]
            return "Return a summary for {{ self.name }}"

    engine = sa.create_engine("sqlite+pysqlite:///:memory:")
    Base.metadata.create_all(engine)

    with Session(engine) as session:
        m = Model(name="Ada")
        session.add(m)
        session.flush()

        out = m.summary
        assert isinstance(out, Summary)
        assert out.summary == "templated"
        assert calls["completion"][0]["messages"][0]["content"] == (
            "Return a summary for Ada"
        )
