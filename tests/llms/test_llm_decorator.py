import asyncio
import sys
import types

import pytest
from pydantic import BaseModel

from etl_decorators.llms import LLM
from etl_decorators.llms.llm import _require_type_annotation
from etl_decorators.llms.validators import require_pydantic_model


class Summary(BaseModel):
    summary: str


def _install_fake_litellm(monkeypatch, *, completion_resp, acompletion_resp=None):
    mod = types.SimpleNamespace()
    calls = {"completion": [], "acompletion": []}

    def completion(**kwargs):
        calls["completion"].append(kwargs)
        return completion_resp

    async def acompletion(**kwargs):
        calls["acompletion"].append(kwargs)
        return acompletion_resp if acompletion_resp is not None else completion_resp

    mod.completion = completion
    mod.acompletion = acompletion
    monkeypatch.setitem(sys.modules, "litellm", mod)
    return calls


def test_llm_decorator_sync_text(monkeypatch):
    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"content": "ok"}}]},
    )

    llm = LLM(model="fake")

    @llm
    def prompt(name: str) -> str:
        return f"Hello {name}"

    out = prompt("Bob")
    assert out == "ok"
    assert calls["completion"][0]["model"] == "fake"
    assert calls["completion"][0]["messages"] == [{"role": "user", "content": "Hello Bob"}]


def test_llm_passes_api_key(monkeypatch):
    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"content": "ok"}}]},
    )

    llm = LLM(model="fake", api_key="secret")

    @llm
    def prompt() -> str:
        return "x"

    assert prompt() == "ok"
    assert calls["completion"][0]["api_key"] == "secret"
    # Ensure api_key doesn't leak in repr
    assert "secret" not in repr(llm)


def test_llm_passes_api_key_async(monkeypatch):
    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"content": "ok"}}]},
        acompletion_resp={"choices": [{"message": {"content": "async-ok"}}]},
    )

    llm = LLM(model="fake", api_key="secret")

    @llm
    async def prompt() -> str:
        return "x"

    out = asyncio.run(prompt())
    assert out == "async-ok"
    assert calls["acompletion"][0]["api_key"] == "secret"


def test_llm_decorator_sync_structured(monkeypatch):
    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"parsed": {"summary": "hi"}}}]},
    )

    llm = LLM(model="fake")

    @llm(return_type=Summary)
    def prompt(_: str) -> str:
        return "Return a summary"

    out = prompt("x")
    assert isinstance(out, Summary)
    assert out.summary == "hi"
    assert calls["completion"][0]["response_format"] is Summary


def test_llm_infers_return_type_from_annotation(monkeypatch):
    """If `return_type` isn't provided, LLM should infer it from `-> BaseModel`."""

    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"parsed": {"summary": "hi"}}}]},
    )

    llm = LLM(model="fake")

    @llm
    def prompt(_: str) -> Summary:  # type: ignore[return-value]
        return "Return a summary"

    out = prompt("x")
    assert isinstance(out, Summary)
    assert out.summary == "hi"
    assert calls["completion"][0]["response_format"] is Summary


def test_llm_decorator_rejects_non_str_prompt(monkeypatch):
    _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"content": "ok"}}]},
    )
    llm = LLM(model="fake")

    @llm
    def prompt() -> str:  # type: ignore[return-value]
        return 123  # noqa: PLR2004

    with pytest.raises(TypeError, match="must return a str prompt"):
        prompt()


def test_llm_decorator_accepts_str_return_type(monkeypatch):
    llm = LLM(model="fake")

    calls = _install_fake_litellm(
        monkeypatch=monkeypatch,
        completion_resp={"choices": [{"message": {"content": "ok"}}]},
    )

    # return_type=str is supported as explicit text mode
    @llm(return_type=str)
    def prompt() -> str:
        return "x"

    assert prompt() == "ok"

    # Ensure we did NOT request structured output.
    assert "response_format" not in calls["completion"][0]


def test_llm_decorator_rejects_none_return_type():
    llm = LLM(model="fake")

    with pytest.raises(TypeError, match="return_type cannot be None"):

        @llm(return_type=None)  # type: ignore[arg-type]
        def prompt() -> str:
            return "x"


def test_llm_decorator_rejects_non_type_return_type():
    llm = LLM(model="fake")

    with pytest.raises(TypeError, match="return_type must be a type annotation"):

        @llm(return_type=object())  # type: ignore[arg-type]
        def prompt() -> str:
            return "x"


def test_llm_decorator_async_text(monkeypatch):
    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"content": "ok"}}]},
        acompletion_resp={"choices": [{"message": {"content": "async-ok"}}]},
    )

    llm = LLM(model="fake")

    @llm
    async def prompt(name: str) -> str:
        return f"Hello {name}"

    out = asyncio.run(prompt("Bob"))
    assert out == "async-ok"
    assert calls["acompletion"][0]["messages"] == [{"role": "user", "content": "Hello Bob"}]


def test_llm_decorator_async_structured(monkeypatch):
    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"content": "ignored"}}]},
        acompletion_resp={"choices": [{"message": {"parsed": {"summary": "async-hi"}}}]},
    )

    llm = LLM(model="fake")

    @llm(return_type=Summary)
    async def prompt() -> str:
        return "Return a summary"

    out = asyncio.run(prompt())
    assert isinstance(out, Summary)
    assert out.summary == "async-hi"
    assert calls["acompletion"][0]["response_format"] is Summary


def test_llm_extract_text_content_attr_path(monkeypatch):
    """Cover the `resp.choices[0].message.content` extraction branch."""

    class _Msg:
        content = "attr-ok"

    class _Choice:
        message = _Msg()

    class _Resp:
        choices = [_Choice()]

    calls = _install_fake_litellm(monkeypatch, completion_resp=_Resp())

    llm = LLM(model="fake")

    @llm
    def prompt() -> str:
        return "x"

    assert prompt() == "attr-ok"


def test_llm_extract_text_content_raises(monkeypatch):
    """Cover the error path when the response isn't OpenAI-like."""
    calls = _install_fake_litellm(monkeypatch, completion_resp=object())
    llm = LLM(model="fake")

    @llm
    def prompt() -> str:
        return "x"

    with pytest.raises(TypeError, match="Unable to extract text completion"):
        prompt()


def test_llm_extract_structured_from_content_json(monkeypatch):
    """Cover structured output fallback parsing from message content as JSON."""
    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"content": '{"summary": "from-json"}'}}]},
    )

    llm = LLM(model="fake")

    @llm(return_type=Summary)
    def prompt() -> str:
        return "x"

    out = prompt()
    assert isinstance(out, Summary)
    assert out.summary == "from-json"


def test_llm_extract_structured_attr_parsed(monkeypatch):
    """Cover the `resp.choices[0].message.parsed` structured extraction branch."""

    class _Msg:
        parsed = {"summary": "attr-parsed"}

    class _Choice:
        message = _Msg()

    class _Resp:
        choices = [_Choice()]

    calls = _install_fake_litellm(monkeypatch, completion_resp=_Resp())

    llm = LLM(model="fake")

    @llm(return_type=Summary)
    def prompt() -> str:
        return "x"

    out = prompt()
    assert isinstance(out, Summary)
    assert out.summary == "attr-parsed"


def test_llm_async_requires_acompletion(monkeypatch):
    """Cover the branch where litellm lacks `acompletion`."""
    mod = types.SimpleNamespace()

    def completion(**kwargs):
        return {"choices": [{"message": {"content": "ok"}}]}

    # Intentionally do NOT expose acompletion
    mod.completion = completion
    monkeypatch.setitem(sys.modules, "litellm", mod)

    llm = LLM(model="fake")

    @llm
    async def prompt() -> str:
        return "x"

    with pytest.raises(RuntimeError, match="does not expose `acompletion`"):
        asyncio.run(prompt())


def test_llm_decorator_sync_wrapped_scalar(monkeypatch):
    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"parsed": {"result": 123}}}]},
    )

    llm = LLM(model="fake")

    @llm(return_type=int)
    def prompt() -> str:
        return "Return an integer"

    out = prompt()
    assert out == 123
    assert isinstance(out, int)
    assert "response_format" in calls["completion"][0]

    # The decorated callable should advertise the inner type (int), not the wrapper model.
    assert prompt.__annotations__["return"] is int


def test_llm_infers_wrapped_scalar_from_annotation(monkeypatch):
    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"parsed": {"result": 7}}}]},
    )

    llm = LLM(model="fake")

    @llm
    def prompt() -> int:  # type: ignore[return-value]
        return "Return an integer"

    out = prompt()
    assert out == 7
    assert isinstance(out, int)
    assert "response_format" in calls["completion"][0]
    assert prompt.__annotations__["return"] is int


def test_llm_decorator_rejects_string_forward_ref_return_type():
    llm = LLM(model="fake")

    with pytest.raises(TypeError, match="string forward reference"):

        @llm(return_type="ForwardRef")  # type: ignore[arg-type]
        def prompt() -> str:
            return "x"


def test_require_type_annotation_rejects_none():
    with pytest.raises(TypeError, match="return_type cannot be None"):
        _require_type_annotation(None)


def test_llm_decorator_accepts_typing_construct_return_type(monkeypatch):
    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"parsed": {"result": [1, 2]}}}]},
    )

    llm = LLM(model="fake")

    @llm(return_type=list[int])
    def prompt() -> str:
        return "Return a list of ints"

    out = prompt()
    assert out == [1, 2]
    assert isinstance(out, list)
    assert "response_format" in calls["completion"][0]


def test_llm_inference_falls_back_when_get_type_hints_raises(monkeypatch):
    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"content": "ok"}}]},
    )

    llm = LLM(model="fake")

    @llm
    def prompt() -> "NotDefined":  # noqa: F821
        return "x"

    out = prompt()
    assert out == "ok"
    assert "response_format" not in calls["completion"][0]


def test_llm_inference_falls_back_when_require_pydantic_model_raises(monkeypatch):
    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"content": "ok"}}]},
    )

    llm = LLM(model="fake")

    def boom(_: type[BaseModel]) -> type[BaseModel]:
        raise TypeError("boom")

    monkeypatch.setattr("etl_decorators.llms.llm.require_pydantic_model", boom)

    @llm
    def prompt() -> Summary:  # type: ignore[return-value]
        return "x"

    out = prompt()
    assert out == "ok"
    assert "response_format" not in calls["completion"][0]
    assert prompt.__annotations__["return"] is str


def test_llm_async_wrapped_scalar(monkeypatch):
    calls = _install_fake_litellm(
        monkeypatch,
        completion_resp={"choices": [{"message": {"content": "ignored"}}]},
        acompletion_resp={"choices": [{"message": {"parsed": {"result": 5}}}]},
    )

    llm = LLM(model="fake")

    @llm(return_type=int)
    async def prompt() -> str:
        return "Return an int"

    out = asyncio.run(prompt())
    assert out == 5
    assert isinstance(out, int)
    assert "response_format" in calls["acompletion"][0]


def test_require_pydantic_model_rejects_non_type():
    with pytest.raises(TypeError, match="pydantic.BaseModel"):
        require_pydantic_model("nope")


def test_require_pydantic_model_rejects_non_basemodel_subclass():
    class NotModel:
        pass

    with pytest.raises(TypeError, match="pydantic.BaseModel"):
        require_pydantic_model(NotModel)
