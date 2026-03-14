# `etl_decorators.llms.LLM`

`etl_decorators.llms.LLM` is a small convenience wrapper around
[`litellm`](https://github.com/BerriAI/litellm) that lets you:

- write a function whose job is only to **build a prompt** (returns `str`)
- decorate it with an `LLM` instance
- call it like a regular function to get either:
  - a **completion string**, or
  - a **structured Pydantic model** (via litellm structured output)

## Installation

```bash
pip install etl-decorators[llms]
```

## Basic usage (text output)

```python
from etl_decorators.llms import LLM

llm = LLM(model="gpt-4o-mini")


@llm
def summarize(text: str) -> str:
    return f"Summarize the following text in 3 bullet points:\n\n{text}"


result: str = summarize("Hello world")
```

The decorated function (`summarize`) returns a `str` completion.

## Structured output (Pydantic)

If you pass `return_type=...`, it **must** be a subclass of `pydantic.BaseModel`.
The wrapper will ask litellm for structured output and will return an instance
of that model.

```python
from pydantic import BaseModel
from etl_decorators.llms import LLM


class Summary(BaseModel):
    title: str
    bullets: list[str]


llm = LLM(model="gpt-4o-mini")


@llm(return_type=Summary)
def summarize(text: str) -> str:
    return f"Return JSON with title and bullets for: {text}"


result: Summary = summarize("Hello world")
```

## Async prompt builders

If your prompt-building function is `async def`, the decorated callable is also
async, and the wrapper will call `litellm.acompletion(...)`.

```python
from etl_decorators.llms import LLM

llm = LLM(model="gpt-4o-mini")


@llm
async def summarize(user_id: int) -> str:
    # example: fetch data from an API/DB
    profile = await fetch_profile(user_id)
    return f"Summarize this profile: {profile}"


result = await summarize(123)
```

## Passing model/provider parameters

Pass default `litellm.completion(...)` arguments via `completion_kwargs`:

```python
llm = LLM(
    model="gpt-4o-mini",
    completion_kwargs={
        "temperature": 0.2,
        "max_tokens": 400,
    },
)

```

You can also pass an explicit `api_key` to the `LLM` constructor (it will be
forwarded to litellm as `api_key=...`):

```python
llm = LLM(
    model="openai/gpt-4o-mini",
    api_key="...",
)
```

## Notes / limitations

- The current message format is a single `user` message containing the prompt.
- Structured-output support depends on litellm/provider capabilities.
- The wrapper includes a few defensive extraction paths to read either text
  output or structured output from the returned response.

## Decorator stacking / interoperability

### Inferring structured output from annotations

When you don't pass `return_type=...`, `LLM` will **infer** structured output
from the prompt builder's annotated return type when it is a
`pydantic.BaseModel` subclass.

```python
from pydantic import BaseModel
from etl_decorators.llms import LLM


class Summary(BaseModel):
    summary: str


llm = LLM(model="gpt-4o-mini")


@llm
def summarize(text: str) -> Summary:  # type: ignore[return-value]
    return f"Return JSON with a summary of: {text}"
```

Note: because the function body still returns a prompt `str`, static type
checkers may complain. Use `@llm(return_type=Summary)` if you'd rather keep the
prompt builder annotated as `-> str`.

### Stacking with `materialized_property`

`etl_decorators.sqlalchemy.materialized_property` inspects runtime return
annotations to choose storage types. `LLM` therefore rewrites the wrapped
callable's runtime `__annotations__["return"]` to reflect the *actual* decorated
return type when using structured output.

This enables patterns like:

```python
from etl_decorators.sqlalchemy import materialized_property


class Model(Base):
    @materialized_property
    @llm(return_type=Summary)
    def summary(self) -> str:
        return "Return a summary"
```

Important: prefer decorator order bottom-up as:
`@template` (prompt rendering) -> `@llm` (LLM call) -> `@materialized_property`
(DB caching).
