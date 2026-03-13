# `etl_decorators.llms`

Small helpers for ETL pipelines that call Large Language Models (LLMs).

This package is an **optional extra**:

```bash
pip install etl-decorators[llms]
```

## User documentation

User-facing usage and examples live under `doc/`:

- [`doc/llm.md`](../../../doc/llm.md)

## Optional dependencies & lazy imports

This subpackage does **not** import `litellm` at import time. Instead, `litellm`
is required only when an LLM-decorated function is executed.

If the dependency is missing, the code raises a `RuntimeError` with an install
hint (e.g. `pip install etl-decorators[llms]`).

## Code layout

The implementation is split into small modules:

- [`llm.py`](llm.py): the `LLM` decorator / request orchestration
- [`validators.py`](validators.py): runtime validation (prompt is `str`, return_type is BaseModel)
- [`response_parsing.py`](response_parsing.py): extraction of text / structured output from litellm responses
- [`pydantic_utils.py`](pydantic_utils.py): optional import of `pydantic.BaseModel`

## Tests

Tests live under [`tests/llms/`](../../../tests/llms/).

Note: integration tests that hit a real provider are environment-gated and may
be skipped unless model/API key env vars are set.

[`__init__.py`](__init__.py) re-exports `LLM` to preserve a stable import path:

```python
from etl_decorators.llms import LLM
```
