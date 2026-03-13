# `etl_decorators.logging.log`

`etl_decorators.logging.log` is a small decorator that wraps an ETL step with
stdlib logging.

It emits:

- a **start** log line before executing the function
- an **end** log line after completion
- or a **failed** log line (with traceback) when an exception is raised

## Basic usage

```python
from etl_decorators.logging import log


@log(level="INFO", with_arguments=True, with_result=False, with_duration=True)
def extract(org_id: str) -> list[int]:
    return [1, 2, 3]


_ = extract("acme")
```

## Parameters

- `level: str | int = "DEBUG"`
  - accepts typical level names (`"DEBUG"`, `"INFO"`, `"WARNING"`, ...)
  - also accepts integers (`10`, `20`, ...)
- `with_result: bool = True`
  - include a safe/truncated representation of the returned value
- `with_arguments: bool = True`
  - include a safe/truncated representation of bound arguments
- `with_duration: bool = True`
  - include duration in seconds (using `time.perf_counter()`)

## Async functions

`log` supports `async def` functions.

```python
import asyncio
from etl_decorators.logging import log


@log(level="DEBUG")
async def step(x: int) -> int:
    return x + 1


assert asyncio.run(step(1)) == 2
```

## Safety notes

- Representations of arguments/results are **best-effort** (repr should not
  raise) and **truncated** to avoid logging huge payloads by default.
- Exceptions are logged with `exc_info=True` so handlers can include full
  tracebacks.
