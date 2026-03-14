# `etl_decorators.logging.log`

`etl_decorators.logging.log` is a small decorator that wraps an ETL step with
stdlib logging.

It emits:

- a **START** log line before executing the function
- a **DONE** log line after completion
- or an **ERROR** log line (with traceback) when an exception is raised

Each invocation gets a short *run id* (8 hex chars) so you can correlate the
START/DONE/ERROR lines for a given call.

## Basic usage

```python
from etl_decorators.logging import log


@log(level="INFO", with_arguments=True, with_result=False, with_duration=True)
def extract(org_id: str) -> list[int]:
    return [1, 2, 3]


_ = extract("acme")

# Example log lines:
# [START 1a2b3c4d] @my_module.py:12 extract(org_id='acme')
# [DONE  1a2b3c4d] @my_module.py:12 extract: duration=0.012345s
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
- `with_location: bool = True`
  - include `@<filename>:<line>` where the decorated function is defined

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
