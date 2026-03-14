# `etl_decorators.resilience.timeout`

Enforce a maximum execution time for ETL steps.

## Usage

```python
from etl_decorators.resilience import timeout


@timeout(seconds=10)
def step() -> None:
    ...
```

## Async usage

```python
from etl_decorators.resilience import timeout


@timeout(seconds=10)
async def step_async() -> None:
    ...
```

## Parameters

- `seconds: float` (required, must be `> 0`)
- `timeout_error: type[Exception]` (default: `TimeoutError`)
- `message: str | None` (default: `None`)

## Implementation notes / limitations

- **Async**: implemented with `asyncio.wait_for`.
- **Sync**: implemented with a background thread.
  When a timeout occurs, the wrapped function cannot be forcefully stopped in
  pure Python and may keep running in the background.
