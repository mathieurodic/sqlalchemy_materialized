# `etl_decorators.resilience.retry`

General-purpose retries with exponential backoff.

## Usage

```python
from etl_decorators.resilience import retry


@retry(
    retry_on=RuntimeError,
    max_attempts=3,
    interval=1.0,
    factor=2.0,
)
def fetch() -> str:
    ...
```

## Async usage

```python
from etl_decorators.resilience import retry


@retry(retry_on=RuntimeError, max_attempts=3)
async def fetch_async() -> str:
    ...
```

## Predicate-based retry

If you need custom logic, use `retry_if`. If `retry_if` is provided, it takes
precedence over `retry_on`.

```python
from etl_decorators.resilience import retry


def is_transient(exc: BaseException) -> bool:
    return isinstance(exc, ValueError) and str(exc) == "transient"


@retry(retry_if=is_transient, max_attempts=5, interval=0.25)
def step() -> int:
    ...
```

## Backoff formula

For attempt numbers starting at 1, the sleep before retrying is:

```text
sleep = interval * (factor ** (attempt - 1))
```

## Jitter

Set `jitter` to randomize the delay (to reduce thundering herd effects).

This decorator uses *multiplicative* jitter:

```text
sleep *= uniform(1-jitter, 1+jitter)
```

So `jitter=0.1` applies a random multiplier in `[0.9, 1.1]`.

## Non-retriable exceptions

The decorator never retries:

- `KeyboardInterrupt`
- `SystemExit`
- `GeneratorExit`
- `asyncio.CancelledError`
