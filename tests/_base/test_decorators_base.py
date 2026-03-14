import asyncio

def test_decorator_base_process_result_async_delegates_to_sync():
    from etl_decorators._base.decorators import DecoratorBase

    class D(DecoratorBase[[int], int, None]):
        def process_result(self, fn, result, args, kwargs, state):
            return result + 1

    async def f(x: int) -> int:
        return x

    wrapped = D().decorate(f)
    assert asyncio.run(wrapped(1)) == 2


def test_decorator_base_async_process_exception_can_handle():
    from etl_decorators._base.decorators import DecoratorBase

    class D(DecoratorBase[[], int, None]):
        def process_result(self, fn, result, args, kwargs, state):
            return result

        async def process_exception_async(self, fn, exc, args, kwargs, state):
            return 123

    async def f() -> int:
        raise ValueError("boom")

    wrapped = D().decorate(f)
    assert asyncio.run(wrapped()) == 123


def test_decorator_base_sync_process_exception_can_handle():
    from etl_decorators._base.decorators import DecoratorBase

    class D(DecoratorBase[[], int, None]):
        def process_result(self, fn, result, args, kwargs, state):
            return result

        def process_exception(self, fn, exc, args, kwargs, state):
            return 456

    def f() -> int:
        raise ValueError("boom")

    wrapped = D().decorate(f)
    assert wrapped() == 456


def test_optional_fn_decorator_base_binds_optional():
    from etl_decorators._base.decorators import OptionalFnDecoratorBase

    binder = OptionalFnDecoratorBase()

    def deco(fn):
        def wrapped(*args, **kwargs):
            return fn(*args, **kwargs) + 1

        return wrapped

    def f() -> int:
        return 1

    # fn=None path
    d = binder.bind_optional(None, deco)
    assert d(f)() == 2

    # fn provided path
    assert binder.bind_optional(f, deco)() == 2
