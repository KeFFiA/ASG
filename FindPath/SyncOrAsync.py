import asyncio
from functools import wraps


def sync_async_method(func):
    @wraps(func)
    def wrapper(self, *args, **kwargs):  # Явно принимаем self
        try:
            loop = asyncio.get_running_loop()
            async def async_wrapper():
                return await func(self, *args, **kwargs)
            return async_wrapper()
        except RuntimeError:
            return asyncio.run(func(self, *args, **kwargs))
    return wrapper
