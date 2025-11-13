import asyncio
from asyncio import Future
from concurrent.futures import Executor
from functools import partial
from typing import Callable, Any

from .schedulers import Schedulers


def _wrapper_template(func: Callable[..., Any], executor: Executor, *args, **kwargs) -> Future[Any]:
    partial_function: Callable[[], Any] = partial(func, *args, **kwargs)
    return asyncio.get_event_loop().run_in_executor(executor, partial_function)

def io(func: Callable[..., Any]) -> Callable[..., Future[Any]]:
    def wrapper(*args, **kwargs) -> Future[Any]:
        return _wrapper_template(func, Schedulers.io(), *args, **kwargs)
    return wrapper

def computation(func: Callable[..., Any]) -> Callable[..., Future[Any]]:
    def wrapper(*args, **kwargs) -> Future[Any]:
        return _wrapper_template(func, Schedulers.computation(), *args, **kwargs)
    return wrapper

def single(func: Callable[..., Any]) -> Callable[..., Future[Any]]:
    def wrapper(*args, **kwargs) -> Future[Any]:
        return _wrapper_template(func, Schedulers.single(), *args, **kwargs)
    return wrapper
