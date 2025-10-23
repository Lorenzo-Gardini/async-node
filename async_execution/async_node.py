"""
AsyncNode: A Python asynchronous computation wrapper.

This module provides the `AsyncNode` class, which allows chaining,
combining, and executing asynchronous computations in a flexible way,
optionally using a concurrent executor.

Core concepts:
- AsyncNode represents a computation that will produce a value of type I.
- You can chain operations using map, run, combine, etc.
- Executors can be used for CPU-bound or IO tasks.

---

Classes:
----------
AsyncNode(Generic[I])
    Represents an asynchronous computation producing a value of type I.
"""
import asyncio
from concurrent.futures import Executor
from typing import Any, Generic, List, Optional, Awaitable, Union

# Type aliases imported from your functional_types
from async_execution.functional_types import (
    I, Supplier, Function, O, Runnable, Consumable,
    AsyncFunction, AsyncRunnable, CombiningFunction,
    AsyncCombiningFunction, AsyncConsumable,
    ExceptionHandler, AsyncExceptionHandler
)


class AsyncNode(Generic[I]):
    """
    Represents an asynchronous computation producing a value of type I.

    Supports:
        - Mapping functions over the value (sync or async)
        - Running side-effect functions (sync or async)
        - Combining multiple AsyncNodes
        - Exception handling
        - Optional use of an Executor for CPU-bound tasks
        - Lazy retrieval of the result with caching

    Parameters
    ----------
    future_result : Awaitable[I]
        The underlying awaitable computation.
    executor : Optional[Executor]
        The executor to run blocking/cpu-bound tasks, if needed.
    """

    def __init__(self, future_result: Awaitable[I], executor: Optional[Executor]=None):
        self.future_result: Awaitable[I] = future_result
        self.executor: Executor = executor
        self._cached_result: Optional[I]=None

    # -----------------------
    # OPERATORS
    # -----------------------

    def map(self, function: Function[I, O]) -> 'AsyncNode[O]':
        """
        Apply a synchronous function to the result of this AsyncNode.

        Parameters
        ----------
        function : Callable[[I], O]
            Function to apply.

        Returns
        -------
        AsyncNode[O]
            A new AsyncNode containing the transformed value.
        """
        async def wrapper() -> O:
            return await self._map(function, await self.get(), self.executor)
        return AsyncNode[O](wrapper(), executor=self.executor)

    def map_async(self, function: AsyncFunction[I, O]) -> 'AsyncNode[O]':
        """
        Apply an asynchronous function to the result of this AsyncNode.

        Parameters
        ----------
        function : Callable[[I], Awaitable[O]]
            Async function to apply.

        Returns
        -------
        AsyncNode[O]
            A new AsyncNode containing the asynchronously transformed value.
        """
        async def wrapper() -> O:
            return await function(await self.get())
        return AsyncNode[O](wrapper(), executor=self.executor)

    def run(self, function: Runnable) -> 'AsyncNode[None]':
        """
        Run a synchronous side-effect function after this AsyncNode completes.

        Parameters
        ----------
        function : Callable[[], None] or Supplier[O]
            Function to run.

        Returns
        -------
        AsyncNode[None]
        """
        async def wrapper() -> None:
            await self.get()
            await self._run(function)
        return AsyncNode[None](wrapper(), executor=self.executor)

    def run_async(self, function: AsyncRunnable) -> 'AsyncNode[None]':
        """
        Run an asynchronous side-effect function after this AsyncNode completes.

        Parameters
        ----------
        function : Callable[[], Awaitable[None]]
            Async function to run.

        Returns
        -------
        AsyncNode[None]
        """
        async def wrapper() -> None:
            await self.get()
            await function()
        return AsyncNode[None](wrapper(), executor=self.executor)

    def combine(self, *nodes: 'AsyncNode[Any]', combine_function: CombiningFunction[O]) -> 'AsyncNode[O]':
        """
        Combine this AsyncNode with other AsyncNodes using a synchronous function.

        Parameters
        ----------
        nodes : AsyncNode[Any]
            Other nodes to combine.
        combine_function : Callable[..., O]
            Function combining all results.

        Returns
        -------
        AsyncNode[O]
        """
        async def wrapper() -> O:
            values: List[Any] = await asyncio.gather(*([self.get()] + [n.get() for n in nodes]))
            return await self._run(lambda: combine_function(*values), self.executor)
        return AsyncNode[O](wrapper(), executor=self.executor)

    def combine_async(self, *nodes: 'AsyncNode[Any]', combine_function: AsyncCombiningFunction[O]) -> 'AsyncNode[O]':
        """
        Combine this AsyncNode with other AsyncNodes using an asynchronous function.

        Parameters
        ----------
        nodes : AsyncNode[Any]
            Other nodes to combine.
        combine_function : Callable[..., Awaitable[O]]
            Async function combining all results.

        Returns
        -------
        AsyncNode[O]
        """
        async def wrapper() -> O:
            values: List[Any] = await asyncio.gather(*([self.get()] + [n.get() for n in nodes]))
            return await combine_function(*values)
        return AsyncNode[O](wrapper(), executor=self.executor)

    def consume(self, consumable: Consumable[I]) -> 'AsyncNode[None]':
        """
        Consume the value with a synchronous side-effect function.

        Parameters
        ----------
        consumable : Callable[[I], None]
            Function consuming the value.

        Returns
        -------
        AsyncNode[None]
        """
        async def wrapper() -> None:
            await self._map(consumable, await self.get(), self.executor)
        return AsyncNode[None](wrapper(), executor=self.executor)

    def consume_async(self, consumable: AsyncConsumable[I]) -> 'AsyncNode[None]':
        """
        Consume the value with an asynchronous side-effect function.

        Parameters
        ----------
        consumable : Callable[[I], Awaitable[None]]
            Async function consuming the value.

        Returns
        -------
        AsyncNode[None]
        """
        async def wrapper() -> None:
            await consumable(await self.get())
        return AsyncNode[None](wrapper(), executor=self.executor)

    # -----------------------
    # ERROR HANDLING
    # -----------------------

    def exceptionally(self, handler: ExceptionHandler[O]) -> Union['AsyncNode[I]', 'AsyncNode[O]']:
        """
        Handle exceptions synchronously if the computation fails.

        Parameters
        ----------
        handler : Callable[[Exception], O]
            Function to handle exceptions.

        Returns
        -------
        AsyncNode[I] or AsyncNode[O]
        """
        async def wrapper() -> I | O:
            try:
                return await self.get()
            except Exception as ex:
                return await self._map(handler, ex, self.executor)
        return AsyncNode[I | O](wrapper(), executor=self.executor)

    def exceptionally_async(self, handler: AsyncExceptionHandler[I]) -> 'AsyncNode[I]':
        """
        Handle exceptions asynchronously if the computation fails.

        Parameters
        ----------
        handler : Callable[[Exception], Awaitable[I]]
            Async function to handle exceptions.

        Returns
        -------
        AsyncNode[I]
        """
        async def wrapper() -> I:
            try:
                return await self.get()
            except Exception as ex:
                return await handler(ex)
        return AsyncNode[I](wrapper(), executor=self.executor)

    # -----------------------
    # EXECUTOR MANAGEMENT
    # -----------------------

    def on(self, executor: Executor) -> 'AsyncNode[I]':
        """
        Set an executor for this AsyncNode.

        Parameters
        ----------
        executor : Executor
            Executor to run tasks.

        Returns
        -------
        AsyncNode[I]
        """
        return AsyncNode[I](self.future_result, executor=executor)

    def on_main_thread(self) -> 'AsyncNode[I]':
        """
        Remove the executor and run tasks on the main thread.

        Returns
        -------
        AsyncNode[I]
        """
        return AsyncNode[I](self.future_result, executor=None)

    # -----------------------
    # RETRIEVE VALUE
    # -----------------------

    async def get(self) -> I:
        """
        Retrieve the result of the computation asynchronously.

        Returns
        -------
        I
            The computed value.
        """
        if self._cached_result is not None:
            return self._cached_result
        result: I = await self.future_result
        self._cached_result = result
        return result

    # -----------------------
    # COMPUTATIONAL FUNCTIONS
    # -----------------------

    @staticmethod
    async def _run(function: Union[Runnable, Supplier[O]], executor: Optional[Executor]=None) -> Union[None, O]:
        """
        Run a function using the executor if provided, otherwise synchronously.

        Parameters
        ----------
        function : Callable or Supplier
        executor : Optional[Executor]

        Returns
        -------
        Result of function
        """
        if executor:
            return await asyncio.get_running_loop().run_in_executor(executor, function)
        else:
            return function()

    @staticmethod
    async def _map(function: Function[I, O], argument: I, executor: Optional[Executor]=None) -> O:
        """
        Apply a synchronous function to an argument, optionally using an executor.

        Parameters
        ----------
        function : Callable[[I], O]
        argument : I
        executor : Optional[Executor]

        Returns
        -------
        O
        """
        if executor:
            result = await asyncio.get_running_loop().run_in_executor(executor, function, argument)
        else:
            result = function(argument)
        return result

    # -----------------------
    # FACTORIES
    # -----------------------

    @classmethod
    def from_value(cls, value: I, executor: Optional[Executor]=None) -> 'AsyncNode[I]':
        """
        Create an AsyncNode from a pre-existing value.

        Parameters
        ----------
        value : I
        executor : Optional[Executor]

        Returns
        -------
        AsyncNode[I]
        """
        async def wrapper():
            return value
        return cls(wrapper(), executor=executor)

    @classmethod
    def from_supplier(cls, supplier: Supplier[I], executor: Optional[Executor]=None) -> 'AsyncNode[I]':
        """
        Create an AsyncNode from a synchronous supplier function.

        Parameters
        ----------
        supplier : Callable[[], I]
        executor : Optional[Executor]

        Returns
        -------
        AsyncNode[I]
        """
        async def async_supplier() -> I:
            return await cls._run(supplier, executor=executor)
        return cls[I](async_supplier(), executor=executor)

    @classmethod
    def from_runnable(cls, runnable: Runnable, executor: Optional[Executor]=None) -> 'AsyncNode[None]':
        """
        Create an AsyncNode from a synchronous Runnable function.

        Parameters
        ----------
        runnable : Callable[[], None]
        executor : Optional[Executor]

        Returns
        -------
        AsyncNode[None]
        """
        async def async_runnable() -> None:
            await cls._run(runnable, executor=executor)
        return cls[None](async_runnable(), executor=executor)