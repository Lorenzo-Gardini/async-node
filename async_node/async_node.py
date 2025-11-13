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
from typing import Any, Generic, Awaitable, Union

# Type aliases imported from your functional_types
from ._functional_types import (
    I, Supplier, Function, O, Runnable, Consumable,
    AsyncFunction, AsyncRunnable, CombiningFunction,
    AsyncCombiningFunction, AsyncConsumable
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
    executor : Executor | None
        The executor to run blocking/cpu-bound tasks, if needed.
    """

    def __init__(self, future_result: Awaitable[I], executor: Executor | None=None):
        self.future_result: Awaitable[I] = future_result
        self.executor: Executor = executor
        self._cached_result: I | None = None


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
            values: list[Any] = await asyncio.gather(*([self.get()] + [n.get() for n in nodes]))
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
            values: list[Any] = await asyncio.gather(*([self.get()] + [n.get() for n in nodes]))
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

    def peek(self, function: Consumable[I]) -> 'AsyncNode[I]':
        """
        Apply a synchronous side-effect function to the value without modifying it.

        This method allows you to observe or log the result of the AsyncNode
        without transforming it, similar to `peek` in Java streams or RxJava.

        Parameters
        ----------
        function : Callable[[I], None]
            A synchronous function that takes the computed value as input and
            performs a side-effect (e.g., logging, debugging).

        Returns
        -------
        AsyncNode[I]
            A new AsyncNode containing the same value as the original,
            unchanged.

        Example
        -------
        ```python
        node.peek(lambda x: print(f"Value is {x}"))
        ```
        """

        async def wrapper() -> I:
            value: I = await self.get()
            await self._map(function, value, self.executor)
            return value

        return AsyncNode[I](wrapper(), executor=self.executor)

    def peek_async(self, function: AsyncConsumable[I]) -> 'AsyncNode[I]':
        """
        Apply an asynchronous side-effect function to the value without modifying it.

        This method allows you to observe or log the result of the AsyncNode
        asynchronously, without transforming it, similar to `peek` in Java streams
        or RxJava. The function should be an async function.

        Parameters
        ----------
        function : Callable[[I], Awaitable[None]]
            An asynchronous function that takes the computed value as input and
            performs a side-effect (e.g., logging, debugging).

        Returns
        -------
        AsyncNode[I]
            A new AsyncNode containing the same value as the original,
            unchanged.

        Example
        -------
        ```python
        async def log_value(x):
            await asyncio.sleep(0.1)
            print(f"Async value is {x}")

        node.peek_async(log_value)
        ```
        """

        async def wrapper() -> I:
            value: I = await self.get()
            await function(value)
            return value

        return AsyncNode[I](wrapper(), executor=self.executor)

    # -----------------------
    # DELAYED EXECUTION
    # -----------------------

    def wait(self, delay: float=0.0) -> 'AsyncNode[I]':
        """
        Delay the completion of this AsyncNode by a specified amount of time.

        This method returns a new AsyncNode that waits for the original node
        to complete and then optionally delays the result by `delay` seconds.
        Useful for scheduling or pacing asynchronous computations.

        Parameters
        ----------
        delay : float, optional
            Number of seconds to wait after the original computation completes
            before returning the result (default is 0.0).

        Returns
        -------
        AsyncNode[I]
            A new AsyncNode that produces the same result as the original node
            after the optional delay.

        Example
        -------
        ```python
        result = await node.wait(2.0).get()  # waits for node and then 2 more seconds
        ```
        """

        async def wrapper() -> I:
            if delay > 0:
                await asyncio.sleep(delay)
            return await self.get()
        return AsyncNode[I](wrapper(), self.executor)

    # -----------------------
    # ERROR HANDLING
    # -----------------------

    def exceptionally(self, handler: Function[Exception, O]) -> Union['AsyncNode[I]', 'AsyncNode[O]']:
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

    def exceptionally_async(self, handler: AsyncFunction[Exception, O]) -> Union['AsyncNode[I]', 'AsyncNode[O]']:
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

    def retry(self, times: int, delay: float=0.1) -> 'AsyncNode[I]':
        """
        Retry the AsyncNode computation a specified number of times if it fails.

        This method attempts to execute the AsyncNode up to `times` times if
        exceptions are raised. Optionally, a delay can be added between retries.
        It is useful for transient errors such as network failures or temporary
        resource contention.

        Parameters
        ----------
        times : int
            Maximum number of attempts before giving up.
        delay : float, optional
            Delay in seconds between retries (default is 0).

        Returns
        -------
        AsyncNode[I]
            A new AsyncNode that retries the original computation and eventually
            returns the computed value if successful.

        Raises
        ------
        Exception
            If all retry attempts fail, the last exception encountered is raised.

        Example
        -------
        ```python
        node.retry(times=3, delay=1.0)  # retry up to 3 times with 1 second delay
        ```
        """

        async def wrapper() -> O:
            last_exception: Exception | None = None
            for _ in range(times):
                try:
                    return await self.get()
                except Exception as e:
                    last_exception = e
                    if delay > 0:
                        await asyncio.sleep(delay)
            raise last_exception

        return AsyncNode[O](wrapper(), executor=self.executor)

    def retry_backoff(self, times: int, initial_delay: float=0.1, factor: float=2.0) -> 'AsyncNode[I]':
        """
        Retry the AsyncNode computation a specified number of times using exponential backoff.

        Parameters
        ----------
        times : int
            Maximum number of attempts before giving up.
        initial_delay : float, optional
            Initial delay in seconds before the first retry (default 0.1).
        factor : float, optional
            Exponential factor to increase delay after each failure (default 2.0).

        Returns
        -------
        AsyncNode[I]
            A new AsyncNode that retries the computation with exponential backoff.
        """

        async def wrapper() -> I:
            delay: float = initial_delay
            last_exception: Exception | None = None
            for _ in range(times):
                try:
                    return await self.get()
                except Exception as ex:
                    last_exception = ex
                    await asyncio.sleep(delay)
                    delay *= factor
            raise last_exception

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

    async def get_with_timeout(self, timeout: float) -> I:
        """
        Retrieve the result of the AsyncNode, but fail if it takes longer than the specified timeout.

        This method wraps the regular `get()` call with a timeout using
        `asyncio.wait_for`. If the underlying computation does not complete
        within the given time, an `asyncio.TimeoutError` is raised.

        Parameters
        ----------
        timeout : float
            Maximum number of seconds to wait for the result.

        Returns
        -------
        I
            The computed value of the AsyncNode if it completes in time.

        Raises
        ------
        asyncio.TimeoutError
            If the computation does not complete within the specified timeout.

        Example
        -------
        ```python
        result = await node.get_with_timeout(5.0)  # waits up to 5 seconds
        ```
        """
        return await asyncio.wait_for(self.get(), timeout=timeout)

    def block(self) -> I:
        """
        Synchronously retrieve the result of the AsyncNode, blocking until it is ready.

        This method allows converting an asynchronous computation into a
        synchronous one. It runs the underlying awaitable in the current event loop
        until completion. Use this for testing or in synchronous code that needs
        the result immediately.

        Returns
        -------
        I
            The computed value of the AsyncNode.

        Example
        -------
        ```python
        result = node.block()  # waits for the AsyncNode to complete
        ```

        Notes
        -----
        - If called from within an existing running event loop (e.g., inside another
          async function), this will raise a `RuntimeError`. In such cases, use
          `await node.get()` instead.
        """
        return asyncio.get_event_loop().run_until_complete(self.get())

    def block_with_timeout(self, timeout: float) -> I:
        """
        Synchronously retrieve the result of the AsyncNode, blocking up to a specified timeout.

        This method allows converting an asynchronous computation into a synchronous one,
        but will raise an exception if the computation does not complete within the
        specified `timeout` in seconds. It wraps the `get_with_timeout` coroutine
        and executes it in the current event loop.

        Parameters
        ----------
        timeout : float
            Maximum number of seconds to wait for the result.

        Returns
        -------
        I
            The computed value of the AsyncNode if it completes within the timeout.

        Raises
        ------
        asyncio.TimeoutError
            If the AsyncNode does not complete within the given timeout.
        RuntimeError
            If called from within a running event loop (e.g., from another async function),
            since `run_until_complete` cannot be called from a running loop.

        Example
        -------
        ```python
        try:
            result = node.block_with_timeout(5.0)  # Wait up to 5 seconds synchronously
        except asyncio.TimeoutError:
            print("Computation did not finish in time")
        ```

        Notes
        -----
        - Use this method for synchronous code that requires a blocking call with a timeout.
        - In asynchronous contexts, prefer `await node.get_with_timeout(timeout)` instead
          to avoid `RuntimeError`.
        """
        return asyncio.get_event_loop().run_until_complete(self.get_with_timeout(timeout))

    # -----------------------
    # COMPUTATIONAL FUNCTIONS
    # -----------------------

    @staticmethod
    async def _run(function: Union[Runnable, Supplier[O]], executor: Executor | None=None) -> Union[None, O]:
        """
        Run a function using the executor if provided, otherwise synchronously.

        Parameters
        ----------
        function : Callable or Supplier
        executor : Executor | None

        Returns
        -------
        Result of function
        """
        if executor:
            return await asyncio.get_running_loop().run_in_executor(executor, function)
        else:
            return function()

    @staticmethod
    async def _map(function: Function[Any, O], argument: Any, executor: Executor | None=None) -> O:
        """
        Apply a synchronous function to an argument, optionally using an executor.

        Parameters
        ----------
        function : Callable[[I], O]
        argument : I
        executor : Executor | None

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
    # PARALLEL
    # -----------------------

    @classmethod
    def all_of(cls, nodes: list['AsyncNode[Any]']) -> 'AsyncNode[list[Any]]':
        """
        Combine multiple AsyncNodes and wait for all of them to complete.

        This method returns a new AsyncNode that completes when all the provided
        AsyncNodes have completed, collecting their results into a list. This is
        similar to `CompletableFuture.allOf` in Java.

        Parameters
        ----------
        nodes : list[AsyncNode[Any]]
            A list of AsyncNodes to wait for.

        Returns
        -------
        AsyncNode[list[Any]]
            A new AsyncNode containing a list of results from all input nodes,
            in the same order as the input list.
        """

        async def wrapper() -> list[Any]:
            return await asyncio.gather(*[n.get() for n in nodes])

        return cls[list[Any]](wrapper())

    @classmethod
    def any_of(cls, nodes: list['AsyncNode[Any]']) -> 'AsyncNode[Any]':
        """
        Wait for the first AsyncNode to complete among multiple nodes.

        This method returns a new AsyncNode that completes as soon as any one of the
        provided AsyncNodes completes. The result of this node will be the result
        of the first completed AsyncNode. This is similar to `CompletableFuture.anyOf` in Java.

        Parameters
        ----------
        nodes : list[AsyncNode[Any]]
            A list of AsyncNodes to wait for.

        Returns
        -------
        AsyncNode[Any]
            A new AsyncNode containing the result of the first completed input node.
        """

        async def wrapper() -> Any:
            done, _ = await asyncio.wait(*[n.get() for n in nodes], return_when=asyncio.FIRST_COMPLETED)
            return list(done)[0].result()

        return cls[Any](wrapper())

    # -----------------------
    # FACTORIES
    # -----------------------

    @classmethod
    def from_value(cls, value: I, executor: Executor | None=None) -> 'AsyncNode[I]':
        """
        Create an AsyncNode from a pre-existing value.

        Parameters
        ----------
        value : I
        executor : Executor | None

        Returns
        -------
        AsyncNode[I]
        """
        async def wrapper() -> I:
            return value
        return cls[I](wrapper(), executor=executor)

    @classmethod
    def from_supplier(cls, supplier: Supplier[I], executor: Executor | None=None) -> 'AsyncNode[I]':
        """
        Create an AsyncNode from a synchronous supplier function.

        Parameters
        ----------
        supplier : Callable[[], I]
        executor : Executor | None

        Returns
        -------
        AsyncNode[I]
        """
        async def async_supplier() -> I:
            return await cls._run(supplier, executor=executor)
        return cls[I](async_supplier(), executor=executor)

    @classmethod
    def from_runnable(cls, runnable: Runnable, executor: Executor | None=None) -> 'AsyncNode[None]':
        """
        Create an AsyncNode from a synchronous Runnable function.

        Parameters
        ----------
        runnable : Callable[[], None]
        executor : Executor | None

        Returns
        -------
        AsyncNode[None]
        """
        async def async_runnable() -> None:
            await cls._run(runnable, executor=executor)
        return cls[None](async_runnable(), executor=executor)