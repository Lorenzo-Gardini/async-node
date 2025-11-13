# AsyncNode

**AsyncNode** is a Python asynchronous computation wrapper that allows you to create, chain, combine, and execute asynchronous computations flexibly, optionally using executors for CPU-bound or IO-bound tasks. It supports wrapping synchronous and asynchronous computations, chaining operations using `map`, `run`, `consume`, combining multiple `AsyncNode` instances, handling exceptions synchronously or asynchronously, and lazy evaluation with caching. `Schedulers` is a centralized factory for executors that provides **singleton-style access**, meaning each executor instance (IO-bound, CPU-bound, or single-threaded) is created only once and reused throughout your application. Executors are lazily instantiated when first accessed.

You can create nodes from values, synchronous suppliers, or synchronous runnables, for example:

```python
from async_node.async_node import AsyncNode

node1 = AsyncNode.from_value(10)
node2 = AsyncNode.from_supplier(lambda: 5)
node3 = AsyncNode.from_runnable(lambda: print("Task done"))
```

Nodes can be transformed using synchronous mapping functions:

```python
node4 = node1.map(lambda x: x * 2)
```

or asynchronous mapping functions:

```python
async def async_double(x):
    await asyncio.sleep(1)
    return x * 2

node5 = node2.map_async(async_double)
```

Side-effects can be run synchronously or asynchronously:

```python
node1.run(lambda: print("Value computed"))

async def async_print():
    await asyncio.sleep(1)
    print("Async task completed")

node2.run_async(async_print)
```

Nodes can be combined using synchronous functions:

```python
def combine_sum(a, b):
    return a + b

combined_node = node1.combine(node2, combine_function=combine_sum)
```

or asynchronous functions:

```python
async def async_combine(a, b):
    await asyncio.sleep(1)
    return a + b

combined_async_node = node1.combine_async(node2, combine_function=async_combine)
```

Values can be consumed synchronously or asynchronously:

```python
node1.consume(lambda x: print(f"Consumed value: {x}"))

async def async_consume(x):
    await asyncio.sleep(1)
    print(f"Consumed async: {x}")

node2.consume_async(async_consume)
```

Exceptions can be handled synchronously or asynchronously:

```python
def handle_exception(e):
    print(f"Exception occurred: {e}")
    return 0

safe_node = node1.exceptionally(handle_exception)

async def async_handle_exception(e):
    print(f"Async exception: {e}")
    return 0

safe_node_async = node2.exceptionally_async(async_handle_exception)
```

Executors can be used via `Schedulers`, and they are **singleton**, so each call returns the same instance:

```python
from async_node.schedulers import Schedulers

io_executor = Schedulers.io()  # singleton IO-bound executor
cpu_executor = Schedulers.computation()  # singleton CPU-bound executor
single_executor = Schedulers.single()  # singleton single-thread executor

node_with_executor = node1.on(cpu_executor)
node_on_main = node_with_executor.on_main_thread()
```

Worker timeouts can be configured before executors are created:

```python
Schedulers.set_workers_timeout(30)  # seconds
```

Finally, results are retrieved asynchronously:

```python
import asyncio

async def main():
    result = await node1.get()
    print(result)

asyncio.run(main())
```

`AsyncNode` makes it easy to manage asynchronous workflows in Python, supporting both synchronous and asynchronous functions, chaining, combination, exception handling, and optional executor usage, with lazy evaluation and caching for efficiency. `Schedulers` simplifies managing and reusing singleton executors for IO-bound, CPU-bound, or single-threaded tasks, ensuring optimal performance across different computation types.

### Executor Decorators for AsyncNode

You can conveniently decorate synchronous functions to run asynchronously on specific executors using the decorators:

`@io`: Runs the function on the IO-bound thread pool executor.

`@computation`: Runs the function on the CPU-bound thread pool executor.

`@single`: Runs the function on a single-threaded executor (useful for tasks requiring serialized execution).

```python
import asyncio
from async_node.decorators import io, computation, single

@io
def blocking_io_task(seconds: int) -> str:
    import time
    time.sleep(seconds)  # blocking IO simulation
    return f"IO task slept for {seconds} seconds"

@computation
def cpu_intensive_task(n: int) -> int:
    return sum(i * i for i in range(n))

@single
def single_thread_task(message: str) -> str:
    return f"Single-thread says: {message}"

async def main():
    # Schedule all tasks concurrently
    futures = [
        blocking_io_task(2),
        cpu_intensive_task(10_000),
        single_thread_task("Hello from single thread"),
    ]

    # Await all results concurrently using gather
    io_result, cpu_result, single_result = await asyncio.gather(*futures)

    print(io_result)          # IO task slept for 2 seconds
    print(f"CPU task result: {cpu_result}")  # CPU task result: sum of squares
    print(single_result)      # Single-thread says: Hello from single thread

asyncio.run(main())
```