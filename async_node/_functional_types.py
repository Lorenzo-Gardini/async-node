from typing import TypeVar, Callable, Awaitable

# Generic Input
I = TypeVar("I")
# Generic Output
O = TypeVar("O")

# A function that takes no arguments and returns a value of type I
type Supplier[I] = Callable[[], I]

# An asynchronous function that takes no arguments and returns a value of type I
type AsyncSupplier[I] = Callable[[], Awaitable[I]]

# A function that takes an argument of type I and returns a value of type O
type Function[I, O] = Callable[[I], O]

# An asynchronous function that takes an argument of type I and returns a value of type O
type AsyncFunction[I, O] = Callable[[I], Awaitable[O]]

# A function that takes no arguments and returns nothing (like a Runnable in Java)
type Runnable = Callable[[], None]

# An asynchronous function that takes no arguments and returns nothing
type AsyncRunnable = Callable[[], Awaitable[None]]

# A function that consumes a value of type I and returns nothing
type Consumable[I] = Callable[[I], None]

# An asynchronous function that consumes a value of type I and returns nothing
type AsyncConsumable[I] = Callable[[I], Awaitable[None]]

# A function that takes any number of arguments and combines them into a single output of type O
type CombiningFunction[O] = Callable[..., O]

# An asynchronous function that takes any number of arguments and combines them into a single output of type O
type AsyncCombiningFunction[O] = Callable[..., Awaitable[O]]