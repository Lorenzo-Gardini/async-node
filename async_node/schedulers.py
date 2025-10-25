import os
from concurrent.futures import Executor, ThreadPoolExecutor
from typing import Optional

from loky import get_reusable_executor

from async_node._dynamic_io_executor import IOScheduler


class Schedulers:
    """
    A centralized factory and manager for different types of thread executors.

    This class provides singleton-style access to three types of executors:
    1. IO-bound executor (`io`) using a dynamic IOScheduler.
    2. CPU-bound executor (`computation`) using a reusable ThreadPoolExecutor 
       with a worker count equal to the number of CPU cores.
    3. Single-thread executor (`single`) using a ThreadPoolExecutor with a single worker.

    Executors are lazily instantiated when first accessed and reused thereafter.
    The class also allows configuring the worker timeout for IO and computation executors
    before they are created. In this case set timeout for workers.
    """

    _io_executor: Optional[Executor] = None
    _computation_executor: Optional[Executor] = None
    _single_executor: Optional[Executor] = None
    _executor_workers_timeout: int = 20

    @classmethod
    def set_workers_timeout(cls, timeout: int):
        """
        Set the timeout (in seconds) for idle workers of IO and computation executors.

        This method must be called before any executor is created. Once an executor
        has been instantiated, changing the timeout is not allowed.

        Args:
            timeout (int): Timeout in seconds for idle worker threads.

        Raises:
            RuntimeError: If any executor has already been instantiated.
        """
        if any(executor for executor in [cls._io_executor, cls._computation_executor, cls._single_executor]):
            raise RuntimeError("Cannot change timeout after creation")
        else:
            cls._executor_workers_timeout = timeout

    @staticmethod
    def io() -> Executor:
        """
        Return the singleton IO-bound executor.

        Uses the custom IOScheduler with a timeout defined by `_executor_workers_timeout`.
        If the executor has not been created yet, it is instantiated lazily.

        Returns:
            Executor: Singleton IO-bound executor instance.
        """
        if Schedulers._io_executor is None:
            Schedulers._io_executor = IOScheduler(timeout=Schedulers._executor_workers_timeout)
        return Schedulers._io_executor

    @staticmethod
    def computation() -> Executor:
        """
        Return the singleton CPU-bound executor.

        Uses a reusable ThreadPoolExecutor with `max_workers` equal to the number
        of CPU cores (or 1 if unable to determine). The executor is lazily instantiated.

        Returns:
            Executor: Singleton CPU-bound executor instance.
        """
        if Schedulers._computation_executor is None:
            Schedulers._computation_executor = get_reusable_executor(
                max_workers=os.cpu_count() or 1,
                timeout=Schedulers._executor_workers_timeout
            )
        return Schedulers._computation_executor

    @staticmethod
    def single() -> Executor:
        """
        Return the singleton single-thread executor.

        Uses a ThreadPoolExecutor with a single worker thread. The executor
        is lazily instantiated on first access.

        Returns:
            Executor: Singleton single-thread executor instance.
        """
        if Schedulers._single_executor is None:
            Schedulers._single_executor = ThreadPoolExecutor(max_workers=1)
        return Schedulers._single_executor
