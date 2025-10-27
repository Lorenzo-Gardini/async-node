import threading
from queue import Queue, Empty
from threading import Thread, Lock
from concurrent.futures import Future, Executor
from typing import List, Callable, Any


class IOScheduler(Executor):
    """
    A lightweight, dynamic thread-based scheduler that executes submitted tasks
    in background threads and automatically scales the number of worker threads
    based on demand.

    This scheduler behaves similarly to a cached thread pool:
    - If tasks are submitted and no free worker is available, new threads are created.
    - Idle threads will shut down after a period of inactivity (`timeout` seconds).
    - Threads are daemon threads, so they will not prevent program termination.

    Attributes
    ----------
    tasks : Queue
        Queue holding pending tasks to execute. Each item is a tuple:
        (callable, args, kwargs, future)
    timeout : int
        Time in seconds a worker should wait for a new task before shutting down.
    lock : Lock
        Synchronization primitive for accessing shared thread data safely.
    threads : List[Thread]
        List of currently running worker threads.
    free_threads : int
        Number of worker threads currently idle and available to take tasks.
    """

    def __init__(self, timeout: int=20):
        """
        Initialize the scheduler.

        Parameters
        ----------
        timeout : int, default=20
            Number of seconds a worker thread stays alive without work
            before automatically terminating.
        """
        self.tasks: Queue = Queue()
        self.timeout: int = timeout
        self.lock: Lock = Lock()
        self.threads: List[Thread] = []
        self.free_threads: int = 0

    def submit(self, func: Callable[..., Any], *args, **kwargs) -> Future:
        """
        Submit a callable task for asynchronous execution.

        Parameters
        ----------
        func : Callable
            The function to execute.
        *args, **kwargs :
            Arguments for the function.

        Returns
        -------
        Future
            A Future that will hold the result of the execution.

        Notes
        -----
        If no free threads are available, a new worker thread will be created.
        """
        future: Future = Future()
        self.tasks.put((func, args, kwargs, future))
        self._ensure_threads()
        return future

    def _ensure_threads(self):
        """
        Create new worker threads if needed.

        A new thread is spawned only when the number of pending tasks exceeds
        the number of currently free (idle) threads.
        """
        with self.lock:
            needed = self.tasks.qsize() > self.free_threads
            for _ in range(needed):
                t = Thread(target=self._worker, daemon=True)
                t.start()
                self.threads.append(t)

    def _worker(self):
        """
        Worker loop that continuously processes tasks from the queue.

        The worker:
        - Fetches work from the queue.
        - Executes the task and sets the result on the associated Future.
        - Increases the idle thread count when finished.
        - Shuts down after `timeout` seconds of inactivity.
        """
        current_thread: Thread = threading.current_thread()
        while True:
            try:
                func, args, kwargs, future = self.tasks.get(timeout=self.timeout)
            except Empty:
                # No task received; thread should shut down
                with self.lock:
                    if current_thread in self.threads:
                        self.threads.remove(current_thread)
                        self.free_threads = max(0, self.free_threads - 1)
                break

            # Mark thread as busy
            with self.lock:
                if self.free_threads > 0:
                    self.free_threads -= 1

            try:
                result: Any = func(*args, **kwargs)
                future.set_result(result)
            except Exception as e:
                future.set_exception(e)
            finally:
                self.tasks.task_done()
                # Mark thread as free again
                with self.lock:
                    self.free_threads += 1
