"""
mppfc - multi-processing persistent function cache

see README.md for details

MIT licence
-----------

Copyright (c) 2023 Richard Hartmann

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
"""

# python imports
import inspect
import multiprocessing as mp
import queue
import signal
import threading
import time
import traceback
from typing import Any, Callable, Union
from pathlib import Path
import warnings

# third party imports
import binfootprint as bf

# mppfc module imports
from .cache import CacheFileBased


def parse_num_proc(num_proc: Union[int, float, str]) -> int:
    """
    Parse `num_proc` to positive non-zero integer.

    Parameters:
        num_proc: can be
            a) positive int: explicit number of processes, must not exceed the number of available cores
            b) negative int or zero: number of available cores - abs(num_proc) (leaves abs(num_proc) cores unused
            c) float in the interval (0,1]: percentage of available cores.
            d) string 'all': as many clients processes as core available
    """
    n_cpu = mp.cpu_count()
    if num_proc == "all":
        return n_cpu
    if isinstance(num_proc, float):
        if (num_proc > 0) and (num_proc <= 1):
            return int(num_proc * n_cpu)
        else:
            raise ValueError("num_proc ({}, float) out of range (0,1]".format(num_proc))
    if isinstance(num_proc, int):
        if num_proc > 0:
            if num_proc <= n_cpu:
                return num_proc
            else:
                raise ValueError(
                    (
                        "num_proc ({}, positive integer) must not be larger"
                        + "than the number of available cores ({})"
                    ).format(num_proc, n_cpu)
                )
        else:  # num_proc <= 0
            if num_proc > -n_cpu:
                return n_cpu + num_proc
            else:
                raise ValueError(
                    (
                        "num_proc ({}, negative integer) must not be smaller"
                        + "than *minus* the number of available cores ({})"
                    ).format(num_proc, n_cpu)
                )
    else:
        raise ValueError("num_proc ({}) is of invalid type".format(num_proc))


class ErroneousFunctionCall(Exception):
    """
    This class is used as result if the function call failed.
    The member 'exc' holds the exception causing the failure.
    """

    def __init__(self, ex: Exception, tb: str):
        super().__init__()
        self.ex = ex
        self.tb = tb

    def __str__(self):
        return "original Exception from subprocess ({}: {})\n{}".format(
            self.ex.__class__.__name__, self.ex, self.tb
        )


class MultiProcCachedFunction:
    """
    A wrapper which enables **parallel function evaluation** on multiple cores and **persistent caching** for the results

    Given an arbitrary function `f(*args, **kwargs)`, this class provides a wrapper function
    `F = MultiProcCachedFunction(f)` which
        - implements persistent caching of `F(*args, **kwargs)`
        - and enables parallel evaluation by calling `F.start_mp()`
    Note that the caching requires that the arguments (args and kwargs) can be serialized by `dump` from the
    `binfootprint` module.

    Example:

        >>> @MultiProcCachedFunctionDec()
        >>> def f(x):
        >>>     return something

        `f` is now an instance of the wrapper MultiProcCachedFunction.
        This means, that the results of `f` are being cached on disk.

        >>> f.start_mp(num_proc)

        starts num_proc subprocesses which wait for not-yet-cached arguments.

        >>> for xi in many_x:
        >>>     y = f(xi)

        Instead of crunching all xi sequentially (continue after f(xi) has returned), f(xi) returns as soon as
        xi has been put to the queue, from which the subprocesses fetch their arguments.
        Note that the for loop has the same syntax as for the original function `f`.
        However, y will be None if the result is not ready yet.
        It is safe to call f(xi) for the same xi more than once. It will be queued only once, or not at all, if it
        was found in the cache.
        So in an interactive shell, we can simply call the for loop again, to fetch the current status (see
        `examples/live_update_example.ipynb` for a demonstration).

        In a non-interactive script we can wait untill all arguments have been progressed by calling

        >>> f.wait(status_interval_in_sec)

        If `status_interval_in_sec` is not None, a status message will be printed repeatedly after
        `status_interval_in_sec` seconds.
        `wait` also stops the subprocesses (see also `join`) once all arguments have been processed.
        Therefore, the main script will terminate if it reaches its end.

        If `wait` was not called, the main script does **not** terminate if it reaches its end, because the subprocesses
        spawned by `start_mp` are still running.
        In order to prevent that a function decoration only causes the script to not terminating, `start_mp` is not
        called automatically on init.

        We rather expect, that typing `start_mp` will remind you to also type `wait` (or `join` or `terminate`).

        Once `wait` has returned, all arguments have been cached, and we can call

        >>> for xi in many_x:
        >>>     y = f(xi)

        again, where for each `xi` the result `f(xi)` is loaded from the cache.
    """

    def __init__(
        self,
        function: Callable[..., Any],
        path: Union[Path, str] = ".cache",
        include_module_name: bool = True,
    ):
        """
        Initialize the `MultiProcCachedFunction` wrapper class with

        Parameters:
            function: the function to be wrapped
            path: where to store the data of the cache (overall location, each function becomes its own subdirectory)
            include_module_name: whether the name of the subdirectory should include
                                 the name of the module defining the function
                                 (see `cache/CacheFileBased` for further details)

        """
        self.num_proc = 0
        self.fnc = function
        self.sig = inspect.signature(function)
        self.cached_fnc = CacheFileBased(
            fnc=function, path=path, include_module_name=include_module_name
        )
        self._mp = False

        # the manager provides proxi access to python objects
        self.m = mp.Manager()

        # contains the args to be evaluated and their hash value
        self.kwargs_q = self.m.Queue()

        # Any arg that has been put to the Queue, ist hash is also added to that dict (should be a set),
        # so we can keep track of what has been put to the Queue. The values of the dict are irrelevant.
        # When an item has been processed (successfully crunched and cached to disk or failed) it is removed from
        # that dict.
        self.kwargs_hash_set = self.m.dict()

        # save exception and traceback, so it can be raised in the main process
        self.erroneous_call_dict = self.m.dict()

        self.total_cpu_time = mp.Value("d", 0.0)
        self.stop_event = self.m.Event()

        self.kwargs_cnt = 0
        self.procs = []
        self._all_done = False

    @property
    def number_tasks_waiting(self) -> int:
        """
        Returns:
            The number of tasks/arguments still waiting to be fetched by the subprocesses.
        """
        return self.kwargs_q.qsize()

    @property
    def number_tasks_issued_in_total(self) -> int:
        """
        Returns:
            The total number of tasks/arguments requested to be evaluated.
            This number increases by one if the function is called for an arguments which has not been
            cached yet.
            When calling `start_mp` this number is set to 'number_tasks_waiting'.
        """
        return self.kwargs_cnt

    @property
    def number_tasks_not_done(self) -> int:
        """
        Returns:
            The number of tasks/arguments which have not been processed.
            Note that if processing an arguments raises an exception, this argument is marked as done, but it is not
            added to the cache.
        """
        return len(self.kwargs_hash_set)

    @property
    def number_tasks_in_progress(self) -> int:
        """
        Returns:
            The number of tasks/arguemnts which are currently progressed.
            If the queue is not empty, this should correspond to the number of subprocesses spawned-.
        """
        return (
            self.number_tasks_issued_in_total
            - self.number_tasks_waiting
            - self.number_tasks_done
        )

    @property
    def number_tasks_done(self) -> int:
        """
        Returns:
            The number of tasks/arguments which have been processed.
            Note that if processing an arguments raises an exception, this argument is marked as done, but it is not
            added to the cache.
        """
        return self.number_tasks_issued_in_total - self.number_tasks_not_done

    @property
    def number_tasks_failed(self) -> int:
        """
        Returns:
            The number of tasks/arguments which have raised an exception while been processed.
        """
        return len(self.erroneous_call_dict)

    @property
    def cache_dir(self) -> str:
        """
        directory which contains the cache data

        If `include_module_name` is True, then `path / module_name.function_name`,
        otherwise `path / function_name`.
        """
        return self.cached_fnc.cache_dir

    @property
    def average_time_per_function_call(self) -> Union[None, float]:
        """
        Return the average time it took to evaluate the function.
        If results are taken from cache, they are not included.
        In case no items have been processed (function was never called), return None
        """
        if self.number_tasks_done == 0:
            return None

        return self.total_cpu_time.value / self.number_tasks_done

    @property
    def mp_enabled(self) -> bool:
        """
        Return True if multiprocessing is currently enabled (`start_mp` was called),
        otherwise False (`wait`, `join`, `terminate` and `kill` stops multiprocessing)
        """
        return self._mp

    def start_mp(self, num_proc: Union[int, float, str] = "all") -> bool:
        """
        Spawns the client processes. Return True on success.

        Note that, if you use multiprocessing (you have called 'start_mp'), you need to join / terminate the
        subprocesses (call `wait`, `join` or `terminate`) in order to allow the main process to exit.

        If there are still some subprocesses running (from a previous call of start_mp) no
        further processes will be spawned. In that case, return False.

        Parameters:
            num_proc:
                control the number of client processes. This parameter can be
                a) positive int: explicit number of processes, must not exceed the number of available cores
                b) negative int or zero: number of available cores - abs(num_proc) (leaves abs(num_proc) cores unused
                c) float in the interval (0,1]: percentage of available cores.
                d) string 'all': as many clients processes as core available
        """

        if len(self.procs) != 0:
            warnings.warn(
                "Cannot start multiprocessing! Some subprocesses are still running."
            )
            return False

        self._mp = True
        self.num_proc = parse_num_proc(num_proc)
        self.kwargs_cnt = self.kwargs_q.qsize()
        self.total_cpu_time.value = 0

        self.stop_event.clear()
        for i in range(self.num_proc):
            p = mp.Process(
                target=self._runner,
                args=(
                    self.cached_fnc,
                    self.kwargs_q,
                    self.kwargs_hash_set,
                    self.erroneous_call_dict,
                    self.stop_event,
                    self.total_cpu_time,
                ),
            )
            p.start()
            self.procs.append(p)
        return True

    def __call__(self, *args: Any, **kwargs: Any) -> Union[Any, None]:
        """
        The wrapped call of the original function.

        In case multiprocessing is not active (`start_mp` was not called, or it was stopped via `wait`, `join` or
        `terminate`) this simply reduces to the cache wrapper CacheFileBased (see `cache` submodule).

        If multiprocessing is active, calling this wrapper returns the cached results, if present.
        Otherwise, it put the argument to a queue and returns None. A spawned subprocess fetches that argument
        from the queue, processes it and saves it to the cache.

        Calling the function with the same argument again, returns the cached result.
        If the result is not yet in the cache, returns None, but does not queue the argument again.
        So it is perfectly fine to call the wrapper with the same arguments several times.

        Not that in case of multiprocessing being active, the cache wrapper extra kwarg `_cache_flag`
        is not available. Using that keyword argument raises a ValueError.
        """

        # fallback if multiprocessing has not been started yet
        if self._mp is False:
            return self.cached_fnc(*args, **kwargs)

        if "_cache_flag" in kwargs:
            self.terminate()
            raise ValueError(
                "You cannot use the '_cache_flag' kwarg if in multiprocessing mode"
            )

        # see if we can find the result in the cache
        try:
            return self.cached_fnc(*args, **kwargs, _cache_flag="cache_only")
        except KeyError:
            pass

        ba = self.sig.bind(*args, **kwargs)
        ba.apply_defaults()
        sorted_arguments = tuple(sorted(ba.arguments.items(), key=lambda item: item[0]))
        arg_hash = bf.hash_hex_from_object(sorted_arguments)

        if arg_hash in self.erroneous_call_dict:
            ex, tb = self.erroneous_call_dict[arg_hash]
            raise ErroneousFunctionCall(ex, tb)

        # arg has already been put to the queue
        if arg_hash in self.kwargs_hash_set:
            return None
        # arg has not been put to the queue
        else:
            self.kwargs_q.put((ba.arguments, arg_hash))
            self.kwargs_cnt += 1
            self.kwargs_hash_set[arg_hash] = False
        return None

    @staticmethod
    def _runner(
        cached_fnc: CacheFileBased,
        kwargs_q: queue,
        kwargs_hash_set: dict,
        erroneous_call_dict: dict,
        stop_event: threading.Event,
        total_cpu_time: mp.Value,
    ) -> None:
        """
        The function to be run by multiple subprocesses

        Args:
            cached_fnc: cache wrapper of the original function
            kwargs_q:
                Shared joinable queue from which to get the pair (kwargs, arg_hash).
                Kwargs is passed to cached_fnc, arg_hash is used to uniquely identify the kwargs.
            kwargs_hash_set:
                Shared dictionary to mark successfully processed arguments by deleting arg_hash.
            erroneous_call_dict:
                In case an error occurs while processing an argument, the exception and traceback are
                stored as value in that dict with the argument hash as key.
            stop_event:
                A shared Event which, when set, signals that no more arguments should be fetched from the queue.
            total_cpu_time:
                A shared float Value to accumulate the CPU time used to process the arguments.
        """

        def sigterm_to_interrupted_error(*args):
            raise InterruptedError("received SIGTERM")

        signal.signal(signal.SIGTERM, sigterm_to_interrupted_error)

        while not stop_event.is_set():
            # wait until an item is available
            try:
                kwargs, arg_hash = kwargs_q.get(block=True, timeout=0.3)
            except queue.Empty:
                continue

            t0 = time.perf_counter_ns()
            try:
                cached_fnc(**kwargs)
            except InterruptedError:
                pass
            except Exception as e:
                erroneous_call_dict[arg_hash] = (e, traceback.format_exc())
            finally:
                del kwargs_hash_set[arg_hash]
                t1 = time.perf_counter_ns()
                with total_cpu_time.get_lock():
                    total_cpu_time.value += (t1 - t0) / 10**9
                kwargs_q.task_done()

    def wait(self, status_interval_in_sec: Union[float, None] = None) -> None:
        """
        Wait until all tasks have been processed.
        When `wait` returns, multiprocessing hase become inactive.

        If status_interval_in_sec is not None, show status with given time interval.
        """
        if status_interval_in_sec is not None:
            while True:
                time.sleep(status_interval_in_sec)
                s = self.status(return_str=True)
                print("\r{}".format(s), end="", flush=True)
                if self.number_tasks_not_done == 0:
                    print()
                    break
        else:
            self.kwargs_q.join()
        # continues when all subprocesses have finished
        self.join()

    def _is_alive(self, p, timeout):
        p.join(timeout=timeout)
        if p.is_alive():
            self._all_done = False

    def join(self, timeout: Union[float, None] = None) -> bool:
        """
        Tell the client processes to **not** fetch a new argument (set stop_event).
        So the process will terminate once the (current) function evaluation has finished.
        When `join` returns, multiprocessing hase become inactive.

        If timeout is 'None' this function will return `True` once all client processes have finished.
        Otherwise, wait at most timeout seconds. If the clients have not finished within that
        time interval, return `False`.

        Args:
            timeout: time in seconds, gives each process timeout/num_proc seconds to join

        Returns:
            `True` if all processes have finished, `False` otherwise.
        """
        self.stop_event.set()
        self._mp = False
        self._all_done = True
        thread_list = []
        for p in self.procs:
            t = threading.Thread(target=self._is_alive, args=(p, timeout))
            t.start()
            thread_list.append(t)

        for is_alive_thread in thread_list:
            is_alive_thread.join()

        if self._all_done:
            self.procs.clear()
        return self._all_done

    def terminate(self, timeout: Union[float, None] = None) -> bool:
        """
        Trigger terminate() on each client process, than wait for them to join.
        When `terminate` returns, multiprocessing hase become inactive.

        Similar to 'join()' but with additional process.terminate() called before.

        Args:
            timeout: time in seconds for each process to join

        Returns:
            `True` if all processes have finished, `False` otherwise.
        """
        self.stop_event.set()
        self._mp = False
        for p in self.procs:
            p.terminate()

        return self.join(timeout)

    def status(self, return_str: bool = False) -> Union[None, str]:
        """
        Print multiprocessing status information.

        This includes information on the number of tasks (in progress, remaining, finished, failed and total),
        and on the time (average time per task, an estimate on the remaining time to process all tasks).

        Args:
            return_str: if `True`, do not print the status, but return the status as string

        Returns:
            None or the status information as string
        """
        l_tot = len(str(self.number_tasks_issued_in_total))
        l_num_proc = len(str(self.num_proc))
        s = "TASKS in prog:{5:>{4}} rem:{1:>{0}}, fin:{2:>{0}}, fail:{6:>{0}}, tot:{3:} ".format(
            l_tot,
            self.number_tasks_not_done,
            self.number_tasks_done,
            self.number_tasks_issued_in_total,
            l_num_proc,
            self.number_tasks_in_progress,
            self.number_tasks_failed,
        )
        if self.number_tasks_done > 0:
            avrg_cpu_time = self.total_cpu_time.value / self.number_tasks_done
            time_to_go = avrg_cpu_time * self.number_tasks_not_done / self.num_proc
            hour = int(time_to_go // 3600)
            mnt = int((time_to_go - 3600 * hour) // 60)
            sec = int(time_to_go - 3600 * hour - 60 * mnt)
            s += "TIME avrg. per task: {:.2e}s, remaining: {}h:{:0>2}m:{:0>2}s".format(
                avrg_cpu_time, hour, mnt, sec
            )
        else:
            s += "TIME ???"

        if return_str:
            return s

        print(s)


class MultiProcCachedFunctionDec:
    """
    The MultiProcCachedFunctionDec class works as decorator for caching and parallel evaluation of function calls.

    Calling an instance of MultiProcCachedFunctionDec (decorator) with an arbitrary functions as argument
    returns an instance of MultiProcCachedFunction (wrapper function, callable class) which uses
        - CacheFileBased from the cache module for caching
        - and provides a nearly drop-in mechanism to realize parallel evaluation.
    """

    def __init__(
        self, path: Union[Path, str] = ".cache", include_module_name: bool = True
    ):
        """
        The parameters `path` and `include_module_name` are passed to the init of MultiProcCachedFunction,
        which is returned by this decorator.

        Parameters:
            path: where to store the data of the cache (overall location, each function becomes its own subdirectory)
            include_module_name: whether the name of the subdirectory should include
                                 the name of the module defining the function
                                 (see `cache/CacheFileBased` for further details)
        """
        self.path = path
        self.include_module_name = include_module_name

    def __call__(self, function: Callable[..., Any]) -> MultiProcCachedFunction:
        """
        return the multiprocessing cache wrapper for `function` (an instance of MultiProcCachedFunction)

        Parameters:
            function: function to be wrapped
        """
        return MultiProcCachedFunction(
            function=function,
            path=self.path,
            include_module_name=self.include_module_name,
        )
