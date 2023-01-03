"""
mppfc - multi-processing persistent function cache

MIT licence
-----------

Copyright (c) 2022 Richard Hartmann

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

import multiprocessing as mp
import queue
import inspect
import binfootprint as bf
from .cache import CacheFileBased
import time
import warnings


def parse_num_proc(num_proc):
    """
    Parse `num_proc` to positive non-zero integer.

    The parameter `num_proc` can be
        a) positive int: explicit number of processes, must not exceed the number of available cores
        b) negative int or zero: number of available cores - abs(num_proc) (leaves abs(num_proc) cores unused
        c) float in the interval (0,1]: percentage of available cores.
        d) string 'all': as many clients processes as core available
    """
    n_cpu = mp.cpu_count()
    if num_proc == 'all':
        return n_cpu
    if isinstance(num_proc, float):
        if (num_proc > 0) and (num_proc <= 1):
            return int(num_proc*n_cpu)
        else:
            raise ValueError("num_proc ({}, float) out of range (0,1]".format(num_proc))
    if isinstance(num_proc, int):
        if (num_proc > 0):
            if (num_proc <= n_cpu):
                return num_proc
            else:
                raise ValueError(
                    ("num_proc ({}, positive integer) must not be larger"+
                     "than the number of available cores ({})").format(
                        num_proc,
                        n_cpu
                    ))
        else:  # num_proc <= 0
            if (num_proc > -n_cpu):
                return n_cpu+num_proc
            else:
                raise ValueError(
                    ("num_proc ({}, negative integer) must not be smaller"+
                     "than *minus* the number of available cores ({})").format(
                        num_proc,
                        n_cpu
                    ))
    else:
        raise ValueError("num_proc ({}) is of invalid type".format(num_proc))


class ErroneousFunctionCall:
    """
    This class is used as result if the function call failed.
    The member 'exc' holds the exception causing the failure.
    """
    def __init__(self, exception):
        self.exc = exception


class MultiProcCachedFunctionDec:
    """
    The MultiProcCachedFunctionDec class works as function decorator for caching and parallel evaluation.

    Calling an instance of MultiProcCachedFunctionDec with an arbitrary functions as arguments
    returns an instance of MultiProcCachedFunction which uses
        - CacheFileBased from the cache module for caching
        - and provides a nearly drop-in mechanism to realize parallel evaluation.

    Example
    -------
    """
    def __init__(self, path='.cache', include_module_name=True):
        """

        The init of the decorator class allows to specify 'path' and 'include_module_name' which are
        passed to the
        """
        self.path = path
        self.include_module_name = include_module_name

    def __call__(self, function):
        return MultiProcCachedFunction(
            function=function,
            path=self.path,
            include_module_name=self.include_module_name
        )


class MultiProcCachedFunction:
    """
    Parallel function evaluation on multiple cores and persistent caching of the results

    Exmaple:
    --------
    """
    def __init__(self, function, path='.cache', include_module_name=True):
        """
        The results of 'function' are cached using the CacheFileBased class.

        When calling 'start_mp' python's multiprocessing is used to evaluate the function calls using
        multiple subprocesses.
        Note that this requires to call 'join' or 'terminate' at some point in order to allow the main process to exit.

        :param function: any function with *args and **kwargs
        """
        self.num_proc = 0
        self.fnc = function
        self.sig = inspect.signature(function)
        self.cached_fnc = CacheFileBased(fnc=function, path=path, include_module_name=include_module_name)
        self._mp = False

        # contains the args to be evaluated and their hash value
        self.kwargs_q = mp.JoinableQueue()
        # the manager provides a shared list (proxied list)
        self.m = mp.Manager()
        # any arg that has been put to the Queue, is also added to that dict, so we can keep track of what has
        # been put to the Queue. The values of the dict are
        #     False: item is in Queue, but has not finished
        #     instance of ErroneousFunctionCall: if the function call failed with some error.
        # When an item has been processed successfully (crunched and cached to disk) it is removed from that dict.
        self.kwargs_for_progress = self.m.dict()
        self.kwargs_cnt = 0
        self.total_CPU_time = mp.Value('d', 0.0)
        self.stop_event = mp.Event()
        self.procs = []

    def start_mp(self, num_proc='all'):
        """
        Start the client processes. Return True on success.

        Note that, if you use multiprocessing (you have called 'start_mp'), you need to join / terminate the
        subprocesses (call 'join' or 'terminate') in order to allow the main process to exit.

        If there are still some subprocesses running (from a previous call of start_mp) no
        further processes will be spawned. In that case return False.

        :param num_proc: the number of client processes, can be
            a) positive int: explicit number of processes, must not exceed the number of available cores
            b) negative int or zero: number of available cores - abs(num_proc) (leaves abs(num_proc) cores unused
            c) float in the interval (0,1]: percentage of available cores.
            d) string 'all': as many clients processes as core available
        """

        if len(self.procs) != 0:
            warnings.warn("Cannot start multiprocessing! Some subprocesses are still running.")
            return False

        self._mp = True
        self.num_proc = parse_num_proc(num_proc)

        self.stop_event.clear()
        for i in range(self.num_proc):
            p = mp.Process(
                target=self._runner,
                args=(self.cached_fnc, self.kwargs_q, self.kwargs_for_progress, self.stop_event, self.total_CPU_time)
            )
            p.start()
            self.procs.append(p)
        return True

    def __call__(self, *args, **kwargs):
        # fallback if multiprocessing has not been started yet
        if self._mp is False:
            return self.cached_fnc(*args, **kwargs)

        # see if we can find the result in the cache
        try:
            return self.cached_fnc(*args, **kwargs, _cache_flag='cache_only')
        except KeyError:
            pass

        ba = self.sig.bind(*args, **kwargs)
        ba.apply_defaults()
        sorted_arguments = tuple(sorted(ba.arguments.items(), key=lambda item: item[0]))
        arg_hash = bf.hash_hex_from_object(sorted_arguments)

        # arg has already been put to the queue
        if arg_hash in self.kwargs_for_progress:
            # ... function call was erroneous
            if isinstance(self.kwargs_for_progress[arg_hash], ErroneousFunctionCall):
                self.terminate()
                raise self.kwargs_for_progress[arg_hash]

            return None
        # arg has not been put to the queue
        else:
            self.kwargs_q.put((ba.arguments, arg_hash))
            self.kwargs_cnt += 1
            self.kwargs_for_progress[arg_hash] = False
        return None

    @staticmethod
    def _runner(cached_fnc, kwargs_q, kwargs_for_progress, stop_event, total_CPU_time):
        while not stop_event.is_set():
            # wait until an item is available
            try:
                kwargs, arg_hash = kwargs_q.get(block=True, timeout=0.3)
            except queue.Empty:
                continue

            try:
                t0 = time.perf_counter_ns()
                cached_fnc(**kwargs)
                t1 = time.perf_counter_ns()
                with total_CPU_time.get_lock():
                    total_CPU_time.value += (t1 - t0) / 10**9
                del kwargs_for_progress[arg_hash]
            except Exception as e:
                kwargs_for_progress[arg_hash] = ErroneousFunctionCall(e)
            finally:
                kwargs_q.task_done()

    def wait(self):
        """
        wait until all tasks have been processed
        """
        # continues when the Queue is empty
        self.kwargs_q.join()
        # continues when all subprocesses have finished
        self.join()

    def join(self, timeout=None):
        """
        Tell the client processes to NOT fetch a new argument (set stop_event).
        So the process will terminate once the (current) function evaluation has finished.

        If timeout is 'None' this function will return only when all client processes have finished.
        Otherwise, wait at most timeout seconds for each client process to finish.

        The function returns 'True' if all client processes have finished, otherwise 'False'.

        :param timeout: time in seconds for each process to join
        :return: 'True' if all processes have finished
        """
        self.stop_event.set()
        self._mp = False
        all_done = True
        for p in self.procs:
            p.join(timeout)
            if p.exitcode is None:
                all_done = False
        if all_done:
            self.procs.clear()
        return all_done

    def terminate(self, timeout=None):
        """
        Trigger terminate() on each client process, than wait for them to join.

        Similar to 'join()' but with additional process.terminate() called before.

        :param timeout: time in seconds for each process to join
        :return: 'True' if all processes have finished
        """
        self.stop_event.set()
        self._mp = False
        for p in self.procs:
            p.terminate()

        return self.join(timeout)

    def kill(self):
        """
        Trigger kill() on each client process.
        """
        self.stop_event.set()
        self._mp = False
        for p in self.procs:
            p.kill()
        self.procs.clear()

    def status(self, return_str=False):
        remaining = len(self.kwargs_for_progress)
        finished = self.kwargs_cnt - remaining
        s = "tasks remaining: {}, finished: {}, total :{}\n".format(remaining, finished, self.kwargs_cnt)
        if finished > 0:
            avrg_CPU_time = self.total_CPU_time.value / finished
            time_to_go = avrg_CPU_time * remaining / self.num_proc
            hour = int(time_to_go // 3600)
            mnt = int((time_to_go - 3600*hour) // 60)
            sec = int(time_to_go - 3600*hour - 60*mnt)
            s += "avrg. CPU time per task: {:.3e}s, est. remaining time: {}h:{}m:{}s".format(
                avrg_CPU_time, hour, mnt, sec
            )
        else:
            s += "avrg. CPU time per task: ?, est. remaining time: ?"

        if return_str:
            return s

        print(s)




