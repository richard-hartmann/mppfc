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


class MultiProcDec:
    """
    Makes function evaluation on multiple cores available as decorator.
    The aim is to provide a simple drop-in mechanism for parallelization.
    """
    def __init__(self, num_proc='all'):
        self.num_proc = num_proc

    def __call__(self, function):
        return MultiProcFunction(
            function=function,
            num_proc=self.num_proc
        )


class MultiProcFunction:
    """
    Parallel function evaluation on multiple cores.

    Exmaple:
    --------
    """
    def __init__(self, function, num_proc='all'):
        """
        setup parallelization for calls of 'function' using 'num_proc' client processes

        :param function: any function with *args and **kwargs
        :param num_proc: the number of client processes, can be
            a) positive int: explicit number of processes, must not exceed the number of available cores
            b) negative int or zero: number of available cores - abs(num_proc) (leaves abs(num_proc) cores unused
            c) float in the interval (0,1]: percentage of available cores.
            d) string 'all': as many clients processes as core available
        """
        self.n = parse_num_proc(num_proc)
        self.fnc = function
        self.sig = inspect.signature(function)
        self._start()

    def _start(self):
        """
        Set up the shared data objects and start the client processes.
        """
        # contains the args to be evaluated and their hash value
        self.kwargs_q = mp.Queue()

        # the manager provides a shared list (proxied list)
        self.m = mp.Manager()
        # the collection of results, as dict with keys and values (arg_idx, result_for_idx)
        self.result_dict = self.m.dict()

        self.stop_event = mp.Event()
        self.procs = []
        for i in range(self.n):
            p = mp.Process(
                target=self._runner,
                args=(self.fnc, self.kwargs_q, self.result_dict, self.stop_event)
            )
            p.start()
            self.procs.append(p)

    def __call__(self, *args, **kwargs):
        ba = self.sig.bind(*args, **kwargs)
        ba.apply_defaults()
        sorted_arguments = tuple(sorted(ba.arguments.items(), key=lambda item: item[0]))
        arg_hash = bf.hash_hex_from_object(sorted_arguments)
        if arg_hash in self.result_dict:
            return self.result_dict[arg_hash]
        self.kwargs_q.put((ba.arguments, arg_hash))
        return None

    @staticmethod
    def _runner(fnc, kwargs_q, result_dict, stop_event):
        while not stop_event.is_set():
            # wait until an item is available
            try:
                kwargs, arg_hash = kwargs_q.get(block=True, timeout=1)
            except queue.Empty:
                continue

            try:
                r = fnc(**kwargs)
            except Exception as e:
                r = ErroneousFunctionCall(e)
            result_dict[arg_hash] = r

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
        for p in self.procs:
            p.terminate()

        return self.join(timeout)

    def kill(self):
        """
        Trigger kill() on each client process.
        """
        self.stop_event.set()
        for p in self.procs:
            p.kill()
        self.procs.clear()




