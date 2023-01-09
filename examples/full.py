import time
import mppfc
import shutil


# The MultiProcCachedFunctionDec changed the regular function 'slow_function2'
# to becomes an instance of the `MultiProcCachedFunction` class.
# path:
#   specify the path to store the cache (default is '.cache')
# include_module_name:
#   with the aim to have a different location for each function being cached
#   the actual cache directory is 'path / module_name.function_name'
#   where 'module_name' is the name of the module defining the function
#   and 'function_name' the name of the function.
#   if `include_module_name` is set to `False`, 'path / function_name' is used as cache directory.
@mppfc.MultiProcCachedFunctionDec(path='./path_for_cache', include_module_name=True)
def slow_function2(x):
    # complicated stuff
    time.sleep(1)
    return x


# the cache directory is returned by the property `cache_dir`
print("cache data will be at:", slow_function2.cache_dir)

# uncomment to clear the cache, be careful what you are doing
# this will remove a whole directory tree WITHOUT ASKING
# shutil.rmtree(slow_function.cache_dir)

# without starting the multiprocessing, `slow_function2` is extended by caching only.
# As of that, the new keyword argument `_cache_flag` becomes available.
# It takes the values
#       'no_cache': Simple call of `fnc` with no caching.
#       'update': Call `fnc` and update the cache with recent return value.
#       'has_key': Return `True` if the call has already been cached, otherwise `False`.
#       'cache_only': Raises a `KeyError` if the result has not been cached yet.
x = 3
y = slow_function2(x=x)
print('slow_function(x={}) = {}'.format(x, y))
y = slow_function2(x=x, _cache_flag='has_key')
print('x={} is in cache: {}'.format(x, y))

# now turn on multiprocessing
# num_proc controls the number of client processes. This parameter can be
#       a) positive int: explicit number of processes, must not exceed the number of available cores
#       b) negative int or zero: number of available cores - abs(num_proc) (leaves abs(num_proc) cores unused
#       c) float in the interval (0,1]: percentage of available cores.
#       d) string 'all': as many clients processes as core available
slow_function2.start_mp(num_proc=2)

# Simply calling the (decorated) function queue the arguments, if not already in the cache, and returns.
# The subprocesses do the actual work and cache the results to disk.
# Note that in multiprocessing mode the keyword argument `_cache_flag` must not be set.
for x in range(1, 15):
    slow_function2(x)

# Wait for all tasks to finish.
# Print status information on the progress every second.
# Wait joins all subprocesses and, thus, disables the multiprocessing mode.
slow_function2.wait(status_interval_in_sec=1)

# Now the results are quickly accessible from cache.
# The cache flag 'cache_only' is of course not necessary here and of educational purpose only.
for x in [4, 10]:
    y = slow_function2(x, _cache_flag='cache_only')
    print('slow_function(x={}) = {} (from cache)'.format(x, y))

# Start multiprocessing again
slow_function2.start_mp(num_proc=2)
# and submit more arguments to be crunched.
for x in range(20, 200):
    slow_function2(x)

# Gracefully stop the calculation after 3 seconds with `join()`.
# By calling `join(timeout)`, the subprocess are signaled to stop.
# They are allowed to finsh the current task before they return.
# That means they do not fetch a new argument.
# `join` waits at most timeout second before it returns.
# It returns True if **all** subprocesses have terminated, False otherwise.
time.sleep(3.2)
print("\ngracefully interrupt the calculation")
while True:
    print("join subprocesses, wait no more than 0.2 seconds.")
    joined = slow_function2.join(timeout=0.2)
    print("all processed joined: {}".format(joined))
    if joined:
        break
# The status shows that some arguments have not been processed.
slow_function2.status()
# These details and some more are accessible by the following properties
print("number_tasks_done", slow_function2.number_tasks_done)
print("number_tasks_waiting", slow_function2.number_tasks_waiting)
print("number_tasks_not_done", slow_function2.number_tasks_not_done)
print("number_tasks_failed", slow_function2.number_tasks_failed)
print("number_tasks_issued_in_total", slow_function2.number_tasks_issued_in_total)
print("number_tasks_in_progress", slow_function2.number_tasks_in_progress)
print("average_time_per_function_call", slow_function2.average_time_per_function_call)
print("mp_enabled", slow_function2.mp_enabled)


# resume, now with all but 2 subprocesses (note that this will fail if the system has 2 or less core only).
slow_function2.start_mp(num_proc=-2)
# Interrupt the subprocesses by calling `terminate`.
# Behaves similar to `join` but sends a SIGTERM to the subprocesses, so the current call of the original function
# is interrupted. This means that the argument will have been removed from the queue, it will also be marked as
# done but no result is written to the cache.

time.sleep(1.2)
print("\ninterrupt the calculation with SIGTERM")
while True:
    print("terminate subprocesses, wait no more than 0.2 seconds.")
    joined = slow_function2.terminate(timeout=0.2)
    print("all processed joined: {}".format(joined))
    if joined:
        break
# The status shows that some arguments have not been processed.
slow_function2.status()

