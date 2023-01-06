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
print('slow_function(x={}) = {}'.formart(x, y))
y = slow_function2(x=x, _cache_flag='has_key')
print('x={} is in cache: {}'.formart(x, y))

# now turn on multiprocessing
# num_proc controls the number of client processes. This parameter can be
#       a) positive int: explicit number of processes, must not exceed the number of available cores
#       b) negative int or zero: number of available cores - abs(num_proc) (leaves abs(num_proc) cores unused
#       c) float in the interval (0,1]: percentage of available cores.
#       d) string 'all': as many clients processes as core available
slow_function.start_mp(num_proc=2)

# with multiprocessing
for x in [1, 2, 3, 5]:
    y = slow_function(x)
    print("x={}, y={}".format(x, y))
slow_function.wait()
