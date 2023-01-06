import time
import mppfc
import shutil


@mppfc.MultiProcCachedFunctionDec()
def slow_function(x):
    # complicated stuff
    time.sleep(1)
    return x

# uncomment to clear the cache, be careful what you are doing
# this will remove a whole directory tree WITHOUT ASKING
# shutil.rmtree(slow_function.cache_dir)


slow_function.start_mp()
for x in [1, 2, 3, 5]:
    y = slow_function(x)
    print("x={}, y={}".format(x, y))
slow_function.wait()

# We start the multiprocessing mode with `start_mp()` and simply call our function
# `slow_function` without caring about its results.
# By calling `wait()`, we wait until all parameters have been processed and cached to disk.
#

# Now we can call `slow_function` as usual and use its return value.
# Adding the `_cache_flag="cache_only"` parameter is not necessary.
# However, it emphasizes that all results are taken from the cache.
