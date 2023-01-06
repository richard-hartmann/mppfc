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
