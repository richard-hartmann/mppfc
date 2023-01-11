import mppfc
import math

@mppfc.MultiProcCachedFunctionDec()
def pitfall_1(x):
    return math.sqrt(x)


x = 1
print("pitfall_1(x={}) = {}".format(x, pitfall_1(x=x)))
x = 1.0
print("BUT, x={} in cache: {}".format(x, pitfall_1(x=x, _cache_flag="has_key")))
print("and obviously: pitfall_1(x={}) = {}".format(x, pitfall_1(x=x, _cache_flag="no_cache")))


@mppfc.MultiProcCachedFunctionDec()
def pitfall_2(arr):
    return sum(arr)

arr = [1, 2, 3]
print("pitfall_2(arr={}) = {}".format(arr, pitfall_2(arr=arr)))
arr = (1, 2, 3)
print("BUT, arr={} in cache: {}".format(arr, pitfall_2(arr=arr, _cache_flag="has_key")))
print("and obviously: pitfall_1(arr={}) = {}".format(arr, pitfall_2(arr=arr, _cache_flag="no_cache")))
