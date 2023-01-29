# mppfc - Multi-Processing Persistent Function Cache

[![PyPI version](https://badge.fury.io/py/mppfc.svg)](https://badge.fury.io/py/mppfc)

The `mppfc` module allows to speed up the evaluation of computationally 
expansive functions by 
a) processing several arguments in parallel and 
b) persistent caching of the results to disk.
Persistent caching becomes available by simply decorating a given function.
With no more than two extra lines of code, parallel evaluation is realized.

Here is a [minimal example](https://github.com/richard-hartmann/mppfc/blob/main/examples/minimal.py):

```python
import mppfc

@mppfc.MultiProcCachedFunctionDec()
def slow_function(x):
    # complicated stuff
    return x

slow_function.start_mp()
for x in some_range:
    y = slow_function(x)
slow_function.wait()
```
The first time you run this script, all `y` are `None`, since the evaluation 
is done by several background processes.
Once `wait()` returns, all parameters have been cached to disk.
So calling the script a second time yields (almost immediately) the
desired results in `y`.

Evaluating only the `for` loop in a jupyter notebook cell
will give you partial results if the background processes are still doing some work.
In that way you can already show successfully retrieved results.
(see the examples [simple.ipynb](https://github.com/richard-hartmann/mppfc/blob/main/examples/simple.ipynb) 
and [live_update.ipynb](https://github.com/richard-hartmann/mppfc/blob/main/examples/live_update.ipynb))

For a nearly exhaustive example see [full.py](https://github.com/richard-hartmann/mppfc/blob/main/examples/full.py).

### caching class instantiation

*new in Version 1.1*

When class instantiation, i.e. calling `__init__(...)` takes very long, you can cache the instantiation
by subclassing from `mppfc.CacheInit`.

```python
class SomeClass(mppfc.CacheInit):
    """instantiation is being cached simply by subclassing from `CacheInit`"""
    def __init__(self, a, t=1):
        time.sleep(t)
        self.a = a
```

Note that subclassing such a cached class is not supported.
If you try that, a `CacheInitSubclassError` is raised.
However, you can simply circumvent this problem by creating a dummy class for caching, e.g.

```python
class S0:
    s0 = 's0'

class S1(S0):
    s1 = 's1'
    def __init__(self, s):
        self.s = s

class S1Cached(mppfc.CacheInit, S1):
    """dummy 'subclass' of S1 with caching"""
    def __init__(self, s):
        super().__init__(s)

class S2(mppfc.CacheInit, S1):
    """S2 inherits from S1 AND is being cached"""
    s2 = "s2"
    def __init__(self, s):
        super().__init__(s)
```

When subclassing from `CacheInit` the following extra keyword arguments can be used
to control the Cache

* `_CacheInit_serializer`: a function which serializes an object to binary data
    (default is binfootprint.dump).
* `_CacheInit_path`: the path where to put the cache data (default is '.CacheInit')
* `_CacheInit_include_module_name`: if `True` (default) include the name of module where the class
   is defined into the path where the instances will be cached.
   (useful during development stage where Classes might be moved around or module name are still
   under debate)

### pitfalls

Note that arguments are distinguished by their binary representation obtained from the 
[binfootprint](https://github.com/richard-hartmann/binfootprint) module.
This implies that the integer `1` and the float `1.0` are treated as different arguments, even though
in many numeric situations the result does not differ.

```python
import mppfc
import math

@mppfc.MultiProcCachedFunctionDec()
def pitfall_1(x):
    return math.sqrt(x)

x = 1
print("pitfall_1(x={}) = {}".format(x, pitfall_1(x=x)))
# pitfall_1(x=1) = 1.0
x = 1.0
print("BUT, x={} in cache: {}".format(x, pitfall_1(x=x, _cache_flag="has_key")))
# BUT, x=1.0 in cache: False
print("and obviously: pitfall_1(x={}) = {}".format(x, pitfall_1(x=x, _cache_flag="no_cache")))
# and obviously: pitfall_1(x=1.0) = 1.0
```

The same holds true for lists and tuples.

```python
import mppfc
import math

@mppfc.MultiProcCachedFunctionDec()
def pitfall_2(arr):
    return sum(arr)

arr = [1, 2, 3]
print("pitfall_2(arr={}) = {}".format(arr, pitfall_2(arr=arr)))
# pitfall_2(arr=[1, 2, 3]) = 6
arr = (1, 2, 3)
print("BUT, arr={} in cache: {}".format(arr, pitfall_2(arr=arr, _cache_flag="has_key")))
# BUT, arr=(1, 2, 3) in cache: False
print("and obviously: pitfall_1(arr={}) = {}".format(arr, pitfall_2(arr=arr, _cache_flag="no_cache")))
# and obviously: pitfall_1(arr=(1, 2, 3)) = 6
```

For more details see [binfootprint's README](https://github.com/richard-hartmann/binfootprint).

## Installation

### pip

    pip install mppfc

### poetry

Using poetry allows you to include this package in your project as a dependency.

### git

check out the code from github

    git clone https://github.com/richard-hartmann/mppfc.git

## Dependencies

 - requires at least python 3.8
 - uses [`binfootprint`](https://github.com/richard-hartmann/binfootprint) 
   to serialize and hash the arguments of a function 

## Licence

### MIT licence
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
