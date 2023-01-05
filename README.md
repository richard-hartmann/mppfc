# mppfc - Multi-Processing Persistent Function Cache

The `mppfc` module allows to speed up the evaluation of computationally 
expansive functions by 
a) processing several arguments in parallel and 
b) persistent caching of the results to disk.
Persistent caching becomes available by simply decorating a given function.
With no more than two extra lines of code, parallel evaluation is realized.

Here is a minimal example:

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

Evaluating only the `for` loop in a jupyter notebook cell will give
you partial results if the background processes are still doing some work.
In that way you can already show successfully retrieved results.

**ToDo**: reference to examples and documentation 

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



