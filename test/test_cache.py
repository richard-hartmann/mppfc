import inspect

import mppfc
import pathlib
import pytest
import shutil
import time

import logging
import sys

sh = logging.StreamHandler(sys.stdout)
sh.setLevel("WARNING")
mppfc.cache.log.addHandler(sh)

path_for_cache = pathlib.Path(".CacheInit/test.test_cache").absolute()
non_default_path_for_cache = pathlib.Path(".myCache/test.test_cache").absolute()


class SomeClass(mppfc.CacheInit):
    """instantiation is being cached simply by subclassing from `CacheInit`"""

    def __init__(self, a, t=1):
        time.sleep(t)
        self.a = a


class A:
    """
    This base class set the class member 'my_msg' when being subclassed.

    So the test is to create a class B which inherits from A, but also from CacheInit
    in order to cache the instantiation of B while correctly doing the subclassing of A.
    """

    my_msg = None

    def __init__(self, a):
        self.a = a

    def __init_subclass__(cls, **kwargs):
        cls.my_msg = kwargs["msg"]

    def __str__(self):
        return f"A: a={self.a}"


class B(mppfc.CacheInit, A, msg="hi"):
    """
    Instantiation is being cached, and the class member 'my_msg'
    of class A is set to 'hi'.
    """

    def __init__(self, b, **kwargs):
        super(B, self).__init__(**kwargs)
        self.b = b

    def __str__(self):
        return f"B: a={self.a}, b={self.b}"


def test_cache_class_init():
    """
    check caching of a simple class, verify caching by timing
    """
    shutil.rmtree(path_for_cache / "SomeClass", ignore_errors=True)

    t0 = time.perf_counter_ns()
    sc = SomeClass(a=1, t=1)
    t1 = time.perf_counter_ns()
    assert (t1 - t0) / 10**9 > 1
    assert isinstance(sc, SomeClass)
    assert sc.a == 1
    assert sc.__init__.__name__ == "_cached_init"
    assert sc.__init_subclass__.__class__.__name__ == "DenyFurtherSubclassing"
    assert sc.loaded_from_cache is False

    t0 = time.perf_counter_ns()
    sc = SomeClass(a=1, t=1)
    t1 = time.perf_counter_ns()
    assert (t1 - t0) / 10**9 < 1
    assert isinstance(sc, SomeClass)
    assert sc.a == 1
    assert sc.__init__.__name__ == "_cached_init"
    assert sc.__init_subclass__.__class__.__name__ == "DenyFurtherSubclassing"
    assert sc.loaded_from_cache is True

    t0 = time.perf_counter_ns()
    sc = SomeClass(1, t=1)
    t1 = time.perf_counter_ns()
    assert isinstance(sc, SomeClass)
    assert sc.a == 1
    assert sc.__init__.__name__ == "_cached_init"
    assert sc.__init_subclass__.__class__.__name__ == "DenyFurtherSubclassing"
    assert sc.loaded_from_cache is True
    assert (t1 - t0) / 10**9 < 1


def test_cache_class_init_subclass():
    """
    test if caching subclasses works appropriately
    """
    shutil.rmtree(path_for_cache / "B", ignore_errors=True)
    b = B(a="A", b="BB")
    assert b.a == "A"
    assert b.b == "BB"
    assert b.loaded_from_cache is False
    assert b.my_msg == "hi"

    b = B(a="A", b="BB")
    assert b.a == "A"
    assert b.b == "BB"
    assert b.loaded_from_cache is True
    assert b.my_msg == "hi"


def test_subclass_a_cached_class():
    """
    we cannot subclass a cached Class
    """
    with pytest.raises(mppfc.cache.CacheInitSubclassError):

        class C(B, msg="hi"):
            pass


class S0:
    s0 = "s0"
    pass


class S1(S0):
    s1 = "s1"

    def __init__(self, s):
        self.s = s


class S1Cached(mppfc.CacheInit, S1):
    def __init__(self, s):
        super().__init__(s)


class S2(mppfc.CacheInit, S1):
    s2 = "s2"

    def __init__(self, s):
        super().__init__(s)


def test_inheritance():
    shutil.rmtree(path_for_cache / "S1Cached", ignore_errors=True)

    s1 = S1Cached(1)
    assert s1.loaded_from_cache is False
    assert s1.s0 == "s0"
    assert s1.s1 == "s1"
    assert s1.s == 1

    s1 = S1Cached(1)
    assert s1.loaded_from_cache is True
    assert s1.s0 == "s0"
    assert s1.s1 == "s1"
    assert s1.s == 1

    shutil.rmtree(path_for_cache / "S1Cached", ignore_errors=True)

    shutil.rmtree(path_for_cache / "S2", ignore_errors=True)
    s2 = S2(2)
    assert s2.loaded_from_cache is False
    assert s2.s0 == "s0"
    assert s2.s1 == "s1"
    assert s2.s2 == "s2"
    assert s2.s == 2

    s2 = S2(2)
    assert s2.loaded_from_cache is True
    assert s2.s0 == "s0"
    assert s2.s1 == "s1"
    assert s2.s2 == "s2"
    assert s2.s == 2

    # assure that class S1 is not being cached!
    for f in path_for_cache.iterdir():
        if "S1" in str(f):
            assert False


def test_CacheInit_args():
    shutil.rmtree(path_for_cache / "SomeClass", ignore_errors=True)
    shutil.rmtree(non_default_path_for_cache / "SomeClass", ignore_errors=True)

    # default case
    c = SomeClass(a=1, t=0)
    assert c.loaded_from_cache is False
    c = SomeClass(a=1, t=0)
    assert c.loaded_from_cache is True

    # use non default path to store the cache data
    c = SomeClass(a=1, t=0, _CacheInit_path=".myCache")
    assert c.loaded_from_cache is False
    c = SomeClass(a=1, t=0, _CacheInit_path=".myCache")
    assert c.loaded_from_cache is True

    shutil.rmtree(".myCache/SomeClass", ignore_errors=True)
    # do not use the module name to construct the path in which the
    # cache data is stored
    c = SomeClass(
        a=1, t=0, _CacheInit_path=".myCache", _CacheInit_include_module_name=False
    )
    assert c.loaded_from_cache is False
    c = SomeClass(
        a=1, t=0, _CacheInit_path=".myCache", _CacheInit_include_module_name=False
    )
    assert c.loaded_from_cache is True
    shutil.rmtree(".myCache/SomeClass", ignore_errors=True)
    c = SomeClass(
        a=1, t=0, _CacheInit_path=".myCache", _CacheInit_include_module_name=False
    )
    assert c.loaded_from_cache is False

    # use pickle.dump as serializer
    c = SomeClass(a=1, t=0, _CacheInit_serializer=mppfc.cache.pickle_serializer)
    assert c.loaded_from_cache is False
    c = SomeClass(a=1, t=0, _CacheInit_serializer=mppfc.cache.pickle_serializer)
    assert c.loaded_from_cache is True


def test_manipulate_func_siganture():
    def my_func(a, c, b=2):
        pass

    s = inspect.signature(my_func)
    s.bind(a=1, c=2, b=3)

    s2 = mppfc.cache._remove_params_from_signature(s, "a")
    with pytest.raises(TypeError):
        s2.bind(a=1, c=2, b=3)
    s2.bind(c=2, b=3)

    s3 = mppfc.cache._remove_params_from_signature(s, ("a", "b"))
    with pytest.raises(TypeError):
        s3.bind(a=1, c=2, b=3)
    s3.bind(c=2)


def test_cache_bounded_method():

    with pytest.raises(TypeError):

        class TestClass:
            @mppfc.cache.CacheFileBasedDec()
            def get(self, a):
                return a * self.x


class X:
    class Z(mppfc.CacheInit):
        def __init__(self, z):
            self.z = z

    def __init__(self, x, z):
        self.x = x
        self.myZ = X.Z(z)


class XX:
    class Y:
        class Z(mppfc.CacheInit):
            def __init__(self, z):
                self.z = z


def test_nested_classes():
    shutil.rmtree(path_for_cache / "X.Z", ignore_errors=True)
    shutil.rmtree(path_for_cache / "XX.Y.Z", ignore_errors=True)

    z = X.Z(z=4)
    assert str(z.path_for_cache).endswith("X.Z")
    assert z.loaded_from_cache is False
    z = X.Z(z=4)
    assert z.loaded_from_cache is True

    z2 = XX.Y.Z(z=4)
    assert str(z2.path_for_cache).endswith("XX.Y.Z")
    assert z2.loaded_from_cache is False


if __name__ == "__main__":
    path_for_cache = pathlib.Path(".CacheInit/__main__/")
    non_default_path_for_cache = pathlib.Path(".myCache/__main__").absolute()

    # test_cache_class_init()
    # test_cache_class_init_subclass()
    # test_subclass_a_cached_class()
    # test_inheritance()
    # test_CacheInit_args()
    # sh.setLevel("DEBUG")
    # test_manipulate_func_siganture()
    # test_cache_bounded_method()
    test_nested_classes()
