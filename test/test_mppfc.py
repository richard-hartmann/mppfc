import pickle
import time

import mppfc
import multiprocessing as mp
import random
import shutil


def test_parse_num_proc():
    nc = mp.cpu_count()
    n = mppfc.parse_num_proc(1)
    assert n == 1
    n = mppfc.parse_num_proc(nc)
    assert n == nc
    try:
        mppfc.parse_num_proc(nc+1)
    except ValueError:
        pass
    else:
        assert False, "ValueError should have been raised if num_proc exceeds number of cores"

    n = mppfc.parse_num_proc(-1)
    assert n == nc-1
    n = mppfc.parse_num_proc(-3)
    assert n == nc - 3
    n = mppfc.parse_num_proc(0)
    assert n == nc
    try:
        mppfc.parse_num_proc(-nc)
    except ValueError:
        pass
    else:
        assert False, "ValueError should have been raised if num_proc <= - number of cores"

    try:
        mppfc.parse_num_proc(-nc-1)
    except ValueError:
        pass
    else:
        assert False, "ValueError should have been raised if num_proc <= - number of cores"

    n = mppfc.parse_num_proc(0.5)
    assert n == int(0.5*nc)

    n = mppfc.parse_num_proc(0.9)
    assert n == int(0.9*nc)

    n = mppfc.parse_num_proc(1.0)
    assert n == nc

    try:
        mppfc.parse_num_proc(1.1)
    except ValueError:
        pass
    else:
        assert False, "ValueError should have been raised if num_proc(float) > 1.0"

    try:
        mppfc.parse_num_proc(0.0)
    except ValueError:
        pass
    else:
        assert False, "ValueError should have been raised if num_proc(float) <= 0.0"


def test_hash_bytes_to_3_hex():
    b = bytes.fromhex('ffff ff ff ab')
    h = mppfc.CacheFileBased.hash_bytes_to_3_hex(b)
    assert h[0] == '3fff'
    assert h[1] == '3fff'
    assert h[2] == 'fab'

    b = bytes.fromhex('6312 11 22 33')
    h = mppfc.CacheFileBased.hash_bytes_to_3_hex(b)
    assert h[0] == '1111'
    assert h[1] == '2222'
    assert h[2] == '333'

    random.seed(0)
    two_bits = ['0', '1', '2', '3']
    for _ in range(10):
        s1 = random.choice(two_bits)
        s2 = random.choice(two_bits)
        for _ in range(3):
            s1 += random.choice(mppfc.cache.hex_alphabet)
            s2 += random.choice(mppfc.cache.hex_alphabet)

        s3 = ''
        for _ in range(7):
            s3 += random.choice(mppfc.cache.hex_alphabet)

        s1_0 = int(s1[0])
        s2_0 = int(s2[0])

        i = (s1_0 << 2) + s2_0
        h0 = mppfc.cache.hex_alphabet[i]
        h1 = s3[0]
        h = h0 + h1 + s1[1] + s2[1] + s1[2:] + s2[2:] + s3[1:]
        b = bytes.fromhex(h)
        s_prime = mppfc.CacheFileBased.hash_bytes_to_3_hex(b)
        assert s_prime[0] == s1
        assert s_prime[1] == s2
        assert s_prime[2] == s3


class Point:
    def __init__(self, x, y):
        self.x = x
        self.y = y

    def __getstate__(self):
        return [self.x, self.y]

    def __setstate__(self, state):
        self.x = state[0]
        self.y = state[1]


@mppfc.cache.CacheFileBasedDec()
def fnc(p, a=1, b=2):
    return (p.x * p.y) ** a, a * b


def test_cache():
    shutil.rmtree(fnc.cache_dir)

    p = Point(4, -2)
    r = fnc(p)
    assert r[0] == -8
    assert r[1] == 2
    assert fnc(p, _cache_flag="has_key") is True

    assert fnc(p, a=2, _cache_flag="has_key") is False
    fnc(p, a=2, _cache_flag="no_cache")
    assert fnc(p, a=2, _cache_flag="has_key") is False
    fnc(p, a=2)
    assert fnc(p, a=2, _cache_flag="has_key") is True

    f_name = fnc.get_f_name(p, a=2)
    with open(f_name, 'wb') as f:
        pickle.dump(None, f)

    assert fnc(p, a=2) is None
    r = fnc(p, a=2, _cache_flag="update")
    assert r is not None
    assert fnc(p, a=2, _cache_flag="has_key") is True

    try:
        fnc(p, a=2, b=0, _cache_flag="cache_only")
    except KeyError:
        pass
    else:
        assert False, "KeyError should have been raised"

    r = fnc(p, a=1.234, _cache_flag="no_cache")
    fnc.set_result(p, a=1.234, _cache_result=r)
    assert fnc(p, a=1.234, _cache_flag="has_key")
    r2 = fnc(p, a=1.234, _cache_flag="cache_only")
    assert r[0] == r2[0]
    assert r[1] == r2[1]

    try:
        fnc.set_result(p, a=1.234, _cache_result=None)
    except ValueError:
        pass
    else:
        assert False, "ValueError should have been raised"


@mppfc.MultiProcCachedFunctionDec()
def some_function(x, a='y'):
    time.sleep(x)
    return 42*x, a


def test_multi_proc_dec():
    shutil.rmtree(some_function.cached_fnc.cache_dir)

    sleep_in_sec = 0.1
    r_no_cache = some_function(x=sleep_in_sec, _cache_flag='no_cache')
    r_with_caching = some_function(x=sleep_in_sec)
    r_from_cache = some_function(x=sleep_in_sec, _cache_flag='cache_only')

    assert r_no_cache[0] == r_with_caching[0]
    assert r_no_cache[1] == r_with_caching[1]

    assert r_no_cache[0] == r_from_cache[0]
    assert r_no_cache[1] == r_from_cache[1]

    some_function.start_mp(num_proc=2)

    t0 = time.perf_counter_ns()
    for sleep_in_sec in [1, 1.1, 1.2, 1.3]:
        some_function(x=sleep_in_sec)
    t1 = time.perf_counter_ns()
    some_function.wait()
    t3 = time.perf_counter_ns()






    #some_function.join()
    #print(some_function.result_dict)






if __name__ == "__main__":
    # test_multi_proc_dec()
    # test_parse_num_proc()
    # test_hash_bytes_to_3_hex()
    test_multi_proc_dec()