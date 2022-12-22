import mppfc
import multiprocessing as mp


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


# @mpdec.MultiProcDec(num_proc=2)
# def some_function(x, a='y'):
#     return 42
#
#
# def test_multi_proc_dec():
#     some_function(x=None)
#     some_function.join()
#     print(some_function.result_dict)
#
#
#
if __name__ == "__main__":
    # test_multi_proc_dec()
    test_parse_num_proc()
