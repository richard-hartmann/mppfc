import mppfc
import time
import matplotlib.pyplot as plt


def slow_function(sleeping_time):
    time.sleep(sleeping_time)
    return sleeping_time ** 2


if __name__ == "__main_:":

    # basic calculation
    t0 = time.perf_counter_ns()
    data = []
    for x in range(-10, 10):
        t = 2 + x / 10
        y = slow_function(sleeping_time=t)
        data.append([t, y])
    time_basic = (time.perf_counter_ns() - t0) / 10 ** 9
