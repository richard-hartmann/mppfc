import binfootprint as bf
import pathlib
import random
import time

import numpy as np
import matplotlib.pyplot as plt


def create_data(number_of_files, where='.'):
    p = pathlib.Path(where).absolute() / "n_{}".format(number_of_files)
    if p.exists():
        return
    p.mkdir(parents=True, exist_ok=True)
    print("create {} files in {}".format(number_of_files, p))
    for i in range(number_of_files):
        fname = bf.hash_hex_from_object([i, number_of_files])
        with open(p / fname, 'wt') as txt:
            txt.write("{} / {}".format(i+1, number_of_files))


def measure_file_access_time(number_of_files, how_many_time, where='.'):
    p = pathlib.Path(where).absolute() / "n_{}".format(number_of_files)
    access_times = []
    for i in range(how_many_time):
        idx = random.randint(0, number_of_files-1)
        fname = bf.hash_hex_from_object([idx, number_of_files])
        f = p / fname
        t0 = time.perf_counter_ns()
        assert f.exists()
        t1 = time.perf_counter_ns()
        access_times.append(t1-t0)
    return access_times


def measure_file_read_time(number_of_files, how_many_time, where='.'):
    p = pathlib.Path(where).absolute() / "n_{}".format(number_of_files)
    access_times = []
    cc = ''
    for i in range(how_many_time):
        idx = random.randint(0, number_of_files-1)
        fname = bf.hash_hex_from_object([idx, number_of_files])
        f = p / fname
        t0 = time.perf_counter_ns()
        with open(f, "rt") as txt:
            c = txt.read(1)
        t1 = time.perf_counter_ns()
        cc += c
        access_times.append(t1-t0)
    return access_times


if __name__ == "__main__":
    sub_dir = "../../mppfc_dev_file_access_time"
    log_n_low = 10
    log_n_high = 22
    samples = 100

    log_n_list = list(range(log_n_low, log_n_high))
    n_list = [2**ni for ni in log_n_list]

    for log_n in log_n_list:
        n = 2**log_n
        create_data(number_of_files=n, where=sub_dir)

    fig, ax = plt.subplots(nrows=2, figsize=(8, 10))

    num_runs = 75

    axc = ax[0]
    for l in range(5):
        data_file_exists = np.zeros(shape=(len(log_n_list), 2))
        for k in range(num_runs):
            for i, log_n in enumerate(log_n_list):
                n = 2**log_n
                access_times = measure_file_access_time(number_of_files=n, how_many_time=samples, where=sub_dir)
                mean = np.mean(access_times)/10**6
                devi = np.std(access_times)/10**6
                print(l, k, n, mean, devi)
                data_file_exists[i, 0] += mean
                data_file_exists[i, 1] += devi
        data_file_exists /= num_runs

        axc.plot(n_list, data_file_exists[:, 0], marker='.', label="run {}".format(l+1))
    axc.legend(loc='upper left')
    axc.set_xscale('log')
    axc.set_xlabel("number of files per directory")
    axc.set_ylabel("time of file exists call (ms)")
    axc.set_title("file exists")

    axc = ax[1]
    data_file_read = np.zeros(shape=(len(log_n_list), 2))

    for l in range(5):
        for k in range(num_runs):
            for i, log_n in enumerate(log_n_list):
                n = 2 ** log_n
                access_times = measure_file_read_time(number_of_files=n, how_many_time=samples//10, where=sub_dir)
                mean = np.mean(access_times)/10**6
                devi = np.std(access_times)/10**6
                print(l, k, n, mean, devi)
                data_file_read[i, 0] += mean
                data_file_read[i, 1] += devi
        data_file_read /= num_runs

        axc.plot(n_list, data_file_read[:, 0], marker='.', label="run {}".format(l+1))
    axc.legend(loc='upper left')
    axc.set_xscale('log')
    axc.set_xlabel("number of files per directory")
    axc.set_ylabel("time to read one character (ms)")
    axc.set_title("read one character from file")

    fig.tight_layout()
    fig.subplots_adjust(hspace=0.3)
    fig.savefig("file_access_time.pdf")
    fig.savefig("file_access_time.jpg", dpi=300)
    #plt.show()
