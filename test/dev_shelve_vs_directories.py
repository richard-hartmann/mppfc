import random
import shelve
import binfootprint as bf
import time


class SomeParam(bf.ABCParameter):
    __slots__ = ['n', 'x']

    def __init__(self, n, x):
        self.n, self.x = n, x


def generate_data(p):
    return [p.x] * p.n


N = 300


with shelve.open("some_param.shelve") as db:
    db.clear()
random.seed(0)

t0 = time.perf_counter_ns()
with shelve.open("some_param.shelve") as db:
    for i in range(N):
        p = SomeParam(n=random.randint(1, 500), x=random.random())
        r = generate_data(p)
        hash_hex = bf.hash_hex_from_object(p)
        db[hash_hex] = r
t1 = time.perf_counter_ns()
print("shelve (write, keep open) {:.4e}".format((t1-t0)*1e-9))


with shelve.open("some_param.shelve") as db:
    db.clear()
random.seed(0)

t0 = time.perf_counter_ns()
for i in range(N):
    with shelve.open("some_param.shelve") as db:
        p = SomeParam(n=random.randint(1, 500), x=random.random())
        r = generate_data(p)
        hash_hex = bf.hash_hex_from_object(p)
        db[hash_hex] = r
t1 = time.perf_counter_ns()
print("shelve (write, open single) {:.4e}".format((t1-t0)*1e-9))



