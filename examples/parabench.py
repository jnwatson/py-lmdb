
# Roughly approximates some of Symas microbenchmark.

import multiprocessing
import os
import random
import shutil
import sys
import tempfile
import time

try:
    import affinity
except:
    affinity = False
import lmdb


USE_SPARSE_FILES = sys.platform != 'darwin'
DB_PATH = '/ram/dbtest'
MAX_KEYS = int(4e6)

if os.path.exists('/ram'):
    DB_PATH = '/ram/dbtest'
else:
    DB_PATH = tempfile.mktemp(prefix='parabench')


def open_env():
    return lmdb.open(DB_PATH,
        map_size=1048576 * 1024,
        metasync=False,
        sync=False,
        map_async=True,
        writemap=USE_SPARSE_FILES)


def make_keys():
    t0 = time.time()
    urandom = file('/dev/urandom', 'rb', 1048576).read

    keys = set()
    while len(keys) < MAX_KEYS:
        for _ in xrange(min(1000, MAX_KEYS - len(keys))):
            keys.add(urandom(16))

    print 'make %d keys in %.2fsec' % (len(keys), time.time() - t0)
    keys = list(keys)

    nextkey = iter(keys).next
    run = True
    val = ' ' * 100
    env = open_env()
    while run:
        with env.begin(write=True) as txn:
            try:
                for _ in xrange(10000):
                    txn.put(nextkey(), val)
            except StopIteration:
                run = False

    d = time.time() - t0
    env.sync(True)
    env.close()
    print 'insert %d keys in %.2fsec (%d/sec)' % (len(keys), d, len(keys) / d)


if 'drop' in sys.argv and os.path.exists(DB_PATH):
    shutil.rmtree(DB_PATH)

if not os.path.exists(DB_PATH):
    make_keys()


env = open_env()
with env.begin() as txn:
    keys = list(txn.cursor().iternext(values=False))
env.close()


def run(idx):
    if affinity:
        affinity.set_process_affinity_mask(os.getpid(), 1 << idx)

    env = open_env()
    k = list(keys)
    random.shuffle(k)
    k = k[:1000]

    while 1:
        with env.begin() as txn:
            nextkey = iter(k).next
            try:
                while 1:
                    hash(txn.get(nextkey()))
            except StopIteration:
                pass
            arr[idx] += len(k)



nproc = int(sys.argv[1])
arr = multiprocessing.Array('L', xrange(nproc))
for x in xrange(nproc):
    arr[x] = 0
procs = [multiprocessing.Process(target=run, args=(x,)) for x in xrange(nproc)]
[p.start() for p in procs]


t0 = time.time()
while True:
    time.sleep(2)
    d = time.time() - t0
    lk = sum(arr)
    print 'lookup %d keys in %.2fsec (%d/sec)' % (lk, d, lk / d)

