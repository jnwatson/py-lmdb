
# Roughly approximates some of Symas microbenchmark.

from time import time, sleep
import affinity
import random
import shutil
import os
import sys

import multiprocessing
import lmdb


val = ' ' * 100
max_keys = int(4e6)

t0 = time()

urandom = file('/dev/urandom', 'rb', 1048576).read
'''
keys = set()
while len(keys) < max_keys:
    for _ in xrange(min(1000, max_keys - len(keys))):
        keys.add(urandom(16))

print 'make %d keys in %.2fsec' % (len(keys), time() - t0)
keys = list(keys)


if os.path.exists('/tmp/dbtest'):
    shutil.rmtree('/tmp/dbtest')
'''
env = lmdb.open('/tmp/dbtest', map_size=1048576 * 1024
    #, metasync=False, sync=False, map_async=True)
)
'''
print len(keys)
exit()

nextkey = iter(keys).next
run = True
while run:
    with env.begin(write=True) as txn:
        try:
            for _ in xrange(10000):
                txn.put(nextkey(), val)
        except StopIteration:
            run = False

d = time() - t0
env.sync(True)
print 'insert %d keys in %.2fsec (%d/sec)' % (len(keys), d, len(keys) / d)

'''

keys = list(env.cursor().iternext(values=False))
env.close()


import os

def run(idx):
    affinity.set_process_affinity_mask(os.getpid(), 1 << idx)

    env = lmdb.open('/ram/dbtest', map_size=1048576 * 1024,
        metasync=False, sync=False, map_async=True)

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

        samp = random.sample(keys, int(len(k) / 10))
        with env.begin(write=True) as txn:
            for sk in samp:
                txn.put(sk, sk+sk)
        arrw[idx] += len(samp)



nproc = int(sys.argv[1])
arr = multiprocessing.Array('L', xrange(nproc))
arrw = multiprocessing.Array('L', xrange(nproc))
for x in xrange(nproc):
    arr[x] = 0
    arrw[x] = 0
procs = [multiprocessing.Process(target=run, args=(x,)) for x in xrange(nproc)]
[p.start() for p in procs]


t0 = time()
while True:
    sleep(2)
    d = time() - t0
    lk = sum(arr)
    lkw = sum(arrw)
    print 'lookup %d keys insert %d keys in %.2fsec (%d/%d/%d/sec)' %\
        (lk, lkw, d, lk / d, lkw / d, (lk+lkw) / d)

