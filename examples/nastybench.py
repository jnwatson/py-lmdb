
# Roughly approximates some of Symas microbenchmark.

from time import time
import random
import shutil
import os
import tempfile

import lmdb


val = ' ' * 100
MAX_KEYS = int(1e6)

t0 = time()

urandom = file('/dev/urandom', 'rb', 1048576).read

keys = set()
while len(keys) < MAX_KEYS:
    for _ in xrange(min(1000, MAX_KEYS - len(keys))):
        keys.add(urandom(16))

print 'make %d keys in %.2fsec' % (len(keys), time() - t0)
keys = list(keys)

if os.path.exists('/ram'):
    DB_PATH = '/ram/dbtest'
else:
    DB_PATH = tempfile.mktemp(prefix='nastybench')

if os.path.exists(DB_PATH):
    shutil.rmtree(DB_PATH)

env = lmdb.open(DB_PATH, map_size=1048576 * 1024,
    metasync=False, sync=False, map_async=True)

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



nextkey = iter(keys).next
t0 = time()

with env.begin() as txn:
    try:
        while 1:
            txn.get(nextkey())
    except StopIteration:
        pass

d = time() - t0
print 'random lookup %d keys in %.2fsec (%d/sec)' % (len(keys), d, len(keys)/d)


nextkey = iter(keys).next
t0 = time()

with env.begin(buffers=True) as txn:
    try:
        while 1:
            txn.get(nextkey())
    except StopIteration:
        pass

d = time() - t0
print 'random lookup %d buffers in %.2fsec (%d/sec)' % (len(keys), d, len(keys)/d)


nextkey = iter(keys).next
t0 = time()

with env.begin(buffers=True) as txn:
    try:
        while 1:
            hash(txn.get(nextkey()))
    except StopIteration:
        pass

d = time() - t0
print 'random lookup+hash %d buffers in %.2fsec (%d/sec)' % (len(keys), d, len(keys)/d)



nextkey = iter(keys).next
t0 = time()

with env.begin(buffers=True) as txn:
    nextrec = txn.cursor().iternext().next
    try:
        while 1:
            nextrec()
    except StopIteration:
        pass

d = time() - t0
print 'seq read %d buffers in %.2fsec (%d/sec)' % (len(keys), d, len(keys)/d)
