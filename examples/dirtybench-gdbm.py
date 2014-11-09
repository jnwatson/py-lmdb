
from pprint import pprint
import os
import shutil
import tempfile

from time import time as now
import random
import gdbm

MAP_SIZE = 1048576 * 400
DB_PATH = '/ram/testdb-gdbm'

if os.path.exists('/ram'):
    DB_PATH = '/ram/testdb-gdbm'
else:
    DB_PATH = tempfile.mktemp(prefix='dirtybench-gdbm')


def x():
    big = '' # '*' * 400

    if os.path.exists(DB_PATH):
        os.unlink(DB_PATH)

    t0 = now()
    words = set(file('/usr/share/dict/words').readlines())
    words.update([w.upper() for w in words])
    words.update([w[::-1] for w in words])
    words.update([w[::-1].upper() for w in words])
    words.update(['-'.join(w) for w in words])
    #words.update(['+'.join(w) for w in words])
    #words.update(['/'.join(w) for w in words])
    words = list(words)
    alllen = sum(len(w) for w in words)
    avglen = alllen  / len(words)
    print 'permutate %d words avglen %d took %.2fsec' % (len(words), avglen, now()-t0)

    getword = iter(words).next

    env = gdbm.open(DB_PATH, 'c')

    run = True
    t0 = now()
    last = t0
    while run:
        try:
            for _ in xrange(50000):
                word = getword()
                env[word] = big or word
        except StopIteration:
            run = False

        t1 = now()
        if (t1 - last) > 2:
            print '%.2fs (%d/sec)' % (t1-t0, len(words)/(t1-t0))
            last = t1

    t1 = now()
    print 'done all %d in %.2fs (%d/sec)' % (len(words), t1-t0, len(words)/(t1-t0))
    last = t1

    print
    print

    t0 = now()
    lst = sum(env[k] and 1 for k in env.keys())
    t1 = now()
    print 'enum %d (key, value) pairs took %.2f sec' % ((lst), t1-t0)

    t0 = now()
    lst = sum(1 or env[k] for k in reversed(env.keys()))
    t1 = now()
    print 'reverse enum %d (key, value) pairs took %.2f sec' % ((lst), t1-t0)

    t0 = now()
    for word in words:
        env[word]
    t1 = now()
    print 'rand lookup all keys %.2f sec (%d/sec)' % (t1-t0, lst/(t1-t0))

    t0 = now()
    for word in words:
        hash(env[word])
    t1 = now()
    print 'per txn rand lookup+hash all keys %.2f sec (%d/sec)' % (t1-t0, lst/(t1-t0))

    t0 = now()
    for word in words:
        hash(env[word])
    t1 = now()
    print 'rand lookup+hash all keys %.2f sec (%d/sec)' % (t1-t0, lst/(t1-t0))

    t0 = now()
    for word in words:
        env[word]
    t1 = now()
    print 'rand lookup all buffers %.2f sec (%d/sec)' % (t1-t0, lst/(t1-t0))

    t0 = now()
    for word in words:
        hash(env[word])
    t1 = now()
    print 'rand lookup+hash all buffers %.2f sec (%d/sec)' % (t1-t0, lst/(t1-t0))


    #
    # get+put
    #

    getword = iter(sorted(words)).next
    run = True
    t0 = now()
    last = t0
    while run:
        try:
            for _ in xrange(50000):
                word = getword()
                old = env[word]
                env[word] = word
        except StopIteration:
            run = False

        t1 = now()
        if (t1 - last) > 2:
            print '%.2fs (%d/sec)' % (t1-t0, len(words)/(t1-t0))
            last = t1

    t1 = now()
    print 'get+put all %d in %.2fs (%d/sec)' % (len(words), t1-t0, len(words)/(t1-t0))
    last = t1


    #
    # REPLACE
    #

    getword = iter(sorted(words)).next
    run = True
    t0 = now()
    last = t0
    while run:
        try:
            for _ in xrange(50000):
                word = getword()
                old = env[word]
        except StopIteration:
            run = False

        t1 = now()
        if (t1 - last) > 2:
            print '%.2fs (%d/sec)' % (t1-t0, len(words)/(t1-t0))
            last = t1

    t1 = now()
    print 'replace all %d in %.2fs (%d/sec)' % (len(words), t1-t0, len(words)/(t1-t0))
    last = t1




x()
