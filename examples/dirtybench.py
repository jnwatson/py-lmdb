
from pprint import pprint
import os
import shutil

from time import time as now
import random
import lmdb

MAP_SIZE = 1048576 * 400
DB_PATH = '/ram/testdb'


def x():
    big = '' # '*' * 400

    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)

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

    env = lmdb.open(DB_PATH, map_size=MAP_SIZE)
    print 'stat:', env.stat()

    run = True
    t0 = now()
    last = t0
    while run:
        with env.begin(write=True) as txn:
            try:
                for _ in xrange(50000):
                    word = getword()
                    txn.put(word, big or word)
            except StopIteration:
                run = False

        t1 = now()
        if (t1 - last) > 2:
            print '%.2fs (%d/sec)' % (t1-t0, len(words)/(t1-t0))
            last = t1

    t1 = now()
    print 'done all %d in %.2fs (%d/sec)' % (len(words), t1-t0, len(words)/(t1-t0))
    last = t1

    st = env.stat()
    print 'stat:', st
    print 'k+v size %.2fkb avg %d, on-disk size: %.2fkb avg %d' %\
        ((2*alllen) / 1024., (2*alllen)/len(words),
         (st['psize'] * st['leaf_pages']) / 1024.,
         (st['psize'] * st['leaf_pages']) / len(words))


    with env.begin() as txn:
        t0 = now()
        lst = sum(1 for _ in txn.cursor())
        t1 = now()
        print 'enum %d (key, value) pairs took %.2f sec' % ((lst), t1-t0)

    with env.begin() as txn:
        t0 = now()
        lst = sum(1 for _ in txn.cursor().iterprev())
        t1 = now()
        print 'reverse enum %d (key, value) pairs took %.2f sec' % ((lst), t1-t0)

    with env.begin() as txn:
        t0 = now()
        for word in words:
            txn.get(word)
        t1 = now()
        print 'rand lookup all keys %.2f sec (%d/sec)' % (t1-t0, lst/(t1-t0))

    t0 = now()
    for word in words:
        with env.begin() as txn:
            hash(txn.get(word))
    t1 = now()
    print 'per txn rand lookup+hash all keys %.2f sec (%d/sec)' % (t1-t0, lst/(t1-t0))

    with env.begin() as txn:
        t0 = now()
        for word in words:
            hash(txn.get(word))
        t1 = now()
        print 'rand lookup+hash all keys %.2f sec (%d/sec)' % (t1-t0, lst/(t1-t0))

    with env.begin(buffers=True) as txn:
        t0 = now()
        for word in words:
            txn.get(word)
        t1 = now()
        print 'rand lookup all buffers %.2f sec (%d/sec)' % (t1-t0, lst/(t1-t0))

    with env.begin(buffers=True) as txn:
        t0 = now()
        for word in words:
            hash(txn.get(word))
        t1 = now()
        print 'rand lookup+hash all buffers %.2f sec (%d/sec)' % (t1-t0, lst/(t1-t0))

    with env.begin(buffers=True) as txn:
        cursget = txn.cursor().get
        t0 = now()
        for word in words:
            cursget(word)
        t1 = now()
        print 'rand lookup all buffers (cursor) %.2f sec (%d/sec)' % (t1-t0, lst/(t1-t0))

    with env.begin(buffers=True) as txn:
        t0 = now()
        lst = sum(1 for _ in txn.cursor())
        t1 = now()
        print 'enum %d (key, value) buffers took %.2f sec' % ((lst), t1-t0)




    #
    # get+put
    #

    getword = iter(sorted(words)).next
    run = True
    t0 = now()
    last = t0
    while run:
        with env.begin(write=True) as txn:
            try:
                for _ in xrange(50000):
                    word = getword()
                    old = txn.get(word)
                    txn.put(word, word)
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
        with env.begin(write=True) as txn:
            try:
                for _ in xrange(50000):
                    word = getword()
                    old = txn.replace(word, word)
            except StopIteration:
                run = False

        t1 = now()
        if (t1 - last) > 2:
            print '%.2fs (%d/sec)' % (t1-t0, len(words)/(t1-t0))
            last = t1

    t1 = now()
    print 'replace all %d in %.2fs (%d/sec)' % (len(words), t1-t0, len(words)/(t1-t0))
    last = t1


    print
    print
    print '--- MDB_WRITEMAP mode ---'
    print

    env.close()
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    env = lmdb.open(DB_PATH, map_size=MAP_SIZE, writemap=True)


    getword = iter(words).next
    run = True
    t0 = now()
    last = t0
    while run:
        with env.begin(write=True) as txn:
            try:
                for _ in xrange(50000):
                    word = getword()
                    txn.put(word, big or word)
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
    print '--- MDB_WRITEMAP + one cursor mode ---'
    print

    env.close()
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    env = lmdb.open(DB_PATH, map_size=MAP_SIZE, writemap=True)


    getword = iter(words).next
    run = True
    t0 = now()
    last = t0
    while run:
        with env.begin(write=True) as txn:
            curs = txn.cursor()
            try:
                for _ in xrange(50000):
                    word = getword()
                    curs.put(word, big or word)
            except StopIteration:
                run = False

        t1 = now()
        if (t1 - last) > 2:
            print '%.2fs (%d/sec)' % (t1-t0, len(words)/(t1-t0))
            last = t1

    t1 = now()
    print 'done all %d in %.2fs (%d/sec)' % (len(words), t1-t0, len(words)/(t1-t0))


    print
    print
    print '--- MDB_WRITEMAP + putmulti mode ---'
    print

    env.close()
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    env = lmdb.open(DB_PATH, map_size=MAP_SIZE, writemap=True)


    items = [(w, big or w) for w in words]
    itt = iter(items)
    import itertools

    run = True
    t0 = now()
    last = t0
    while run:
        with env.begin(write=True) as txn:
            curs = txn.cursor()
            consumed, added = curs.putmulti(itertools.islice(itt, 50000))
            run = added > 0

        t1 = now()
        if (t1 - last) > 2:
            print '%.2fs (%d/sec)' % (t1-t0, len(words)/(t1-t0))
            last = t1

    t1 = now()
    print 'done all %d in %.2fs (%d/sec)' % (len(words), t1-t0, len(words)/(t1-t0))
    last = t1


    print
    print
    print '--- MDB_APPEND mode ---'
    print

    env.close()
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    env = lmdb.open(DB_PATH, map_size=MAP_SIZE)


    getword = iter(sorted(words)).next
    run = True
    t0 = now()
    last = t0
    while run:
        with env.begin(write=True) as txn:
            try:
                for _ in xrange(50000):
                    word = getword()
                    txn.put(word, big or word, append=True)
            except StopIteration:
                run = False

        t1 = now()
        if (t1 - last) > 2:
            print '%.2fs (%d/sec)' % (t1-t0, len(words)/(t1-t0))
            last = t1

    t1 = now()
    print 'done all %d in %.2fs (%d/sec)' % (len(words), t1-t0, len(words)/(t1-t0))
    last = t1

    st = env.stat()
    print 'stat:', st
    print 'k+v size %.2fkb avg %d, on-disk size: %.2fkb avg %d' %\
        ((2*alllen) / 1024., (2*alllen)/len(words),
         (st['psize'] * st['leaf_pages']) / 1024.,
         (st['psize'] * st['leaf_pages']) / len(words))

x()
