
from pprint import pprint
import itertools
import os
import shutil

from time import time as now
import random
import lmdb

MAP_SIZE = 1048576 * 400
DB_PATH = '/ram/testdb'


def reopen_env(**kwargs):
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    return lmdb.open(DB_PATH, map_size=MAP_SIZE, **kwargs)


def case(title, **params):
    def wrapper(func):
        t0 = now()
        count = func()
        t1 = now()
        print('%40s:  %2.3fs   %8d/sec' % (title, t1-t0, count/(t1-t0)))
        return func
    return wrapper


def x():
    big = '' # '*' * 400

    t0 = now()
    words = set(file('/usr/share/dict/words'))
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

    words_sorted = sorted(words)
    items = [(w, big or w) for w in words]
    items_sorted = [(w, big or w) for w in words_sorted]

    env = reopen_env()

    @case('insert')
    def test():
        with env.begin(write=True) as txn:
            for word in words:
                txn.put(word, big or word)
            return len(words)


    st = env.stat()
    print
    print 'stat:', st
    print 'k+v size %.2fkb avg %d, on-disk size: %.2fkb avg %d' %\
        ((2*alllen) / 1024., (2*alllen)/len(words),
         (st['psize'] * st['leaf_pages']) / 1024.,
         (st['psize'] * st['leaf_pages']) / len(words))
    print


    @case('enum (key, value) pairs')
    def test():
        with env.begin() as txn:
            return sum(1 for _ in txn.cursor())


    @case('reverse enum (key, value) pairs')
    def test():
        with env.begin() as txn:
            return sum(1 for _ in txn.cursor().iterprev())


    @case('rand lookup all keys')
    def test():
        with env.begin() as txn:
            for word in words:
                txn.get(word)
        return len(words)


    @case('per txn rand lookup+hash all keys')
    def test():
        for word in words:
            with env.begin() as txn:
                hash(txn.get(word))
        return len(words)


    @case('rand lookup+hash all keys')
    def test():
        with env.begin() as txn:
            for word in words:
                hash(txn.get(word))
        return len(words)


    @case('rand lookup all buffers')
    def test():
        with env.begin(buffers=True) as txn:
            for word in words:
                txn.get(word)
        return len(words)


    @case('rand lookup+hash all buffers')
    def test():
        with env.begin(buffers=True) as txn:
            for word in words:
                hash(txn.get(word))
        return len(words)


    @case('rand lookup buffers (cursor)')
    def test():
        with env.begin(buffers=True) as txn:
            cursget = txn.cursor().get
            for word in words:
                cursget(word)
        return len(words)


    @case('enum (key, value) buffers')
    def test():
        with env.begin(buffers=True) as txn:
            return sum(1 for _ in txn.cursor())


    @case('get+put')
    def test():
        with env.begin(write=True) as txn:
            for word in words:
                txn.get(word)
                txn.put(word, word)
            return len(words)


    @case('replace all')
    def test():
        with env.begin(write=True) as txn:
            for word in words:
                txn.replace(word, word)
        return len(words)


    env = reopen_env(writemap=True)
    @case('writemap insert')
    def test():
        with env.begin(write=True) as txn:
            for word in words:
                txn.put(word, big or word)
        return len(words)


    env = reopen_env(writemap=True)
    @case('writemap + one cursor')
    def test():
        with env.begin(write=True) as txn:
            curs = txn.cursor()
            for word in words:
                curs.put(word, big or word)
        return len(words)


    env = reopen_env(writemap=True)
    @case('writemap+putmulti')
    def test():
        with env.begin(write=True) as txn:
            txn.cursor().putmulti(items)
        return len(words)


    env = reopen_env(writemap=True)
    @case('writemap+putmulti+generator')
    def test():
        with env.begin(write=True) as txn:
            txn.cursor().putmulti((w, big or w) for w in words)
        return len(words)


    env = reopen_env()
    @case('append')
    def test():
        with env.begin(write=True) as txn:
            for word in words_sorted:
                txn.put(word, big or word, append=True)
        return len(words)


    env = reopen_env()
    @case('append+putmulti')
    def test():
        with env.begin(write=True) as txn:
            txn.cursor().putmulti(items_sorted, append=True)
        return len(words)


    print
    st = env.stat()
    print 'stat:', st
    print 'k+v size %.2fkb avg %d, on-disk size: %.2fkb avg %d' %\
        ((2*alllen) / 1024., (2*alllen)/len(words),
         (st['psize'] * st['leaf_pages']) / 1024.,
         (st['psize'] * st['leaf_pages']) / len(words))

x()
