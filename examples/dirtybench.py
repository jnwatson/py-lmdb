
from pprint import pprint
import atexit
import gzip
import itertools
import os
import shutil
import sys
import tempfile

from time import time as now
import random
import lmdb

MAP_SIZE = 1048576 * 400
DB_PATH = '/ram/testdb'
USE_SPARSE_FILES = sys.platform != 'darwin'

if os.path.exists('/ram'):
    DB_PATH = '/ram/testdb'
else:
    DB_PATH = tempfile.mktemp(prefix='dirtybench')


env = None
@atexit.register
def cleanup():
    if env:
        env.close()
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)


def reopen_env(**kwargs):
    if env:
        env.close()
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    return lmdb.open(DB_PATH, map_size=MAP_SIZE, writemap=USE_SPARSE_FILES, **kwargs)


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
    words_path = os.path.join(os.path.dirname(__file__), 'words.gz')
    words = set(gzip.open(words_path).read().splitlines())
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
    print 'DB_PATH:', DB_PATH

    words_sorted = sorted(words)
    items = [(w, big or w) for w in words]
    items_sorted = [(w, big or w) for w in words_sorted]

    global env
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


    @case('enum (key, value) buffers')
    def test():
        with env.begin(buffers=True) as txn:
            return sum(1 for _ in txn.cursor())


    print


    @case('rand lookup')
    def test():
        with env.begin() as txn:
            for word in words:
                txn.get(word)
        return len(words)


    @case('per txn rand lookup')
    def test():
        for word in words:
            with env.begin() as txn:
                txn.get(word)
        return len(words)


    @case('rand lookup+hash')
    def test():
        with env.begin() as txn:
            for word in words:
                hash(txn.get(word))
        return len(words)


    @case('rand lookup buffers')
    def test():
        with env.begin(buffers=True) as txn:
            for word in words:
                txn.get(word)
        return len(words)


    @case('rand lookup+hash buffers')
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


    print


    @case('get+put')
    def test():
        with env.begin(write=True) as txn:
            for word in words:
                txn.get(word)
                txn.put(word, word)
            return len(words)


    @case('replace')
    def test():
        with env.begin(write=True) as txn:
            for word in words:
                txn.replace(word, word)
        return len(words)


    @case('get+put (cursor)')
    def test():
        with env.begin(write=True) as txn:
            with txn.cursor() as cursor:
                for word in words:
                    cursor.get(word)
                    cursor.put(word, word)
            return len(words)


    @case('replace (cursor)')
    def test():
        with env.begin(write=True) as txn:
            with txn.cursor() as cursor:
                for word in words:
                    cursor.replace(word, word)
        return len(words)


    print


    env = reopen_env()
    @case('insert (rand)')
    def test():
        with env.begin(write=True) as txn:
            for word in words:
                txn.put(word, big or word)
        return len(words)


    env = reopen_env()
    @case('insert (seq)')
    def test():
        with env.begin(write=True) as txn:
            for word in words_sorted:
                txn.put(word, big or word)
        return len(words)


    env = reopen_env()
    @case('insert (rand), reuse cursor')
    def test():
        with env.begin(write=True) as txn:
            curs = txn.cursor()
            for word in words:
                curs.put(word, big or word)
        return len(words)
    env = reopen_env()


    @case('insert (seq), reuse cursor')
    def test():
        with env.begin(write=True) as txn:
            curs = txn.cursor()
            for word in words_sorted:
                curs.put(word, big or word)
        return len(words)


    env = reopen_env()
    @case('insert, putmulti')
    def test():
        with env.begin(write=True) as txn:
            txn.cursor().putmulti(items)
        return len(words)


    env = reopen_env()
    @case('insert, putmulti+generator')
    def test():
        with env.begin(write=True) as txn:
            txn.cursor().putmulti((w, big or w) for w in words)
        return len(words)


    print


    env = reopen_env()
    @case('append')
    def test():
        with env.begin(write=True) as txn:
            for word in words_sorted:
                txn.put(word, big or word, append=True)
        return len(words)


    env = reopen_env()
    @case('append, reuse cursor')
    def test():
        with env.begin(write=True) as txn:
            curs = txn.cursor()
            for word in words_sorted:
                curs.put(word, big or word, append=True)
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
