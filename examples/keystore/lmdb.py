
from __future__ import absolute_import
import operator

import twisted.internet.defer
import twisted.internet.threads
import zope.interface

import keystore.interfaces


class LmdbKeyStoreSync(object):
    zope.interface.implements(keystore.interfaces.IKeyStoreSync)
    cursor = None

    def __init__(self, env, write):
        self.txn = env.begin(write=write)

    def __enter__(self):
        return self

    def __exit__(self, e_type, e_val, e_tb):
        if e_type:
            self.txn.abort()
        else:
            self.txn.commit()
        self.txn = None

    def get(self, key):
        return self.txn.get(key)

    def put(self, key, value):
        self.txn.put(key, value)

    def delete(self, key):
        self.txn.delete(key)

    #
    # Cursor.
    #

    @property
    def key(self):
        return self.cursor.key()

    @property
    def value(self):
        return self.cursor.value()

    def seek(self, key):
        self.cursor.set_range(key)

    def next(self):
        return self.cursor.next()

    def prev(self):
        return self.cursor.prev()


def _reader_task(env, func):
    with LmdbKeyStoreSync(env, write=False) as sync:
        return func(sync)


def _writer_task(env, func):
    with LmdbKeyStoreSync(env, write=True) as sync:
        return func(sync)


class LmdbKeyStore(object):
    zope.interface.implements(keystore.interfaces.IKeyStore)

    def __init__(self, reactor, pool, env):
        self.reactor = reactor
        self.pool = pool
        self.env = env

    def _call_in_thread(self, func):
        return twisted.internet.threads.deferToThreadPool(
            self.reactor,
            self.pool,
            func)

    def get(self, key):
        twisted.python.log.msg('get(%r, %r)', key)
        get = lambda sync: sync.get(key)
        return self._call_in_thread(lambda: _reader_task(self.env, get))

    def _get_forward(self, sync, key, count, getter):
        positioned = sync.seek(key)
        out = []
        for x in xrange(count):
            if not positioned:
                break
            out.append(getter(sync))
            positioned = sync.next()
        return out

    def _get_reverse(self, sync, key, count, getter):
        out = []
        positioned = sync.seek(key)
        if not positioned:
            positioned = sync.last()
        for x in xrange(count):
            if not positioned:
                break
            out.append(sync.key)
            positioned = sync.prev()
        return out

    _key_getter = operator.attrgetter('key')
    _item_getter = operator.attrgetter('key', 'value')

    def getKeys(self, key, count):
        get = lambda sync: self._get_forward(sync, key, count,
                                             self._key_getter)
        return self._call_in_thread(lambda: _reader_task(self.env, get))

    def getKeysReverse(self, key, count):
        get = lambda sync: self._get_reverse(sync, key, count,
                                             self._key_getter)
        return self._call_in_thread(lambda: _reader_task(self.env, get))

    def getItems(self, key, count):
        get = lambda sync: self._get_forward(sync, key, count,
                                             self._item_getter)
        return self._call_in_thread(lambda: _reader_task(self.env, get))

    def getItemsReverse(self, key, count):
        get = lambda sync: self._get_reverse(sync, key, count,
                                             self._item_getter)
        return self._call_in_thread(lambda: _reader_task(self.env, get))

    def put(self, key, value):
        twisted.python.log.msg('put(%r, %r)', key, value)
        put = lambda sync: sync.put(key, value)
        return self._call_in_thread(lambda: _writer_task(self.env, put))

    def delete(self, key):
        delete = lambda sync: sync.delete(key)
        return self._call_in_thread(lambda: _writer_task(self.env, delete))

    def putGroup(self, func):
        return self._call_in_thread(lambda: _writer_task(self.env, func))
