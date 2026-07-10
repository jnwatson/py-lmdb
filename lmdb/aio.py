# Copyright 2013-2025 The py-lmdb authors, all rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted only as authorized by the OpenLDAP
# Public License.
#
# A copy of this license is available in the file LICENSE in the
# top-level directory of the distribution or, alternatively, at
# <http://www.OpenLDAP.org/license.html>.
#
# OpenLDAP is a registered trademark of the OpenLDAP Foundation.
#
# Individual files and/or contributed packages may be copyright by
# other parties and/or subject to additional restrictions.
#
# This work also contains materials derived from public sources.
#
# Additional information about OpenLDAP can be obtained at
# <http://www.openldap.org/>.

"""
Async wrappers for py-lmdb via :func:`asyncio.loop.run_in_executor`.

Usage::

    import lmdb
    import lmdb.aio

    env = lmdb.open('/tmp/mydb')
    aenv = lmdb.aio.wrap(env)

    async with aenv.begin(write=True) as txn:
        await txn.put(b'key', b'value')
        val = await txn.get(b'key')
"""

import asyncio
import functools
from concurrent.futures import ThreadPoolExecutor

from . import Cursor, Environment, Transaction


def wrap(env, executor=None):
    """Wrap an :class:`lmdb.Environment` for async use.

    *executor* is passed to :meth:`loop.run_in_executor`.  ``None`` (the
    default) uses the loop's default executor.
    """
    return AsyncEnvironment(env, executor)


class _AsyncContextWrapper:
    """Wraps a coroutine so it can be used as both ``await`` and ``async with``.

    Supports::

        txn = await aenv.begin(write=True)      # just await
        async with aenv.begin(write=True) as txn:  # context manager
    """

    __slots__ = ('_coro', '_result')

    def __init__(self, coro):
        self._coro = coro
        self._result = None

    def __await__(self):
        return self._coro.__await__()

    async def __aenter__(self):
        self._result = await self._coro
        return self._result

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        assert self._result is not None  # set by __aenter__
        return await self._result.__aexit__(exc_type, exc_val, exc_tb)


# ---------------------------------------------------------------------------
# Proxy method factories
# ---------------------------------------------------------------------------

def _sync_method(sync):
    """Return a method that calls *sync* directly, without an executor."""
    @functools.wraps(sync)
    def method(self, *args, **kwargs):
        return sync(getattr(self, self._WRAPS), *args, **kwargs)
    return method


def _async_method(sync):
    """Return a coroutine method that calls *sync* in the executor."""
    @functools.wraps(sync)
    async def method(self, *args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            self._executor,
            functools.partial(sync, getattr(self, self._WRAPS), *args, **kwargs),
        )
    return method


def _async_method_locked(sync):
    """Like :func:`_async_method`, but acquires ``self._lock`` first."""
    @functools.wraps(sync)
    async def method(self, *args, **kwargs):
        async with self._lock:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                self._executor,
                functools.partial(sync, getattr(self, self._WRAPS), *args, **kwargs),
            )
    return method


def _collect_locked(sync):
    """Like :func:`_async_method_locked`, but *sync* returns an iterator
    consumed in the executor and returned as a list."""
    @functools.wraps(sync)
    async def method(self, *args, **kwargs):
        async with self._lock:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                self._executor,
                lambda: list(sync(getattr(self, self._WRAPS), *args, **kwargs)),
            )
    return method


# ---------------------------------------------------------------------------
# Async wrappers
# ---------------------------------------------------------------------------

class AsyncEnvironment:
    """Async wrapper for :py:class:`lmdb.Environment`.

    Created by :py:func:`wrap`.  All methods of the underlying
    :py:class:`~lmdb.Environment` are available and are dispatched to an
    executor, except for the low-overhead accessors ``path()``,
    ``max_key_size()``, ``max_readers()``, and ``flags()``, which are called
    directly.

    Supports ``async with`` for lifetime management — the environment is
    closed on exit.
    """

    __slots__ = ('_env', '_executor')

    _WRAPS = '_env'

    def __init__(self, env, executor=None):
        self._env = env
        self._executor = executor

    # -- context manager --------------------------------------------------

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, self._env.close)

    # -- methods that return wrapped objects -------------------------------

    def begin(self, *args, **kwargs):
        """Start a new transaction, returning an :py:class:`AsyncTransaction`.

        Accepts the same arguments as :py:meth:`lmdb.Environment.begin`.
        Can be used with ``await`` or ``async with``::

            async with aenv.begin(write=True) as txn:
                await txn.put(b'key', b'value')

        A **write** transaction is given its own private single-thread
        executor, so ``begin``, every operation, and ``commit``/``abort`` all
        run on one OS thread.  LMDB ties a write transaction (and, on Linux,
        its robust process-shared write mutex) to the thread that began it, so
        dispatching its operations across a shared multi-threaded executor
        could release the mutex on the wrong thread (see issue #465).  Read
        transactions are safe to migrate between threads under ``MDB_NOTLS``
        and use the environment's executor.
        """
        # begin(db=None, parent=None, write=False, buffers=False)
        write = bool(kwargs.get('write', args[2] if len(args) > 2 else False))

        async def _begin():
            private = ThreadPoolExecutor(max_workers=1) if write else None
            executor = private if private is not None else self._executor
            loop = asyncio.get_running_loop()
            try:
                txn = await loop.run_in_executor(
                    executor,
                    functools.partial(self._env.begin, *args, **kwargs),
                )
            except BaseException:
                if private is not None:
                    private.shutdown(wait=False)
                raise
            return AsyncTransaction(
                txn, executor, owns_executor=private is not None)
        return _AsyncContextWrapper(_begin())

    # -- proxied methods --------------------------------------------------

    path = _sync_method(Environment.path)
    max_key_size = _sync_method(Environment.max_key_size)
    max_readers = _sync_method(Environment.max_readers)
    flags = _sync_method(Environment.flags)

    stat = _async_method(Environment.stat)
    info = _async_method(Environment.info)
    close = _async_method(Environment.close)
    copy = _async_method(Environment.copy)
    copyfd = _async_method(Environment.copyfd)
    sync = _async_method(Environment.sync)
    readers = _async_method(Environment.readers)
    reader_check = _async_method(Environment.reader_check)
    set_mapsize = _async_method(Environment.set_mapsize)
    open_db = _async_method(Environment.open_db)
    dbs = _async_method(Environment.dbs)

    # -- attribute fallback -----------------------------------------------

    def __getattr__(self, name):
        attr = getattr(self._env, name)
        if callable(attr):
            raise AttributeError(name)
        return attr


class AsyncTransaction:
    """Async wrapper for :py:class:`lmdb.Transaction`.

    All methods of the underlying :py:class:`~lmdb.Transaction` are available.
    Most are dispatched to an executor; ``id()`` is called directly.

    An :py:class:`asyncio.Lock` serializes all operations dispatched through
    this transaction, including operations on its cursors.  This makes
    :py:func:`asyncio.gather` safe on the same transaction.

    Supports ``async with`` — write transactions are committed on clean exit
    and aborted on exception.
    """

    __slots__ = ('_txn', '_executor', '_lock', '_owns_executor', '_done')

    _WRAPS = '_txn'

    def __init__(self, txn, executor=None, owns_executor=False):
        self._txn = txn
        self._executor = executor
        # True for write transactions, whose *executor* is a private
        # single-thread pool created by :py:meth:`AsyncEnvironment.begin` and
        # torn down when the transaction finishes.
        self._owns_executor = owns_executor
        self._done = False
        self._lock = asyncio.Lock()

    def _shutdown_executor(self):
        if self._owns_executor:
            self._owns_executor = False
            executor = self._executor
            assert executor is not None  # an owned executor is a real pool
            executor.shutdown(wait=False)

    # -- context manager --------------------------------------------------

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, _exc_val, _exc_tb):
        async with self._lock:
            # An explicit commit()/abort() may already have finished the txn.
            if self._done:
                return
            self._done = True
            loop = asyncio.get_running_loop()
            try:
                # abort, like commit, must run on the executor so that a
                # write txn releases its thread-bound mutex on its own thread.
                if exc_type:
                    await loop.run_in_executor(self._executor, self._txn.abort)
                else:
                    await loop.run_in_executor(self._executor, self._txn.commit)
            finally:
                self._shutdown_executor()

    # -- methods that return wrapped objects -------------------------------

    def cursor(self, *args, **kwargs):
        """Open a cursor, returning an :py:class:`AsyncCursor`.

        Accepts the same arguments as :py:meth:`lmdb.Transaction.cursor`.
        Can be used with ``await`` or ``async with``::

            async with txn.cursor() as cur:
                await cur.first()
                items = await cur.iternext()
        """
        async def _cursor():
            async with self._lock:
                loop = asyncio.get_running_loop()
                cur = await loop.run_in_executor(
                    self._executor,
                    functools.partial(self._txn.cursor, *args, **kwargs),
                )
            return AsyncCursor(cur, self._executor, self._lock)
        return _AsyncContextWrapper(_cursor())

    # -- proxied methods --------------------------------------------------

    id = _sync_method(Transaction.id)

    stat = _async_method_locked(Transaction.stat)
    drop = _async_method_locked(Transaction.drop)
    get = _async_method_locked(Transaction.get)
    put = _async_method_locked(Transaction.put)
    replace = _async_method_locked(Transaction.replace)
    pop = _async_method_locked(Transaction.pop)
    delete = _async_method_locked(Transaction.delete)

    async def commit(self):
        """Commit the transaction and release its executor if privately owned."""
        async with self._lock:
            self._done = True
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(self._executor, self._txn.commit)
            finally:
                self._shutdown_executor()

    async def abort(self):
        """Abort the transaction and release its executor if privately owned."""
        async with self._lock:
            self._done = True
            loop = asyncio.get_running_loop()
            try:
                await loop.run_in_executor(self._executor, self._txn.abort)
            finally:
                self._shutdown_executor()

    # -- attribute fallback -----------------------------------------------

    def __getattr__(self, name):
        attr = getattr(self._txn, name)
        if callable(attr):
            raise AttributeError(name)
        return attr


class AsyncCursor:
    """Async wrapper for :py:class:`lmdb.Cursor`.

    All methods of the underlying :py:class:`~lmdb.Cursor` are available.
    Most are dispatched to an executor; ``key()``, ``value()``, and
    ``item()`` are called directly.

    Iterator methods (``iternext()``, ``iterprev()``, etc.) are consumed in
    the executor and returned as a list.

    Shares the parent transaction's :py:class:`asyncio.Lock`.

    Supports ``async with`` — the cursor is closed on exit.
    """

    __slots__ = ('_cursor', '_executor', '_lock')

    _WRAPS = '_cursor'

    def __init__(self, cursor, executor=None, lock=None):
        self._cursor = cursor
        self._executor = executor
        self._lock = lock or asyncio.Lock()

    # -- context manager --------------------------------------------------

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        # Serialize with sibling operations on the shared transaction and run
        # on the executor, so closing never races an in-flight op in a worker
        # thread (which would touch the same MDB_txn concurrently).
        async with self._lock:
            loop = asyncio.get_running_loop()
            await loop.run_in_executor(self._executor, self._cursor.close)

    # -- proxied methods --------------------------------------------------

    key = _sync_method(Cursor.key)
    value = _sync_method(Cursor.value)
    item = _sync_method(Cursor.item)

    close = _async_method_locked(Cursor.close)
    first = _async_method_locked(Cursor.first)
    first_dup = _async_method_locked(Cursor.first_dup)
    last = _async_method_locked(Cursor.last)
    last_dup = _async_method_locked(Cursor.last_dup)
    prev = _async_method_locked(Cursor.prev)
    prev_dup = _async_method_locked(Cursor.prev_dup)
    prev_nodup = _async_method_locked(Cursor.prev_nodup)
    next = _async_method_locked(Cursor.next)
    next_dup = _async_method_locked(Cursor.next_dup)
    next_nodup = _async_method_locked(Cursor.next_nodup)
    set_key = _async_method_locked(Cursor.set_key)
    set_key_dup = _async_method_locked(Cursor.set_key_dup)
    set_range = _async_method_locked(Cursor.set_range)
    set_range_dup = _async_method_locked(Cursor.set_range_dup)
    delete = _async_method_locked(Cursor.delete)
    count = _async_method_locked(Cursor.count)
    put = _async_method_locked(Cursor.put)
    putmulti = _async_method_locked(Cursor.putmulti)
    replace = _async_method_locked(Cursor.replace)
    pop = _async_method_locked(Cursor.pop)
    get = _async_method_locked(Cursor.get)
    getmulti = _async_method_locked(Cursor.getmulti)

    iternext = _collect_locked(Cursor.iternext)
    iternext_dup = _collect_locked(Cursor.iternext_dup)
    iternext_nodup = _collect_locked(Cursor.iternext_nodup)
    iterprev = _collect_locked(Cursor.iterprev)
    iterprev_dup = _collect_locked(Cursor.iterprev_dup)
    iterprev_nodup = _collect_locked(Cursor.iterprev_nodup)

    # -- attribute fallback -----------------------------------------------

    def __getattr__(self, name):
        attr = getattr(self._cursor, name)
        if callable(attr):
            raise AttributeError(name)
        return attr
