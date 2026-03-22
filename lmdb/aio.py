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


# Methods that are cheap, pure-Python accessors — no I/O, no GIL release.
# Calling these through run_in_executor would just add overhead.
_ENV_SYNC = frozenset({'path', 'max_key_size', 'max_readers', 'flags'})
_TXN_SYNC = frozenset({'id'})
_CURSOR_SYNC = frozenset({'key', 'value', 'item'})

# Iterator methods return generators — must be consumed in the executor.
_CURSOR_ITERS = frozenset({
    'iternext', 'iternext_dup', 'iternext_nodup',
    'iterprev', 'iterprev_dup', 'iterprev_nodup',
})


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
        return await self._result.__aexit__(exc_type, exc_val, exc_tb)


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

    def __init__(self, env, executor=None):
        self._env = env
        self._executor = executor

    # -- context manager --------------------------------------------------

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        self._env.close()

    # -- methods that return wrapped objects -------------------------------

    def begin(self, *args, **kwargs):
        """Start a new transaction, returning an :py:class:`AsyncTransaction`.

        Accepts the same arguments as :py:meth:`lmdb.Environment.begin`.
        Can be used with ``await`` or ``async with``::

            async with aenv.begin(write=True) as txn:
                await txn.put(b'key', b'value')
        """
        async def _begin():
            loop = asyncio.get_running_loop()
            txn = await loop.run_in_executor(
                self._executor,
                functools.partial(self._env.begin, *args, **kwargs),
            )
            return AsyncTransaction(txn, self._executor)
        return _AsyncContextWrapper(_begin())

    # -- attribute proxy ---------------------------------------------------

    def __getattr__(self, name):
        attr = getattr(self._env, name)
        if not callable(attr):
            return attr
        if name in _ENV_SYNC:
            return attr
        return _async_method(attr, self._executor)


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

    __slots__ = ('_txn', '_executor', '_lock')

    def __init__(self, txn, executor=None):
        self._txn = txn
        self._executor = executor
        self._lock = asyncio.Lock()

    # -- context manager --------------------------------------------------

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, _exc_val, _exc_tb):
        async with self._lock:
            if exc_type:
                self._txn.abort()
            else:
                loop = asyncio.get_running_loop()
                await loop.run_in_executor(self._executor, self._txn.commit)

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

    # -- attribute proxy ---------------------------------------------------

    def __getattr__(self, name):
        attr = getattr(self._txn, name)
        if not callable(attr):
            return attr
        if name in _TXN_SYNC:
            return attr
        return _async_method_locked(attr, self._executor, self._lock)


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

    def __init__(self, cursor, executor=None, lock=None):
        self._cursor = cursor
        self._executor = executor
        self._lock = lock or asyncio.Lock()

    # -- context manager --------------------------------------------------

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc):
        self._cursor.close()

    # -- iterators: consume in the executor and return a list -------------

    async def iternext(self, **kw):
        return await _collect_locked(
            self._cursor.iternext, kw, self._executor, self._lock)

    async def iternext_dup(self, **kw):
        return await _collect_locked(
            self._cursor.iternext_dup, kw, self._executor, self._lock)

    async def iternext_nodup(self, **kw):
        return await _collect_locked(
            self._cursor.iternext_nodup, kw, self._executor, self._lock)

    async def iterprev(self, **kw):
        return await _collect_locked(
            self._cursor.iterprev, kw, self._executor, self._lock)

    async def iterprev_dup(self, **kw):
        return await _collect_locked(
            self._cursor.iterprev_dup, kw, self._executor, self._lock)

    async def iterprev_nodup(self, **kw):
        return await _collect_locked(
            self._cursor.iterprev_nodup, kw, self._executor, self._lock)

    # -- attribute proxy ---------------------------------------------------

    def __getattr__(self, name):
        attr = getattr(self._cursor, name)
        if not callable(attr):
            return attr
        if name in _CURSOR_SYNC:
            return attr
        if name in _CURSOR_ITERS:
            return functools.partial(
                _collect_locked, attr, {}, self._executor, self._lock)
        return _async_method_locked(attr, self._executor, self._lock)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _async_method(method, executor):
    """Return a coroutine function that calls *method* in *executor*."""
    @functools.wraps(method)
    async def wrapper(*args, **kwargs):
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(
            executor,
            functools.partial(method, *args, **kwargs),
        )
    return wrapper


def _async_method_locked(method, executor, lock):
    """Like :func:`_async_method`, but acquires *lock* first."""
    @functools.wraps(method)
    async def wrapper(*args, **kwargs):
        async with lock:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(
                executor,
                functools.partial(method, *args, **kwargs),
            )
    return wrapper


async def _collect_locked(iter_method, kwargs, executor, lock):
    """Call an iterator method in the executor and return a list."""
    loop = asyncio.get_running_loop()

    def _consume():
        return list(iter_method(**kwargs))

    async with lock:
        return await loop.run_in_executor(executor, _consume)
