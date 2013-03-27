# Copyright 2013 The py-lmdb authors, all rights reserved.
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
cffi wrapper for OpenLDAP's "Lightning" MDB database.

Please see http://lmdb.readthedocs.org/
"""

from __future__ import absolute_import

import os
import shutil
import tempfile
import warnings
import weakref

import cffi

__all__ = ['Environment', 'Database', 'Cursor', 'Transaction', 'connect',
           'Error']

_ffi = cffi.FFI()
_ffi.cdef('''
    typedef int mode_t;
    typedef ... MDB_env;
    typedef struct MDB_txn MDB_txn;
    typedef struct MDB_cursor MDB_cursor;
    typedef unsigned int MDB_dbi;
    enum MDB_cursor_op {
        MDB_FIRST,
        MDB_FIRST_DUP,
        MDB_GET_BOTH,
        MDB_GET_BOTH_RANGE,
        MDB_GET_CURRENT,
        MDB_GET_MULTIPLE,
        MDB_LAST,
        MDB_LAST_DUP,
        MDB_NEXT,
        MDB_NEXT_DUP,
        MDB_NEXT_MULTIPLE,
        MDB_NEXT_NODUP,
        MDB_PREV,
        MDB_PREV_DUP,
        MDB_PREV_NODUP,
        MDB_SET,
        MDB_SET_KEY,
        MDB_SET_RANGE,
        ...
    };
    typedef enum MDB_cursor_op MDB_cursor_op;

    struct MDB_val {
        size_t mv_size;
        void *mv_data;
        ...;
    };
    typedef struct MDB_val MDB_val;

    struct MDB_stat {
        unsigned int ms_psize;
        unsigned int ms_depth;
        size_t ms_branch_pages;
        size_t ms_leaf_pages;
        size_t ms_overflow_pages;
        size_t ms_entries;
        ...;
    };
    typedef struct MDB_stat MDB_stat;

    struct MDB_envinfo {
        void *me_mapaddr;
        size_t me_mapsize;
        size_t me_last_pgno;
        size_t me_last_txnid;
        unsigned int me_maxreaders;
        unsigned int me_numreaders;
        ...;
    };
    typedef struct MDB_envinfo MDB_envinfo;

    typedef int (*MDB_cmp_func)(const MDB_val *a, const MDB_val *b);
    typedef void (*MDB_rel_func)(MDB_val *item, void *oldptr, void *newptr,
                   void *relctx);

    char *mdb_strerror(int err);
    int mdb_env_create(MDB_env **env);
    int mdb_env_open(MDB_env *env, const char *path, unsigned int flags,
                     mode_t mode);
    int mdb_env_copy(MDB_env *env, const char *path);
    int mdb_env_stat(MDB_env *env, MDB_stat *stat);
    int mdb_env_info(MDB_env *env, MDB_envinfo *stat);
    int mdb_env_sync(MDB_env *env, int force);
    void mdb_env_close(MDB_env *env);
    int mdb_env_set_flags(MDB_env *env, unsigned int flags, int onoff);
    int mdb_env_get_flags(MDB_env *env, unsigned int *flags);
    int mdb_env_get_path(MDB_env *env, const char **path);
    int mdb_env_set_mapsize(MDB_env *env, size_t size);
    int mdb_env_set_maxreaders(MDB_env *env, unsigned int readers);
    int mdb_env_get_maxreaders(MDB_env *env, unsigned int *readers);
    int mdb_env_set_maxdbs(MDB_env *env, MDB_dbi dbs);
    int mdb_txn_begin(MDB_env *env, MDB_txn *parent, unsigned int flags,
                      MDB_txn **txn);
    int mdb_txn_commit(MDB_txn *txn);
    void mdb_txn_abort(MDB_txn *txn);
    int mdb_dbi_open(MDB_txn *txn, const char *name, unsigned int flags,
                     MDB_dbi *dbi);
    int mdb_stat(MDB_txn *txn, MDB_dbi dbi, MDB_stat *stat);
    void mdb_dbi_close(MDB_env *env, MDB_dbi dbi);
    int mdb_drop(MDB_txn *txn, MDB_dbi dbi, int del_);
    int mdb_get(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
    int mdb_cursor_open(MDB_txn *txn, MDB_dbi dbi, MDB_cursor **cursor);
    void mdb_cursor_close(MDB_cursor *cursor);
    int mdb_cursor_del(MDB_cursor *cursor, unsigned int flags);
    int mdb_cursor_count(MDB_cursor *cursor, size_t *countp);
    int mdb_cursor_get(MDB_cursor *cursor, MDB_val *key, MDB_val*data, int op);

    #define EINVAL ...
    #define MDB_APPEND ...
    #define MDB_CREATE ...
    #define MDB_DBS_FULL ...
    #define MDB_DUPSORT ...
    #define MDB_KEYEXIST ...
    #define MDB_MAPASYNC ...
    #define MDB_MAP_FULL ...
    #define MDB_NODUPDATA ...
    #define MDB_NOMETASYNC ...
    #define MDB_NOOVERWRITE ...
    #define MDB_NOSUBDIR ...
    #define MDB_NOSYNC ...
    #define MDB_NOTFOUND ...
    #define MDB_RDONLY ...
    #define MDB_READERS_FULL ...
    #define MDB_REVERSEKEY ...
    #define MDB_TXN_FULL ...

    // Helpers below inline MDB_vals. Avoids key alloc/dup on CPython, where
    // cffi will use PyString_AS_STRING when passed as an argument.
    static int pymdb_del(MDB_txn *txn, MDB_dbi dbi,
                         char *key_s, size_t keylen,
                         char *val_s, size_t vallen);
    static int pymdb_put(MDB_txn *txn, MDB_dbi dbi,
                         char *key_s, size_t keylen,
                         char *val_s, size_t vallen,
                         unsigned int flags);
    static int pymdb_get(MDB_txn *txn, MDB_dbi dbi,
                         char *key_s, size_t keylen,
                         MDB_val *val_out);
    static int pymdb_cursor_get(MDB_cursor *cursor,
                                char *key_s, size_t keylen,
                                MDB_val *key, MDB_val *data, int op);
    static int pymdb_cursor_put(MDB_cursor *cursor,
                                char *key_s, size_t keylen,
                                char *val_s, size_t vallen, int flags);
''')

_lib = _ffi.verify('''
    #include <sys/stat.h>
    #include "lmdb.h"

    // Helpers below inline MDB_vals. Avoids key alloc/dup on CPython, where
    // cffi will use PyString_AS_STRING when passed as an argument.
    static int pymdb_get(MDB_txn *txn, MDB_dbi dbi, char *key_s, size_t keylen,
                         MDB_val *val_out)
    {
        MDB_val key = {keylen, key_s};
        return mdb_get(txn, dbi, &key, val_out);
    }

    static int pymdb_put(MDB_txn *txn, MDB_dbi dbi, char *key_s, size_t keylen,
                         char *val_s, size_t vallen, unsigned int flags)
    {
        MDB_val key = {keylen, key_s};
        MDB_val val = {vallen, val_s};
        return mdb_put(txn, dbi, &key, &val, flags);
    }

    static int pymdb_del(MDB_txn *txn, MDB_dbi dbi, char *key_s, size_t keylen,
                         char *val_s, size_t vallen)
    {
        MDB_val key = {keylen, key_s};
        MDB_val val = {vallen, val_s};
        MDB_val *valptr;
        if(vallen == 0) {
            valptr = NULL;
        } else {
            valptr = &val;
        }
        return mdb_del(txn, dbi, &key, valptr);
    }

    static int pymdb_cursor_get(MDB_cursor *cursor, char *key_s, size_t keylen,
                                MDB_val *key, MDB_val *data, int op)
    {
        MDB_val tmpkey = {keylen, key_s};
        int rc = mdb_cursor_get(cursor, &tmpkey, data, op);
        if(rc == 0) {
            *key = tmpkey;
        }
        return rc;
    }

    static int pymdb_cursor_put(MDB_cursor *cursor, char *key_s, size_t keylen,
                                char *val_s, size_t vallen, int flags)
    {
        MDB_val tmpkey = {keylen, key_s};
        MDB_val tmpval = {vallen, val_s};
        return mdb_cursor_put(cursor, &tmpkey, &tmpval, flags);
    }
''',
    ext_package='lmdb',
    sources=['lib/mdb.c', 'lib/midl.c'],
    extra_compile_args=['-Wno-shorten-64-to-32', '-Ilib']
)

globals().update((k, getattr(_lib, k))
                 for k in dir(_lib) if k[:4] in ('mdb_', 'MDB_', 'pymd'))
EINVAL = _lib.EINVAL

class Error(Exception):
    """Raised when any MDB error occurs."""
    def hints(self):
        # This is wrapped in a function to allow mocking out cffi for
        # readthedocs.
        return {
            MDB_MAP_FULL:
                "Please use a larger Environment(map_size=) parameter",
            MDB_DBS_FULL:
                "Please use a larger Environment(max_dbs=) parameter",
            MDB_READERS_FULL:
                "Please use a larger Environment(max_readers=) parameter",
            MDB_TXN_FULL:
                "Please do less work within your transaction",
        }

    def __init__(self, what, code=0):
        self.what = what
        self.code = code
        self.reason = _ffi.string(mdb_strerror(code))
        msg = what
        if code:
            msg = '%s: %s' % (what, self.reason)
            hint = self.hints().get(code)
            if hint:
                msg += ' (%s)' % (hint,)
        Exception.__init__(self, msg)


def _kill_dependents(parent):
    """Notify all dependents of `parent` that `parent` is about to become
    invalid."""
    deps = parent._deps
    while deps:
        chid, chref = deps.popitem()
        child = chref()
        if child:
            child._invalidate()

class Some_LMDB_Resource_That_Was_Deleted_Or_Closed(object):
    """We need this because cffi on PyPy treats None as cffi.NULL, instead of
    throwing an exception it feeds MDB null pointers. We use a weird name to
    make exceptions more obvious."""
    def __nonzero__(self):
        return 0
    def __repr__(self):
        return "<This used a resource that was deleted or closed>"
_invalid = Some_LMDB_Resource_That_Was_Deleted_Or_Closed()

def _depend(parent, child):
    """Mark `child` as dependent on `parent`, so its `_invalidate()` method
    will be called before the resource associated with `parent` is
    destroyed."""
    parent._deps[id(child)] = weakref.ref(child)

def _undepend(parent, child):
    """Clean up `parent`'s dependency dict by removing `child` from it."""
    parent._deps.pop(id(child), None)

def _mvbuf(mv):
    """Convert a MDB_val cdata to a cffi buffer object."""
    return _ffi.buffer(mv.mv_data, mv.mv_size)

def _mvstr(mv):
    """Convert a MDB_val cdata to Python bytes."""
    return _ffi.buffer(mv.mv_data, mv.mv_size)[:]

def connect(path, **kwargs):
    """Shorthand for ``lmdb.Environment(path, **kwargs)``"""
    return Environment(path, **kwargs)


class Environment(object):
    """
    Structure for a database environment. An environment may contain multiple
    databases, all residing in the same shared-memory map and underlying disk
    file.

    To write to the environment a :py:class:`Transaction` must be created. One
    simultaneous write transaction is allowed, however there is no limit on the
    number of read transactions even when a write transaction exists. Due to
    this, write transactions should be kept as short as possible.

        `path`:
            Location of directory (if `subdir=True`) or file prefix to store
            the database.

        `map_size`:
            Maximum size database may grow to; used to size the memory mapping.
            If database grows larger than ``map_size``, an exception will be
            raised and the user must close and reopen :py:class:`Environment`.
            On 64-bit there is no penalty for making this huge (say 1TB). Must
            be <2GB on 32-bit.

            *Note*: **the default map size is set low to encourage a crash**,
            so users can figure out a good value before learning about this
            option too late.

        `subdir`:
            If ``True``, `path` refers to a subdirectory to store the data and
            lock files within, otherwise it refers to a filename prefix.

        `readonly`:
            If ``True``, disallow any write operations. Note the lock file is
            still modified.

        `metasync`:
            If ``False``, never explicitly flush metadata pages to disk. OS
            will flush at its disgression, or user can flush with
            :py:meth:`sync`.

        `sync`
            If ``False``, never explicitly flush data pages to disk. OS will
            flush at its disgression, or user can flush with :py:meth:`sync`.
            This optimization means a system crash can corrupt the database or
            lose the last transactions if buffers are not yet flushed to disk.

        `mode`:
            File creation mode.

        `create`:
            If ``False``, do not create the directory `path` if it is missing.

        `max_readers`:
            Slots to allocate in lock file for read threads; attempts to open
            the environment by more than this many clients simultaneously will
            fail. only meaningful for environments that aren't already open.

        `max_dbs`:
            Maximum number of databases available. If 0, assume environment
            will be used as a single database.
    """
    def __init__(self, path, map_size=10485760, subdir=True,
            readonly=False, metasync=True, sync=True, map_async=False,
            mode=0644, create=True, max_readers=126, max_dbs=0):
        envpp = _ffi.new('MDB_env **')

        rc = mdb_env_create(envpp)
        if rc:
            raise Error("Creating environment", rc)
        self._env = envpp[0]
        self._deps = {}

        rc = mdb_env_set_mapsize(self._env, map_size)
        if rc:
            raise Error("Setting map size", rc)

        rc = mdb_env_set_maxreaders(self._env, max_readers)
        if rc:
            raise Error("Setting max readers", rc)

        rc = mdb_env_set_maxdbs(self._env, max_dbs)
        if rc:
            raise Error("Setting max DBs", rc)

        if create and subdir and not os.path.exists(path):
            os.mkdir(path)

        flags = 0
        if not subdir:
            flags |= MDB_NOSUBDIR
        if readonly:
            flags |= MDB_RDONLY
        if not metasync:
            flags |= MDB_NOMETASYNC
        if not sync:
            flags |= MDB_NOSYNC
        if map_async:
            flags |= MDB_MAPASYNC

        rc = mdb_env_open(self._env, path, flags, mode)
        if rc:
            raise Error(path, rc)
        with self.begin() as txn:
            self._db = Database(self, txn)

    def close(self):
        if self._env:
            _kill_dependents(self)
            mdb_env_close(self._env)
            self._env = _invalid

    def __del__(self):
        self.close()

    def path(self):
        """Directory path or file name prefix where this environment is
        stored."""
        path = _ffi.new('char **')
        mdb_env_get_path(self._env, path)
        return _ffi.string(path[0])

    def max_readers(self):
        """Return the maximum number of client threads that may read this
        environment simultaneously."""
        readers = _ffi.new('unsigned int *')
        mdb_env_get_maxreaders(self._env, readers)
        return readers[0]

    def copy(self, path):
        """Make a consistent copy of the environment in the given destination
        directory.
        """
        rc = mdb_env_copy(self._env, path)
        if rc:
            raise Error("Copying environment", rc)

    def sync(self, force=False):
        """Flush the data buffers to disk.

        Data is always written to disk when ``Transaction.commit()`` is called,
        but the operating system may keep it buffered. MDB always flushes the
        OS buffers upon commit as well, unless the environment was opened with
        ``sync=False`` or ``metasync=False``.

        ``force``
            If non-zero, force a synchronous flush. Otherwise if the
            environment was opened with ``sync=False`` the flushes will be
            omitted, and with #MDB_MAPASYNC they will be asynchronous.
        """
        rc = mdb_env_sync(self._env, force)
        if rc:
            raise Error("Flushing", rc)

    def stat(self):
        """stat()

        Return some nice environment statistics as a dict:

        +--------------------+---------------------------------------+
        | ``psize``          | Size of a database page in bytes.     |
        +--------------------+---------------------------------------+
        | ``depth``          | Height of the B-tree.                 |
        +--------------------+---------------------------------------+
        | ``branch_pages``   | Number of internal (non-leaf) pages.  |
        +--------------------+---------------------------------------+
        | ``leaf_pages``     | Number of leaf pages.                 |
        +--------------------+---------------------------------------+
        | ``overflow_pages`` | Number of overflow pages.             |
        +--------------------+---------------------------------------+
        | ``entries``        | Number of data items.                 |
        +--------------------+---------------------------------------+
        """
        st = _ffi.new('MDB_stat *')
        rc = mdb_env_stat(self._env, st)
        if rc:
            raise Error("Getting environment statistics", rc)
        return {
            "psize": st.ms_psize,
            "depth": st.ms_depth,
            "branch_pages": st.ms_branch_pages,
            "leaf_pages": st.ms_leaf_pages,
            "overflow_pages": st.ms_overflow_pages,
            "entries": st.ms_entries
        }

    def info(self):
        """Return some nice environment information as a dict:

        +--------------------+---------------------------------------+
        | map_addr           | Address of database map in RAM.       |
        +--------------------+---------------------------------------+
        | map_size           | Size of database map in RAM.          |
        +--------------------+---------------------------------------+
        | last_pgno          | ID of last used page.                 |
        +--------------------+---------------------------------------+
        | last_txnid         | ID of last committed transaction.     |
        +--------------------+---------------------------------------+
        | max_readers        | Maximum number of threads.            |
        +--------------------+---------------------------------------+
        | num_readers        | Number of threads in use.             |
        +--------------------+---------------------------------------+
        """
        info = _ffi.new('MDB_envinfo *')
        rc = mdb_env_info(self._env, info)
        if rc:
            raise Error('Getting environment info', rc)
        return {
            "map_size": info.me_mapsize,
            "last_pgno": info.me_last_pgno,
            "last_txnid": info.me_last_txnid,
            "max_readers": info.me_maxreaders,
            "num_readers": info.me_numreaders
        }

    def open(self, **kwargs):
        """Create or open a database *inside a write transaction*. This cannot
        be called from within an existing write transaction. Parameters are as
        for :py:class:`Database` constructor.

        As a special case, the main database is always open."""
        if not kwargs.get('name'):
            return self._db
        with self.begin() as txn:
            return Database(self, txn, **kwargs)

    def begin(self, **kwargs):
        """Shortcut for ``lmdb.Transaction(self, **kwargs)``"""
        return Transaction(self, **kwargs)


class Database(object):
    """
    Get a reference to or create a database within an environment.

    The database handle may be discarded by calling :py:meth:`close`. A newly
    created database will not exist if the transaction that created it aborted,
    nor if another process deleted it. **The handle resides in the shared
    environment, it is not owned by the current transaction or process**. Only
    one thread should call this function; it is not mutex-protected in a
    read-only transaction.

    Preexisting transactions, other than the current transaction and any
    parents, must not use the new handle. Nor must their children.

        `env`:
            :py:class:`Environment` the database will be opened or created in.

        `name`:
            Database name. If ``None``, indicates the main database should be
            opened, otherwise indicates a sub-database should be created
            **inside the main database**. In other words, **a key representing
            the database will be visible in the main database, and the database
            name cannot conflict with any existing key**

        `txn`:
            Transaction used to create the database if it does not exist.

        `reverse_key`:
            If ``True``, keys are compared from right to left (e.g. DNS names).

        `dupsort`:
            Duplicate keys may be used in the database. (Or, from another
            perspective, keys may have multiple data items, stored in sorted
            order.) By default keys must be unique and may have only a single
            data item.

            **py-lmdb** does not yet fully support dupsort.

        `create`:
            If ``True``, create the database if it doesn't exist, otherwise
            raise an exception.
    """
    def __init__(self, env, txn, name=None, reverse_key=False,
                 dupsort=False, create=True):
        _depend(env, self)
        self.env = env
        self._deps = {}

        flags = 0
        if reverse_key:
            flags |= MDB_REVERSEKEY
        if dupsort:
            flags |= MDB_DUPSORT
        if create:
            flags |= MDB_CREATE
        dbipp = _ffi.new('MDB_dbi *')
        self._dbi = None
        rc = mdb_dbi_open(txn._txn, name or _ffi.NULL, flags, dbipp)
        if rc:
            raise Error("Opening database %r" % name, rc)
        self._dbi = dbipp[0]

    def _invalidate(self):
        pass

    def __del__(self):
        _undepend(self.env, self)

    def close(self):
        """Close the database handle.

        **Warning**: closing the handle closes it for all processes and threads
        with the database open.

        This call is not mutex protected. Handles should only be closed by a
        single thread, and only if no other threads are going to reference the
        database handle or one of its cursors any further. Do not close a
        handle if an existing transaction has modified its database.
        """
        if self._dbi:
            _kill_dependents(self)
            mdb_dbi_close(self.env._env, self._dbi)
            self._dbi = _invalid

    def drop(self, delete=True):
        """Delete all keys and optionally delete the database itself. Deleting
        the database causes it to become unavailable, and invalidates existing
        cursors.
        """
        if self._dbi:
            _kill_dependents(self)
            rc = mdb_drop(self.txn._txn, self._dbi, delete)
            if rc:
                raise Error('Dropping database', rc)


class Transaction(object):
    """
    A transaction handle.

    All operations require a transaction handle, transactions may be read-only
    or read-write. Transactions may not span threads; a transaction must only
    be used by a single thread. A thread may only have a single transaction.

    Cursors may not span transactions; each cursor must be opened and closed
    within a single transaction.

        `env`:
            Environment the transaction should be on.

        `parent`:
            ``None``, or a parent transaction (see lmdb.h).

        `readonly`:
            Read-only?

        `buffers`:
            If ``True``, indicates **py-lmdb** should not convert database
            values into Python strings, but instead return buffer objects. This
            setting applies to the :py:class:`Transaction` instance itself and
            any :py:class:`Cursors <Cursor>` created within the transaction.

            This feature significantly improves performance, since MDB has a
            totally zero-copy read design, but it requires care when
            manipulating the returned buffer objects. With small keys and
            values, the benefit of this facility is greatly diminished.
    """
    def __init__(self, env, parent=None, readonly=False, buffers=False):
        _depend(env, self)
        self.env = env # hold ref
        self._env = env._env
        self._key = _ffi.new('MDB_val *')
        self._val = _ffi.new('MDB_val *')
        self._to_py = _mvbuf if buffers else _mvstr
        self._deps = {}
        self.readonly = readonly
        flags = 0
        if readonly:
            flags |= MDB_RDONLY
        txnpp = _ffi.new('MDB_txn **')
        if parent:
            self._parent = parent
            parent_txn = parent._txn
            _depend(parent, self)
        else:
            parent_txn = _ffi.NULL
        rc = mdb_txn_begin(self._env, parent_txn, flags, txnpp)
        if rc:
            rdonly = 'read-only' if readonly else 'write'
            raise Error('Beginning %s transaction' % rdonly, rc)
        self._txn = txnpp[0]

    def _invalidate(self):
        self.abort()
        self._env = _invalid

    def __del__(self):
        self.abort()
        _undepend(self.env, self)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if exc_type:
            self.abort()
        else:
            self.commit()

    def commit(self):
        """Commit the pending transaction."""
        if self._txn:
            _kill_dependents(self)
            rc = mdb_txn_commit(self._txn)
            self._txn = _invalid
            if rc:
                raise Error("Committing transaction", rc)

    def abort(self):
        """Abort the pending transaction."""
        if self._txn:
            _kill_dependents(self)
            rc = mdb_txn_abort(self._txn)
            self._txn = _invalid
            if rc:
                raise Error("Aborting transaction", rc)

    def get(self, key, default=None, db=None):
        """Fetch the first value matching `key`, otherwise return `default`. A
        cursor must be used to fetch all values for a key in a `dupsort=True`
        database.
        """
        rc = pymdb_get(self._txn, (db or self.env._db)._dbi,
                       key, len(key), self._val)
        if rc:
            if rc == MDB_NOTFOUND:
                return default
            raise Error(repr(key), rc)
        return self._to_py(self._val)

    def put(self, key, value, dupdata=False, overwrite=True, append=False,
            db=None):
        """Store a record, returning ``True`` if it was written, or ``False``
        to indicate the key was already present and `override=False`.

            `key`:
                String key to store.

            `value`:
                String value to store.

            `dupdata`:
                If ``True`` and database was opened with `dupsort=True`, add
                pair as a duplicate if the given key already exists. Otherwise
                overwrite any existing matching key.

            `overwrite`:
                If ``False``, do not overwrite any existing matching key.

            `append`:
                If ``True``, append the pair to the end of the database without
                comparing its order first. Appending a key that is not greater
                than the highest existing key will cause corruption.
        """
        flags = 0
        if not dupdata:
            flags |= MDB_NODUPDATA
        if not overwrite:
            flags |= MDB_NOOVERWRITE
        if append:
            flags |= MDB_APPEND

        rc = pymdb_put(self._txn, (db or self.env._db)._dbi,
                       key, len(key), value, len(value), flags)
        if rc:
            if rc == MDB_KEYEXIST:
                return False
            raise Error("Setting key", rc)
        return True

    def delete(self, key, value='', db=None):
        """Delete a key from the database.

            `key`:
                The key to delete.

            value:
                If the database was opened with dupsort=True and value is not
                the empty string, then delete elements matching only this
                `(key, value)` pair, otherwise all values for key are deleted.

        Returns True if at least one key was deleted.
        """
        rc = pymdb_del(self._txn, (db or self.env._db)._dbi,
                       key, len(key), value, len(value))
        if rc:
            if rc == MDB_NOTFOUND:
                return False
            raise Error("Deleting key", rc)
        return True

    def cursor(self, db=None, **kwargs):
        """Shorthand for ``lmdb.Cursor(self, **kwargs)``"""
        return Cursor(db or self.env._db, self, **kwargs)


class Cursor(object):
    """
    Structure for navigating a database.

        `db`:
            :py:class:`Database` to navigate.

        `txn`:
            :py:class:`Transaction` to navigate.

    As a convenience, :py:meth:`Transaction.cursor` can be used to quickly
    return a cursor:

        ::

            >>> env = lmdb.connect()
            >>> child_db = lmdb.open(name='child_db')
            >>> with env.begin() as txn:
            ...     cursor = txn.cursor()           # Cursor on main database.
            ...     cursor2 = txn.cursor(child_db)  # Cursor on child database.

    Cursors start in an unpositioned state: if :py:meth:`forward` or
    :py:meth:`reverse` are used to create an iterator in this state, iteration
    proceeds from the first or last key respectively. Iterators directly track
    position using the cursor, meaning strange behavior will result when
    multiple iterators exist on the same cursor.

        ::

            >>> with env.begin() as txn:
            ...     for i, (key, value) in enumerate(txn.cursor().reverse()):
            ...         print '%dth last item is (%r, %r)' % (1 + i, key, value)

    Both :py:meth:`forward` and :py:meth:`reverse` accept `keys` and `values`
    arguments. If both are ``True``, then the value of :py:meth:`item` is
    yielded on each iteration. If only `keys` is ``True``, :py:meth:`key` is
    yielded, otherwise only :py:meth:`value` is yielded.

    Prior to iteration, a cursor can be positioned anywhere in the database:

        ::

            >>> with env.begin() as txn:
            ...     cursor = txn.cursor()
            ...     if not cursor.set_range('5'): # Position at first key >= '5'.
            ...         print 'Not found!'
            ...     else:
            ...         for key, value in cursor: # Iterate from first key >= '5'.
            ...             print key, value

    Iteration is not required to navigate, and sometimes results in ugly or
    inefficient code. In cases where the iteration order is not obvious, or is
    related to the data being read, use of :py:meth:`set_key`,
    :py:meth:`set_range`, :py:meth:`key`, :py:meth:`value`, and :py:meth:`item`
    are often preferable:

        ::

            >>> # Record the path from a child to the root of a tree.
            >>> path = ['child14123']
            >>> while path[-1] != 'root':
            ...     assert cursor.set_key(path[-1]), \\
            ...         'Tree is broken! Path: %s' % (path,)
            ...     path.append(cursor.value())

    """
    def __init__(self, db, txn):
        _depend(db, self)
        _depend(txn, self)
        self.db = db # hold ref
        self.txn = txn # hold ref
        self._dbi = db._dbi
        self._txn = txn._txn
        self._key = _ffi.new('MDB_val *')
        self._val = _ffi.new('MDB_val *')
        self._valid = False
        self._to_py = txn._to_py
        curpp = _ffi.new('MDB_cursor **')
        self._cur = None
        rc = mdb_cursor_open(self._txn, self._dbi, curpp)
        if rc:
            raise Error("Creating cursor", rc)
        self._cur = curpp[0]

    def _invalidate(self):
        if self._cur:
            mdb_cursor_close(self._cur)
        self._cur = _invalid
        self._dbi = _invalid
        self._txn = _invalid

    def __del__(self):
        if self._cur:
            mdb_cursor_close(self._cur)
        _undepend(self.db, self)
        _undepend(self.txn, self)

    def key(self):
        """Return the current key."""
        return self._to_py(self._key)

    def value(self):
        """Return the current value."""
        return self._to_py(self._val)

    def item(self):
        """Return the current `(key, value)` pair."""
        return self._to_py(self._key), self._to_py(self._val)

    def _iter(self, op, keys, values):
        if not values:
            get = self.key
        elif not keys:
            get = self.value
        else:
            get = self.item

        cur = self._cur
        key = self._key
        val = self._val
        while self._valid:
            yield get()
            rc = mdb_cursor_get(cur, key, val, op)
            self._valid = not rc
            if rc and rc != MDB_NOTFOUND:
                raise Error('during iteration', rc)

    def forward(self, keys=True, values=True):
        """Return a forward iterator that yields the current element before
        calling :py:meth:`next`, repeating until the end of the database is
        reached. As a convenience, :py:class:`Cursor` implements the iterator
        protocol by automatically returning a forward iterator when invoked:

            ::

                >>> # Equivalent:
                >>> it = iter(cursor)
                >>> it = cursor.forward(keys=True, values=True)
        """
        if not self._valid:
            self.first()
        return self._iter(MDB_NEXT, keys, values)
    __iter__ = forward

    def reverse(self, keys=True, values=True):
        """Return a reverse iterator that yields the current element before
        calling :py:meth:`prev`, until the start of the database is reached."""
        if not self._valid:
            self.last()
        return self._iter(MDB_PREV, keys, values)

    def _cursor_get(self, op):
        rc = mdb_cursor_get(self._cur, self._key, self._val, op)
        v = not rc
        if rc:
            self._key.mv_size = 0
            self._val.mv_size = 0
            if rc != MDB_NOTFOUND:
                if not (rc == EINVAL and op == MDB_GET_CURRENT):
                    raise Error("Advancing cursor", rc)
        self._valid = v
        return v

    def _cursor_get_key(self, op, k):
        rc = pymdb_cursor_get(self._cur, k, len(k), self._key, self._val, op)
        v = not rc
        if rc:
            self._key.mv_size = 0
            self._val.mv_size = 0
            if rc != MDB_NOTFOUND:
                if not (rc == EINVAL and op == MDB_GET_CURRENT):
                    raise Error("Advancing cursor", rc)
        self._valid = v
        return v

    def first(self):
        """Move to the first element, returning ``True`` on success or
        ``False`` if the database is empty."""
        return self._cursor_get(MDB_FIRST)

    def last(self):
        """Move to the last element, returning ``True`` on success or ``False``
        if the database is empty."""
        v = self._cursor_get(MDB_LAST)
        if v: # TODO: why is this necessary?
            return self._cursor_get(MDB_PREV)
        return v

    def prev(self):
        """Move to the previous element, returning ``True`` on success or
        ``False`` if there is no previous element."""
        return self._cursor_get(MDB_PREV)

    def next(self):
        """Move to the next element, returning ``True`` on success or ``False``
        if there is no next element."""
        return self._cursor_get(MDB_NEXT)

    def set_key(self, key):
        """Seek exactly to `key`, returning ``True`` on success or ``False`` if
        the exact key was not found.

        It is an error to :py:meth:`set_key` the empty string.
        """
        return self._cursor_get_key(MDB_SET_KEY, key)

    def set_range(self, key):
        """Seek to the first key greater than or equal `key`, returning
        ``True`` on success, or ``False`` to indicate key was past end of
        database.

        Behaves like :py:meth:`first` if `key` is the empty string.
        """
        if not key: # TODO: set_range() throws INVAL on an empty store, whereas
                    # set_key() returns NOTFOUND
            return self.first()
        return self._cursor_get_key(MDB_SET_RANGE, key)

    def delete(self):
        """Delete the current element and move to the next element, returning
        ``True`` on success or ``False`` if the database was empty."""
        v = self._valid
        if v:
            rc = mdb_cursor_del(self._cur, 0)
            if rc:
                raise Error("Deleting current key", rc)
            self._cursor_get(MDB_GET_CURRENT)
            v = rc == 0
        return v

    def count(self):
        """Return the number of duplicates for the current key. This is only
        meaningful for databases that have `dupdata=True`."""
        countp = _ffi.new('size_t *')
        rc = mdb_cursor_count(self._cur, countp)
        if rc:
            raise Error("Getting duplicate count", rc)
        return countp[0]

    def put(self, key, val, dupdata=False, overwrite=True, append=False):
        """Store a record, returning ``True`` if it was written, or ``False``
        to indicate the key was already present and `override=False`. On
        success, the cursor is positioned on the key.

            `key`:
                String key to store.

            `val`:
                String value to store.

            `dupdata`:
                If ``True`` and database was opened with `dupsort=True`, add
                pair as a duplicate if the given key already exists. Otherwise
                overwrite any existing matching key.

            `overwrite`:
                If ``False``, do not overwrite any existing matching key.

            `append`:
                If ``True``, append the pair to the end of the database without
                comparing its order first. Appending a key that is not greater
                than the highest existing key will cause corruption.
        """
        flags = 0
        if not dupdata:
            flags |= MDB_NODUPDATA
        if not overwrite:
            flags |= MDB_NOOVERWRITE
        if append:
            flags |= MDB_APPEND

        rc = pymdb_cursor_put(self._cur, key, len(key), val, len(val), flags)
        if rc:
            if rc == MDB_KEYEXIST:
                return False
            raise Error("Setting key", rc)
        self._cursor_get(MDB_GET_CURRENT)
        return True
