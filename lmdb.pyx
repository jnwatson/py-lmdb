# Copyright 2013 The Python-lmdb authors, all rights reserved.
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

import os
import warnings

cdef extern from "sys/stat.h":
    ctypedef int mode_t

cdef extern from "lmdb.h":
    ctypedef struct MDB_env
    ctypedef struct MDB_txn
    ctypedef struct MDB_cursor
    ctypedef unsigned int MDB_dbi
    ctypedef enum MDB_cursor_op:
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

    ctypedef int (*MDB_cmp_func)(const MDB_val *a, const MDB_val *b)
    ctypedef void (*MDB_rel_func)(MDB_val *item, void *oldptr, void *newptr,
                   void *relctx)

    struct MDB_val:
        size_t mv_size
        void *mv_data

    struct MDB_stat:
        unsigned int ms_psize
        unsigned int ms_depth
        size_t ms_branch_pages
        size_t ms_leaf_pages
        size_t ms_overflow_pages
        size_t ms_entries

    struct MDB_envinfo:
        void *me_mapaddr
        size_t me_mapsize
        size_t me_last_pgno
        size_t me_last_txnid
        unsigned int me_maxreaders
        unsigned int me_numreaders

    int MDB_APPEND
    int MDB_APPENDDUP
    int MDB_CORRUPTED
    int MDB_CREATE
    int MDB_CURRENT
    int MDB_CURSOR_FULL
    int MDB_DBS_FULL
    int MDB_DUPFIXED
    int MDB_DUPSORT
    int MDB_FIXEDMAP
    int MDB_INTEGERDUP
    int MDB_INTEGERKEY
    int MDB_INVALID
    int MDB_KEYEXIST
    int MDB_MAPASYNC
    int MDB_MAP_FULL
    int MDB_MULTIPLE
    int MDB_NODUPDATA
    int MDB_NOMETASYNC
    int MDB_NOOVERWRITE
    int MDB_NOSUBDIR
    int MDB_NOSYNC
    int MDB_NOTFOUND
    int MDB_PAGE_FULL
    int MDB_PAGE_NOTFOUND
    int MDB_PANIC
    int MDB_RDONLY
    int MDB_READERS_FULL
    int MDB_RESERVE
    int MDB_REVERSEDUP
    int MDB_REVERSEKEY
    int MDB_SUCCESS
    int MDB_TLS_FULL
    int MDB_TXN_FULL
    int MDB_VERINT(a,b,c)
    int MDB_VERSION_DATE
    int MDB_VERSION_MAJOR
    int MDB_VERSION_MINOR
    int MDB_VERSION_MISMATCH
    int MDB_VERSION_PATCH
    int MDB_WRITEMAP

    char *mdb_version(int *major, int *minor, int *patch)
    char *mdb_strerror(int err)
    int mdb_env_create(MDB_env **env)
    int mdb_env_open(MDB_env *env, const char *path, unsigned int flags, mode_t mode)
    int mdb_env_copy(MDB_env *env, const char *path)
    int mdb_env_stat(MDB_env *env, MDB_stat *stat)
    int mdb_env_info(MDB_env *env, MDB_envinfo *stat)
    int mdb_env_sync(MDB_env *env, int force)
    void mdb_env_close(MDB_env *env)
    int mdb_env_set_flags(MDB_env *env, unsigned int flags, int onoff)
    int mdb_env_get_flags(MDB_env *env, unsigned int *flags)
    int mdb_env_get_path(MDB_env *env, const char **path)
    int mdb_env_set_mapsize(MDB_env *env, size_t size)
    int mdb_env_set_maxreaders(MDB_env *env, unsigned int readers)
    int mdb_env_get_maxreaders(MDB_env *env, unsigned int *readers)
    int mdb_env_set_maxdbs(MDB_env *env, MDB_dbi dbs)
    int mdb_txn_begin(MDB_env *env, MDB_txn *parent, unsigned int flags, MDB_txn **txn)
    int mdb_txn_commit(MDB_txn *txn)
    void mdb_txn_abort(MDB_txn *txn)
    void mdb_txn_reset(MDB_txn *txn)
    int mdb_txn_renew(MDB_txn *txn)

    int mdb_dbi_open(MDB_txn *txn, const char *name, unsigned int flags, MDB_dbi *dbi)
    int mdb_stat(MDB_txn *txn, MDB_dbi dbi, MDB_stat *stat)
    void mdb_dbi_close(MDB_env *env, MDB_dbi dbi)
    int mdb_drop(MDB_txn *txn, MDB_dbi dbi, int del_)
    int mdb_set_compare(MDB_txn *txn, MDB_dbi dbi, MDB_cmp_func *cmp)
    int mdb_set_dupsort(MDB_txn *txn, MDB_dbi dbi, MDB_cmp_func *cmp)
    int mdb_set_relfunc(MDB_txn *txn, MDB_dbi dbi, MDB_rel_func *rel)
    int mdb_set_relctx(MDB_txn *txn, MDB_dbi dbi, void *ctx)
    int mdb_get(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data)
    int mdb_put(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data,
                    unsigned int flags)
    int mdb_del(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data)
    int mdb_cursor_open(MDB_txn *txn, MDB_dbi dbi, MDB_cursor **cursor)
    void mdb_cursor_close(MDB_cursor *cursor)
    int mdb_cursor_renew(MDB_txn *txn, MDB_cursor *cursor)
    MDB_txn *mdb_cursor_txn(MDB_cursor *cursor)
    MDB_dbi mdb_cursor_dbi(MDB_cursor *cursor)
    int mdb_cursor_get(MDB_cursor *cursor, MDB_val *key, MDB_val *data,
                    MDB_cursor_op op)
    int mdb_cursor_put(MDB_cursor *cursor, MDB_val *key, MDB_val *data,
                    unsigned int flags)
    int mdb_cursor_del(MDB_cursor *cursor, unsigned int flags)
    int mdb_cursor_count(MDB_cursor *cursor, size_t *countp)
    int mdb_cmp(MDB_txn *txn, MDB_dbi dbi, const MDB_val *a, const MDB_val *b)
    int mdb_dcmp(MDB_txn *txn, MDB_dbi dbi, const MDB_val *a, const MDB_val *b)


class Error(Exception):
    """Raised when any MDB error occurs."""

cdef _throw(const char *what, int rc):
    if rc:
        raise Error(what + ": " + mdb_strerror(rc))


def connect(path, **kwargs):
    """connect(path, **kwargs)

    Shorthand for ``lmdb.Environment(path, **kwargs)``
    """
    return Environment(path, **kwargs)


cdef class Environment:
    """
    Structure for a database environment.

    A DB environment supports multiple databases, all residing in the same
    shared-memory map.
    """
    cdef MDB_env *env_

    def __init__(self, const char *path, size_t map_size=10485760,
            subdir=True, readonly=False, metasync=True,
            sync=True, map_async=False, mode_t mode=0644,
            create=True, int max_readers=126, int max_dbs=0):
        """\\__init__(self, path, map_size=(64Mb), subdir=True, readonly=False,
        metasync=True, sync=True, mode=0644, create=True, max_readers=126,
        max_dbs=0)

        Create and open an environment.

        ``path``
            Location of directory (if ``subdir=True``) or file prefix to store
            the database.
        ``map_size``
            Maximum size database may grow to; used to size the memory mapping.
            If database grows larger than ``map_size``, exception will be
            raised and Environment must be recreated. On 64-bit there is no
            penalty for making this huge (say 1TB). Must be <2GB on 32-bit.
        ``subdir``
            If True, `path` refers to a subdirectory to store the data and lock
            files within, otherwise it refers to a filename prefix.
        ``readonly``
            If True, disallow any write operations. Note the lock file is still
            modified.
        ``metasync``
            If False, never explicitly flush metadata pages to disk. OS will
            flush at its disgression, or user can flush with
            Environment.sync().
        ``sync``
            If False, never explicitly fluh data pages to disk. OS will flush
            at its disgression, or user can flush with ``Environment.sync()``.
            This optimization means a system crash can corrupt the database or
            lose the last transactions if buffers are not yet flushed to disk.
        ``mode``
            File creation mode.
        ``create``
            If False, do not create the directory `path` if it is missing.
        ``max_readers``
            Slots to allocate in lock file for read threads; attempts to open
            the environment by more than this many clients simultaneously will
            fail. only meaningful for environments that aren't already open.
        ``max_dbs``
            Maximum number of databases available. If 0, assume environment
            will be used as a single database.
        """
        _throw("Creating environment", mdb_env_create(&self.env_))
        _throw("Setting map size", mdb_env_set_mapsize(self.env_, map_size))
        _throw("Setting max readers",
               mdb_env_set_maxreaders(self.env_, max_readers))
        _throw("Setting max DBs", mdb_env_set_maxdbs(self.env_, max_dbs))

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

        if create and subdir and not os.path.exists(path):
            os.mkdir(path)
        _throw(path, mdb_env_open(self.env_, path, flags, mode))

    def __dealloc__(self):
        mdb_env_close(self.env_)

    property path:
        """Directory path or file name prefix where this environment is
        stored."""
        def __get__(self):
            cdef const char *path
            mdb_env_get_path(self.env_, &path)
            return path

    property max_readers:
        """Maximum number of client threads that may read this environment
        simultaneously."""
        def __get__(self):
            cdef unsigned int readers
            mdb_env_get_maxreaders(self.env_, &readers)
            return readers

    def sync(self, force=False):
        """sync(force=False)

        Flush the data buffers to disk.

        Data is always written to disk when ``Transaction.commit()`` is called,
        but the operating system may keep it buffered. MDB always flushes the
        OS buffers upon commit as well, unless the environment was opened with
        ``sync=False`` or ``metasync=False``.
     
        ``force``
            If non-zero, force a synchronous flush. Otherwise if the
            environment was opened with ``sync=False`` the flushes will be
            omitted, and with #MDB_MAPASYNC they will be asynchronous.
        """
        _throw("Flushing", mdb_env_sync(self.env_, force))

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
        cdef MDB_stat st
        _throw("Getting environment statistics",
               mdb_env_stat(self.env_, &st))
        return {
            "psize": st.ms_psize,
            "depth": st.ms_depth,
            "branch_pages": st.ms_branch_pages,
            "leaf_pages": st.ms_leaf_pages,
            "overflow_pages": st.ms_overflow_pages,
            "entries": st.ms_entries
        }

    def begin(self, **kwargs):
        """begin(parent=None, readonly=False)

        Shortcut for ``lmdb.Transaction(self, **kwargs)``
        """
        return Transaction(self, **kwargs)


cdef class Transaction:
    """
    A transaction handle.

    All database operations require a transaction handle. Transactions may be
    read-only or read-write.
    """
    cdef Environment env
    cdef MDB_txn *txn_
    cdef int running
    cdef int readonly

    cdef _throw(self):
        """Raise an exception if this transaction is complete."""
        if not self.running:
            raise Error("transaction already aborted or committed")

    def __init__(self, Environment env not None, Transaction parent=None,
            readonly=False):
        """Transaction(env, parent=None, readonly=False)

        Start a new transaction.
            env: Environment the transaction should be on.
            parent: None, or a parent transaction (see lmdb.h).
            readonly: Read-only?
        """
        self.env = env
        self.readonly = readonly
        cdef const char *what
        if readonly:
            what = "Beginning read-only transaction"
            flags = MDB_RDONLY
        else:
            what = "Beginning write transaction"
            flags = 0
        cdef MDB_txn *parent_txn = parent.txn_ if parent else NULL
        _throw(what, mdb_txn_begin(env.env_, parent_txn, flags, &self.txn_))
        self.running = 1

    def __dealloc__(self):
        if self.running:
            self.running = 0
            mdb_txn_abort(self.txn_)

    def open(self, **kwargs):
        """open(**kwargs)

        Shorthand for ``lmdb.Database(self, **kwargs)``
        """
        return Database(self, **kwargs)

    def commit(self):
        """commit()

        Commit the pending transaction.
        """
        self._throw()
        try:
            _throw("Committing transaction", mdb_txn_commit(self.txn_))
        finally:
            self.running = 0

    def abort(self):
        """abort()

        Abort the pending transaction.
        """
        self._throw()
        self.running = 0
        mdb_txn_abort(self.txn_)


cdef class Database:
    """
    Handle for an individual database in the DB environment.
    """
    cdef Transaction txn
    cdef MDB_dbi dbi_
    cdef int dropped

    def __init__(self, Transaction txn not None, name=None, reverse_key=False,
            dupsort=False, create=True):
        """Database(txn, name=None, reverse_key=False, dupsort=False, create=True)

        Get a reference to or create a database within an environment.
            txn: Transaction.
            reverse_key: If True, keys are compared from right to left (e.g. DNS
                names)
            dupsort: Duplicate keys may be used in the database. (Or, from
                another perspective, keys may have multiple data items, stored
                in sorted order.) By default keys must be unique and may have
                only a single data item.
            create: If True, create the database if it doesn't exist, otherwise
                raise an exception.

        The Python module does not yet fully support dupsort.
        """
        self.txn = txn
        self.dropped = 0
        cdef const char *c_name = NULL
        if name:
            c_name = name
        cdef int flags = 0
        if reverse_key:
            flags |= MDB_REVERSEKEY
        if dupsort:
            flags |= MDB_DUPSORT
        if create:
            flags |= MDB_CREATE
        _throw("Opening database",
               mdb_dbi_open(self.txn.txn_, c_name, flags, &self.dbi_))

    def __dealloc__(self):
        if self.txn.running and not self.dropped:
            mdb_dbi_close(self.txn.env.env_, self.dbi_)

    cdef _throw(self):
        if self.dropped:
            raise Error("database was dropped.")
        self.txn._throw()

    def drop(self, delete=True):
        """drop(delete=True)

        Delete all keys and optionally delete the database itself. Deleting the
        database causes it to become unavailable, and invalidates existing
        cursors.
        """
        self._throw()
        mdb_drop(self.txn.txn_, self.dbi_, delete)
        if delete:
            self.dropped = 1

    def get(self, key, default=None):
        """get(key, default=None)

        Fetch the first value matching `key`, otherwise return `default`. A
        cursor must be used to fetch all values for a key in a `dupsort=True`
        database.
        """
        self._throw()
        cdef MDB_val key_val
        cdef MDB_val value_val
        key_val.mv_size = len(key)
        key_val.mv_data = <char *>key
        cdef int rc = mdb_get(self.txn.txn_, self.dbi_, &key_val, &value_val)
        if rc:
            if rc == MDB_NOTFOUND:
                return default
            _throw("Getting key", rc)
        return <bytes> (<char *>value_val.mv_data)[:value_val.mv_size]

    def put(self, key, value, dupdata=False, overwrite=True, append=False):
        """put(key, value, dupdata=False, overwrite=True, append=False)

        Store key/value pairs in the database.
            key: String key to store.
            value: String value to store.
            dupdata: If True and database was opened with dupsort=True, add
                pair as a duplicate if the given key already exists. Otherwise
                overwrite any existing matching key.
            overwrite: If False, do not overwrite any existing matching key.
            append: If True, append the pair to the end of the database without
                comparing its order first. Appending a key that is not greater
                than the highest existing key will cause corruption.

        Returns True if the pair was written or False to indicate the key was
        already present and override=False.
        """
        self._throw()
        cdef MDB_val key_val
        cdef MDB_val value_val

        key_val.mv_size = len(key)
        key_val.mv_data = <char *>key
        value_val.mv_size = len(value)
        value_val.mv_data = <char *>value

        cdef int flags = 0
        if not dupdata:
            flags |= MDB_NODUPDATA
        if not overwrite:
            flags |= MDB_NOOVERWRITE
        if append:
            flags |= MDB_APPEND

        cdef int rc = mdb_put(self.txn.txn_, self.dbi_,
                              &key_val, &value_val, flags)
        if rc == MDB_KEYEXIST:
            return False
        _throw("Setting key", rc)
        return True

    def delete(self, key, value=None):
        """delete(key, value=None)

        Delete a key from the database.
            key: The key to delete.
            value: If the database was opened with dupsort=True and value is
                not None, then delete elements matching only this (key, value)
                pair, otherwise all values for key are deleted.

        Returns True if at least one key was deleted.
        """
        self._throw()
        cdef MDB_val key_val
        cdef MDB_val value_val
        cdef MDB_val *val_ptr
        key_val.mv_size = len(key)
        key_val.mv_data = <char *>key
        if value is None:
            val_ptr = NULL
        else:
            value_val.mv_size = len(value)
            value_val.mv_data = <char *>value
            val_ptr = &value_val

        cdef int rc = mdb_del(self.txn.txn_, self.dbi_, &key_val, val_ptr)
        if rc:
            if rc == MDB_NOTFOUND:
                return False
            _throw("Deleting key", rc)
        return True

    def cursor(self, **kwargs):
        """cursor(**kwargs)

        Shorthand for lmdb.Cursor(self, **kwargs)
        """
        return Cursor(self, **kwargs)


cdef class Cursor:
    """
    Structure for navigating through a database.

    Cursors by default are positioned on the first element in the database.
    """
    cdef Database db
    cdef Transaction txn
    cdef MDB_cursor *cursor_
    cdef MDB_val key_
    cdef MDB_val val_

    cdef int do_keys_
    cdef int do_values_

    def __init__(self, Database db not None, keys=True, values=True):
        """Cursor(db, keys=True, values=True)

        Create a new cursor. If both `keys` and `values` are True, the Python
        iterator protocol applied to this object will yield ``(key, value)``
        tuples, if only `value` is True then a sequence of values will be
        generated, otherwise a sequence of keys will be generated.
        """
        self.db  = db
        self.txn = db.txn
        self.txn._throw()
        self.do_keys_ = keys
        self.do_values_ = values
        # Sentinel indicating iteration has not started.
        self.key_.mv_data = NULL
        self.key_.mv_size = 0
        self.val_.mv_data = NULL
        self.val_.mv_size = 0
        _throw("Creating cursor",
               mdb_cursor_open(self.txn.txn_, db.dbi_, &self.cursor_))

    def __dealloc__(self):
        if self.txn.running:
            mdb_cursor_close(self.cursor_)

    cpdef _throw_stop(self, const char *what, int rc):
        if rc == MDB_NOTFOUND:
            raise StopIteration
        _throw(what, rc)

    cpdef _cursor_get(self, MDB_cursor_op op):
        self.db._throw()
        rc = mdb_cursor_get(self.cursor_, &self.key_, &self.val_, op)
        self._throw_stop("Advancing cursor", rc)

    property key:
        """The current key. Raises an exception if the cursor is invalid."""
        def __get__(self):
            self.db._throw()
            return <bytes> (<char *>self.key_.mv_data)[:self.key_.mv_size]

    property value:
        """Returns the current value. Raises an exception if the cursor is
        invalid."""
        def __get__(self):
            self.db._throw()
            return <bytes> (<char *>self.val_.mv_data)[:self.val_.mv_size]

    cpdef _itervalue(self):
        # Ordered such that keys=False,values=False still returns key anyway.
        if self.do_values_:
            if self.do_keys_:
                return self.key, self.value
            return self.value
        else:
            return self.key

    def __iter__(self):
        return self

    cpdef first(self):
        """first()
        
        Reposition the cursor on the first element in the database."""
        self._cursor_get(MDB_FIRST)
        return self._itervalue()

    cpdef last(self):
        """last()
        
        Reposition the cursor on the last element in the database."""
        self._cursor_get(MDB_LAST)
        return self._itervalue()

    cpdef prev(self):
        """prev()
        
        Reposition the cursor on the previous element in the database."""
        self._cursor_get(MDB_PREV)
        return self._itervalue()

    cpdef next(self):
        """next()
        
        Reposition the cursor on the next element in the database."""
        self._cursor_get(MDB_NEXT)
        return self._itervalue()

    def __next__(self):
        self._cursor_get(MDB_NEXT if self.key_.mv_data else MDB_FIRST)
        return self._itervalue()

    cpdef delete(self):
        """delete()

        Delete the current key, and position the cursor on the next element in
        the database, if any.
        """
        self._throw_stop("Deleting current key",
                         mdb_cursor_del(self.cursor_, 0))
        self._cursor_get(MDB_GET_CURRENT)

    property count:
        """Count of duplicates for the current key. Raises an exception if the
        cursor is invalid."""
        def __get__(self):
            self.db._throw()
            cdef size_t count
            _throw("Getting duplicate count",
                   mdb_cursor_count(self.cursor_, &count))
            return count
