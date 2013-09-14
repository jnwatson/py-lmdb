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
CPython/cffi wrapper for OpenLDAP's "Lightning" MDB database.

Please see http://lmdb.readthedocs.org/
"""

from __future__ import absolute_import

import os
import shutil
import tempfile
import threading
import warnings
import weakref

import lmdb
try:
    from lmdb import _config
except ImportError:
    _config = None


__all__ = ['Environment', 'Cursor', 'Transaction', 'open',
           'enable_drop_gil', 'version']
__all__ += ['Error', 'KeyExistsError', 'NotFoundError', 'PageNotFoundError',
            'CorruptedError', 'PanicError', 'VersionMismatchError',
            'InvalidError', 'MapFullError', 'DbsFullError', 'ReadersFullError',
            'TlsFullError', 'TxnFullError', 'CursorFullError', 'PageFullError',
            'MapResizedError', 'IncompatibleError', 'BadRslotError',
            'BadTxnError', 'BadValsizeError', 'ReadonlyError',
            'InvalidParameterError', 'LockError', 'MemoryError', 'DiskError']

# Used to track context across cffi callbcks.
_callbacks = threading.local()

_CFFI_CDEF = '''
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
    int mdb_env_copyfd(MDB_env *env, int fd);
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
    int mdb_drop(MDB_txn *txn, MDB_dbi dbi, int del_);
    int mdb_get(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
    int mdb_cursor_open(MDB_txn *txn, MDB_dbi dbi, MDB_cursor **cursor);
    void mdb_cursor_close(MDB_cursor *cursor);
    int mdb_cursor_del(MDB_cursor *cursor, unsigned int flags);
    int mdb_cursor_count(MDB_cursor *cursor, size_t *countp);
    int mdb_cursor_get(MDB_cursor *cursor, MDB_val *key, MDB_val*data, int op);

    typedef int (MDB_msg_func)(const char *msg, void *ctx);
    int mdb_reader_list(MDB_env *env, MDB_msg_func *func, void *ctx);
    int mdb_reader_check(MDB_env *env, int *dead);

    #define MDB_VERSION_MAJOR ...
    #define MDB_VERSION_MINOR ...
    #define MDB_VERSION_PATCH ...

    #define EACCES ...
    #define EAGAIN ...
    #define EINVAL ...
    #define ENOMEM ...
    #define ENOSPC ...
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
    #define MDB_WRITEMAP ...
    #define MDB_NOTLS ...

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
'''

_CFFI_VERIFY = '''
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
'''

if not lmdb._reading_docs():
    import cffi

    # Try to use distutils-bundled cffi configuration to avoid a recompile and
    # potential compile errors during first module import.
    _config_vars = _config.CONFIG if _config else {
        'extra_sources': ['lib/mdb.c', 'lib/midl.c'],
        'extra_include_dirs': ['lib'],
        'extra_library_dirs': [],
        'libraries': []
    }

    _ffi = cffi.FFI()
    _ffi.cdef(_CFFI_CDEF)
    _lib = _ffi.verify(_CFFI_VERIFY,
        modulename='lmdb_cffi',
        ext_package='lmdb',
        sources=_config_vars['extra_sources'],
        extra_compile_args=['-Wno-shorten-64-to-32'],
        include_dirs=_config_vars['extra_include_dirs'],
        libraries=_config_vars['libraries'],
        library_dirs=_config_vars['extra_library_dirs'])

    globals().update((k, getattr(_lib, k))
                     for k in dir(_lib) if k[:4] in ('mdb_', 'MDB_', 'pymd'))
    EACCES = _lib.EACCES
    EAGAIN = _lib.EAGAIN
    EINVAL = _lib.EINVAL
    ENOMEM = _lib.ENOMEM
    ENOSPC = _lib.ENOSPC

    @_ffi.callback("int(char *, void *)")
    def _msg_func(s, _):
        """mdb_msg_func() callback. Appends `s` to _callbacks.msg_func list.
        """
        _callbacks.msg_func.append(_ffi.string(s))
        return 0

class Error(Exception):
    """Raised when an LMDB-related error occurs, and no more specific
    :py:class:`lmdb.Error` subclass exists."""
    def __init__(self, what, code=0):
        self.what = what
        self.code = code
        self.reason = _ffi.string(mdb_strerror(code))
        msg = what
        if code:
            msg = '%s: %s' % (what, self.reason)
            hint = getattr(self, 'MDB_HINT', None)
            if hint:
                msg += ' (%s)' % (hint,)
        Exception.__init__(self, msg)

class KeyExistsError(Error):
    """Key/data pair already exists."""
    MDB_NAME = 'MDB_KEYEXIST'

class NotFoundError(Error):
    """No matching key/data pair found."""
    MDB_NAME = 'MDB_NOTFOUND'

class PageNotFoundError(Error):
    """Request page not found."""
    MDB_NAME = 'MDB_PAGE_NOTFOUND'

class CorruptedError(Error):
    """Located page was of the wrong type."""
    MDB_NAME = 'MDB_CORRUPTED'

class PanicError(Error):
    """Update of meta page failed."""
    MDB_NAME = 'MDB_PANIC'

class VersionMismatchError(Error):
    """Database environment version mismatch."""
    MDB_NAME = 'MDB_VERSION_MISMATCH'

class InvalidError(Error):
    """File is not an MDB file."""
    MDB_NAME = 'MDB_INVALID'

class MapFullError(Error):
    """Environment map_size= limit reached."""
    MDB_NAME = 'MDB_MAP_FULL'
    MDB_HINT = 'Please use a larger Environment(map_size=) parameter'

class DbsFullError(Error):
    """Environment max_dbs= limit reached."""
    MDB_NAME = 'MDB_DBS_FULL'
    MDB_HINT = 'Please use a larger Environment(max_dbs=) parameter'

class ReadersFullError(Error):
    """Environment max_readers= limit reached."""
    MDB_NAME = 'MDB_READERS_FULL'
    MDB_HINT = 'Please use a larger Environment(max_readers=) parameter'

class TlsFullError(Error):
    """Thread-local storage keys full - too many environments open."""
    MDB_NAME = 'MDB_TLS_FULL'

class TxnFullError(Error):
    """Transaciton has too many dirty pages - transaction too big."""
    MDB_NAME = 'MDB_TXN_FULL'
    MDB_HINT = 'Please do less work within your transaction'

class CursorFullError(Error):
    """Internal error - cursor stack limit reached."""
    MDB_NAME = 'MDB_CURSOR_FULL'

class PageFullError(Error):
    """Internal error - page has no more space."""
    MDB_NAME = 'MDB_PAGE_FULL'

class MapResizedError(Error):
    """Database contents grew beyond environment map_size=."""
    MDB_NAME = 'MDB_MAP_RESIZED'

class IncompatibleError(Error):
    """Operation and DB incompatible, or DB flags changed."""
    MDB_NAME = 'MDB_INCOMPATIBLE'

class BadRslotError(Error):
    """Invalid reuse of reader locktable slot."""
    MDB_NAME = 'MDB_BAD_RSLOT'

class BadTxnError(Error):
    """Transaction cannot recover - it must be aborted."""
    MDB_NAME = 'MDB_BAD_TXN'

class BadValsizeError(Error):
    """Too big key/data, key is empty, or wrong DUPFIXED size."""
    MDB_NAME = 'MDB_BAD_VALSIZE'

class ReadonlyError(Error):
    """An attempt was made to modify a read-only database."""
    MDB_NAME = 'EACCES'

class InvalidParameterError(Error):
    """An invalid parameter was specified."""
    MDB_NAME = 'EINVAL'

class LockError(Error):
    """The environment was locked by another process."""
    MDB_NAME = 'EAGAIN'

class MemoryError(Error):
    """Out of memory."""
    MDB_NAME = 'ENOMEM'

class DiskError(Error):
    """No more disk space."""
    MDB_NAME = 'ENOSPC'

# Prepare _error_map, a mapping of integer MDB_ERROR_CODE to exception class.
_error_map = {}
for obj in globals().values():
    if getattr(obj, '__name__', '').endswith('Error'):
        code = globals().get(getattr(obj, 'MDB_NAME', None))
        _error_map[code] = obj
del obj, code

def _error(what, rc):
    """Lookup and instantiate the correct exception class for the error code
    `rc`, using :py:class:`Error` if no better class exists."""
    return _error_map.get(rc, Error)(what, rc)

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

def enable_drop_gil():
    """
    Arrange for the global interpreter lock to be released during database IO.
    This flag is ignored and always assumed to be ``True`` on cffi. Note this
    can only be set once per process.

    Continually dropping and reacquiring the GIL may incur unnecessary overhead
    in single-threaded programs. Since Python intra-process concurrency is
    already limited, and LMDB supports inter-process access, programs using
    LMDB will achieve better throughput by forking rather than using threads.

    *Caution:* this function should be invoked before any threads are created.
    """

def version():
    """
    Return a tuple of integers `(major, minor, patch)` describing the LMDB
    library version that the binding is linked against. The version of the
    binding itself is available from ``lmdb.__version__``.
    """
    return (MDB_VERSION_MAJOR, MDB_VERSION_MINOR, MDB_VERSION_PATCH)


class Environment(object):
    """
    Structure for a database environment. An environment may contain multiple
    databases, all residing in the same shared-memory map and underlying disk
    file.

    To write to the environment a :py:class:`Transaction` must be created. One
    simultaneous write transaction is allowed, however there is no limit on the
    number of read transactions even when a write transaction exists. Due to
    this, write transactions should be kept as short as possible.

    Equivalent to `mdb_env_open()
    <http://symas.com/mdb/doc/group__mdb.html#ga1fe2740e25b1689dc412e7b9faadba1b>`_

        `path`:
            Location of directory (if `subdir=True`) or file prefix to store
            the database.

        `map_size`:
            Maximum size database may grow to; used to size the memory mapping.
            If database grows larger than ``map_size``, an exception will be
            raised and the user must close and reopen :py:class:`Environment`.
            On 64-bit there is no penalty for making this huge (say 1TB). Must
            be <2GB on 32-bit.

            .. note::

                **The default map size is set low to encourage a crash**, so
                users can figure out a good value before learning about this
                option too late.

        `subdir`:
            If ``True``, `path` refers to a subdirectory to store the data and
            lock files in, otherwise it refers to a filename prefix.

        `readonly`:
            If ``True``, disallow any write operations. Note the lock file is
            still modified. If specified, the ``write`` flag to
            :py:meth:`begin` or :py:class:`Transaction` is ignored.

        `metasync`:
            If ``False``, never explicitly flush metadata pages to disk. OS
            will flush at its discretion, or user can flush with
            :py:meth:`sync`.

        `sync`
            If ``False``, never explicitly flush data pages to disk. OS will
            flush at its discretion, or user can flush with :py:meth:`sync`.
            This optimization means a system crash can corrupt the database or
            lose the last transactions if buffers are not yet flushed to disk.

        `mode`:
            File creation mode.

        `create`:
            If ``False``, do not create the directory `path` if it is missing.

        `writemap`:
            If ``True`` LMDB will use a writeable memory map to update the
            database. This option is incompatible with nested transactions.

        `map_async`:
             When ``writemap=True``, use asynchronous flushes to disk. As with
             ``sync=False``, a system crash can then corrupt the database or
             lose the last transactions. Calling :py:meth:`sync` ensures
             on-disk database integrity until next commit.

        `max_readers`:
            Maximum number of simultaneous read transactions. Can only be set
            by the first process to open an environment, as it affects the size
            of the lock file and shared memory area. Attempts to simultaneously
            start more than this many *read* transactions will fail.

        `max_dbs`:
            Maximum number of databases available. If 0, assume environment
            will be used as a single database.

        `max_spare_txns`:
            Read-only transactions to cache after becoming unused. Caching
            transactions avoids two allocations, one lock and linear scan
            of the shared environment per invocation of :py:meth:`begin`,
            :py:class:`Transaction`, :py:meth:`get`, :py:meth:`gets`, or
            :py:meth:`cursor`. Should match the process's maximum expected
            concurrent transactions (e.g. thread count).

            *Note:* ignored on cffi.

        `max_spare_cursors`:
            Read-only cursors to cache after becoming unused. Caching cursors
            avoids two allocations per :py:class:`Cursor` or :py:meth:`cursor`
            or :py:meth:`Transaction.cursor` invocation.

            *Note:* ignored on cffi.

        `max_spare_iters`:
            Iterators to cache after becoming unused. Caching iterators avoids
            one allocation per :py:class:`Cursor` ``iter*`` method invocation.

            *Note:* ignored on cffi.
    """
    def __init__(self, path, map_size=10485760, subdir=True,
            readonly=False, metasync=True, sync=True, map_async=False,
            mode=0o644, create=True, writemap=False, max_readers=126,
            max_dbs=0, max_spare_txns=1, max_spare_cursors=32,
            max_spare_iters=32):
        envpp = _ffi.new('MDB_env **')

        rc = mdb_env_create(envpp)
        if rc:
            raise _error("mdb_env_create", rc)
        self._env = envpp[0]
        self._deps = {}

        rc = mdb_env_set_mapsize(self._env, map_size)
        if rc:
            raise _error("mdb_env_set_mapsize", rc)

        rc = mdb_env_set_maxreaders(self._env, max_readers)
        if rc:
            raise _error("mdb_env_set_maxreaders", rc)

        rc = mdb_env_set_maxdbs(self._env, max_dbs)
        if rc:
            raise _error("mdb_env_set_maxdbs", rc)

        if create and subdir and not os.path.exists(path):
            os.mkdir(path)

        flags = MDB_NOTLS
        if not subdir:
            flags |= MDB_NOSUBDIR
        if readonly:
            flags |= MDB_RDONLY
        self.readonly = readonly
        if not metasync:
            flags |= MDB_NOMETASYNC
        if not sync:
            flags |= MDB_NOSYNC
        if map_async:
            flags |= MDB_MAPASYNC
        if writemap:
            flags |= MDB_WRITEMAP

        if isinstance(path, type(u'')):
            path = path.encode(sys.getfilesystemencoding())

        rc = mdb_env_open(self._env, path, flags, mode)
        if rc:
            raise _error(path, rc)
        with self.begin(db=object()) as txn:
            self._db = _Database(self, txn, None, False, False, True)
        self._dbs = {None: weakref.ref(self._db)}

    def close(self):
        """Close the environment, invalidating any open iterators, cursors, and
        transactions.

        Equivalent to `mdb_env_close()
        <http://symas.com/mdb/doc/group__mdb.html#ga4366c43ada8874588b6a62fbda2d1e95>`_
        """
        if self._env:
            _kill_dependents(self)
            mdb_env_close(self._env)
            self._env = _invalid

    def __del__(self):
        self.close()

    def path(self):
        """Directory path or file name prefix where this environment is
        stored.

        Equivalent to `mdb_env_get_path()
        <http://symas.com/mdb/doc/group__mdb.html#gac699fdd8c4f8013577cb933fb6a757fe>`_
        """
        path = _ffi.new('char **')
        rc = mdb_env_get_path(self._env, path)
        if rc:
            raise _error("mdb_env_get_path", rc)
        return _ffi.string(path[0])

    def copy(self, path):
        """Make a consistent copy of the environment in the given destination
        directory.

        Equivalent to `mdb_env_copy()
        <http://symas.com/mdb/doc/group__mdb.html#ga5d51d6130325f7353db0955dbedbc378>`_
        """
        rc = mdb_env_copy(self._env, path)
        if rc:
            raise _error("mdb_env_copy", rc)

    def copyfd(self, fd):
        """Copy a consistent version of the environment to file descriptor
        `fd`.

        Equivalent to `mdb_env_copyfd()
        <http://symas.com/mdb/doc/group__mdb.html#ga5d51d6130325f7353db0955dbedbc378>`_
        """
        rc = mdb_env_copyfd(self._env, fd)
        if rc:
            raise _error("mdb_env_copyfd", rc)

    def sync(self, force=False):
        """Flush the data buffers to disk.

        Equivalent to `mdb_env_sync()
        <http://symas.com/mdb/doc/group__mdb.html#ga85e61f05aa68b520cc6c3b981dba5037>`_

        Data is always written to disk when :py:meth:`Transaction.commit` is
        called, but the operating system may keep it buffered. MDB always
        flushes the OS buffers upon commit as well, unless the environment was
        opened with `sync=False` or `metasync=False`.

        `force`:
            If ``True``, force a synchronous flush. Otherwise if the
            environment was opened with `sync=False` the flushes will be
            omitted, and with `map_async=True` they will be asynchronous.
        """
        rc = mdb_env_sync(self._env, force)
        if rc:
            raise _error("mdb_env_sync", rc)

    def _convert_stat(self, st):
        """Convert a MDB_stat to a dict.
        """
        return {
            "psize": st.ms_psize,
            "depth": st.ms_depth,
            "branch_pages": st.ms_branch_pages,
            "leaf_pages": st.ms_leaf_pages,
            "overflow_pages": st.ms_overflow_pages,
            "entries": st.ms_entries
        }

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

        Equivalent to `mdb_env_stat()
        <http://symas.com/mdb/doc/group__mdb.html#gaf881dca452050efbd434cd16e4bae255>`_
        """
        st = _ffi.new('MDB_stat *')
        rc = mdb_env_stat(self._env, st)
        if rc:
            raise _error("mdb_env_stat", rc)
        return self._convert_stat(st)

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

        Equivalent to `mdb_env_info()
        <http://symas.com/mdb/doc/group__mdb.html#ga18769362c7e7d6cf91889a028a5c5947>`_
        """
        info = _ffi.new('MDB_envinfo *')
        rc = mdb_env_info(self._env, info)
        if rc:
            raise _error("mdb_env_info", rc)
        return {
            "map_size": info.me_mapsize,
            "last_pgno": info.me_last_pgno,
            "last_txnid": info.me_last_txnid,
            "max_readers": info.me_maxreaders,
            "num_readers": info.me_numreaders
        }

    def readers(self):
        """Return a list of newline-terminated human readable strings
        describing the current state of the reader lock table.
        """
        _callbacks.msg_func = []
        try:
            rc = mdb_reader_list(self._env, _msg_func, _ffi.NULL)
            if rc:
                raise _error("mdb_reader_list", rc)
            return ''.join(_callbacks.msg_func)
        finally:
            del _callbacks.msg_func

    def reader_check(self):
        """Search the reader lock table for stale entries, for example due to a
        crashed process. Returns the number of stale entries that were cleared.
        """
        reaped = _ffi.new('int[]', 1)
        rc = mdb_reader_check(self._env, reaped)
        if rc:
            raise _error('mdb_reader_check', rc)
        return reaped[0]

    def open_db(self, name=None, txn=None, reverse_key=False, dupsort=False,
            create=True):
        """
        Open a database, returning an opaque handle. Repeat
        :py:meth:`Environment.open_db` calls for the same name will return the
        same handle. As a special case, the main database is always open.

        Equivalent to `mdb_dbi_open()
        <http://symas.com/mdb/doc/group__mdb.html#gac08cad5b096925642ca359a6d6f0562a>`_

        A newly created database will not exist if the transaction that created
        it aborted, nor if another process deleted it. The handle resides in
        the shared environment, it is not owned by the current transaction or
        process. Only one thread should call this function; it is not
        mutex-protected in a read-only transaction.

        Preexisting transactions, other than the current transaction and any
        parents, must not use the new handle, nor must their children.

            `name`:
                Database name. If ``None``, indicates the main database should
                be returned, otherwise indicates a sub-database should be
                created inside the main database. In other words, **a key
                representing the database will be visible in the main database,
                and the database name cannot conflict with any existing key**

            `txn`:
                Transaction used to create the database if it does not exist.
                If unspecified, a temporarily write transaction is used. Do not
                call :py:meth:`open_db` from inside an existing transaction
                without supplying it here. Note the passed transaction must
                have `write=True`.

            `reverse_key`:
                If ``True``, keys are compared from right to left (e.g. DNS
                names).

            `dupsort`:
                Duplicate keys may be used in the database. (Or, from another
                perspective, keys may have multiple data items, stored in
                sorted order.) By default keys must be unique and may have only
                a single data item.

                *dupsort* is not yet fully supported.

            `create`:
                If ``True``, create the database if it doesn't exist, otherwise
                raise an exception.
        """
        ref = self._dbs.get(name)
        if ref:
            db = ref()
            if db:
                return db

        if txn:
            db = _Database(self, txn, name, reverse_key, dupsort, create)
        else:
            with self.begin(write=True) as txn:
                db = _Database(self, txn, name, reverse_key, dupsort, create)
        self._dbs[name] = weakref.ref(db)
        return db

    def begin(self, db=None, parent=None, write=False, buffers=False):
        """Shortcut for :py:class:`lmdb.Transaction`"""
        return Transaction(self, db, parent, write, buffers)

    def get(self, key, default=None, db=None):
        """Use a temporary read transaction to invoke
        :py:meth:`Transaction.get`."""
        with Transaction(self) as txn:
            return txn.get(key, default, db)

    def gets(self, keys, db=None):
        """Use a temporary read transaction to invoke
        :py:meth:`Transaction.get` for each key in `keys`. The returned value
        is a dict containing one element for each key that existed."""
        dct = {}
        with Transaction(self) as txn:
            for key in keys:
                value = txn.get(key, None, db)
                if value is not None:
                    dct[key] = value
        return dct

    def put(self, key, value, dupdata=False, overwrite=True, append=False,
            db=None):
        """Use a temporary write transaction to invoke
        :py:meth:`Transaction.put`."""
        with Transaction(self, write=True) as txn:
            txn.put(key, value, dupdata, overwrite, append, db)

    def puts(self, items, dupdata=False, overwrite=True, append=False,
             db=None):
        """Use a temporary write transaction to invoke
        :py:meth:`Transaction.put` as `put(x, y)` for each `(x, y)`
        in `items`. Items must be a dict, or an iterable producing 2-tuples.
        This function requires 2-tuples, no other sequence type is accepted.

        Returns a list of :py:meth:`Transaction.put` return values.

            .. code-block:: python

                a_existed, b_existed = env.puts(overwrite=False, items={
                    'a': '1',
                    'b': '2'
                })

                if a_existed:
                    print 'Did not overwrite a, it already existed.'
                if b_existed:
                    print 'Did not overwrite b, it already existed.'
        """
        if type(items) is dict:
            items = items.iteritems()
        with Transaction(self, write=True) as txn:
            return [txn.put(key, value, dupdata, overwrite, append, db)
                    for key, value in items]

    def delete(self, key, value='', db=None):
        """Use a temporary write transaction to invoke
        :py:meth:`Transaction.delete`."""
        with Transaction(self, write=True) as txn:
            return txn.delete(key, value, db)

    def deletes(self, keys, db=None):
        """Use a temporary write transaction to invoke
        :py:meth:`Transaction.delete` for each key in `keys`. Returns a list of
        :py:meth:`Transaction.delete` return values."""
        with Transaction(self, write=True) as txn:
            return [txn.delete(key, '', db) for key in keys]

    def cursor(self, buffers=False, db=None):
        """Use a temporary read transaction to return a :py:class:`Cursor`. The
        transaction will remain alive for as long as the cursor exists.
        """
        txn = Transaction(self, db, None, False, buffers)
        return Cursor(db or self._db, txn)


class _Database(object):
    """Internal database handle."""
    def __init__(self, env, txn, name, reverse_key, dupsort, create):
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
            raise _error("mdb_dbi_open", rc)
        self._dbi = dbipp[0]

    def _invalidate(self):
        pass

    def __del__(self):
        _undepend(self.env, self)

open = Environment


class Transaction(object):
    """
    A transaction object. All operations require a transaction handle,
    transactions may be read-only or read-write. Write transactions may not
    span threads. Transaction objects implement the context manager protocol,
    so that reliable release of the transaction happens even in the face of
    unhandled exceptions:

        .. code-block:: python

            # Transaction aborts correctly:
            with env.begin(write=True) as txn:
                crash()

            # Transaction commits automatically:
            with env.begin(write=True) as txn:
                txn.put('a', 'b')

    Equivalent to `mdb_txn_begin()
    <http://symas.com/mdb/doc/group__mdb.html#gad7ea55da06b77513609efebd44b26920>`_

        `env`:
            Environment the transaction should be on.

        `db`:
            Default sub-database to operate on. If unspecified, defaults to the
            environment's main database. Can be overridden on a per-call basis
            below.

        `parent`:
            ``None``, or a parent transaction (see lmdb.h).

        `write`:
            Transactions are read-only by default. To modify the database, you
            must pass `write=True`. This flag is ignored if
            :py:class:`Environment` was opened with ``readonly=True``.

        `buffers`:
            If ``True``, indicates :py:func:`buffer` objects should be yielded
            instead of strings. This setting applies to the
            :py:class:`Transaction` instance itself and any :py:class:`Cursors
            <Cursor>` created within the transaction.

            This feature significantly improves performance, since MDB has a
            zero-copy design, but it requires care when manipulating the
            returned buffer objects. The benefit of this facility is diminished
            when using small keys and values.
    """
    def __init__(self, env, db=None, parent=None, write=False, buffers=False):
        _depend(env, self)
        self.env = env # hold ref
        self._db = db or env._db
        self._env = env._env
        self._key = _ffi.new('MDB_val *')
        self._val = _ffi.new('MDB_val *')
        self._to_py = _mvbuf if buffers else _mvstr
        self._deps = {}
        if write and env.readonly:
            raise _error('Cannot start write transaction with read-only env')
        if write:
            flags = 0
        else:
            flags = MDB_RDONLY
        txnpp = _ffi.new('MDB_txn **')
        if parent:
            self._parent = parent
            parent_txn = parent._txn
            _depend(parent, self)
        else:
            parent_txn = _ffi.NULL
        rc = mdb_txn_begin(self._env, parent_txn, flags, txnpp)
        if rc:
            raise _error("mdb_txn_begin", rc)
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

    def stat(self, db):
        """stat(db)

        Return statistics like :py:meth:`Environment.stat`, except for a single
        DBI. `db` must be a database handle returned by :py:meth:`open_db`.
        """
        st = _ffi.new('MDB_stat *')
        rc = mdb_stat(self._txn, db._dbi, st)
        if rc:
            raise _error('mdb_stat', rc)
        return self.env._convert_stat(st)

    def drop(self, db, delete=True):
        """Delete all keys in a sub-database, and optionally delete the
        sub-database itself. Deleting the sub-database causes it to become
        unavailable, and invalidates existing cursors.

        Equivalent to `mdb_drop()
        <http://symas.com/mdb/doc/group__mdb.html#gab966fab3840fc54a6571dfb32b00f2db>`_
        """
        _kill_dependents(db)
        rc = mdb_drop(self._txn, db._dbi, delete)
        if rc:
            raise _error("mdb_drop", rc)

    def commit(self):
        """Commit the pending transaction.

        Equivalent to `mdb_txn_commit()
        <http://symas.com/mdb/doc/group__mdb.html#ga846fbd6f46105617ac9f4d76476f6597>`_
        """
        if self._txn:
            _kill_dependents(self)
            rc = mdb_txn_commit(self._txn)
            self._txn = _invalid
            if rc:
                raise _error("mdb_txn_commit", rc)

    def abort(self):
        """Abort the pending transaction.

        Equivalent to `mdb_txn_abort()
        <http://symas.com/mdb/doc/group__mdb.html#ga73a5938ae4c3239ee11efa07eb22b882>`_
        """
        if self._txn:
            _kill_dependents(self)
            rc = mdb_txn_abort(self._txn)
            self._txn = _invalid
            if rc:
                raise _error("mdb_txn_abort", rc)

    def get(self, key, default=None, db=None):
        """Fetch the first value matching `key`, returning `default` if `key`
        does not exist. A cursor must be used to fetch all values for a key in
        a `dupsort=True` database.

        Equivalent to `mdb_get()
        <http://symas.com/mdb/doc/group__mdb.html#ga8bf10cd91d3f3a83a34d04ce6b07992d>`_
        """
        rc = pymdb_get(self._txn, (db or self._db)._dbi,
                       key, len(key), self._val)
        if rc:
            if rc == MDB_NOTFOUND:
                return default
            raise _error("mdb_cursor_get", rc)
        return self._to_py(self._val)

    def put(self, key, value, dupdata=False, overwrite=True, append=False,
            db=None):
        """Store a record, returning ``True`` if it was written, or ``False``
        to indicate the key was already present and `overwrite=False`.

        Equivalent to `mdb_put()
        <http://symas.com/mdb/doc/group__mdb.html#ga4fa8573d9236d54687c61827ebf8cac0>`_

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

        rc = pymdb_put(self._txn, (db or self._db)._dbi,
                       key, len(key), value, len(value), flags)
        if rc:
            if rc == MDB_KEYEXIST:
                return False
            raise _error("mdb_put", rc)
        return True

    def delete(self, key, value='', db=None):
        """Delete a key from the database.

        Equivalent to `mdb_del()
        <http://symas.com/mdb/doc/group__mdb.html#gab8182f9360ea69ac0afd4a4eaab1ddb0>`_

            `key`:
                The key to delete.

            value:
                If the database was opened with dupsort=True and value is not
                the empty string, then delete elements matching only this
                `(key, value)` pair, otherwise all values for key are deleted.

        Returns True if at least one key was deleted.
        """
        rc = pymdb_del(self._txn, (db or self._db)._dbi,
                       key, len(key), value, len(value))
        if rc:
            if rc == MDB_NOTFOUND:
                return False
            raise _error("mdb_del", rc)
        return True

    def cursor(self, db=None):
        """Shortcut for ``lmdb.Cursor(db, self)``"""
        return Cursor(db or self._db, self)


class Cursor(object):
    """
    Structure for navigating a database.

    Equivalent to `mdb_cursor_open()
    <http://symas.com/mdb/doc/group__mdb.html#ga9ff5d7bd42557fd5ee235dc1d62613aa>`_

        `db`:
            :py:class:`Database` to navigate.

        `txn`:
            :py:class:`Transaction` to navigate.

    As a convenience, :py:meth:`Transaction.cursor` can be used to quickly
    return a cursor:

        ::

            >>> env = lmdb.open('/tmp/foo')
            >>> child_db = env.open_db('child_db')
            >>> with env.begin() as txn:
            ...     cursor = txn.cursor()           # Cursor on main database.
            ...     cursor2 = txn.cursor(child_db)  # Cursor on child database.

    Cursors start in an unpositioned state: if :py:meth:`iternext` or
    :py:meth:`iterprev` are used in this state, iteration proceeds from the
    start or end respectively. Iterators directly position using the cursor,
    meaning strange behavior results when multiple iterators exist on the same
    cursor.

        ::

            >>> with env.begin() as txn:
            ...     for i, (key, value) in enumerate(txn.cursor().iterprev()):
            ...         print '%dth last item is (%r, %r)' % (1 + i, key, value)

    Both :py:meth:`iternext` and :py:meth:`iterprev` accept `keys` and `values`
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
    may be preferable:

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
            raise _error("mdb_cursor_open", rc)
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
                raise _error("mdb_cursor_get", rc)

    def iternext(self, keys=True, values=True):
        """Return a forward iterator that yields the current element before
        calling :py:meth:`next`, repeating until the end of the database is
        reached. As a convenience, :py:class:`Cursor` implements the iterator
        protocol by automatically returning a forward iterator when invoked:

            ::

                >>> # Equivalent:
                >>> it = iter(cursor)
                >>> it = cursor.iternext(keys=True, values=True)

        If the cursor was not yet positioned, it is moved to the first record
        in the database, otherwise iteration proceeds from the current
        position.
        """
        if not self._valid:
            self.first()
        return self._iter(MDB_NEXT, keys, values)
    __iter__ = iternext

    def iterprev(self, keys=True, values=True):
        """Return a reverse iterator that yields the current element before
        calling :py:meth:`prev`, until the start of the database is reached.

        If the cursor was not yet positioned, it is moved to the last record in
        the database, otherwise iteration proceeds from the current position.
        """
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
                    raise _error("mdb_cursor_get", rc)
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
                    raise _error("mdb_cursor_get", rc)
        self._valid = v
        return v

    def first(self):
        """Move to the first element, returning ``True`` on success or
        ``False`` if the database is empty.

        Equivalent to `mdb_cursor_get()
        <http://symas.com/mdb/doc/group__mdb.html#ga48df35fb102536b32dfbb801a47b4cb0>`_
        with `MDB_FIRST
        <http://symas.com/mdb/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127>`_
        """
        return self._cursor_get(MDB_FIRST)

    def last(self):
        """Move to the last element, returning ``True`` on success or ``False``
        if the database is empty.

        Equivalent to `mdb_cursor_get()
        <http://symas.com/mdb/doc/group__mdb.html#ga48df35fb102536b32dfbb801a47b4cb0>`_
        with `MDB_LAST
        <http://symas.com/mdb/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127>`_
        """
        return self._cursor_get(MDB_LAST)

    def prev(self):
        """Move to the previous element, returning ``True`` on success or
        ``False`` if there is no previous element.

        Equivalent to `mdb_cursor_get()
        <http://symas.com/mdb/doc/group__mdb.html#ga48df35fb102536b32dfbb801a47b4cb0>`_
        with `MDB_PREV
        <http://symas.com/mdb/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127>`_
        """
        return self._cursor_get(MDB_PREV)

    def next(self):
        """Move to the next element, returning ``True`` on success or ``False``
        if there is no next element.

        Equivalent to `mdb_cursor_get()
        <http://symas.com/mdb/doc/group__mdb.html#ga48df35fb102536b32dfbb801a47b4cb0>`_
        with `MDB_NEXT
        <http://symas.com/mdb/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127>`_
        """
        return self._cursor_get(MDB_NEXT)

    def set_key(self, key):
        """Seek exactly to `key`, returning ``True`` on success or ``False`` if
        the exact key was not found.

        It is an error to :py:meth:`set_key` the empty string.

        Equivalent to `mdb_cursor_get()
        <http://symas.com/mdb/doc/group__mdb.html#ga48df35fb102536b32dfbb801a47b4cb0>`_
        with `MDB_SET_KEY
        <http://symas.com/mdb/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127>`_
        """
        return self._cursor_get_key(MDB_SET_KEY, key)

    def get(self, key, default=None):
        """Equivalent to :py:meth:`set_key()`, except :py:meth:`value` is
        returned if `key` was found, otherwise `default`.
        """
        if self._cursor_get_key(MDB_SET_KEY, key):
            return self.value()
        return default

    def set_range(self, key):
        """Seek to the first key greater than or equal to `key`, returning
        ``True`` on success, or ``False`` to indicate key was past end of
        database.

        Behaves like :py:meth:`first` if `key` is the empty string.

        Equivalent to `mdb_cursor_get()
        <http://symas.com/mdb/doc/group__mdb.html#ga48df35fb102536b32dfbb801a47b4cb0>`_ with `MDB_SET_RANGE <http://symas.com/mdb/doc/group__mdb.html#ga1206b2af8b95e7f6b0ef6b28708c9127>`_
        """
        if not key: # TODO: set_range() throws INVAL on an empty store, whereas
                    # set_key() returns NOTFOUND
            return self.first()
        return self._cursor_get_key(MDB_SET_RANGE, key)

    def delete(self):
        """Delete the current element and move to the next element, returning
        ``True`` on success or ``False`` if the database was empty.

        Equivalent to `mdb_cursor_del()
        <http://symas.com/mdb/doc/group__mdb.html#ga26a52d3efcfd72e5bf6bd6960bf75f95>`_
        """
        v = self._valid
        if v:
            rc = mdb_cursor_del(self._cur, 0)
            if rc:
                raise _error("mdb_cursor_del", rc)
            self._cursor_get(MDB_GET_CURRENT)
            v = rc == 0
        return v

    def count(self):
        """Return the number of duplicates for the current key. This is only
        meaningful for databases that have `dupdata=True`.

        Equivalent to `mdb_cursor_count()
        <http://symas.com/mdb/doc/group__mdb.html#ga4041fd1e1862c6b7d5f10590b86ffbe2>`_
        """
        countp = _ffi.new('size_t *')
        rc = mdb_cursor_count(self._cur, countp)
        if rc:
            raise _error("mdb_cursor_count", rc)
        return countp[0]

    def put(self, key, val, dupdata=False, overwrite=True, append=False):
        """Store a record, returning ``True`` if it was written, or ``False``
        to indicate the key was already present and `overwrite=False`. On
        success, the cursor is positioned on the key.

        Equivalent to `mdb_cursor_put()
        <http://symas.com/mdb/doc/group__mdb.html#ga1f83ccb40011837ff37cc32be01ad91e>`_

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
            raise _error("mdb_cursor_put", rc)
        self._cursor_get(MDB_GET_CURRENT)
        return True

    def _iter_from(self, k, reverse):
        """Helper for centidb. Please do not rely on this interface, it may be
        removed in future.
        """
        if not k and not reverse:
            found = self.first()
        else:
            found = self.set_range(k)
        if reverse:
            if not found:
                self.last()
            return self.iterprev()
        else:
            if not found:
                return iter(())
            return self.iternext()
