/*
 * Copyright 2013 The py-lmdb authors, all rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted only as authorized by the OpenLDAP
 * Public License.
 *
 * A copy of this license is available in the file LICENSE in the
 * top-level directory of the distribution or, alternatively, at
 * <http://www.OpenLDAP.org/license.html>.
 *
 * OpenLDAP is a registered trademark of the OpenLDAP Foundation.
 *
 * Individual files and/or contributed packages may be copyright by
 * other parties and/or subject to additional restrictions.
 *
 * This work also contains materials derived from public sources.
 *
 * Additional information about OpenLDAP can be obtained at
 * <http://www.openldap.org/>.
 */

#define PY_SSIZE_T_CLEAN

#include <errno.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>
#include <sys/stat.h>
#include <tgmath.h>

#include "Python.h"
#include "structmember.h"

#ifdef HAVE_MEMSINK
#define USING_MEMSINK
#include "memsink.h"
#endif

#include "lmdb.h"


// Comment out for copious debug.
#define NODEBUG

#ifdef NODEBUG
#   define DEBUG(s, ...)
#else
#   define DEBUG(s, ...) fprintf(stderr, \
    "lmdb.cpython: %s:%d: " s "\n", __func__, __LINE__, ## __VA_ARGS__);
#endif

// Inlining control for compatible compilers.
#if (__GNUC__ > 3 || (__GNUC__ == 3 && __GNUC_MINOR__ >= 4))
#   define NOINLINE __attribute__((noinline))
#elif defined(_MSC_VER)
#   define NOINLINE __declspec(noinline)
#else
#   define NOINLINE
#endif


/**
 * Integer indices into `string_tbl', representing an associated string. We
 * use an enum of indices since they can be stored with 1 byte instead of 8
 * bytes for a direct pointer, shaving a few KB data off all argspec arrays,
 * and allowing indirect reference to a table of interned strings created
 * during initialization, to avoid constructing and hashing temporary PyStrings
 * to parse keyword arguments on every call.
 *
 * Must be in the same order as `strings' below.
 */
enum string_id {
    APPEND_S,
    BUFFERS_S,
    CREATE_S,
    DB_S,
    DEFAULT_S,
    DELETE_S,
    DUPDATA_S,
    DUPSORT_S,
    ENV_S,
    FD_S,
    FORCE_S,
    ITEMS_S,
    ITERITEMS_S,
    KEY_S,
    KEYS_S,
    MAP_ASYNC_S,
    MAP_SIZE_S,
    MAX_DBS_S,
    MAX_READERS_S,
    MAX_SPARE_CURSORS_S,
    MAX_SPARE_ITERS_S,
    MAX_SPARE_TXNS_S,
    METASYNC_S,
    MODE_S,
    NAME_S,
    OVERWRITE_S,
    PARENT_S,
    PATH_S,
    READAHEAD_S,
    READONLY_S,
    REVERSE_S,
    REVERSE_KEY_S,
    SUBDIR_S,
    SYNC_S,
    TXN_S,
    VALUE_S,
    VALUES_S,
    WRITE_S,
    WRITEMAP_S,

    // Must be last.
    STRING_ID_COUNT
};

/**
 * NUL-separated array of strings corresponding to `string_ids`. Expanded into
 * `string_tbl` PyObject* array representing interned strings during module
 * init.
 */
static const char *strings = (
    "append\0"
    "buffers\0"
    "create\0"
    "db\0"
    "default\0"
    "delete\0"
    "dupdata\0"
    "dupsort\0"
    "env\0"
    "fd\0"
    "force\0"
    "items\0"
    "iteritems\0"
    "key\0"
    "keys\0"
    "map_async\0"
    "map_size\0"
    "max_dbs\0"
    "max_readers\0"
    "max_spare_cursors\0"
    "max_spare_iters\0"
    "max_spare_txns\0"
    "metasync\0"
    "mode\0"
    "name\0"
    "overwrite\0"
    "parent\0"
    "path\0"
    "readahead\0"
    "readonly\0"
    "reverse\0"
    "reverse_key\0"
    "subdir\0"
    "sync\0"
    "txn\0"
    "value\0"
    "values\0"
    "write\0"
    "writemap\0"
);

/** Interned string array corresponding to `string_ids'. */
static PyObject **string_tbl;
/** PyLong representing integer 0. */
static PyObject *py_zero;
/** PyLong representing INT_MAX. */
static PyObject *py_int_max;
/** PyLong representing SIZE_MAX. */
static PyObject *py_size_max;
/** lmdb.Error type. */
static PyObject *Error;
/** If 1, save_thread() and restore_thread() drop GIL. */
static int drop_gil = 0;

/** Typedefs and forward declarations. */
static PyTypeObject PyDatabase_Type;
static PyTypeObject PyEnvironment_Type;
static PyTypeObject PyTransaction_Type;
static PyTypeObject PyCursor_Type;
static PyTypeObject PyIterator_Type;

typedef struct CursorObject CursorObject;
typedef struct DbObject DbObject;
typedef struct EnvObject EnvObject;
typedef struct IterObject IterObject;
typedef struct TransObject TransObject;


// --------------------------
// Buffer object abuse macros
// --------------------------

#if PY_MAJOR_VERSION >= 3

// Python 3.3 kindly exports the struct definitions for us.
#   define MOD_RETURN(mod) return mod;
#   define MODINIT_NAME PyInit_cpython
#   define BUFFER_TYPE PyMemoryViewObject
#   define MAKE_BUFFER() PyMemoryView_FromMemory("", 0, PyBUF_READ)
#   define SET_BUFFER(buff, ptr, size) {\
        (buff)->view.buf = (ptr); \
        (buff)->view.len = (size); \
        (buff)->hash = -1; \
    }

#else

// So evil.
typedef struct {
    PyObject_HEAD
    PyObject *b_base;
    void *b_ptr;
    Py_ssize_t b_size;
    Py_ssize_t b_offset;
    int b_readonly;
    long b_hash;
} PyBufferObject;

#   define PyUnicode_InternFromString PyString_InternFromString
#   define PyBytes_AS_STRING PyString_AS_STRING
#   define PyBytes_GET_SIZE PyString_GET_SIZE
#   define PyBytes_CheckExact PyString_CheckExact
#   define PyBytes_FromStringAndSize PyString_FromStringAndSize
#   define _PyBytes_Resize _PyString_Resize
#   define MOD_RETURN(mod) return
#   define MODINIT_NAME initcpython
#   define BUFFER_TYPE PyBufferObject
#   define MAKE_BUFFER() PyBuffer_FromMemory("", 0)
#   define SET_BUFFER(buf, ptr, size) {\
        (buf)->b_hash = -1; \
        (buf)->b_ptr = (ptr); \
        (buf)->b_size = (size); \
    }

// Python 2.5
#ifndef Py_TYPE
#   define Py_TYPE(ob) (((PyObject*)(ob))->ob_type)
#endif

#endif

#if (PY_MAJOR_VERSION == 2) && (PY_MINOR_VERSION < 6)
static PyObject *
PyUnicode_FromString(const char *u)
{
    PyObject *s = PyString_FromString(u);
    if(s) {
        PyObject *t = PyUnicode_FromEncodedObject(
            s, Py_FileSystemDefaultEncoding, "strict");
        Py_DECREF(s);
        s = t;
    }
    return s;
}
#endif


struct list_head {
    struct lmdb_object *prev;
    struct lmdb_object *next;
};

#define LmdbObject_HEAD \
    PyObject_HEAD \
    struct list_head siblings; \
    struct list_head children; \
    int valid;

struct lmdb_object {
    LmdbObject_HEAD
};

#define OBJECT_INIT(o) \
    ((struct lmdb_object *)o)->siblings.prev = NULL; \
    ((struct lmdb_object *)o)->siblings.next = NULL; \
    ((struct lmdb_object *)o)->children.prev = NULL; \
    ((struct lmdb_object *)o)->children.next = NULL; \
    ((struct lmdb_object *)o)->valid = 1;


/** lmdb._Database */
struct DbObject {
    LmdbObject_HEAD
    /** Python Environment reference. */
    struct EnvObject *env; // Not refcounted.
    /** MDB database handle. */
    MDB_dbi dbi;
};

/** lmdb.Environment */
struct EnvObject {
    LmdbObject_HEAD
    /** Python-managed list of weakrefs to this object. */
    PyObject *weaklist;
    /** MDB environment object. */
    MDB_env *env;
    /** DBI for main database, opened during Environment construction. */
    DbObject *main_db;
    /**  1 if env opened read-only; transactions must always be read-only. */
    int readonly;
};

/** TransObject.flags bitfield values. */
enum trans_flags {
    /** Buffers should be yielded by get. */
    TRANS_BUFFERS       = 1,
    /** Transaction is read-only and go on the freelist at deallocation. */
    TRANS_RDONLY        = 2
};

/** lmdb.Transaction */
struct TransObject {
    LmdbObject_HEAD
    EnvObject *env;
#ifdef HAVE_MEMSINK
    /** Copy-on-invalid list head. */
    PyObject *sink_head;
#endif
    /** MDB transaction object. */
    MDB_txn *txn;
    /** Bitfield of trans_flags values. */
    int flags;
    /** NULL if !TRANS_BUFFERS, or prior to any call to get(). */
    BUFFER_TYPE *key_buf;
    /** Default database if none specified. */
    DbObject *db;
    /** Number of mutations occurred since start of transaction. Required to
     * know when cursor key/value must be refreshed. */
    int mutations;
};

/** lmdb.Cursor */
struct CursorObject {
    LmdbObject_HEAD
    /** Transaction cursor belongs to. */
    TransObject *trans;
    /** 1 if mdb_cursor_get() has been called and it last returned 0. */
    int positioned;
    /** MDB-level cursor object. */
    MDB_cursor *curs;
    /** NULL if trans->buffers==0, or prior to any fetch call. */
    BUFFER_TYPE *key_buf;
    /** NULL if trans->buffers==0, or prior to any fetch call. */
    BUFFER_TYPE *val_buf;
    /** NULL if trans->buffers==0, or prior to any item() call.*/
    PyObject *item_tup;
    /** mv_size==0 if positioned==0, otherwise points to current key. */
    MDB_val key;
    /** mv_size==0 if positioned==0, otherwise points to current value. */
    MDB_val val;
    /** If TransObject.mutations!=last_mutation, must MDB_GET_CURRENT to
     * refresh `key' and `val'. */
    int last_mutation;
};


typedef PyObject *(*IterValFunc)(CursorObject *);

/** lmdb.Iterator
 *
 * This is separate from Cursor since we want to define Cursor.next() to mean
 * MDB_NEXT, and a Python iterator's next() has different semantics.
 */
struct IterObject {
    PyObject_HEAD
    /** Cursor being iterated, or NULL for freelist iterator. */
    CursorObject *curs;
    /** 1 if iteration has started (Cursor should advance on next()). */
    int started;
    /** Operation used to advance cursor. */
    MDB_cursor_op op;
    /** Iterator value function, should be item(), key(), or value(). */
    IterValFunc val_func;
};


/**
 * Link `child` into `parent`'s list of dependent objects. Use LINK_CHILD()
 * maro to avoid casting PyObject to lmdb_object.
 */
static void link_child(struct lmdb_object *parent, struct lmdb_object *child)
{
    struct lmdb_object *sibling = parent->children.next;
    if(sibling) {
        child->siblings.next = sibling;
        sibling->siblings.prev = child;
    }
    parent->children.next = child;
}

#define LINK_CHILD(parent, child) link_child((void *)parent, (void *)child);


/**
 * Remove `child` from `parent`'s list of dependent objects. Use UNLINK_CHILD
 * macro to avoid casting PyObject to lmdb_object.
 */
static void unlink_child(struct lmdb_object *parent, struct lmdb_object *child)
{
    if(! parent) {
        return;
    }

    struct lmdb_object *prev = child->siblings.prev;
    struct lmdb_object *next = child->siblings.next;
    if(prev) {
        prev->siblings.next = next;
             // If double unlink_child(), this test my legitimately fail:
    } else if(parent->children.next == child) {
        parent->children.next = next;
    }
    if(next) {
        next->siblings.prev = prev;
    }
    child->siblings.prev = NULL;
    child->siblings.next = NULL;
}

#define UNLINK_CHILD(parent, child) unlink_child((void *)parent, (void *)child);


/**
 * Notify dependents of `parent` that `parent` is about to become invalid,
 * and that they should free any dependent resources.
 *
 * To save effort, tp_clear is overloaded to be the invalidation function,
 * instead of carrying a separate pointer. Objects are added to their parent's
 * list during construction and removed during deallocation.
 *
 * When the environment is closed, it walks its list calling tp_clear on each
 * child, which in turn walk their own lists. Child transactions are added to
 * their parent transaction's list. Iterators keep no significant state, so
 * they are not tracked.
 *
 * Use INVALIDATE() macro to avoid casting PyObject to lmdb_object.
 */
static void invalidate(struct lmdb_object *parent)
{
    struct lmdb_object *child = parent->children.next;
    while(child) {
        struct lmdb_object *next = child->siblings.next;
        DEBUG("invalidating parent=%p child %p", parent, child)
        Py_TYPE(child)->tp_clear((PyObject *) child);
        child = next;
    }
}

#define INVALIDATE(parent) invalidate((void *)parent);


// ----------
// Exceptions
// ----------

struct error_map {
    int code;
    const char *name;
};

/** Array of Error subclasses corresponding to `error_map'. */
static PyObject **error_tbl;
/** Mapping from LMDB error code to py-lmdb exception class. */
static const struct error_map error_map[] = {
    {MDB_KEYEXIST, "KeyExistsError"},
    {MDB_NOTFOUND, "NotFoundError"},
    {MDB_PAGE_NOTFOUND, "PageNotFoundError"},
    {MDB_CORRUPTED, "CorruptedError"},
    {MDB_PANIC, "PanicError"},
    {MDB_VERSION_MISMATCH, "VersionMismatchError"},
    {MDB_INVALID, "InvalidError"},
    {MDB_MAP_FULL, "MapFullError"},
    {MDB_DBS_FULL, "DbsFullError"},
    {MDB_READERS_FULL, "ReadersFullError"},
    {MDB_TLS_FULL, "TlsFullError"},
    {MDB_TXN_FULL, "TxnFullError"},
    {MDB_CURSOR_FULL, "CursorFullError"},
    {MDB_PAGE_FULL, "PageFullError"},
    {MDB_MAP_RESIZED, "MapResizedError"},
    {MDB_INCOMPATIBLE, "IncompatibleError"},
    {MDB_BAD_RSLOT, "BadRSlotError"},
    {MDB_BAD_TXN, "BadTxnError"},
    {MDB_BAD_VALSIZE, "BadValsizeError"},
    {EACCES, "ReadonlyError"},
    {EINVAL, "InvalidParameterError"},
    {EAGAIN, "LockError"},
    {ENOMEM, "MemoryError"},
    {ENOSPC, "DiskError"}
};

// -------
// Helpers
// -------

/**
 * Describes the type of a struct field.
 */
enum field_type {
    /** Last field in set, stop converting. */
    TYPE_EOF,
    /** Unsigned 32bit integer. */
    TYPE_UINT,
    /** size_t */
    TYPE_SIZE,
    /** void pointer */
    TYPE_ADDR
};

/**
 * Describes a struct field.
 */
struct dict_field {
    /** Field type. */
    enum field_type type;
    /** Field name in target dict. */
    const char *name;
    /* Offset into structure where field is found. */
    int offset;
};

/**
 * Convert the structure `o` described by `fields` to a dict and return the new
 * dict.
 */
static PyObject *
dict_from_fields(void *o, const struct dict_field *fields)
{
    PyObject *dict = PyDict_New();
    if(! dict) {
        return NULL;
    }

    while(fields->type != TYPE_EOF) {
        uint8_t *p = ((uint8_t *) o) + fields->offset;
        unsigned PY_LONG_LONG l = 0;
        if(fields->type == TYPE_UINT) {
            l = *(unsigned int *)p;
        } else if(fields->type == TYPE_SIZE) {
            l = *(size_t *)p;
        } else if(fields->type == TYPE_ADDR) {
            l = (intptr_t) *(void **)p;
        }

        PyObject *lo = PyLong_FromUnsignedLongLong(l);
        if(! lo) {
            Py_DECREF(dict);
            return NULL;
        }

        if(PyDict_SetItemString(dict, fields->name, lo)) {
            Py_DECREF(lo);
            Py_DECREF(dict);
            return NULL;
        }
        Py_DECREF(lo);
        fields++;
    }
    return dict;
}

/**
 * Given an MDB_val `val`, create a new buffer object to describe it, storing
 * the result in `bufp`. If `*bufp` is not NULL, then it is assumed to already
 * contain a buffer object. In that case simply update the existing object.
 * Return the buffer object on success, or NULL on failure.
 */
static PyObject * NOINLINE
buffer_from_val(BUFFER_TYPE **bufp, MDB_val *val)
{
    BUFFER_TYPE *buf = *bufp;
    if(! buf) {
        buf = (BUFFER_TYPE *) MAKE_BUFFER();
        if(! buf) {
            return NULL;
        }
        *bufp = buf;
    }

    SET_BUFFER(buf, val->mv_data, val->mv_size);
    Py_INCREF(buf);
    return (PyObject *) buf;
}

/**
 * Given an MDB_val `val`, convert it to a Python string or bytes object,
 * depending on the Python version. Returns a new reference to the object on
 * sucess, or NULL on failure.
 */
static PyObject *
string_from_val(MDB_val *val)
{
    return PyBytes_FromStringAndSize(val->mv_data, val->mv_size);
}

/**
 * Given some Python object, try to get at its raw data. For string or bytes
 * objects, this is the object value. For Unicode objects, this is the UTF-8
 * representation of the object value. For all other objects, attempt to invoke
 * the Python 2.x buffer protocol.
 */
static int NOINLINE
val_from_buffer(MDB_val *val, PyObject *buf)
{
    if(PyBytes_CheckExact(buf)) {
        val->mv_data = PyBytes_AS_STRING(buf);
        val->mv_size = PyBytes_GET_SIZE(buf);
        return 0;
    }
#if PY_MAJOR_VERSION >= 3
    if(PyUnicode_CheckExact(buf)) {
        char *data;
        Py_ssize_t size;
        if(! (data = PyUnicode_AsUTF8AndSize(buf, &size))) {
            return -1;
        }
        val->mv_data = data;
        val->mv_size = size;
        return 0;
    }
#endif
    return PyObject_AsReadBuffer(buf,
        (const void **) &val->mv_data,
        (Py_ssize_t *) &val->mv_size);
}


// -------------------
// Concurrency control
// -------------------

static NOINLINE PyThreadState *save_thread(void)
{
    PyThreadState *s = NULL;
    if(drop_gil) {
        s = PyEval_SaveThread();
    }
    return s;
}

static NOINLINE void restore_thread(PyThreadState *state)
{
    if(drop_gil) {
        PyEval_RestoreThread(state);
    }
}

// Like Py_BEGIN_ALLOW_THREADS
#define DROP_GIL \
    { PyThreadState *_save; _save = save_thread();

// Like Py_END_ALLOW_THREADS
#define LOCK_GIL \
    restore_thread(_save); }

#define UNLOCKED(out, e) \
    DROP_GIL \
    out = (e); \
    LOCK_GIL


// ----------
// Exceptions
// ----------

/**
 * Raise an exception appropriate for the given `rc` MDB error code.
 */
static void * NOINLINE
err_set(const char *what, int rc)
{
    size_t count = sizeof error_map / sizeof error_map[0];
    PyObject *klass = Error;
    size_t i;

    if(rc) {
        for(i = 0; i < count; i++) {
            if(error_map[i].code == rc) {
                klass = error_tbl[i];
                break;
            }
        }
    }

    PyErr_Format(klass, "%s: %s", what, mdb_strerror(rc));
    return NULL;
}

static void * NOINLINE
err_invalid(void)
{
    PyErr_Format(Error, "Attempt to operate on closed/deleted/dropped object.");
    return NULL;
}

static void * NOINLINE
type_error(const char *what)
{
    PyErr_Format(PyExc_TypeError, "%s", what);
    return NULL;
}


// ----------------
// Argument parsing
// ----------------

#define OFFSET(k, y) offsetof(struct k, y)
#define SPECSIZE() (sizeof(argspec) / sizeof(argspec[0]))
enum arg_type {
    ARG_DB,     /** DbObject*               */
    ARG_TRANS,  /** TransObject*            */
    ARG_ENV,    /** EnvObject*              */
    ARG_OBJ,    /** PyObject*               */
    ARG_BOOL,   /** int                     */
    ARG_BUF,    /** MDB_val                 */
    ARG_STR,    /** char*                   */
    ARG_INT,    /** int                     */
    ARG_SIZE    /** size_t                  */
};
struct argspec {
    unsigned char type;
    unsigned char string_id;
    unsigned short offset;
};

static PyTypeObject *type_tbl[] = {
    &PyDatabase_Type,
    &PyTransaction_Type,
    &PyEnvironment_Type
};


static int NOINLINE
parse_ulong(PyObject *obj, uint64_t *l, PyObject *max)
{
    int rc = PyObject_RichCompareBool(obj, py_zero, Py_GE);
    if(rc == -1) {
        return -1;
    } else if(! rc) {
        type_error("Integer argument must be >= 0");
        return -1;
    }
    rc = PyObject_RichCompareBool(obj, max, Py_LE);
    if(rc == -1) {
        return -1;
    } else if(! rc) {
        type_error("Integer argument exceeds limit.");
        return -1;
    }
#if PY_MAJOR_VERSION >= 3
    *l = PyLong_AsUnsignedLongLongMask(obj);
#else
    *l = PyInt_AsUnsignedLongLongMask(obj);
#endif
    return 0;
}

/**
 * Parse a single argument specified by `spec` into `out`, returning 0 on
 * success or setting an exception and returning -1 on error.
 */
static int
parse_arg(const struct argspec *spec, PyObject *val, void *out)
{
    void *dst = ((uint8_t *)out) + spec->offset;
    int ret = 0;
    uint64_t l;

    if(val != Py_None) {
        switch((enum arg_type) spec->type) {
        case ARG_DB:
        case ARG_TRANS:
        case ARG_ENV:
            if(val->ob_type != type_tbl[spec->type]) {
                type_error("invalid type");
                return -1;
            }
            /* fallthrough */
        case ARG_OBJ:
            *((PyObject **) dst) = val;
            break;
        case ARG_BOOL:
            *((int *)dst) = val == Py_True;
            break;
        case ARG_BUF:
            ret = val_from_buffer((MDB_val *)dst, val);
            break;
        case ARG_STR: {
            MDB_val mv;
            if(! (ret = val_from_buffer(&mv, val))) {
                *((char **) dst) = mv.mv_data;
            }
            break;
        }
        case ARG_INT:
            if(! (ret = parse_ulong(val, &l, py_int_max))) {
                *((int *) dst) = l;
            }
            break;
        case ARG_SIZE:
            if(! (ret = parse_ulong(val, &l, py_size_max))) {
                *((size_t *) dst) = l;
            }
            break;
        }
    }
    return ret;
}

/**
 * Like PyArg_ParseTupleAndKeywords except types are specialized for this
 * module, keyword strings aren't dup'd every call and the code is >3x smaller.
 */
static int NOINLINE
parse_args(int valid, int specsize, const struct argspec *argspec,
           PyObject *args, PyObject *kwds, void *out)
{
    if(! valid) {
        err_invalid();
        return -1;
    }

    unsigned set = 0;
    unsigned i;
    if(args) {
        int size = PyTuple_GET_SIZE(args);
        if(size > specsize) {
            type_error("too many positional arguments.");
            return -1;
        }
        size = fmin(specsize, size);
        for(i = 0; i < size; i++) {
            if(parse_arg(argspec + i, PyTuple_GET_ITEM(args, i), out)) {
                return -1;
            }
            set |= 1 << i;
        }
    }

    if(kwds) {
        int size = PyDict_Size(kwds);
        int c = 0;

        for(i = 0; i < specsize && c != size; i++) {
            const struct argspec *spec = argspec + i;
            PyObject *kwd = string_tbl[spec->string_id];
            PyObject *val = PyDict_GetItem(kwds, kwd);
            if(val) {
                if(set & (1 << i)) {
                    PyErr_Format(PyExc_TypeError, "duplicate argument: %s",
                                 PyBytes_AS_STRING(kwd));
                    return -1;
                }
                if(parse_arg(spec, val, out)) {
                    return -1;
                }
                c++;
            }
        }

        if(c != size) {
            type_error("unrecognized keyword argument");
            return -1;
        }
    }
    return 0;
}

/**
 * Return 1 if `db` is associated with the given `env`, otherwise raise an
 * exception. Used to prevent DBIs from unrelated envs from being mixed
 * together (which in future, would cause one env to access another's cursor
 * pointers).
 */
static int
db_owner_check(DbObject *db, EnvObject *env)
{
    if(db->env != env) {
        err_set("Database handle belongs to another environment.", 0);
        return 0;
    }
    return 1;
}


// --------------------------------------------------------
// Functionality shared between Transaction and Environment
// --------------------------------------------------------

static PyObject *
make_trans(EnvObject *env, DbObject *db, TransObject *parent, int write, int buffers)
{
    DEBUG("make_trans(env=%p, parent=%p, write=%d, buffers=%d)",
        env, parent, write, buffers)
    if(! env->valid) {
        return err_invalid();
    }

    if(! db) {
        db = env->main_db;
    } else if(! db_owner_check(db, env)) {
        return NULL;
    }

    MDB_txn *parent_txn = NULL;
    if(parent) {
        if(parent->flags & TRANS_RDONLY) {
            return err_set("Read-only transactions cannot be nested.", 0);
        }
        if(! parent->valid) {
            return err_invalid();
        }
        parent_txn = parent->txn;
    }

    if(write && env->readonly) {
        return err_set("Cannot start write transaction with read-only env", 0);
    }

    TransObject *self = PyObject_New(TransObject, &PyTransaction_Type);
    if(! self) {
        return NULL;
    }

    int flags = (write && !env->readonly) ? 0 : MDB_RDONLY;
    int rc;
    UNLOCKED(rc, mdb_txn_begin(env->env, parent_txn, flags, &self->txn));
    if(rc) {
        PyObject_Del(self);
        return err_set("mdb_txn_begin", rc);
    }

    OBJECT_INIT(self)
    LINK_CHILD(env, self)
    self->env = env;
    Py_INCREF(env);
    self->db = db;
    Py_INCREF(db);
#ifdef HAVE_MEMSINK
    self->sink_head = NULL;
#endif
    self->key_buf = NULL;

    self->mutations = 0;
    self->flags = 0;
    if(! write) {
        self->flags |= TRANS_RDONLY;
    }
    if(buffers) {
        self->flags |= TRANS_BUFFERS;
    }
    return (PyObject *)self;
}

static PyObject *
make_cursor(DbObject *db, TransObject *trans)
{
    if(! trans->valid) {
        return err_invalid();
    }
    if(! db) {
        db = trans->env->main_db;
    } else if(! db_owner_check(db, trans->env)) {
        return NULL;
    }

    CursorObject *self = PyObject_New(CursorObject, &PyCursor_Type);
    int rc;
    UNLOCKED(rc, mdb_cursor_open(trans->txn, db->dbi, &self->curs));
    if(rc) {
        PyObject_Del(self);
        return err_set("mdb_cursor_open", rc);
    }

    DEBUG("sizeof cursor = %d", (int) sizeof *self)
    OBJECT_INIT(self)
    LINK_CHILD(trans, self)
    self->positioned = 0;
    self->key_buf = NULL;
    self->val_buf = NULL;
    self->key.mv_size = 0;
    self->val.mv_size = 0;
    self->item_tup = NULL;
    self->trans = trans;
    self->last_mutation = trans->mutations;
    Py_INCREF(self->trans);
    return (PyObject *) self;
}


// --------
// Database
// --------

static DbObject *
db_from_name(EnvObject *env, MDB_txn *txn, const char *name,
             unsigned int flags)
{
    MDB_dbi dbi;
    int rc;

    UNLOCKED(rc, mdb_dbi_open(txn, name, flags, &dbi));
    if(rc) {
        err_set("mdb_dbi_open", rc);
        return NULL;
    }

    DbObject *dbo = PyObject_New(DbObject, &PyDatabase_Type);
    if(! dbo) {
        return NULL;
    }

    OBJECT_INIT(dbo)
    LINK_CHILD(env, dbo)
    dbo->env = env; // no refcount
    dbo->dbi = dbi;
    DEBUG("DbObject '%s' opened at %p", name, dbo)
    return dbo;
}

/**
 * Use a temporary transaction to manufacture a new _Database object for
 * `name`.
 */
static DbObject *
txn_db_from_name(EnvObject *env, const char *name,
                 unsigned int flags)
{
    int rc;
    MDB_txn *txn;

    int begin_flags = (name == NULL || env->readonly) ? MDB_RDONLY : 0;
    UNLOCKED(rc, mdb_txn_begin(env->env, NULL, begin_flags, &txn));
    if(rc) {
        err_set("mdb_txn_begin", rc);
        return NULL;
    }

    DbObject *dbo = db_from_name(env, txn, name, flags);
    if(! dbo) {
        DROP_GIL
        mdb_txn_abort(txn);
        LOCK_GIL
        return NULL;
    }

    UNLOCKED(rc, mdb_txn_commit(txn));
    if(rc) {
        Py_DECREF(dbo);
        return err_set("mdb_txn_commit", rc);
    }
    return dbo;
}

static int
db_clear(DbObject *self)
{
    if(self->env) {
        UNLINK_CHILD(self->env, self)
        self->env = NULL;
    }
    self->valid = 0;
    return 0;
}

/**
 * _Database.__del__()
 */
static void
db_dealloc(DbObject *self)
{
    db_clear(self);
    PyObject_Del(self);
}

static PyTypeObject PyDatabase_Type = {
    PyObject_HEAD_INIT(NULL)
    .tp_basicsize = sizeof(DbObject),
    .tp_dealloc = (destructor) db_dealloc,
    .tp_clear = (inquiry) db_clear,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_name = "_Database"
};


// -----------
// Environment
// -----------

static int
env_clear(EnvObject *self)
{
    DEBUG("killing env..")
    if(self->env) {
        INVALIDATE(self)
        DEBUG("Closing env")
        DROP_GIL
        mdb_env_close(self->env);
        LOCK_GIL
        self->env = NULL;
    }
    if(self->main_db) {
        Py_CLEAR(self->main_db);
    }
    return 0;
}

/**
 * Environment.__del__()
 */
static void
env_dealloc(EnvObject *self)
{
    env_clear(self);
    PyObject_Del(self);
}

/**
 * Environment() -> new object.
 */
static PyObject *
env_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    struct env_new {
        char *path;
        size_t map_size;
        int subdir;
        int readonly;
        int metasync;
        int sync;
        int map_async;
        int mode;
        int create;
        int readahead;
        int writemap;
        int max_readers;
        int max_dbs;
        ssize_t max_spare_txns;
        ssize_t max_spare_cursors;
        ssize_t max_spare_iters;
    } arg = {NULL, 10485760, 1, 0, 1, 1, 0, 0644, 1, 1, 0, 126, 0, 1, 32, 32};

    static const struct argspec argspec[] = {
        {ARG_STR, PATH_S, OFFSET(env_new, path)},
        {ARG_SIZE, MAP_SIZE_S, OFFSET(env_new, map_size)},
        {ARG_BOOL, SUBDIR_S, OFFSET(env_new, subdir)},
        {ARG_BOOL, READONLY_S, OFFSET(env_new, readonly)},
        {ARG_BOOL, METASYNC_S, OFFSET(env_new, metasync)},
        {ARG_BOOL, SYNC_S, OFFSET(env_new, sync)},
        {ARG_BOOL, MAP_ASYNC_S, OFFSET(env_new, map_async)},
        {ARG_INT, MODE_S, OFFSET(env_new, mode)},
        {ARG_BOOL, CREATE_S, OFFSET(env_new, create)},
        {ARG_BOOL, READAHEAD_S, OFFSET(env_new, readahead)},
        {ARG_BOOL, WRITEMAP_S, OFFSET(env_new, writemap)},
        {ARG_INT, MAX_READERS_S, OFFSET(env_new, max_readers)},
        {ARG_INT, MAX_DBS_S, OFFSET(env_new, max_dbs)},
        {ARG_SIZE, MAX_SPARE_TXNS_S, OFFSET(env_new, max_spare_txns)},
        {ARG_SIZE, MAX_SPARE_CURSORS_S, OFFSET(env_new, max_spare_cursors)},
        {ARG_SIZE, MAX_SPARE_ITERS_S, OFFSET(env_new, max_spare_iters)}
    };

    if(parse_args(1, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }

    if(! arg.path) {
        return type_error("'path' argument required");
    }

    EnvObject *self = PyObject_New(EnvObject, type);
    if(! self) {
        return NULL;
    }

    OBJECT_INIT(self)
    self->weaklist = NULL;
    self->main_db = NULL;
    self->env = NULL;

    int rc;
    if((rc = mdb_env_create(&self->env))) {
        err_set("mdb_env_create", rc);
        goto fail;
    }

    if((rc = mdb_env_set_mapsize(self->env, arg.map_size))) {
        err_set("mdb_env_set_mapsize", rc);
        goto fail;
    }

    if((rc = mdb_env_set_maxreaders(self->env, arg.max_readers))) {
        err_set("mdb_env_set_maxreaders", rc);
        goto fail;
    }

    if((rc = mdb_env_set_maxdbs(self->env, arg.max_dbs))) {
        err_set("mdb_env_set_maxdbs", rc);
        goto fail;
    }

    if(arg.create && arg.subdir) {
        struct stat st;
        errno = 0;
        stat(arg.path, &st);
        if(errno == ENOENT) {
            if(mkdir(arg.path, 0700)) {
                PyErr_SetFromErrnoWithFilename(PyExc_OSError, arg.path);
                goto fail;
            }
        }
    }

    int flags = MDB_NOTLS;
    if(! arg.subdir) {
        flags |= MDB_NOSUBDIR;
    }
    if(arg.readonly) {
        flags |= MDB_RDONLY;
    }
    self->readonly = arg.readonly;
    if(! arg.metasync) {
        flags |= MDB_NOMETASYNC;
    }
    if(! arg.sync) {
        flags |= MDB_NOSYNC;
    }
    if(arg.map_async) {
        flags |= MDB_MAPASYNC;
    }
    if(! arg.readahead) {
        flags |= MDB_NORDAHEAD;
    }
    if(arg.writemap) {
        flags |= MDB_WRITEMAP;
    }

    DEBUG("mdb_env_open(%p, '%s', %d, %o);", self->env, arg.path, flags, arg.mode)
    UNLOCKED(rc, mdb_env_open(self->env, arg.path, flags, arg.mode));
    if(rc) {
        err_set(arg.path, rc);
        goto fail;
    }

    self->main_db = txn_db_from_name(self, NULL, 0);
    if(self->main_db) {
        self->valid = 1;
        DEBUG("EnvObject '%s' opened at %p", arg.path, self)
        return (PyObject *) self;
    }

fail:
    DEBUG("initialization failed")
    if(self) {
        env_dealloc(self);
    }
    return NULL;
}

/**
 * Environment.begin()
 */
static PyObject *
env_begin(EnvObject *self, PyObject *args, PyObject *kwds)
{
    struct env_begin {
        DbObject *db;
        TransObject *parent;
        int write;
        int buffers;
    } arg = {self->main_db, NULL, 0, 0};

    static const struct argspec argspec[] = {
        {ARG_DB, DB_S, OFFSET(env_begin, db)},
        {ARG_TRANS, PARENT_S, OFFSET(env_begin, parent)},
        {ARG_BOOL, WRITE_S, OFFSET(env_begin, write)},
        {ARG_BOOL, BUFFERS_S, OFFSET(env_begin, buffers)},
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }
    return make_trans(self, arg.db, arg.parent, arg.write, arg.buffers);
}

/**
 * Environment.close()
 */
static PyObject *
env_close(EnvObject *self)
{
    if(self->valid) {
        INVALIDATE(self)
        self->valid = 0;
        DEBUG("Closing env")
        DROP_GIL
        mdb_env_close(self->env);
        LOCK_GIL
        self->env = NULL;
    }
    Py_RETURN_NONE;
}

/**
 * Environment.copy()
 */
static PyObject *
env_copy(EnvObject *self, PyObject *args)
{
    struct env_copy {
        char *path;
    } arg = {NULL};

    static const struct argspec argspec[] = {
        {ARG_STR, PATH_S, OFFSET(env_copy, path)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, NULL, &arg)) {
        return NULL;
    }
    if(! arg.path) {
        return type_error("path argument required");
    }
    int rc;
    UNLOCKED(rc, mdb_env_copy(self->env, arg.path));
    if(rc) {
        return err_set("mdb_env_copy", rc);
    }
    Py_RETURN_NONE;
}

/**
 * Environment.copyfd(fd)
 */
static PyObject *
env_copyfd(EnvObject *self, PyObject *args)
{
    struct env_copyfd {
        int fd;
    } arg = {-1};

    static const struct argspec argspec[] = {
        {ARG_INT, FD_S, OFFSET(env_copyfd, fd)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, NULL, &arg)) {
        return NULL;
    }
    if(arg.fd == -1) {
        return type_error("fd argument required");
    }
    int rc;
    UNLOCKED(rc, mdb_env_copyfd(self->env, arg.fd));
    if(rc) {
        return err_set("mdb_env_copyfd", rc);
    }
    Py_RETURN_NONE;
}

/**
 * Environment.info() -> dict
 */
static PyObject *
env_info(EnvObject *self)
{
    static const struct dict_field fields[] = {
        {TYPE_ADDR, "map_addr",    offsetof(MDB_envinfo, me_mapaddr)},
        {TYPE_SIZE, "map_size",    offsetof(MDB_envinfo, me_mapsize)},
        {TYPE_SIZE, "last_pgno",   offsetof(MDB_envinfo, me_last_pgno)},
        {TYPE_SIZE, "last_txnid",  offsetof(MDB_envinfo, me_last_txnid)},
        {TYPE_UINT, "max_readers", offsetof(MDB_envinfo, me_maxreaders)},
        {TYPE_UINT, "num_readers", offsetof(MDB_envinfo, me_numreaders)},
        {TYPE_EOF, NULL, 0}
    };

    if(! self->valid) {
        return err_invalid();
    }

    MDB_envinfo info;
    int rc;
    UNLOCKED(rc, mdb_env_info(self->env, &info));
    if(rc) {
        err_set("mdb_env_info", rc);
        return NULL;
    }
    return dict_from_fields(&info, fields);
}

/**
 * Environment.open_db() -> handle
 */
static PyObject *
env_open_db(EnvObject *self, PyObject *args, PyObject *kwds)
{
    struct env_open_db {
        const char *name;
        TransObject *txn;
        int reverse_key;
        int dupsort;
        int create;
    } arg = {NULL, NULL, 0, 0, 1};

    static const struct argspec argspec[] = {
        {ARG_STR, NAME_S, OFFSET(env_open_db, name)},
        {ARG_TRANS, TXN_S, OFFSET(env_open_db, txn)},
        {ARG_BOOL, REVERSE_KEY_S, OFFSET(env_open_db, reverse_key)},
        {ARG_BOOL, DUPSORT_S, OFFSET(env_open_db, dupsort)},
        {ARG_BOOL, CREATE_S, OFFSET(env_open_db, create)},
    };

    if(parse_args(1, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }

    int flags = 0;
    if(arg.reverse_key) {
        flags |= MDB_REVERSEKEY;
    }
    if(arg.dupsort) {
        flags |= MDB_DUPSORT;
    }
    if(arg.create) {
        flags |= MDB_CREATE;
    }

    if(arg.txn) {
        return (PyObject *) db_from_name(self, arg.txn->txn, arg.name, flags);
    } else {
        return (PyObject *) txn_db_from_name(self, arg.name, flags);
    }
}

/**
 * Environment.path() -> Unicode
 */
static PyObject *
env_path(EnvObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }

    const char *path;
    int rc;
    if((rc = mdb_env_get_path(self->env, &path))) {
        return err_set("mdb_env_get_path", rc);
    }
    return PyUnicode_FromString(path);
}

static const struct dict_field mdb_stat_fields[] = {
    {TYPE_UINT, "psize",          offsetof(MDB_stat, ms_psize)},
    {TYPE_UINT, "depth",          offsetof(MDB_stat, ms_depth)},
    {TYPE_SIZE, "branch_pages",   offsetof(MDB_stat, ms_branch_pages)},
    {TYPE_SIZE, "leaf_pages",     offsetof(MDB_stat, ms_leaf_pages)},
    {TYPE_SIZE, "overflow_pages", offsetof(MDB_stat, ms_overflow_pages)},
    {TYPE_SIZE, "entries",        offsetof(MDB_stat, ms_entries)},
    {TYPE_EOF, NULL, 0}
};

/**
 * Environment.stat() -> dict
 */
static PyObject *
env_stat(EnvObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }

    MDB_stat st;
    int rc;
    UNLOCKED(rc, mdb_env_stat(self->env, &st));
    if(rc) {
        err_set("mdb_env_stat", rc);
        return NULL;
    }
    return dict_from_fields(&st, mdb_stat_fields);
}

/**
 * Callback to receive string result for env_readers(). Return 0 on success or
 * -1 on error.
 */
static int env_readers_callback(const char *msg, void *str_)
{
    PyObject **str = str_;
    PyObject *s = PyUnicode_FromString(msg);
    if(! s) {
        return -1;
    }
    PyObject *new = PyUnicode_Concat(*str, s);
    Py_CLEAR(*str);
    *str = new;
    if(! new) {
        return -1;
    }
    return 0;
}

/**
 * Environment.readers() -> string
 */
static PyObject *
env_readers(EnvObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }

    PyObject *str = PyUnicode_FromString("");
    if(! str) {
        return NULL;
    }

    if(mdb_reader_list(self->env, env_readers_callback, &str)) {
        Py_CLEAR(str);
    }
    return str;
}

/**
 * Environment.reader_check() -> int
 */
static PyObject *
env_reader_check(EnvObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }

    int dead;
    int rc = mdb_reader_check(self->env, &dead);
    if(rc) {
        return err_set("mdb_reader_check", rc);
    }
    return PyLong_FromLongLong(dead);
}

/**
 * Environment.sync()
 */
static PyObject *
env_sync(EnvObject *self, PyObject *args)
{
    struct env_sync {
        int force;
    } arg = {0};

    static const struct argspec argspec[] = {
        {ARG_BOOL, FORCE_S, OFFSET(env_sync, force)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, NULL, &arg)) {
        return NULL;
    }

    int rc;
    UNLOCKED(rc, mdb_env_sync(self->env, arg.force));
    if(rc) {
        return err_set("mdb_env_sync", rc);
    }
    Py_RETURN_NONE;
}

static struct PyMethodDef env_methods[] = {
    {"begin", (PyCFunction)env_begin, METH_VARARGS|METH_KEYWORDS},
    {"close", (PyCFunction)env_close, METH_NOARGS},
    {"copy", (PyCFunction)env_copy, METH_VARARGS},
    {"copyfd", (PyCFunction)env_copyfd, METH_VARARGS},
    {"info", (PyCFunction)env_info, METH_NOARGS},
    {"open_db", (PyCFunction)env_open_db, METH_VARARGS|METH_KEYWORDS},
    {"path", (PyCFunction)env_path, METH_NOARGS},
    {"stat", (PyCFunction)env_stat, METH_NOARGS},
    {"readers", (PyCFunction)env_readers, METH_NOARGS},
    {"reader_check", (PyCFunction)env_reader_check, METH_NOARGS},
    {"sync", (PyCFunction)env_sync, METH_VARARGS},
    {NULL, NULL}
};

static PyTypeObject PyEnvironment_Type = {
    PyObject_HEAD_INIT(0)
    .tp_basicsize = sizeof(EnvObject),
    .tp_dealloc = (destructor) env_dealloc,
    .tp_clear = (inquiry) env_clear,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = env_methods,
    .tp_name = "Environment",
    .tp_new = env_new,
    .tp_weaklistoffset = offsetof(EnvObject, weaklist),
};


// -------
// Cursors
// -------

static int
cursor_clear(CursorObject *self)
{
    if(self->valid) {
        INVALIDATE(self)
        UNLINK_CHILD(self->trans, self)
        DROP_GIL
        mdb_cursor_close(self->curs);
        LOCK_GIL
        self->valid = 0;
    }
    if(self->key_buf) {
        SET_BUFFER(self->key_buf, "", 0);
        Py_CLEAR(self->key_buf);
    }
    if(self->val_buf) {
        SET_BUFFER(self->val_buf, "", 0);
        Py_CLEAR(self->val_buf);
    }
    if(self->item_tup) {
        Py_CLEAR(self->item_tup);
    }
    Py_CLEAR(self->trans);
    return 0;
}

/**
 * Cursor.__del__()
 */
static void
cursor_dealloc(CursorObject *self)
{
    DEBUG("destroying cursor")
    cursor_clear(self);
    PyObject_Del(self);
}

/**
 * Environment.Cursor(db, trans) -> new instance.
 */
static PyObject *
cursor_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    struct cursor_new {
        DbObject *db;
        TransObject *trans;
    } arg = {NULL, NULL};

    static const struct argspec argspec[] = {
        {ARG_DB, DB_S, OFFSET(cursor_new, db)},
        {ARG_TRANS, TXN_S, OFFSET(cursor_new, trans)}
    };

    if(parse_args(1, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }

    if(! (arg.db && arg.trans)) {
        return type_error("db and transaction parameters required.");
    }
    return make_cursor(arg.db, arg.trans);
}

/**
 * Cursor.count() -> long
 */
static PyObject *
cursor_count(CursorObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }

    size_t count;
    int rc;
    UNLOCKED(rc, mdb_cursor_count(self->curs, &count));
    if(rc) {
        return err_set("mdb_cursor_count", rc);
    }
    return PyLong_FromUnsignedLongLong(count);
}

/**
 * Apply `op` to the cursor using the Cursor instance's `key` and `val`
 * MDB_vals. On completion, check for an error and if one occurred, set the
 * cursor to the unpositioned state. Finally record the containing
 * transaction's last mutation count, so we know if `key` and `val` become
 * invalid before the next attempt to read them.
 */
static int
_cursor_get_c(CursorObject *self, enum MDB_cursor_op op)
{
    int rc;
    UNLOCKED(rc, mdb_cursor_get(self->curs, &self->key, &self->val, op));
    self->positioned = rc == 0;
    self->last_mutation = self->trans->mutations;
    if(rc) {
        self->key.mv_size = 0;
        self->val.mv_size = 0;
        if(rc != MDB_NOTFOUND) {
            if(! (rc == EINVAL && op == MDB_GET_CURRENT)) {
                err_set("mdb_cursor_get", rc);
                return -1;
            }
        }
    }
    return 0;
}

/**
 * Wrap _cursor_get_c() to return True or False depending on whether the
 * Cursor's final state is positioned.
 */
static PyObject *
_cursor_get(CursorObject *self, enum MDB_cursor_op op)
{
    if(_cursor_get_c(self, op)) {
        return NULL;
    }
    PyObject *res = self->positioned ? Py_True : Py_False;
    Py_INCREF(res);
    return res;
}

/**
 * Cursor.delete() -> bool
 */
static PyObject *
cursor_delete(CursorObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    PyObject *ret = Py_False;
    if(self->positioned) {
        DEBUG("deleting key '%.*s'",
              (int) self->key.mv_size,
              (char*) self->key.mv_data)
        int rc;
        UNLOCKED(rc, mdb_cursor_del(self->curs, 0));
        self->trans->mutations++;
        if(rc) {
            return err_set("mdb_cursor_del", rc);
        }
        ret = Py_True;
        _cursor_get_c(self, MDB_GET_CURRENT);
    }
    Py_INCREF(ret);
    return ret;
}

/**
 * Cursor.first() -> bool
 */
static PyObject *
cursor_first(CursorObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    return _cursor_get(self, MDB_FIRST);
}

static PyObject *
cursor_value(CursorObject *self);

/**
 * Cursor.get() -> result
 */
static PyObject *
cursor_get(CursorObject *self, PyObject *args, PyObject *kwds)
{
    if(! self->valid) {
        return err_invalid();
    }

    struct cursor_get {
        MDB_val key;
        PyObject *default_;
    } arg = {{0, 0}, Py_None};

    static const struct argspec argspec[] = {
        {ARG_BUF, KEY_S, OFFSET(cursor_get, key)},
        {ARG_OBJ, DEFAULT_S, OFFSET(cursor_get, default_)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }

    if(! arg.key.mv_data) {
        return type_error("key must be given.");
    }

    self->key = arg.key;
    if(_cursor_get_c(self, MDB_SET_KEY)) {
        return NULL;
    }
    if(! self->positioned) {
        Py_INCREF(arg.default_);
        return arg.default_;
    }
    return cursor_value(self);
}

/**
 * Cursor.item() -> (key, value)
 */
static PyObject *
cursor_item(CursorObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    // Must refresh `key` and `val` following mutation.
    if(self->last_mutation != self->trans->mutations &&
       _cursor_get_c(self, MDB_GET_CURRENT)) {
        return NULL;
    }
    if(self->trans->flags & TRANS_BUFFERS) {
        if(! buffer_from_val(&self->key_buf, &self->key)) {
            return NULL;
        }
        if(! buffer_from_val(&self->val_buf, &self->val)) {
            return NULL;
        }
        if(! self->item_tup) {
            self->item_tup = PyTuple_Pack(2, self->key_buf, self->val_buf);
        }
        if(! self->item_tup) {
            return NULL;
        }
        Py_INCREF(self->item_tup);
        return self->item_tup;
    }

    PyObject *key = string_from_val(&self->key);
    if(! key) {
        return NULL;
    }
    PyObject *val = string_from_val(&self->val);
    if(! val) {
        Py_DECREF(key);
        return NULL;
    }
    PyObject *tup = PyTuple_New(2);
    if(! tup) {
        Py_DECREF(key);
        Py_DECREF(val);
        return NULL;
    }
    PyTuple_SET_ITEM(tup, 0, key);
    PyTuple_SET_ITEM(tup, 1, val);
    return tup;
}

/**
 * Cursor.key() -> result
 */
static PyObject *
cursor_key(CursorObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    // Must refresh `key` and `val` following mutation.
    if(self->last_mutation != self->trans->mutations &&
       _cursor_get_c(self, MDB_GET_CURRENT)) {
        return NULL;
    }
    if(self->trans->flags & TRANS_BUFFERS) {
        if(! buffer_from_val(&self->key_buf, &self->key)) {
            return NULL;
        }
        Py_INCREF(self->key_buf);
        return (PyObject *) self->key_buf;
    }
    return string_from_val(&self->key);
}

/**
 * Cursor.last() -> bool
 */
static PyObject *
cursor_last(CursorObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    return _cursor_get(self, MDB_LAST);
}

/**
 * Cursor.next() -> bool
 */
static PyObject *
cursor_next(CursorObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    return _cursor_get(self, MDB_NEXT);
}

/**
 * Cursor.prev() -> bool
 */
static PyObject *
cursor_prev(CursorObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    return _cursor_get(self, MDB_PREV);
}

/**
 * Cursor.put() -> bool
 */
static PyObject *
cursor_put(CursorObject *self, PyObject *args, PyObject *kwds)
{
    struct cursor_put {
        MDB_val key;
        MDB_val val;
        int dupdata;
        int overwrite;
        int append;
    } arg = {{0, 0}, {0, 0}, 0, 1, 0};

    static const struct argspec argspec[] = {
        {ARG_BUF, KEY_S, OFFSET(cursor_put, key)},
        {ARG_BUF, VALUE_S, OFFSET(cursor_put, val)},
        {ARG_BOOL, DUPDATA_S, OFFSET(cursor_put, dupdata)},
        {ARG_BOOL, OVERWRITE_S, OFFSET(cursor_put, overwrite)},
        {ARG_BOOL, APPEND_S, OFFSET(cursor_put, append)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }

    int flags = 0;
    if(! arg.dupdata) {
        flags |= MDB_NODUPDATA;
    }
    if(! arg.overwrite) {
        flags |= MDB_NOOVERWRITE;
    }
    if(arg.append) {
        flags |= MDB_APPEND;
    }

    int rc;
    UNLOCKED(rc, mdb_cursor_put(self->curs, &arg.key, &arg.val, flags));
    self->trans->mutations++;
    if(rc) {
        if(rc == MDB_KEYEXIST) {
            Py_RETURN_FALSE;
        }
        return err_set("mdb_put", rc);
    }
    Py_RETURN_TRUE;
}

/**
 * Cursor.replace() -> None|result
 */
static PyObject *
cursor_replace(CursorObject *self, PyObject *args, PyObject *kwds)
{
    struct cursor_replace {
        MDB_val key;
        MDB_val val;
    } arg = {{0, 0}, {0, 0}};

    static const struct argspec argspec[] = {
        {ARG_BUF, KEY_S, OFFSET(cursor_replace, key)},
        {ARG_BUF, VALUE_S, OFFSET(cursor_replace, val)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }

    /* arg.val is updated if MDB_KEYEXIST. */
    MDB_val newval = arg.val;
    int flags = MDB_NOOVERWRITE;
    int rc;
    UNLOCKED(rc, mdb_cursor_put(self->curs, &arg.key, &arg.val, flags));
    self->trans->mutations++;
    if(! rc) {
        Py_RETURN_NONE;
    } else if(rc != MDB_KEYEXIST) {
        return err_set("mdb_put", rc);
    }

    PyObject *old = string_from_val(&arg.val);
    if(! old) {
        return NULL;
    }

    UNLOCKED(rc, mdb_cursor_put(self->curs, &arg.key, &newval, 0));
    if(rc) {
        Py_DECREF(old);
        return err_set("mdb_put", rc);
    }

    return old;
}

/**
 * Cursor.pop() -> None|result
 */
static PyObject *
cursor_pop(CursorObject *self, PyObject *args, PyObject *kwds)
{
    struct cursor_pop {
        MDB_val key;
    } arg = {{0, 0}};

    static const struct argspec argspec[] = {
        {ARG_BUF, KEY_S, OFFSET(cursor_pop, key)},
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }

    self->key = arg.key;
    if(_cursor_get_c(self, MDB_SET_KEY)) {
        return NULL;
    }
    if(! self->positioned) {
        Py_RETURN_NONE;
    }
    PyObject *old = string_from_val(&self->val);
    if(! old) {
        return NULL;
    }

    int rc;
    UNLOCKED(rc, mdb_cursor_del(self->curs, 0));
    self->trans->mutations++;
    if(rc) {
        Py_DECREF(old);
        return err_set("mdb_cursor_del", rc);
    }
    return old;
}

/**
 * Cursor.set_key() -> bool
 */
static PyObject *
cursor_set_key(CursorObject *self, PyObject *arg)
{
    if(! self->valid) {
        return err_invalid();
    }
    if(val_from_buffer(&self->key, arg)) {
        return NULL;
    }
    return _cursor_get(self, MDB_SET_KEY);
}

/**
 * Cursor.set_range() -> bool
 */
static PyObject *
cursor_set_range(CursorObject *self, PyObject *arg)
{
    if(! self->valid) {
        return err_invalid();
    }
    if(val_from_buffer(&self->key, arg)) {
        return NULL;
    }
    if(self->key.mv_size) {
        return _cursor_get(self, MDB_SET_RANGE);
    }
    return _cursor_get(self, MDB_FIRST);
}

/**
 * Cursor.value() -> result
 */
static PyObject *
cursor_value(CursorObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    // Must refresh `key` and `val` following mutation.
    if(self->last_mutation != self->trans->mutations &&
       _cursor_get_c(self, MDB_GET_CURRENT)) {
        return NULL;
    }
    if(self->trans->flags & TRANS_BUFFERS) {
        if(! buffer_from_val(&self->val_buf, &self->val)) {
            return NULL;
        }
        Py_INCREF(self->val_buf);
        return (PyObject *) self->val_buf;
    }
    return string_from_val(&self->val);
}

static PyObject *
new_iterator(CursorObject *cursor, IterValFunc val_func, MDB_cursor_op op)
{
    IterObject *iter = PyObject_New(IterObject, &PyIterator_Type);
    if(iter) {
        iter->val_func = val_func;
        iter->curs = cursor;
        Py_INCREF(cursor);
        iter->started = 0;
        iter->op = op;
    }
    return (PyObject *) iter;
}

static PyObject *
iter_from_args(CursorObject *self, PyObject *args, PyObject *kwds,
               enum MDB_cursor_op pos_op, enum MDB_cursor_op op)
{
    struct iter_from_args {
        int keys;
        int values;
    } arg = {1, 1};

    static const struct argspec argspec[] = {
        {ARG_BOOL, KEYS_S, OFFSET(iter_from_args, keys)},
        {ARG_BOOL, VALUES_S, OFFSET(iter_from_args, values)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }

    if(! self->positioned) {
        if(_cursor_get_c(self, pos_op)) {
            return NULL;
        }
    }

    void *val_func;
    if(! arg.values) {
        val_func = cursor_key;
    } else if(! arg.keys) {
        val_func = cursor_value;
    } else {
        val_func = cursor_item;
    }
    return new_iterator(self, val_func, op);
}

static PyObject *
cursor_iter(CursorObject *self)
{
    return iter_from_args(self, NULL, NULL, MDB_FIRST, MDB_NEXT);
}

/**
 * Cursor.iternext() -> Iterator
 */
static PyObject *
cursor_iternext(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    return iter_from_args(self, args, kwargs, MDB_FIRST, MDB_NEXT);
}

/**
 * Cursor.iterprev() -> Iterator
 */
static PyObject *
cursor_iterprev(CursorObject *self, PyObject *args, PyObject *kwargs)
{
    return iter_from_args(self, args, kwargs, MDB_LAST, MDB_PREV);
}

/**
 * Cursor._iter_from() -> Iterator
 */
static PyObject *
cursor_iter_from(CursorObject *self, PyObject *args)
{
    struct cursor_iter_from {
        MDB_val key;
        int reverse;
    } arg = {{0, 0}, 0};

    static const struct argspec argspec[] = {
        {ARG_BUF, KEY_S, OFFSET(cursor_iter_from, key)},
        {ARG_BOOL, REVERSE_S, OFFSET(cursor_iter_from, reverse)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, NULL, &arg)) {
        return NULL;
    }

    int rc;
    if((! arg.key.mv_size) && (! arg.reverse)) {
        rc = _cursor_get_c(self, MDB_FIRST);
    } else {
        self->key = arg.key;
        rc = _cursor_get_c(self, MDB_SET_RANGE);
    }

    if(rc) {
        return NULL;
    }

    enum MDB_cursor_op op = MDB_NEXT;
    if(arg.reverse) {
        op = MDB_PREV;
        if(! self->positioned) {
            if(_cursor_get_c(self, MDB_LAST)) {
                return NULL;
            }
        }
    }

    DEBUG("positioned? %d", self->positioned)
    return new_iterator(self, (void *)cursor_item, op);
}

static struct PyMethodDef cursor_methods[] = {
    {"count", (PyCFunction)cursor_count, METH_NOARGS},
    {"delete", (PyCFunction)cursor_delete, METH_NOARGS},
    {"first", (PyCFunction)cursor_first, METH_NOARGS},
    {"get", (PyCFunction)cursor_get, METH_VARARGS|METH_KEYWORDS},
    {"item", (PyCFunction)cursor_item, METH_NOARGS},
    {"iternext", (PyCFunction)cursor_iternext, METH_VARARGS|METH_KEYWORDS},
    {"iterprev", (PyCFunction)cursor_iterprev, METH_VARARGS|METH_KEYWORDS},
    {"key", (PyCFunction)cursor_key, METH_NOARGS},
    {"last", (PyCFunction)cursor_last, METH_NOARGS},
    {"next", (PyCFunction)cursor_next, METH_NOARGS},
    {"prev", (PyCFunction)cursor_prev, METH_NOARGS},
    {"put", (PyCFunction)cursor_put, METH_VARARGS|METH_KEYWORDS},
    {"replace", (PyCFunction)cursor_replace, METH_VARARGS|METH_KEYWORDS},
    {"pop", (PyCFunction)cursor_pop, METH_VARARGS|METH_KEYWORDS},
    {"set_key", (PyCFunction)cursor_set_key, METH_O},
    {"set_range", (PyCFunction)cursor_set_range, METH_O},
    {"value", (PyCFunction)cursor_value, METH_NOARGS},
    {"_iter_from", (PyCFunction)cursor_iter_from, METH_VARARGS},
    {NULL, NULL}
};

static PyTypeObject PyCursor_Type = {
    PyObject_HEAD_INIT(0)
    .tp_basicsize = sizeof(CursorObject),
    .tp_dealloc = (destructor) cursor_dealloc,
    .tp_clear = (inquiry) cursor_clear,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_iter = (getiterfunc)cursor_iter,
    .tp_methods = cursor_methods,
    .tp_name = "Cursor",
    .tp_new = cursor_new
};


// ---------
// Iterators
// ---------


/**
 * Iterator.__del__()
 */
static void
iter_dealloc(IterObject *self)
{
    DEBUG("destroying iterator")
    Py_CLEAR(self->curs);
    PyObject_Del(self);
}

/**
 * Iterator.__iter__() -> self
 */
static PyObject *
iter_iter(IterObject *self)
{
    Py_INCREF(self);
    return (PyObject *)self;
}

/**
 * Iterator.next() -> result
 */
static PyObject *
iter_next(IterObject *self)
{
    if(! self->curs->valid) {
        return err_invalid();
    }
    if(! self->curs->positioned) {
        return NULL;
    }
    if(self->started) {
        if(_cursor_get_c(self->curs, self->op)) {
            return NULL;
        }
        if(! self->curs->positioned) {
            return NULL;
        }
    }
    PyObject *val = self->val_func(self->curs);
    self->started = 1;
    return val;
}

static struct PyMethodDef iter_methods[] = {
    {"next", (PyCFunction)cursor_next, METH_NOARGS},
    {NULL, NULL}
};

static PyTypeObject PyIterator_Type = {
    PyObject_HEAD_INIT(0)
    .tp_basicsize = sizeof(IterObject),
    .tp_dealloc = (destructor) iter_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_iter = (getiterfunc)iter_iter,
    .tp_iternext = (iternextfunc)iter_next,
    .tp_methods = iter_methods,
    .tp_name = "Iterator"
};


// ------------
// Transactions
// ------------

static int
trans_clear(TransObject *self)
{
    if(self->valid) {
        INVALIDATE(self)
#ifdef HAVE_MEMSINK
        ms_notify((PyObject *) self, &self->sink_head);
#endif
        if(self->txn) {
            DEBUG("aborting")
            DROP_GIL
            mdb_txn_abort(self->txn);
            LOCK_GIL
            self->txn = NULL;
        }
        Py_CLEAR(self->db);
        self->valid = 0;
    }
    UNLINK_CHILD(self->env, self)
    Py_CLEAR(self->env);
    return 0;
}

/**
 * Transaction.__del__()
 */
static void
trans_dealloc(TransObject *self)
{
    DEBUG("deleting trans")
    trans_clear(self);
    PyObject_Del(self);
}

/**
 * Transaction(env, db) -> new instance.
 */
static PyObject *
trans_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    struct trans_new {
        EnvObject *env;
        DbObject *db;
        TransObject *parent;
        int write;
        int buffers;
    } arg = {NULL, NULL, NULL, 0, 0};

    static const struct argspec argspec[] = {
        {ARG_ENV, ENV_S, OFFSET(trans_new, env)},
        {ARG_DB, DB_S, OFFSET(trans_new, db)},
        {ARG_TRANS, PARENT_S, OFFSET(trans_new, parent)},
        {ARG_BOOL, WRITE_S, OFFSET(trans_new, write)},
        {ARG_BOOL, BUFFERS_S, OFFSET(trans_new, buffers)}
    };

    if(parse_args(1, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }
    if(! arg.env) {
        return type_error("'env' argument required");
    }
    return make_trans(arg.env, arg.db, arg.parent, arg.write, arg.buffers);
}

/**
 * Transaction.abort()
 */
static PyObject *
trans_abort(TransObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    DEBUG("aborting")
    INVALIDATE(self)
#ifdef HAVE_MEMSINK
    ms_notify((PyObject *) self, &self->sink_head);
#endif
    DROP_GIL
    mdb_txn_abort(self->txn);
    LOCK_GIL
    self->txn = NULL;
    self->valid = 0;
    Py_RETURN_NONE;
}

/**
 * Transaction.commit()
 */
static PyObject *
trans_commit(TransObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    DEBUG("committing")
    INVALIDATE(self)
#ifdef HAVE_MEMSINK
    ms_notify((PyObject *) self, &self->sink_head);
#endif
    int rc;
    UNLOCKED(rc, mdb_txn_commit(self->txn));
    self->txn = NULL;
    self->valid = 0;
    if(rc) {
        return err_set("mdb_txn_commit", rc);
    }
    Py_RETURN_NONE;
}

/**
 * Transaction.cursor() -> Cursor
 */
static PyObject *
trans_cursor(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_cursor {
        DbObject *db;
    } arg = {self->db};

    static const struct argspec argspec[] = {
        {ARG_DB, DB_S, OFFSET(trans_cursor, db)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }
    return make_cursor(arg.db, self);
}

/**
 * Transaction.delete() -> bool
 */
static PyObject *
trans_delete(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_delete {
        MDB_val key;
        MDB_val val;
        DbObject *db;
    } arg = {{0, 0}, {0, 0}, self->db};

    static const struct argspec argspec[] = {
        {ARG_BUF, KEY_S, OFFSET(trans_delete, key)},
        {ARG_BUF, VALUE_S, OFFSET(trans_delete, val)},
        {ARG_DB, DB_S, OFFSET(trans_delete, db)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }
    if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }
    MDB_val *val_ptr = arg.val.mv_size ? &arg.val : NULL;
    self->mutations++;
    int rc;
    UNLOCKED(rc, mdb_del(self->txn, arg.db->dbi, &arg.key, val_ptr));
    if(rc) {
        if(rc == MDB_NOTFOUND) {
             Py_RETURN_FALSE;
        }
        return err_set("mdb_del", rc);
    }
    Py_RETURN_TRUE;
}

/**
 * Transaction.drop(db)
 */
static PyObject *
trans_drop(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_drop {
        DbObject *db;
        int delete;
    } arg = {NULL, 1};

    static const struct argspec argspec[] = {
        {ARG_DB, DB_S, OFFSET(trans_drop, db)},
        {ARG_BOOL, DELETE_S, OFFSET(trans_drop, delete)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }
    if(! arg.db) {
        return type_error("'db' argument required.");
    } else if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }

    int rc;
    UNLOCKED(rc, mdb_drop(self->txn, arg.db->dbi, arg.delete));
    self->mutations++;
    if(rc) {
        return err_set("mdb_drop", rc);
    }
    Py_RETURN_NONE;
}

/**
 * Transaction.get() -> result
 */
static PyObject *
trans_get(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_get {
        MDB_val key;
        PyObject *default_;
        DbObject *db;
    } arg = {{0, 0}, Py_None, self->db};

    static const struct argspec argspec[] = {
        {ARG_BUF, KEY_S, OFFSET(trans_get, key)},
        {ARG_OBJ, DEFAULT_S, OFFSET(trans_get, default_)},
        {ARG_DB, DB_S, OFFSET(trans_get, db)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }
    if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }

    if(! arg.key.mv_data) {
        return type_error("key must be given.");
    }

    MDB_val val;
    int rc;
    UNLOCKED(rc, mdb_get(self->txn, arg.db->dbi, &arg.key, &val));
    if(rc) {
        if(rc == MDB_NOTFOUND) {
            Py_INCREF(arg.default_);
            return arg.default_;
        }
        return err_set("mdb_get", rc);
    }
    if(self->flags & TRANS_BUFFERS) {
        return buffer_from_val(&self->key_buf, &val);
    }
    return string_from_val(&val);
}

/**
 * Transaction.put() -> bool
 */
static PyObject *
trans_put(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_put {
        MDB_val key;
        MDB_val value;
        int dupdata;
        int overwrite;
        int append;
        DbObject *db;
    } arg = {{0, 0}, {0, 0}, 0, 1, 0, self->db};

    static const struct argspec argspec[] = {
        {ARG_BUF, KEY_S, OFFSET(trans_put, key)},
        {ARG_BUF, VALUE_S, OFFSET(trans_put, value)},
        {ARG_BOOL, DUPDATA_S, OFFSET(trans_put, dupdata)},
        {ARG_BOOL, OVERWRITE_S, OFFSET(trans_put, overwrite)},
        {ARG_BOOL, APPEND_S, OFFSET(trans_put, append)},
        {ARG_DB, DB_S, OFFSET(trans_put, db)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }
    if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }

    int flags = 0;
    if(! arg.dupdata) {
        flags |= MDB_NODUPDATA;
    }
    if(! arg.overwrite) {
        flags |= MDB_NOOVERWRITE;
    }
    if(arg.append) {
        flags |= MDB_APPEND;
    }

    DEBUG("inserting '%.*s' (%d) -> '%.*s' (%d)",
        (int)arg.key.mv_size, (char *)arg.key.mv_data,
        (int)arg.key.mv_size,
        (int)arg.value.mv_size, (char *)arg.value.mv_data,
        (int)arg.value.mv_size)

    self->mutations++;
    int rc;
    UNLOCKED(rc, mdb_put(self->txn, (arg.db)->dbi,
                         &arg.key, &arg.value, flags));
    if(rc) {
        if(rc == MDB_KEYEXIST) {
            Py_RETURN_FALSE;
        }
        return err_set("mdb_put", rc);
    }
    Py_RETURN_TRUE;
}

static PyObject *
make_cursor(DbObject *db, TransObject *trans);

/**
 * Transaction.replace() -> None|result
 */
static PyObject *
trans_replace(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_replace {
        MDB_val key;
        MDB_val value;
        DbObject *db;
    } arg = {{0, 0}, {0, 0}, self->db};

    static const struct argspec argspec[] = {
        {ARG_BUF, KEY_S, OFFSET(trans_replace, key)},
        {ARG_BUF, VALUE_S, OFFSET(trans_replace, value)},
        {ARG_DB, DB_S, OFFSET(trans_replace, db)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }
    if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }

    CursorObject *cursor = (CursorObject *) make_cursor(arg.db, self);
    if(! cursor) {
        return NULL;
    }

    /* arg.val is updated if MDB_KEYEXIST. */
    MDB_val newval = arg.value;
    int flags = MDB_NOOVERWRITE;
    self->mutations++;
    int rc;
    UNLOCKED(rc, mdb_cursor_put(cursor->curs, &arg.key, &arg.value, flags));
    if(! rc) {
        Py_DECREF((PyObject *) cursor);
        Py_RETURN_NONE;
    } else if(rc != MDB_KEYEXIST) {
        Py_DECREF((PyObject *) cursor);
        return err_set("mdb_put", rc);
    }

    PyObject *old = string_from_val(&arg.value);
    if(! old) {
        Py_DECREF((PyObject *) cursor);
        return NULL;
    }

    UNLOCKED(rc, mdb_cursor_put(cursor->curs, &arg.key, &newval, 0));
    Py_DECREF((PyObject *) cursor);
    if(rc) {
        Py_DECREF(old);
        return err_set("mdb_put", rc);
    }
    return old;
}

static int
_cursor_get_c(CursorObject *self, enum MDB_cursor_op op);

/**
 * Transaction.pop() -> None|result
 */
static PyObject *
trans_pop(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_pop {
        MDB_val key;
        DbObject *db;
    } arg = {{0, 0}, self->db};

    static const struct argspec argspec[] = {
        {ARG_BUF, KEY_S, OFFSET(trans_pop, key)},
        {ARG_DB, DB_S, OFFSET(trans_pop, db)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }
    if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }

    CursorObject *cursor = (CursorObject *) make_cursor(arg.db, self);
    if(! cursor) {
        return NULL;
    }

    cursor->key = arg.key;
    if(_cursor_get_c(cursor, MDB_SET_KEY)) {
        Py_DECREF((PyObject *)cursor);
        return NULL;
    }
    if(! cursor->positioned) {
        Py_DECREF((PyObject *)cursor);
        Py_RETURN_NONE;
    }
    PyObject *old = string_from_val(&cursor->val);
    if(! old) {
        Py_DECREF((PyObject *)cursor);
        return NULL;
    }

    int rc;
    UNLOCKED(rc, mdb_cursor_del(cursor->curs, 0));
    Py_DECREF((PyObject *)cursor);
    self->mutations++;
    if(rc) {
        Py_DECREF(old);
        return err_set("mdb_cursor_del", rc);
    }
    return old;
}

/**
 * Transaction.__enter__()
 */
static PyObject *trans_enter(TransObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    Py_INCREF(self);
    return (PyObject *)self;
}

/**
 * Transaction.__exit__()
 */
static PyObject *trans_exit(TransObject *self, PyObject *args)
{
    if(! self->valid) {
        return err_invalid();
    }
    if(PyTuple_GET_ITEM(args, 0) == Py_None) {
        return trans_commit(self);
    } else {
        return trans_abort(self);
    }
}

/**
 * Transaction.stat() -> dict
 */
static PyObject *
trans_stat(TransObject *self, PyObject *args, PyObject *kwds)
{
    struct trans_stat {
        DbObject *db;
    } arg = {self->db};

    static const struct argspec argspec[] = {
        {ARG_DB, DB_S, OFFSET(trans_stat, db)}
    };

    if(parse_args(self->valid, SPECSIZE(), argspec, args, kwds, &arg)) {
        return NULL;
    }
    if(! db_owner_check(arg.db, self->env)) {
        return NULL;
    }

    MDB_stat st;
    int rc;
    UNLOCKED(rc, mdb_stat(self->txn, arg.db->dbi, &st));
    if(rc) {
        return err_set("mdb_stat", rc);
    }
    return dict_from_fields(&st, mdb_stat_fields);
}

static struct PyMethodDef trans_methods[] = {
    {"__enter__", (PyCFunction)trans_enter, METH_NOARGS},
    {"__exit__", (PyCFunction)trans_exit, METH_VARARGS},
    {"abort", (PyCFunction)trans_abort, METH_NOARGS},
    {"commit", (PyCFunction)trans_commit, METH_NOARGS},
    {"cursor", (PyCFunction)trans_cursor, METH_VARARGS|METH_KEYWORDS},
    {"delete", (PyCFunction)trans_delete, METH_VARARGS|METH_KEYWORDS},
    {"drop", (PyCFunction)trans_drop, METH_VARARGS|METH_KEYWORDS},
    {"get", (PyCFunction)trans_get, METH_VARARGS|METH_KEYWORDS},
    {"put", (PyCFunction)trans_put, METH_VARARGS|METH_KEYWORDS},
    {"replace", (PyCFunction)trans_replace, METH_VARARGS|METH_KEYWORDS},
    {"pop", (PyCFunction)trans_pop, METH_VARARGS|METH_KEYWORDS},
    {"stat", (PyCFunction)trans_stat, METH_VARARGS|METH_KEYWORDS},
    {NULL, NULL}
};

static PyTypeObject PyTransaction_Type = {
    PyObject_HEAD_INIT(0)
    .tp_basicsize = sizeof(TransObject),
    .tp_dealloc = (destructor) trans_dealloc,
    .tp_clear = (inquiry) trans_clear,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = trans_methods,
    .tp_name = "Transaction",
    .tp_new = trans_new,
};

/**
 * lmdb.enable_drop_gil()
 */
static PyObject *
enable_drop_gil(void)
{
    drop_gil = 1;
    Py_RETURN_NONE;
}

/**
 * lmdb.get_version() -> tuple
 */
static PyObject *
get_version(void)
{
    return Py_BuildValue("iii", MDB_VERSION_MAJOR,
        MDB_VERSION_MINOR, MDB_VERSION_PATCH);
}

static struct PyMethodDef module_methods[] = {
    {"enable_drop_gil", (PyCFunction) enable_drop_gil, METH_NOARGS, ""},
    {"version", (PyCFunction) get_version, METH_NOARGS, ""},
    {0, 0, 0, 0}
};

#if PY_MAJOR_VERSION >= 3
static struct PyModuleDef moduledef = {
    PyModuleDef_HEAD_INIT,
    "cpython",
    NULL,
    -1,
    module_methods,
    NULL,
    NULL,
    NULL,
    NULL
};
#endif

/**
 * Initialize and publish the LMDB built-in types.
 */
static int init_types(PyObject *mod)
{
    static PyTypeObject *types[] = {
        &PyEnvironment_Type,
        &PyCursor_Type,
        &PyTransaction_Type,
        &PyIterator_Type,
        &PyDatabase_Type,
        NULL
    };

    int i;
    for(i = 0; types[i]; i++) {
        PyTypeObject *type = types[i];
        if(PyType_Ready(type)) {
            return -1;
        }
        if(PyObject_SetAttrString(mod, type->tp_name, (PyObject *)type)) {
            return -1;
        }
    }
    return 0;
}

/**
 * Produce `string_tbl' array of PyObjects from `strings'.
 */
static int init_strings(PyObject *mod)
{
    string_tbl = malloc(sizeof(PyObject *) * STRING_ID_COUNT);
    if(! string_tbl) {
        return -1;
    }

    const char *cur = strings;
    int i;
    for(i = 0; i < STRING_ID_COUNT; i++) {
        if(! ((string_tbl[i] = PyUnicode_InternFromString(cur)))) {
            return -1;
        }
        cur += PyUnicode_GET_SIZE(string_tbl[i]) + 1;
    }
    return 0;
}

/**
 * Initialize a bunch of constants used to ease number compares.
 */
static int init_constants(PyObject *mod)
{
    if(! ((py_zero = PyLong_FromUnsignedLongLong(0)))) {
        return -1;
    }
    if(! ((py_int_max = PyLong_FromUnsignedLongLong(INT_MAX)))) {
        return -1;
    }
    if(! ((py_size_max = PyLong_FromUnsignedLongLong(SIZE_MAX)))) {
        return -1;
    }
    return 0;
}

/**
 * Create lmdb.Error exception class, and one subclass for each entry in
 * `error_map`.
 */
static int init_errors(PyObject *mod)
{
    Error = PyErr_NewException("lmdb.Error", NULL, NULL);
    if(! Error) {
        return -1;
    }
    if(PyObject_SetAttrString(mod, "Error", Error)) {
        return -1;
    }

    size_t count = (sizeof error_map / sizeof error_map[0]);
    error_tbl = malloc(sizeof(PyObject *) * count);
    if(! error_tbl) {
        return -1;
    }

    int i;
    char qualname[64];
    for(i = 0; i < count; i++) {
        const struct error_map *error = &error_map[i];
        snprintf(qualname, sizeof qualname, "lmdb.%s", error->name);
        qualname[sizeof qualname - 1] = '\0';

        PyObject *klass = PyErr_NewException(qualname, Error, NULL);
        if(! klass) {
            return -1;
        }

        error_tbl[i] = klass;
        if(PyObject_SetAttrString(mod, error->name, klass)) {
            return -1;
        }
    }
    return 0;
}

/**
 * Do all required to initialize the lmdb.cpython module.
 */
PyMODINIT_FUNC
MODINIT_NAME(void)
{
#if PY_MAJOR_VERSION >= 3
    PyObject *mod = PyModule_Create(&moduledef);
#else
    PyObject *mod = Py_InitModule3("cpython", module_methods, "");
#endif
    if(! mod) {
        MOD_RETURN(NULL);
    }

    if(init_types(mod)) {
        MOD_RETURN(NULL);
    }

#ifdef HAVE_MEMSINK
    MemSink_IMPORT;
    if(ms_init_source(&PyTransaction_Type, offsetof(TransObject, sink_head))) {
        MOD_RETURN(NULL);
    }
#endif

    if(init_strings(mod)) {
        MOD_RETURN(NULL);
    }
    if(init_constants(mod)) {
        MOD_RETURN(NULL);
    }
    if(init_errors(mod)) {
        MOD_RETURN(NULL);
    }
    if(PyObject_SetAttrString(mod, "open", (PyObject *)&PyEnvironment_Type)) {
        MOD_RETURN(NULL);
    }
    MOD_RETURN(mod);
}
