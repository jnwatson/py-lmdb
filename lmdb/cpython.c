#include <sys/stat.h>
#include <stdbool.h>
#include <string.h>
#include <errno.h>
#include <stdint.h>

#include "Python.h"
#include "structmember.h"

#include "lmdb.h"

#define DEBUG(s, ...) \
    fprintf(stderr, "lmdb.cpython: %s:%d: " s "\n", __func__, __LINE__, \
            ## __VA_ARGS__);

static PyObject *Error;
extern PyTypeObject PyDatabase_Type;
extern PyTypeObject PyEnvironment_Type;
extern PyTypeObject PyTransaction_Type;
extern PyTypeObject PyCursor_Type;

struct EnvObject;


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


typedef struct {
    PyObject_HEAD
    int valid;
    struct EnvObject *env;
    MDB_dbi dbi;
} DbObject;

typedef struct EnvObject {
    PyObject_HEAD
    int valid;
    MDB_env *env;
    DbObject *main_db;
} EnvObject;

typedef struct {
    PyObject_HEAD
    int valid;
    EnvObject *env;

    MDB_txn *txn;
    int buffers;
    PyBufferObject *key_buf;
} TransObject;

typedef struct {
    PyObject_HEAD
    int valid;
    EnvObject *env;
    DbObject *db;
    TransObject *trans;

    int positioned;
    MDB_cursor *curs;
    PyBufferObject *key_buf;
    PyBufferObject *val_buf;
    PyObject *item_tup;
    MDB_val key;
    MDB_val val;
} CursorObject;

typedef struct {
    PyObject_HEAD
    int valid;
    EnvObject *env;
    DbObject *db;
    CursorObject *curs;
    int reverse;
} IterObject;



// ----------- helpers
//
//
//

enum { TYPE_EOF, TYPE_UINT, TYPE_SIZE, TYPE_ADDR };

struct dict_field {
    int type;
    const char *name;
    int offset;
};

static PyObject *
dict_from_fields(void *o, const struct dict_field *fields)
{
    PyObject *dict = PyDict_New();
    if(! dict) {
        return NULL;
    }

    while(fields->type != TYPE_EOF) {
        uint8_t *p = ((uint8_t *) o) + fields->offset;
        unsigned PY_LONG_LONG l;
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


static PyObject *
buffer_from_val(PyBufferObject **bufp, MDB_val *val)
{
    PyBufferObject *buf = *bufp;
    if(! buf) {
        buf = (PyBufferObject *) PyBuffer_FromMemory("", 0);
        if(! buf) {
            return NULL;
        }
        *bufp = buf;
    }

    buf->b_hash = -1;
    buf->b_ptr = val->mv_data;
    buf->b_size = val->mv_size;
    Py_INCREF(buf);
    return (PyObject *) buf;
}


static PyObject *
string_from_val(MDB_val *val)
{
    return PyString_FromStringAndSize(val->mv_data, val->mv_size);
}


// ------------------------
// Exceptions.
// ------------------------

static void *
err_set(const char *what, int rc)
{
    PyErr_Format(Error, "%s: %s", what, mdb_strerror(rc));
    return NULL;
}

static void *
err_invalid(void)
{
    PyErr_Format(Error, "Attempt to operate on closed/deleted/dropped object.");
    return NULL;
}


// ----------------------------
// Database
// ----------------------------


static DbObject *
db_from_name(EnvObject *env, MDB_txn *txn, const char *name,
             unsigned int flags)
{
    MDB_dbi dbi;
    int rc;

    if((rc = mdb_dbi_open(txn, name, flags, &dbi))) {
        err_set("mdb_dbi_open", rc);
        return NULL;
    }

    DbObject *dbo = PyObject_New(DbObject, &PyDatabase_Type);
    dbo->env = env;
    Py_INCREF(env);
    dbo->dbi = dbi;
    dbo->valid = 1;
    DEBUG("DbObject '%s' opened at %p", name, dbo)
    return dbo;
}


static DbObject *
txn_db_from_name(EnvObject *env, const char *name,
                 unsigned int flags)
{
    int rc;
    MDB_txn *txn;
    if((rc = mdb_txn_begin(env->env, NULL, 0, &txn))) {
        err_set("Write transaction to open database", rc);
        return NULL;
    }

    DbObject *dbo = db_from_name(env, txn, name, flags);
    if(! dbo) {
        mdb_txn_abort(txn);
        return NULL;
    }

    if((rc = mdb_txn_commit(txn))) {
        Py_DECREF(dbo);
        return err_set("mdb_txn_commit", rc);
    }
    return dbo;
}

static PyObject *
db_close(DbObject *self)
{
    if(2 != (self->valid + self->env->valid)) {
        return err_invalid();
    }

    mdb_dbi_close(self->env->env, self->dbi);
    self->valid = 0;
    Py_RETURN_NONE;
}


static struct PyMethodDef db_methods[] = {
    {"close", (PyCFunction)db_close, METH_NOARGS},
    {NULL, NULL}
};

static PyObject *
db_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {
        "env", "txn", "name", "reverse_key", "dupsort", "create", NULL
    };
    const char *name = NULL;
    EnvObject *env;
    TransObject *trans;
    int reverse_key = 0;
    int dupsort = 0;
    int create = 1;

    if(!PyArg_ParseTupleAndKeywords(args, kwds, "O!O!|ziii", kwlist,
        &PyEnvironment_Type, &env,
        &PyTransaction_Type, &trans,
        &name, &reverse_key, &dupsort, &create)) {
        return NULL;
    }

    if(2 != (env->valid + trans->valid)) {
        return err_invalid();
    }

    int flags = 0;
    if(reverse_key) {
        flags |= MDB_REVERSEKEY;
    }
    if(dupsort) {
        flags |= MDB_DUPSORT;
    }
    if(create) {
        flags |= MDB_CREATE;
    }
    return (PyObject *) db_from_name(env, trans->txn, name, flags);
}

static void
db_dealloc(DbObject *self)
{
    Py_DECREF(self->env);
    PyObject_Del(self);
}

PyTypeObject PyDatabase_Type = {
    PyObject_HEAD_INIT(NULL)
    .tp_basicsize = sizeof(DbObject),
    .tp_dealloc = (destructor) db_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = db_methods,
    .tp_name = "Database",
    .tp_new = db_new
};


// -------------------------
// Environment.
// -------------------------

static void
env_dealloc(EnvObject *self)
{
    DEBUG("kiling env..")
    if(self->main_db) {
        Py_CLEAR(self->main_db);
    }
    if(self->env) {
        DEBUG("Closing env")
        mdb_env_close(self->env);
    }
    PyObject_Del(self);
}

static PyObject *
env_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    char *path = NULL;
    int map_size = 10485760;
    int subdir = 1;
    int readonly = 0;
    int metasync = 1;
    int sync = 1;
    int map_async = 0;
    int mode = 0644;
    int create = 1;
    int max_readers = 126;
    int max_dbs = 0;

    static char *kwlist[] = {
        "path", "map_size", "subdir", "readonly", "metasync", "sync",
        "map_async", "mode", "create", "max_readers", "max_dbs", NULL
    };

    if(!PyArg_ParseTupleAndKeywords(args, kwds, "s|iiiiiiiiii", kwlist,
        &path, &map_size, &subdir, &readonly, &metasync, &sync, &map_async,
        &mode, &create, &max_readers, &max_dbs))
    {
        return NULL;
    }

    EnvObject *self = PyObject_New(EnvObject, type);
    if(! self) {
        return NULL;
    }

    self->valid = 0;
    self->env = NULL;

    int rc;
    if((rc = mdb_env_create(&(self->env)))) {
        err_set("Creating environment", rc);
        goto fail;
    }

    if((rc = mdb_env_set_mapsize(self->env, map_size))) {
        err_set("Setting map size", rc);
        goto fail;
    }

    if((rc = mdb_env_set_maxreaders(self->env, max_readers))) {
        err_set("Setting max readers", rc);
        goto fail;
    }

    if((rc = mdb_env_set_maxdbs(self->env, max_dbs))) {
        err_set("Setting max DBs", rc);
        goto fail;
    }

    if(create && subdir) {
        struct stat st;
        stat(path, &st);
        if(errno == ENOENT) {
            if(mkdir(path, 0700)) {
                PyErr_SetFromErrnoWithFilename(PyExc_OSError, path);
                goto fail;
            }
        }
    }

    int flags = 0;
    if(! subdir) {
        flags |= MDB_NOSUBDIR;
    }
    if(readonly) {
        flags |= MDB_RDONLY;
    }
    if(! metasync) {
        flags |= MDB_NOMETASYNC;
    }
    if(! sync) {
        flags |= MDB_NOSYNC;
    }
    if(map_async) {
        flags |= MDB_MAPASYNC;
    }

    DEBUG("mdb_env_open(%p, '%s', %d, %o);", self->env, path, flags, mode)
    if((rc = mdb_env_open(self->env, path, flags, mode))) {
        err_set(path, rc);
        goto fail;
    }

    self->main_db = txn_db_from_name(self, NULL, 0);
    if(self->main_db) {
        self->valid = 1;
        DEBUG("EnvObject '%s' opened at %p", path, self)
        return (PyObject *) self;
    }

fail:
    DEBUG("initialization failed")
    if(self) {
        env_dealloc(self);
    }
    return NULL;
}


static TransObject *
make_trans(EnvObject *env, TransObject *parent, int readonly, int buffers)
{
    if(! env->valid) {
        return err_invalid();
    }

    MDB_txn *parent_txn = NULL;
    if(parent) {
        if(! parent->valid) {
            return err_invalid();
        }
        parent_txn = parent->txn;
    }

    TransObject *self = PyObject_New(TransObject, &PyTransaction_Type);
    if(! self) {
        return NULL;
    }

    int flags = readonly ? MDB_RDONLY : 0;
    int rc = mdb_txn_begin(env->env, parent_txn, flags, &(self->txn));
    if(rc) {
        PyObject_Del(self);
        return err_set("mdb_txn_begin", rc);
    }

    self->valid = 1;
    self->env = env;
    Py_INCREF(env);
    self->buffers = buffers;
    self->key_buf = NULL;
    return self;
}

static PyObject *
env_begin(EnvObject *self, PyObject *args, PyObject *kwds)
{
    if(! self->valid) {
        return err_invalid();
    }

    TransObject *parent = NULL;
    int readonly = 0;
    int buffers = 0;

    static char *kwlist[] = {
        "parent", "readonly", "buffers", NULL
    };
    if(!PyArg_ParseTupleAndKeywords(args, kwds, "|O!ii", kwlist,
            &PyTransaction_Type, &parent, &readonly, &buffers)) {
        return NULL;
    }
    return (PyObject *) make_trans(self, parent, readonly, buffers);
}

static PyObject *
env_close(EnvObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }
    self->valid = 0;
    DEBUG("Closing env")
    mdb_env_close(self->env);
    self->env = NULL;
    Py_RETURN_NONE;
}

static PyObject *
env_copy(EnvObject *self, PyObject *args)
{
    if(! self->valid) {
        return err_invalid();
    }

    PyObject *path;
    if(! PyArg_ParseTuple(args, "|O:copy", &path)) {
        return NULL;
    }
    return NULL;
}

static PyObject *
env_info(EnvObject *self)
{
    static const struct dict_field fields[] = {
        { TYPE_ADDR, "map_addr",        offsetof(MDB_envinfo, me_mapaddr) },
        { TYPE_SIZE, "map_size",        offsetof(MDB_envinfo, me_mapsize) },
        { TYPE_SIZE, "last_pgno",       offsetof(MDB_envinfo, me_last_pgno) },
        { TYPE_SIZE, "last_txnid",      offsetof(MDB_envinfo, me_last_txnid) },
        { TYPE_UINT, "max_readers",     offsetof(MDB_envinfo, me_maxreaders) },
        { TYPE_UINT, "num_readers",     offsetof(MDB_envinfo, me_numreaders) },
        { TYPE_EOF, NULL, 0 }
    };

    if(! self->valid) {
        return err_invalid();
    }

    MDB_envinfo info;
    int rc = mdb_env_info(self->env, &info);
    if(rc) {
        err_set("Getting environment info", rc);
        return NULL;
    }
    return dict_from_fields(&info, fields);
}


static PyObject *
env_open(EnvObject *self, PyObject *args, PyObject *kwds)
{
    static char *kwlist[] = {
        "name", "reverse_key", "dupsort", "create", NULL
    };
    const char *name = NULL;
    int reverse_key = 0;
    int dupsort = 0;
    int create = 1;

    if(!PyArg_ParseTupleAndKeywords(args, kwds, "|ziii", kwlist,
        &name, &reverse_key, &dupsort, &create)) {
        return NULL;
    }

    int flags = 0;
    if(reverse_key) {
        flags |= MDB_REVERSEKEY;
    }
    if(dupsort) {
        flags |= MDB_DUPSORT;
    }
    if(create) {
        flags |= MDB_CREATE;
    }

    return (PyObject *) txn_db_from_name(self, name, flags);
}


static PyObject *
env_path(EnvObject *self)
{
    if(! self->valid) {
        return err_invalid();
    }

    const char *path;
    int rc;
    if((rc = mdb_env_get_path(self->env, &path))) {
        return err_set("Getting path", rc);
    }
    return PyString_FromString(path);
}


static PyObject *
env_stat(EnvObject *self)
{
    static const struct dict_field fields[] = {
        { TYPE_UINT, "psize",           offsetof(MDB_stat, ms_psize) },
        { TYPE_UINT, "depth",           offsetof(MDB_stat, ms_depth) },
        { TYPE_SIZE, "branch_pages",    offsetof(MDB_stat, ms_branch_pages) },
        { TYPE_SIZE, "leaf_pages",      offsetof(MDB_stat, ms_leaf_pages) },
        { TYPE_SIZE, "overflow_pages",  offsetof(MDB_stat, ms_overflow_pages) },
        { TYPE_SIZE, "entries",         offsetof(MDB_stat, ms_entries) },
        { TYPE_EOF, NULL, 0 }
    };

    if(! self->valid) {
        return err_invalid();
    }

    MDB_stat st;
    int rc = mdb_env_stat(self->env, &st);
    if(rc) {
        err_set("Getting environment statistics", rc);
        return NULL;
    }
    return dict_from_fields(&st, fields);
}

static PyObject *
env_sync(EnvObject *self, PyObject *arg)
{
    if(! self->valid) {
        return err_invalid();
    }

    int force = arg == NULL;
    if(arg) {
        int force = PyObject_IsTrue(arg);
        if(force == -1) {
            return NULL;
        }
    }

    int rc = mdb_env_sync(self->env, force);
    if(rc) {
        return err_set("Flushing", rc);
    }
    Py_RETURN_NONE;
}

static struct PyMethodDef env_methods[] = {
    {"begin", (PyCFunction)env_begin, METH_VARARGS|METH_KEYWORDS},
    {"close", (PyCFunction)env_close, METH_NOARGS},
    {"copy", (PyCFunction)env_copy, METH_VARARGS},
    {"info", (PyCFunction)env_info, METH_NOARGS},
    {"open", (PyCFunction)env_open, METH_VARARGS|METH_KEYWORDS},
    {"path", (PyCFunction)env_path, METH_NOARGS},
    {"stat", (PyCFunction)env_stat, METH_NOARGS},
    {"sync", (PyCFunction)env_sync, METH_OLDARGS},
    {NULL, NULL}
};


PyTypeObject PyEnvironment_Type = {
    PyObject_HEAD_INIT(0)
    .tp_basicsize = sizeof(EnvObject),
    .tp_dealloc = (destructor) env_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = env_methods,
    .tp_name = "Environment",
    .tp_new = env_new,
};


/// cursors
//
//
//

static int cursor_valid(CursorObject *self)
{
    return 4 == (self->valid + self->env->valid +
                 self->trans->valid + self->db->valid);
}

static PyObject *
curs_count(CursorObject *self)
{
    if(! cursor_valid(self)) {
        return err_invalid();
    }

    size_t count;
    int rc = mdb_cursor_count(self->curs, &count);
    if(rc) {
        return err_set("mdb_cursor_count", rc);
    }
    return PyLong_FromUnsignedLongLong(count);
}

static PyObject *
_cursor_get(CursorObject *self, enum MDB_cursor_op op)
{
    int rc = mdb_cursor_get(self->curs, &(self->key), &(self->val), op);
    self->positioned = rc == 0;
    if(rc) {
        self->key.mv_size = 0;
        self->val.mv_size = 0;
        if(rc != MDB_NOTFOUND) {
            if(! (rc == EINVAL && op == MDB_GET_CURRENT)) {
                return err_set("mdb_cursor_get", rc);
            }
        }
    }
    return PyBool_FromLong(self->positioned);
}


static PyObject *
curs_delete(CursorObject *self)
{
    if(! cursor_valid(self)) {
        return err_invalid();
    }
    if(self->positioned) {
        int rc = mdb_cursor_del(self->curs, 0);
        if(rc) {
            return err_set("mdb_cursor_del", rc);
        }
        return _cursor_get(self, MDB_GET_CURRENT);
    }
    Py_RETURN_FALSE;
}

static PyObject *
curs_first(CursorObject *self)
{
    if(! cursor_valid(self)) {
        return err_invalid();
    }
    return _cursor_get(self, MDB_FIRST);
}

static PyObject *
curs_forward(PyObject *self)
{
    return NULL;
}

static PyObject *
curs_item(CursorObject *self)
{
    if(! cursor_valid(self)) {
        return err_invalid();
    }
    if(self->trans->buffers) {
        if(! buffer_from_val(&(self->key_buf), &(self->key))) {
            return NULL;
        }
        if(! buffer_from_val(&(self->val_buf), &(self->val))) {
            return NULL;
        }
        if(! self->item_tup) {
            self->item_tup = PyTuple_Pack(2, self->key_buf, self->val_buf);
        }
        Py_INCREF(self->item_tup);
        return self->item_tup;
    }

    PyObject *key = string_from_val(&(self->key));
    if(! key) {
        return NULL;
    }
    PyObject *val = string_from_val(&(self->val));
    if(! val) {
        Py_DECREF(key);
        return NULL;
    }
    PyObject *tup = PyTuple_Pack(2, key, val);
    if(! tup) {
        Py_DECREF(key);
        Py_DECREF(val);
        return NULL;
    }
    return tup;
}

static PyObject *
curs_key(CursorObject *self)
{
    if(! cursor_valid(self)) {
        return err_invalid();
    }
    if(self->trans->buffers) {
        if(! buffer_from_val(&(self->key_buf), &(self->key))) {
            return NULL;
        }
        Py_INCREF(self->key_buf);
        return (PyObject *) self->key_buf;
    }
    return string_from_val(&(self->key));
}

static PyObject *
curs_last(PyObject *self)
{
    return NULL;
}

static PyObject *
curs_next(PyObject *self)
{
    return NULL;
}

static PyObject *
curs_prev(PyObject *self)
{
    return NULL;
}

static PyObject *
curs_put(PyObject *self, PyObject *args)
{
        PyObject *key;
        PyObject *value;
        PyObject *dupdata = false;
        PyObject *overwrite;
        PyObject *append;
        if (!PyArg_ParseTuple(args, ":put",
                              &key, &value, &dupdata, &overwrite, &append))
                return NULL;
        return NULL;
}

static PyObject *
curs_reverse(PyObject *self)
{
    return NULL;
}

static PyObject *
curs_set_key(PyObject *self, PyObject *args)
{
        PyObject *key;
        if (!PyArg_ParseTuple(args, "O:set_key",
                              &key))
                return NULL;
    return NULL;
}

static PyObject *
curs_set_range(PyObject *self, PyObject *args)
{
        PyObject *key;
        if (!PyArg_ParseTuple(args, "O:set_range",
                              &key))
                return NULL;
    return NULL;
}

static PyObject *
curs_value(CursorObject *self)
{
    if(! cursor_valid(self)) {
        return err_invalid();
    }
    if(self->trans->buffers) {
        if(! buffer_from_val(&(self->val_buf), &(self->val))) {
            return NULL;
        }
        Py_INCREF(self->val_buf);
        return (PyObject *) self->val_buf;
    }
    return string_from_val(&self->val);
}

static struct PyMethodDef curs_methods[] = {
    {"count", (PyCFunction)curs_count, METH_NOARGS},
    {"delete", (PyCFunction)curs_delete, METH_NOARGS},
    {"first", (PyCFunction)curs_first, METH_NOARGS},
    {"forward", (PyCFunction)curs_forward, METH_NOARGS},
    {"item", (PyCFunction)curs_item, METH_NOARGS},
    {"key", (PyCFunction)curs_key, METH_NOARGS},
    {"last", (PyCFunction)curs_last, METH_NOARGS},
    {"next", (PyCFunction)curs_next, METH_NOARGS},
    {"prev", (PyCFunction)curs_prev, METH_NOARGS},
    {"put", (PyCFunction)curs_put, METH_VARARGS},
    {"reverse", (PyCFunction)curs_reverse, METH_NOARGS},
    {"set_key", (PyCFunction)curs_set_key, METH_VARARGS},
    {"set_range", (PyCFunction)curs_set_range, METH_VARARGS},
    {"value", (PyCFunction)curs_value, METH_NOARGS},
    {NULL, NULL}
};

static void
curs_dealloc(CursorObject *self)
{
    Py_DECREF(self->trans);
    Py_DECREF(self->db);
    Py_DECREF(self->env);
    if(self->key_buf) {
        self->key_buf->b_size = 0;
        Py_DECREF(self->key_buf);
    }
    if(self->val_buf) {
        self->val_buf->b_size = 0;
        Py_DECREF(self->val_buf);
    }
    if(self->item_tup) {
        Py_DECREF(self->item_tup);
    }
    if(self->valid) {
        DEBUG("destroying cursor")
        mdb_cursor_close(self->curs);
    }
    PyObject_Del(self);
}

static PyObject *
make_cursor(DbObject *db, TransObject *trans)
{
    if(! db) {
        db = trans->env->main_db;
    }

    if(3 != (db->valid + trans->valid + trans->env->valid)) {
        return err_invalid();
    }

    CursorObject *self = PyObject_New(CursorObject, &PyCursor_Type);
    int rc = mdb_cursor_open(trans->txn, db->dbi, &(self->curs));
    if(rc) {
        PyObject_Del(self);
        return err_set("mdb_cursor_open", rc);
    }

    self->positioned = 0;
    self->valid = 1;
    self->env = trans->env;
    self->key_buf = NULL;
    self->val_buf = NULL;
    self->key.mv_size = 0;
    self->val.mv_size = 0;
    self->item_tup = NULL;
    Py_INCREF(self->env);
    self->db = db;
    Py_INCREF(self->db);
    self->trans = trans;
    Py_INCREF(self->trans);
    return (PyObject *) self;
}

static PyObject *
curs_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    DbObject *db;
    TransObject *trans;

    static char *kwlist[] = {"db", "txn", NULL};
    if(! PyArg_ParseTupleAndKeywords(args, kwds, "O!O!", kwlist,
            &PyDatabase_Type, &db, &PyTransaction_Type, &trans)) {
        return NULL;
    }
    return make_cursor(db, trans);
}

PyTypeObject PyCursor_Type = {
    PyObject_HEAD_INIT(0)
    .tp_basicsize = sizeof(CursorObject),
    .tp_dealloc = (destructor) curs_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = curs_methods,
    .tp_name = "Cursor",
    .tp_new = curs_new
};



// ==================================
// Iterators
// ==================================

static PyObject *
iter_dummy(PyObject *self)
{
    return NULL;
}

static struct PyMethodDef iter_methods[] = {
        {"dummy", (PyCFunction)iter_dummy, METH_NOARGS},
        {NULL, NULL}
};

static PyObject *
iter_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    return NULL;
}

static void
iter_dealloc(PyObject *ob)
{
}

static PyTypeObject PyIterator_Type = {
    PyObject_HEAD_INIT(0)
    .tp_basicsize = sizeof(IterObject),
    .tp_dealloc = iter_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = iter_methods,
    .tp_name = "Iterator",
};



// ==========================
// cursors
// =============================


/// ------------------------
//transactions
//////////////////////
//
//
//

static void
trans_dealloc(TransObject *self)
{
    DEBUG("deleting trans")
    if(self->txn && self->env->valid) {
        DEBUG("aborting")
        mdb_txn_abort(self->txn);
        self->txn = NULL;
    }
    Py_DECREF(self->env);
    PyObject_Del(self);
}

static int
trans_valid(TransObject *self)
{
    return 2 == (self->valid + self->env->valid);
}


static PyObject *
trans_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    EnvObject *env = NULL;
    TransObject *parent = NULL;
    int readonly = 0;
    int buffers = 0;

    static char *kwlist[] = {
        "env", "parent", "readonly", "buffers", NULL
    };
    if(!PyArg_ParseTupleAndKeywords(args, kwds, "O!|O!ii", kwlist,
            &PyEnvironment_Type, &env,
            &PyTransaction_Type, &parent, &readonly, &buffers)) {
        return NULL;
    }
    return (PyObject *) make_trans(env, parent, readonly, buffers);
}

static PyObject *
trans_abort(TransObject *self)
{
    if(! trans_valid(self)) {
        return err_invalid();
    }
    DEBUG("aborting")
    mdb_txn_abort(self->txn);
    self->txn = NULL;
    self->valid = 0;
    Py_RETURN_NONE;
}

static PyObject *
trans_commit(TransObject *self)
{
    if(! trans_valid(self)) {
        return err_invalid();
    }
    DEBUG("committing")
    int rc = mdb_txn_commit(self->txn);
    self->txn = NULL;
    self->valid = 0;
    if(rc) {
        return err_set("mdb_txn_commit", rc);
    }
    Py_RETURN_NONE;
}


static PyObject *
trans_cursor(TransObject *self, PyObject *args, PyObject *kwds)
{
    if(! trans_valid(self)) {
        return err_invalid();
    }

    DbObject *db = NULL;
    static char *kwlist[] = {"db", NULL};
    if(! PyArg_ParseTupleAndKeywords(args, kwds, "|O!", kwlist,
            &PyDatabase_Type, &db)) {
        return NULL;
    }

    if(! db) {
        db = self->env->main_db;
    }
    if(! (trans_valid(self) && db->valid)) {
        return err_invalid();
    }
    return make_cursor(db, self);
}


static PyObject *
trans_delete(TransObject *self, PyObject *args, PyObject *kwds)
{
    MDB_val key = {0, 0};
    MDB_val val = {0, 0};
    DbObject *db = NULL;

    static char *kwlist[] = {"key", "value", "db", NULL};
    if(! PyArg_ParseTupleAndKeywords(args, kwds, "s#|s#O!", kwlist,
            &key.mv_data, &key.mv_size, &val.mv_data, &val.mv_size,
            &PyDatabase_Type, &db)) {
        return NULL;
    }
    if(! db) {
        db = self->env->main_db;
    }
    if(! (trans_valid(self) && db->valid)) {
        return err_invalid();
    }
    MDB_val *val_ptr = val.mv_size ? &val : NULL;
    int rc = mdb_del(self->txn, db->dbi, &key, val_ptr);
    if(rc) {
        if(rc == MDB_NOTFOUND) {
             Py_RETURN_FALSE;
        }
        return err_set("mdb_del", rc);
    }
    Py_RETURN_TRUE;
}


static PyObject *
trans_drop(TransObject *self, PyObject *arg)
{
    int delete = arg == NULL;
    if(arg) {
        delete = PyObject_IsTrue(arg);
        if(delete == -1) {
            return NULL;
        }
    }

    //int rc = mdb_drop();
    return NULL;
}

static PyObject *
trans_get(TransObject *self, PyObject *args, PyObject *kwds)
{
    MDB_val key = {0, 0};
    PyObject *default_ = NULL;
    DbObject *db = NULL;

    static char *kwlist[] = {"key", "default", "db", NULL};
    if(! PyArg_ParseTupleAndKeywords(args, kwds, "s#|OO!", kwlist,
            &key.mv_data, &key.mv_size, &default_, &PyDatabase_Type, &db)) {
        return NULL;
    }
    if(! db) {
        db = self->env->main_db;
    }
    if(! (trans_valid(self) && db->valid)) {
        return err_invalid();
    }

    MDB_val val;
    int rc = mdb_get(self->txn, db->dbi, &key, &val);
    if(rc) {
        if(rc == MDB_NOTFOUND) {
            if(default_) {
                Py_INCREF(default_);
                return default_;
            }
            Py_RETURN_NONE;
        }
        return err_set("mdb_get", rc);
    }
    if(self->buffers) {
        return buffer_from_val(&(self->key_buf), &val);
    }
    return PyString_FromStringAndSize(val.mv_data, val.mv_size);
}

static PyObject *
trans_put(TransObject *self, PyObject *args, PyObject *kwds)
{
    MDB_val key = {0, 0};
    MDB_val val = {0, 0};
    int dupdata = 0;
    int overwrite = 1;
    int append = 0;
    DbObject *db = NULL;

    static char *kwlist[] = {"key", "value", "dupdata", "overwrite", "append",
                             "db", NULL};
    if(! PyArg_ParseTupleAndKeywords(args, kwds, "s#s#|iiiO!", kwlist,
            &key.mv_data, &key.mv_size, &val.mv_data, &val.mv_size,
            &dupdata, &overwrite, &append, &PyDatabase_Type, &db)) {
        return NULL;
    }
    if(! db) {
        db = self->env->main_db;
    }
    if(3 != (self->valid + self->env->valid + db->valid)) {
        return err_invalid();
    }

    int flags = 0;
    if(! dupdata) {
        flags |= MDB_NODUPDATA;
    }
    if(! overwrite) {
        flags |= MDB_NOOVERWRITE;
    }
    if(append) {
        flags |= MDB_APPEND;
    }

    int rc = mdb_put(self->txn, db->dbi, &key, &val, flags);
    if(rc) {
        if(rc == MDB_KEYEXIST) {
            Py_RETURN_FALSE;
        }
        return err_set("mdb_put", rc);
    }
    Py_RETURN_TRUE;
}

static struct PyMethodDef trans_methods[] = {
    {"abort", (PyCFunction)trans_abort, METH_NOARGS},
    {"commit", (PyCFunction)trans_commit, METH_NOARGS},
    {"cursor", (PyCFunction)trans_cursor, METH_VARARGS|METH_KEYWORDS},
    {"delete", (PyCFunction)trans_delete, METH_VARARGS|METH_KEYWORDS},
    {"get", (PyCFunction)trans_get, METH_VARARGS|METH_KEYWORDS},
    {"put", (PyCFunction)trans_put, METH_VARARGS|METH_KEYWORDS},
    {NULL, NULL}
};

PyTypeObject PyTransaction_Type = {
    PyObject_HEAD_INIT(0)
    .tp_basicsize = sizeof(TransObject),
    .tp_dealloc = (destructor) trans_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT,
    .tp_methods = trans_methods,
    .tp_name = "Transaction",
    .tp_new = trans_new,
};

static struct PyMethodDef module_methods[] = {
    {NULL, NULL}
};


static int add_type(PyObject *mod, PyTypeObject *type)
{
    if(PyType_Ready(type)) {
        return -1;
    }
    return PyObject_SetAttrString(mod, type->tp_name, (PyObject *)type);
 }

PyMODINIT_FUNC
initcpython(void)
{
    PyObject *mod = Py_InitModule3("cpython", module_methods, "");
    if(! mod) {
        return;
    }

    Error = PyErr_NewException("lmdb.Error", NULL, NULL);
    if(! Error) {
        return;
    }
    if(PyObject_SetAttrString(mod, "Error", Error)) {
        return;
    }

    if(add_type(mod, &PyEnvironment_Type)) {
        return;
    }
    if(PyObject_SetAttrString(mod, "connect", (PyObject *)&PyEnvironment_Type)) {
        return;
    }
    if(add_type(mod, &PyCursor_Type)) {
        return;
    }
    if(add_type(mod, &PyDatabase_Type)) {
        return;
    }
    if(add_type(mod, &PyDatabase_Type)) {
        return;
    }
    if(add_type(mod, &PyTransaction_Type)) {
        return;
    }
}
