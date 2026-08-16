/*
 * Copyright 2013-2026 The py-lmdb authors, all rights reserved.
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

/*
 * Per-engine LMDB API vtable.
 *
 * py-lmdb can bundle two binary-incompatible LMDB versions (0.9.x with data
 * format v1 and 1.0.x with data format v3) in one extension module.  Each
 * bundled tree is compiled with a symbol-prefix rename header (see
 * lib/py-lmdb/rename.h and lib1/py-lmdb/rename.h), and an engine.c glue TU
 * per tree populates one MdbApi with that tree's entry points.  The binding
 * selects an engine per Environment by sniffing the data file's format
 * version, and calls LMDB exclusively through the vtable.
 *
 * This header must be included AFTER lmdb.h: it uses lmdb.h's types but
 * deliberately does not include it, because each translation unit must pick
 * up its own tree's copy.
 *
 * Layout compatibility contract: on non-MDB_VL32 builds (the only builds
 * py-lmdb makes), MDB_val, MDB_stat, MDB_envinfo and the callback typedefs
 * are layout-identical between 0.9.x and 1.0.x (mdb_size_t == size_t), so a
 * single set of types serves both engines.
 */

#ifndef PYLMDB_MDB_API_H
#define PYLMDB_MDB_API_H

typedef struct MdbApi {
    /* Compile-time identity of this engine, from its own lmdb.h. */
    int major;
    int minor;
    int patch;
    /* MDB_DATA_VERSION the engine reads/writes: 1 for 0.9.x, 3 for 1.0.x. */
    unsigned int data_version;

    /* Environment. */
    int    (*env_create)(MDB_env **env);
    int    (*env_open)(MDB_env *env, const char *path, unsigned int flags,
                       mdb_mode_t mode);
    void   (*env_close)(MDB_env *env);
    int    (*env_copy2)(MDB_env *env, const char *path, unsigned int flags);
    int    (*env_copyfd2)(MDB_env *env, mdb_filehandle_t fd,
                          unsigned int flags);
    /* NULL unless built with the py-lmdb env-copy-txn patch. */
    int    (*env_copy3)(MDB_env *env, const char *path, unsigned int flags,
                        MDB_txn *txn);
    int    (*env_copyfd3)(MDB_env *env, mdb_filehandle_t fd,
                          unsigned int flags, MDB_txn *txn);
    int    (*env_sync)(MDB_env *env, int force);
    int    (*env_stat)(MDB_env *env, MDB_stat *stat);
    int    (*env_info)(MDB_env *env, MDB_envinfo *stat);
    int    (*env_get_flags)(MDB_env *env, unsigned int *flags);
    int    (*env_set_flags)(MDB_env *env, unsigned int flags, int onoff);
    int    (*env_get_maxkeysize)(MDB_env *env);
    int    (*env_get_maxreaders)(MDB_env *env, unsigned int *readers);
    int    (*env_get_path)(MDB_env *env, const char **path);
    int    (*env_set_mapsize)(MDB_env *env, size_t size);
    int    (*env_set_maxdbs)(MDB_env *env, MDB_dbi dbs);
    int    (*env_set_maxreaders)(MDB_env *env, unsigned int readers);
    int    (*reader_check)(MDB_env *env, int *dead);
    int    (*reader_list)(MDB_env *env, MDB_msg_func *func, void *ctx);

    /* Transactions. */
    int    (*txn_begin)(MDB_env *env, MDB_txn *parent, unsigned int flags,
                        MDB_txn **txn);
    int    (*txn_commit)(MDB_txn *txn);
    void   (*txn_abort)(MDB_txn *txn);
    void   (*txn_reset)(MDB_txn *txn);
    int    (*txn_renew)(MDB_txn *txn);
    size_t (*txn_id)(MDB_txn *txn);

    /* Databases. */
    int    (*dbi_open)(MDB_txn *txn, const char *name, unsigned int flags,
                       MDB_dbi *dbi);
    int    (*dbi_flags)(MDB_txn *txn, MDB_dbi dbi, unsigned int *flags);
    void   (*dbi_close)(MDB_env *env, MDB_dbi dbi);
    int    (*drop)(MDB_txn *txn, MDB_dbi dbi, int del);
    int    (*get)(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
    int    (*put)(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data,
                  unsigned int flags);
    int    (*del)(MDB_txn *txn, MDB_dbi dbi, MDB_val *key, MDB_val *data);
    int    (*stat)(MDB_txn *txn, MDB_dbi dbi, MDB_stat *stat);

    /* Cursors. */
    int    (*cursor_open)(MDB_txn *txn, MDB_dbi dbi, MDB_cursor **cursor);
    void   (*cursor_close)(MDB_cursor *cursor);
    int    (*cursor_get)(MDB_cursor *cursor, MDB_val *key, MDB_val *data,
                         MDB_cursor_op op);
    int    (*cursor_put)(MDB_cursor *cursor, MDB_val *key, MDB_val *data,
                         unsigned int flags);
    int    (*cursor_del)(MDB_cursor *cursor, unsigned int flags);
    int    (*cursor_count)(MDB_cursor *cursor, size_t *countp);

    /* Misc. */
    char  *(*strerror_fn)(int err);
    char  *(*version_fn)(int *major, int *minor, int *patch);
} MdbApi;

/* Engine vtables provided by engine.c instances; which of these exist is
 * decided by setup.py via -D flags. */
#ifdef LMDB_ENGINE_SYS
extern const MdbApi lmdb_api_sys;
#endif
#ifdef LMDB_ENGINE_V09
extern const MdbApi lmdb_api_v09;
#endif
#ifdef LMDB_ENGINE_V10
extern const MdbApi lmdb_api_v10;
#endif

#endif /* PYLMDB_MDB_API_H */
