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
 * Engine glue: populates one MdbApi vtable with this translation unit's
 * LMDB entry points.
 *
 * For bundled builds, setup.py copies this file into each tree's build
 * directory (build/lib09, build/lib10) and prepends
 * '#include "lmdb_rename.h"', so `#include "lmdb.h"` below resolves to that
 * tree's header, every mdb_* name resolves to the prefixed symbol, and
 * LMDB_API_VTABLE names the per-engine vtable (lmdb_api_v09/lmdb_api_v10).
 *
 * For LMDB_FORCE_SYSTEM builds this file is compiled in place against the
 * system liblmdb, with no renaming, producing lmdb_api_sys.
 */

#include "lmdb.h"

#ifndef LMDB_API_VTABLE
# define LMDB_API_VTABLE lmdb_api_sys
#endif

/* mdb_api.h's conditional externs want the LMDB_ENGINE_* flags; they are
 * irrelevant for populating the vtable itself. */
#include "mdb_api.h"

const MdbApi LMDB_API_VTABLE = {
    MDB_VERSION_MAJOR,
    MDB_VERSION_MINOR,
    MDB_VERSION_PATCH,
    /* Data format: LMDB 1.0.x writes MDB_DATA_VERSION 3, 0.9.x writes 1. */
    (MDB_VERSION_MAJOR >= 1) ? 3u : 1u,

    /* env */
    mdb_env_create,
    mdb_env_open,
    mdb_env_close,
    mdb_env_copy2,
    mdb_env_copyfd2,
#ifdef HAVE_PATCHED_LMDB
    mdb_env_copy3,
    mdb_env_copyfd3,
#else
    0,
    0,
#endif
    mdb_env_sync,
    mdb_env_stat,
    mdb_env_info,
    mdb_env_get_flags,
    mdb_env_set_flags,
    mdb_env_get_maxkeysize,
    mdb_env_get_maxreaders,
    mdb_env_get_path,
    mdb_env_set_mapsize,
    mdb_env_set_maxdbs,
    mdb_env_set_maxreaders,
    mdb_reader_check,
    mdb_reader_list,

    /* txn */
    mdb_txn_begin,
    mdb_txn_commit,
    mdb_txn_abort,
    mdb_txn_reset,
    mdb_txn_renew,
    mdb_txn_id,

    /* dbi */
    mdb_dbi_open,
    mdb_dbi_flags,
    mdb_dbi_close,
    mdb_drop,
    mdb_get,
    mdb_put,
    mdb_del,
    mdb_stat,

    /* cursor */
    mdb_cursor_open,
    mdb_cursor_close,
    mdb_cursor_get,
    mdb_cursor_put,
    mdb_cursor_del,
    mdb_cursor_count,

    /* misc */
    mdb_strerror,
    mdb_version,
};
