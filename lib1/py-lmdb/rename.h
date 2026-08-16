/* Symbol-rename header for the bundled LMDB 10 tree.
 *
 * Force-included (by prepending '#include "lmdb_rename.h"' during the
 * setup.py source-copy step) into every translation unit of this engine
 * so that both bundled LMDB versions can be linked into a single
 * extension module without duplicate-symbol collisions.
 *
 * Regenerate the symbol list with:
 *   gcc -c -w -I<tree> <tree>/mdb.c <tree>/midl.c && nm -g --defined-only *.o
 * The list below is the union of both trees' externs plus the
 * py-lmdb env-copy-txn patch additions; renaming a name that does not
 * occur in a given tree is harmless.
 */
#ifndef LMDB_RENAME_H
#define LMDB_RENAME_H

#define LMDB_API_VTABLE lmdb_api_v10

#define mdb_cmp                      mdb10_cmp
#define mdb_cursor_close             mdb10_cursor_close
#define mdb_cursor_count             mdb10_cursor_count
#define mdb_cursor_dbi               mdb10_cursor_dbi
#define mdb_cursor_del               mdb10_cursor_del
#define mdb_cursor_get               mdb10_cursor_get
#define mdb_cursor_is_db             mdb10_cursor_is_db
#define mdb_cursor_open              mdb10_cursor_open
#define mdb_cursor_put               mdb10_cursor_put
#define mdb_cursor_renew             mdb10_cursor_renew
#define mdb_cursor_txn               mdb10_cursor_txn
#define mdb_dbi_close                mdb10_dbi_close
#define mdb_dbi_flags                mdb10_dbi_flags
#define mdb_dbi_open                 mdb10_dbi_open
#define mdb_dcmp                     mdb10_dcmp
#define mdb_del                      mdb10_del
#define mdb_drop                     mdb10_drop
#define mdb_env_close                mdb10_env_close
#define mdb_env_copy                 mdb10_env_copy
#define mdb_env_copy2                mdb10_env_copy2
#define mdb_env_copy3                mdb10_env_copy3
#define mdb_env_copyfd               mdb10_env_copyfd
#define mdb_env_copyfd2              mdb10_env_copyfd2
#define mdb_env_copyfd3              mdb10_env_copyfd3
#define mdb_env_create               mdb10_env_create
#define mdb_env_get_fd               mdb10_env_get_fd
#define mdb_env_get_flags            mdb10_env_get_flags
#define mdb_env_get_maxkeysize       mdb10_env_get_maxkeysize
#define mdb_env_get_maxreaders       mdb10_env_get_maxreaders
#define mdb_env_get_path             mdb10_env_get_path
#define mdb_env_get_userctx          mdb10_env_get_userctx
#define mdb_env_incr_dump            mdb10_env_incr_dump
#define mdb_env_incr_dumpfd          mdb10_env_incr_dumpfd
#define mdb_env_incr_loadfd          mdb10_env_incr_loadfd
#define mdb_env_info                 mdb10_env_info
#define mdb_env_open                 mdb10_env_open
#define mdb_env_rollback             mdb10_env_rollback
#define mdb_env_set_assert           mdb10_env_set_assert
#define mdb_env_set_checksum         mdb10_env_set_checksum
#define mdb_env_set_encrypt          mdb10_env_set_encrypt
#define mdb_env_set_flags            mdb10_env_set_flags
#define mdb_env_set_mapsize          mdb10_env_set_mapsize
#define mdb_env_set_maxdbs           mdb10_env_set_maxdbs
#define mdb_env_set_maxreaders       mdb10_env_set_maxreaders
#define mdb_env_set_pagesize         mdb10_env_set_pagesize
#define mdb_env_set_userctx          mdb10_env_set_userctx
#define mdb_env_stat                 mdb10_env_stat
#define mdb_env_sync                 mdb10_env_sync
#define mdb_env_sync0                mdb10_env_sync0
#define mdb_get                      mdb10_get
#define mdb_mid2l_alloc              mdb10_mid2l_alloc
#define mdb_mid2l_append             mdb10_mid2l_append
#define mdb_mid2l_free               mdb10_mid2l_free
#define mdb_mid2l_insert             mdb10_mid2l_insert
#define mdb_mid2l_need               mdb10_mid2l_need
#define mdb_mid2l_search             mdb10_mid2l_search
#define mdb_mid3l_insert             mdb10_mid3l_insert
#define mdb_mid3l_search             mdb10_mid3l_search
#define mdb_midl_alloc               mdb10_midl_alloc
#define mdb_midl_append              mdb10_midl_append
#define mdb_midl_append_list         mdb10_midl_append_list
#define mdb_midl_append_range        mdb10_midl_append_range
#define mdb_midl_free                mdb10_midl_free
#define mdb_midl_need                mdb10_midl_need
#define mdb_midl_search              mdb10_midl_search
#define mdb_midl_shrink              mdb10_midl_shrink
#define mdb_midl_sort                mdb10_midl_sort
#define mdb_midl_xmerge              mdb10_midl_xmerge
#define mdb_put                      mdb10_put
#define mdb_reader_check             mdb10_reader_check
#define mdb_reader_list              mdb10_reader_list
#define mdb_set_compare              mdb10_set_compare
#define mdb_set_dupsort              mdb10_set_dupsort
#define mdb_set_relctx               mdb10_set_relctx
#define mdb_set_relfunc              mdb10_set_relfunc
#define mdb_stat                     mdb10_stat
#define mdb_strerror                 mdb10_strerror
#define mdb_txn_abort                mdb10_txn_abort
#define mdb_txn_begin                mdb10_txn_begin
#define mdb_txn_commit               mdb10_txn_commit
#define mdb_txn_env                  mdb10_txn_env
#define mdb_txn_flags                mdb10_txn_flags
#define mdb_txn_id                   mdb10_txn_id
#define mdb_txn_prepare              mdb10_txn_prepare
#define mdb_txn_renew                mdb10_txn_renew
#define mdb_txn_reset                mdb10_txn_reset
#define mdb_version                  mdb10_version

#endif /* LMDB_RENAME_H */
