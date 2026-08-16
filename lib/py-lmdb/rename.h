/* Symbol-rename header for the bundled LMDB 09 tree.
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

#define LMDB_API_VTABLE lmdb_api_v09

#define mdb_cmp                      mdb09_cmp
#define mdb_cursor_close             mdb09_cursor_close
#define mdb_cursor_count             mdb09_cursor_count
#define mdb_cursor_dbi               mdb09_cursor_dbi
#define mdb_cursor_del               mdb09_cursor_del
#define mdb_cursor_get               mdb09_cursor_get
#define mdb_cursor_is_db             mdb09_cursor_is_db
#define mdb_cursor_open              mdb09_cursor_open
#define mdb_cursor_put               mdb09_cursor_put
#define mdb_cursor_renew             mdb09_cursor_renew
#define mdb_cursor_txn               mdb09_cursor_txn
#define mdb_dbi_close                mdb09_dbi_close
#define mdb_dbi_flags                mdb09_dbi_flags
#define mdb_dbi_open                 mdb09_dbi_open
#define mdb_dcmp                     mdb09_dcmp
#define mdb_del                      mdb09_del
#define mdb_drop                     mdb09_drop
#define mdb_env_close                mdb09_env_close
#define mdb_env_copy                 mdb09_env_copy
#define mdb_env_copy2                mdb09_env_copy2
#define mdb_env_copy3                mdb09_env_copy3
#define mdb_env_copyfd               mdb09_env_copyfd
#define mdb_env_copyfd2              mdb09_env_copyfd2
#define mdb_env_copyfd3              mdb09_env_copyfd3
#define mdb_env_create               mdb09_env_create
#define mdb_env_get_fd               mdb09_env_get_fd
#define mdb_env_get_flags            mdb09_env_get_flags
#define mdb_env_get_maxkeysize       mdb09_env_get_maxkeysize
#define mdb_env_get_maxreaders       mdb09_env_get_maxreaders
#define mdb_env_get_path             mdb09_env_get_path
#define mdb_env_get_userctx          mdb09_env_get_userctx
#define mdb_env_incr_dump            mdb09_env_incr_dump
#define mdb_env_incr_dumpfd          mdb09_env_incr_dumpfd
#define mdb_env_incr_loadfd          mdb09_env_incr_loadfd
#define mdb_env_info                 mdb09_env_info
#define mdb_env_open                 mdb09_env_open
#define mdb_env_rollback             mdb09_env_rollback
#define mdb_env_set_assert           mdb09_env_set_assert
#define mdb_env_set_checksum         mdb09_env_set_checksum
#define mdb_env_set_encrypt          mdb09_env_set_encrypt
#define mdb_env_set_flags            mdb09_env_set_flags
#define mdb_env_set_mapsize          mdb09_env_set_mapsize
#define mdb_env_set_maxdbs           mdb09_env_set_maxdbs
#define mdb_env_set_maxreaders       mdb09_env_set_maxreaders
#define mdb_env_set_pagesize         mdb09_env_set_pagesize
#define mdb_env_set_userctx          mdb09_env_set_userctx
#define mdb_env_stat                 mdb09_env_stat
#define mdb_env_sync                 mdb09_env_sync
#define mdb_env_sync0                mdb09_env_sync0
#define mdb_get                      mdb09_get
#define mdb_mid2l_alloc              mdb09_mid2l_alloc
#define mdb_mid2l_append             mdb09_mid2l_append
#define mdb_mid2l_free               mdb09_mid2l_free
#define mdb_mid2l_insert             mdb09_mid2l_insert
#define mdb_mid2l_need               mdb09_mid2l_need
#define mdb_mid2l_search             mdb09_mid2l_search
#define mdb_mid3l_insert             mdb09_mid3l_insert
#define mdb_mid3l_search             mdb09_mid3l_search
#define mdb_midl_alloc               mdb09_midl_alloc
#define mdb_midl_append              mdb09_midl_append
#define mdb_midl_append_list         mdb09_midl_append_list
#define mdb_midl_append_range        mdb09_midl_append_range
#define mdb_midl_free                mdb09_midl_free
#define mdb_midl_need                mdb09_midl_need
#define mdb_midl_search              mdb09_midl_search
#define mdb_midl_shrink              mdb09_midl_shrink
#define mdb_midl_sort                mdb09_midl_sort
#define mdb_midl_xmerge              mdb09_midl_xmerge
#define mdb_put                      mdb09_put
#define mdb_reader_check             mdb09_reader_check
#define mdb_reader_list              mdb09_reader_list
#define mdb_set_compare              mdb09_set_compare
#define mdb_set_dupsort              mdb09_set_dupsort
#define mdb_set_relctx               mdb09_set_relctx
#define mdb_set_relfunc              mdb09_set_relfunc
#define mdb_stat                     mdb09_stat
#define mdb_strerror                 mdb09_strerror
#define mdb_txn_abort                mdb09_txn_abort
#define mdb_txn_begin                mdb09_txn_begin
#define mdb_txn_commit               mdb09_txn_commit
#define mdb_txn_env                  mdb09_txn_env
#define mdb_txn_flags                mdb09_txn_flags
#define mdb_txn_id                   mdb09_txn_id
#define mdb_txn_prepare              mdb09_txn_prepare
#define mdb_txn_renew                mdb09_txn_renew
#define mdb_txn_reset                mdb09_txn_reset
#define mdb_version                  mdb09_version

#endif /* LMDB_RENAME_H */
