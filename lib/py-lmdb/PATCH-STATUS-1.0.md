# py-lmdb patch status against LMDB 1.0.1

Assessment of the 23 patches in `lib/py-lmdb/` against the imported upstream
LMDB 1.0.1 sources (tag `LMDB_1.0.1`, released 2026-08-06). Line numbers refer
to pristine 1.0.1 `lib/mdb.c` and shift once earlier patches in `setup.py`'s
list are applied.

Verdicts are based on reading the 1.0.1 code paths; no corrupt-database
reproducers were run against 1.0.1.

## Drop: fixed upstream or no longer applicable

| Patch | Verdict | Basis |
| --- | --- | --- |
| `fix-large-write` | Fixed upstream | ITS#10054 (`b0facd0`) caps every write at `MAX_WRITE` (1GiB) and chunks large overflow pages, mdb.c:4196, 4247. Copy-path hunk landed as ITS#9223 (`e11d5a0`), mdb.c:11248. |
| `fix-win-flush-large-write` | Fixed upstream | ITS#10538 (`36e581a`) rewrote the Win32 `mdb_page_flush` to chunk writes, mdb.c:4215-4243. |
| `win32-sparse-file` | No longer applicable | 1.0 defaults to incremental file growth via `NtCreateSection(SEC_RESERVE)` with a NULL section size, mdb.c:5087-5098; full preallocation is now opt-in through `MDB_FIXEDSIZE`. |
| `cve-2019-16225-reject-dirty-pages` | Not portable as written | The `P_DIRTY` page-header flag no longer exists; dirtiness is derived from `mp_txnid` (`IS_DIRTY_NW`/`IS_MUTABLE`/`IS_WRITABLE`, mdb.c:1153-1157). See "Follow-up" below. |

## Port: still needed

Applies with offsets or low fuzz; re-anchor fuzzy hunks rather than shipping
them fuzzy.

| Patch | Notes |
| --- | --- |
| `cursor-next-prev-uninitialized` | Applies. `C_DEL` still unconsulted at mdb.c:7874, 7958. |
| `cve-2019-16224-validate-db-flags` | Applies (one hunk fuzz 2). No `BAD_DB_FLAGS` equivalent upstream. |
| `cve-2019-16226-validate-node-del-size` | Applies (fuzz 1; BIGDATA branch now uses `sizeof(MDB_ovpage)`). |
| `cve-2019-16227-guard-xcursor-null` | 8/9 hunks apply. Hunk 7 is comment-only and needs re-anchoring. Hunk 8 is now redundant (mdb.c:10583 already tests `m3->mc_xcursor`) and can be dropped. |
| `cve-2019-16228-validate-psize` | Applies (fuzz 1). More important than before: ITS#9291 added `fsize / env->me_psize` at mdb.c:5570, so `mm_psize == 0` now divides by zero at env open. |
| `validate-node-read-size` | Applies (fuzz 2; `mv_size` assignment hoisted above the `F_BIGDATA` branch). |
| `validate-xcursor-nodedsz` | Applies (fuzz 2). Shares context with `cve-2019-16227` hunk 7; re-anchor together. |
| `validate-leaf2-keysize` | Applies once `validate-page-bounds` is applied (shares its `psize` local). |
| `guard-xcursor-null-d3d4` | Applies. Both sites intact at mdb.c:8359 and 9127-9147. |
| `validate-node-shrink-delta` | Applies. `mdb_node_shrink` unchanged, mdb.c:9513-9524. |
| `validate-nodedsz-cursor-put` | Applies. mdb.c:8747. |
| `validate-nodedsz-page-split` | Applies. ITS#10551 (`27b154b`) does *not* supersede it: that fixes even-padding in the split-point estimator (mdb.c:10866-10877), not the node-copy loop at mdb.c:10966-10974. |
| `validate-md-root` | Applies with fuzz 2; regenerate context. Only `mdb_cassert(mc, root > 1)` at mdb.c:7613, which compiles out under NDEBUG. |
| `fix-overflow-page-size-mul` | Keep hunk 1 only (`mdb_page_unspill`, mdb.c:2989). Hunk 2 is obsolete: the copy path now uses `MDB_ovpage.op_pages` typed `mdb_size_t` (mdb.c:1163), so the arithmetic promotes to 64-bit. |

## Port: needs hand-rewriting

| Patch | Why |
| --- | --- |
| `env-copy-txn` | 5/6 hunks apply. Hunk 6 fails: `mdb_env_copy2` now delegates to a new `mdb_env_copy_open()` helper (mdb.c:11776-11790). This is a feature patch py-lmdb hard-depends on — `mdb_env_copy3`/`mdb_env_copyfd3` are referenced from both `cpython.c` and `cffi.py`, and neither exists upstream. |
| `validate-page-bounds` | Lands with fuzz 2 but does not compile: 1.0.1 moved the `MDB_env *env` declaration into an inner block (mdb.c:7433). Use `txn->mt_env->me_psize`. `mdb_page_get` also lost its `int *lvl` out-parameter. |
| `validate-subpage-bounds` | Hunk fails; do not force it (with `-F3` it silently applies to the wrong function). Re-anchor before `MP_FLAGS(mp) = fp_flags;` at mdb.c:8865 (upstream dropped `| P_DIRTY` and inserted a `md_leaf_pages++`). |
| `validate-overflow-pages` | Both hunks fail. Overflow references now live in the node as `MDB_ovpage {op_pgno, op_pages, op_txnid}` (mdb.c:1163); rewrite in those terms. `mdb_page_get` validates only the first page, and drops `numpgs` entirely unless `MDB_RPAGE_CACHE` *and* `MDB_REMAP_CHUNKS` are active. `mdb_drop0` (mdb.c:12469) no longer fetches the page at all. |
| `validate-md-depth` | Hunk 1 lands only with fuzz 2 (the `BAD_DB_FLAGS`/`MDB_INVALID` context is gone; 1.0.1 uses `MDB_INCOMPATIBLE` at mdb.c:7594-7600). Hunk 2 fails outright: `mdb_dbi_open` was restructured (mdb.c:12265-12380) and the cleanup must now also undo `mt_dbiseqs[slot]`/`mt_dbflags[slot]`. |

## Follow-up

`cve-2019-16225-reject-dirty-pages` rejected a page fetched from the map that
claimed `P_DIRTY`, which would otherwise let `mdb_page_touch` skip
copy-on-write and then write through a `PROT_READ` mapping. That flag is gone,
but the same shape appears reachable via `mp_txnid`: no read path validates it,
and a large value satisfies `IS_WRITABLE` (mdb.c:1155-1157), which makes
`mdb_page_touch` (mdb.c:3028) return `MDB_SUCCESS` without copying while the
non-`MDB_WRITEMAP` map stays read-only (mdb.c:5117). Retaining this protection
needs a new patch, not a rebase. Unverified by reproducer.
