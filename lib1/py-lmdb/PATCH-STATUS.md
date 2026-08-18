# py-lmdb patch status against LMDB 1.0.1

Outcome of porting the `lib/py-lmdb/` patch series (written against LMDB
0.9.35) to the bundled LMDB 1.0.1 tree (tag `LMDB_1.0.1`, released
2026-08-06). The ported series lives alongside this file and is registered in
`setup.py`'s `ENGINES` table.

Each patch here is diffed against the tree state after all preceding patches,
so the series applies without fuzz. Regenerate with the replay approach
described under "Maintaining this series" below.

## Dropped: fixed upstream or no longer applicable

| Patch | Verdict | Basis |
| --- | --- | --- |
| `fix-large-write` | Fixed upstream | ITS#10054 (`b0facd0`) caps every write at `MAX_WRITE` (1GiB) and chunks large overflow pages. The copy-path hunk landed as ITS#9223 (`e11d5a0`). |
| `fix-win-flush-large-write` | Fixed upstream | ITS#10538 (`36e581a`) rewrote the Win32 `mdb_page_flush` to chunk writes. This was py-lmdb's fix, contributed upstream; it is also in the pending 0.9.36. |
| `win32-sparse-file` | No longer applicable | 1.0 defaults to incremental file growth via `NtCreateSection(SEC_RESERVE)` with a NULL section size; full preallocation is now opt-in through `MDB_FIXEDSIZE`. |
| `cve-2019-16225-reject-dirty-pages` | Not portable as written | The `P_DIRTY` page-header flag no longer exists; dirtiness is derived from `mp_txnid` (`IS_DIRTY_NW`/`IS_MUTABLE`/`IS_WRITABLE`). See "Open items" below. |

## Ported with hand-rewriting

| Patch | Change required |
| --- | --- |
| `env-copy-txn` | 5/6 hunks carried over; the `mdb_env_copy2` hunk was re-anchored on 1.0's new `mdb_env_copy_open()` helper. `mdb_env_copy3`/`mdb_env_copyfd3` are hard dependencies of both binding implementations. |
| `validate-page-bounds` | 1.0 moved the `MDB_env *env` declaration into an inner block, so the added check uses `txn->mt_env->me_psize`. Note `mdb_page_get` also lost its `int *lvl` out-parameter. |
| `validate-subpage-bounds` | Re-anchored on `MP_FLAGS(mp) = fp_flags;` — 1.0 dropped `| P_DIRTY` (folded into `P_ADM_FLAGS`) and inserted `md_leaf_pages++` above. Do **not** force this hunk with `-F3`: it then applies silently to the wrong function. |
| `validate-overflow-pages` | Rewritten for 1.0's `MDB_ovpage {op_pgno, op_pages, op_txnid}` node layout. More important on 1.0 than 0.9: `mdb_drop0` no longer fetches the page at all, so an unvalidated range reaches `mt_free_pgs` directly. `mdb_page_get` validates only the first page and ignores its `numpgs` argument unless `MDB_RPAGE_CACHE` **and** `MDB_REMAP_CHUNKS` are active. |
| `fix-overflow-page-size-mul` | Hunk 1 only (`mdb_page_unspill`). Hunk 2 is obsolete: the copy path now reads `MDB_ovpage.op_pages` typed `mdb_size_t`, so the arithmetic promotes to 64-bit. |
| `cve-2019-16227-guard-xcursor-null` | 8/9 hunks carry over. Hunk 7 is comment-only and was re-anchored (1.0's `mdb_xcursor_init1` gained a leading `mc_flags &= ...` line). Hunk 8's guard is redundant on 1.0 — the enclosing condition already tests `m3->mc_xcursor` — but is harmless and was kept for parity with the 0.9 series. |

## Ported mechanically

`cursor-next-prev-uninitialized`, `cve-2019-16224-validate-db-flags`,
`cve-2019-16226-validate-node-del-size`, `cve-2019-16228-validate-psize`,
`validate-node-read-size`, `validate-xcursor-nodedsz`,
`validate-leaf2-keysize`, `guard-xcursor-null-d3d4`,
`validate-nodedsz-page-split`, `validate-node-shrink-delta`,
`validate-nodedsz-cursor-put`, `validate-md-depth`, `validate-md-root`,
`validate-md-pad`.

Two notes on these:

- `validate-nodedsz-page-split` is **not** superseded by ITS#10551 ("fix
  mdb_page_split nodesize calculation"). That commit fixes even-padding in the
  split-point estimator; this patch bounds-checks the node-copy loop.
- `validate-md-depth` needed no hand-porting only because it follows
  `cve-2019-16224` in the series, which re-creates the `BAD_DB_FLAGS` error
  block it anchors on. Assessed against a pristine tree it appears to fail.

## Verified

An unvalidated `mm_psize` of 0 read from a crafted `data.mdb` is used as a
divisor during `mdb_env_open()`, raising SIGFPE. This affects **both** upstream
release lines, at different sites:

- 0.9.35 — `mdb.c:4552`, `env->me_maxpg = env->me_mapsize / env->me_psize;`
- 1.0.1 — `mdb.c:5570`, `pgno_t maxpgno = fsize / env->me_psize;` (the
  ITS#9291 root-page sanity check, which precedes the `me_maxpg` division that
  1.0 also still has)

`cve-2019-16228-validate-psize` fixes both. Confirmed by C reproducer against
pristine trees: unpatched 0.9.35 and 1.0.1 both die with SIGFPE; the patched
1.0 tree returns `MDB_INVALID`. Worth reporting upstream — see
`docs/upstream-psize-sigfpe.md`.

### `validate-md-pad` (new; also added to the 0.9 series)

The LEAF2 fixed key size was bounded only on the *page* (`mp_pad`, by
`validate-leaf2-keysize` in `mdb_page_get`), never on the *DB record*
(`md_pad`) that every LEAF2 computation actually uses, and nothing required
the two to agree. `mdb_xcursor_init1`'s `memcpy` for an `F_SUBDATA` node was
unchecked entirely, so forging that one field left every page internally
consistent while the key size became arbitrary.

The 1.0 port is mechanical — the affected functions are structurally
identical — but the *reproducer* is not: **1.0 aligns node data to an even
offset** (`NODEDATA` uses `EVEN(mn_ksize)`, where 0.9 uses `mn_ksize`
directly), so a walker written for 0.9 lands one byte off for odd-length
keys and silently reads the wrong field. `tests/cve_test.py` handles this in
`_nodedata_off()`; `misc/md_pad_repro.py` in `Layout.nodedata()`.

Confirmed on **both** engines with `misc/md_pad_repro.py`. Unpatched, all
five cases fail identically on 0.9.35 and 1.0.1: a forged `md_pad` of
`0x8000` returns 32768 bytes for a 7-byte record (~28.7 KB of adjacent
mapping contents, no error raised), `0xFFFFFFFF` faults on both the plain
read and `getmulti` paths, `0x1000` faults in the `mdb_node_add` **write**
path, and the sub-page `mp_pad` variant faults as well. Patched, all five
are refused with `MDB_BAD_TXN` on both engines.

Note the bound accepts zero. `mdb_dbi_open` zeroes the whole record when
creating a DB, so a perfectly normal `MDB_DUPFIXED` database has
`md_pad == 0`; only a DUPFIXED *sub*-DB, whose pages are always LEAF2,
additionally requires non-zero. `FREE_DBI` is excluded because its `md_pad`
aliases `mm_psize` and legitimately equals the full page size.

## Open items

- **Integer-overflow audit of new 1.0 code** — see
  `docs/lmdb-1.0-overflow-audit.md`. CodeQL raised 20 multiplication-overflow
  alerts against `lib1/mdb.c`; 13 are in code that does not exist in 0.9.
  One of them (`mdb_env_incr_loadfd`) **blocks exporting the incremental
  backup API** and needs a patch first. The rest are deferred, not
  dismissed: they become reachable if `MDB_REMAP_CHUNKS` is ever exposed.

- **`cve-2019-16225`'s protection is not carried forward.** The patch rejected
  a mapped page claiming `P_DIRTY`, which would otherwise let
  `mdb_page_touch` skip copy-on-write and then write through a `PROT_READ`
  mapping. That flag is gone, but the same shape appears reachable via
  `mp_txnid`: no read path validates it, and a large value satisfies
  `IS_WRITABLE`, which makes `mdb_page_touch` return `MDB_SUCCESS` without
  copying. Retaining this protection needs a new patch written against
  `mp_txnid`. **Unverified by reproducer** — this is code reading only.
- **`mdb_ovpage_free` remains unchecked** on both lines: it takes `ovpages`
  from the page header and feeds it to `mdb_midl_append_range` with no bound.
  Out of scope for this port (the 0.9 patch never covered it), but it is the
  natural companion to `validate-overflow-pages`.
- **`tests/cve_test.py` covers 17 of its 25 cases on the 1.0 engine.** The
  file now detects `PAGEHDRSZ` and `PAGEBASE` at import and expresses every
  offset in terms of them, so most corruption recipes work on either engine.
  Run the suite against 1.0 with `LMDB_DEFAULT_LIB_VERSION=1`.

  Eight cases remain marked `only_v09` (see the decorator's docstring for
  the per-test reasons). None of them crash on 1.0 — the corruption simply
  does not reach the same code path — but the hardening they exercise is
  verified only on 0.9 until each recipe is re-derived. The `P_DIRTY` one
  cannot be ported at all; it is the same gap as the `cve-2019-16225` item
  above.

  Note when re-deriving them: **1.0 sets `PAGEBASE = PAGEHDRSZ`**, where 0.9
  has `PAGEBASE = 0` (ITS#7713 graduated out of `MDB_DEVEL`). That shifts the
  frame of reference for `mp_lower`, `mp_upper` and every `mp_ptrs` entry —
  it was the single cause of most of the initial cross-engine failures.

## Maintaining this series

`misc/run-upstream-mtests.sh` builds upstream's `mtest` programs against both
the pristine `lib1/` tree and the patched `build/lib10-plain`, and diffs the
output; the series should be indistinguishable from pristine LMDB on
well-formed input. It also checks `lib1/`'s sources still match the upstream
tag byte for byte. Currently `mtest` through `mtest5` are identical on both
engines. See the 0.9 file for what is excluded and why (`mtest6`, and 1.0's
`RPROGS`: `mtest_remap`, `mtest_enc`, `mtest_enc2`).

When bumping the bundled 1.0 tree, regenerate rather than hand-editing: replay
the series onto the new tree one patch at a time, snapshotting between steps
and diffing consecutive snapshots. That keeps every patch's line numbers
consistent with the state after all preceding patches, which is what
`setup.py` requires. The four hand-ported hunks above are the ones to re-check
first.
