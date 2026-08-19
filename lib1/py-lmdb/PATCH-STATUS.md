# py-lmdb patch status against LMDB 1.0.1

Outcome of porting the `lib/py-lmdb/` patch series (written against LMDB
0.9.x, now 0.9.36) to the bundled LMDB 1.0.1 tree (tag `LMDB_1.0.1`, released
2026-08-06). The ported series lives alongside this file and is registered in
`setup.py`'s `ENGINES` table.

Each patch here is diffed against the tree state after all preceding patches,
so the series applies without fuzz. Regenerate with the replay approach
described under "Maintaining this series" below.

## Dropped: fixed upstream or no longer applicable

| Patch | Verdict | Basis |
| --- | --- | --- |
| `fix-large-write` | Fixed upstream | ITS#10054 (`b0facd0`) caps every write at `MAX_WRITE` (1GiB) and chunks large overflow pages. The copy-path hunk landed as ITS#9223 (`e11d5a0`). LMDB 0.9.36 took ITS#10054 but **not** ITS#9223, so the 0.9 series still carries the copy-path hunk alone. |
| `fix-win-flush-large-write` | Fixed upstream | ITS#10538 (`36e581a`) rewrote the Win32 `mdb_page_flush` to chunk writes. This was py-lmdb's fix, contributed upstream; it is in 0.9.36 as well, so the 0.9 series no longer carries it either. |
| `win32-sparse-file` | No longer applicable | 1.0 defaults to incremental file growth via `NtCreateSection(SEC_RESERVE)` with a NULL section size; full preallocation is now opt-in through `MDB_FIXEDSIZE`. |
| `cve-2019-16225-reject-dirty-pages` | Rewritten, not dropped | The `P_DIRTY` page-header flag no longer exists; dirtiness is derived from `mp_txnid` (`IS_DIRTY_NW`/`IS_MUTABLE`/`IS_WRITABLE`). Replaced by `cve-2019-16225-validate-mp-txnid`, which bounds that field instead — see "Verified" below. |

## Ported with hand-rewriting

| Patch | Change required |
| --- | --- |
| `env-copy-txn` | 5/6 hunks carried over; the `mdb_env_copy2` hunk was re-anchored on 1.0's new `mdb_env_copy_open()` helper. `mdb_env_copy3`/`mdb_env_copyfd3` are hard dependencies of both binding implementations. |
| `validate-page-bounds` | 1.0 moved the `MDB_env *env` declaration into an inner block, so the added check uses `txn->mt_env->me_psize`. Note `mdb_page_get` also lost its `int *lvl` out-parameter. |
| `validate-subpage-bounds` | Re-anchored on `MP_FLAGS(mp) = fp_flags;` — 1.0 dropped `| P_DIRTY` (folded into `P_ADM_FLAGS`) and inserted `md_leaf_pages++` above. Do **not** force this hunk with `-F3`: it then applies silently to the wrong function. Its `mp_upper` bound was later corrected for LEAF2 sub-pages (issue #481); the 0.9 file has the analysis, and the change is identical on both trees. |
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

### `cve-2019-16225-validate-mp-txnid` (1.0 only)

The open item recorded here — that 1.0 lost CVE-2019-16225's protection when
`P_DIRTY` was removed — was **code reading only**. It is now confirmed by
reproducer, and the hypothesis was right.

`mdb_page_touch()` opens with:

```c
if (IS_SUBP(mp) || IS_WRITABLE(txn, mp))
        return MDB_SUCCESS;
```

`IS_WRITABLE(txn, p)` is `(p->mp_txnid >= txn->mt_workid)`, and nothing
validated `mp_txnid` on a page read from the map. A crafted value therefore
makes `mdb_page_touch()` report success without performing the
copy-on-write, and the caller writes to a page it does not own.

Forging that one `uint64` on every B-tree page, then rewriting the records:

| Forged `mp_txnid` | Unpatched |
| --- | --- |
| `0xFFFFFFFFFFFFFFFF` | **SIGSEGV** — write through the `PROT_READ` map |
| `1000000` | **SIGSEGV** |
| `5` (> `mt_txnid`, small) | `MDB_PROBLEM` — reaches `mdb_page_unspill` on a page that was never spilled |
| `0xFFFFFFFFFFFFFFFF`, `MDB_WRITEMAP` | **writes land in place, silently** — the map is writable, so rather than faulting, pages of the previous snapshot are overwritten directly |

The fix bounds the field where the 0.9 patch bounds `P_DIRTY` — in
`mdb_page_get`, on the mapped-page path only. Placement matters twice over:

- It must **not** go at the shared `done:` label. Dirty pages reach that
  label too, and theirs legitimately carry `mp_txnid >= mt_workid > mt_txnid`
  for non-`MDB_WRITEMAP`; checking there rejects every write.
- The bound is `mp_txnid > mt_txnid`, not `>=`. Equality is legitimate: a
  page this txn spilled was flushed with `mp_txnid = mt_txnid`, and a read
  txn sees pages written by the txn that created its snapshot.

Meta pages are unaffected — they leave `mp_txnid` at zero, so they pass.

#### Known residual: `mp_txnid == mt_txnid` under `MDB_WRITEMAP`

One value still gets through, and it is worth stating plainly rather than
leaving as a footnote. A write txn's id is `last_committed + 1`, which an
attacker crafting the file knows, since the meta page carries the committed
id. Forging every B-tree page to exactly that value passes the bound above,
and under `MDB_WRITEMAP` `mt_workid == mt_txnid`, so `IS_WRITABLE()` is true
and `mdb_page_touch()` still skips the copy-on-write.

Demonstrated, patch applied, forging pages to `last_txnid + 1` and then
writing 200 records and calling **abort**:

| | write | on disk after abort |
| --- | --- | --- |
| no `MDB_WRITEMAP` | refused (`MDB_PROBLEM`) | 201 `ORIGINAL`, 0 `MODIFIED` |
| `MDB_WRITEMAP` | allowed | 1 `ORIGINAL`, **200 `MODIFIED`** |

So this is not only an MVCC problem for concurrent readers: because the
pages were never copied, **`abort()` does not roll back**. The writes are
already in the previous snapshot's pages.

The bound cannot simply be tightened to `>=`. Under `MDB_WRITEMAP` a page
this txn dirtied in place carries exactly `mt_txnid` (`SET_PGTXNID` sets
`mt_workid`), and re-reading such a page later in the same txn is completely
routine, so `>=` would reject normal operation.

Nor can it be closed by cross-checking the dirty list, which is the obvious
alternative. `mdb_page_dirty()` under `MDB_WRITEMAP` is:

```c
if (txn->mt_flags & MDB_TXN_WRITEMAP) {
        txn->mt_flags |= MDB_TXN_DIRTY;
        return;
}
```

— it sets a flag and returns without recording the page. There is no
per-page set to test against, because avoiding exactly that bookkeeping is
what `MDB_WRITEMAP` is for. Closing this would mean adding tracking upstream
deliberately omits.

0.9 has the same hole and a wider one: its patch excludes `MDB_WRITEMAP`
outright, so on 0.9 *every* forged value gets through, not just this one.
That is not an argument that this is fine.

It is worth being clear about which side of py-lmdb's threat-model
divergence this falls on. Upstream does not treat a hostile file as an
attack, so there is no upstream bug here and nothing to report — the same
conclusion `docs/lmdb-1.0-overflow-audit.md` reaches for the rest of the
series. But py-lmdb has already adopted the stricter model and shipped the
patches to enforce it, including the non-`MDB_WRITEMAP` half of this very
issue. By our own standard this is in scope and uncovered, not out of scope.
The divergence explains why the hole exists; it does not make it acceptable.

What follows from that is a documented boundary rather than a costly fix:
the series covers the default configuration, and callers opening files they
do not control should leave `writemap` at `False`. That is now stated in the
ChangeLog and in `lib/py-lmdb/PATCH-STATUS.md`, which carries the 0.9 side
of the analysis. Tracked in issue #484.

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
seven cases fail identically on 0.9.35 and 1.0.1: a forged `md_pad` of
`8 * psize` returns 32768 bytes for a 7-byte record (~28.7 KB of adjacent
mapping contents, no error raised), `0xFFFFFFFF` faults on both the plain
read and `getmulti` paths, `psize` faults in the `mdb_node_add` **write**
path, and the sub-page `mp_pad` variant faults as well. Patched, all seven
are refused on both engines.

Bounding the key size alone is not sufficient — `LEAF2KEY()` multiplies it
by the key index, so a size that fits the page still addresses far outside
it once a search reaches the middle of a full page. The patch also enforces
`NUMKEYS(page) * key_size <= usable space`, the relation `mdb_node_add`
maintains. See the 0.9 file for why that check is split in two, and for how
CI on macOS/arm64 (16 KB pages) exposed the gap that a 4 KB-page test suite
could not.

Note the bound accepts zero on a DB record. `mdb_dbi_open` zeroes the whole
record when creating a DB, so a perfectly normal `MDB_DUPFIXED` database has
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

- ~~**`cve-2019-16225`'s protection is not carried forward**~~ — carried
  forward by `cve-2019-16225-validate-mp-txnid`; see "Verified" below. The
  code-reading hypothesis recorded here was confirmed by reproducer.
- ~~**`mdb_ovpage_free` remains unchecked**~~ — fixed by
  `validate-ovpage-free`, added to both series. See the 0.9 file for the
  analysis; the patch is identical on both trees, since both take `ovpages`
  from the page header here even though 1.0's *put* path reads the count
  from the node instead.
- ~~**free-DB record structure is unvalidated in `mdb_page_alloc`**~~ —
  fixed by `validate-freedb-record`, added to both series. See the 0.9 file
  for the analysis; the patch is identical on both trees, since
  `mdb_page_alloc`'s reuse path, the skipped `me_maxpg` bound, and the
  `mdb_page_dirty` assertion are all common code. This is the structural
  ("Tier 0") half only: it guarantees a page number reused from a record is
  real and in range, which removes the memory-safety half of issue #484's
  free-DB caveat but not the semantic one — a coherent forgery naming a
  genuinely live page still needs an offline reachability verifier.
- **`tests/cve_test.py` covers 17 of its 25 cases on the 1.0 engine.** The
  file now detects `PAGEHDRSZ` and `PAGEBASE` at import and expresses every
  offset in terms of them, so most corruption recipes work on either engine.
  Run the suite against 1.0 with `LMDB_DEFAULT_LIB_VERSION=1`.

  Eight cases remain marked `only_v09` (see the decorator's docstring for
  the per-test reasons). None of them crash on 1.0 — the corruption simply
  does not reach the same code path — but the hardening they exercise is
  verified only on 0.9 until each recipe is re-derived.

  The `P_DIRTY` one still cannot be ported, because the flag is gone. It is
  no longer a coverage gap, though: the protection it tests is carried by
  `cve-2019-16225-validate-mp-txnid`, and three `only_v10` cases in the same
  test class exercise it against `mp_txnid` instead. That pair is the model
  for the rest — where 1.0 renamed the mechanism rather than removing it, the
  recipe needs rewriting, not skipping.

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
