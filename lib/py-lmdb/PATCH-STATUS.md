# py-lmdb patch status for the LMDB 0.9 tree

The 0.9 series in this directory is the long-standing baseline; its patches
are listed in `setup.py`'s `ENGINES` table and apply cleanly to the bundled
LMDB 0.9.35 sources. This file records **open items** against it, plus
findings worth keeping next to the patch that resolved them.

For the 1.0 tree, see `lib1/py-lmdb/PATCH-STATUS.md`, which additionally
documents which of these patches were dropped, ported mechanically, or
hand-rewritten for 1.0.

## Open items

None currently recorded against this tree. See
`lib1/py-lmdb/PATCH-STATUS.md` for items that affect both engines but were
found during the 1.0 port.

## Resolved

### `validate-ovpage-free` — unbounded overflow extent on the delete path

`mdb_ovpage_free()` takes the extent from the page header
(`ovpages = mp->mp_pages`) of a page named by an `F_BIGDATA` node, and uses
it three ways: as an allocation size (`mdb_midl_need`), as the number of
page numbers appended to `mt_free_pgs` (`mdb_midl_append_range`), and as a
loop bound writing into `me_pghead`. Nothing bounded it.

`validate-overflow-pages` covers the two sites that read the same field
elsewhere — `mdb_cursor_put` and `mdb_drop0` — but not this one, so the
**delete** path (`mdb_cursor_del` → `mdb_page_get` → `mdb_ovpage_free`)
reached it unchecked on both engines. 1.0 is affected equally: its *put*
path reads the count from the node (`MDB_ovpage.op_pages`), but
`mdb_ovpage_free` still reads the page header.

Reproduced on 0.9.35 and 1.0.1 alike by forging only that one `uint32`,
then deleting the record:

| Forged `mp_pages` | Unpatched behaviour |
| --- | --- |
| `0xFFFFFFFF` | SIGSEGV |
| `100000` | **delete commits silently**; `md_overflow_pages` underflows to ~2^64, and the free DB is left holding 100000 page numbers past the end of a 13-page file for a later txn to hand out — on 1.0 the database will not reopen |
| `0x7FFFFFFF` | `ENOMEM` |
| `0x40000000` | `MDB_BAD_VALSIZE` at commit |
| `0` | commits, frees nothing, leaks the extent |

The middle row is the one that matters. The extreme values fail loudly, but
a merely large count is accepted, committed, and *persists* — the damage
outlives the transaction that caused it, which none of the other cases in
this series do.

The check goes inside `mdb_ovpage_free` rather than at its two call sites,
so it covers both and any future caller. It also rejects `!IS_OVERFLOW(mp)`
— `mdb_drop0` only asserted that, and an assertion is not a validation —
and orders `pg > mt_next_pgno - ovpages` after the bound on `ovpages` so
the subtraction cannot wrap on a 32-bit `pgno_t`.

Regression tests are in `tests/cve_test.py` (`OverflowPagesTest`), including
`test_normal_overflow_delete_still_works`, which passes with the patch
removed — a positive test that only guards against false rejection is worth
having precisely because it does not fail for the wrong reason.

### `validate-subpage-bounds` — rejected 1-byte `MDB_DUPFIXED` values

Reported as issue #481: an `MDB_DUPFIXED` database with **1-byte values**
failed with `MDB_CORRUPTED` from the fifth duplicate on, on a file py-lmdb
had just created. Released in 2.3.0; both engines were affected, and
`LMDB_PURE=1` and pristine upstream were not.

The patch bounded a sub-page's `mp_upper` by the sub-page's own size:

```c
MP_UPPER(fp) + PAGEBASE > olddata.mv_size
```

That holds for a page whose nodes vary in size, but not for LEAF2.
`mdb_node_add` adjusts `mp_upper` by `ksize - sizeof(indx_t)`, which is a
**negative** delta once the fixed key size drops below `sizeof(indx_t)`, so
for a 1-byte value size `mp_upper` grows by one per insert and legitimately
overruns the sub-page. At the failing call: `numkeys=4 mp_pad=1
lower=24 upper=24` against a 20-byte sub-page.

The confusion is that `mp_upper` is only a data boundary where nodes vary in
size — which is exactly the branch whose `memcpy` length the check was
protecting (`olddata.mv_size - MP_UPPER(fp) - PAGEBASE`, an unsigned
underflow if `mp_upper` is too large). LEAF2 data is addressed by
`LEAF2KEY()` instead, and its copy length is `NUMKEYS(fp) * mp_pad`, which
`validate-md-pad` bounds. So the fix keeps the bound for variable-size
sub-pages and, for LEAF2, applies the bound that actually holds:

```c
MP_UPPER(fp) + PAGEBASE > olddata.mv_size + 2 * NUMKEYS(fp)
```

which follows from the same adjustment (`mp_upper` can exceed the sub-page
by at most 2 per key). Keeping *a* bound matters: `MP_UPPER(mp) =
MP_UPPER(fp) + offset` propagates the value into the destination page, and
for a sub-DB root reached through `sub_root` that page is never re-fetched
through `mdb_page_get`, so nothing downstream would re-check it.

`validate-md-pad`'s clause at the same site also had to change: it took
`olddata.mv_size >= PAGEHDRSZ` for granted, which it used to inherit from
the old `mp_upper` bound, and now establishes itself.

Two things worth remembering:

- **The existing `test_dupfixed` stored only two 1-byte values**, which fit
  the sub-page LMDB allocates up front, so it never reached the check. The
  regression tests in `tests/txn_test.py` push past that boundary and sweep
  the value sizes either side of `sizeof(indx_t)`.
- Fixing a patch this early in the series moves every later patch. See
  "Renumbering the series after an early fix" below.

### `validate-md-pad` — unvalidated LEAF2 key size

`md_pad` is the fixed key size of an `MDB_DUPFIXED` (LEAF2) database.
`validate-leaf2-keysize` bounds the *page's* `mp_pad`
(`0 < mp_pad <= psize - PAGEHDRSZ`) in `mdb_page_get`, but every LEAF2
computation uses the size held in the *DB record*, and nothing required the
two to agree. Of the four places an `MDB_db` is loaded from disk, three
(`mdb_txn_renew0`, the `DB_STALE` reload in `mdb_page_search`,
`mdb_dbi_open`) ran `BAD_DB_FLAGS`, which checks only `md_flags`; the
fourth, `mdb_xcursor_init1`'s `memcpy(&mx->mx_db, NODEDATA(node),
sizeof(MDB_db))` for an `F_SUBDATA` node, was unchecked entirely. Forging
`md_pad` there leaves every page in the file internally consistent, so
`validate-page-bounds` and `validate-leaf2-keysize` both still pass.

Reproduced on 0.9.35 and 1.0.1 alike (`misc/md_pad_repro.py`), forging only
that one field, on 4 KB pages:

| Case | Unpatched behaviour |
| --- | --- |
| `md_pad = 8 * psize`, read | `cursor.value()` returns 32768 bytes for a 7-byte record, ~28.7 KB of it adjacent mapping contents — silently, no error raised |
| `md_pad = 0xFFFFFFFF`, read | SIGBUS |
| `md_pad = 0xFFFFFFFF`, `getmulti` | SIGBUS (`mv_size = NUMKEYS(page) * md_pad`, in 32-bit arithmetic) |
| `md_pad = psize`, write | SIGBUS; smaller values write past the key slot and **commit silently**, corrupting the file |
| sub-page `mp_pad = 0x4000`, read | SIGBUS |
| `md_pad = psize / 8`, read | 512 bytes returned for a 7-byte record; SIGBUS on the write path |
| sub-page `mp_pad = 14`, read | 14 bytes returned for a 7-byte record |

The write case is the most severe: the LEAF2 branch of `mdb_node_add` uses
`md_pad` as a `memmove()`/`memcpy()` length, which is an out-of-bounds write
inside the writable mapping. Regression tests are in `tests/cve_test.py`
(`MdPadTest`); patched, all seven cases are refused on both engines.

The last two rows are the ones a per-key bound does not stop, and they cost
a second round after CI caught them. **Bounding the key size is not enough:
`LEAF2KEY()` multiplies it by the key index**, so a size that comfortably
fits the page still addresses far outside it once a search reaches the
middle of a full page — a 512-byte key size on a 4 KB page reads 140 KB
past the page it names. The invariant to enforce is the one `mdb_node_add`
maintains, `NUMKEYS(page) * key_size <= usable space`, which the patch adds
as `BAD_LEAF2_KEYS()` and applies in `mdb_page_get` (to both `mp_pad` and
`md_pad`), in `mdb_xcursor_init1`'s sub-page branch, and at the
`NUMKEYS(fp) * fp->mp_pad` `memcpy` in `mdb_cursor_put`.

That last site needs the count check *without* the size check: on the
`prep_subDB` path a sub-page is legitimately still empty (`olddata.mv_size
== PAGEHDRSZ`) while already carrying the key size it is about to be filled
with, so testing `ksize > space` there rejects ordinary DUPFIXED inserts.
`BAD_LEAF2_KEYS()` and `BAD_LEAF2_PAD()` are split for exactly this reason.

How CI found it is worth recording: **the bug was invisible on 4 KB pages
with the values the tests used.** macOS on arm64 has 16 KB pages, which put
the 0x1000 the write test had hard-coded inside the per-key bound, so the
patched build accepted it and bus-errored in `mdb_node_search`. Corruption
recipes that hard-code sizes silently test something different on a
different page size; `MdPadTest` now derives every forged value from the
page size in the file.

Two further details, both of which cost a wrong first attempt:

- **The bound must accept zero.** `mdb_dbi_open` zeroes the whole record
  when creating a DB, so a perfectly normal `MDB_DUPFIXED` database has
  `md_pad == 0`. Rejecting zero on a top-level DB record breaks every
  DUPFIXED database. Only a DUPFIXED *sub*-DB, whose pages are always
  LEAF2, additionally requires a non-zero value. `FREE_DBI` is excluded
  entirely: its `md_pad` aliases `mm_psize` and legitimately equals the
  full page size (`cve-2019-16228-validate-psize` checks that field).
- **The sub-page check must run before the sub-cursor is initialized.**
  `mdb_xcursor_init1` returns `void`, so it signals corruption by setting
  `MDB_TXN_ERROR`; that only becomes visible when something subsequently
  consults the txn. The `F_SUBDATA` path gets this for free, because it
  bails out before `mc_pg[0]` is set and the caller's page search then
  reports `MDB_BAD_TXN`. The sub-page path assigns `mc_pg[0] = fp` and
  `C_INITIALIZED` directly, so no page search follows; validating after
  that point leaves the caller reading a half-built cursor and quietly
  getting a zero-length value instead of an error.

Note the CodeQL alert that prompted this (`NUMKEYS * md_pad` can overflow)
described the least important part. `NUMKEYS` is already bounded to
`psize/2` by `validate-page-bounds`, and wrapping only makes the result
*smaller*. The defect was that `md_pad` was unbounded as a length at all —
the silent 32 KB disclosure needs no multiplication. The alert did point at
the right *expression*, though: `NUMKEYS * md_pad` is the quantity that has
to be bounded, just for fit rather than for overflow.

Also note that a reproducer written against 0.9 does not transfer to 1.0
unchanged: **1.0 aligns node data to an even offset** (`NODEDATA` uses
`EVEN(mn_ksize)`), so walking to an `F_SUBDATA` record with 0.9's
`mn_data + mn_ksize` lands one byte off for odd-length keys and silently
reads the wrong field.

## Renumbering the series after an early fix

Every patch is a diff against the tree state after all preceding ones, so
its hunk headers are relative to that state. Fixing a patch early in the
series therefore moves every patch after it — `validate-subpage-bounds` is
10th of 24 here and 9th of 20 in the 1.0 series, so one fix left 25 patches
applying at an offset.

`misc/renumber-patches.py <patch-name>` does that bookkeeping: it replays
each engine's series a patch at a time and rewrites the `@@` headers of
everything from `<patch-name>` onward. It changes headers only, and stops
with an error if a patch's body no longer matches what the replay produces —
which means the edit changed text that patch anchors on, and renumbering
cannot fix it. Repair that patch by hand, then re-run.

That happened once here: `validate-md-pad` adds a clause to the very
condition `validate-subpage-bounds` creates, so it had to be rewritten
before the rest could be renumbered.

Confirm the result with a build and check the log for `offset` and `fuzz`;
that, not the tool's own output, is what proves the series is consistent.

## Validating against upstream's own tests

`misc/run-upstream-mtests.sh` fetches the `mtest` programs from the upstream
tag matching each bundled tree (they are not part of this distribution),
builds each one twice — against the pristine tree and against the patched
one — and diffs the output. A hardening patch that only rejects corrupt
input should be **indistinguishable** from pristine LMDB on well-formed
input, so a difference is the signal, not the exit status.

Run it after touching either series. It pins the tests' RNG seed (they seed
from the clock), normalises pointers and pids, and verifies the bundled
sources still match the upstream tag before trusting the baseline.

Result for `validate-md-pad`: `mtest` through `mtest5` produce byte-identical
output on both engines, ~4,300 lines per engine. `mtest4` is the one that
matters — it is the only upstream test using `MDB_DUPFIXED`, and it drives
510 duplicates under a single key through `MDB_NEXT_MULTIPLE`, which is
exactly the `F_SUBDATA` sub-DB and `fetchm` path this patch guards.

Two upstream tests are excluded, both failing identically with and without
the patch:

- `mtest6` is not in upstream's `PROGS` and is not built by `make test`. It
  needs `mdb_dkey`, which exists only under `MDB_DEBUG`; forcing
  `-DMDB_DEBUG=1` then fails to compile on 0.9 (an unrelated `Yu` format-macro
  bug) and segfaults on 1.0 — on the **pristine** tree as well, with identical
  trace output up to the crash.
- 1.0's `mtest_remap`, `mtest_enc`, `mtest_enc2` are upstream's `RPROGS`,
  needing `MDB_REMAP_CHUNKS` and the `chacha8`/`crypto.lm` loadable module.
  Not wired up here; they exercise features py-lmdb does not expose.
