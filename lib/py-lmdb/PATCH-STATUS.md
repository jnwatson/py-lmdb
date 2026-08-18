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
