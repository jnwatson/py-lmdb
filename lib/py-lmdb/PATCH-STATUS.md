# py-lmdb patch status for the LMDB 0.9 tree

The 0.9 series in this directory is the long-standing baseline; its patches
are listed in `setup.py`'s `ENGINES` table and apply cleanly to the bundled
LMDB 0.9.35 sources. This file records only **open items** against it.

For the 1.0 tree, see `lib1/py-lmdb/PATCH-STATUS.md`, which additionally
documents which of these patches were dropped, ported mechanically, or
hand-rewritten for 1.0.

## Open items

- **`md_pad` from the DB record is unvalidated.** CONFIRMED with a
  reproducer on 0.9.35 (patched build, x86-64, 4096-byte pages). Needs a
  `validate-md-pad` patch in **both** series, since the code is identical in
  0.9 and 1.0. Found during the 1.0 CodeQL audit; see also
  `docs/lmdb-1.0-overflow-audit.md`.

  `md_pad` is the fixed key size of a `MDB_DUPFIXED` (LEAF2) database.
  `validate-leaf2-keysize` bounds the *page's* `mp_pad`
  (`0 < mp_pad <= psize - PAGEHDRSZ`) in `mdb_page_get`, but every
  arithmetic site uses the **DB record's** `md_pad`, and nothing requires
  the two to agree. Of the four places an `MDB_db` is loaded from disk,
  three (`mdb_txn_renew0`, the `DB_STALE` reload in `mdb_page_search`,
  `mdb_dbi_open`) run `BAD_DB_FLAGS`, which checks only `md_flags`; the
  fourth, `mdb_xcursor_init1`'s `memcpy(&mx->mx_db, NODEDATA(node),
  sizeof(MDB_db))` for an `F_SUBDATA` node, is unchecked entirely. Forging
  `md_pad` there leaves every page in the file internally consistent, so
  `validate-page-bounds` and `validate-leaf2-keysize` both still pass.

  Confirmed impact, forging only that one field:
  - *Silent out-of-bounds read.* `md_pad = 0x8000` makes `cursor.value()`
    return 32768 bytes for a 7-byte record — 28700 of the surplus bytes
    non-zero, i.e. adjacent mapping contents — with no error raised.
  - *Read crash.* `md_pad = 0xFFFFFFFF` gives SIGBUS in `cursor.value()`,
    and in `getmulti()` via `mv_size = NUMKEYS(page) * md_pad`.
  - *Out-of-bounds write.* The LEAF2 branch of `mdb_node_add` does
    `memmove(ptr+ksize, ptr, dif*ksize)` and `memcpy(ptr, key->mv_data,
    ksize)` with `ksize = md_pad`. `md_pad = 0x100` writes past the key
    slot and commits silently, corrupting the file; `md_pad >= 0x1000`
    gives SIGBUS. This is a write primitive inside the writable mapping,
    so it is the most severe of the three.

  Note the CodeQL alert's framing (`NUMKEYS * md_pad` can wrap) is the
  least important part: `NUMKEYS` is already bounded by `validate-page-bounds`
  to `psize/2`, and wrapping only makes the result *smaller*. The defect is
  that `md_pad` is unbounded as a length at all.

  Suggested shape, mirroring `BAD_DB_FLAGS`:

      #define BAD_MD_PAD(db, psize) \
          (((db)->md_flags & MDB_DUPFIXED) && \
           ((db)->md_pad == 0 || (db)->md_pad > (psize) - PAGEHDRSZ))

  applied at all four load sites. Gating on `MDB_DUPFIXED` is required, not
  cosmetic: `mm_psize` aliases `mm_dbs[FREE_DBI].md_pad`, so an ungated
  bound would reject every database. `FREE_DBI` is never `MDB_DUPFIXED`,
  and `mm_psize` has its own check in `cve-2019-16228-validate-psize`.

  Related, not separately reproduced: `mdb_xcursor_init1` also takes
  `mx_db.md_pad` from an embedded sub-page's `mp_pad`. Sub-pages live inside
  node data and are never fetched through `mdb_page_get`, so
  `validate-leaf2-keysize` does not cover them, and `validate-subpage-bounds`
  checks that sub-page's `mp_lower`/`mp_upper` but not its `mp_pad`. It is
  `uint16_t`, so the exposure is bounded at 64KB rather than 4GB. Worth
  covering in the same patch.
