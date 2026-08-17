# py-lmdb patch status for the LMDB 0.9 tree

The 0.9 series in this directory is the long-standing baseline; its patches
are listed in `setup.py`'s `ENGINES` table and apply cleanly to the bundled
LMDB 0.9.35 sources. This file records only **open items** against it.

For the 1.0 tree, see `lib1/py-lmdb/PATCH-STATUS.md`, which additionally
documents which of these patches were dropped, ported mechanically, or
hand-rewritten for 1.0.

## Open items

- **`md_pad` from the DB record is unvalidated.** `validate-leaf2-keysize`
  bounds the *page's* `mp_pad` (`0 < mp_pad <= psize - PAGEHDRSZ`) in
  `mdb_page_get`, but nothing in this series bounds `md_pad` as read from
  the **DB record**. In `mdb_cursor_get`, `data->mv_size = NUMKEYS(page) *
  mx->mc_db->md_pad` becomes a buffer length handed back to the caller;
  `NUMKEYS` is bounded by `validate-page-bounds`, but if `md_pad` is not,
  the product can wrap.

  Not yet confirmed with a reproducer. If confirmed it needs a
  `validate-md-pad` patch in **both** series, since the code is identical in
  0.9 and 1.0. Found during the 1.0 CodeQL audit; full analysis in
  `docs/lmdb-1.0-overflow-audit.md`.
