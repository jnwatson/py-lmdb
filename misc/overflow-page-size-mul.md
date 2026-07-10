# 32-bit overflow computing byte lengths from an overflow-page count

Fixed by `lib/py-lmdb/fix-overflow-page-size-mul.patch`.

## What

An LMDB overflow value is stored across `mp_pages` pages, where `mp_pages`
is a **`uint32_t`** (`MDB_page.mp_pb.pb_pages`).  A single value may be up to
`MAXDATASIZE` (`0xffffffff`, i.e. 4 GiB - 1), so `mp_pages` can reach
`~2^32 / psize` (≈ 1,048,577 for a 4 KiB page).

Two spots multiplied that count by the page size in 32-bit arithmetic and
only then widened the result to `size_t`:

| Site | Expression |
|------|------------|
| `mdb_page_touch` (COW spill)      | `memcpy(np, mp, num * env->me_psize)` |
| `mdb_env_cwalk`  (compacting copy)| `me_psize * (omp->mp_pages - 1)` |

`num`/`me_psize`/`pb_pages` are all 32-bit, so for a value within one page of
`MAXDATASIZE` the product reaches or exceeds `2^32` and wraps.  Example with a
4 KiB page and a value of exactly `0xffffffff` bytes (`mp_pages = 1048577`):

* `num * me_psize` = `1048577 * 4096` = `2^32 + 4096` -> wraps to `4096`
* `me_psize * (mp_pages - 1)` = `1048576 * 4096` = `2^32` -> wraps to `0`

The wrapped (tiny) length is then widened to `size_t` and used as a `memcpy`
length, so on 64-bit builds the copy is silently truncated -> data loss in the
copied / spilled value.

## Fix

Widen the multiplication to `size_t` before it can overflow:

```c
memcpy(np, mp, (size_t)num * env->me_psize);
my->mc_olen[toggle] = (size_t)my->mc_env->me_psize * (omp->mp_pages - 1);
```

`mc_olen` / `mc_wlen` are already `size_t`, so no further truncation occurs on
POSIX (`write()` takes `size_t`).

## Reproducing

The wrap only triggers for a value within one page of `MAXDATASIZE`, which is
awkward to exercise end-to-end because committing / copying a ~4 GiB single
value runs into *separate*, pre-existing large-single-write limitations that
are **out of scope** for this fix:

* **POSIX**: `mdb_page_flush` / `mdb_env_copythr` issue a single `write()` for
  the whole overflow extent.  Linux `write()` caps a single call at ~2 GiB and
  returns a short count; the flush of a >2 GiB overflow page fails with `EIO`
  at `mdb_txn_commit` before a copy is ever attempted.
* **Windows**: `mdb_env_copythr`'s `DO_WRITE` passes the `size_t` length to
  `WriteFile`, whose `nNumberOfBytesToWrite` is a 32-bit `DWORD`.  A length of
  exactly `2^32` truncates to `0`, so the compacting copy fails with `EIO`.

Because of those adjacent issues a full ~4 GiB round-trip is not a practical
CI test.  `tests/overflow_test.py` instead exercises the same overflow-copy
code path (compacting copy of multi-page overflow values) at ordinary sizes,
which guards against the `size_t` widening regressing the common case.

The adjacent large-write limitations above are noted here as follow-up work;
they are not addressed by this patch.
