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

The wrap only triggers for a value within one page of `MAXDATASIZE`, so a
reproducer needs a ~4 GiB value (and a >4 GiB map).  `tests/overflow_test.py`
`HugeValueCompactCopyTest` (opt-in, `LMDB_TEST_HUGE=1`) commits such a value
and round-trips it through a compacting copy:

* Without this fix, `me_psize * (mp_pages - 1)` in `mdb_env_cwalk` wraps to 0,
  so the overflow tail is skipped -- the copy truncates the value or walks off
  the end of the map (observed as `SIGBUS`).
* With the fix the value round-trips byte-for-byte.

It is skipped by default (needs ~4 GiB of RAM and disk).  The non-huge tests
in the same file exercise the same overflow-copy code path at ordinary sizes
to guard the common case against regression.

Committing a ~4 GiB value in the first place depends on the large-single-write
fix (PR #474): Linux `write()`/`pwrite()` transfers at most `0x7ffff000` bytes
per call, and the flush/copy writers must loop over the short count.

The huge test runs on Linux and Windows.  Committing such a value on Windows
also needs the `WriteFile` `DWORD`-length fix (PR #476): `mdb_page_flush` writes
each overflow page in one call whose length is a 32-bit `DWORD`, and for a value
near `MAXDATASIZE` the extent `psize * mp_pages` exceeds `2^32` and truncates.
With #476 and this fix both applied the value round-trips on both platforms.
