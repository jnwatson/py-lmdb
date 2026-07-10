# Large single-write handling for multi-GB overflow values

Fixed by `lib/py-lmdb/fix-large-write.patch`.

## What

A single LMDB value is stored in one contiguous overflow page spanning
`mp_pages` pages, and is written to disk in a single call of `mp_pages * psize`
bytes.  For a value larger than ~2 GiB that single call exceeds platform
per-call write limits:

* **POSIX** — Linux `write()`/`pwrite()` transfers at most `0x7ffff000`
  (~2 GiB) and returns a short count.  In `mdb_page_flush` the short count was
  compared with the requested size and treated as fatal
  (`rc = EIO; "short write, filesystem full?"`), so committing a >2 GiB value
  failed with `EIO`.
* **Windows** — `mdb_env_copythr` (the compacting-copy writer) passes the
  `size_t` length straight to `WriteFile`, whose `nNumberOfBytesToWrite` is a
  32-bit `DWORD`.  A length of exactly `2^32` truncates to `0` and the copy
  fails.  (`mdb_env_copyfd1` already caps each call at `MAX_WRITE`; the
  threaded writer did not.)

## Fix

* `mdb_page_flush`: on a non-negative short write, advance past the bytes
  written (skip completed iovecs, adjust the first partial one) and loop via
  `goto retry_write` until the whole page is written.
* `mdb_env_copythr`: cap each `DO_WRITE` at `MAX_WRITE` (1 GiB on 64-bit),
  matching `mdb_env_copyfd1`.

## Reproducing

`tests/large_write_test.py` (opt-in, `LMDB_TEST_LARGE=1`) stores a 2.25 GiB
value, commits, reads it back, and round-trips it through a compacting copy.
Before the fix the commit fails with `mdb_txn_commit: Input/output error` on
Linux; after the fix both succeed.  It is skipped by default because it needs
>2 GiB of RAM and disk.

## Verification

Verified end-to-end on both Linux and Windows: the opt-in test (a 2.25 GiB
value committed, read back, and round-tripped through a compacting copy)
passes on both, and the full suite is green on both.  The `mdb_env_copythr`
cap is exercised on Windows at 2.25 GiB (> the 1 GiB `MAX_WRITE`).  The exact
`2^32`-byte boundary that truncates the `WriteFile` `DWORD` occurs only for a
value at `MAXDATASIZE` (~4 GiB); the cap prevents it but that specific size is
not driven by a test.
