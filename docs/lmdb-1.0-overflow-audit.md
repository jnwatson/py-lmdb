# LMDB 1.0 integer-overflow audit (CodeQL, PR #479)

Status: **open**. The `mdb_env_incr_loadfd` finding below must be resolved
*before* the incremental-backup API is exported. See "Action required".

Audit of the 20 CodeQL "Multiplication result converted to larger type"
alerts raised against `lib1/mdb.c` when the LMDB 1.0.1 tree was vendored in
PR #479, plus the analysis behind classifying each one.

## Why we do not simply dismiss these

py-lmdb's threat model is deliberately stricter than upstream LMDB's.
Upstream does not treat a hostile or damaged database file as an attack the
library must defend against; py-lmdb does, because its users routinely open
`.mdb` files that arrived from somewhere else. The whole `lib*/py-lmdb/`
hardening series exists for that reason.

That difference matters here: several of these alerts sit on values read
straight out of a file. Under upstream's model they are not bugs. Under
ours they are.

The corollary — and the reason this file exists — is that "we don't call
that code today" is **not** a sufficient reason to dismiss an alert, if we
intend to start calling it. Dismissing on reachability grounds and then
exporting the function later is how a known defect becomes a shipped one.

## Action required before exporting the incremental-backup API

`mdb_env_incr_loadfd()` — which `mdb_env_incr_dump()`, `mdb_env_incr_dumpfd()`
and `mdb_env_incr_loadfd()` would make reachable — mis-sizes both a heap
buffer and an I/O loop from a value it reads out of the dump file.

`numpgs` and the byte counters both derive from `rp->mp_pages`, a `uint32_t`
read from the header of each page **in the dump file being restored**. On a
restore path that is untrusted input.

There are two defective multiplications. CodeQL flagged only the second.

### 1. `rsize *= rp->mp_pages;` — sizes the `pbuf` heap buffer (not flagged)

```c
#ifdef _WIN32
    DWORD rsize, rlen, w2;          /* 32-bit */
#else
    size_t rsize, w2;               /* 64-bit */
#endif
    ...
    rsize = env->me_psize;
    if (IS_OVERFLOW(rp)) {
        numpgs = rp->mp_pages;
        rsize *= rp->mp_pages;
        if (rp->mp_pages > 1) {
            ptr = realloc(pbuf, rsize);
```

* **POSIX** — `rsize` is `size_t`, so this promotes to 64 bits and is
  correct. An absurd `mp_pages` just makes `realloc` fail and the function
  returns `ENOMEM`.
* **Windows** — `rsize` is `DWORD`, 32-bit, so the product wraps.

CodeQL did not flag this because the vulnerable declaration is inside
`#ifdef _WIN32` and the analysis runs on Linux.

### 2. `rsize = numpgs * env->me_psize;` — sizes the write loop (alert #43, `lib1/mdb.c:12039`)

`numpgs` is `int`, `env->me_psize` is `unsigned int`. The multiplication is
performed in 32 bits on **both** platforms and only then widened to
`size_t`. It sets the length of the loop that writes `pbuf` into the
destination data file.

### Impact

**POSIX — silent data loss, not memory unsafety.** `pbuf` is sized
correctly in 64-bit arithmetic, and the wrapped `rsize` is always less than
or equal to the true product, so nothing reads out of bounds. But the write
loop then writes the *truncated* count. With 4 KiB pages and
`mp_pages == 2^20`, `rsize` wraps to exactly zero and that entire page range
is silently never written. The restore reports success and produces a
database that differs from the backup.

**Windows — heap buffer overflow.** `rsize` is 32-bit throughout, so defect
1 wraps first. `realloc(pbuf, small_or_zero)` yields a minimal buffer;
`memcpy(pbuf, buf, sizeof(buf))` immediately writes `PAGEHDRSZ` bytes past
it; then `rsize -= rlen` underflows to roughly `0xFFFFFFE8`, driving the
read loop to pour up to 4 GiB into that allocation.

**Confidence.** The POSIX truncation follows directly from the types and can
be demonstrated with a crafted dump file. The Windows overflow is code
reading only — it needs a Windows build and a reproducer before anyone
relies on it or reports it upstream.

### Proposed fix

A new patch in `lib1/py-lmdb/` that:

1. Uses a 64-bit byte count on all platforms. The Win32 `DWORD rsize`
   declaration has to change, not merely acquire a cast.
2. Clamps each `ReadFile`/`read` call to `MAX_WRITE`, mirroring what the
   write loop immediately below it already does. This is the same shape as
   the existing `fix-large-write` patch and is what makes a 64-bit count
   safe to hand to a 32-bit API.
3. Validates `mp_pages` against the map size before it is used to size
   anything, in the spirit of `validate-overflow-pages`.

Upstream should also get a report, with a reproducer, per the process in
`docs/upstream-psize-sigfpe.md`. Expect upstream to consider a malicious
dump file out of scope; that does not change what we need to ship.

## Full triage

All 20 alerts are "Multiplication result converted to larger type", all in
`lib1/mdb.c`.

### Pre-existing — identical code already in `lib/mdb.c` (0.9.35)

Flagged only because `lib1/mdb.c` is a new file. Unchanged risk relative to
what py-lmdb already ships on the 0.9 engine.

| Line | Function | Note |
| --- | --- | --- |
| 2989 | `mdb_page_unspill` | **Already fixed** by `fix-overflow-page-size-mul` (hunk 1). Still flagged because CodeQL scans the vendored `lib1/mdb.c`, not the patched `build/lib10/mdb.c`. |
| 8418 | `mdb_cursor_get` | `NUMKEYS(page) * mx->mc_db->md_pad`. Reachable today. See open question below. |
| 9355 | `mdb_node_add` | `dif * ksize`, `ksize` = `md_pad`. Reachable today. |
| 9476 | `mdb_node_del` | `x * ksize`. Reachable today. |
| 10792, 10798 | `mdb_page_split` | `* ksize`. Reachable today. |
| 11346 | `mdb_env_cwalk` | `me_psize * mc.mc_snum`. Reachable today via `copy(compact=True)`, but **not overflowable**: `me_psize <= MAX_PAGESIZE` (0x10000) and `mc_snum` is an `unsigned short` bounded by `CURSOR_STACK` (32), so the product cannot exceed 2^21. False positive. |

### New in 1.0 — export candidate

| Line | Function | Note |
| --- | --- | --- |
| 12039 | `mdb_env_incr_loadfd` | **Blocking.** See "Action required" above. |

### New in 1.0 — reachable only under `MDB_REMAP_CHUNKS`

`MDB_RPAGE_CACHE` does not exist in 0.9.35 at all (zero occurrences); this
is entirely new 1.0 code. It **is compiled** — `MDB_RPAGE_CACHE` defaults to
1 in `lib1/lmdb.h:194` and these symbols are present in the built extension
— but every path is gated at runtime on `MDB_REMAPPING(flags)`, which in our
configuration expands to `((flags) & MDB_REMAP_CHUNKS)`. py-lmdb never sets
that flag.

Lines 3781, 3794 (`mdb_txn_end`), 6464 (`mdb_env_close_active`), 6863, 6877
(`mdb_rpage_encsum`), 6984, 7019, 7063, 7105, 7130, 7167, 7327
(`mdb_rpage_get`).

These are **not dismissed** — they are deferred, on the same principle as
the incremental-backup finding. If `MDB_REMAP_CHUNKS` is ever exposed, they
must be audited first.

Note specifically that exposing **encryption or checksums**
(`mdb_env_set_encrypt`, `mdb_env_set_checksum`), which is a live candidate,
does **not** make 6863/6877 reachable: `mdb_rpage_encsum` is called only
from within `mdb_rpage_get`, which is itself gated on `MDB_REMAPPING`.

### Clean

No alerts in `mdb_env_incr_dumpfd` / `mdb_env_incr_dump` (their byte counter
is `mdb_size_t`, already 64-bit), `mdb_txn_prepare`, `mdb_env_rollback`,
`mdb_env_set_pagesize`, or `mdb_cursor_is_db`.

## RESOLVED (affected BOTH engines): `md_pad` from the DB record

Fixed by `validate-md-pad` in both series. Full write-up in
`lib/py-lmdb/PATCH-STATUS.md`; reproducer in `misc/md_pad_repro.py`;
regression tests in `tests/cve_test.py` (`MdPadTest`).

Alerts 8418, 9355, 9476, 10792 and 10798 all multiply by `ksize` /
`md_pad`. `validate-leaf2-keysize` bounds the **page's** `mp_pad` in
`mdb_page_get`, but nothing bounded `md_pad` as read from the **DB
record**, and the two were never required to agree.

Investigating it changed the severity assessment in two ways worth
recording, because both cut against how the alerts read:

- **The multiplication was the least important part.** `NUMKEYS` is
  already bounded to `psize/2` by `validate-page-bounds`, and wrapping the
  32-bit product only makes the result *smaller*. The real defect was that
  `md_pad` was unbounded as a length at all: a forged `0x8000` made
  `cursor.value()` return 32768 bytes for a 7-byte record — silently, no
  error, ~28.7 KB of adjacent mapping contents — with no multiplication
  involved.
- **It was also an out-of-bounds write**, which no alert flagged. The
  LEAF2 branch of `mdb_node_add` (alert 9355) uses `md_pad` as a
  `memmove()`/`memcpy()` length. Modest forged values write past the key
  slot and **commit silently**, corrupting the file; larger ones fault.
  That makes it a write primitive inside the writable mapping, not the
  read-only disclosure the alert list suggested.

This is a good illustration of the threat-model divergence described
above: upstream does not treat a crafted `data.mdb` as an attack, so none
of this is a bug by their standards, while under py-lmdb's model it was
the most serious finding in this audit.

## Re-running this audit

The alerts live at
`https://github.com/jnwatson/py-lmdb/security/code-scanning?query=tool%3ACodeQL+path%3Alib1%2Fmdb.c`.

CodeQL analyses the **vendored** `lib1/mdb.c`, not the patched
`build/lib10/mdb.c` that is actually compiled. Two consequences:

* Fixing something via a patch in `lib1/py-lmdb/` will **not** clear its
  alert. Alert #43 on line 2989 is the proof: our
  `fix-overflow-page-size-mul` patch already rewrites exactly that line.
* Any alert dismissal should say which patch addresses it, so the reasoning
  survives the next upstream bump.
