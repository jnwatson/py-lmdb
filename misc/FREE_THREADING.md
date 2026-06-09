# Free-threaded CPython (PEP 703) support — analysis & notes

Status: **investigation only, not scheduled.** This is a design note, not a
commitment. It captures what free-threading support would involve, what it
would actually buy users, and why the default (GIL) build is unaffected by the
approach described here.

## TL;DR

- **Today:** py-lmdb's C extension does *not* declare free-threading support, so
  importing it on a free-threaded interpreter (`python3.13t`/`3.14t`,
  `Py_GIL_DISABLED`) makes CPython **silently re-enable the GIL at runtime** plus
  a `RuntimeWarning`. It is correct, but you get no parallelism. With
  `PYTHON_GIL=0` it would run truly GIL-free and the data-race assumptions below
  would be violated.
- **The benefit is real but narrow.** Every LMDB call already runs with the GIL
  released (`UNLOCKED`/`ENV_UNLOCKED` always do `Py_BEGIN_ALLOW_THREADS`), so the
  I/O and B-tree work is *already* parallel across threads. Free threading only
  parallelizes the thin Python glue (arg parsing, buffer/result construction,
  refcounting, Python-level loop bodies). The headline "parallel reads" story is
  largely already available today via multiprocessing + shared mmap.
- **The default GIL build need not slow down.** Use CPython's
  `Py_BEGIN_CRITICAL_SECTION` API, which compiles to no-ops under the GIL. No
  hand-rolled `#ifdef`-ed locks are required (only a back-compat shim for
  Python < 3.13).
- **The cost is engineering risk, not runtime cost:** re-proving the #180 and
  #465 races without the GIL serializing entry into that code.

## How free threading treats non-supporting modules

PEP 703 adds an opt-in handshake for C extensions:

- A module declares it is free-threading-safe via the `Py_mod_gil` slot set to
  `Py_MOD_GIL_NOT_USED` (multi-phase init) or by calling
  `PyUnstable_Module_SetGIL(...)`.
- If a module does **not** declare support, importing it on a free-threaded
  interpreter **re-enables the GIL at runtime** and warns. `PYTHON_GIL=0` /
  `-X gil=0` forces it off — with no protection.

py-lmdb currently uses **single-phase init** with no GIL slot:

- `lmdb/cpython.c:4690` — `PyModuleDef` with `m_size = -1`, no `m_slots`.
- `lmdb/cpython.c:4822` — `PyModule_Create(&moduledef)`.
- No `Py_mod_gil` / `PyUnstable_Module_SetGIL` anywhere.
- `setup.py` builds a full (non-`abi3`) extension per Python version.

The pure-Python `lmdb/cffi.py` path (PyPy, or `LMDB_FORCE_CFFI=1`) has no
handshake — it's just Python — but relies on the same GIL-atomicity assumptions.
PyPy has no free-threaded build today, so this matters only for a hypothetical
free-threaded CPython + CFFI.

## Why py-lmdb is not free-threading-safe as written

The GIL currently does real synchronization work, not just refcount protection.
The races closed over the years (#180, #465) are all reasoned about "under the
GIL." Shared mutable bookkeeping on `EnvObject` that would race without it:

- **`active_ops` / `active_ops_waiter`** (`lmdb/cpython.c:197-203`) — counter of
  in-flight GIL-released ops; `env_clear` waits for it to hit 0 before
  `mdb_env_close`. Macros say "**Must be called with the GIL held**"
  (`ACTIVE_OPS_DEC` `:711`, `ENV_UNLOCKED` `:733-765`).
- **`write_txn_tid` / `write_txn_waiter`** (`:192`, `:209-211`) — the #465 fix.
  `env_clear` checks `write_txn_tid` "**under the GIL** ... so the owning thread
  ... cannot signal before we ... tear the mapping down" (`:1361-1372`). That
  ordering guarantee is entirely the GIL.
- **`spare_txn` free-list** (`:185`), the **invalidation linked lists**
  (`:350`, `:3920` — "releasing the GIL would allow other threads to modify the
  list or free..."), and object-lifetime / `Py_DECREF` ordering around teardown.

The CFFI version mirrors this (`_write_txn_tid`, `_write_txn_cond`, deps lists)
and relies on CPython GIL atomicity of attribute/list mutations.

## What would it buy users?

Key fact: **the GIL is already released around every LMDB C call.** Both
`UNLOCKED` and `ENV_UNLOCKED` always wrap the call in `Py_BEGIN_ALLOW_THREADS`
(e.g. `mdb_get`, cursor moves, `put`, `mdb_env_sync`, `mdb_env_copy`, commit's
msync/fsync). `enable_drop_gil()` is now a no-op kept for backward compat
(`lmdb/cpython.c:4646-4649`). So two threads already run the I/O and B-tree work
in parallel; the GIL only serializes the Python glue on either end.

Therefore free threading does **not** unlock concurrent LMDB access — that exists
now. It parallelizes the glue layer.

### Genuine wins

- **Read-heavy, multi-threaded, in-process workloads doing many small ops.**
  For small values, `mdb_get` is sub-microsecond and per-call cost is dominated
  by Python glue that's still serialized. N threads doing `txn.get()` in a loop
  serialize on that glue today; free threading lets it scale across cores.
- **Collapsing a multiprocessing setup back to threads.** The canonical way to
  scale LMDB past the GIL today is multiple processes sharing the env (LMDB is
  built for this — it's why robust mutexes / the #465 saga exist). It works now,
  and the shared mapping means the page cache isn't duplicated — but you pay to
  pickle results across the process boundary. Free threading keeps everything in
  one address space (read large values straight into an in-process
  index/model/array) with no IPC/serialization. This is the most concrete win.

### Little or no benefit

- **Writes:** LMDB permits one writer at a time; writers serialize on LMDB's own
  write mutex regardless of the GIL. No gain, possibly more contention.
- **Commit/sync-bound work:** fsync/msync already runs GIL-free.
- **Large-value reads:** time is in memcpy/page faults (GIL already released).
- **Single-threaded, asyncio (`lmdb.aio`), or users happy with multiprocessing +
  shared mmap.** No change.

### Bottom line

For a memory-mapped store whose flagship concurrency model is already "many
processes share one mapping, readers are lock-free MVCC," free threading is an
**incremental convenience** (eliminating IPC/pickle overhead and process
management for in-process multithreaded read scaling), **not a categorical new
capability.** Weigh that against re-proving the #180/#465 races without the GIL.

Recommendation: **low priority, demand-driven.** The cheap, safe move is to add
a `cp313t` CI job so the extension keeps importing cleanly (GIL auto-re-enabled,
fully correct) as free-threaded Python matures, and defer real no-GIL safety work
until a user presents the specific read-heavy in-process workload that needs it —
ideally with a benchmark.

## Does the explicit locking slow down the normal (GIL) build?

No — provided you use CPython's critical-section API rather than hand-rolled
locks. There are three categories of "extra locking":

### 1. Object-level mutual exclusion → free on the GIL build

`Py_BEGIN_CRITICAL_SECTION(op)` / `Py_END_CRITICAL_SECTION()` (public since 3.13):

- **Default (GIL) build:** expands to essentially `{ }` — no lock, no atomic,
  same machine code as today.
- **`Py_GIL_DISABLED`:** locks the per-object mutex in the `PyObject` header.

So guard `active_ops`, `write_txn_tid`, `spare_txn`, and the invalidation lists
with critical sections; the regular build is unaffected. You do **not** write
your own `#ifdef Py_GIL_DISABLED` — CPython already did.

The only manual `#define` needed is a back-compat shim, because py-lmdb supports
Python 3.8–3.12 where the macros don't exist:

```c
#if PY_VERSION_HEX < 0x030D0000
#  define Py_BEGIN_CRITICAL_SECTION(op) {
#  define Py_END_CRITICAL_SECTION() }
#endif
```

That shim *is* the "compile it out for non-freethread builds" mechanism — a few
lines at the top, not scattered through the code.

### 2. The counters → not free, but negligible

Making `active_ops` a C11 `_Atomic` / `_Py_atomic_add` does emit a real atomic
instruction on the GIL build (on x86, a `lock`-prefixed add — not a no-op). But
it sits right next to a full thread-state release/reacquire
(`lmdb/cpython.c:746-750`), so the cost is unmeasurable. Better: just protect the
counter with the same critical section as everything else (category 1), which is
free on the GIL build.

### 3. The real constraint: can't hold a critical section across the GIL release

A critical section (like the GIL in FT) is **suspended when the thread detaches**
— you cannot hold one across `Py_BEGIN_ALLOW_THREADS`, which py-lmdb does on every
LMDB call. The pattern must be:

```
enter critical section
  mutate/read bookkeeping (active_ops++, check write_txn_tid, ...)
exit critical section
Py_BEGIN_ALLOW_THREADS
  mdb_* call          // no lock held, as required
Py_END_ALLOW_THREADS
enter critical section
  ACTIVE_OPS_DEC, etc.
exit critical section
```

This is close to the existing structure (bookkeeping under the GIL, GIL released
for the LMDB call), so the work is wrapping those windows, not redesigning them.

## What full support would involve (rough plan)

1. **Build/handshake (small, but a promise).** Multi-phase init with
   `Py_mod_gil = Py_MOD_GIL_NOT_USED` (or `PyUnstable_Module_SetGIL`). Add a
   free-threaded interpreter (`cp313t`/`cp314t`) to the CI matrix and to
   cibuildwheel. Do **not** flip the flag until 2–3 are done.
2. **Make py-lmdb's own bookkeeping explicitly thread-safe.** Wrap `active_ops`,
   `write_txn_tid`, `spare_txn`, and the invalidation lists in critical sections
   (or atomics for the counters). Audit every "GIL held" / "don't release the
   GIL" comment — each is a latent race once the GIL is gone.
3. **Re-validate LMDB's threading contract under true parallelism.** The
   #180/#465 windows widen without the GIL incidentally serializing LMDB
   entry/exit. Re-verify the write-txn-to-OS-thread binding and the
   robust-mutex-unlock-on-owning-thread guarantee (the heart of #465), ideally
   with the aarch64 robust-mutex stress tests on a free-threaded build under
   ThreadSanitizer.
4. **Buffer-object lifetime.** `buffers=True` hands out `memoryview`s backed by
   the mmap, invalidated when the txn ends. "Don't touch a buffer after its txn
   closes" goes from a soft race to a hard one; document/tighten as needed.

**Pragmatic path:** ship step 1 *without* declaring GIL-safety first (default
behavior: GIL auto-re-enables, correct but not parallel), add `cp313t` to CI to
confirm it imports and passes, and only flip the `Py_mod_gil` slot after 2–3 are
validated under TSan + the crash/race suite on a free-threaded interpreter. Since
the default build is unaffected, this is low-risk to existing users — the risk is
concentrated entirely in the free-threaded path, gated behind the flag.

## References

- PEP 703 — Making the GIL optional in CPython.
- CPython C-API: "Supporting Free Threading" / `Py_mod_gil`,
  `PyUnstable_Module_SetGIL`, `Py_BEGIN_CRITICAL_SECTION`.
- py-lmdb issues #180 (env-close vs in-flight ops) and #465 (cross-thread close
  vs active write txn / robust mutex on aarch64).
- LMDB threading contract: `lib/lmdb.h` (txn/thread rules, `MDB_NOTLS`).
