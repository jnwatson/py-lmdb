# Local-model review findings

Source: a code review by a local llama.cpp model (Qwen 3.8 27B distill; see
the /local-review skill), each finding hand-verified against the source
before being recorded here. Numbering is the order findings were logged.

## Disposition (2026-08-23, branch fix-local-review-findings)

Fixed: 1, 2, 3, 4, 6, 8 (verify_fuzz.py); 11, 12, 13 (tool.py); 14, 15, 16,
21 (cpython.c); 17, 18, 19, 20, 22 (cffi.py); 23, 24 (bundled patches).

Deferred, with reason:
- 5 (verify_fuzz only catches VerifyError around V.verify): deliberate --
  a verifier exception should surface loudly, not be swallowed as "rejected".
- 7 (counterexample dirs kept): by design -- they are the fuzzer's output.
- 9, 10 (engine.c designated initializers / _Static_assert layout contract):
  good hardening but defensive-only on working glue with build-matrix
  (compiler/C-standard) risk; no live bug. Worth a separate focused change.
- 25 (env-copy-txn: assert txn->mt_env == env): caller-error guard, not
  attacker-reachable via data files; needs multi-hunk patch surgery that
  would risk the series for little value. Deferred to keep patch edits to
  the two zero-structural-risk in-place changes.

Refuted findings are marked inline below; do not act on them.

## Real

1. **No timeout on the forked child.** `run_child` blocks in
   `os.waitpid(pid, 0)` forever. A mutation that makes the engine *hang*
   (e.g. a cyclic B-tree or free-list walk) rather than crash stalls the
   whole fuzz run silently. Fix: deadline via `waitpid(WNOHANG)` polling +
   `SIGKILL`, or `signal.alarm` in the child.

2. **Harness exceptions are recorded as engine counterexamples.** The child
   exits 3 on any `BaseException` that isn't `lmdb.Error`, and the parent
   treats any nonzero exit the same as a signal death. A Python-level harness
   bug (or an unexpected exception type escaping the binding) would be
   counted as "verify passed but engine died". Fix: distinguish signal deaths
   (true crashes) from exit 3 (report separately as harness errors).

3. **`lmdb.open()` in `engine_walk` is outside the `try/except lmdb.Error`.**
   A verify-accepted file that the engine cleanly *refuses to open* (raises,
   doesn't crash) escapes to `run_child`'s `BaseException` handler and is
   recorded as a counterexample (exit 3). Sharpens finding 2: move the open
   inside the try (guarding `env.close()` for the unbound case).
   [Qwen 3.8 27B unablated, 8k reasoning cap; hand-verified]

4. **`--maxflips 0` raises `ValueError`** — `rnd.randint(1, 0)` has an empty
   range; no lower-bound validation on the argument.
   [same source; hand-verified]

## Plausible / minor

5. **Only `V.VerifyError` is caught around `V.verify(work)`.** Any other
   exception from the verifier on a mutated file kills the entire run,
   losing all progress. (Never observed in ~9000 iterations; crashing loudly
   on a verifier bug is arguably desirable — could instead be counted as its
   own "verifier-exception" bucket.)

6. **`engine_walk` can exhaust `max_dbs` slots on a corrupted file.**
   Spuriously-successful `env.open_db(k, txn=t)` calls on garbage main-DB
   keys each consume one of the 8 handle slots and are never closed; later
   legitimate opens then fail (caught, walk continues). Consequence is
   minor, mechanism is real. Fix: raise `max_dbs` or track/close handles.

## Noted, by design / cosmetic

7. Counterexample directories (`fuzz_ce_*`) are never cleaned up —
   intentional: they are the fuzzer's output, and the path is printed.
8. The child leaks the `devnull` fd for the instant before `os._exit`
   (`os.close(devnull)` after `dup2` would be tidier).

# engine.c / mdb_api.h hardening (own glue code, not upstream trees)

From the same review experiment (Qwen distill, capped thinking), verified
2026-08-23. Both are defensive improvements, not live bugs:

9. **Use designated initializers for the vtable in `lmdb/engine.c`.** The
   ~45-slot MdbApi is populated positionally; several neighboring slots share
   a prototype (`env_get_flags`/`env_get_maxreaders`; the txn int-returning
   group), so a one-slot editing mistake within such a run would compile and
   dispatch to the wrong function. `.env_create = mdb_env_create,` style
   removes the class.

10. **`_Static_assert` the layout contract in `lmdb/mdb_api.h`.** The 0.9/1.0
    layout-identity contract (MDB_val/MDB_stat/MDB_envinfo, mdb_size_t ==
    size_t) lives only in a comment; compile-time asserts in each engine TU
    (platform-relative, not hardcoded LP64 sizes) would turn upstream drift
    into a build failure instead of memory corruption.

# Main binding files (chunked review, 2026-08-23; each item hand-verified)

## lmdb/tool.py — real, all reachable from ordinary use

11. **`restore` hangs forever on truncated input.** `read_until = lambda sep:
    b''.join(iter(read1, sep))` (tool.py:280): at EOF `read(1)` returns `b''`
    forever, never matching the sentinel. Any cdbmake file cut off inside a
    length field spins the process. DoS if restoring untrusted dumps.
12. **`warm` hangs forever on a truncated data file.** tool.py:506 loops
    `while fp.tell() < last_offset: fp.readinto(buf)`; at EOF readinto
    returns 0 and tell() stops advancing.
13. **`drop :main:` shows a traceback instead of the intended error.**
    tool.py:319: `map(ENV.open_db, ...)` is lazy, so `open_db(b':main:')`
    runs before the `name == ':main:'` guard.

## lmdb/cpython.c — real

14. **`parse_ulong` silently accepts non-int numerics** (cpython.c:1083).
    A float (e.g. `set_mapsize(1e9)`) passes both RichCompareBool range
    checks, then `PyLong_AsUnsignedLongLongMask` sets a TypeError and
    returns (u64)-1 — both ignored: the caller proceeds with a garbage
    value and the exception surfaces at some unrelated later call.
15. **`getmulti(keyfixed=True)` freezes `key_size` from the first key**
    (cpython.c:3278/3332): later keys of a different size are memcpy'd
    with the first key's length — an out-of-bounds READ off the mmap and
    silently corrupted results. Destination is sized for key_size, so no
    write overflow. Should validate each key's size matches and raise.
16. **`db_from_name` leaks the MDB_dbi if `PyObject_New` fails**
    (cpython.c:1461): the dbi_flags failure path closes the dbi, the
    OOM path does not. Each leak burns a max_dbs slot. OOM-only.

## lmdb/cffi.py — real

17. **`set_mapsize` after `close()` raises a cffi TypeError, not Error.**
    close() sets `_dbs = None` (cffi.py:1160) before `_env = _invalid`,
    so the pre-open fast path (cffi.py:1008) passes the `_invalid`
    sentinel to C. Move the `_env is _invalid` check first.
18. **`copy()` breaks on a bytes path** (cffi.py:1212): unconditional
    `path.encode(...)`; `__init__` handles str and bytes. API
    inconsistency with the C extension.
19. **`flags()`, `max_key_size()`, `max_readers()` skip `_close_lock`**
    (cffi.py:1377-1413): every other C-calling method takes it to
    serialize against close(); these three race a concurrent close
    (same hardening class as issues #465/#475).

## Plausible, not fully verified

20. cffi `Environment.__init__` may leak the MDB_env if an exception
    fires between `mdb_env_create` and `_pid` assignment (`__del__`
    guards on `_pid`). 21. cpython `env_copyfd` reports
    "mdb_env_copyfd3" in errors even on the copyfd2 path. 22. cffi
    `dbs()` can emit `b''` if the main DB holds an empty key.

Notable refuted claims (do not re-report): missing GC/tp_traverse
(DbObject/child linkage is a deliberate non-refcounted parent-child
invalidation scheme); reverse iteration mispositioning (MDB_LAST rescue
at cpython.c:4176, verified empirically); iterator silently swallowing
hard errors (_cursor_get_c raises for anything but NOTFOUND); cffi
commit "leak" (mdb_txn_commit frees the txn even on failure — the
suggested abort-on-failure fix would be a double-free).

# Bundled-LMDB hardening patches (reviewed 2026-08-23; verified vs source)

Reviewing our own patch series (safe to change; we are conservative about
the upstream trees themselves). Most model findings were refuted by the
MAX_PAGESIZE cap or by misread page geometry; these survived:

23. **`validate-overflow-pages` uses a wrap-prone bound; the sibling patch
    does not.** Both sites check `op_pgno + op_pages > mt_next_pgno`
    (lib1 and lib). `op_pgno`/`op_pages` are u64 read from disk; at the
    `mdb_drop0` free site the sum can wrap for a crafted extent and pass.
    The sibling `validate-ovpage-free` deliberately writes it wrap-safe
    (`ovpages > next || pg > next - ovpages`, per its own comment). Make
    `validate-overflow-pages` match that form for consistency. Low
    severity (the cursor_put site fetches the page via MDB_PAGE_GET first),
    but the inconsistency is real. **Best patch-review finding.**

24. **`cve-2019-16228-validate-psize` has no minimum bound.** It rejects
    zero, non-power-of-2, and `> MAX_PAGESIZE`, but admits 1/2/4/8 — all
    smaller than PAGEHDRSZ. A crafted meta with psize=8 sets me_psize=8;
    downstream header reads land in adjacent pages. Low severity
    (crafted-file only; later md_pad/page-bounds checks read garbage but
    stay in-map), but adding `meta.mm_psize < PAGEHDRSZ` closes it cleanly.

25. **`env-copy-txn` does not check `txn->mt_env == env`.** A caller
    passing a txn from a different environment would copy from the wrong
    mmap. Caller-error only (not attacker-reachable via data files), but a
    one-line assert in our own patch would harden it.

Refuted (do NOT act on): node-del `ptr < MP_UPPER` is correct (nodes grow
downward from page end; deletes work — the "inverted, breaks all deletes"
claim is a geometry misread); BAD_LEAF2_KEYS 32-bit product cannot overflow
(MAX_PAGESIZE = 0x8000/0x10000 caps the product at 2^31, the model assumed
96KB-256KB pages); freedb spill-path DOES set MDB_TXN_ERROR (inside
mdb_page_dirty itself); ovpage-free already excludes meta pages via
IS_OVERFLOW(mp); validate-page-bounds "dead clause" in 1.0 is deliberate
(the macro is shared with 0.9 where PAGEBASE=0 makes it live).
