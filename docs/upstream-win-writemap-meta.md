# Upstream bug report draft: MDB_WRITEMAP commit fails on Windows

Draft for filing at <https://bugs.openldap.org/>, following the OpenLDAP
bug-writing guidelines. Tracked on the py-lmdb side as issue #486, where
py-lmdb skips the affected configuration rather than patching it.

**Not yet filed, and not yet reproduced on Windows by us** — see "Status of
this draft" at the end before sending.

---

**Summary:** MDB_WRITEMAP commit fails on Windows without MDB_NOSYNC

**Version:** LMDB 1.0.1. LMDB 0.9.35 is **not** affected.

**OS/Platform:** Windows only (observed on `windows-latest` GitHub Actions
runners, MSVC x64, Python 3.11 and 3.14). Linux and macOS are unaffected on
both release lines.

## Description

On Windows, an environment opened with `MDB_WRITEMAP` cannot commit a write
transaction unless `MDB_NOSYNC` or `MDB_NOMETASYNC` is also set.
`mdb_txn_commit()` fails with `ERROR_INVALID_HANDLE` ("The handle is
invalid").

The chain appears to be:

1. `mdb_env_open()` does not open `me_mfd` when `MDB_WRITEMAP` is set:

       if (!(flags & (MDB_RDONLY|MDB_WRITEMAP))) {
               rc = mdb_fopen(env, &fname, MDB_O_META, mode, &env->me_mfd);

   so `me_mfd` keeps the `INVALID_HANDLE_VALUE` assigned in
   `mdb_env_create()`. 0.9.35 has the identical guard.

2. `mdb_env_write_meta()`'s `MDB_WRITEMAP` fast path — which updates the
   meta page directly in the map and `goto done` without touching `me_mfd`
   — is wrapped in `#ifndef _WIN32`:

       #ifndef _WIN32 /* We don't want to ever use MSYNC/FlushViewOfFile in Windows */
           if (flags & MDB_WRITEMAP) {
                   ...
                   goto done;
           }
       #endif

   On Windows a `MDB_WRITEMAP` environment therefore falls through to the
   file-write path below it.

3. That path selects the handle as

       mfd = (flags & (MDB_NOSYNC|MDB_NOMETASYNC)) ? env->me_fd : env->me_mfd;

   With neither flag set this is `me_mfd`, i.e. `INVALID_HANDLE_VALUE`, and
   the following `WriteFile()` fails with `ERROR_INVALID_HANDLE`.

0.9.35 has no `#ifndef _WIN32` around its equivalent fast path, so a
`MDB_WRITEMAP` environment takes the map route there and never reaches the
`me_mfd` selection. That is the difference between the release lines.

Step 1 is a fact about both trees; steps 2 and 3 are quoted from 1.0.1. The
causal chain joining them is inferred from reading the code — the attached
reproducer is designed to confirm or refute it without instrumenting
`mdb.c` (see "Reproducer").

## Steps to reproduce

On Windows, with LMDB 1.0.1:

1. `mdb_env_create()`, `mdb_env_set_mapsize()`.
2. `mdb_env_open(env, dir, MDB_WRITEMAP, 0664)` — no other flags.
3. `mdb_txn_begin()`, `mdb_dbi_open()`, one or more `mdb_put()`.
4. `mdb_txn_commit()`.

The attached reproducer does this as case A, alongside three controls.

## Actual results

    case env flags                          mdb_txn_commit
    A    MDB_WRITEMAP                       The handle is invalid.
    B    MDB_WRITEMAP|MDB_NOSYNC +sync      ok
    C    MDB_WRITEMAP|MDB_NOMETASYNC        ok
    D    (no MDB_WRITEMAP)                  ok

(Case A as observed through a binding; B/C/D as predicted by the analysis
above. See "Status of this draft".)

## Expected results

All four cases commit. `MDB_WRITEMAP` at default sync settings is a
supported configuration and works on 0.9.35 on the same machine.

## Suggested fix

Three candidates, smallest first. We have not formed a view on which suits
the upstream design.

1. **Open `me_mfd` on Windows even under `MDB_WRITEMAP`** — drop
   `MDB_WRITEMAP` from the guard in `mdb_env_open()` when `_WIN32`. Smallest
   change; costs one extra file handle in a configuration that currently
   cannot commit at all.

2. **Select `me_fd` on the Windows `MDB_WRITEMAP` path** — make the `mfd`
   expression fall back when `me_mfd` is not open. Avoids the extra handle,
   but loses the write-through sync `me_mfd` exists to provide, so the meta
   write would need an explicit sync to keep the same durability.

3. **Restore the fast path on Windows** using an appropriate flush, if the
   `#ifndef _WIN32` was meant to avoid `FlushViewOfFile` specifically rather
   than to disable the whole branch.

Note that the `#ifndef _WIN32` comment ("We don't want to ever use
MSYNC/FlushViewOfFile in Windows") explains why the *sync* inside that
branch is unwanted on Windows, but the branch also performs the meta-page
update itself, which is what Windows then loses. If the intent was only to
avoid the flush, option 3 may be closer to it.

## Notes

- `MDB_NOSYNC` and `MDB_NOMETASYNC` both avoid the failure, which is a
  usable workaround but trades away the durability the caller asked for.
- The two flags avoid it for different reasons, which is what makes the
  reproducer's case C diagnostic: `MDB_NOSYNC` also stops `mdb_env_sync0()`
  from running its body, whereas `MDB_NOMETASYNC` changes only the `mfd`
  selection. If C passes while A fails, the meta-write handle is implicated
  and the sync path is not.
- `me_ovfd`, a Windows-only handle new in 1.0, is selected on a similar
  `MDB_NOSYNC` condition in `mdb_page_flush()`. It is opened
  unconditionally on Windows in `mdb_env_open()`, so it is not the handle at
  fault here, but it is worth checking that it is initialised — unlike
  `me_fd`, `me_lfd` and `me_mfd` it is not assigned `INVALID_HANDLE_VALUE`
  in `mdb_env_create()`, while `mdb_env_close0()` tests it against that
  value before closing. That is a separate latent issue, not this one.

## Status of this draft

Facts, as observed:

- The failure is real and reproducible in CI on `windows-latest`, via the
  py-lmdb binding, in both its CPython and CFFI builds and on Python 3.11
  and 3.14.
- It reproduces with **unpatched** upstream sources (`LMDB_PURE=1`, i.e.
  none of py-lmdb's local patches applied), which is what attributes it
  upstream rather than to py-lmdb.
- 0.9.35 runs the identical sequence on the same runner without error.
- `sync=False` (`MDB_NOSYNC`) avoids it.

Not yet established:

- The C reproducer below has been **compiled and run on Linux only**, where
  it correctly reports all four cases passing against pristine 1.0.1 and
  0.9.35. It has not been run on Windows, because no Windows machine was
  available to this work. Its case A is expected to fail there, and cases
  B/C/D to pass.
- The causal chain in "Description" is derived from reading 1.0.1, not from
  a debugger. Case C is the check: if it passes while A fails, the
  derivation holds; if both fail, it does not, and case B's explicit
  `mdb_env_sync()` result indicates whether the sync path is involved
  instead.

**Run the reproducer on Windows against pristine 1.0.1 and paste its table
into the report before filing.** If case C fails, revise the Description —
the analysis above would be wrong, and the report should say only what was
observed.

## Reproducer

`misc/win-writemap-sync-repro.c` in the py-lmdb tree. Build and run:

    cl /O2 /I lib1 misc\win-writemap-sync-repro.c lib1\mdb.c lib1\midl.c ^
       /Fe:repro.exe /link advapi32.lib
    repro

Exit status 0 = unaffected, 1 = bug reproduced, 2 = setup error. Building
against a 0.9.35 tree instead runs the same cases on that release line, which
is expected to print "all cases succeeded".
