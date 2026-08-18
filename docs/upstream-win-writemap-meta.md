# Upstream bug report draft: MDB_WRITEMAP commit fails on Windows

Draft for filing at <https://bugs.openldap.org/>, following the OpenLDAP
bug-writing guidelines. Tracked on the py-lmdb side as issue #486, where
py-lmdb skips the affected configuration rather than patching it.

**Not yet filed.** Reproduced and confirmed on Windows against pristine
upstream sources — see "Confirmation".

---

**Summary:** MDB_WRITEMAP commit fails on Windows without MDB_NOSYNC

**Version:** LMDB 1.0.1. LMDB 0.9.35 is **not** affected.

**OS/Platform:** Windows only. Reproduced on Windows 10.0.26200.8875 x64
with MSVC 19.44.35211 (Visual Studio 2022 Build Tools), building the
attached reproducer directly against the unmodified 1.0.1 sources. Linux
and macOS are unaffected on both release lines.

## Description

On Windows, an environment opened with `MDB_WRITEMAP` cannot commit a write
transaction unless `MDB_NOSYNC` or `MDB_NOMETASYNC` is also set.
`mdb_txn_commit()` fails with `ERROR_INVALID_HANDLE` ("The handle is
invalid").

The chain is:

1. `mdb_env_open()` does not open `me_mfd` when `MDB_WRITEMAP` is set:

       if (!(flags & (MDB_RDONLY|MDB_WRITEMAP))) {
               rc = mdb_fopen(env, &fname, MDB_O_META, mode, &env->me_mfd);

   so `me_mfd` keeps the `INVALID_HANDLE_VALUE` assigned in
   `mdb_env_create()`. 0.9.35 has the identical guard, so this alone is not
   the difference between the release lines.

2. `mdb_env_write_meta()`'s `MDB_WRITEMAP` fast path — which updates the
   meta page directly in the map and `goto done` without ever touching
   `me_mfd` — is wrapped in `#ifndef _WIN32`:

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

## Steps to reproduce

On Windows, with LMDB 1.0.1:

1. `mdb_env_create()`, `mdb_env_set_mapsize()`.
2. `mdb_env_open(env, dir, MDB_WRITEMAP, 0664)` — no other flags.
3. `mdb_txn_begin()`, `mdb_dbi_open()`, one or more `mdb_put()`.
4. `mdb_txn_commit()`.

The attached reproducer does this as case A, alongside three controls.

## Actual results

Against pristine 1.0.1:

    LMDB 1.0.1: (Aug 6, 2026)
    100 records per case

    case env flags                          mdb_txn_commit                 mdb_env_sync
    A    MDB_WRITEMAP                       The handle is invalid.         -
    B    MDB_WRITEMAP|MDB_NOSYNC +sync      ok                             ok
    C    MDB_WRITEMAP|MDB_NOMETASYNC        ok                             -
    D    (no MDB_WRITEMAP)                  ok                             -

    BUG REPRODUCED.

Exit status 1. The same program built against pristine 0.9.35, run on the
same machine moments later:

    LMDB 0.9.35: (Jan 27, 2026)
    100 records per case

    case env flags                          mdb_txn_commit                 mdb_env_sync
    A    MDB_WRITEMAP                       ok                             -
    B    MDB_WRITEMAP|MDB_NOSYNC +sync      ok                             ok
    C    MDB_WRITEMAP|MDB_NOMETASYNC        ok                             -
    D    (no MDB_WRITEMAP)                  ok                             -

    OK: all cases succeeded.

Exit status 0.

## Expected results

All four cases commit, as they do on 0.9.35 on the same machine and as they
do for 1.0.1 on Linux and macOS. `MDB_WRITEMAP` at default sync settings is
a supported configuration.

## Confirmation

The cases are chosen so that the mechanism can be isolated from outside the
library, with no instrumentation of `mdb.c`. The two flags that avoid the
failure do so for *different* reasons:

- `MDB_NOSYNC` makes `mdb_env_sync0()` skip its body **and** changes the
  `mfd` selection in step 3.
- `MDB_NOMETASYNC` changes **only** the `mfd` selection. It does not stop
  `mdb_env_sync0()` from running.

Case C therefore discriminates between the two candidate explanations, and
it passes: with syncing still active and only the meta-write handle moved to
`me_fd`, the commit succeeds. Case B corroborates from the other side — an
explicit `mdb_env_sync(env, 1)` on a working environment returns success, so
the sync path is not at fault.

What is directly observed: cases A–D above, on both release lines, built
from unmodified upstream sources.

What is inferred: that the failing call is specifically the `WriteFile()` to
`mfd` in `mdb_env_write_meta()`. This was not watched in a debugger. It is
what steps 1–3 predict, and case C moves exactly that one selection and
fixes the failure, but a maintainer may wish to confirm it directly.

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

The `#ifndef _WIN32` comment ("We don't want to ever use
MSYNC/FlushViewOfFile in Windows") explains why the *sync* inside that
branch is unwanted on Windows, but the branch also performs the meta-page
update itself, which is what Windows then loses. If the intent was only to
avoid the flush, option 3 may be closest to it.

## Notes

- `MDB_NOSYNC` and `MDB_NOMETASYNC` both avoid the failure, which is a
  usable workaround but trades away the durability the caller asked for.
- Independently corroborated through the py-lmdb binding, which is how this
  was first noticed: the same failure appears on `windows-latest` CI runners
  in both its CPython and CFFI builds, on Python 3.11 and 3.14, and with
  py-lmdb's local patch series both applied and absent. Only the 1.0 engine
  is affected there too.
- `me_ovfd`, a Windows-only handle new in 1.0, is selected on a similar
  `MDB_NOSYNC` condition in `mdb_page_flush()`. It is opened unconditionally
  on Windows in `mdb_env_open()`, so it is not the handle at fault here, but
  it is worth checking that it is initialised — unlike `me_fd`, `me_lfd` and
  `me_mfd` it is not assigned `INVALID_HANDLE_VALUE` in `mdb_env_create()`,
  while `mdb_env_close0()` tests it against that value before closing. That
  is a separate latent issue, not this one.

## Reproducer

`misc/win-writemap-sync-repro.c` in the py-lmdb tree. Build and run, from a
tree containing the 1.0.1 sources in `lib1/`:

    cl /O2 /I lib1 misc\win-writemap-sync-repro.c lib1\mdb.c lib1\midl.c ^
       /Fe:repro.exe /link advapi32.lib
    repro

Exit status 0 = unaffected, 1 = bug reproduced, 2 = setup error. Building
against a 0.9.35 tree instead runs the same cases on that release line,
which prints "all cases succeeded".
