# Upstream bug report draft: SIGFPE on zero mm_psize

Draft for filing at <https://bugs.openldap.org/>, following the OpenLDAP
bug-writing guidelines. py-lmdb carries the fix locally as
`cve-2019-16228-validate-psize.patch` in both `lib/py-lmdb/` and
`lib1/py-lmdb/`.

---

**Summary:** mdb_env_open() divides by zero on crafted mm_psize

**Version:** LMDB 1.0.1 and 0.9.35 (both release lines)

**OS/Platform:** Reproduced on Linux x86_64, gcc 13.3.0, glibc 2.39. Not
platform-specific — the defect is in portable code.

## Description

`mdb_env_open2()` uses the meta page's `mm_psize` as a divisor before
validating it. A data file declaring `mm_psize == 0` therefore raises SIGFPE
(integer division by zero) during `mdb_env_open()`, terminating the process
rather than returning an error.

Both release lines are affected, at different sites. LMDB 1.0.1 faults
earlier, in the ITS#9291 root-page sanity check:

    mdb.c:5570    pgno_t maxpgno = fsize / env->me_psize;

LMDB 0.9.35 faults at the map-size calculation, which 1.0.1 also still has
(at mdb.c:5660):

    mdb.c:4552    env->me_maxpg = env->me_mapsize / env->me_psize;

`mdb_env_read_header()` checks the page's `P_META` flag, `mm_magic` and
`mm_version`, but never `mm_psize`; `mdb_env_open2()` then assigns
`env->me_psize = meta.mm_psize` unconditionally.

## Steps to reproduce

1. Create an ordinary environment and write one record.
2. Zero the 32-bit `mm_psize` field in meta page 0. It is
   `mm_dbs[FREE_DBI].md_pad`, at `PAGEHDRSZ + 24` — offset 40 on 0.9
   (16-byte page header), offset 48 on 1.0 (24-byte header, `mp_txnid`
   added). Locating it by scanning for `MDB_MAGIC` (0xBEEFC0DE) works on
   both.
3. Reopen the environment with `mdb_env_open()`.

A self-contained C reproducer is attached below.

## Actual results

    $ ./repro_psize
    meta magic at 24, zeroing mm_psize at 48
    reopening...
    Floating point exception (core dumped)

Exit status 136 (SIGFPE). Identical on 0.9.35, with the magic at offset 16.

## Expected results

`mdb_env_open()` returns an error — `MDB_INVALID` or `MDB_CORRUPTED` — as it
does for a bad magic or version.

## Suggested fix

Validate `mm_psize` where the header is accepted, before any use as a
divisor. py-lmdb rejects a `mm_psize` that is zero, not a power of two, or
larger than `MAX_PAGESIZE`, in `mdb_env_open2()`:

```c
	} else {
		/* Validate page size from the file.  A zero or non-power-of-2
		 * value causes divide-by-zero or other undefined behavior. */
		if (!meta.mm_psize || (meta.mm_psize & (meta.mm_psize - 1)) ||
			meta.mm_psize > MAX_PAGESIZE) {
			return MDB_INVALID;
		}
		env->me_psize = meta.mm_psize;
	}
```

Rejecting non-power-of-two values matters beyond the divide: `mm_psize` also
scales page addresses (`env->me_map + env->me_psize * pgno`).

## Notes

Reported as part of maintaining py-lmdb, which bundles LMDB and reads
untrusted `.mdb` files supplied by third parties. This is one of a set of
meta-page validation patches py-lmdb carries; the others are tracked in the
project's `lib/py-lmdb/` and `lib1/py-lmdb/` directories and can be submitted
separately if useful.

---

## Reproducer

```c
/* Build against an unmodified LMDB tree:
 *   gcc -o repro_psize repro_psize.c mdb.c midl.c -lpthread -I.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "lmdb.h"

int main(void)
{
    const char *dir = "/tmp/lmdb_repro_psize";
    char path[256];
    MDB_env *env;
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val k, v;
    unsigned char buf[128];
    FILE *fp;
    int rc, i;

    snprintf(path, sizeof path, "rm -rf %s && mkdir -p %s", dir, dir);
    if (system(path)) return 1;

    if ((rc = mdb_env_create(&env))) goto err;
    if ((rc = mdb_env_open(env, dir, 0, 0644))) goto err;
    if ((rc = mdb_txn_begin(env, NULL, 0, &txn))) goto err;
    if ((rc = mdb_dbi_open(txn, NULL, 0, &dbi))) goto err;
    k.mv_data = (void *) "key"; k.mv_size = 3;
    v.mv_data = (void *) "val"; v.mv_size = 3;
    if ((rc = mdb_put(txn, dbi, &k, &v, 0))) goto err;
    if ((rc = mdb_txn_commit(txn))) goto err;
    mdb_env_close(env);

    /* Zero mm_psize (= mm_dbs[FREE_DBI].md_pad), located by scanning for
     * MDB_MAGIC so the offset works on both 0.9 and 1.0 layouts. */
    snprintf(path, sizeof path, "%s/data.mdb", dir);
    if (!(fp = fopen(path, "r+b"))) return 1;
    if (fread(buf, 1, sizeof buf, fp) != sizeof buf) return 1;
    for (i = 0; i + 4 <= (int) sizeof buf; i += 4) {
        unsigned int word;
        memcpy(&word, buf + i, 4);
        if (word == 0xBEEFC0DE) {
            unsigned int zero = 0;
            long off = i + 24;   /* magic,version,address,mapsize -> md_pad */
            printf("meta magic at %d, zeroing mm_psize at %ld\n", i, off);
            fseek(fp, off, SEEK_SET);
            fwrite(&zero, sizeof zero, 1, fp);
            break;
        }
    }
    fclose(fp);

    printf("reopening...\n");
    fflush(stdout);
    if ((rc = mdb_env_create(&env))) goto err;
    rc = mdb_env_open(env, dir, 0, 0644);
    printf("mdb_env_open returned %d (%s)\n", rc, mdb_strerror(rc));
    mdb_env_close(env);
    return 0;

err:
    fprintf(stderr, "setup failed: %s\n", mdb_strerror(rc));
    return 1;
}
```
