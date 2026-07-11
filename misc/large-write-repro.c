/*
 * Reproducer for: mdb_txn_commit() fails with EIO when a single value is
 * larger than the platform per-call write limit (~2 GiB on Linux).
 *
 * A value that does not fit in a normal page is stored in one contiguous
 * overflow page and flushed by mdb_page_flush() in a single pwrite()/writev()
 * of mp_pages * psize bytes.  Linux write()/pwrite() transfers at most
 * 0x7ffff000 bytes and returns a *short count*; mdb_page_flush() compares that
 * count with the requested size and treats the mismatch as fatal
 * ("short write, filesystem full?"), returning EIO.  So committing any value
 * larger than ~2 GiB fails, even though the value is well within MAXDATASIZE
 * (4 GiB - 1) and there is plenty of free space.
 *
 * (On Windows the analogous single WriteFile() in the compacting-copy writer
 * mdb_env_copythr() truncates its size_t length to a 32-bit DWORD.)
 *
 *   Unpatched:  "BUG REPRODUCED: mdb_txn_commit failed: Input/output error"
 *   Fixed:      "OK: committed and read back 2415919104-byte value intact"
 *
 * Build:  gcc -O2 -o repro large-write-repro.c mdb.c midl.c -lpthread
 * Run:    ./repro [scratch-dir]      (64-bit only; needs ~2.5 GiB RAM + disk)
 *
 * Exit status: 0 = fixed/OK, 1 = bug reproduced, 2 = setup error.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include "lmdb.h"

#define CHK(expr) do { int rc_ = (expr); if (rc_) { \
    fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, #expr, \
            mdb_strerror(rc_)); return 2; } } while (0)

int main(int argc, char **argv)
{
    const char *dir = argc > 1 ? argv[1] : "./lw-repro-db";
    /* 2.25 GiB: one overflow page whose single write exceeds the ~2 GiB
     * per-call limit (0x7ffff000). */
    const size_t VALSIZE = (size_t)0x90000000UL;
    MDB_env *env;
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, val, got;
    char *buf;
    int rc;

    if (sizeof(size_t) < 8) {
        fprintf(stderr, "This reproducer requires a 64-bit build.\n");
        return 2;
    }

    mkdir(dir, 0755);

    buf = (char *)malloc(VALSIZE);
    if (!buf) { fprintf(stderr, "malloc(%zu) failed\n", VALSIZE); return 2; }
    memset(buf, 'x', VALSIZE);
    memcpy(buf, "HEAD", 4);
    memcpy(buf + VALSIZE - 4, "TAIL", 4);

    CHK(mdb_env_create(&env));
    CHK(mdb_env_set_mapsize(env, VALSIZE + (512UL << 20)));
    CHK(mdb_env_open(env, dir, 0, 0664));

    CHK(mdb_txn_begin(env, NULL, 0, &txn));
    CHK(mdb_dbi_open(txn, NULL, 0, &dbi));
    key.mv_data = (void *)"big"; key.mv_size = 3;
    val.mv_data = buf;          val.mv_size = VALSIZE;
    CHK(mdb_put(txn, dbi, &key, &val, 0));

    rc = mdb_txn_commit(txn);
    if (rc) {
        fprintf(stderr, "BUG REPRODUCED: mdb_txn_commit failed: %s (rc=%d)\n",
                mdb_strerror(rc), rc);
        mdb_env_close(env);
        free(buf);
        return 1;
    }

    /* Verify the value round-trips without truncation. */
    CHK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
    CHK(mdb_get(txn, dbi, &key, &got));
    if (got.mv_size != VALSIZE ||
        memcmp(got.mv_data, "HEAD", 4) != 0 ||
        memcmp((char *)got.mv_data + VALSIZE - 4, "TAIL", 4) != 0) {
        fprintf(stderr, "BUG: value corrupt/truncated: got %zu of %zu bytes\n",
                got.mv_size, VALSIZE);
        mdb_txn_abort(txn); mdb_env_close(env); free(buf);
        return 1;
    }
    mdb_txn_abort(txn);
    mdb_env_close(env);
    free(buf);

    printf("OK: committed and read back %zu-byte value intact\n", VALSIZE);
    return 0;
}
