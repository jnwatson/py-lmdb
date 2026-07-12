/*
 * Reproducer for: on Windows, mdb_page_flush() writes each overflow page with
 * a single WriteFile() whose nNumberOfBytesToWrite is a 32-bit DWORD.  A value
 * whose overflow extent (psize * mp_pages) reaches 2**32 -- a value a few KB
 * below MAXDATASIZE -- has its length truncated; an extent of exactly 2**32
 * truncates to 0, so WriteFile() writes nothing and the value is silently
 * lost.  The commit still "succeeds" (a 0-byte WriteFile returns TRUE), so the
 * bug shows up as a value that reads back as zeroes instead of its contents.
 *
 *   Unpatched:  "BUG REPRODUCED: value truncated/lost ..."   (exit 1)
 *   Fixed:      "OK: committed and read back ... intact"     (exit 0)
 *
 * The bug is in the _WIN32 flush path, so it reproduces on Windows.  (On POSIX
 * the same value exercises mdb_page_flush()'s short-write loop instead; that
 * is PR #474.)  Needs a 64-bit build and ~4 GiB of RAM + disk.
 *
 * Build (MSVC, from a "x64 Native Tools" prompt, at the repo root):
 *   cl /O2 /I lib misc\win-flush-large-write-repro.c build\lib\mdb.c ^
 *      build\lib\midl.c /Fe:repro.exe /link advapi32.lib
 * Build (gcc / MinGW or POSIX):
 *   gcc -O2 -I lib -o repro misc/win-flush-large-write-repro.c \
 *       lib/mdb.c lib/midl.c -lpthread
 * Run:
 *   repro [scratch-dir]
 *
 * Exit status: 0 = fixed/OK, 1 = bug reproduced, 2 = setup error.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#  include <direct.h>
#  define MKDIR(d) _mkdir(d)
#else
#  include <sys/stat.h>
#  define MKDIR(d) mkdir((d), 0755)
#endif
#include "lmdb.h"

#define CHK(expr) do { int rc_ = (expr); if (rc_) { \
    fprintf(stderr, "%s:%d: %s: %s\n", __FILE__, __LINE__, #expr, \
            mdb_strerror(rc_)); return 2; } } while (0)

int main(int argc, char **argv)
{
    const char *dir = argc > 1 ? argv[1] : "lw-repro-db";
    /* Overflow extent psize*mp_pages == 2**32 on a 4 KiB-page system:
     * WriteFile's DWORD length truncates 2**32 -> 0.  The value data stays
     * below the 4 GiB file offset so the read-back itself is unaffected. */
    const size_t VALSIZE = (size_t)0xFFFFF000UL;
    MDB_env *env;
    MDB_txn *txn;
    MDB_dbi dbi;
    MDB_val key, val, got;
    unsigned char *p;
    char *buf;

    if (sizeof(size_t) < 8) {
        fprintf(stderr, "This reproducer requires a 64-bit build.\n");
        return 2;
    }

    MKDIR(dir);

    buf = (char *)malloc(VALSIZE);
    if (!buf) { fprintf(stderr, "malloc(%zu) failed\n", VALSIZE); return 2; }
    memset(buf, 'x', VALSIZE);
    memcpy(buf, "HEAD", 4);
    memcpy(buf + VALSIZE - 4, "TAIL", 4);

    CHK(mdb_env_create(&env));
    CHK(mdb_env_set_mapsize(env, VALSIZE + (256UL << 20)));
    CHK(mdb_env_open(env, dir, 0, 0664));
    CHK(mdb_txn_begin(env, NULL, 0, &txn));
    CHK(mdb_dbi_open(txn, NULL, 0, &dbi));
    key.mv_data = (void *)"big"; key.mv_size = 3;
    val.mv_data = buf;           val.mv_size = VALSIZE;
    CHK(mdb_put(txn, dbi, &key, &val, 0));
    CHK(mdb_txn_commit(txn));    /* the overflow-page WriteFile happens here */
    free(buf);

    CHK(mdb_txn_begin(env, NULL, MDB_RDONLY, &txn));
    CHK(mdb_get(txn, dbi, &key, &got));
    p = (unsigned char *)got.mv_data;
    if (got.mv_size != VALSIZE ||
        memcmp(p, "HEAD", 4) != 0 ||
        memcmp(p + VALSIZE - 4, "TAIL", 4) != 0) {
        fprintf(stderr,
            "BUG REPRODUCED: value truncated/lost: size=%zu "
            "head=%02x%02x%02x%02x tail=%02x%02x%02x%02x\n",
            got.mv_size, p[0], p[1], p[2], p[3],
            p[VALSIZE - 4], p[VALSIZE - 3], p[VALSIZE - 2], p[VALSIZE - 1]);
        mdb_txn_abort(txn); mdb_env_close(env);
        return 1;
    }
    mdb_txn_abort(txn);
    mdb_env_close(env);

    printf("OK: committed and read back %zu-byte value intact\n", VALSIZE);
    return 0;
}
