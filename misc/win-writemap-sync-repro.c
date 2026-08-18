/*
 * Reproducer for: on Windows, an environment opened with MDB_WRITEMAP cannot
 * commit a write transaction unless MDB_NOSYNC or MDB_NOMETASYNC is also set.
 * mdb_txn_commit() returns ERROR_INVALID_HANDLE ("The handle is invalid").
 *
 * LMDB 1.0.1 only: 0.9.35 runs the same sequence on the same machine without
 * error.
 *
 * Derived cause (from reading 1.0.1; the cases below are chosen to confirm or
 * refute it from outside the library):
 *
 *   1. mdb_env_open() does not open me_mfd when MDB_WRITEMAP is set --
 *        if (!(flags & (MDB_RDONLY|MDB_WRITEMAP)))
 *              rc = mdb_fopen(env, &fname, MDB_O_META, mode, &env->me_mfd);
 *      so me_mfd keeps the INVALID_HANDLE_VALUE it was initialised with.
 *
 *   2. mdb_env_write_meta()'s MDB_WRITEMAP fast path -- which updates the
 *      meta page in the map and returns without touching me_mfd -- is
 *      wrapped in "#ifndef _WIN32".  On Windows a MDB_WRITEMAP environment
 *      therefore falls through to the file-write path.
 *
 *   3. That path selects
 *        mfd = (flags & (MDB_NOSYNC|MDB_NOMETASYNC)) ? env->me_fd : env->me_mfd;
 *      so without either flag it writes the meta page to me_mfd, which is
 *      INVALID_HANDLE_VALUE.
 *
 * 0.9.35 has no "#ifndef _WIN32" around its equivalent fast path, so it takes
 * the map route and never reaches the me_mfd selection.  That is the
 * difference between the two release lines.
 *
 * Cases:
 *
 *   A  MDB_WRITEMAP                    commit
 *   B  MDB_WRITEMAP|MDB_NOSYNC         commit, then an explicit mdb_env_sync()
 *   C  MDB_WRITEMAP|MDB_NOMETASYNC     commit
 *   D  (no MDB_WRITEMAP)               commit                        [control]
 *
 * Case C is the decisive one.  MDB_NOMETASYNC changes only the mfd selection
 * in step 3; unlike MDB_NOSYNC it does not stop mdb_env_sync0() from running
 * its body.  So:
 *
 *   A fails, C passes  -> the meta-page write handle is the fault, as derived.
 *   A fails, C fails   -> the derivation is wrong; case B's explicit
 *                         mdb_env_sync() then says whether the sync path is
 *                         implicated instead.
 *
 *   Affected:    "BUG REPRODUCED" plus a per-case table   (exit 1)
 *   Unaffected:  "OK: all cases succeeded"                (exit 0)
 *
 * Build (MSVC, from a "x64 Native Tools" prompt, at the repo root):
 *   cl /O2 /I lib1 misc\win-writemap-sync-repro.c lib1\mdb.c lib1\midl.c ^
 *      /Fe:repro.exe /link advapi32.lib
 * Build (gcc / MinGW or POSIX):
 *   gcc -O2 -I lib1 -o repro misc/win-writemap-sync-repro.c \
 *       lib1/mdb.c lib1/midl.c -lpthread
 * Run:
 *   repro [scratch-dir]
 *
 * Building against lib/ instead of lib1/ runs the same cases on 0.9.35, which
 * is the contrast: it is expected to print "all cases succeeded" on the same
 * machine.
 *
 * Exit status: 0 = unaffected, 1 = bug reproduced, 2 = setup error.
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

#define NRECS 100

struct result {
    const char *name;
    const char *flags;
    int rc_commit;      /* from mdb_txn_commit()                       */
    int rc_sync;        /* from mdb_env_sync(), case B only; -1 = n/a  */
};

/* mdb_strerror() returns FormatMessage() text on Windows, which ends in
 * "\r\n".  Trim it so the table below stays on one line per case. */
static const char *errstr(int rc)
{
    static char bufs[4][256];
    static int turn;
    char *b = bufs[turn++ & 3];
    size_t len;

    if (!rc)
        return "ok";
    snprintf(b, sizeof(bufs[0]), "%s", mdb_strerror(rc));
    len = strlen(b);
    while (len && (b[len-1] == '\n' || b[len-1] == '\r' || b[len-1] == ' '))
        b[--len] = '\0';
    return b;
}

/* Run one case.  Returns 2 on setup trouble, 0 otherwise; the return codes
 * under test are reported through *r rather than as failures here, so one
 * failing case does not stop the others from running. */
static int run_case(const char *dir, unsigned int flags, int do_sync,
                    struct result *r)
{
    MDB_env *env = NULL;
    MDB_txn *txn = NULL;
    MDB_dbi dbi;
    MDB_val key, val;
    char kbuf[32];
    int i, rc;

    r->rc_commit = 0;
    r->rc_sync = -1;

    MKDIR(dir);
    if ((rc = mdb_env_create(&env)) != 0) {
        fprintf(stderr, "mdb_env_create: %s\n", mdb_strerror(rc));
        return 2;
    }
    if ((rc = mdb_env_set_mapsize(env, 10UL << 20)) != 0) {
        fprintf(stderr, "mdb_env_set_mapsize: %s\n", mdb_strerror(rc));
        return 2;
    }
    if ((rc = mdb_env_open(env, dir, flags, 0664)) != 0) {
        fprintf(stderr, "mdb_env_open(%s): %s\n", r->flags, mdb_strerror(rc));
        mdb_env_close(env);
        return 2;
    }
    if ((rc = mdb_txn_begin(env, NULL, 0, &txn)) != 0) {
        fprintf(stderr, "mdb_txn_begin: %s\n", mdb_strerror(rc));
        mdb_env_close(env);
        return 2;
    }
    if ((rc = mdb_dbi_open(txn, NULL, 0, &dbi)) != 0) {
        fprintf(stderr, "mdb_dbi_open: %s\n", mdb_strerror(rc));
        mdb_txn_abort(txn);
        mdb_env_close(env);
        return 2;
    }

    for (i = 0; i < NRECS; i++) {
        sprintf(kbuf, "k%05d", i);
        key.mv_data = kbuf;
        key.mv_size = strlen(kbuf);
        val.mv_data = (void *)"0123456789012345678901234567890123456789";
        val.mv_size = 40;
        if ((rc = mdb_put(txn, dbi, &key, &val, 0)) != 0) {
            fprintf(stderr, "mdb_put: %s\n", mdb_strerror(rc));
            mdb_txn_abort(txn);
            mdb_env_close(env);
            return 2;
        }
    }

    /* The call under test. */
    r->rc_commit = mdb_txn_commit(txn);

    /* Probe mdb_env_sync0() directly, on an environment whose commit worked. */
    if (!r->rc_commit && do_sync)
        r->rc_sync = mdb_env_sync(env, 1);

    mdb_env_close(env);
    return 0;
}

int main(int argc, char **argv)
{
    const char *base = argc > 1 ? argv[1] : "wmsync-repro-db";
    struct result results[4];
    char dir[512];
    int i, bad = 0;

    static const struct {
        const char *name;
        const char *flags;
        unsigned int mask;
        int do_sync;
    } cases[] = {
        { "A", "MDB_WRITEMAP",                   MDB_WRITEMAP,                0 },
        { "B", "MDB_WRITEMAP|MDB_NOSYNC +sync",  MDB_WRITEMAP|MDB_NOSYNC,     1 },
        { "C", "MDB_WRITEMAP|MDB_NOMETASYNC",    MDB_WRITEMAP|MDB_NOMETASYNC, 0 },
        { "D", "(no MDB_WRITEMAP)",              0,                           0 }
    };

    MKDIR(base);
    printf("%s\n", mdb_version(NULL, NULL, NULL));
    printf("%d records per case\n\n", NRECS);

    for (i = 0; i < 4; i++) {
        sprintf(dir, "%s/case-%s", base, cases[i].name);
        results[i].name = cases[i].name;
        results[i].flags = cases[i].flags;
        if (run_case(dir, cases[i].mask, cases[i].do_sync, &results[i]))
            return 2;
    }

    printf("%-4s %-34s %-30s %s\n",
           "case", "env flags", "mdb_txn_commit", "mdb_env_sync");
    for (i = 0; i < 4; i++) {
        printf("%-4s %-34s %-30s %s\n",
               results[i].name, results[i].flags,
               errstr(results[i].rc_commit),
               results[i].rc_sync < 0 ? "-" : errstr(results[i].rc_sync));
        if (results[i].rc_commit || results[i].rc_sync > 0)
            bad = 1;
    }
    putchar('\n');

    if (!bad) {
        printf("OK: all cases succeeded.\n");
        return 0;
    }

    printf("BUG REPRODUCED.\n");
    if (results[0].rc_commit && !results[2].rc_commit) {
        printf("  A fails and C passes.  MDB_NOMETASYNC changes only which\n"
               "  handle mdb_env_write_meta() writes the meta page to, and\n"
               "  does not disable mdb_env_sync0(), so the fault is the\n"
               "  me_mfd handle -- which mdb_env_open() does not open when\n"
               "  MDB_WRITEMAP is set.\n");
    } else if (results[0].rc_commit && results[2].rc_commit) {
        printf("  A and C both fail, so the meta-write handle does not\n"
               "  explain it.  mdb_env_sync() in case B returned: %s\n",
               results[1].rc_sync < 0 ? "(not reached)"
                                      : errstr(results[1].rc_sync));
    }
    return 1;
}
