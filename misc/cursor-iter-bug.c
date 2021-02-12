// Demonstrates a bug introduced in 0.9.27
// https://bugs.openldap.org/show_bug.cgi?id=9461
// gcc -g -I ../lib -o cursor-iter-bug cursor-iter-bug.c -lpthread
// In a dupsort DB, deleting a value before where a cursor is set causes the
// cursor to lose its place.  mdb_cursor_get with MDB_NEXT will improperly
// return the current value and not MDB_NOT_FOUND as is correct.

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <assert.h>

#include "lmdb.h"
#include "mdb.c"
#include "midl.c"


void check(int x)
{
    if(x) {
        fprintf(stderr, "eek %s\n", mdb_strerror(x));
        _exit(1);
    }
}

#define DB_PATH "tmp.lmdb"

MDB_txn *txn;
MDB_env *env;

void new_txn(void)
{
    if(txn) {
        fprintf(stderr, "commit\n");
        check(mdb_txn_commit(txn));
    }
    check(mdb_txn_begin(env, NULL, 0, &txn));
}

int main(void)
{

    MDB_dbi dbi;
    MDB_cursor *c1;
    MDB_val keyv;
    MDB_val valv;

    check(mdb_env_create(&env));
    check(mdb_env_set_mapsize(env, 1048576UL*1024UL*3UL));
    check(mdb_env_set_maxreaders(env, 126));
    check(mdb_env_set_maxdbs(env, 2));
    if(! access(DB_PATH, X_OK)) {
        system("rm -rf " DB_PATH);
    }
    check(mkdir(DB_PATH, 0777));
    check(mdb_env_open(env, DB_PATH, MDB_MAPASYNC|MDB_NOSYNC|MDB_NOMETASYNC, 0644));
    new_txn();
    check(mdb_dbi_open(txn, "db", MDB_DUPSORT | MDB_CREATE, &dbi));

    new_txn();
    check(mdb_cursor_open(txn, dbi, &c1));

    keyv.mv_size = 2;
    keyv.mv_data = "\x00\x01";
    valv.mv_size = 4;
    valv.mv_data = "hehe";
    check(mdb_cursor_put(c1, &keyv, &valv, 0));

    keyv.mv_size = 2;
    keyv.mv_data = "\x00\x02";
    valv.mv_size = 4;
    valv.mv_data = "haha";
    check(mdb_cursor_put(c1, &keyv, &valv, 0));

    check(mdb_cursor_get(c1, &keyv, &valv, MDB_SET_KEY));

    check(mdb_cursor_get(c1, &keyv, &valv, MDB_GET_CURRENT));
    assert(keyv.mv_size == 2);
    char * key = keyv.mv_data;
    assert(key[0] == 0 && key[1] == 2);

    keyv.mv_size = 2;
    keyv.mv_data = "\x00\x01";
    check(mdb_del(txn, dbi, &keyv, NULL));

    int rc = mdb_cursor_get(c1, &keyv, &valv, MDB_NEXT);

    // Below assertion fails in LMDB 0.9.27 and LMDB 0.9.28 (and passes in 0.9.26)

    assert(rc == MDB_NOTFOUND);
    new_txn();
}
