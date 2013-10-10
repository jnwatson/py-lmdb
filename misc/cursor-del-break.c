// http://www.openldap.org/its/index.cgi/Software%20Bugs?id=7722
// gcc -g -I src/py-lmdb/lib -o cursor-del-break cursor-del-break.c
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>

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

#define RECS 2048
#define DB_PATH "/ram/tdb"

MDB_dbi dbi;
MDB_txn *txn;
MDB_env *env;
MDB_cursor *c1;

char recpattern[256];
MDB_val keyv;
MDB_val valv;

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
    check(mdb_env_create(&env));
    check(mdb_env_set_mapsize(env, 1048576UL*1024UL*3UL));
    check(mdb_env_set_maxreaders(env, 126));
    check(mdb_env_set_maxdbs(env, 1));
    if(! access(DB_PATH, X_OK)) {
        system("rm -rf " DB_PATH);
    }
    check(mkdir(DB_PATH, 0777));
    check(mdb_env_open(env, DB_PATH, MDB_MAPASYNC|MDB_NOSYNC|MDB_NOMETASYNC, 0644));
    new_txn();
    check(mdb_dbi_open(txn, NULL, 0, &dbi));

    // make pattern
    int i;
    for(i = 0; i < sizeof recpattern; i++) {
        recpattern[i] = i % 256;
    }

    for(i = 0; i < RECS; i++) {
        char keybuf[40];
        keyv.mv_size = sprintf(keybuf, "%08x", i);
        keyv.mv_data = keybuf;
        valv.mv_size = sizeof recpattern;
        valv.mv_data = recpattern;
        check(mdb_put(txn, dbi, &keyv, &valv, 0));
    }

    new_txn();

    check(mdb_cursor_open(txn, dbi, &c1));
    check(mdb_cursor_get(c1, &keyv, &valv, MDB_FIRST));
    check(mdb_del(txn, dbi, &keyv, NULL));

    for(i = 1; i < RECS; i++) {
        check(mdb_cursor_get(c1, &keyv, &valv, MDB_NEXT));
        char keybuf[40];
        int sz = sprintf(keybuf, "%08x", i);
        check((!(sz==keyv.mv_size)) || memcmp(keyv.mv_data, keybuf, sz));
        check(memcmp(valv.mv_data, recpattern, sizeof recpattern));
        printf("%d\n", i);
        check(mdb_del(txn, dbi, &keyv, NULL));
    }

    new_txn();
}
