// http://www.openldap.org/its/index.cgi/Software%20Bugs?id=7733
// gcc -g -I ../lib -o its7733 its7733.c
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

#define DB_PATH "/ram/tdb"

MDB_dbi dbi;
MDB_txn *txn;
MDB_env *env;
MDB_cursor *c1;

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


void put(const char *k)
{
    keyv.mv_size = strlen(k);
    keyv.mv_data = k;
    valv.mv_size = 0;
    valv.mv_data = "";
    check(mdb_put(txn, dbi, &keyv, &valv, 0));
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

    put("a");
    put("b");
    put("baa");
    put("d");

    new_txn();

    check(mdb_cursor_open(txn, dbi, &c1));
    check(mdb_cursor_get(c1, &keyv, &valv, MDB_LAST));
    check(mdb_cursor_del(c1, 0));
    check(mdb_cursor_del(c1, 0));
    new_txn();
}
