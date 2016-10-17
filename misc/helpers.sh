quiet() {
    "$@" > /tmp/$$ || { cat /tmp/$$; return 1; }
}

clean() {
    git clean -qdfx
    find /usr/local/lib -name '*lmdb*' | xargs rm -rf
    find /usr/lib -name '*lmdb*' | xargs rm -rf
}

with_gdb() {
    gdb --batch -x misc/gdb.commands --args "$@"
}

native() {
    clean
    unset LMDB_FORCE_CFFI
    quiet $1 setup.py develop
    quiet $1 -c 'import lmdb.cpython'
    with_gdb $1 -m pytest tests || fail=1
}

cffi() {
    clean
    LMDB_FORCE_CFFI=1 quiet $1 setup.py install
    LMDB_FORCE_CFFI=1 quiet $1 -c 'import lmdb.cffi'
    with_gdb $1 -m pytest tests || fail=1
}
