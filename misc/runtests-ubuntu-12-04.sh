#!/bin/bash -ex

quiet() {
    "$@" > /tmp/$$ || { cat /tmp/$$; return 1; }
}

clean() {
    git clean -qdfx
    find /usr/local/lib -name '*lmdb*' | xargs rm -rf
    find /usr/lib -name '*lmdb*' | xargs rm -rf
}

cat > /tmp/gdb.$$ <<-EOF
    set confirm off
    define hook-stop
        if $_isvoid ($_exitcode)
            echo Abnormal stop.\n
            backtrace
            quit 1
        else
            echo Normal exit.\n
            quit $_exitcode
        end
    end
    run
EOF
trap "rm /tmp/gdb.$$" EXIT

with_gdb() {
    gdb -x /tmp/gdb.$$ --args "$@"
}

native() {
    clean
    quiet $1 setup.py develop
    quiet $1 -c 'import lmdb.cpython'
    with_gdb $2 tests || fail=1
}

cffi() {
    clean
    LMDB_FORCE_CFFI=1 quiet $1 setup.py install
    LMDB_FORCE_CFFI=1 quiet $1 -c 'import lmdb.cffi'
    with_gdb $2 tests || fail=1
}

native python2.5 py.test-2.5
native python2.6 py.test-2.6
native python2.7 py.test-2.7
native python3.3 py.test-3.3
cffi pypy "pypy -mpy.test"
cffi python2.6 py.test-2.6
cffi python2.7 py.test-2.7
cffi python3.1 py.test-3.1
cffi python3.2 py.test-3.2
cffi python3.3 py.test-3.3

[ "$fail" ] && exit 1
exit 0
