#!/bin/bash
#
# Run the upstream LMDB test programs against the bundled trees, comparing
# pristine sources to the py-lmdb patch series.
#
#   misc/run-upstream-mtests.sh [workdir]
#
# The mtest programs are not part of the py-lmdb distribution -- lib/ and
# lib1/ carry only the library sources -- so this fetches them from the
# upstream tags matching what we bundle.  Requires network access to
# raw.githubusercontent.com, a compiler, and a prior `setup.py build_ext`
# (the patched trees it compares against are build/lib09-plain and
# build/lib10-plain).
#
# The comparison, not the exit status, is the point: a hardening patch that
# rejects corrupt input should be indistinguishable from pristine LMDB on
# well-formed input.  Any difference in these programs' output is a
# regression worth explaining.
#
# Two sources of false differences are removed first.  The tests seed the
# RNG from the clock, so the seed is pinned; and they print raw pointers and
# (under MDB_DEBUG) the pid, so both are normalised.
#
set -u

work="${1:-$(mktemp -d)}"
here="$(cd "$(dirname "$0")/.." && pwd)"
raw="https://raw.githubusercontent.com/LMDB/lmdb"
tests="mtest mtest2 mtest3 mtest4 mtest5"
fail=0

norm() { sed -E 's/0x[0-9a-f]+/0xPTR/g; s/^>[0-9]+:/>PID:/'; }

build_and_run() {   # <tag> <tree> <label>
    local tag="$1" tree="$2" label="$3" t bin dir rv
    for t in $tests; do
        bin="$work/$label-$t"
        if ! gcc -O2 -pthread -w -I"$tree" -o "$bin" "$work/$tag/$t.c" \
             "$tree/mdb.c" "$tree/midl.c" 2>"$work/$label-$t.build"; then
            echo "  $t: BUILD-FAIL ($label)"; fail=1; continue
        fi
        dir="$work/run-$label-$t"; rm -rf "$dir"; mkdir -p "$dir/testdb"
        ( cd "$dir" && timeout 300 "$bin" ) 2>&1 | norm >"$work/$label-$t.out"
        # PIPESTATUS[0], not $?: the latter is norm's status, not the test's.
        rv=${PIPESTATUS[0]}
        rm -rf "$dir"
        [ "$rv" -eq 0 ] || { echo "  $t: exit $rv ($label)"; fail=1; }
    done
}

for spec in "LMDB_0.9.36 lib build/lib09-plain 0.9" \
            "LMDB_1.0.1  lib1 build/lib10-plain 1.0"; do
    set -- $spec
    tag="$1"; pristine="$here/$2"; patched="$here/$3"; name="$4"

    echo "=== LMDB $name ($tag) ==="
    if [ ! -d "$patched" ]; then
        echo "  $patched missing; run setup.py build_ext first" >&2
        exit 2
    fi

    mkdir -p "$work/$tag"
    for t in $tests; do
        curl -sSf -o "$work/$tag/$t.c" "$raw/$tag/libraries/liblmdb/$t.c" \
            || { echo "  fetch failed: $t.c" >&2; exit 2; }
        # Pin the seed so both builds see identical data.
        sed -i 's/srand(time(NULL));/srand(20260818);/' "$work/$tag/$t.c"
    done

    # Confirm the bundled tree really is the upstream tag, or "pristine"
    # means nothing.
    for f in mdb.c midl.c lmdb.h midl.h; do
        curl -sSf -o "$work/$tag/up-$f" "$raw/$tag/libraries/liblmdb/$f" || exit 2
        if ! cmp -s "$work/$tag/up-$f" "$pristine/$f"; then
            echo "  WARNING: $2/$f differs from $tag upstream"
            fail=1
        fi
    done

    build_and_run "$tag" "$pristine" "$name-pristine"
    build_and_run "$tag" "$patched"  "$name-patched"

    for t in $tests; do
        a="$work/$name-pristine-$t.out"; b="$work/$name-patched-$t.out"
        if cmp -s "$a" "$b"; then
            echo "  $t: identical ($(wc -l < "$a") lines)"
        else
            echo "  $t: DIFFERS"
            diff "$a" "$b" | head -20
            fail=1
        fi
    done
done

echo
if [ "$fail" -eq 0 ]; then
    echo "OK: patched trees behave identically to pristine upstream"
else
    echo "FAILURES above"
fi
exit $fail
