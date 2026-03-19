#!/usr/bin/env python
#
# Copyright 2013 The py-lmdb authors, all rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted only as authorized by the OpenLDAP
# Public License.
#
# A copy of this license is available in the file LICENSE in the
# top-level directory of the distribution or, alternatively, at
# <http://www.OpenLDAP.org/license.html>.
#
# OpenLDAP is a registered trademark of the OpenLDAP Foundation.
#
# Individual files and/or contributed packages may be copyright by
# other parties and/or subject to additional restrictions.
#
# This work also contains materials derived from public sources.
#
# Additional information about OpenLDAP can be obtained at
# <http://www.openldap.org/>.
#

"""
Create or read LMDB databases and dump contents for equivalence comparison.

Used by CI to verify that patched and pure (unpatched) LMDB builds produce
identical results for the same operations, and that databases written by one
build are readable by the other.

Usage:
    python tests/equiv_dump.py create <dir>   # Create DBs and dump to stdout
    python tests/equiv_dump.py read <dir>      # Read existing DBs and dump to stdout
"""

from __future__ import print_function

import hashlib
import os
import sys

import lmdb


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def val_repr(v):
    """Compact deterministic representation of a value."""
    if len(v) <= 64:
        return 'hex=' + v.hex()
    return 'len=%d sha256=%s' % (len(v), hashlib.sha256(v).hexdigest())


def make_val(size):
    """Deterministic value of exactly *size* bytes."""
    pattern = ('V%08d' % size).encode()
    reps = (size // len(pattern)) + 1
    return (pattern * reps)[:size]


def make_key(prefix, n):
    """Deterministic key like b'prefix-000042'."""
    return ('%s-%06d' % (prefix, n)).encode()


def emit(line):
    print(line)


def dump_section(env, db, label, dupsort=False):
    """Dump a DB's contents to stdout."""
    emit('=== SCENARIO: %s ===' % label)
    with env.begin(db=db) as txn:
        stat = txn.stat(db)
        emit('STAT: entries=%d depth=%d' % (stat['entries'], stat['depth']))
        cur = txn.cursor()
        if dupsort:
            if cur.first():
                while True:
                    key = cur.key()
                    for v in cur.iternext_dup(values=True):
                        emit('DUP: %s -> %s' % (key.hex(), val_repr(v)))
                    if not cur.next_nodup():
                        break
        else:
            for key, val in cur.iternext():
                emit('KV: %s -> %s' % (key.hex(), val_repr(val)))
    emit('---')
    emit('')


# ---------------------------------------------------------------------------
# Scenarios — each returns (db_handle, label, dupsort_flag)
# ---------------------------------------------------------------------------

def scenario_overflow_values(env, psize):
    """Overflow values at various page-boundary sizes, plus overwrites."""
    db = env.open_db(b'overflow_values')
    sizes = [psize - 1, psize, psize + 1, 2 * psize + 1, 10 * psize]

    with env.begin(write=True, db=db) as txn:
        for i, sz in enumerate(sizes):
            txn.put(make_key('bnd', i), make_val(sz))

        # Overwrite overflow with different overflow
        txn.put(make_key('ovr', 0), make_val(2 * psize + 1))
    with env.begin(write=True, db=db) as txn:
        txn.put(make_key('ovr', 0), make_val(3 * psize + 1))

        # Overwrite overflow with small
        txn.put(make_key('ovr', 1), make_val(2 * psize + 1))
    with env.begin(write=True, db=db) as txn:
        txn.put(make_key('ovr', 1), make_val(32))

        # Overwrite small with overflow
        txn.put(make_key('ovr', 2), make_val(32))
    with env.begin(write=True, db=db) as txn:
        txn.put(make_key('ovr', 2), make_val(2 * psize + 1))

        # Delete and re-add overflow
        txn.put(make_key('del', 0), make_val(psize * 2 + 1))
    with env.begin(write=True, db=db) as txn:
        txn.delete(make_key('del', 0))
        txn.put(make_key('del', 0), make_val(psize * 3 + 1))

    return db, 'overflow_values', False


def scenario_dupsort_subpage(env, psize):
    """Dupsort DB with few dups (sub-page), multiple keys."""
    db = env.open_db(b'dupsort_subpage', dupsort=True)

    with env.begin(write=True, db=db) as txn:
        # Single key with 5 dups
        for i in range(5):
            txn.put(b'key', make_key('val', i), dupdata=True)
        # 10 keys each with 5 dups
        for ki in range(10):
            key = make_key('mk', ki)
            for di in range(5):
                txn.put(key, make_key('dv', di), dupdata=True)

    return db, 'dupsort_subpage', True


def scenario_dupsort_subdb(env, psize):
    """Dupsort DB with enough dups to promote sub-page to sub-DB."""
    db = env.open_db(b'dupsort_subdb', dupsort=True)
    n = max(psize // 10, 50)

    with env.begin(write=True, db=db) as txn:
        # One key promoted to sub-DB
        for i in range(n):
            txn.put(b'big', str(i).zfill(8).encode(), dupdata=True)
        # 3 keys each promoted
        for ki in range(3):
            key = make_key('pk', ki)
            for i in range(n):
                txn.put(key, str(i).zfill(8).encode(), dupdata=True)

    return db, 'dupsort_subdb', True


def scenario_dupfixed_leaf2(env, psize):
    """Dupfixed DB with enough fixed-size dups for LEAF2 pages."""
    db = env.open_db(b'dupfixed_leaf2', dupsort=True, dupfixed=True)
    n = (psize // 8) * 4

    with env.begin(write=True, db=db) as txn:
        # One key with many fixed dups
        for i in range(n):
            txn.put(b'leaf2', str(i).zfill(8).encode(), dupdata=True)
        # 5 keys each with 50 dups
        for ki in range(5):
            key = make_key('fx', ki)
            for i in range(50):
                txn.put(key, str(i).zfill(8).encode(), dupdata=True)

    return db, 'dupfixed_leaf2', True


def scenario_deep_btree(env, psize):
    """100K entries to force a deep B-tree (depth >= 3)."""
    db = env.open_db(b'deep_btree')

    with env.begin(write=True, db=db) as txn:
        for i in range(100000):
            txn.put(make_key('br', i), make_val(64))

    return db, 'deep_btree', False


def scenario_large_keys(env, psize):
    """Max-size keys with various value sizes."""
    db = env.open_db(b'large_keys')
    maxk = env.max_key_size()

    with env.begin(write=True, db=db) as txn:
        for i in range(50):
            key = str(i).zfill(6).encode() + b'K' * (maxk - 6)
            txn.put(key, make_val(64))

    return db, 'large_keys', False


def scenario_nested_txn(env, psize):
    """Nested transactions with different data sizes."""
    db = env.open_db(b'nested_txn')

    # Three-level nesting, all commit
    with env.begin(write=True, db=db) as txn:
        txn.put(b'L0', make_val(32))
        child = env.begin(write=True, parent=txn, db=db)
        child.put(b'L1', make_val(psize + 1))
        grandchild = env.begin(write=True, parent=child, db=db)
        grandchild.put(b'L2', make_val(psize * 3 + 1))
        grandchild.commit()
        child.commit()

    # Middle-abort: only 'A0' survives
    with env.begin(write=True, db=db) as txn:
        txn.put(b'A0', make_val(32))
        child = env.begin(write=True, parent=txn, db=db)
        child.put(b'A1', make_val(psize + 1))
        grandchild = env.begin(write=True, parent=child, db=db)
        grandchild.put(b'A2', make_val(psize * 3 + 1))
        grandchild.commit()
        child.abort()

    return db, 'nested_txn', False


def scenario_mixed_overwrites(env, psize):
    """Cycles of small/overflow overwrites on the same key."""
    db = env.open_db(b'mixed_overwrites')

    key = b'cycle'
    sizes = [32, psize * 2 + 1, 16, psize * 3 + 1, 64, psize + 1,
             8, psize * 4 + 1, 48, psize * 2 + 7]
    for sz in sizes:
        with env.begin(write=True, db=db) as txn:
            txn.put(key, make_val(sz))

    return db, 'mixed_overwrites', False


ALL_SCENARIOS = [
    scenario_overflow_values,
    scenario_dupsort_subpage,
    scenario_dupsort_subdb,
    scenario_dupfixed_leaf2,
    scenario_deep_btree,
    scenario_large_keys,
    scenario_nested_txn,
    scenario_mixed_overwrites,
]

# Metadata needed to re-open DBs in read mode (name, dupsort, dupfixed)
DB_META = [
    (b'overflow_values', False, False),
    (b'dupsort_subpage', True, False),
    (b'dupsort_subdb', True, False),
    (b'dupfixed_leaf2', True, True),
    (b'deep_btree', False, False),
    (b'large_keys', False, False),
    (b'nested_txn', False, False),
    (b'mixed_overwrites', False, False),
]


# ---------------------------------------------------------------------------
# Main entry points
# ---------------------------------------------------------------------------

def create_all(path):
    """Create databases and dump contents to stdout."""
    env = lmdb.open(path, max_dbs=10, map_size=1048576 * 1024)
    psize = env.stat()['psize']
    emit('# psize=%d' % psize)
    emit('')

    for scenario_fn in ALL_SCENARIOS:
        db, label, dupsort = scenario_fn(env, psize)
        dump_section(env, db, label, dupsort=dupsort)

    env.close()


def read_all(path):
    """Open existing databases read-only and dump contents to stdout."""
    env = lmdb.open(path, max_dbs=10, map_size=1048576 * 1024, readonly=True)
    psize = env.stat()['psize']
    emit('# psize=%d' % psize)
    emit('')

    labels = [
        'overflow_values', 'dupsort_subpage', 'dupsort_subdb',
        'dupfixed_leaf2', 'deep_btree', 'large_keys',
        'nested_txn', 'mixed_overwrites',
    ]

    for (name, dupsort, dupfixed), label in zip(DB_META, labels):
        db = env.open_db(name, dupsort=dupsort, dupfixed=dupfixed, create=False)
        dump_section(env, db, label, dupsort=dupsort)

    env.close()


def main():
    if len(sys.argv) != 3 or sys.argv[1] not in ('create', 'read'):
        print('Usage: %s create|read <dir>' % sys.argv[0], file=sys.stderr)
        sys.exit(2)

    mode = sys.argv[1]
    path = sys.argv[2]

    if mode == 'create':
        os.makedirs(path, exist_ok=True)
        create_all(path)
    else:
        read_all(path)


if __name__ == '__main__':
    main()
