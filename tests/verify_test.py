#
# Copyright 2013-2026 The py-lmdb authors, all rights reserved.
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

"""Tests for the offline structural verifier (lmdb/verify.py).

The verifier is pure Python and works on every build (patched, pure, system,
cffi), so unlike cve_test.py these need no SKIP_PURE guard.  New environments
are created with whichever bundled engine LMDB_DEFAULT_LIB_VERSION selects, so
the CI matrix exercises both the 0.9 and 1.0 layouts by running the suite
twice.

Positive tests assert that a wide range of legitimately-built databases pass.
Negative tests forge each documented attack against a data.mdb and assert the
verifier rejects it -- the two halves together are the "passes verify <=> safe
for the engine" claim the tool rests on.
"""

import os
import struct
import unittest

import lmdb
from lmdb import verify as V

import testlib
from testlib import LmdbTest, temp_dir


MDB_MAGIC = 0xBEEFC0DE


def _build(path, fn, **kwargs):
    env = lmdb.open(path, max_dbs=16, map_size=32 * 1024 * 1024, **kwargs)
    try:
        fn(env)
        env.sync(True)
    finally:
        env.close()


def _detect_layout(path):
    """Return (hdrsz, pagebase, psize) for a data.mdb, or None if the current
    build's layout is not one the verifier supports (e.g. 32-bit)."""
    with open(os.path.join(path, 'data.mdb'), 'rb') as f:
        raw = f.read(4096)
    for off in range(0, 64, 4):
        if struct.unpack_from('<I', raw, off)[0] == MDB_MAGIC:
            hdrsz = off
            break
    else:
        return None
    if hdrsz not in (16, 24):
        return None
    pagebase = 0 if hdrsz == 16 else hdrsz
    psize = struct.unpack_from('<I', raw, hdrsz + 24)[0]
    return hdrsz, pagebase, psize


def _engine_supported():
    path = temp_dir()
    _build(path, lambda e: None)
    return _detect_layout(path) is not None


SUPPORTED = _engine_supported()


# --- byte-level navigation for forging (host little-endian, 64-bit) ---------

class _Layout:
    def __init__(self, raw):
        self.raw = raw
        for off in range(0, 64, 4):
            if struct.unpack_from('<I', raw, off)[0] == MDB_MAGIC:
                self.hdrsz = off
                break
        self.pagebase = 0 if self.hdrsz == 16 else self.hdrsz
        self.psize = struct.unpack_from('<I', raw, self.hdrsz + 24)[0]
        self.npages = len(raw) // self.psize

    def u16(self, off):
        return struct.unpack_from('<H', self.raw, off)[0]

    def u64(self, off):
        return struct.unpack_from('<Q', self.raw, off)[0]

    def set16(self, off, v):
        struct.pack_into('<H', self.raw, off, v)

    def set32(self, off, v):
        struct.pack_into('<I', self.raw, off, v)

    def set64(self, off, v):
        struct.pack_into('<Q', self.raw, off, v)

    def meta_off(self, pg):
        return pg * self.psize + self.hdrsz

    def pick_meta(self):
        def txnid(pg):
            return self.u64(self.meta_off(pg) + 24 + 2 * 48 + 8)
        return 1 if txnid(0) < txnid(1) else 0

    def main_off(self):
        return self.meta_off(self.pick_meta()) + 24 + 48

    def free_root(self):
        return self.u64(self.meta_off(self.pick_meta()) + 24 + 40)

    def main_root(self):
        return self.u64(self.main_off() + 40)

    def pg_flags(self, pg):
        return self.u16(pg * self.psize + self.hdrsz - 6)

    def node_off(self, pg, i):
        ptr = self.u16(pg * self.psize + self.hdrsz + 2 * i)
        return pg * self.psize + ptr + self.pagebase

    def find_leaf(self):
        for pg in range(2, self.npages):
            fl = self.pg_flags(pg)
            if (fl & 0x02) and not (fl & 0x40):     # P_LEAF, not P_SUBP
                return pg
        return None

    def descend_free_leaf(self):
        root = self.free_root()
        if root == (1 << 64) - 1:
            return None
        pg = root
        while True:
            fl = self.pg_flags(pg)
            if fl & 0x02:
                return pg
            o = self.node_off(pg, 0)
            pg = self.u16(o) | (self.u16(o + 2) << 16) | (self.u16(o + 4) << 32)

    def free_value(self):
        """Return (value_offset, count) of freeDB leaf node 0, or None."""
        leaf = self.descend_free_leaf()
        if leaf is None:
            return None
        o = self.node_off(leaf, 0)
        nf = self.u16(o + 4)
        ks = self.u16(o + 6)
        if nf & 0x01:                                # F_BIGDATA, skip
            return None
        doff = o + 8 + (((ks + 1) & ~1) if self.hdrsz == 24 else ks)
        return doff, self.u64(doff)


# ============================ positive tests ================================

@unittest.skipUnless(SUPPORTED, 'verifier supports 64-bit 0.9/1.0 files only')
class VerifyPositiveTest(LmdbTest):
    def _check(self, fn, **kwargs):
        path = temp_dir()
        _build(path, fn, **kwargs)
        errors = V.verify(path)
        self.assertEqual(errors, [], 'unexpected verify errors: %r' % (errors[:8],))

    def test_empty(self):
        self._check(lambda e: None)

    def test_plain(self):
        def fn(e):
            with e.begin(write=True) as t:
                for i in range(50):
                    t.put(b'key%03d' % i, b'value-%d' % i)
        self._check(fn)

    def test_many_pages(self):
        def fn(e):
            with e.begin(write=True) as t:
                for i in range(5000):
                    t.put(b'k%06d' % i, b'v' * (i % 50))
        self._check(fn)

    def test_deletions_populate_freedb(self):
        def fn(e):
            with e.begin(write=True) as t:
                for i in range(3000):
                    t.put(b'k%06d' % i, b'v' * 25)
            with e.begin(write=True) as t:
                for i in range(0, 3000, 2):
                    t.delete(b'k%06d' % i)
        self._check(fn)

    def test_overflow_values(self):
        def fn(e):
            with e.begin(write=True) as t:
                for i in range(40):
                    t.put(b'big%03d' % i, (b'x' * 9000) + bytes([i & 0xff]))
        self._check(fn)

    def test_dupsort_subpages(self):
        def fn(e):
            db = e.open_db(b'dups', dupsort=True)
            with e.begin(write=True) as t:
                for i in range(60):
                    for j in range(25):
                        t.put(b'k%03d' % i, b'dv-%05d' % j, db=db)
        self._check(fn)

    def test_dupsort_promoted_subdb(self):
        def fn(e):
            db = e.open_db(b'dups', dupsort=True)
            with e.begin(write=True) as t:
                for i in range(12):
                    for j in range(400):
                        t.put(b'k%03d' % i, b'dupvalue-%06d' % j, db=db)
        self._check(fn)

    def test_dupfixed_leaf2(self):
        def fn(e):
            db = e.open_db(b'df', dupsort=True, dupfixed=True)
            with e.begin(write=True) as t:
                for i in range(20):
                    for j in range(500):
                        t.put(b'k%03d' % i, struct.pack('<Q', j), db=db)
        self._check(fn)

    def test_integerkey(self):
        def fn(e):
            db = e.open_db(b'ints', integerkey=True)
            with e.begin(write=True) as t:
                for i in range(2000):
                    t.put(struct.pack('<Q', (i * 7) % 100000), b'v%d' % i, db=db)
        self._check(fn)

    def test_reversekey(self):
        def fn(e):
            db = e.open_db(b'rev', reverse_key=True)
            with e.begin(write=True) as t:
                for i in range(1500):
                    t.put(b'k%06d' % i, b'v', db=db)
        self._check(fn)

    def test_reversedup(self):
        def fn(e):
            db = e.open_db(b'rd', dupsort=True, reverse_key=False)
            with e.begin(write=True) as t:
                for i in range(40):
                    for j in range(30):
                        t.put(b'k%03d' % i, b'd%05d' % j, db=db)
        self._check(fn)

    def test_integerdup(self):
        def fn(e):
            db = e.open_db(b'id', dupsort=True, dupfixed=True, integerdup=True)
            with e.begin(write=True) as t:
                for i in range(30):
                    for j in range(40):
                        t.put(b'k%03d' % i, struct.pack('<Q', j * 3), db=db)
        self._check(fn)

    def test_empty_named_db(self):
        def fn(e):
            e.open_db(b'created_but_empty')
            db = e.open_db(b'has_data')
            with e.begin(write=True) as t:
                t.put(b'x', b'y', db=db)
        self._check(fn)

    def test_reverse_and_integer_key(self):
        # Both flags set: mdb_default_cmp uses the REVERSEKEY comparator, so the
        # verifier must too (regression guard for comparator priority).
        def fn(e):
            db = e.open_db(b'ri', reverse_key=True, integerkey=True)
            with e.begin(write=True) as t:
                for i in range(400):
                    t.put(struct.pack('<Q', (i * 11) % 5000), b'v', db=db)
        self._check(fn)

    def test_freedb_overflow_record(self):
        # Free thousands of pages in one transaction so that txn's freeDB value
        # is itself large enough to spill onto overflow pages (F_BIGDATA).
        def fn(e):
            with e.begin(write=True) as t:
                for i in range(1500):
                    t.put(b'k%06d' % i, b'x' * 5000)      # overflow values
            with e.begin(write=True) as t:
                for i in range(1500):
                    t.delete(b'k%06d' % i)
        self._check(fn)

    def test_many_named_dbs(self):
        def fn(e):
            dbs = [e.open_db(('n%d' % n).encode(), dupsort=(n % 2 == 0))
                   for n in range(5)]
            with e.begin(write=True) as t:
                for n, db in enumerate(dbs):
                    for i in range(150):
                        if n % 2 == 0:
                            for j in range(4):
                                t.put(b'k%04d' % i, b'v%d-%d' % (i, j), db=db)
                        else:
                            t.put(b'k%04d' % i, b'val%d' % i, db=db)
        self._check(fn)

    def test_single_file_env(self):
        path = testlib.temp_file()
        env = lmdb.open(path, subdir=False, map_size=8 * 1024 * 1024)
        try:
            with env.begin(write=True) as t:
                for i in range(200):
                    t.put(b'k%04d' % i, b'v' * 10)
            env.sync(True)
        finally:
            env.close()
        self.assertEqual(V.verify(path, subdir=False), [])


# ============================ negative tests ================================

@unittest.skipUnless(SUPPORTED, 'verifier supports 64-bit 0.9/1.0 files only')
class VerifyNegativeTest(LmdbTest):
    def _forge(self, mutate, deletions=False):
        """Build a DB (optionally with deletions to populate the freeDB),
        confirm it passes clean, then apply `mutate` to the raw bytes and
        return the verify() result on the forged file."""
        path = temp_dir()

        def fn(e):
            with e.begin(write=True) as t:
                for i in range(2000):
                    t.put(b'k%06d' % i, b'v' * 20)
            if deletions:
                with e.begin(write=True) as t:
                    for i in range(0, 2000, 3):
                        t.delete(b'k%06d' % i)
        _build(path, fn)
        self.assertEqual(V.verify(path), [], 'clean DB should pass first')

        data = os.path.join(path, 'data.mdb')
        with open(data, 'rb') as f:
            raw = bytearray(f.read())
        lay = _Layout(raw)
        applied = mutate(lay)
        if applied is False:
            self.skipTest('could not locate forge target')
        with open(data, 'wb') as f:
            f.write(lay.raw)
        return path

    def _assert_rejected(self, mutate, deletions=False, needle=None):
        path = self._forge(mutate, deletions)
        try:
            errors = V.verify(path)
        except V.VerifyError:
            return          # a fatal-to-parse forgery is a valid rejection
        self.assertTrue(errors, 'verify PASSED a forged file')
        if needle is not None:
            self.assertTrue(any(needle in e for e in errors),
                            'expected %r in %r' % (needle, errors))

    def test_bad_magic(self):
        self._assert_rejected(lambda l: l.set32(l.hdrsz, 0xDEADBEEF))

    def test_bad_version(self):
        self._assert_rejected(lambda l: l.set32(l.hdrsz + 4, 999))

    def test_nonpow2_psize(self):
        self._assert_rejected(lambda l: l.set32(l.hdrsz + 24, 4097))

    def test_main_root_is_meta(self):
        self._assert_rejected(lambda l: l.set64(l.main_off() + 40, 0),
                              needle='root page 0 out of range')

    def test_main_md_pad_huge(self):
        self._assert_rejected(lambda l: l.set32(l.main_off(), 0xFFFF),
                              needle='md_pad')

    def test_main_bad_flags(self):
        # MDB_DUPFIXED without MDB_DUPSORT.
        self._assert_rejected(lambda l: l.set16(l.main_off() + 4, 0x10),
                              needle='invalid flag combination')

    def test_reachable_page_dirty(self):
        def mut(l):
            pg = l.find_leaf()
            if pg is None:
                return False
            if l.hdrsz == 16:                       # 0.9: P_DIRTY
                fl = l.pg_flags(pg)
                l.set16(pg * l.psize + l.hdrsz - 6, fl | 0x10)
            else:                                   # 1.0: mp_txnid too new
                l.set64(pg * l.psize + 8, 1 << 62)
        self._assert_rejected(mut)

    def test_mp_upper_oob(self):
        def mut(l):
            pg = l.find_leaf()
            if pg is None:
                return False
            l.set16(pg * l.psize + l.hdrsz - 2, l.psize + 100)
        self._assert_rejected(mut, needle='bad mp_lower/mp_upper')

    def test_truncated_reachable_page(self):
        def mut(l):
            pg = l.find_leaf()
            if pg is None:
                return False
            del l.raw[pg * l.psize:]
        self._assert_rejected(mut, needle='beyond EOF')

    def test_freedb_pgno_out_of_range(self):
        def mut(l):
            fv = l.free_value()
            if not fv or fv[1] < 1:
                return False
            l.set64(fv[0] + 8, 10_000_000)
        self._assert_rejected(mut, deletions=True, needle='out-of-range page')

    def test_freedb_count_mismatch(self):
        def mut(l):
            fv = l.free_value()
            if not fv:
                return False
            l.set64(fv[0], 999999)
        self._assert_rejected(mut, deletions=True,
                              needle='does not match value size')

    def test_freedb_duplicate_pgno(self):
        def mut(l):
            fv = l.free_value()
            if not fv or fv[1] < 2:
                return False
            first = l.u64(fv[0] + 8)
            l.set64(fv[0] + 16, first)              # break strictly-descending
        self._assert_rejected(mut, deletions=True,
                              needle='not strictly descending')

    def test_freedb_names_live_page(self):
        """The Tier-3 signature: a coherent freeDB record naming a genuinely
        live page.  Only an offline global view catches this -- the page is
        both reachable and free, which no legitimate file allows."""
        def mut(l):
            fv = l.free_value()
            if not fv or fv[1] < 1:
                return False
            l.set64(fv[0] + 8, l.main_root())       # a live page
        self._assert_rejected(mut, deletions=True,
                              needle='both reachable and in the freeDB')


if __name__ == '__main__':
    unittest.main()
