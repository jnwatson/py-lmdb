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

    def u32(self, off):
        return struct.unpack_from('<I', self.raw, off)[0]

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

    def numkeys(self, pg):
        lower = self.u16(pg * self.psize + self.hdrsz - 4)
        return (lower - (self.hdrsz - self.pagebase)) >> 1

    def find_leaf(self):
        for pg in range(2, self.npages):
            fl = self.pg_flags(pg)
            if (fl & 0x02) and not (fl & 0x40):     # P_LEAF, not P_SUBP
                return pg
        return None

    def descend_leaf(self, root):
        """Follow child 0 from `root` down to a leaf page, or None."""
        pg = root
        for _ in range(64):
            if pg >= self.npages:
                return None
            fl = self.pg_flags(pg)
            if fl & 0x02:
                return pg
            o = self.node_off(pg, 0)
            pg = self.u16(o) | (self.u16(o + 2) << 16) | (self.u16(o + 4) << 32)
        return None

    def descend_free_leaf(self):
        root = self.free_root()
        if root == (1 << 64) - 1:
            return None
        return self.descend_leaf(root)

    def bigdata_node(self):
        """Return (node_off, data_off, op_pgno) of the first F_BIGDATA node in
        a live-looking leaf, or None."""
        for pg in range(2, self.npages):
            fl = self.pg_flags(pg)
            if not (fl & 0x02) or (fl & 0x40):
                continue
            nk = self.numkeys(pg)
            if nk < 1 or nk > self.psize // 2:
                continue
            for i in range(nk):
                o = self.node_off(pg, i)
                if o + 8 > (pg + 1) * self.psize:
                    break
                if self.u16(o + 4) & 0x01:          # F_BIGDATA
                    ks = self.u16(o + 6)
                    doff = o + 8 + (((ks + 1) & ~1) if self.hdrsz == 24 else ks)
                    return o, doff, self.u64(doff)
        return None

    def named_db_record(self):
        """Return the MDB_db data offset of the named sub-DB record stored as
        node 0 of the main tree's leftmost leaf, or None."""
        root = self.main_root()
        if root == (1 << 64) - 1:
            return None
        pg = self.descend_leaf(root)
        if pg is None or self.numkeys(pg) < 1:
            return None
        o = self.node_off(pg, 0)
        if not (self.u16(o + 4) & 0x02):            # F_SUBDATA
            return None
        ks = self.u16(o + 6)
        return o + 8 + (((ks + 1) & ~1) if self.hdrsz == 24 else ks)

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

    def test_writemap_env(self):
        # writemap mode flushes pages through the map rather than write(); the
        # at-rest file must look identical to the verifier (in particular, no
        # in-memory page flags may leak to disk).
        def fn(e):
            db = e.open_db(b'dups', dupsort=True)
            with e.begin(write=True) as t:
                for i in range(1500):
                    t.put(b'k%06d' % i, b'v' * 25)
                for i in range(50):
                    for j in range(10):
                        t.put(b'd%03d' % i, b'x%02d' % j, db=db)
            with e.begin(write=True) as t:
                for i in range(0, 1500, 2):
                    t.delete(b'k%06d' % i)
        self._check(fn, writemap=True)

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

    def test_pre_9388_dupsort_counters(self):
        # Files written by LMDB < 0.9.36 track only a DUPSORT DB's own tree in
        # md_branch/leaf_pages (no mdb_subdb_adjust aggregation), so real
        # legacy databases hold the own-tree counts.  Model one by rewriting a
        # fresh file's aggregate back to the own-tree value: it must still
        # verify clean.
        path = temp_dir()

        def fn(e):
            db = e.open_db(b'df', dupsort=True, dupfixed=True)
            with e.begin(write=True) as t:
                for i in range(20):
                    for j in range(500):
                        t.put(b'k%03d' % i, struct.pack('<Q', j), db=db)
        _build(path, fn)
        self.assertEqual(V.verify(path), [])
        data = os.path.join(path, 'data.mdb')
        with open(data, 'rb') as f:
            raw = bytearray(f.read())
        lay = _Layout(raw)
        doff = lay.named_db_record()
        if doff is None:
            self.skipTest('could not locate the named-DB record')
        # 20 short keys fit one leaf: the own tree is one leaf, no branches.
        lay.set64(doff + 8, 0)                      # md_branch_pages
        lay.set64(doff + 16, 1)                     # md_leaf_pages
        with open(data, 'wb') as f:
            f.write(lay.raw)
        self.assertEqual(V.verify(path), [])

    # -- crash consistency: garbage outside the committed snapshot is fine --

    def test_trailing_uncommitted_garbage(self):
        # A crash can leave allocated-but-uncommitted pages past the committed
        # frontier (mm_last_pg); they are outside the snapshot and must not
        # fail verification.
        path = temp_dir()

        def fn(e):
            with e.begin(write=True) as t:
                for i in range(200):
                    t.put(b'k%04d' % i, b'v' * 10)
        _build(path, fn)
        layout = _detect_layout(path)
        self.assertIsNotNone(layout)
        assert layout is not None
        psize = layout[2]
        with open(os.path.join(path, 'data.mdb'), 'ab') as f:
            f.write(b'\xa5' * (4 * psize))
        self.assertEqual(V.verify(path), [])

    def test_garbage_in_free_pages(self):
        # Pages the committed snapshot lists as free may hold arbitrary bytes
        # (e.g. writes from a transaction lost in a crash).  Their contents are
        # outside the snapshot: verify must still pass, and the engine must
        # still read the file.
        path = temp_dir()

        def fn(e):
            with e.begin(write=True) as t:
                for i in range(2000):
                    t.put(b'k%06d' % i, b'v' * 20)
            with e.begin(write=True) as t:
                for i in range(0, 2000, 3):
                    t.delete(b'k%06d' % i)
        _build(path, fn)
        data = os.path.join(path, 'data.mdb')
        with open(data, 'rb') as f:
            f.seek(0, os.SEEK_END)
            v = V._Verifier(f, f.tell())
            self.assertEqual(v.verify(), [])
            free_pages = [pg for pg in range(2, v.next_pgno) if v.free[pg]]
        self.assertTrue(free_pages, 'expected a populated freeDB')
        with open(data, 'r+b') as f:
            for pg in free_pages[:8]:
                f.seek(pg * v.psize)
                f.write(b'\xff' * v.psize)
        self.assertEqual(V.verify(path), [])
        env = lmdb.open(path, readonly=True, lock=False)
        try:
            with env.begin() as t:
                n = sum(1 for _ in t.cursor())
        finally:
            env.close()
        self.assertTrue(n)

    def test_crash_rollback(self):
        # Model a mid-commit crash: later transactions' page writes are in the
        # file but the metas still name an older committed snapshot.  Copy-on-
        # write guarantees those writes only touched pages the old snapshot
        # holds free (or beyond its frontier), so the rolled-back image must
        # verify clean and the engine must read the old data from it.
        path = temp_dir()
        data = os.path.join(path, 'data.mdb')
        env = lmdb.open(path, map_size=32 * 1024 * 1024)
        try:
            with env.begin(write=True) as t:
                for i in range(2000):
                    t.put(b'k%06d' % i, b'A' * 20)
            env.sync(True)
        finally:
            env.close()
        with open(data, 'rb') as f:
            snap = f.read()

        env = lmdb.open(path, map_size=32 * 1024 * 1024)
        try:
            with env.begin(write=True) as t:
                for i in range(0, 2000, 2):
                    t.delete(b'k%06d' % i)
            with env.begin(write=True) as t:
                for i in range(3000):
                    t.put(b'n%06d' % i, b'B' * 30)
            env.sync(True)
        finally:
            env.close()
        with open(data, 'rb') as f:
            later = bytearray(f.read())

        psize = _Layout(bytearray(snap)).psize
        later[:2 * psize] = snap[:2 * psize]      # metas from the old snapshot
        crash = temp_dir()
        with open(os.path.join(crash, 'data.mdb'), 'wb') as f:
            f.write(later)

        self.assertEqual(V.verify(crash), [])
        env = lmdb.open(crash, readonly=True, lock=False)
        try:
            with env.begin() as t:
                self.assertEqual(sum(1 for _ in t.cursor()), 2000)
                self.assertEqual(t.get(b'k000000'), b'A' * 20)
                self.assertIsNone(t.get(b'n000000'))
        finally:
            env.close()


# ============================ negative tests ================================

@unittest.skipUnless(SUPPORTED, 'verifier supports 64-bit 0.9/1.0 files only')
class VerifyNegativeTest(LmdbTest):
    def _forge(self, mutate, deletions=False, big=False):
        """Build a DB (optionally with deletions to populate the freeDB, or
        with `big` overflow values), confirm it passes clean, then apply
        `mutate` to the raw bytes and return the verify() result on the
        forged file."""
        path = temp_dir()

        def fn(e):
            with e.begin(write=True) as t:
                for i in range(2000):
                    t.put(b'k%06d' % i, b'v' * 20)
            if big:
                with e.begin(write=True) as t:
                    for i in range(30):
                        t.put(b'big%03d' % i, b'x' * 9000)
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

    def _assert_rejected(self, mutate, deletions=False, big=False, needle=None):
        path = self._forge(mutate, deletions, big)
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

    def test_leaf_keys_out_of_order(self):
        # Zero out a live leaf's second key so it sorts below its predecessor:
        # the comparator machinery must notice.
        def mut(l):
            pg = l.descend_leaf(l.main_root())
            if pg is None or l.numkeys(pg) < 2:
                return False
            o = l.node_off(pg, 1)
            ks = l.u16(o + 6)
            if not ks:
                return False
            l.raw[o + 8:o + 8 + ks] = b'\x00' * ks
        self._assert_rejected(mut, needle='out of order')

    def test_entries_counter_forged(self):
        def mut(l):
            l.set64(l.main_off() + 32, l.u64(l.main_off() + 32) + 1)
        self._assert_rejected(mut, needle='entries, record says')

    def test_leaf_pages_counter_forged(self):
        def mut(l):
            l.set64(l.main_off() + 16, l.u64(l.main_off() + 16) + 1)
        self._assert_rejected(mut, needle='leaf pages, record says')

    def test_leaf_with_overflow_flag(self):
        # A reachable leaf forged as P_LEAF|P_OVERFLOW must be rejected, not
        # treated as a leaf with a stray bit.
        def mut(l):
            pg = l.descend_leaf(l.main_root())
            if pg is None:
                return False
            off = pg * l.psize + l.hdrsz - 6
            l.set16(off, l.u16(off) | 0x04)
        self._assert_rejected(mut, needle='mixed or invalid page flags')

    def test_overflow_page_with_leaf_flag(self):
        # The converse forgery: an overflow page carrying P_LEAF as well.
        def mut(l):
            bn = l.bigdata_node()
            if bn is None:
                return False
            _, _, op = bn
            off = op * l.psize + l.hdrsz - 6
            l.set16(off, l.u16(off) | 0x02)
        self._assert_rejected(mut, big=True, needle='not a pure overflow')

    def test_overflow_header_pages_forged(self):
        # The overflow page header's mp_pages must equal OVPAGES(dsize).
        def mut(l):
            bn = l.bigdata_node()
            if bn is None:
                return False
            _, _, op = bn
            hoff = op * l.psize + l.hdrsz - 4
            l.set32(hoff, l.u32(hoff) + 1)
        self._assert_rejected(mut, big=True, needle='header mp_pages')

    def test_overflow_pointer_at_live_page(self):
        # Point an F_BIGDATA node at a live tree page instead of its extent.
        def mut(l):
            bn = l.bigdata_node()
            if bn is None:
                return False
            _, doff, _ = bn
            l.set64(doff, l.main_root())
        self._assert_rejected(mut, big=True, needle='not a pure overflow')

    def test_overflow_extent_shifted(self):
        # Shift the extent start into its own body: the "first" page is then
        # raw data, not an overflow header.
        def mut(l):
            bn = l.bigdata_node()
            if bn is None:
                return False
            _, doff, op = bn
            l.set64(doff, op + 1)
        self._assert_rejected(mut, big=True)

    def test_overflow_node_pages_forged(self):
        # 1.0 duplicates the extent length in the node (op_pages); it must
        # agree with OVPAGES(dsize).  0.9 has no node-level count: skip.
        def mut(l):
            if l.hdrsz != 24:
                return False
            bn = l.bigdata_node()
            if bn is None:
                return False
            _, doff, _ = bn
            l.set64(doff + 16, l.u64(doff + 16) + 1)
        self._assert_rejected(mut, big=True, needle='overflow op_pages')


# ============================ MDB_REVERSEDUP ================================

@unittest.skipUnless(SUPPORTED, 'verifier supports 64-bit 0.9/1.0 files only')
class VerifyReversedupTest(LmdbTest):
    """py-lmdb never sets MDB_REVERSEDUP itself, so files that use it (written
    by other bindings or C code) are modelled by building a lexical dupsort DB
    and flipping the flag into its named-DB record.  Single-byte duplicates
    order identically under the lexical and reverse comparators, so the forged
    file is genuinely valid (positive); multi-byte duplicates do not, so the
    verifier must flag them (negative) -- proving the reverse-duplicate
    comparator path actually runs."""

    def _forge_reversedup(self, values):
        path = temp_dir()

        def fn(e):
            db = e.open_db(b'rd', dupsort=True)
            with e.begin(write=True) as t:
                for i in range(4):
                    for v in values:
                        t.put(b'k%d' % i, v, db=db)
        _build(path, fn)
        self.assertEqual(V.verify(path), [], 'clean DB should pass first')

        data = os.path.join(path, 'data.mdb')
        with open(data, 'rb') as f:
            raw = bytearray(f.read())
        lay = _Layout(raw)
        doff = lay.named_db_record()
        if doff is None:
            self.skipTest('could not locate the named-DB record')
        flags = lay.u16(doff + 4)
        self.assertTrue(flags & 0x04, 'expected MDB_DUPSORT set')
        lay.set16(doff + 4, flags | 0x40)           # + MDB_REVERSEDUP
        with open(data, 'wb') as f:
            f.write(lay.raw)
        return path

    def test_reversedup_valid_order(self):
        path = self._forge_reversedup([b'a', b'b', b'c', b'd'])
        self.assertEqual(V.verify(path), [])

    def test_reversedup_detects_disorder(self):
        # Lexically 'ab' < 'ba', but reversed 'ba' < 'ab': the engine-built
        # lexical order violates the REVERSEDUP comparator the flag demands.
        path = self._forge_reversedup([b'ab', b'ba'])
        errors = V.verify(path)
        self.assertTrue(any('dup data out of order' in e for e in errors),
                        'expected a dup-order error, got %r' % (errors,))


if __name__ == '__main__':
    unittest.main()
