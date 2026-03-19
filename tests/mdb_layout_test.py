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
Low-level LMDB data layout validation tests.

These tests exercise boundary conditions around page sizes, overflow nodes,
sub-page promotion, LEAF2 pages, and branch pages in mdb.c. They are
designed to catch regressions like #431 (false MDB_CORRUPTED on overflow
value overwrite).

All data sizes are derived from env.stat()['psize'] at runtime so the
tests work correctly regardless of OS page size (4K, 16K, etc.).
"""

from __future__ import absolute_import
import unittest

import pytest

import testlib
from testlib import B

import lmdb

pytestmark = pytest.mark.mdb_layout


def _psize(env):
    """Return the page size for the given environment."""
    return env.stat()['psize']


def _make_val(size):
    """Return a bytes value of exactly *size* bytes."""
    return b'V' * size


def _make_key(prefix, n):
    """Return a key like b'prefix-0001'."""
    return B('{}-{:04d}'.format(prefix, n))


# ---------------------------------------------------------------------------
# Class 1: OverflowValueTest
# ---------------------------------------------------------------------------

class OverflowValueTest(testlib.LmdbTest):
    """Targets: overflow pages, node data-size validation, the #431
    regression (overwrite overflow with overflow)."""

    def test_put_get_overflow(self):
        """Put and read back a value larger than page size."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        val = _make_val(psize * 2 + 1)
        with env.begin(write=True) as txn:
            txn.put(B('key1'), val)
        with env.begin() as txn:
            self.assertEqual(txn.get(B('key1')), val)

    def test_overwrite_overflow_with_overflow(self):
        """Overwrite an overflow value with another overflow value.
        This is the exact operation that triggered #431."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        val1 = _make_val(psize * 2 + 1)
        val2 = _make_val(psize * 3 + 1)
        with env.begin(write=True) as txn:
            txn.put(B('key1'), val1)
        with env.begin(write=True) as txn:
            txn.put(B('key1'), val2)
        with env.begin() as txn:
            self.assertEqual(txn.get(B('key1')), val2)

    def test_overwrite_overflow_with_small(self):
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        big = _make_val(psize * 2 + 1)
        small = _make_val(32)
        with env.begin(write=True) as txn:
            txn.put(B('key1'), big)
        with env.begin(write=True) as txn:
            txn.put(B('key1'), small)
        with env.begin() as txn:
            self.assertEqual(txn.get(B('key1')), small)

    def test_overwrite_small_with_overflow(self):
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        small = _make_val(32)
        big = _make_val(psize * 2 + 1)
        with env.begin(write=True) as txn:
            txn.put(B('key1'), small)
        with env.begin(write=True) as txn:
            txn.put(B('key1'), big)
        with env.begin() as txn:
            self.assertEqual(txn.get(B('key1')), big)

    def test_delete_overflow(self):
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        val = _make_val(psize * 2 + 1)
        with env.begin(write=True) as txn:
            txn.put(B('key1'), val)
        with env.begin(write=True) as txn:
            self.assertTrue(txn.delete(B('key1')))
        with env.begin() as txn:
            self.assertIsNone(txn.get(B('key1')))

    def test_cursor_traverse_mixed_sizes(self):
        """Cursor iteration over a mix of small and overflow values."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        items = []
        for i in range(20):
            k = _make_key('k', i)
            if i % 3 == 0:
                v = _make_val(psize * 2 + i)
            else:
                v = _make_val(32 + i)
            items.append((k, v))
        with env.begin(write=True) as txn:
            for k, v in items:
                txn.put(k, v)
        items.sort()
        with env.begin() as txn:
            cur = txn.cursor()
            result = list(cur.iternext())
            self.assertEqual(result, items)

    def test_page_split_with_overflow(self):
        """Insert enough overflow entries to force page splits."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        count = 100
        items = {}
        with env.begin(write=True) as txn:
            for i in range(count):
                k = _make_key('split', i)
                v = _make_val(psize + 1 + i)
                txn.put(k, v)
                items[k] = v
        with env.begin() as txn:
            for k, v in items.items():
                self.assertEqual(txn.get(k), v)

    def test_nested_txn_overflow_commit(self):
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        val = _make_val(psize * 2 + 1)
        with env.begin(write=True) as txn:
            with txn.cursor() as cur:
                pass  # ensure txn is active
            child = env.begin(write=True, parent=txn)
            child.put(B('nested'), val)
            child.commit()
        with env.begin() as txn:
            self.assertEqual(txn.get(B('nested')), val)

    def test_nested_txn_overflow_abort(self):
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        val = _make_val(psize * 2 + 1)
        with env.begin(write=True) as txn:
            child = env.begin(write=True, parent=txn)
            child.put(B('nested'), val)
            child.abort()
            txn.put(B('other'), B('yes'))
        with env.begin() as txn:
            self.assertIsNone(txn.get(B('nested')))
            self.assertEqual(txn.get(B('other')), B('yes'))

    def test_boundary_sizes(self):
        """Test values at exact page-size boundaries."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        sizes = [psize - 1, psize, psize + 1, 2 * psize + 1, 10 * psize]
        items = {}
        with env.begin(write=True) as txn:
            for i, sz in enumerate(sizes):
                k = _make_key('bnd', i)
                v = _make_val(sz)
                txn.put(k, v)
                items[k] = v
        with env.begin() as txn:
            for k, v in items.items():
                self.assertEqual(txn.get(k), v)

    def test_many_overflow_overwrites(self):
        """Repeatedly overwrite the same key with different overflow sizes."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        key = B('rewrite')
        for i in range(20):
            val = _make_val(psize + 1 + i * 100)
            with env.begin(write=True) as txn:
                txn.put(key, val)
            with env.begin() as txn:
                self.assertEqual(txn.get(key), val)


# ---------------------------------------------------------------------------
# Class 2: DupsortSubpageTest
# ---------------------------------------------------------------------------

class DupsortSubpageTest(testlib.LmdbTest):
    """Targets: sub-page bounds validation, node shrink delta."""

    def _open_dupsort(self, env):
        return env.open_db(B('dupsort'), dupsort=True)

    def test_few_dups_subpage(self):
        """A small number of dups should stay on a sub-page."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupsort(env)
        with env.begin(write=True, db=db) as txn:
            for i in range(5):
                txn.put(B('key'), _make_key('val', i), dupdata=True)
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            self.assertTrue(cur.set_key(B('key')))
            vals = list(cur.iternext_dup(values=True))
            self.assertEqual(len(vals), 5)

    def test_cursor_next_prev_dup(self):
        """Cursor next_dup / prev_dup within a sub-page."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupsort(env)
        dups = [_make_key('d', i) for i in range(5)]
        with env.begin(write=True, db=db) as txn:
            for d in dups:
                txn.put(B('key'), d, dupdata=True)
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            cur.set_key(B('key'))
            # forward
            fwd = [cur.value()]
            while cur.next_dup():
                fwd.append(cur.value())
            self.assertEqual(fwd, sorted(dups))
            # backward from last dup
            cur.last_dup()
            rev = [cur.value()]
            while cur.prev_dup():
                rev.append(cur.value())
            self.assertEqual(rev, sorted(dups, reverse=True))

    def test_delete_single_dup(self):
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupsort(env)
        dups = [_make_key('d', i) for i in range(5)]
        with env.begin(write=True, db=db) as txn:
            for d in dups:
                txn.put(B('key'), d, dupdata=True)
        with env.begin(write=True, db=db) as txn:
            txn.delete(B('key'), dups[2])
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            cur.set_key(B('key'))
            vals = list(cur.iternext_dup(values=True))
            self.assertEqual(len(vals), 4)
            self.assertNotIn(dups[2], vals)

    def test_delete_all_dups(self):
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupsort(env)
        dups = [_make_key('d', i) for i in range(5)]
        with env.begin(write=True, db=db) as txn:
            for d in dups:
                txn.put(B('key'), d, dupdata=True)
        with env.begin(write=True, db=db) as txn:
            cur = txn.cursor()
            cur.set_key(B('key'))
            cur.delete(dupdata=True)  # delete all dups for this key
        with env.begin(db=db) as txn:
            self.assertIsNone(txn.get(B('key')))

    def test_multiple_keys_with_subpages(self):
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupsort(env)
        with env.begin(write=True, db=db) as txn:
            for ki in range(10):
                key = _make_key('k', ki)
                for di in range(5):
                    txn.put(key, _make_key('v', di), dupdata=True)
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            keys_seen = set()
            if cur.first():
                while True:
                    keys_seen.add(cur.key())
                    vals = list(cur.iternext_dup(values=True))
                    self.assertEqual(len(vals), 5)
                    if not cur.next_nodup():
                        break
            self.assertEqual(len(keys_seen), 10)

    def test_subpage_crud_cycle(self):
        """Add, delete some, re-add — exercises node shrink/grow on sub-pages."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupsort(env)
        key = B('cycle')
        with env.begin(write=True, db=db) as txn:
            for i in range(8):
                txn.put(key, _make_key('v', i), dupdata=True)
        with env.begin(write=True, db=db) as txn:
            for i in range(0, 8, 2):
                txn.delete(key, _make_key('v', i))
        with env.begin(write=True, db=db) as txn:
            for i in range(10, 15):
                txn.put(key, _make_key('v', i), dupdata=True)
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            cur.set_key(key)
            vals = list(cur.iternext_dup(values=True))
            # 4 surviving originals + 5 new = 9
            self.assertEqual(len(vals), 9)


# ---------------------------------------------------------------------------
# Class 3: DupsortSubDbTest
# ---------------------------------------------------------------------------

class DupsortSubDbTest(testlib.LmdbTest):
    """Targets: xcursor node data-size validation, sub-page → sub-DB
    promotion."""

    def _open_dupsort(self, env):
        return env.open_db(B('dupsort'), dupsort=True)

    def _dup_count_for_promotion(self, psize):
        """Return a count of small dups that's large enough to trigger
        promotion from sub-page to sub-DB. We use enough 10-byte values
        to exceed half a page."""
        val_size = 10
        # sub-page overhead is small; fill > half page to force promotion
        return max(psize // val_size, 50)

    def test_promote_subpage_to_subdb(self):
        """Add enough dups to promote sub-page → sub-DB, verify all readable."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupsort(env)
        psize = _psize(env)
        n = self._dup_count_for_promotion(psize)
        dups = [_make_val(10).replace(b'V', bytes([48 + (i % 10)])) +
                str(i).zfill(6).encode() for i in range(n)]
        with env.begin(write=True, db=db) as txn:
            for d in dups:
                txn.put(B('big'), d, dupdata=True)
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            cur.set_key(B('big'))
            vals = list(cur.iternext_dup(values=True))
            self.assertEqual(len(vals), n)

    def test_delete_all_readd_fewer(self):
        """Promote → delete all → re-add fewer dups."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupsort(env)
        psize = _psize(env)
        n = self._dup_count_for_promotion(psize)
        dups = [_make_val(10).replace(b'V', bytes([48 + (i % 10)])) +
                str(i).zfill(6).encode() for i in range(n)]
        with env.begin(write=True, db=db) as txn:
            for d in dups:
                txn.put(B('big'), d, dupdata=True)
        # delete all
        with env.begin(write=True, db=db) as txn:
            cur = txn.cursor()
            cur.set_key(B('big'))
            cur.delete(dupdata=True)
        # re-add fewer
        few = dups[:5]
        with env.begin(write=True, db=db) as txn:
            for d in few:
                txn.put(B('big'), d, dupdata=True)
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            cur.set_key(B('big'))
            vals = list(cur.iternext_dup(values=True))
            self.assertEqual(len(vals), 5)

    def test_cursor_traverse_promoted(self):
        """Full cursor traversal after sub-DB promotion."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupsort(env)
        psize = _psize(env)
        n = self._dup_count_for_promotion(psize)
        with env.begin(write=True, db=db) as txn:
            for i in range(n):
                txn.put(B('prom'), str(i).zfill(8).encode(), dupdata=True)
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            self.assertTrue(cur.set_key(B('prom')))
            count = 0
            while True:
                count += 1
                if not cur.next_dup():
                    break
            self.assertEqual(count, n)

    def test_page_split_within_subdb(self):
        """Force page splits inside the promoted sub-DB."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupsort(env)
        psize = _psize(env)
        # use larger values to force splits inside the sub-DB
        n = self._dup_count_for_promotion(psize) * 3
        with env.begin(write=True, db=db) as txn:
            for i in range(n):
                txn.put(B('split'), str(i).zfill(8).encode(), dupdata=True)
        with env.begin(db=db) as txn:
            stat = txn.stat(db)
            self.assertGreater(stat['depth'], 0)
            cur = txn.cursor()
            cur.set_key(B('split'))
            vals = list(cur.iternext_dup(values=True))
            self.assertEqual(len(vals), n)

    def test_multiple_keys_promoted(self):
        """Multiple keys each promoted to sub-DB."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupsort(env)
        psize = _psize(env)
        n = self._dup_count_for_promotion(psize)
        for ki in range(3):
            key = _make_key('pk', ki)
            with env.begin(write=True, db=db) as txn:
                for i in range(n):
                    txn.put(key, str(i).zfill(8).encode(), dupdata=True)
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            for ki in range(3):
                key = _make_key('pk', ki)
                cur.set_key(key)
                vals = list(cur.iternext_dup(values=True))
                self.assertEqual(len(vals), n)


# ---------------------------------------------------------------------------
# Class 4: Leaf2DupfixedTest
# ---------------------------------------------------------------------------

class Leaf2DupfixedTest(testlib.LmdbTest):
    """Targets: LEAF2 key-size validation (dupfixed pages)."""

    def _open_dupfixed(self, env):
        return env.open_db(B('dupfixed'), dupsort=True, dupfixed=True)

    def _fixed_val(self, i):
        """Return a fixed 8-byte dup value."""
        return str(i).zfill(8).encode()

    def test_dupfixed_basic(self):
        """Insert fixed-size dups, read back."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupfixed(env)
        n = 200
        with env.begin(write=True, db=db) as txn:
            for i in range(n):
                txn.put(B('key'), self._fixed_val(i), dupdata=True)
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            cur.set_key(B('key'))
            vals = list(cur.iternext_dup(values=True))
            self.assertEqual(len(vals), n)

    def test_dupfixed_standalone_leaf2(self):
        """Enough fixed dups to create standalone LEAF2 pages."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupfixed(env)
        psize = _psize(env)
        # need enough 8-byte values to fill multiple pages
        n = (psize // 8) * 4
        with env.begin(write=True, db=db) as txn:
            for i in range(n):
                txn.put(B('key'), self._fixed_val(i), dupdata=True)
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            cur.set_key(B('key'))
            vals = list(cur.iternext_dup(values=True))
            self.assertEqual(len(vals), n)

    def test_dupfixed_cursor_traverse(self):
        """Forward and reverse traversal of dupfixed data."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupfixed(env)
        n = 100
        expected = sorted(self._fixed_val(i) for i in range(n))
        with env.begin(write=True, db=db) as txn:
            for i in range(n):
                txn.put(B('key'), self._fixed_val(i), dupdata=True)
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            cur.set_key(B('key'))
            fwd = list(cur.iternext_dup(values=True))
            self.assertEqual(fwd, expected)
            # last_dup then reverse
            cur.last_dup()
            rev = [cur.value()]
            while cur.prev_dup():
                rev.append(cur.value())
            self.assertEqual(rev, list(reversed(expected)))

    def test_dupfixed_delete_and_merge(self):
        """Delete dups from dupfixed, forcing page merges."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupfixed(env)
        psize = _psize(env)
        n = (psize // 8) * 4
        with env.begin(write=True, db=db) as txn:
            for i in range(n):
                txn.put(B('key'), self._fixed_val(i), dupdata=True)
        # delete every other dup
        with env.begin(write=True, db=db) as txn:
            for i in range(0, n, 2):
                txn.delete(B('key'), self._fixed_val(i))
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            cur.set_key(B('key'))
            vals = list(cur.iternext_dup(values=True))
            self.assertEqual(len(vals), n // 2)

    def test_dupfixed_getmulti(self):
        """getmulti on dupfixed data."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = self._open_dupfixed(env)
        n = 50
        keys = [_make_key('gm', i) for i in range(5)]
        with env.begin(write=True, db=db) as txn:
            for k in keys:
                for i in range(n):
                    txn.put(k, self._fixed_val(i), dupdata=True)
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            result = cur.getmulti(keys, dupdata=True, dupfixed_bytes=8)
            # dupfixed_bytes yields one (key, value) per dup
            self.assertEqual(len(result), len(keys) * n)
            for k, data in result:
                self.assertIn(k, keys)
                self.assertEqual(len(data), 8)


# ---------------------------------------------------------------------------
# Class 5: BranchPageTest
# ---------------------------------------------------------------------------

class BranchPageTest(testlib.LmdbTest):
    """Targets: page bounds validation, root page validation — force a
    deep B-tree (depth >= 3)."""

    def _build_deep_tree(self, env, count=50000):
        """Insert enough entries to guarantee depth >= 3."""
        with env.begin(write=True) as txn:
            for i in range(count):
                txn.put(_make_key('br', i), _make_val(64))
        return count

    def test_deep_tree(self):
        _, env = testlib.temp_env(map_size=1048576 * 256)
        n = self._build_deep_tree(env)
        with env.begin() as txn:
            stat = txn.stat(env.open_db())
            self.assertGreaterEqual(stat['depth'], 3)
            self.assertEqual(stat['entries'], n)

    def test_deep_tree_full_traversal(self):
        _, env = testlib.temp_env(map_size=1048576 * 256)
        n = self._build_deep_tree(env)
        with env.begin() as txn:
            cur = txn.cursor()
            count = sum(1 for _ in cur.iternext())
            self.assertEqual(count, n)

    def test_deep_tree_delete_and_merge(self):
        """Delete half the entries, then traverse remainder."""
        _, env = testlib.temp_env(map_size=1048576 * 256)
        n = self._build_deep_tree(env)
        with env.begin(write=True) as txn:
            for i in range(0, n, 2):
                txn.delete(_make_key('br', i))
        with env.begin() as txn:
            cur = txn.cursor()
            count = sum(1 for _ in cur.iternext())
            self.assertEqual(count, n // 2)

    def test_deep_tree_set_range(self):
        """set_range into the middle of a deep tree."""
        _, env = testlib.temp_env(map_size=1048576 * 256)
        n = self._build_deep_tree(env)
        target = _make_key('br', n // 2)
        with env.begin() as txn:
            cur = txn.cursor()
            self.assertTrue(cur.set_range(target))
            self.assertEqual(cur.key(), target)


# ---------------------------------------------------------------------------
# Class 6: LargeKeyTest
# ---------------------------------------------------------------------------

class LargeKeyTest(testlib.LmdbTest):
    """Test max-key-size boundaries."""

    def test_max_key_with_overflow_value(self):
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        maxk = env.max_key_size()
        key = b'K' * maxk
        val = _make_val(psize * 2 + 1)
        with env.begin(write=True) as txn:
            txn.put(key, val)
        with env.begin() as txn:
            self.assertEqual(txn.get(key), val)

    def test_max_key_page_split(self):
        """Many max-size keys force page splits."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        maxk = env.max_key_size()
        items = {}
        with env.begin(write=True) as txn:
            for i in range(50):
                key = str(i).zfill(6).encode() + b'K' * (maxk - 6)
                val = _make_val(64)
                txn.put(key, val)
                items[key] = val
        with env.begin() as txn:
            for k, v in items.items():
                self.assertEqual(txn.get(k), v)

    def test_max_key_plus_one_rejected(self):
        """A key exceeding max_key_size() must be rejected."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        maxk = env.max_key_size()
        key = b'K' * (maxk + 1)
        with env.begin(write=True) as txn:
            with self.assertRaises(lmdb.BadValsizeError):
                txn.put(key, B('val'))

    def test_max_key_with_small_value(self):
        _, env = testlib.temp_env(map_size=1048576 * 128)
        maxk = env.max_key_size()
        key = b'K' * maxk
        val = B('small')
        with env.begin(write=True) as txn:
            txn.put(key, val)
        with env.begin() as txn:
            self.assertEqual(txn.get(key), val)

    def test_max_key_overwrite(self):
        _, env = testlib.temp_env(map_size=1048576 * 128)
        maxk = env.max_key_size()
        key = b'K' * maxk
        with env.begin(write=True) as txn:
            txn.put(key, B('first'))
        with env.begin(write=True) as txn:
            txn.put(key, B('second'))
        with env.begin() as txn:
            self.assertEqual(txn.get(key), B('second'))


# ---------------------------------------------------------------------------
# Class 7: NestedTxnDataLayoutTest
# ---------------------------------------------------------------------------

class NestedTxnDataLayoutTest(testlib.LmdbTest):
    """Nested transactions interacting with various data layouts."""

    def test_nested_overflow_commit_abort(self):
        """Child commits overflow, grandchild aborts — only child's data persists."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        with env.begin(write=True) as txn:
            child = env.begin(write=True, parent=txn)
            child.put(B('c1'), _make_val(psize * 2 + 1))
            child.commit()

            child2 = env.begin(write=True, parent=txn)
            child2.put(B('c2'), _make_val(psize * 3 + 1))
            child2.abort()
        with env.begin() as txn:
            self.assertIsNotNone(txn.get(B('c1')))
            self.assertIsNone(txn.get(B('c2')))

    def test_nested_dupsort_promotion(self):
        """Nested txn triggers sub-page → sub-DB promotion."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        db = env.open_db(B('ndup'), dupsort=True)
        psize = _psize(env)
        n = max(psize // 10, 50)
        with env.begin(write=True, db=db) as txn:
            # add a few dups in parent
            for i in range(5):
                txn.put(B('key'), str(i).zfill(8).encode(), dupdata=True)
            # promote in child
            child = env.begin(write=True, parent=txn, db=db)
            for i in range(5, n):
                child.put(B('key'), str(i).zfill(8).encode(), dupdata=True)
            child.commit()
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            cur.set_key(B('key'))
            vals = list(cur.iternext_dup(values=True))
            self.assertEqual(len(vals), n)

    def test_three_levels_nesting(self):
        """Three levels of nesting with different data sizes."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        with env.begin(write=True) as txn:
            txn.put(B('L0'), _make_val(32))

            child = env.begin(write=True, parent=txn)
            child.put(B('L1'), _make_val(psize + 1))

            grandchild = env.begin(write=True, parent=child)
            grandchild.put(B('L2'), _make_val(psize * 3 + 1))
            grandchild.commit()

            child.commit()
        with env.begin() as txn:
            self.assertEqual(len(txn.get(B('L0'))), 32)
            self.assertEqual(len(txn.get(B('L1'))), psize + 1)
            self.assertEqual(len(txn.get(B('L2'))), psize * 3 + 1)

    def test_three_levels_middle_abort(self):
        """Three levels — middle aborts, only L0 persists."""
        _, env = testlib.temp_env(map_size=1048576 * 128)
        psize = _psize(env)
        with env.begin(write=True) as txn:
            txn.put(B('L0'), _make_val(32))

            child = env.begin(write=True, parent=txn)
            child.put(B('L1'), _make_val(psize + 1))

            grandchild = env.begin(write=True, parent=child)
            grandchild.put(B('L2'), _make_val(psize * 3 + 1))
            grandchild.commit()

            child.abort()  # L1 and L2 both lost
        with env.begin() as txn:
            self.assertEqual(txn.get(B('L0')), _make_val(32))
            self.assertIsNone(txn.get(B('L1')))
            self.assertIsNone(txn.get(B('L2')))


if __name__ == '__main__':
    unittest.main()
