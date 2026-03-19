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

"""Tests for CVE fixes against crafted data.mdb files."""

from __future__ import absolute_import
import os
import struct
import unittest

import lmdb
import testlib

PAGE_SIZE = 4096

# Meta page layout (64-bit):
#   [0..15]   page header
#   [16..19]  mm_magic (0xBEEFC0DE)
#   [20..23]  mm_version
#   [24..31]  mm_address (void*)
#   [32..39]  mm_mapsize (size_t)
#   [40..43]  mm_dbs[0].md_pad (FREE_DBI)
#   [44..45]  mm_dbs[0].md_flags
# mm_dbs[1] (MAIN_DBI) starts at offset 40+48=88, md_flags at 92.

FREE_DBI_FLAGS_OFFSETS = (44, PAGE_SIZE + 44)
MAIN_DBI_FLAGS_OFFSETS = (92, PAGE_SIZE + 92)

# mp_flags is at offset 10 within each page (after pgno(8) + pad(2)).
MP_FLAGS_OFFSET = 10
P_LEAF = 0x02
P_DIRTY = 0x10


def _patch_file(path, offset, data):
    with open(path, 'r+b') as f:
        f.seek(offset)
        f.write(data)


def _patch_u16(path, offset, value):
    _patch_file(path, offset, struct.pack('<H', value))


def _read_u16(path, offset):
    with open(path, 'rb') as f:
        f.seek(offset)
        return struct.unpack('<H', f.read(2))[0]


class CVE_2019_16224_Test(unittest.TestCase):
    """CVE-2019-16224: MDB_DUPFIXED without MDB_DUPSORT causes heap
    buffer overflow in mdb_node_add via crafted md_flags."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_free_dbi_flags(self):
        """Corrupt FREE_DBI md_flags with MDB_DUPFIXED; opening must
        raise InvalidError instead of crashing."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            for i in range(50):
                txn.put(b'key%04d' % i, b'x' * 200)
        env.close()

        # Patch FREE_DBI md_flags: 0x08 -> 0x18 (add MDB_DUPFIXED)
        db_path = os.path.join(path, 'data.mdb')
        for off in FREE_DBI_FLAGS_OFFSETS:
            _patch_u16(db_path, off, 0x18)

        self.assertRaises(lmdb.InvalidError, lmdb.open, path)

    def test_corrupt_main_dbi_flags(self):
        """Corrupt MAIN_DBI md_flags with MDB_DUPFIXED; opening must
        raise InvalidError instead of crashing."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'key', b'val')
        env.close()

        db_path = os.path.join(path, 'data.mdb')
        for off in MAIN_DBI_FLAGS_OFFSETS:
            _patch_u16(db_path, off, 0x10)  # MDB_DUPFIXED only

        self.assertRaises(lmdb.InvalidError, lmdb.open, path)

    def test_valid_dupsort_dupfixed_still_works(self):
        """Ensure valid MDB_DUPSORT|MDB_DUPFIXED databases are not
        rejected by the flag validation."""
        path, env = testlib.temp_env()
        db = env.open_db(b'dupdb', dupsort=True, dupfixed=True)
        with env.begin(write=True, db=db) as txn:
            txn.put(b'key', b'val1')
            txn.put(b'key', b'val2')

        with env.begin(db=db) as txn:
            self.assertEqual(txn.stat(db)['entries'], 2)


class CVE_2019_16225_Test(unittest.TestCase):
    """CVE-2019-16225: P_DIRTY set on disk pages causes mdb_page_touch()
    to skip copy-on-write, leading to writes on read-only mmap'd memory
    (SIGSEGV)."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_leaf_page_dirty_flag(self):
        """Set P_DIRTY on a leaf page on disk; write operations must
        return MDB_CORRUPTED instead of crashing."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'1', b'aaa')
            txn.put(b'2', b'bbb')
            txn.put(b'3', b'ccc')
        env.close()

        # Find and corrupt leaf pages by setting P_DIRTY
        db_path = os.path.join(path, 'data.mdb')
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = 0
        for off in range(PAGE_SIZE * 2, len(raw), PAGE_SIZE):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if (flags & P_LEAF) and not (flags & P_DIRTY):
                struct.pack_into('<H', raw, off + MP_FLAGS_OFFSET,
                                flags | P_DIRTY)
                patched += 1

        self.assertGreater(patched, 0, "No leaf pages found to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path)
        testlib._cleanups.append(env.close)

        # The delete should hit the corrupted page and get MDB_CORRUPTED
        with self.assertRaises(lmdb.CorruptedError):
            with env.begin(write=True) as txn:
                txn.delete(b'1')
                txn.put(b'3', b'ddd')


class CVE_2019_16226_Test(unittest.TestCase):
    """CVE-2019-16226: corrupt mn_hi in a node causes NODEDSZ() to return
    a huge value, leading to an out-of-bounds memmove in mdb_node_del."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_node_mn_hi(self):
        """Corrupt mn_hi on a leaf node; delete must not crash."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'1', b'aaa')
            txn.put(b'2', b'bbb')
            txn.put(b'3', b'ccc')
        env.close()

        # Corrupt mn_hi of first node on a leaf page.
        # MDB_node layout: mn_lo(2) + mn_hi(2) + mn_flags(2) + mn_ksize(2)
        # mn_hi is at offset 2 within the node.
        db_path = os.path.join(path, 'data.mdb')
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = False
        for off in range(PAGE_SIZE * 2, len(raw), PAGE_SIZE):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if not (flags & P_LEAF):
                continue
            # Get first node pointer
            ptr0 = struct.unpack_from('<H', raw, off + 16)[0]
            if ptr0 == 0 or ptr0 >= PAGE_SIZE:
                continue
            node_off = off + ptr0
            mn_hi = struct.unpack_from('<H', raw, node_off + 2)[0]
            if mn_hi == 0:
                struct.pack_into('<H', raw, node_off + 2, 0x0100)
                patched = True
                break

        self.assertTrue(patched, "Could not find node to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path)
        testlib._cleanups.append(env.close)

        # The delete triggers mdb_node_del with the corrupt node size.
        # With the fix, the txn gets MDB_TXN_ERROR and subsequent ops
        # fail with BadTxnError instead of crashing.
        with self.assertRaises((lmdb.BadTxnError, lmdb.Error)):
            with env.begin(write=True) as txn:
                txn.delete(b'1')
                txn.put(b'3', b'ddd')


class CVE_2019_16227_Test(unittest.TestCase):
    """CVE-2019-16227: F_DUPDATA set on a node in a non-DUPSORT DB causes
    NULL dereference of mc_xcursor in mdb_xcursor_init1."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_node_dupdata_flag(self):
        """Set F_DUPDATA on a node in a non-DUPSORT DB; operations must
        return an error instead of crashing."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'1', b'aaa')
            txn.put(b'2', b'bbb')
            txn.put(b'3', b'ccc')
        env.close()

        # Corrupt mn_flags of first node: set F_DUPDATA (0x04).
        # MDB_node: mn_lo(2) + mn_hi(2) + mn_flags(2) + mn_ksize(2)
        # mn_flags is at offset 4 within the node.
        db_path = os.path.join(path, 'data.mdb')
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = False
        for off in range(PAGE_SIZE * 2, len(raw), PAGE_SIZE):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if not (flags & P_LEAF):
                continue
            ptr0 = struct.unpack_from('<H', raw, off + 16)[0]
            if ptr0 == 0 or ptr0 >= PAGE_SIZE:
                continue
            node_off = off + ptr0
            mn_flags = struct.unpack_from('<H', raw, node_off + 4)[0]
            if not (mn_flags & 0x04):  # Not already F_DUPDATA
                struct.pack_into('<H', raw, node_off + 4, mn_flags | 0x04)
                patched = True
                break

        self.assertTrue(patched, "Could not find node to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path)
        testlib._cleanups.append(env.close)

        with self.assertRaises((lmdb.CorruptedError, lmdb.Error)):
            with env.begin(write=True) as txn:
                txn.delete(b'1')
                txn.put(b'3', b'ddd')


class CVE_2019_16228_Test(unittest.TestCase):
    """CVE-2019-16228: zero mm_psize causes divide-by-zero in
    mdb_env_open2."""

    def tearDown(self):
        testlib.cleanup()

    def test_zero_page_size(self):
        """Zero mm_psize in meta pages; open must raise InvalidError."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'k', b'v')
        env.close()

        # mm_psize = mm_dbs[FREE_DBI].md_pad, uint32 at offset 40
        db_path = os.path.join(path, 'data.mdb')
        for off in (40, PAGE_SIZE + 40):
            _patch_file(db_path, off, struct.pack('<I', 0))

        self.assertRaises(lmdb.InvalidError, lmdb.open, path)

    def test_non_power_of_2_page_size(self):
        """Non-power-of-2 mm_psize; open must raise InvalidError."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'k', b'v')
        env.close()

        db_path = os.path.join(path, 'data.mdb')
        for off in (40, PAGE_SIZE + 40):
            _patch_file(db_path, off, struct.pack('<I', 4000))

        self.assertRaises(lmdb.InvalidError, lmdb.open, path)


if __name__ == '__main__':
    unittest.main()
