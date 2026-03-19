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

"""Tests for CVE fixes against crafted data.mdb files.

These tests corrupt data.mdb and verify the patched LMDB rejects
the corruption instead of crashing.  They must be skipped when
running against unpatched (pure) LMDB, since the crashes are real.
"""

from __future__ import absolute_import
import os
import struct
import unittest

import lmdb
import testlib

SKIP_PURE = os.environ.get('LMDB_PURE') is not None

# Meta page layout (64-bit):
#   [0..15]   page header
#   [16..19]  mm_magic (0xBEEFC0DE)
#   [20..23]  mm_version
#   [24..31]  mm_address (void*)
#   [32..39]  mm_mapsize (size_t)
#   [40..43]  mm_dbs[0].md_pad = mm_psize (FREE_DBI)
#   [44..45]  mm_dbs[0].md_flags
# mm_dbs[1] (MAIN_DBI) starts at offset 40+48=88, md_flags at 92.
#
# Page size varies by platform (4096 on x86, 16384 on Apple Silicon).
# Read from the file rather than hardcoding.

PSIZE_OFFSET = 40       # uint32: mm_psize = mm_dbs[FREE_DBI].md_pad
FLAGS_FREE_OFFSET = 44  # uint16: mm_dbs[FREE_DBI].md_flags
FLAGS_MAIN_OFFSET = 92  # uint16: mm_dbs[MAIN_DBI].md_flags

# Offsets within each page
MP_FLAGS_OFFSET = 10    # uint16: mp_flags (after pgno(8) + pad(2))
MP_PTRS_OFFSET = 16     # first mp_ptrs entry (after 16-byte page header)

MP_LOWER_OFFSET = 12    # uint16: mp_lower (after pgno(8) + pad(2) + flags(2))
MP_UPPER_OFFSET = 14    # uint16: mp_upper

P_LEAF = 0x02
P_DIRTY = 0x10


def _read_page_size(db_path):
    """Read mm_psize from the first meta page of a data.mdb file."""
    with open(db_path, 'rb') as f:
        f.seek(PSIZE_OFFSET)
        return struct.unpack('<I', f.read(4))[0]


def _patch_file(path, offset, data):
    with open(path, 'r+b') as f:
        f.seek(offset)
        f.write(data)


def _patch_u16(path, offset, value):
    _patch_file(path, offset, struct.pack('<H', value))


def _db_path(env_path):
    return os.path.join(env_path, 'data.mdb')


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
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

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        for off in (FLAGS_FREE_OFFSET, psize + FLAGS_FREE_OFFSET):
            _patch_u16(db_path, off, 0x18)

        self.assertRaises(lmdb.InvalidError, lmdb.open, path)

    def test_corrupt_main_dbi_flags(self):
        """Corrupt MAIN_DBI md_flags with MDB_DUPFIXED; opening must
        raise InvalidError instead of crashing."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'key', b'val')
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        for off in (FLAGS_MAIN_OFFSET, psize + FLAGS_MAIN_OFFSET):
            _patch_u16(db_path, off, 0x10)

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


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
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

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = 0
        for off in range(psize * 2, len(raw), psize):
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

        with self.assertRaises(lmdb.CorruptedError):
            with env.begin(write=True) as txn:
                txn.delete(b'1')
                txn.put(b'3', b'ddd')


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
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

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if not (flags & P_LEAF):
                continue
            ptr0 = struct.unpack_from('<H', raw, off + MP_PTRS_OFFSET)[0]
            if ptr0 == 0 or ptr0 >= psize:
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

        with self.assertRaises((lmdb.BadTxnError, lmdb.Error)):
            with env.begin(write=True) as txn:
                txn.delete(b'1')
                txn.put(b'3', b'ddd')


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
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

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if not (flags & P_LEAF):
                continue
            ptr0 = struct.unpack_from('<H', raw, off + MP_PTRS_OFFSET)[0]
            if ptr0 == 0 or ptr0 >= psize:
                continue
            node_off = off + ptr0
            mn_flags = struct.unpack_from('<H', raw, node_off + 4)[0]
            if not (mn_flags & 0x04):
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


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
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

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        for off in (PSIZE_OFFSET, psize + PSIZE_OFFSET):
            _patch_file(db_path, off, struct.pack('<I', 0))

        self.assertRaises(lmdb.InvalidError, lmdb.open, path)

    def test_non_power_of_2_page_size(self):
        """Non-power-of-2 mm_psize; open must raise InvalidError."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'k', b'v')
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        for off in (PSIZE_OFFSET, psize + PSIZE_OFFSET):
            _patch_file(db_path, off, struct.pack('<I', 4000))

        self.assertRaises(lmdb.InvalidError, lmdb.open, path)


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class PageBoundsTest(unittest.TestCase):
    """Variant F1+F2: corrupt mp_lower / mp_upper causes NUMKEYS() wrap
    and SIZELEFT() underflow, leading to OOB access."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_mp_lower_underflow(self):
        """Set mp_lower to 0 on a leaf page; NUMKEYS wraps to a huge
        value.  Operations must raise CorruptedError."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'1', b'aaa')
            txn.put(b'2', b'bbb')
            txn.put(b'3', b'ccc')
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if flags & P_LEAF:
                # Set mp_lower to 0, which is < PAGEHDRSZ (16)
                struct.pack_into('<H', raw, off + MP_LOWER_OFFSET, 0)
                patched = True
                break

        self.assertTrue(patched, "No leaf page found to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path)
        testlib._cleanups.append(env.close)

        with self.assertRaises(lmdb.CorruptedError):
            with env.begin() as txn:
                txn.get(b'1')

    def test_corrupt_mp_upper_overflow(self):
        """Set mp_upper beyond page size; operations must raise
        CorruptedError."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'1', b'aaa')
            txn.put(b'2', b'bbb')
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if flags & P_LEAF:
                # Set mp_upper beyond the page size
                struct.pack_into('<H', raw, off + MP_UPPER_OFFSET,
                                psize + 100)
                patched = True
                break

        self.assertTrue(patched, "No leaf page found to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path)
        testlib._cleanups.append(env.close)

        with self.assertRaises(lmdb.CorruptedError):
            with env.begin() as txn:
                txn.get(b'1')

    def test_corrupt_mp_lower_gt_upper(self):
        """Set mp_lower > mp_upper; operations must raise
        CorruptedError."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'1', b'aaa')
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if flags & P_LEAF:
                upper = struct.unpack_from('<H', raw,
                                          off + MP_UPPER_OFFSET)[0]
                # Set mp_lower to upper + 10 (still valid range but > upper)
                struct.pack_into('<H', raw, off + MP_LOWER_OFFSET,
                                upper + 10)
                patched = True
                break

        self.assertTrue(patched, "No leaf page found to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path)
        testlib._cleanups.append(env.close)

        with self.assertRaises(lmdb.CorruptedError):
            with env.begin() as txn:
                txn.get(b'1')


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class NodeReadSizeTest(unittest.TestCase):
    """Variant A1: corrupt NODEDSZ in mdb_node_read exposes arbitrary
    memory via mdb_get / mdb_cursor_get."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_mn_hi_read(self):
        """Corrupt mn_hi on a leaf node; mdb_get must return
        CorruptedError instead of exposing memory."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'1', b'aaa')
            txn.put(b'2', b'bbb')
            txn.put(b'3', b'ccc')
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if not (flags & P_LEAF):
                continue
            ptr0 = struct.unpack_from('<H', raw, off + MP_PTRS_OFFSET)[0]
            if ptr0 == 0 or ptr0 >= psize:
                continue
            node_off = off + ptr0
            # mn_hi is at offset 2 within the MDB_node struct
            mn_hi = struct.unpack_from('<H', raw, node_off + 2)[0]
            if mn_hi == 0:
                # Set mn_hi to make NODEDSZ huge (extends past page)
                struct.pack_into('<H', raw, node_off + 2, 0x0100)
                patched = True
                break

        self.assertTrue(patched, "Could not find node to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path)
        testlib._cleanups.append(env.close)

        with self.assertRaises((lmdb.CorruptedError, lmdb.Error)):
            with env.begin() as txn:
                txn.get(b'1')
                txn.get(b'2')
                txn.get(b'3')


F_DUPDATA = 0x04  # node flag: data is a sub-page or sub-DB


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class SubpageBoundsTest(unittest.TestCase):
    """Variant G3: corrupt mp_upper on an on-disk sub-page causes
    memcpy size underflow (unsigned wrap) in mdb_cursor_put."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_subpage_mp_upper(self):
        """Corrupt mp_upper on a DUPSORT sub-page; put must raise
        CorruptedError instead of heap overflow."""
        path, env = testlib.temp_env()
        db = env.open_db(b'dupdb', dupsort=True)
        with env.begin(write=True, db=db) as txn:
            txn.put(b'key', b'val1')
            txn.put(b'key', b'val2')
            txn.put(b'key', b'val3')
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        # Find a leaf node with F_DUPDATA flag — its data is a sub-page
        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if not (flags & P_LEAF):
                continue
            lower = struct.unpack_from('<H', raw, off + MP_LOWER_OFFSET)[0]
            nkeys = (lower - 16) >> 1  # PAGEHDRSZ=16, PAGEBASE=0
            for idx in range(nkeys):
                ptr = struct.unpack_from('<H', raw,
                                        off + MP_PTRS_OFFSET + idx * 2)[0]
                if ptr == 0 or ptr >= psize:
                    continue
                node_off = off + ptr
                mn_flags = struct.unpack_from('<H', raw, node_off + 4)[0]
                if not (mn_flags & F_DUPDATA):
                    continue
                # Node data is a sub-page.  Skip node header + key.
                mn_ksize = struct.unpack_from('<H', raw, node_off + 6)[0]
                subpage_off = node_off + 8 + mn_ksize  # NODESIZE=8
                # Sub-page header: pgno(8) + pad(2) + flags(2) + lower(2)
                #                  + upper(2)
                sp_upper_off = subpage_off + 14
                # Set mp_upper to 0 so size expression underflows
                struct.pack_into('<H', raw, sp_upper_off, 0)
                patched = True
                break
            if patched:
                break

        self.assertTrue(patched, "No sub-page found to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path, max_dbs=1)
        testlib._cleanups.append(env.close)
        db = env.open_db(b'dupdb', dupsort=True)

        with self.assertRaises((lmdb.CorruptedError, lmdb.Error)):
            with env.begin(write=True, db=db) as txn:
                txn.put(b'key', b'val4')


F_SUBDATA = 0x02  # node flag: data is a sub-DB (MDB_db struct)


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class XcursorNodeDszTest(unittest.TestCase):
    """Variant A5: memcpy sizeof(MDB_db) from NODEDATA in
    mdb_xcursor_init1 without checking node data size."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_subdata_node_size(self):
        """Corrupt mn_lo on a F_SUBDATA node to make NODEDSZ < sizeof(MDB_db);
        operations must raise an error instead of reading past the node."""
        path, env = testlib.temp_env()
        db = env.open_db(b'dupdb', dupsort=True)
        with env.begin(write=True, db=db) as txn:
            # Add enough dups to force sub-DB (not sub-page).
            # Need ~1000 on 16K-page platforms (Apple Silicon).
            for i in range(1000):
                txn.put(b'key', b'val%04d' % i)
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        # Find a leaf node with F_SUBDATA flag
        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if not (flags & P_LEAF):
                continue
            lower = struct.unpack_from('<H', raw, off + MP_LOWER_OFFSET)[0]
            nkeys = (lower - 16) >> 1
            for idx in range(nkeys):
                ptr = struct.unpack_from('<H', raw,
                                        off + MP_PTRS_OFFSET + idx * 2)[0]
                if ptr == 0 or ptr >= psize:
                    continue
                node_off = off + ptr
                mn_flags = struct.unpack_from('<H', raw, node_off + 4)[0]
                if (mn_flags & F_SUBDATA) and (mn_flags & F_DUPDATA):
                    # Set mn_lo to 1 (NODEDSZ = 1, which < sizeof(MDB_db)=48)
                    struct.pack_into('<H', raw, node_off, 1)
                    # Clear mn_hi too
                    struct.pack_into('<H', raw, node_off + 2, 0)
                    patched = True
                    break
            if patched:
                break

        self.assertTrue(patched, "No F_SUBDATA node found to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path, max_dbs=1)
        testlib._cleanups.append(env.close)
        db = env.open_db(b'dupdb', dupsort=True)

        with self.assertRaises((lmdb.CorruptedError, lmdb.BadTxnError,
                                lmdb.Error)):
            with env.begin(db=db) as txn:
                # Navigate to the key to trigger mdb_xcursor_init1
                txn.get(b'key')
                # Also try cursor iteration to cover more code paths
                cur = txn.cursor()
                for key, val in cur:
                    pass


P_LEAF2 = 0x20
MP_PAD_OFFSET = 8  # uint16: mp_pad (after pgno(8))


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class Leaf2KeySizeTest(unittest.TestCase):
    """Variant A6+G1: corrupt md_pad/mp_pad (LEAF2 key size) causes
    OOB access via LEAF2KEY macro."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_mp_pad_zero(self):
        """Set mp_pad to 0 on a LEAF2 page; operations must raise
        CorruptedError."""
        path, env = testlib.temp_env()
        db = env.open_db(b'dfdb', dupsort=True, dupfixed=True)
        with env.begin(write=True, db=db) as txn:
            # Need ~2000 on 16K-page platforms (Apple Silicon)
            for i in range(2000):
                txn.put(b'key', b'v%06d' % i)
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if flags & P_LEAF2:
                # Set mp_pad to 0
                struct.pack_into('<H', raw, off + MP_PAD_OFFSET, 0)
                patched = True
                break

        self.assertTrue(patched, "No LEAF2 page found to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path, max_dbs=1)
        testlib._cleanups.append(env.close)
        db = env.open_db(b'dfdb', dupsort=True, dupfixed=True)

        with self.assertRaises((lmdb.CorruptedError, lmdb.Error)):
            with env.begin(db=db) as txn:
                cur = txn.cursor()
                cur.first()
                list(cur.iternext_dup())

    def test_corrupt_mp_pad_huge(self):
        """Set mp_pad to a huge value on a LEAF2 page; operations must
        raise CorruptedError."""
        path, env = testlib.temp_env()
        db = env.open_db(b'dfdb', dupsort=True, dupfixed=True)
        with env.begin(write=True, db=db) as txn:
            # Need ~2000 on 16K-page platforms (Apple Silicon)
            for i in range(2000):
                txn.put(b'key', b'v%06d' % i)
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if flags & P_LEAF2:
                # Set mp_pad to psize (way too large)
                struct.pack_into('<H', raw, off + MP_PAD_OFFSET, psize)
                patched = True
                break

        self.assertTrue(patched, "No LEAF2 page found to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path, max_dbs=1)
        testlib._cleanups.append(env.close)
        db = env.open_db(b'dfdb', dupsort=True, dupfixed=True)

        with self.assertRaises((lmdb.CorruptedError, lmdb.Error)):
            with env.begin(db=db) as txn:
                cur = txn.cursor()
                cur.first()
                list(cur.iternext_dup())


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class XcursorNullD3D4Test(unittest.TestCase):
    """Variants D3+D4: F_DUPDATA on a node in non-DUPSORT DB causes
    NULL mc_xcursor dereference in MDB_GET_CURRENT and _mdb_cursor_del.
    These are gaps missed by the CVE-2019-16227 patch."""

    def tearDown(self):
        testlib.cleanup()

    def _corrupt_first_node_dupdata(self):
        """Create a DB, set F_DUPDATA on the first leaf node, return path."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'1', b'aaa')
            txn.put(b'2', b'bbb')
            txn.put(b'3', b'ccc')
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if not (flags & P_LEAF):
                continue
            ptr0 = struct.unpack_from('<H', raw, off + MP_PTRS_OFFSET)[0]
            if ptr0 == 0 or ptr0 >= psize:
                continue
            node_off = off + ptr0
            mn_flags = struct.unpack_from('<H', raw, node_off + 4)[0]
            if not (mn_flags & F_DUPDATA):
                struct.pack_into('<H', raw, node_off + 4, mn_flags | F_DUPDATA)
                patched = True
                break

        assert patched, "Could not find node to corrupt"

        with open(db_path, 'wb') as f:
            f.write(raw)
        return path

    def test_get_current_xcursor_null(self):
        """MDB_GET_CURRENT with F_DUPDATA on non-DUPSORT node must
        raise error, not crash (D3)."""
        path = self._corrupt_first_node_dupdata()
        env = lmdb.open(path)
        testlib._cleanups.append(env.close)

        with self.assertRaises((lmdb.CorruptedError, lmdb.Error)):
            with env.begin() as txn:
                cur = txn.cursor()
                cur.first()
                # MDB_GET_CURRENT triggers the F_DUPDATA path
                cur.item()

    def test_cursor_del_xcursor_null(self):
        """cursor.delete() with F_DUPDATA on non-DUPSORT node must
        raise error, not crash (D4)."""
        path = self._corrupt_first_node_dupdata()
        env = lmdb.open(path)
        testlib._cleanups.append(env.close)

        with self.assertRaises((lmdb.CorruptedError, lmdb.Error)):
            with env.begin(write=True) as txn:
                cur = txn.cursor()
                cur.first()
                cur.delete()


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class PageSplitNodeDszTest(unittest.TestCase):
    """Variant A2: corrupt NODEDSZ in mdb_page_split causes OOB memcpy
    via mdb_node_add."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_nodedsz_triggers_split_error(self):
        """Corrupt mn_hi on a node, then force a page split; must raise
        error instead of OOB copy."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            # Fill leaf pages close to capacity with large values.
            # Need ~300 on 16K-page platforms (Apple Silicon).
            for i in range(300):
                txn.put(b'k%04d' % i, b'x' * 40)
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        # Corrupt mn_hi on a node of each leaf page
        patched = 0
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if not (flags & P_LEAF):
                continue
            lower = struct.unpack_from('<H', raw, off + MP_LOWER_OFFSET)[0]
            nkeys = (lower - 16) >> 1
            # Corrupt a middle node's mn_hi
            mid = nkeys // 2
            if mid >= nkeys:
                continue
            ptr = struct.unpack_from('<H', raw,
                                    off + MP_PTRS_OFFSET + mid * 2)[0]
            if ptr == 0 or ptr >= psize:
                continue
            node_off = off + ptr
            struct.pack_into('<H', raw, node_off + 2, 0x0100)
            patched += 1

        self.assertGreater(patched, 0, "Could not find node to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path, map_size=10*1024*1024)
        testlib._cleanups.append(env.close)

        with self.assertRaises((lmdb.CorruptedError, lmdb.BadTxnError,
                                lmdb.Error)):
            with env.begin(write=True) as txn:
                # Insert keys that interleave with existing ones to
                # force splits of the corrupted pages
                for i in range(200):
                    txn.put(b'k%04d_new' % i, b'y' * 40)


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class NodeShrinkUnderflowTest(unittest.TestCase):
    """Variant C1: corrupt sub-page SIZELEFT exceeds NODEDSZ in
    mdb_node_shrink, causing nsize underflow."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_subpage_upper_shrink(self):
        """Corrupt mp_upper on a sub-page to make SIZELEFT > NODEDSZ;
        deleting a dup must not cause nsize underflow."""
        path, env = testlib.temp_env()
        db = env.open_db(b'dupdb', dupsort=True)
        with env.begin(write=True, db=db) as txn:
            txn.put(b'key', b'val1')
            txn.put(b'key', b'val2')
            txn.put(b'key', b'val3')
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        # Find a leaf node with F_DUPDATA (sub-page)
        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if not (flags & P_LEAF):
                continue
            lower = struct.unpack_from('<H', raw, off + MP_LOWER_OFFSET)[0]
            nkeys = (lower - 16) >> 1
            for idx in range(nkeys):
                ptr = struct.unpack_from('<H', raw,
                                        off + MP_PTRS_OFFSET + idx * 2)[0]
                if ptr == 0 or ptr >= psize:
                    continue
                node_off = off + ptr
                mn_flags = struct.unpack_from('<H', raw, node_off + 4)[0]
                if not (mn_flags & F_DUPDATA) or (mn_flags & F_SUBDATA):
                    continue
                # Node data is a sub-page
                mn_ksize = struct.unpack_from('<H', raw, node_off + 6)[0]
                subpage_off = node_off + 8 + mn_ksize
                # Set mp_upper to a huge value (bigger than NODEDSZ)
                # so SIZELEFT = mp_upper - mp_lower is large
                nodedsz = struct.unpack_from('<H', raw, node_off)[0]
                struct.pack_into('<H', raw, subpage_off + 14,
                                nodedsz + 100)
                patched = True
                break
            if patched:
                break

        self.assertTrue(patched, "No sub-page found to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path, max_dbs=1)
        testlib._cleanups.append(env.close)
        db = env.open_db(b'dupdb', dupsort=True)

        # Deleting a dup triggers mdb_node_shrink on the sub-page.
        # With our patch, the corrupt SIZELEFT is caught and the
        # operation errors instead of underflowing.
        try:
            with env.begin(write=True, db=db) as txn:
                txn.delete(b'key', b'val1')
        except lmdb.Error:
            pass  # Any error is acceptable — the point is no crash


P_OVERFLOW = 0x04
MP_PAGES_OFFSET = 12  # uint32: mp_pages (same offset as mp_lower+mp_upper union)


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class OverflowPagesTest(unittest.TestCase):
    """Variant G4: corrupt mp_pages on overflow page causes reading
    past the mmap end."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_overflow_mp_pages(self):
        """Set mp_pages to a huge value on an overflow page; writing
        must raise CorruptedError."""
        path, env = testlib.temp_env(map_size=10*1024*1024)
        with env.begin(write=True) as txn:
            # Create an overflow page by writing a value > page size.
            # Must exceed 16K for Apple Silicon (16384-byte pages).
            txn.put(b'big', b'x' * 32000)
            txn.put(b'small', b'y')
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        # Find an overflow page and corrupt mp_pages
        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if flags & P_OVERFLOW:
                # Set mp_pages to a huge value
                struct.pack_into('<I', raw, off + MP_PAGES_OFFSET, 0xFFFF)
                patched = True
                break

        self.assertTrue(patched, "No overflow page found to corrupt")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path, map_size=10*1024*1024)
        testlib._cleanups.append(env.close)

        with self.assertRaises((lmdb.CorruptedError, lmdb.Error)):
            with env.begin(write=True) as txn:
                # Re-writing the big value triggers the overflow page
                # code path in mdb_cursor_put
                txn.put(b'big', b'z' * 32000)


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class CursorPutNodeDszTest(unittest.TestCase):
    """Variant A3: corrupt NODEDSZ in mdb_cursor_put olddata causes
    heap overflow via memcpy into env->me_pbuf."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_nodedsz_put_overwrite(self):
        """Corrupt mn_hi, then overwrite the key; put must raise error."""
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'1', b'aaa')
            txn.put(b'2', b'bbb')
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if not (flags & P_LEAF):
                continue
            ptr0 = struct.unpack_from('<H', raw, off + MP_PTRS_OFFSET)[0]
            if ptr0 == 0 or ptr0 >= psize:
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

        with self.assertRaises((lmdb.CorruptedError, lmdb.BadTxnError,
                                lmdb.Error)):
            with env.begin(write=True) as txn:
                txn.put(b'1', b'new_value')
                txn.put(b'2', b'new_value')

    def test_overwrite_bigdata_not_rejected(self):
        """Overwriting a value stored on overflow pages must not be
        rejected by the NODEDSZ bounds check.  Regression test for #431:
        F_BIGDATA nodes store the logical data size in NODEDSZ but only
        a pgno on the page."""
        path, env = testlib.temp_env(map_size=10*1024*1024)
        with env.begin(write=True) as txn:
            txn.put(b'big', b'x' * 32000)
        with env.begin(write=True) as txn:
            txn.put(b'big', b'y' * 32000, overwrite=True)
        with env.begin() as txn:
            self.assertEqual(txn.get(b'big'), b'y' * 32000)


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class MdDepthTest(unittest.TestCase):
    """Variant F3: md_depth > CURSOR_STACK causes OOB access to
    mc_pg[] and mc_ki[]."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_md_depth(self):
        """Set md_depth to 100 (> CURSOR_STACK=32) on a named DB;
        operations must raise error."""
        path, env = testlib.temp_env()
        db = env.open_db(b'testdb')
        with env.begin(write=True, db=db) as txn:
            txn.put(b'key', b'val')
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        # Find the named DB node ('testdb') and corrupt md_depth
        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if not (flags & P_LEAF):
                continue
            lower = struct.unpack_from('<H', raw, off + MP_LOWER_OFFSET)[0]
            nkeys = (lower - 16) >> 1
            for idx in range(nkeys):
                ptr = struct.unpack_from('<H', raw,
                                        off + MP_PTRS_OFFSET + idx * 2)[0]
                if ptr == 0 or ptr >= psize:
                    continue
                node_off = off + ptr
                mn_flags = struct.unpack_from('<H', raw, node_off + 4)[0]
                mn_ksize = struct.unpack_from('<H', raw, node_off + 6)[0]
                # Check if this is the F_SUBDATA node for our named DB
                if not (mn_flags & F_SUBDATA):
                    continue
                key_data = raw[node_off + 8:node_off + 8 + mn_ksize]
                if key_data != b'testdb':
                    continue
                # md_depth is at offset 6 within MDB_db data
                data_off = node_off + 8 + mn_ksize
                # Corrupt md_depth on all copies
                struct.pack_into('<H', raw, data_off + 6, 100)
                patched = True
                # Don't break — corrupt all copies

        self.assertTrue(patched, "Could not find named DB node")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path, max_dbs=1)
        testlib._cleanups.append(env.close)

        with self.assertRaises((lmdb.CorruptedError, lmdb.Error)):
            db = env.open_db(b'testdb')
            with env.begin(db=db) as txn:
                txn.get(b'key')


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class MdRootMetaTest(unittest.TestCase):
    """Variant A7: md_root pointing to meta pages (0 or 1) causes
    reading meta pages as B-tree pages."""

    def tearDown(self):
        testlib.cleanup()

    def test_corrupt_md_root_to_meta_page(self):
        """Set md_root to 0 (meta page); operations must raise error."""
        path, env = testlib.temp_env()
        db = env.open_db(b'testdb')
        with env.begin(write=True, db=db) as txn:
            txn.put(b'key', b'val')
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        # Find 'testdb' nodes and corrupt md_root to 0
        # md_root is at offset 40 within MDB_db (last field, pgno_t = 8 bytes)
        patched = False
        for off in range(psize * 2, len(raw), psize):
            flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
            if not (flags & P_LEAF):
                continue
            lower = struct.unpack_from('<H', raw, off + MP_LOWER_OFFSET)[0]
            nkeys = (lower - 16) >> 1
            for idx in range(nkeys):
                ptr = struct.unpack_from('<H', raw,
                                        off + MP_PTRS_OFFSET + idx * 2)[0]
                if ptr == 0 or ptr >= psize:
                    continue
                node_off = off + ptr
                mn_flags = struct.unpack_from('<H', raw, node_off + 4)[0]
                mn_ksize = struct.unpack_from('<H', raw, node_off + 6)[0]
                if not (mn_flags & F_SUBDATA):
                    continue
                key_data = raw[node_off + 8:node_off + 8 + mn_ksize]
                if key_data != b'testdb':
                    continue
                data_off = node_off + 8 + mn_ksize
                # md_root is at offset 40 in MDB_db (pgno_t, 8 bytes)
                struct.pack_into('<Q', raw, data_off + 40, 0)
                patched = True

        self.assertTrue(patched, "Could not find named DB node")

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path, max_dbs=1)
        testlib._cleanups.append(env.close)

        with self.assertRaises((lmdb.CorruptedError, lmdb.Error)):
            db = env.open_db(b'testdb')
            with env.begin(db=db) as txn:
                txn.get(b'key')


if __name__ == '__main__':
    unittest.main()
