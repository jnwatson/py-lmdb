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
running against unpatched LMDB (pure or system), since the crashes are real.
"""

import os
import struct
import unittest

import lmdb
import testlib

SKIP_PURE = (os.environ.get('LMDB_PURE') is not None or
             os.environ.get('LMDB_FORCE_SYSTEM') is not None)

# Page and meta layout.  The page header differs between LMDB versions:
#
#   0.9 (PAGEHDRSZ 16, 64-bit):  pgno(8) pad(2) flags(2) lower(2) upper(2)
#   1.0 (PAGEHDRSZ 24, 64-bit):  pgno(8) txnid(8) pad(2) flags(2)
#                                lower(2) upper(2)
#
# Everything else is identical: MDB_meta follows the header as
#   magic(4) version(4) address(8) mapsize(size_t) mm_dbs[2]...
# and MDB_db is 48 bytes on 64-bit in both versions:
#   md_pad(4) md_flags(2) md_depth(2) md_branch_pages(8) md_leaf_pages(8)
#   md_overflow_pages(8) md_entries(8) md_root(8)
#
# Rather than hardcode a version, detect the header size by locating
# MDB_MAGIC in a freshly created environment.  That also keeps the tests
# correct on 32-bit builds, where pgno_t is narrower.

MDB_MAGIC = 0xBEEFC0DE
SIZEOF_MDB_DB = 48


def _detect_layout():
    """Return (PAGEHDRSZ, PAGEBASE) for the LMDB engine backing new
    environments.

    PAGEHDRSZ is found by locating MDB_MAGIC, the first field of MDB_meta,
    which directly follows the page header.

    PAGEBASE (ITS#7713) is 0 on 0.9 but PAGEHDRSZ on 1.0, where it graduated
    out of MDB_DEVEL.  It shifts the frame of reference for mp_lower,
    mp_upper and every mp_ptrs entry, so it is detected rather than assumed:
    write a known number of keys to a fresh leaf page, then see whether
    mp_lower counts from the start of the page or from the end of its
    header."""
    path = testlib.temp_dir()
    env = lmdb.open(path)
    nkeys = 3
    try:
        with env.begin(write=True) as txn:
            for i in range(nkeys):
                txn.put(b'%d' % i, b'v')
    finally:
        env.close()

    with open(os.path.join(path, 'data.mdb'), 'rb') as fp:
        raw = fp.read()

    hdrsz = None
    for off in range(0, 64, 4):
        if struct.unpack_from('=I', raw, off)[0] == MDB_MAGIC:
            hdrsz = off
            break
    assert hdrsz is not None, 'could not locate MDB_MAGIC in meta page'

    psize = struct.unpack_from('<I', raw, hdrsz + 24)[0]
    for off in range(psize * 2, len(raw), psize):
        flags = struct.unpack_from('<H', raw, off + hdrsz - 6)[0]
        if not (flags & 0x02):          # P_LEAF
            continue
        lower = struct.unpack_from('<H', raw, off + hdrsz - 4)[0]
        if lower == hdrsz + 2 * nkeys:
            return hdrsz, 0
        if lower == 2 * nkeys:
            return hdrsz, hdrsz
    raise AssertionError('could not determine PAGEBASE from a leaf page')


PAGEHDRSZ, PAGEBASE = _detect_layout()

# Offsets within a meta page.
_META = PAGEHDRSZ                       # MDB_meta starts here
_MM_DBS = _META + 24                    # magic(4) version(4) address(8)
                                        #   mapsize(8) -> mm_dbs[0]
PSIZE_OFFSET = _MM_DBS                  # uint32: mm_dbs[FREE_DBI].md_pad
FLAGS_FREE_OFFSET = _MM_DBS + 4         # uint16: mm_dbs[FREE_DBI].md_flags
FLAGS_MAIN_OFFSET = (_MM_DBS + SIZEOF_MDB_DB + 4)  # mm_dbs[MAIN_DBI].md_flags

# Offsets within any page.  lower/upper/ptrs sit at the end of the header.
MP_FLAGS_OFFSET = PAGEHDRSZ - 6         # uint16: mp_flags
MP_LOWER_OFFSET = PAGEHDRSZ - 4         # uint16: mp_lower
MP_UPPER_OFFSET = PAGEHDRSZ - 2         # uint16: mp_upper
MP_PTRS_OFFSET = PAGEHDRSZ              # first mp_ptrs entry

P_LEAF = 0x02
# 0.9 only; 1.0 derives dirtiness from mp_txnid and reuses 0x10.
P_DIRTY = 0x10

# Which LMDB engine backs new environments in this run.  Set
# LMDB_DEFAULT_LIB_VERSION=1 to exercise the tests against the 1.0 engine.
ENGINE_MAJOR = lmdb.version()[0]


def only_v09(reason):
    """Mark a corruption test whose recipe has not been carried over to the
    1.0 engine.

    The layout constants above make most of these tests engine-agnostic, but
    a few depend on structures that 1.0 changed more deeply.  None of them
    crash on 1.0 — the corruption simply does not reach the code path it
    reaches on 0.9 — but until each is re-derived for 1.0 the corresponding
    hardening is verified only on the 0.9 engine.  See
    lib1/py-lmdb/PATCH-STATUS.md.
    """
    return unittest.skipIf(ENGINE_MAJOR >= 1, '0.9 engine only: ' + reason)


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

    @only_v09('1.0 removed the P_DIRTY page flag; dirtiness is derived '
              'from mp_txnid (see PATCH-STATUS.md follow-up)')
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
            node_off = off + ptr0 + PAGEBASE
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
            node_off = off + ptr0 + PAGEBASE
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

    @only_v09('corruption does not reach mdb_page_get on 1.0')
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
                # Set mp_lower to 0, which is < PAGEHDRSZ
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
            node_off = off + ptr0 + PAGEBASE
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

    @only_v09('sub-page framing differs under 1.0 PAGEBASE')
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
            nkeys = (lower - (PAGEHDRSZ - PAGEBASE)) >> 1
            for idx in range(nkeys):
                ptr = struct.unpack_from('<H', raw,
                                        off + MP_PTRS_OFFSET + idx * 2)[0]
                if ptr == 0 or ptr >= psize:
                    continue
                node_off = off + ptr + PAGEBASE
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
            nkeys = (lower - (PAGEHDRSZ - PAGEBASE)) >> 1
            for idx in range(nkeys):
                ptr = struct.unpack_from('<H', raw,
                                        off + MP_PTRS_OFFSET + idx * 2)[0]
                if ptr == 0 or ptr >= psize:
                    continue
                node_off = off + ptr + PAGEBASE
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

    @only_v09('LEAF2 md_pad recipe not re-derived for 1.0')
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

    @only_v09('LEAF2 md_pad recipe not re-derived for 1.0')
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
            node_off = off + ptr0 + PAGEBASE
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
            nkeys = (lower - (PAGEHDRSZ - PAGEBASE)) >> 1
            # Corrupt a middle node's mn_hi
            mid = nkeys // 2
            if mid >= nkeys:
                continue
            ptr = struct.unpack_from('<H', raw,
                                    off + MP_PTRS_OFFSET + mid * 2)[0]
            if ptr == 0 or ptr >= psize:
                continue
            node_off = off + ptr + PAGEBASE
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
            nkeys = (lower - (PAGEHDRSZ - PAGEBASE)) >> 1
            for idx in range(nkeys):
                ptr = struct.unpack_from('<H', raw,
                                        off + MP_PTRS_OFFSET + idx * 2)[0]
                if ptr == 0 or ptr >= psize:
                    continue
                node_off = off + ptr + PAGEBASE
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
# uint32: mp_pages, a union with the mp_lower/mp_upper pair at the tail of
# the page header.
MP_PAGES_OFFSET = MP_LOWER_OFFSET


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class OverflowPagesTest(unittest.TestCase):
    """Variant G4: corrupt mp_pages on overflow page causes reading
    past the mmap end."""

    def tearDown(self):
        testlib.cleanup()

    @only_v09('1.0 reads the overflow page count from the node '
              '(MDB_ovpage.op_pages), not the page header')
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
            node_off = off + ptr0 + PAGEBASE
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

    @only_v09('md_depth recipe not re-derived for 1.0')
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
            nkeys = (lower - (PAGEHDRSZ - PAGEBASE)) >> 1
            for idx in range(nkeys):
                ptr = struct.unpack_from('<H', raw,
                                        off + MP_PTRS_OFFSET + idx * 2)[0]
                if ptr == 0 or ptr >= psize:
                    continue
                node_off = off + ptr + PAGEBASE
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

    @only_v09('md_root recipe not re-derived for 1.0')
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
            nkeys = (lower - (PAGEHDRSZ - PAGEBASE)) >> 1
            for idx in range(nkeys):
                ptr = struct.unpack_from('<H', raw,
                                        off + MP_PTRS_OFFSET + idx * 2)[0]
                if ptr == 0 or ptr >= psize:
                    continue
                node_off = off + ptr + PAGEBASE
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


F_DUPDATA = 0x04        # node flag: data has duplicates

# mp_pad within any page: pgno(8) [txnid(8) on 1.0] pad(2).  The 0.9-only
# MP_PAD_OFFSET above assumes the 16-byte header; this one follows the
# detected layout.
ANY_MP_PAD_OFFSET = PAGEHDRSZ - 8


def _nodedata_off(node_off, ksize):
    """Offset of a node's data.

    1.0 aligns node data to an even offset (NODEDATA uses EVEN(mn_ksize));
    0.9 uses mn_ksize as-is.  Getting this wrong shifts the read by one
    byte for odd-length keys, which silently lands on the wrong field.
    """
    if ENGINE_MAJOR >= 1:
        ksize = (ksize + 1) & ~1
    return node_off + 8 + ksize


def _find_dup_node(raw, psize, want_subdata):
    """Find a leaf node holding duplicate data.

    Returns (node_off, data_off) for the first F_DUPDATA node whose
    F_SUBDATA state matches want_subdata: a real sub-DB (an MDB_db record)
    or an embedded sub-page.  Raises if there is none, so that callers get
    a non-optional pair.
    """
    for off in range(psize * 2, len(raw), psize):
        flags = struct.unpack_from('<H', raw, off + MP_FLAGS_OFFSET)[0]
        if not (flags & P_LEAF) or (flags & P_LEAF2):
            continue
        lower = struct.unpack_from('<H', raw, off + MP_LOWER_OFFSET)[0]
        nkeys = (lower - (PAGEHDRSZ - PAGEBASE)) >> 1
        for idx in range(nkeys):
            ptr = struct.unpack_from('<H', raw,
                                     off + MP_PTRS_OFFSET + idx * 2)[0]
            if ptr == 0 or ptr >= psize:
                continue
            node_off = off + ptr + PAGEBASE
            mn_flags = struct.unpack_from('<H', raw, node_off + 4)[0]
            if not (mn_flags & F_DUPDATA):
                continue
            if bool(mn_flags & F_SUBDATA) != want_subdata:
                continue
            ksize = struct.unpack_from('<H', raw, node_off + 6)[0]
            return node_off, _nodedata_off(node_off, ksize)
    raise AssertionError('no %s node found'
                         % ('sub-DB' if want_subdata else 'sub-page'))


@unittest.skipIf(SKIP_PURE, "CVE tests require patched LMDB")
class MdPadTest(unittest.TestCase):
    """The LEAF2 fixed key size must be bounded wherever it is read from
    disk, not just on the page.

    validate-leaf2-keysize bounds a *page's* mp_pad in mdb_page_get, but
    every LEAF2 computation uses the size held in the *DB record*, and
    nothing required the two to agree.  Forging only md_pad leaves every
    page internally consistent, so the page-level checks still pass while
    the key size becomes arbitrary.  See lib/py-lmdb/validate-md-pad.patch
    and misc/md_pad_repro.py.
    """

    def tearDown(self):
        testlib.cleanup()

    def _forge(self, want_subdata, ndups, new_pad):
        """Build a DUPFIXED database, then rewrite only its LEAF2 key
        size.  Returns the reopened env and db handle.

        new_pad may be a callable taking the file's page size.  Several of
        these values only mean what the test intends relative to it: LMDB
        takes its page size from the OS, and macOS on arm64 uses 16 KB
        pages, where a hard-coded 0x1000 is a legitimate key size rather
        than an oversized one.
        """
        path, env = testlib.temp_env()
        db = env.open_db(b'dfdb', dupsort=True, dupfixed=True)
        with env.begin(write=True, db=db) as txn:
            for i in range(ndups):
                txn.put(b'key0', b'v%06d' % i)
        env.close()

        db_path = _db_path(path)
        psize = _read_page_size(db_path)
        with open(db_path, 'rb') as f:
            raw = bytearray(f.read())

        data_off = _find_dup_node(raw, psize, want_subdata)[1]
        if callable(new_pad):
            new_pad = new_pad(psize)

        if want_subdata:
            # md_pad is the first field of the MDB_db record.
            struct.pack_into('<I', raw, data_off, new_pad)
        else:
            struct.pack_into('<H', raw, data_off + ANY_MP_PAD_OFFSET,
                             new_pad)

        with open(db_path, 'wb') as f:
            f.write(raw)

        env = lmdb.open(path, max_dbs=1)
        testlib._cleanups.append(env.close)
        return env, env.open_db(b'dfdb', dupsort=True, dupfixed=True)

    def test_subdb_md_pad_disclosure(self):
        """A md_pad larger than the page must not become the length of the
        buffer handed back to the caller."""
        env, db = self._forge(True, 4000, lambda psize: 8 * psize)
        with self.assertRaises(lmdb.Error):
            with env.begin(db=db) as txn:
                cur = txn.cursor()
                cur.set_key(b'key0')
                cur.value()

    def test_subdb_md_pad_huge(self):
        """md_pad = UINT32_MAX must be refused rather than faulting."""
        env, db = self._forge(True, 4000, 0xFFFFFFFF)
        with self.assertRaises(lmdb.Error):
            with env.begin(db=db) as txn:
                cur = txn.cursor()
                cur.set_key(b'key0')
                cur.value()

    def test_subdb_md_pad_getmulti(self):
        """mv_size = NUMKEYS(page) * md_pad, in 32-bit arithmetic."""
        env, db = self._forge(True, 4000, 0xFFFFFFFF)
        with self.assertRaises(lmdb.Error):
            with env.begin(db=db) as txn:
                cur = txn.cursor()
                cur.set_key(b'key0')
                cur.getmulti((b'key0',), dupdata=True)

    def test_subdb_md_pad_write(self):
        """The LEAF2 branch of mdb_node_add uses md_pad as a memmove()/
        memcpy() length, making this an out-of-bounds write."""
        env, db = self._forge(True, 4000, lambda psize: psize)
        with self.assertRaises(lmdb.Error):
            with env.begin(write=True, db=db) as txn:
                txn.put(b'key0', b'ZZZZZZZ')

    def test_subdb_md_pad_fits_page_but_not_keys(self):
        """A key size small enough to fit the page is still corrupt if the
        page cannot hold that many keys of that size.

        Bounding md_pad on its own leaves this reachable, and it is the
        more dangerous half: LEAF2KEY() multiplies the size by the key
        index, so a value comfortably inside the page still addresses
        megabytes past it by the time the search reaches the middle of a
        full page.  Found by CI on macOS/arm64, whose 16 KB pages left the
        4 KB key size this originally used inside the per-key bound.
        """
        for pad in (lambda psize: psize // 8,
                    lambda psize: psize - PAGEHDRSZ):
            env, db = self._forge(True, 4000, pad)
            with self.assertRaises(lmdb.Error):
                with env.begin(db=db) as txn:
                    cur = txn.cursor()
                    cur.set_key(b'key0')
                    cur.value()
            env, db = self._forge(True, 4000, pad)
            with self.assertRaises(lmdb.Error):
                with env.begin(write=True, db=db) as txn:
                    txn.put(b'key0', b'ZZZZZZZ')

    def test_subpage_mp_pad(self):
        """For few duplicates the dups live in a sub-page embedded in the
        node, whose mp_pad never passes through mdb_page_get."""
        env, db = self._forge(False, 4, 0x4000)
        with self.assertRaises(lmdb.Error):
            with env.begin(db=db) as txn:
                cur = txn.cursor()
                cur.set_key(b'key0')
                cur.value()

    def test_subpage_mp_pad_fits_node_but_not_keys(self):
        """The sub-page equivalent: 14 bytes fits the node data holding
        four 7-byte dups, but four keys of that size do not."""
        env, db = self._forge(False, 4, 14)
        with self.assertRaises(lmdb.Error):
            with env.begin(db=db) as txn:
                cur = txn.cursor()
                cur.set_key(b'key0')
                cur.value()

    def test_normal_dupfixed_still_works(self):
        """The bound must not reject legitimate databases.  A DUPFIXED DB
        record created by mdb_dbi_open has md_pad == 0, so zero cannot be
        treated as corrupt at the top level."""
        path, env = testlib.temp_env()
        db = env.open_db(b'dfdb', dupsort=True, dupfixed=True)
        with env.begin(write=True, db=db) as txn:
            for i in range(4000):
                txn.put(b'key0', b'v%06d' % i)
        env.close()

        env = lmdb.open(path, max_dbs=1)
        testlib._cleanups.append(env.close)
        db = env.open_db(b'dfdb', dupsort=True, dupfixed=True)
        with env.begin(db=db) as txn:
            cur = txn.cursor()
            assert cur.set_key(b'key0')
            assert cur.value() == b'v000000'
            assert len(list(cur.iternext_dup())) == 4000


if __name__ == '__main__':
    unittest.main()
