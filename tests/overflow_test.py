#
# Regression tests for the overflow-page byte-length computations fixed in
# lib/py-lmdb/fix-overflow-page-size-mul.patch.
#
# mp_pages (the number of overflow pages backing a value) is a uint32_t.  Two
# spots multiplied it by the page size in 32-bit arithmetic before widening to
# size_t:
#
#   * mdb_page_touch : memcpy(np, mp, num * env->me_psize)      (COW spill)
#   * mdb_env_cwalk  : me_psize * (omp->mp_pages - 1)           (compaction)
#
# For a value within one page of MAXDATASIZE (4 GiB - 1) the product wraps.
# In mdb_env_cwalk it wraps to 0, so the overflow tail is skipped and the
# compacting copy either truncates the value or walks off the end of the map
# (SIGBUS).  The fix widens both multiplications to size_t.
#
# OverflowCompactCopyTest exercises the same code path at ordinary sizes so
# the fix cannot regress the common case; HugeValueCompactCopyTest drives the
# actual wrap with a ~4 GiB value (opt-in -- see below).
#

import os
import unittest

import testlib
import lmdb

SKIP_PURE = (os.environ.get('LMDB_PURE') is not None or
             os.environ.get('LMDB_FORCE_SYSTEM') is not None)
# The wrap only triggers within one page of MAXDATASIZE, so the value is ~4 GiB.
# Needs a >4 GiB map and ~4 GiB of RAM/disk, so it is opt-in.
RUN_HUGE = os.environ.get('LMDB_TEST_HUGE') is not None


class OverflowCompactCopyTest(testlib.LmdbTest):
    def _roundtrip_via_compact(self, value):
        _, src = testlib.temp_env(map_size=64 * 1024 * 1024)
        with src.begin(write=True) as txn:
            txn.put(b'k', value)
        dst_dir = testlib.temp_dir()
        src.copy(dst_dir, compact=True)   # exercises mdb_env_cwalk overflow copy
        src.close()

        dst = lmdb.open(dst_dir, map_size=64 * 1024 * 1024, readonly=True)
        try:
            with dst.begin() as txn:
                got = txn.get(b'k')
                assert got is not None
                self.assertEqual(len(got), len(value))
                self.assertEqual(got, value)
        finally:
            dst.close()

    def test_multipage_overflow_value(self):
        # ~1 MiB spans many overflow pages -> exercises the overflow branch.
        self._roundtrip_via_compact(b'A' * (1024 * 1024))

    def test_several_overflow_values(self):
        _, src = testlib.temp_env(map_size=128 * 1024 * 1024)
        payloads = {bytes([i]): os.urandom(500000 + i * 4096) for i in range(5)}
        with src.begin(write=True) as txn:
            for k, v in payloads.items():
                txn.put(k, v)
        dst_dir = testlib.temp_dir()
        src.copy(dst_dir, compact=True)
        src.close()
        dst = lmdb.open(dst_dir, map_size=128 * 1024 * 1024, readonly=True)
        try:
            with dst.begin() as txn:
                for k, v in payloads.items():
                    self.assertEqual(txn.get(k), v)
        finally:
            dst.close()


@unittest.skipIf(SKIP_PURE, "fix lives in the patched bundled LMDB")
@unittest.skipUnless(RUN_HUGE, "set LMDB_TEST_HUGE=1 (needs >4 GiB map + ~4 GiB RAM)")
class HugeValueCompactCopyTest(testlib.LmdbTest):
    """Drive the real 32-bit wrap: a value whose overflow tail spans exactly
    2**32 bytes makes me_psize * (mp_pages - 1) wrap to 0 in mdb_env_cwalk.

    Without the fix the compacting copy truncates the value or crashes with
    SIGBUS; with the fix it round-trips byte-for-byte.

    (Requires the large-single-write fix, PR #474, to commit the value in the
    first place.)
    """

    # A few bytes below MAXDATASIZE: mp_pages - 1 == 2**32 / psize for the
    # common 4K/16K/64K page sizes, so the overflow-tail length wraps to 0.
    _SIZE = 0xFFFFFFF8  # 4294967288

    def test_maxdatasize_value_survives_compact_copy(self):
        size = self._SIZE
        map_size = size + (256 << 20)

        buf = bytearray(size)
        buf[0:4] = b'HEAD'
        mid = size // 2
        buf[mid:mid + 3] = b'MID'
        buf[size - 4:size] = b'TAIL'

        _, src = testlib.temp_env(map_size=map_size)
        with src.begin(write=True) as txn:
            self.assertTrue(txn.put(b'huge', buf))  # bytearray via buffer protocol
        del buf

        dst_dir = testlib.temp_dir()
        src.copy(dst_dir, compact=True)
        src.close()

        dst = lmdb.open(dst_dir, map_size=map_size, readonly=True)
        try:
            with dst.begin(buffers=True) as txn:   # zero-copy read-back
                got = txn.get(b'huge')
                self.assertIsNotNone(got, "value missing from compacted copy")
                assert got is not None  # narrow Optional for type-checkers
                self.assertEqual(len(got), size,
                                 "overflow tail dropped by 32-bit size wrap")
                self.assertEqual(bytes(got[0:4]), b'HEAD')
                self.assertEqual(bytes(got[mid:mid + 3]), b'MID')
                self.assertEqual(bytes(got[size - 4:size]), b'TAIL')
        finally:
            dst.close()


if __name__ == '__main__':
    unittest.main()
