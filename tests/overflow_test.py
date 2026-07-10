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
# For a value within one page of MAXDATASIZE (4 GiB - 1) the product wraps,
# producing a short memcpy and silent truncation on 64-bit builds.  The fix
# widens both multiplications to size_t.
#
# The wrap itself only triggers for a ~4 GiB value, which additionally runs
# into unrelated large-single-write limits in the page-flush / copy writer
# (see misc/overflow-page-size-mul.md).  These tests therefore exercise the
# *same* overflow-copy code path at ordinary sizes to guard against the
# size_t widening regressing the common case.
#

import os
import unittest

import testlib
import lmdb


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


if __name__ == '__main__':
    unittest.main()
