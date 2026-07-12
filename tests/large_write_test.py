#
# Regression test for the large single-write handling fixed in
# lib/py-lmdb/fix-large-write.patch.
#
# A value that occupies a single overflow page larger than the platform per-
# call write limit (~2 GiB on Linux for one write()/pwrite(); the 32-bit DWORD
# length of WriteFile on Windows) was written in one call.  On Linux the short
# return count was misread as a fatal error, so mdb_txn_commit failed with
# EIO; the compacting-copy writer had the analogous issue.  The fix loops over
# the short count in mdb_page_flush and caps the per-call size in the
# mdb_env_copythr writer (matching mdb_env_copyfd1).
#
# Opt-in: needs a >2 GiB value in RAM plus >2 GiB of disk for the env (and
# again for the compacting copy), so it is skipped unless LMDB_TEST_LARGE is
# set.
#

import os
import unittest

import testlib
import lmdb

SKIP_PURE = (os.environ.get('LMDB_PURE') is not None or
             os.environ.get('LMDB_FORCE_SYSTEM') is not None)
RUN_LARGE = os.environ.get('LMDB_TEST_LARGE') is not None

# 2.25 GiB: a single overflow page that exceeds the ~2 GiB single-write limit.
_SIZE = 0x90000000


def _make_value(size):
    buf = bytearray(size)
    buf[0:4] = b'HEAD'
    buf[size - 4:size] = b'TAIL'
    return buf


@unittest.skipIf(SKIP_PURE, "fix lives in the patched bundled LMDB")
@unittest.skipUnless(RUN_LARGE, "set LMDB_TEST_LARGE=1 (needs >2 GiB RAM + disk)")
class LargeWriteTest(testlib.LmdbTest):
    def test_commit_large_overflow_value(self):
        _, env = testlib.temp_env(map_size=_SIZE + (512 << 20))
        with env.begin(write=True) as txn:
            self.assertTrue(txn.put(b'big', _make_value(_SIZE)))
        with env.begin(buffers=True) as txn:
            got = txn.get(b'big')
            self.assertIsNotNone(got)
            assert got is not None  # narrow Optional for type-checkers
            self.assertEqual(len(got), _SIZE)
            self.assertEqual(bytes(got[0:4]), b'HEAD')
            self.assertEqual(bytes(got[_SIZE - 4:_SIZE]), b'TAIL')

    def test_compact_copy_large_overflow_value(self):
        _, env = testlib.temp_env(map_size=_SIZE + (512 << 20))
        with env.begin(write=True) as txn:
            txn.put(b'big', _make_value(_SIZE))
        dst = testlib.temp_dir()
        env.copy(dst, compact=True)
        denv = lmdb.open(dst, map_size=_SIZE + (512 << 20), readonly=True)
        try:
            with denv.begin(buffers=True) as txn:
                got = txn.get(b'big')
                self.assertIsNotNone(got)
                assert got is not None  # narrow Optional for type-checkers
                self.assertEqual(len(got), _SIZE)
                self.assertEqual(bytes(got[_SIZE - 4:_SIZE]), b'TAIL')
        finally:
            denv.close()

    # A value whose overflow extent (psize * mp_pages) reaches 2**32 exercises
    # the per-call write-length limit differently on each platform: the Linux
    # ~2 GiB write() short-count (handled by mdb_page_flush's loop) and, on
    # Windows, mdb_page_flush's single WriteFile whose length is a 32-bit DWORD
    # -- 2**32 truncates to 0, so the whole page is written as 0 bytes and the
    # value is lost.  Needs ~4 GiB of RAM/disk.
    _EXTENT_SIZE = 0xFFFFF000  # psize*mp_pages == 2**32 for 4K/16K/64K pages

    def test_commit_extent_at_2gib_boundary(self):
        size = self._EXTENT_SIZE
        _, env = testlib.temp_env(map_size=size + (256 << 20))
        with env.begin(write=True) as txn:
            self.assertTrue(txn.put(b'ext', _make_value(size)))
        with env.begin(buffers=True) as txn:
            got = txn.get(b'ext')
            self.assertIsNotNone(got)
            assert got is not None  # narrow Optional for type-checkers
            self.assertEqual(len(got), size)
            self.assertEqual(bytes(got[0:4]), b'HEAD')
            self.assertEqual(bytes(got[size - 4:size]), b'TAIL')


if __name__ == '__main__':
    unittest.main()
