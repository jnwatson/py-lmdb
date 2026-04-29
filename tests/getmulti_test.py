import unittest

import testlib, struct
from testlib import KEYSFIXED, ITEMS_MULTI_FIXEDKEY
from testlib import putBigDataMultiFixed

class GetMultiTestBase(unittest.TestCase):

    dupsort = None  # type: bool | None
    dupfixed = None  # type: bool | None

    def tearDown(self):
        testlib.cleanup()

    def setUp(self):
        self.db_key = "testdb".encode('utf-8')
        self.path, self.env = testlib.temp_env(max_dbs=1)
        self.txn = self.env.begin(write=True)
        self.db = self.env.open_db(
            key=self.db_key, txn=self.txn,
            dupsort=self.dupsort,  # type: ignore[arg-type]
            dupfixed=self.dupfixed  # type: ignore[arg-type]
            )
        putBigDataMultiFixed(self.txn, db=self.db)
        self.c = self.txn.cursor(db=self.db)

    def matchList(self, ls_a, ls_b):
        return ((not (ls_a or ls_b)) or
            (ls_a and ls_b and all(map(lambda x, y: x == y, ls_a, ls_b))))


class GetMultiTestNoDupsortNoDupfixed(GetMultiTestBase):

    ITEMS2_MULTI_NODUP = ITEMS_MULTI_FIXEDKEY[1::2]
    dupsort = False
    dupfixed = False

    def testGetMulti(self):
        test_list = self.c.getmulti(KEYSFIXED)  # type: ignore[arg-type]
        self.assertEqual(self.matchList(test_list, self.ITEMS2_MULTI_NODUP), True)


class GetMultiTestValuesOff(GetMultiTestBase):
    """Test getmulti(values=False) returns flat list of found keys."""

    dupsort = False
    dupfixed = False

    def testValuesOff(self):
        test_list = self.c.getmulti(KEYSFIXED, values=False)  # type: ignore[arg-type]
        self.assertIsInstance(test_list, list)
        self.assertEqual(len(test_list), len(KEYSFIXED))
        for k in test_list:
            self.assertIsInstance(k, bytes)
            self.assertIn(k, KEYSFIXED)

    def testValuesOffMissing(self):
        """Missing keys are skipped."""
        search_keys = list(KEYSFIXED) + [b'zzz_missing']
        test_list = self.c.getmulti(search_keys, values=False)
        self.assertEqual(len(test_list), len(KEYSFIXED))

    def testValuesOffEmpty(self):
        """Empty key list returns empty list."""
        test_list = self.c.getmulti([], values=False)
        self.assertEqual(test_list, [])


class GetMultiTestDupsortNoDupfixed(GetMultiTestBase):

    dupsort = True
    dupfixed = False

    def testGetMulti(self):
        test_list = self.c.getmulti(KEYSFIXED, dupdata=True)  # type: ignore[arg-type]
        self.assertEqual(self.matchList(test_list, ITEMS_MULTI_FIXEDKEY), True)

    def testValuesOffNoDupdata(self):
        """values=False works on dupsort DB when dupdata=False."""
        test_list = self.c.getmulti(KEYSFIXED, values=False)  # type: ignore[arg-type]
        self.assertIsInstance(test_list, list)
        self.assertEqual(len(test_list), len(KEYSFIXED))

    def testValuesOffDupdataRaises(self):
        """values=False with dupdata=True raises."""
        self.assertRaises(Exception, self.c.getmulti,
                          KEYSFIXED, dupdata=True, values=False)


class GetMultiTestDupsortDupfixed(GetMultiTestBase):

    dupsort = True
    dupfixed = True

    def testGetMulti(self):
        test_list = self.c.getmulti(KEYSFIXED, dupdata=True, dupfixed_bytes=1)  # type: ignore[arg-type]
        self.assertEqual(self.matchList(test_list, ITEMS_MULTI_FIXEDKEY), True)

class GetMultiTestDupsortDupfixedKeyfixed(GetMultiTestBase):

    dupsort = True
    dupfixed = True

    def testGetMulti(self):
        val_bytes = 1
        arr = bytearray(self.c.getmulti(
            KEYSFIXED, dupdata=True,  # type: ignore[arg-type]
            dupfixed_bytes=val_bytes, keyfixed=True
        ))
        asserts = []
        for i, kv in enumerate(ITEMS_MULTI_FIXEDKEY):
            key, val = kv
            asserts.extend((
                struct.pack('b', arr[i*2]) == key,
                struct.pack('b', arr[i*2+1]) == val
            ))
        self.assertEqual(all(asserts), True)


if __name__ == '__main__':
    unittest.main()
