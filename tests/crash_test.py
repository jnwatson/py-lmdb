#
# This is not a test suite! More like a collection of triggers for previously
# observed crashes. Want to contribute to py-lmdb? Please write a test suite!
#
# what happens when empty keys/ values passed to various funcs
# incorrect types
# try to break cpython arg parsing - too many/few/incorrect args

import atexit
import os
import random
import shutil
import tempfile
import unittest

import lmdb


class EnvMixin:
    def setUp(self):
        self.path = tempfile.mkdtemp(prefix='lmdb_test')
        atexit.register(shutil.rmtree, self.path, ignore_errors=True)
        self.env = lmdb.open(self.path, max_dbs=10)

    def tearDown(self):
        self.env.close()
        del self.env


class CrashTest(EnvMixin, unittest.TestCase):
    # Various efforts to cause segfaults.

    def setUp(self):
        EnvMixin.setUp(self)
        with self.env.begin(write=True) as txn:
            txn.put('dave', '')
            txn.put('dave2', '')

    def testOldCrash(self):
        txn = self.env.begin()
        dir(iter(txn.cursor()))

    def testCloseWithTxn(self):
        txn = self.env.begin(write=True)
        self.env.close()
        self.assertRaises(Exception, (lambda: list(txn.cursor())))

    def testDoubleClose(self):
        self.env.close()
        self.env.close()

    def testDbDoubleClose(self):
        db = self.env.open_db(name='dave3')
        #db.close()
        #db.close()

    def testTxnCloseActiveIter(self):
        with self.env.begin() as txn:
            it = txn.cursor().iternext()
        self.assertRaises(Exception, (lambda: list(it)))

    def testDbCloseActiveIter(self):
        db = self.env.open_db(name='dave3')
        with self.env.begin(write=True) as txn:
            txn.put('a', 'b', db=db)
            it = txn.cursor(db=db).iternext()
        self.assertRaises(Exception, (lambda: list(it)))


class LeakTest(EnvMixin, unittest.TestCase):
    # Various efforts to cause Python-level leaks.
    pass



KEYS = 'a', 'b', 'baa', 'd'
ITEMS = [(k, '') for k in KEYS]
REV_ITEMS = ITEMS[::-1]
VALUES = ['' for k in KEYS]

def putData(t, db=None):
    for k, v in ITEMS:
        if db:
            t.put(k, v, db=db)
        else:
            t.put(k, v)


class CursorTest(EnvMixin, unittest.TestCase):
    def setUp(self):
        EnvMixin.setUp(self)
        self.txn = self.env.begin(write=True)
        self.c = self.txn.cursor()

    def testKeyValueItemEmpty(self):
        self.assertEqual('', self.c.key())
        self.assertEqual('', self.c.value())
        self.assertEqual(('', ''), self.c.item())

    def testFirstLastEmpty(self):
        self.assertEqual(False, self.c.first())
        self.assertEqual(False, self.c.last())

    def testFirstFilled(self):
        putData(self.txn)
        self.assertEqual(True, self.c.first())
        self.assertEqual(ITEMS[0], self.c.item())

    def testLastFilled(self):
        putData(self.txn)
        self.assertEqual(True, self.c.last())
        self.assertEqual(ITEMS[-1], self.c.item())

    def testSetKey(self):
        self.assertRaises(Exception, (lambda: self.c.set_key('')))
        self.assertEqual(False, self.c.set_key('missing'))
        putData(self.txn)
        self.assertEqual(True, self.c.set_key('b'))
        self.assertEqual(False, self.c.set_key('ba'))

    def testSetRange(self):
        self.assertEqual(False, self.c.set_range('x'))
        putData(self.txn)
        self.assertEqual(False, self.c.set_range('x'))
        self.assertEqual(True, self.c.set_range('a'))
        self.assertEqual('a', self.c.key())
        self.assertEqual(True, self.c.set_range('ba'))
        self.assertEqual('baa', self.c.key())
        self.c.set_range('')
        self.assertEqual('a', self.c.key())

    def testDeleteEmpty(self):
        self.assertEqual(False, self.c.delete())

    def testDeleteFirst(self):
        putData(self.txn)
        self.assertEqual(False, self.c.delete())
        self.c.first()
        self.assertEqual(('a', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(('b', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(('baa', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(('d', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(('', ''), self.c.item())
        self.assertEqual(False, self.c.delete())
        self.assertEqual(('', ''), self.c.item())

    def testDeleteLast(self):
        putData(self.txn)
        self.assertEqual(True, self.c.last())
        self.assertEqual(('d', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(('', ''), self.c.item())
        self.assertEqual(False, self.c.delete())
        self.assertEqual(('', ''), self.c.item())

    def testCount(self):
        self.assertRaises(Exception, (lambda: self.c.count()))
        putData(self.txn)
        self.c.first()
        # TODO: complete dup key support.
        #self.assertEqual(1, self.c.count())

    def testPut(self):
        pass

class IteratorTest(EnvMixin, unittest.TestCase):
    def setUp(self):
        EnvMixin.setUp(self)
        self.txn = self.env.begin(write=True)
        self.c = self.txn.cursor()

    def testEmpty(self):
        self.assertEqual([], list(self.c))
        self.assertEqual([], list(self.c.iternext()))
        self.assertEqual([], list(self.c.iterprev()))

    def testFilled(self):
        putData(self.txn)
        self.assertEqual(ITEMS, list(self.c))
        self.assertEqual(ITEMS, list(self.c))
        self.assertEqual(ITEMS, list(self.c.iternext()))
        self.assertEqual(ITEMS[::-1], list(self.txn.cursor().iterprev()))
        self.assertEqual(ITEMS[::-1], list(self.c.iterprev()))
        self.assertEqual(ITEMS, list(self.c))

    def testFilledSkipForward(self):
        putData(self.txn)
        self.c.set_range('b')
        self.assertEqual(ITEMS[1:], list(self.c))

    def testFilledSkipReverse(self):
        putData(self.txn)
        self.c.set_range('b')
        self.assertEqual(REV_ITEMS[-2:], list(self.c.iterprev()))

    def testFilledSkipEof(self):
        putData(self.txn)
        self.assertEqual(False, self.c.set_range('z'))
        self.assertEqual(REV_ITEMS, list(self.c.iterprev()))



class BigReverseTest(EnvMixin, unittest.TestCase):
    # Test for issue with MDB_LAST+MDB_PREV skipping chunks of database.
    def test_big_reverse(self):
        txn = self.env.begin(write=True)
        keys = ['%05d' % i for i in range(0xffff)]
        for k in keys:
            txn.put(k, k, append=True)
        assert list(txn.cursor().iterprev(values=False)) == list(reversed(keys))


class MultiCursorDeleteTest(EnvMixin, unittest.TestCase):
    def test1(self):
        """Ensure MDB_NEXT is ignored on `c1' when it was previously positioned
        on the key that `c2' just deleted."""
        txn = self.env.begin(write=True)
        cur = txn.cursor()
        while cur.first():
            cur.delete()

        for i in range(1, 10):
            cur.put(chr(ord('a') + i) * i, '')

        c1 = txn.cursor()
        c1f = c1.iternext(values=False)
        while next(c1f) != 'ddd':
            pass
        c2 = txn.cursor()
        assert c2.set_key('ddd')
        c2.delete()
        assert next(c1f) == 'eeee'


    def test_monster(self):
        # Generate predictable sself.assertEqualuence of sizes.
        rand = random.Random()
        rand.seed(0)

        txn = self.env.begin(write=True)
        keys = []
        for i in range(20000):
            key = '%06x' % i
            val = 'x' * rand.randint(76, 350)
            assert txn.put(key, val)
            keys.append(key)

        deleted = 0
        for key in txn.cursor().iternext(values=False):
            assert txn.delete(key), key
            deleted += 1

        assert deleted == len(keys), deleted


if __name__ == '__main__':
    unittest.main()
