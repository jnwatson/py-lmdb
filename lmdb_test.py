
import operator
import os
import shutil
import unittest

import lmdb

DB_PATH = '/ram/dbtest'


def make_asserter(op, ops):
    def ass(x, y, msg='', *a):
        if msg:
            if a:
                msg %= a
            msg = ' (%s)' % msg

        f = '%r %s %r%s'
        assert op(x, y), f % (x, ops, y, msg)
    return ass

lt = make_asserter(operator.lt, '<')
eq = make_asserter(operator.eq, '==')
le = make_asserter(operator.le, '<=')

def make_env():
    if os.path.exists(DB_PATH):
        shutil.rmtree(DB_PATH)
    return lmdb.connect(DB_PATH, max_dbs=10)


class SmashinateTestCase(unittest.TestCase):
    # Various efforts to cause segfaults.

    def setUp(self):
        self.env = make_env()
        with self.env.begin() as txn:
            txn.put('dave', '')
            txn.put('dave2', '')
            txn.commit()

    def isInvalid(self, fn, *args, **kwargs):
        self.assertRaises(lmdb.Error, fn, *args, **kwargs)

    def testCloseWithTxn(self):
        txn = self.env.begin()
        self.env.close()
        self.isInvalid(lambda: list(txn.cursor()))

    def testDoubleClose(self):
        self.env.close()
        self.env.close()

    def testDbDoubleClose(self):
        db = self.env.open(name='dave')
        db.close()
        db.close()

    def testTxnCloseActiveIter(self):
        with self.env.begin() as txn:
            it = txn.cursor().forward()
        self.isInvalid(lambda: list(it))

    def testDbCloseActiveIter(self):
        db = self.env.open(name='dave')
        with self.env.begin() as txn:
            it = txn.cursor(db=db).forward()
        self.isInvalid(lambda: list(it))


KEYS = 'a', 'b', 'baa', 'd'
ITEMS = [(k, '') for k in KEYS]
REV_ITEMS = ITEMS[::-1]
VALUES = ['' for k in KEYS]

def putData(t, db=None):
    for k, v in ITEMS:
        t.put(k, v, db=db)


class CursorTest(unittest.TestCase):
    def setUp(self):
        self.env = make_env()
        self.txn = self.env.begin()
        self.c = self.txn.cursor()

    def testFirstLastEmpty(self):
        eq(False, self.c.first())
        eq(False, self.c.last())

    def testFirstFilled(self):
        putData(self.txn)
        eq(True, self.c.first())
        eq(ITEMS[0], self.c.item)

    def testLastFilled(self):
        putData(self.txn)
        eq(True, self.c.last())
        eq(ITEMS[-1], self.c.item)

    def testSetKey(self):
        eq(False, self.c.set_key('missing'))
        putData(self.txn)
        eq(True, self.c.set_key('b'))
        eq(False, self.c.set_key('ba'))

    def testSetRange(self):
        eq(False, self.c.set_range('x'))


class IteratorTest(unittest.TestCase):
    def setUp(self):
        self.env = make_env()
        self.txn = self.env.begin()
        self.c = self.txn.cursor()

    def testEmpty(self):
        eq([], list(self.c))
        eq([], list(self.c.forward()))
        eq([], list(self.c.reverse()))

    def testFilled(self):
        putData(self.txn)
        eq(ITEMS, list(self.c))
        eq(ITEMS, list(self.c.forward()))
        eq(ITEMS[::-1], list(self.c.reverse()))

    def testFilledSkipForward(self):
        putData(self.txn)
        self.c.set_range('b')
        eq(ITEMS[1:], list(self.c))

    def testFilledSkipReverse(self):
        putData(self.txn)
        self.c.set_range('b')
        eq(REV_ITEMS[-2:], list(self.c.reverse()))




if __name__ == '__main__':
    unittest.main()
