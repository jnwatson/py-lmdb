
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

def assertCrash(fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except (TypeError, lmdb.Error):
        return
    assert 0, '%r(%r, %r) did not crash as expected' % (fn, args, kwargs)

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

    def testCloseWithTxn(self):
        txn = self.env.begin()
        self.env.close()
        assertCrash(lambda: list(txn.cursor()))

    def testDoubleClose(self):
        self.env.close()
        assertCrash(lambda: self.env.close())

    def testDbDoubleClose(self):
        db = self.env.open(name='dave')
        db.close()
        db.close()

    def testTxnCloseActiveIter(self):
        with self.env.begin() as txn:
            it = txn.cursor().forward()
        assertCrash(lambda: list(it))

    def testDbCloseActiveIter(self):
        db = self.env.open(name='dave')
        with self.env.begin() as txn:
            it = txn.cursor(db=db).forward()
        assertCrash(lambda: list(it))


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

    def testKeyValueItemEmpty(self):
        eq('', self.c.key())
        eq('', self.c.value())
        eq(('', ''), self.c.item())

    def testFirstLastEmpty(self):
        eq(False, self.c.first())
        eq(False, self.c.last())

    def testFirstFilled(self):
        putData(self.txn)
        eq(True, self.c.first())
        eq(ITEMS[0], self.c.item())

    def testLastFilled(self):
        putData(self.txn)
        eq(True, self.c.last())
        eq(ITEMS[-1], self.c.item())

    def testSetKey(self):
        eq(False, self.c.set_key('missing'))
        putData(self.txn)
        eq(True, self.c.set_key('b'))
        eq(False, self.c.set_key('ba'))

    def testSetRange(self):
        eq(False, self.c.set_range('x'))
        putData(self.txn)
        eq(False, self.c.set_range('x'))
        eq(True, self.c.set_range('a'))
        eq('a', self.c.key())
        eq(True, self.c.set_range('ba'))
        eq('baa', self.c.key())

    def testDeleteEmpty(self):
        eq(False, self.c.delete())

    def testDeleteFirst(self):
        putData(self.txn)
        eq(False, self.c.delete())
        self.c.first()
        eq(('a', ''), self.c.item())
        eq(True, self.c.delete())
        eq(('b', ''), self.c.item())
        eq(True, self.c.delete())
        eq(('baa', ''), self.c.item())
        eq(True, self.c.delete())
        eq(('d', ''), self.c.item())
        eq(True, self.c.delete())
        eq(('', ''), self.c.item())
        eq(False, self.c.delete())
        eq(('', ''), self.c.item())

    def testDeleteLast(self):
        assert 0,'crash'
        putData(self.txn)
        eq(True, self.c.last())
        eq(('d', ''), self.c.item())
        eq(True, self.c.delete())
        eq(False, self.c.delete())

    def testCount(self):
        assertCrash(lambda: self.c.count())
        putData(self.txn)
        self.c.first()
        # TODO: complete dup key support.
        #eq(1, self.c.count())

    def testPut(self):
        pass

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

    def testFilledSkipEof(self):
        putData(self.txn)
        eq(False, self.c.set_range('z'))
        eq(REV_ITEMS, list(self.c.reverse()))



if __name__ == '__main__':
    unittest.main()
