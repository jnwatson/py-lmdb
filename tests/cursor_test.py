#
# Copyright 2013 The py-lmdb authors, all rights reserved.
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

from __future__ import absolute_import
from __future__ import with_statement
import unittest

import testlib
from testlib import B
from testlib import BT


class CursorTest(unittest.TestCase):
    def setUp(self):
        self.path, self.env = testlib.temp_env()
        self.txn = self.env.begin(write=True)
        self.c = self.txn.cursor()

    def testKeyValueItemEmpty(self):
        self.assertEqual(B(''), self.c.key())
        self.assertEqual(B(''), self.c.value())
        self.assertEqual(BT('', ''), self.c.item())

    def testFirstLastEmpty(self):
        self.assertEqual(False, self.c.first())
        self.assertEqual(False, self.c.last())

    def testFirstFilled(self):
        testlib.putData(self.txn)
        self.assertEqual(True, self.c.first())
        self.assertEqual(testlib.ITEMS[0], self.c.item())

    def testLastFilled(self):
        testlib.putData(self.txn)
        self.assertEqual(True, self.c.last())
        self.assertEqual(testlib.ITEMS[-1], self.c.item())

    def testSetKey(self):
        self.assertRaises(Exception, (lambda: self.c.set_key(B(''))))
        self.assertEqual(False, self.c.set_key(B('missing')))
        testlib.putData(self.txn)
        self.assertEqual(True, self.c.set_key(B('b')))
        self.assertEqual(False, self.c.set_key(B('ba')))

    def testSetRange(self):
        self.assertEqual(False, self.c.set_range(B('x')))
        testlib.putData(self.txn)
        self.assertEqual(False, self.c.set_range(B('x')))
        self.assertEqual(True, self.c.set_range(B('a')))
        self.assertEqual(B('a'), self.c.key())
        self.assertEqual(True, self.c.set_range(B('ba')))
        self.assertEqual(B('baa'), self.c.key())
        self.c.set_range(B(''))
        self.assertEqual(B('a'), self.c.key())

    def testDeleteEmpty(self):
        self.assertEqual(False, self.c.delete())

    def testDeleteFirst(self):
        testlib.putData(self.txn)
        self.assertEqual(False, self.c.delete())
        self.c.first()
        self.assertEqual(BT('a', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(BT('b', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(BT('baa', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(BT('d', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(BT('', ''), self.c.item())
        self.assertEqual(False, self.c.delete())
        self.assertEqual(BT('', ''), self.c.item())

    def testDeleteLast(self):
        testlib.putData(self.txn)
        self.assertEqual(True, self.c.last())
        self.assertEqual(BT('d', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(BT('', ''), self.c.item())
        self.assertEqual(False, self.c.delete())
        self.assertEqual(BT('', ''), self.c.item())

    def testCount(self):
        self.assertRaises(Exception, (lambda: self.c.count()))
        testlib.putData(self.txn)
        self.c.first()
        # TODO: complete dup key support.
        #self.assertEqual(1, self.c.count())

    def testPut(self):
        pass

if __name__ == '__main__':
    unittest.main()
