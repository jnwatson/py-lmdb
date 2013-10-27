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


class CursorTest(unittest.TestCase):
    def setUp(self):
        self.path, self.env = testlib.temp_env()
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
        testlib.putData(self.txn)
        self.assertEqual(True, self.c.first())
        self.assertEqual(testlib.ITEMS[0], self.c.item())

    def testLastFilled(self):
        testlib.putData(self.txn)
        self.assertEqual(True, self.c.last())
        self.assertEqual(testlib.ITEMS[-1], self.c.item())

    def testSetKey(self):
        self.assertRaises(Exception, (lambda: self.c.set_key('')))
        self.assertEqual(False, self.c.set_key('missing'))
        testlib.putData(self.txn)
        self.assertEqual(True, self.c.set_key('b'))
        self.assertEqual(False, self.c.set_key('ba'))

    def testSetRange(self):
        self.assertEqual(False, self.c.set_range('x'))
        testlib.putData(self.txn)
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
        testlib.putData(self.txn)
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
        testlib.putData(self.txn)
        self.assertEqual(True, self.c.last())
        self.assertEqual(('d', ''), self.c.item())
        self.assertEqual(True, self.c.delete())
        self.assertEqual(('', ''), self.c.item())
        self.assertEqual(False, self.c.delete())
        self.assertEqual(('', ''), self.c.item())

    def testCount(self):
        self.assertRaises(Exception, (lambda: self.c.count()))
        testlib.putData(self.txn)
        self.c.first()
        # TODO: complete dup key support.
        #self.assertEqual(1, self.c.count())

    def testPut(self):
        pass
