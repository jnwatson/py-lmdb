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

"""
Error behaviour that must be identical on the C extension and CFFI
(issue #503): which exception is raised, its message and attributes.  The
suite runs once per implementation, so each assertion here is checked on
both.
"""

import unittest

import lmdb
import testlib


class ExceptionAttributeTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_lmdb_error_attributes_and_hint(self):
        _, env = testlib.temp_env(map_size=1 << 16)
        with self.assertRaises(lmdb.MapFullError) as cm:
            with env.begin(write=True) as txn:
                for i in range(10000):
                    txn.put(b'%d' % i, b'x' * 100)
        e = cm.exception
        self.assertEqual(e.what, 'mdb_put')
        self.assertEqual(e.code, -30792)  # MDB_MAP_FULL
        self.assertIsInstance(e.reason, str)
        self.assertEqual(str(e), 'mdb_put: %s (Please use a larger '
                         'Environment(map_size=) parameter)' % e.reason)

    def test_use_after_close(self):
        _, env = testlib.temp_env()
        txn = env.begin(write=True)
        cur = txn.cursor()
        env.close()
        for fn in (env.stat, env.info, env.flags, lambda: txn.get(b'a'),
                   txn.commit, cur.first):
            with self.assertRaises(lmdb.Error) as cm:
                fn()
            self.assertEqual(
                str(cm.exception),
                'Attempt to operate on closed/deleted/dropped object.')
            self.assertEqual(cm.exception.code, 0)

    def test_commit_twice(self):
        _, env = testlib.temp_env()
        txn = env.begin(write=True)
        txn.commit()
        self.assertRaises(lmdb.Error, txn.commit)


class ArgumentValidationTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_putmulti_requires_tuples(self):
        _, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            cur = txn.cursor()
            self.assertRaises(TypeError,
                lambda: cur.putmulti([[b'k', b'v']]))  # type: ignore[list-item]
            self.assertRaises(TypeError,
                lambda: cur.putmulti([(b'k', b'v', b'x')]))  # type: ignore[list-item]

    def test_getmulti_invalid_arguments(self):
        _, env = testlib.temp_env(max_dbs=1)
        db = env.open_db(b'd', dupsort=True, dupfixed=True)
        with env.begin(write=True, db=db) as txn:
            cur = txn.cursor()
            self.assertRaises(OverflowError,
                lambda: cur.getmulti([b'k'], dupdata=True, dupfixed_bytes=-1))
            self.assertRaises(TypeError,
                lambda: cur.getmulti([b'k'], dupfixed_bytes=4))
            self.assertRaises(TypeError,
                lambda: cur.getmulti([b'k'], dupdata=True,
                                     keyfixed=True))  # type: ignore[call-overload]
            self.assertRaises(TypeError,
                lambda: cur.getmulti([b'k'], dupdata=True, values=False))

    def test_negative_lib_version(self):
        self.assertRaises(OverflowError, lambda: lmdb.version(lib_version=-1))
        self.assertRaises(OverflowError,
            lambda: lmdb.open(testlib.temp_dir(), lib_version=-1))


class DropTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_empty_keeps_cursors(self):
        '''drop(delete=False) empties the database; its cursors stay usable
        (unpositioned), as in LMDB.'''
        _, env = testlib.temp_env(max_dbs=1)
        db = env.open_db(b'd')
        with env.begin(write=True) as txn:
            txn.put(b'k', b'v', db=db)
            cur = txn.cursor(db)
            self.assertTrue(cur.first())
            txn.drop(db, delete=False)
            self.assertEqual(cur.key(), b'')
            self.assertFalse(cur.first())
            self.assertTrue(cur.put(b'n', b'1'))

    def test_delete_closes_cursors(self):
        _, env = testlib.temp_env(max_dbs=1)
        db = env.open_db(b'd')
        with env.begin(write=True) as txn:
            txn.put(b'k', b'v', db=db)
            cur = txn.cursor(db)
            other = txn.cursor()  # main database: unaffected
            txn.drop(db, delete=True)
            self.assertRaises(lmdb.Error, cur.first)
            other.first()


if __name__ == '__main__':
    unittest.main()
