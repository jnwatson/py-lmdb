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
import os
import unittest

import testlib
from testlib import B
from testlib import BT
from testlib import OCT

import lmdb

try:
    INT_TYPES = (int, long)
except NameError:
    INT_TYPES = (int,)


class VersionTest(unittest.TestCase):
    def test_version(self):
        ver = lmdb.version()
        assert len(ver) == 3
        assert all(isinstance(i, INT_TYPES) for i in ver)
        assert all(i >= 0 for i in ver)


class OpenTest(unittest.TestCase):
    def test_bad_paths(self):
        self.assertRaises(Exception,
            lambda: lmdb.open('/doesnt/exist/at/all'))
        self.assertRaises(Exception,
            lambda: lmdb.open(testlib.temp_file()))

    def test_ok_path(self):
        path, env = testlib.temp_env()
        assert os.path.exists(path)
        assert os.path.exists(os.path.join(path, 'data.mdb'))
        assert os.path.exists(os.path.join(path, 'lock.mdb'))
        assert env.path() == path

    def test_bad_size(self):
        self.assertRaises(OverflowError,
            lambda: testlib.temp_env(map_size=-123))

    def test_subdir_false_junk(self):
        path = testlib.temp_file()
        fp = open(path, 'wb')
        fp.write(B('A' * 8192))
        fp.close()
        self.assertRaises(lmdb.InvalidError,
            lambda: lmdb.open(path, subdir=False))

    def test_subdir_false_ok(self):
        path = testlib.temp_file(create=False)
        _, env = testlib.temp_env(path, subdir=False)
        assert os.path.exists(path)
        assert os.path.isfile(path)
        assert os.path.isfile(path + '-lock')
        assert not env.flags()['subdir']

    def test_subdir_true_noexist_nocreate(self):
        path = testlib.temp_dir(create=False)
        self.assertRaises(lmdb.Error,
            lambda: testlib.temp_env(path, subdir=True, create=False))
        assert not os.path.exists(path)

    def test_subdir_true_noexist_create(self):
        path = testlib.temp_dir(create=False)
        path_, env = testlib.temp_env(path, subdir=True, create=True)
        assert path_ == path
        assert env.path() == path

    def test_subdir_true_exist_nocreate(self):
        path, env = testlib.temp_env()
        assert lmdb.open(path, subdir=True, create=False).path() == path

    def test_subdir_true_exist_create(self):
        path, env = testlib.temp_env()
        assert lmdb.open(path, subdir=True, create=True).path() == path

    def test_readonly_false(self):
        path, env = testlib.temp_env(readonly=False)
        with env.begin(write=True) as txn:
            txn.put(B('a'), B(''))
        with env.begin() as txn:
            assert txn.get(B('a')) == B('')
        assert not env.flags()['readonly']

    def test_readonly_true_noexist(self):
        path = testlib.temp_dir(create=False)
        # Open readonly missing store should fail.
        self.assertRaises(lmdb.Error,
            lambda: lmdb.open(path, readonly=True, create=True))
        # And create=True should not have mkdir'd it.
        assert not os.path.exists(path)

    def test_readonly_true_exist(self):
        path, env = testlib.temp_env()
        env2 = lmdb.open(path, readonly=True)
        assert env2.path() == path
        # Attempting a write txn should fail.
        self.assertRaises(lmdb.ReadonlyError,
            lambda: env2.begin(write=True))
        # Flag should be set.
        assert env2.flags()['readonly']

    def test_metasync(self):
        for flag in True, False:
            path, env = testlib.temp_env(metasync=flag)
            assert env.flags()['metasync'] == flag

    def test_sync(self):
        for flag in True, False:
            path, env = testlib.temp_env(sync=flag)
            assert env.flags()['sync'] == flag

    def test_map_async(self):
        for flag in True, False:
            path, env = testlib.temp_env(map_async=flag)
            assert env.flags()['map_async'] == flag

    def test_mode_subdir_create(self):
        oldmask = os.umask(0)
        try:
            for mode in OCT('777'), OCT('755'), OCT('700'):
                path = testlib.temp_dir(create=False)
                env = lmdb.open(path, subdir=True, create=True, mode=mode)
                fmode = mode & ~OCT('111')
                assert testlib.path_mode(path) == mode
                assert testlib.path_mode(path+'/data.mdb') == fmode
                assert testlib.path_mode(path+'/lock.mdb') == fmode
        finally:
            os.umask(oldmask)

    def test_mode_subdir_nocreate(self):
        oldmask = os.umask(0)
        try:
            for mode in OCT('777'), OCT('755'), OCT('700'):
                path = testlib.temp_dir()
                env = lmdb.open(path, subdir=True, create=False, mode=mode)
                fmode = mode & ~OCT('111')
                assert testlib.path_mode(path+'/data.mdb') == fmode
                assert testlib.path_mode(path+'/lock.mdb') == fmode
        finally:
            os.umask(oldmask)

    def test_readahead(self):
        for flag in True, False:
            path, env = testlib.temp_env(readahead=flag)
            assert env.flags()['readahead'] == flag

    def test_writemap(self):
        for flag in True, False:
            path, env = testlib.temp_env(writemap=flag)
            assert env.flags()['writemap'] == flag

    def test_max_readers(self):
        self.assertRaises(lmdb.InvalidParameterError,
            lambda: testlib.temp_env(max_readers=0))
        for val in 123, 234:
            _, env = testlib.temp_env(max_readers=val)
            assert env.info()['max_readers'] == val

    def test_max_dbs(self):
        self.assertRaises(OverflowError,
            lambda: testlib.temp_env(max_dbs=-1))
        for val in 0, 10, 20:
            _, env = testlib.temp_env(max_dbs=val)
            dbs = [env.open_db('db%d' % i) for i in range(val)]
            self.assertRaises(lmdb.DbsFullError,
                lambda: env.open_db('toomany'))



class CloseTest(unittest.TestCase):
    def test_close_normal(self):
        path, env = testlib.temp_env()
        # Attempting things should be ok.
        txn = env.begin()
        env.close()
        # Repeated calls are ignored:
        env.close()
        # Attempting to use invalid objects should crash.
        self.assertRaises(Exception,
            lambda: txn.cursor())
        self.assertRaises(Exception,
            lambda: txn.commit())
        # Abort should be OK though.
        txn.abort()
        # Attempting to start new txn should crash.
        self.assertRaises(Exception,
            lambda: env.begin())
        # Attempting to get env state should crash.
        self.assertRaises(Exception,
            lambda: env.path())
        self.assertRaises(Exception,
            lambda: env.flags())
        self.assertRaises(Exception,
            lambda: env.info())


if __name__ == '__main__':
    unittest.main()
