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
import signal
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

# Handle moronic Python >=3.0 <3.3.
UnicodeType = type('')
try:
    if UnicodeType is bytes:
        UnicodeType = unicode
except NameError: # Python 2.5 no bytes.
    UnicodeType = unicode

NO_READERS = UnicodeType('(no active readers)\n')


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
    def test_close(self):
        _, env = testlib.temp_env()
        # Attempting things should be ok.
        txn = env.begin(write=True)
        txn.put(B('a'), B(''))
        cursor = txn.cursor()
        list(cursor)
        cursor.first()
        it = iter(cursor)

        env.close()
        # Repeated calls are ignored:
        env.close()
        # Attempting to use invalid objects should crash.
        self.assertRaises(Exception, lambda: txn.cursor())
        self.assertRaises(Exception, lambda: txn.commit())
        self.assertRaises(Exception, lambda: cursor.first())
        self.assertRaises(Exception, lambda: list(it))
        # Abort should be OK though.
        txn.abort()
        # Attempting to start new txn should crash.
        self.assertRaises(Exception,
            lambda: env.begin())


class InfoMethodsTest(unittest.TestCase):
    def test_path(self):
        path, env = testlib.temp_env()
        assert path == env.path()
        assert isinstance(env.path(), UnicodeType)

        env.close()
        self.assertRaises(Exception,
            lambda: env.path())

    def test_stat(self):
        _, env = testlib.temp_env()
        stat = env.stat()
        for k in 'psize', 'depth', 'branch_pages', 'overflow_pages',\
                 'entries':
            assert isinstance(stat[k], INT_TYPES), k
            assert stat[k] >= 0

        assert stat['entries'] == 0
        txn = env.begin(write=True)
        txn.put(B('a'), B('b'))
        txn.commit()
        stat = env.stat()
        assert stat['entries'] == 1

        env.close()
        self.assertRaises(Exception,
            lambda: env.stat())

    def test_info(self):
        _, env = testlib.temp_env()
        info = env.info()
        for k in 'map_addr', 'map_size', 'last_pgno', 'last_txnid', \
                 'max_readers', 'num_readers':
            assert isinstance(info[k], INT_TYPES), k
            assert info[k] >= 0

        assert info['last_txnid'] == 0
        txn = env.begin(write=True)
        txn.put(B('a'), B(''))
        txn.commit()
        info = env.info()
        assert info['last_txnid'] == 1

        env.close()
        self.assertRaises(Exception,
            lambda: env.info())

    def test_flags(self):
        _, env = testlib.temp_env()
        info = env.flags()
        for k in 'subdir', 'readonly', 'metasync', 'sync', 'map_async',\
                 'readahead', 'writemap':
            assert isinstance(info[k], bool)

        env.close()
        self.assertRaises(Exception,
            lambda: env.flags())

    def test_max_key_size(self):
        _, env = testlib.temp_env()
        mks = env.max_key_size()
        assert isinstance(mks, INT_TYPES)
        assert mks > 0

        env.close()
        self.assertRaises(Exception,
            lambda: env.max_key_size())

    def test_max_readers(self):
        _, env = testlib.temp_env()
        mr = env.max_readers()
        assert isinstance(mr, INT_TYPES)
        assert mr > 0 and mr == env.info()['max_readers']

        env.close()
        self.assertRaises(Exception,
            lambda: env.max_readers())

    def test_readers(self):
        _, env = testlib.temp_env()
        r = env.readers()
        assert isinstance(r, UnicodeType)
        assert r == NO_READERS

        rtxn = env.begin()
        r2 = env.readers()
        assert isinstance(env.readers(), UnicodeType)
        assert env.readers() != r

        env.close()
        self.assertRaises(Exception,
            lambda: env.readers())


class OtherMethodsTest(unittest.TestCase):
    def test_copy(self):
        _, env = testlib.temp_env()
        txn = env.begin(write=True)
        txn.put(B('a'), B('b'))
        txn.commit()

        dest_dir = testlib.temp_dir()
        env.copy(dest_dir)
        assert os.path.exists(dest_dir + '/data.mdb')

        cenv = lmdb.open(dest_dir)
        ctxn = cenv.begin()
        assert ctxn.get(B('a')) == B('b')

        env.close()
        self.assertRaises(Exception,
            lambda: env.copy(testlib.temp_dir()))

    def test_copyfd(self):
        path, env = testlib.temp_env()
        txn = env.begin(write=True)
        txn.put(B('a'), B('b'))
        txn.commit()

        dst_path = testlib.temp_file(create=False)
        fp = open(dst_path, 'wb')
        env.copyfd(fp.fileno())

        dstenv = lmdb.open(dst_path, subdir=False)
        dtxn = dstenv.begin()
        assert dtxn.get(B('a')) == B('b')

        env.close()
        self.assertRaises(Exception,
            lambda: env.copyfd(fp.fileno()))
        fp.close()

    def test_sync(self):
        _, env = testlib.temp_env()
        env.sync(False)
        env.sync(True)
        env.close()
        self.assertRaises(Exception,
            lambda: env.sync(False))

    def test_reader_check(self):
        path, env = testlib.temp_env()
        rc = env.reader_check()
        assert rc == 0

        txn1 = env.begin()
        assert env.readers() != NO_READERS
        assert env.reader_check() == 0

        # Start a child, open a txn, then crash the child.
        child_pid = os.fork()
        if not child_pid:
            env2 = lmdb.open(path)
            txn = env2.begin()
            os.kill(os.getpid(), signal.SIGKILL)

        assert os.waitpid(child_pid, 0) == (child_pid, signal.SIGKILL)
        assert env.reader_check() == 1
        assert env.reader_check() == 0
        assert env.readers() != NO_READERS
        txn1.abort()
        assert env.readers() == NO_READERS

        env.close()
        self.assertRaises(Exception,
            lambda: env.reader_check())


class BeginTest(unittest.TestCase):
    def test_begin_readonly(self):
        _, env = testlib.temp_env()
        txn = env.begin()
        # Read txn can't write.
        self.assertRaises(lmdb.ReadonlyError,
            lambda: txn.put(B('a'), B('')))
        txn.abort()

        env.close()
        self.assertRaises(Exception,
            lambda: env.begin())

    def test_begin_write(self):
        _, env = testlib.temp_env()
        txn = env.begin(write=True)
        # Write txn can write.
        assert txn.put(B('a'), B(''))
        txn.commit()

    def test_bind_db(self):
        _, env = testlib.temp_env()
        main = env.open_db(None)
        sub = env.open_db('db1')

        txn = env.begin(write=True, db=sub)
        assert txn.put(B('b'), B(''))           # -> sub
        assert txn.put(B('a'), B(''), db=main)  # -> main
        txn.commit()

        txn = env.begin()
        assert txn.get(B('a')) == B('')
        assert txn.get(B('b')) is None
        assert txn.get(B('a'), db=sub) is None
        assert txn.get(B('b'), db=sub) == B('')
        txn.abort()

    def test_parent_readonly(self):
        _, env = testlib.temp_env()
        parent = env.begin()
        # Nonsensical.
        self.assertRaises(lmdb.InvalidParameterError,
            lambda: env.begin(parent=parent))

    def test_parent(self):
        _, env = testlib.temp_env()
        parent = env.begin(write=True)
        parent.put(B('a'), B('a'))

        child = env.begin(write=True, parent=parent)
        assert child.get(B('a')) == B('a')
        assert child.put(B('a'), B('b'))
        child.abort()

        # put() should have rolled back
        assert parent.get(B('a')) == B('a')

        child = env.begin(write=True, parent=parent)
        assert child.put(B('a'), B('b'))
        child.commit()

        # put() should be visible
        assert parent.get(B('a')) == B('b')

    def test_buffers(self):
        _, env = testlib.temp_env()
        txn = env.begin(write=True, buffers=True)
        assert txn.put(B('a'), B('a'))
        b = txn.get(B('a'))
        assert b is not None
        assert len(b) == 1
        assert not isinstance(b, type(B('')))
        txn.commit()

        txn = env.begin(buffers=False)
        b = txn.get(B('a'))
        assert b is not None
        assert len(b) == 1
        assert isinstance(b, type(B('')))
        txn.abort()


class OpenDbTest(unittest.TestCase):
    def test_main(self):
        _, env = testlib.temp_env()
        # Start write txn, so we cause deadlock if open_db attempts txn.
        txn = env.begin(write=True)
        # Now get main DBI, we should already be open.
        db = env.open_db(None)
        # w00t, deadlock did not occur.
        txn.abort()


if __name__ == '__main__':
    unittest.main()
