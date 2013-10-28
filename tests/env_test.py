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

import lmdb


class VersionTest(unittest.TestCase):
    def test_version(self):
        ver = lmdb.version()
        assert len(ver) == 3
        assert all(isinstance(i, (int, long)) for i in ver)
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

    def test_subdir_junk(self):
        path = testlib.temp_file()
        fp = file(path, 'w')
        fp.write('A' * 8192)
        fp.close()
        self.assertRaises(lmdb.InvalidError,
            lambda: lmdb.open(path, subdir=False))

    def test_subdir_true(self):
        path = testlib.temp_file()
        os.unlink(path)
        env = testlib.temp_env(path=path, subdir=False)
        assert os.path.exists(path)
        assert os.path.isfile(path)
        assert os.path.isfile(path + '-lock')
        assert not env.flags()['subdir']


class CursorTest(unittest.TestCase):
    pass

if __name__ == '__main__':
    unittest.main()
