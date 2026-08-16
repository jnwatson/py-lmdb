#
# Copyright 2026 The py-lmdb authors, all rights reserved.
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

"""Tests for dual-engine (LMDB 0.9.x + 1.0.x) support."""

import os
import struct
import unittest

import lmdb

import testlib
from testlib import B


def _env_reg(env):
    testlib._cleanups.append(env.close)
    return env


def _engine_majors():
    """Return the set of LMDB major versions available in this build."""
    majors = set()
    for major in (0, 1):
        try:
            lmdb.version(lib_version=major)
            majors.add(major)
        except lmdb.Error:
            pass
    return majors

MAJORS = _engine_majors()
DUAL = MAJORS == {0, 1}


class VersionReportingTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_default_version(self):
        # version() reports the engine used for new environments.
        assert lmdb.version() == lmdb.version(lib_version=min(MAJORS))

    @unittest.skipUnless(DUAL, 'requires both bundled engines')
    def test_both_engines_report(self):
        assert lmdb.version(lib_version=0)[:1] == (0,)
        assert lmdb.version(lib_version=1)[:1] == (1,)
        assert lmdb.version(lib_version=1) > lmdb.version(lib_version=0)

    def test_missing_engine_raises(self):
        self.assertRaises(lmdb.Error, lmdb.version, lib_version=7)

    def test_env_lib_version(self):
        _, env = testlib.temp_env()
        assert env.lib_version() == lmdb.version()


@unittest.skipUnless(DUAL, 'requires both bundled engines')
class EngineSelectionTest(unittest.TestCase):
    def tearDown(self):
        testlib.cleanup()

    def test_new_env_default_engine(self):
        _, env = testlib.temp_env()
        assert env.lib_version()[0] == 0

    def test_new_env_explicit_v1(self):
        path = testlib.temp_dir()
        env = lmdb.open(path, lib_version=1)
        _env_reg(env)
        assert env.lib_version() == lmdb.version(lib_version=1)

    def test_sniff_after_explicit_create(self):
        # An env created with the non-default engine must reopen with the
        # same engine automatically.
        for major in (0, 1):
            path = testlib.temp_dir()
            env = lmdb.open(path, lib_version=major)
            with env.begin(write=True) as txn:
                txn.put(B('k'), B('v'))
            env.close()

            env = lmdb.open(path)
            _env_reg(env)
            assert env.lib_version()[0] == major, major
            with env.begin() as txn:
                assert txn.get(B('k')) == B('v')

    def test_sniff_nosubdir(self):
        path = testlib.temp_dir() + os.sep + 'data'
        env = lmdb.open(path, subdir=False, lib_version=1)
        with env.begin(write=True) as txn:
            txn.put(B('k'), B('v'))
        env.close()

        env = lmdb.open(path, subdir=False)
        _env_reg(env)
        assert env.lib_version()[0] == 1

    def test_explicit_mismatch_rejected(self):
        # Forcing the wrong engine onto an existing file must surface LMDB's
        # own rejection, not silently convert the database.  1.0 reports
        # MDB_INVALID for a v1 file (the meta page moved, so the magic is
        # not where it looks for it).
        path = testlib.temp_dir()
        env = lmdb.open(path, lib_version=0)
        with env.begin(write=True) as txn:
            txn.put(B('k'), B('v'))
        env.close()

        self.assertRaises((lmdb.VersionMismatchError, lmdb.InvalidError),
                          lmdb.open, path, lib_version=1)

    def test_data_version_2_rejected(self):
        # lmdb-js's default build writes MDB_DATA_VERSION 2 (a pre-release
        # mdb.master3 snapshot); neither released line reads it.
        path = testlib.temp_dir()
        # 0.9-shaped page 0 on 64-bit: 16-byte header, then meta.
        data = bytearray(64)
        struct.pack_into('=II', data, 16, 0xBEEFC0DE, 2)
        with open(path + os.sep + 'data.mdb', 'wb') as fp:
            fp.write(bytes(data))

        try:
            env = lmdb.open(path)
            _env_reg(env)
            assert False, 'expected lmdb.Error'
        except lmdb.Error as e:
            assert 'v2' in str(e)

    def test_concurrent_engines(self):
        env0 = lmdb.open(testlib.temp_dir(), lib_version=0)
        env1 = lmdb.open(testlib.temp_dir(), lib_version=1)
        _env_reg(env0)
        _env_reg(env1)
        with env0.begin(write=True) as t0, env1.begin(write=True) as t1:
            t0.put(B('a'), B('0'))
            t1.put(B('a'), B('1'))
        with env0.begin() as t0, env1.begin() as t1:
            assert t0.get(B('a')) == B('0')
            assert t1.get(B('a')) == B('1')

    def test_v1_readonly_error_class(self):
        # 1.0 returns MDB_IS_READONLY where 0.9 returned EACCES; both must
        # surface as lmdb.ReadonlyError.
        path = testlib.temp_dir()
        env = lmdb.open(path, lib_version=1)
        _env_reg(env)
        txn = lmdb.Transaction(env)
        try:
            self.assertRaises(lmdb.ReadonlyError,
                              txn.put, B('k'), B('v'))
        finally:
            txn.abort()

    def test_v1_named_dbs(self):
        path = testlib.temp_dir()
        env = lmdb.open(path, lib_version=1, max_dbs=4)
        _env_reg(env)
        db1 = env.open_db(B('db1'))
        with env.begin(write=True, db=db1) as txn:
            txn.put(B('k'), B('v'))
        with env.begin(db=db1) as txn:
            assert txn.get(B('k')) == B('v')


if __name__ == '__main__':
    unittest.main()
