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
import atexit
import contextlib
import os
import shutil
import stat
import sys
import tempfile

import lmdb


def temp_dir(create=True):
    path = tempfile.mkdtemp(prefix='lmdb_test')
    if not create:
        os.rmdir(path)
    atexit.register(shutil.rmtree, path, ignore_errors=True)
    if hasattr(path, 'decode'):
        path = path.decode(sys.getfilesystemencoding())
    return path


def temp_file(create=True):
    fd, path = tempfile.mkstemp(prefix='lmdb_test')
    os.close(fd)
    if not create:
        os.unlink(path)
    atexit.register(lambda: os.path.exists(path) and os.unlink(path))
    pathlock = path + '-lock'
    atexit.register(lambda: os.path.exists(pathlock) and os.unlink(pathlock))
    if hasattr(path, 'decode'):
        path = path.decode(sys.getfilesystemencoding())
    return path


def temp_env(path=None, max_dbs=10, **kwargs):
    if not path:
        path = temp_dir()
    env = lmdb.open(path, max_dbs=max_dbs, **kwargs)
    return path, env


def path_mode(path):
    return stat.S_IMODE(os.stat(path).st_mode)


# Handle moronic Python >=3.0 <3.3.
UnicodeType = getattr(__builtins__, 'unicode', str)
BytesType = getattr(__builtins__, 'bytes', str)


try:
    INT_TYPES = (int, long)
except NameError:
    INT_TYPES = (int,)

# B(ascii 'string') -> bytes
try:
    bytes('')     # Python>=2.6, alias for str().
    B = lambda s: s
except TypeError: # Python3.x, requires encoding parameter.
    B = lambda s: bytes(s, 'ascii')
except NameError: # Python<=2.5.
    B = lambda s: s

# BL('s1', 's2') -> ['bytes1', 'bytes2']
BL = lambda *args: map(B, args)
# TS('s1', 's2') -> ('bytes1', 'bytes2')
BT = lambda *args: tuple(B(s) for s in args)
# O(int) -> length-1 bytes
O = lambda arg: B(chr(arg))
# OCT(s) -> parse string as octal
OCT = lambda s: int(s, 8)


KEYS = BL('a', 'b', 'baa', 'd')
ITEMS = [(k, B('')) for k in KEYS]
REV_ITEMS = ITEMS[::-1]
VALUES = [B('') for k in KEYS]

def putData(t, db=None):
    for k, v in ITEMS:
        if db:
            t.put(k, v, db=db)
        else:
            t.put(k, v)
