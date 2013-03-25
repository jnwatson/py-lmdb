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

import sys
from setuptools import setup

try:
    import lmdb.cffi
    ext_modules = [lmdb.cffi._ffi.verifier.get_extension()]
except ImportError:
    print >> sys.stderr, 'Could not import lmdb; ensure "cffi" is installed!'
    ext_modules = []

setup(
    name = 'lmdb',
    version = '0.52',
    description = "CFFI wrapper for OpenLDAP MDB 'Lightning Database' B-tree library",
    author = 'David Wilson',
    license = 'OpenLDAP BSD',
    url = 'http://github.com/dw/py-lmdb/',
    py_modules = ['lmdb'],
    ext_modules = ext_modules,
    install_requires = ['cffi'],
    zip_safe = False
)
