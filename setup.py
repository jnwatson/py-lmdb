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

import os
import sys
import platform

from setuptools import setup, Extension


use_cpython = platform.python_implementation() == 'CPython'
if os.getenv('LMDB_FORCE_CFFI') is not None:
    use_cpython = False


if use_cpython:
    print 'Using custom CPython extension; set LMDB_FORCE_CFFI=1 to override.'
    install_requires = []
    ext_modules = [Extension(
        name='lmdb.cpython',
        sources=['lmdb/cpython.c', 'lib/mdb.c', 'lib/midl.c'],
        extra_compile_args=['-Wno-shorten-64-to-32'],
        include_dirs=['lib']
    )]
else:
    print 'Using cffi extension.'
    install_requires = ['cffi']
    try:
        import lmdb.cffi
        ext_modules = [lmdb.cffi._ffi.verifier.get_extension()]
    except ImportError:
        print >> sys.stderr, 'Could not import lmdb; ensure "cffi" is installed!'
        ext_modules = []

setup(
    name = 'lmdb',
    version = '0.56',
    description = "cffi/CPython native wrapper for OpenLDAP MDB 'Lightning Database' library",
    author = 'David Wilson',
    license = 'OpenLDAP BSD',
    url = 'http://github.com/dw/py-lmdb/',
    packages = ['lmdb'],
    ext_package = 'lmdb',
    ext_modules = ext_modules,
    install_requires = install_requires,
    zip_safe = False
)
