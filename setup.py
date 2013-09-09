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

from __future__ import absolute_import
from __future__ import with_statement

import os
import sys
import platform

from setuptools import Extension
from setuptools import setup

try:
    import memsink
except ImportError:
    memsink = None


if hasattr(platform, 'python_implementation'):
    use_cpython = platform.python_implementation() == 'CPython'
else:
    use_cpython = True

if os.getenv('LMDB_FORCE_CFFI') is not None:
    use_cpython = False

if sys.version[:3] < '2.5':
    print >> sys.stderr, 'Error: py-lmdb requires at least CPython 2.5'
    raise SystemExit(1)

if sys.version[:3] in ('3.0', '3.1', '3.2'):
    use_cpython = False

if use_cpython:
    print('Using custom CPython extension; set LMDB_FORCE_CFFI=1 to override.')
    install_requires = []
    extra_compile_args = ['-Wno-shorten-64-to-32']
    if memsink:
        extra_compile_args += ['-DHAVE_MEMSINK',
                               '-I' + os.path.dirname(memsink.__file__)]
    ext_modules = [Extension(
        name='cpython',
        sources=['lmdb/cpython.c', 'lib/mdb.c', 'lib/midl.c'],
        extra_compile_args=extra_compile_args,
        include_dirs=['lib']
    )]
else:
    print('Using cffi extension.')
    install_requires = ['cffi']
    try:
        import lmdb.cffi
        ext_modules = [lmdb.cffi._ffi.verifier.get_extension()]
    except ImportError:
        print >> sys.stderr, 'Could not import lmdb; ensure "cffi" is installed!'
        ext_modules = []

def grep_version():
    path = os.path.join(os.path.dirname(__file__), 'lmdb/__init__.py')
    with open(path) as fp:
        for line in fp:
            if line.startswith('__version__'):
                return eval(line.split()[-1])

setup(
    name = 'lmdb',
    version = grep_version(),
    description = "cffi/CPython native wrapper for OpenLDAP MDB 'Lightning Database' library",
    author = 'David Wilson',
    license = 'OpenLDAP BSD',
    url = 'http://github.com/dw/py-lmdb/',
    packages = ['lmdb'],
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: 3",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
    ],
    ext_package = 'lmdb',
    ext_modules = ext_modules,
    install_requires = install_requires,
    zip_safe = False
)
