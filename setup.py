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
    sys.stderr.write('Error: py-lmdb requires at least CPython 2.5\n')
    raise SystemExit(1)

if sys.version[:3] in ('3.0', '3.1', '3.2'):
    use_cpython = False


#
# Figure out which LMDB implementation to use.
#

if os.getenv('LMDB_INCLUDEDIR'):
    extra_include_dirs = [os.getenv('LMDB_INCLUDEDIR')]
else:
    extra_include_dirs = []

if os.getenv('LMDB_LIBDIR'):
    extra_library_dirs = [os.getenv('LMDB_LIBDIR')]
else:
    extra_library_dirs = []

extra_include_dirs += ['lib/py-lmdb']

if os.getenv('LMDB_FORCE_SYSTEM') is not None:
    print('py-lmdb: Using system version of liblmdb.')
    extra_sources = []
    extra_include_dirs += []
    libraries = ['lmdb']
else:
    print('py-lmdb: Using bundled liblmdb; override with LMDB_FORCE_SYSTEM=1.')
    extra_sources = ['lib/mdb.c', 'lib/midl.c']
    extra_include_dirs += ['lib']
    libraries = []


# distutils perplexingly forces NDEBUG for package code!
extra_compile_args = ['-UNDEBUG']

# Disable some Clang/GCC warnings.
if not os.getenv('LMDB_MAINTAINER'):
    extra_compile_args += ['-w']


# Microsoft Visual Studio 9 ships with neither inttypes.h, stdint.h, or a sane
# definition for ssize_t, so here we add lib/win32 to the search path, which
# contains emulation header files provided by a third party. We force-include
# Python.h everywhere since it has a portable definition of ssize_t, which
# inttypes.h and stdint.h lack, and to avoid having to modify the LMDB source
# code. Advapi32 is needed for LMDB's use of Windows security APIs.
p = sys.version.find('MSC v.')
msvc_ver = int(sys.version[p+6:p+10]) if p != -1 else None

if sys.platform.startswith('win'):
    # If running on Visual Studio<=2010 we must provide <stdint.h>. Newer
    # versions provide it out of the box.
    if msvc_ver and not msvc_ver >= 1600:
        extra_include_dirs += ['lib\\win32-stdint']
    extra_include_dirs += ['lib\\win32']
    extra_compile_args += [r'/FIPython.h']
    libraries += ['Advapi32']


# Capture setup.py configuration for later use by cffi, otherwise the
# configuration may differ, forcing a recompile (and therefore likely compile
# errors). This happens even when `use_cpython` since user might want to
# LMDB_FORCE_CFFI=1 during testing.
with open('lmdb/_config.py', 'w') as fp:
    fp.write('CONFIG = dict(%r)\n\n' % ((
        ('extra_compile_args', extra_compile_args),
        ('extra_sources', extra_sources),
        ('extra_library_dirs', extra_library_dirs),
        ('extra_include_dirs', extra_include_dirs),
        ('libraries', libraries),
    ),))


if use_cpython:
    print('py-lmdb: Using CPython extension; override with LMDB_FORCE_CFFI=1.')
    install_requires = []
    if memsink:
        extra_compile_args += ['-DHAVE_MEMSINK',
                               '-I' + os.path.dirname(memsink.__file__)]
    ext_modules = [Extension(
        name='cpython',
        sources=['lmdb/cpython.c'] + extra_sources,
        extra_compile_args=extra_compile_args,
        libraries=libraries,
        include_dirs=extra_include_dirs,
        library_dirs=extra_library_dirs
    )]
else:
    print('Using cffi extension.')
    install_requires = ['cffi>=0.8']
    try:
        import lmdb.cffi
        ext_modules = [lmdb.cffi._ffi.verifier.get_extension()]
    except ImportError:
        sys.stderr.write('Could not import lmdb; ensure cffi is installed!\n')
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
    description = "Universal Python binding for the LMDB 'Lightning' Database",
    author = 'David Wilson',
    license = 'OpenLDAP BSD',
    url = 'http://github.com/dw/py-lmdb/',
    packages = ['lmdb'],
    classifiers = [
        "Programming Language :: Python",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.6",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.0",
        "Programming Language :: Python :: 3.1",
        "Programming Language :: Python :: 3.2",
        "Programming Language :: Python :: 3.3",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
    ],
    ext_package = 'lmdb',
    ext_modules = ext_modules,
    install_requires = install_requires,
    zip_safe = False
)
