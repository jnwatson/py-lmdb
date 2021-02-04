#
# Copyright 2013-2020 The py-lmdb authors, all rights reserved.
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
import shutil
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

if sys.version[:3] < '2.7' or (3, 0) < sys.version_info[:2] < (3, 4):
    sys.stderr.write('Error: py-lmdb requires at least CPython 2.7 or 3.4\n')
    raise SystemExit(1)

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
extra_compile_args = []

patch_lmdb_source = False

if os.getenv('LMDB_FORCE_SYSTEM') is not None:
    print('py-lmdb: Using system version of liblmdb.')
    extra_sources = []
    extra_include_dirs += []
    libraries = ['lmdb']
elif os.getenv('LMDB_PURE') is not None:
    print('py-lmdb: Using bundled unmodified liblmdb; override with LMDB_FORCE_SYSTEM=1.')
    extra_sources = ['lib/mdb.c', 'lib/midl.c']
    extra_include_dirs += ['lib']
    libraries = []
else:
    print('py-lmdb: Using bundled liblmdb with py-lmdb patches; override with LMDB_FORCE_SYSTEM=1 or LMDB_PURE=1.')
    extra_sources = ['build/lib/mdb.c', 'build/lib/midl.c']
    extra_include_dirs += ['build/lib']
    extra_compile_args += ['-DHAVE_PATCHED_LMDB=1']
    libraries = []
    patch_lmdb_source = True

if patch_lmdb_source:
    if sys.platform.startswith('win'):
        try:
            import patch_ng as patch
        except ImportError:
            raise Exception('Building py-lmdb from source on Windows requires the "patch-ng" python module.')

    # Clean out any previously patched files
    dest = 'build' + os.sep + 'lib'
    try:
        os.mkdir('build')
    except Exception:
        pass

    try:
        shutil.rmtree(dest)
    except Exception:
        pass
    shutil.copytree('lib', dest)

    # Copy away the lmdb source then patch it
    if sys.platform.startswith('win'):
        patchfile = 'lib' + os.sep + 'py-lmdb' + os.sep + 'env-copy-txn.patch'
        patchset = patch.fromfile(patchfile)
        rv = patchset.apply(3, root=dest)
        if not rv:
            raise Exception('Applying patch failed')
    else:
        rv = os.system('/usr/bin/patch -N -p3 -d build/lib < lib/py-lmdb/env-copy-txn.patch')
        if rv:
            raise Exception('Applying patch failed')

# distutils perplexingly forces NDEBUG for package code!
extra_compile_args += ['-UNDEBUG']

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
msvc_ver = int(sys.version[p + 6: p + 10]) if p != -1 else None

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
    name='lmdb',
    version=grep_version(),
    description="Universal Python binding for the LMDB 'Lightning' Database",
    long_description="Universal Python binding for the LMDB 'Lightning' Database",
    long_description_content_type="text/plain",
    author='David Wilson',
    maintainer='Nic Watson',
    license='OpenLDAP BSD',
    url='http://github.com/jnwatson/py-lmdb/',
    packages=['lmdb'],

    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Programming Language :: Python :: 3.6",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
    ],
    ext_package='lmdb',
    ext_modules=ext_modules,
    install_requires=install_requires,
)
