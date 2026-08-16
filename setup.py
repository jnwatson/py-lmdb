#
# Copyright 2013-2024 The py-lmdb authors, all rights reserved.
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

if (3, 0) < sys.version_info[:2] < (3, 5):
    sys.stderr.write('Error: py-lmdb requires at CPython 3.5\n')
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

extra_compile_args = []

# Absolute, so the paths recorded in lmdb/_config.py stay valid regardless of
# the working directory.  cffi's verify() builds from its own temporary
# directory on some implementations (notably PyPy), where relative source
# paths would not resolve.
HERE = os.path.dirname(os.path.abspath(__file__))

extra_include_dirs += [os.path.join(HERE, 'lib', 'py-lmdb'),
                       os.path.join(HERE, 'lmdb')]

#
# py-lmdb bundles two binary-incompatible LMDB versions (0.9.x, data format
# v1, in lib/; 1.0.x, data format v3, in lib1/) and links both into one
# extension module.  Each tree is copied to build/, gets its symbol-rename
# header prepended (so the two trees' symbols cannot collide), and an
# engine.c glue TU per tree exports that tree's entry points as an MdbApi
# vtable.  The binding picks an engine per Environment at runtime.
#
# LMDB_FORCE_SYSTEM instead builds a single engine against the system
# liblmdb (whichever version the system ships).
#

# (name, source tree, build dir, engine -D flag).
# Patches are applied in order; each patch's line numbers must
# reflect the state after all preceding patches.
ENGINES = [
    dict(
        name='v09',
        tree='lib',
        dest=os.path.join(HERE, 'build', 'lib09'),
        define='LMDB_ENGINE_V09',
        patch_names=[
            'env-copy-txn',
            'cursor-next-prev-uninitialized',
            'cve-2019-16224-validate-db-flags',
            'cve-2019-16225-reject-dirty-pages',
            'cve-2019-16226-validate-node-del-size',
            'cve-2019-16227-guard-xcursor-null',
            'cve-2019-16228-validate-psize',
            'validate-page-bounds',
            'validate-node-read-size',
            'validate-subpage-bounds',
            'validate-xcursor-nodedsz',
            'validate-leaf2-keysize',
            'guard-xcursor-null-d3d4',
            'validate-nodedsz-page-split',
            'validate-node-shrink-delta',
            'validate-overflow-pages',
            'validate-nodedsz-cursor-put',
            'validate-md-depth',
            'validate-md-root',
            'win32-sparse-file',
            'fix-large-write',
            'fix-win-flush-large-write',
            'fix-overflow-page-size-mul',
        ],
    ),
    dict(
        name='v10',
        tree='lib1',
        dest=os.path.join(HERE, 'build', 'lib10'),
        define='LMDB_ENGINE_V10',
        # The 1.0 series omits four patches carried for 0.9: the two
        # large-write fixes landed upstream (ITS#10054, ITS#10538),
        # win32-sparse-file's defect was designed away by 1.0's incremental
        # file growth, and cve-2019-16225 keys on the P_DIRTY page flag,
        # which no longer exists.  See lib1/py-lmdb/PATCH-STATUS.md.
        patch_names=[
            'env-copy-txn',
            'cursor-next-prev-uninitialized',
            'cve-2019-16224-validate-db-flags',
            'cve-2019-16226-validate-node-del-size',
            'cve-2019-16227-guard-xcursor-null',
            'cve-2019-16228-validate-psize',
            'validate-page-bounds',
            'validate-node-read-size',
            'validate-subpage-bounds',
            'validate-xcursor-nodedsz',
            'validate-leaf2-keysize',
            'guard-xcursor-null-d3d4',
            'validate-nodedsz-page-split',
            'validate-node-shrink-delta',
            'validate-overflow-pages',
            'validate-nodedsz-cursor-put',
            'validate-md-depth',
            'validate-md-root',
            'fix-overflow-page-size-mul',
        ],
    ),
]


def prepare_engine_tree(engine, apply_patches):
    """Copy an LMDB source tree into its build directories and optionally
    apply the py-lmdb patches.

    Two copies are made: <dest>-plain is the (optionally patched) tree as-is,
    used by the cffi implementation, whose per-engine verifier modules are
    separate shared objects and therefore need no symbol renaming.  <dest> is
    the same tree with the symbol-rename header prepended to every C
    translation unit plus the engine.c vtable glue; the CPython extension
    links both engines' <dest> trees into one module.
    """
    tree = os.path.join(HERE, engine['tree'])
    dest = engine['dest']
    plain = dest + '-plain'

    try:
        os.makedirs(os.path.join(HERE, 'build'))
    except Exception:
        pass
    for d in (dest, plain):
        try:
            shutil.rmtree(d)
        except Exception:
            pass
    shutil.copytree(tree, plain)

    if apply_patches:
        if sys.platform.startswith('win'):
            for name in engine['patch_names']:
                patchfile = tree + '\\py-lmdb\\' + name + '.patch'
                patchset = patch.fromfile(patchfile)
                if not patchset:
                    raise Exception('Parsing patch failed: ' + patchfile)
                if not patchset.apply(2, root=plain):
                    raise Exception('Applying patch failed: ' + patchfile)
        else:
            for name in engine['patch_names']:
                patchfile = tree + '/py-lmdb/' + name + '.patch'
                rv = os.system('patch -N -p3 -d ' + plain + ' < ' + patchfile)
                if rv:
                    raise Exception('Applying patch failed: ' + patchfile)

    shutil.copytree(plain, dest)
    shutil.copy(os.path.join(tree, 'py-lmdb', 'rename.h'),
                os.path.join(dest, 'lmdb_rename.h'))
    shutil.copy(os.path.join(HERE, 'lmdb', 'engine.c'),
                os.path.join(dest, 'engine.c'))

    for fname in ('mdb.c', 'midl.c', 'engine.c'):
        path = os.path.join(dest, fname)
        with open(path, 'r') as fp:
            source = fp.read()
        with open(path, 'w') as fp:
            fp.write('#include "lmdb_rename.h"\n')
            fp.write(source)


use_bundled_lmdb = os.getenv('LMDB_FORCE_SYSTEM') is None
apply_patches = use_bundled_lmdb and os.getenv('LMDB_PURE') is None

# Per-engine compiled sources + include dir, consumed both by the CPython
# extension below and (via _config.py) by the cffi implementation.
engine_specs = []

if not use_bundled_lmdb:
    print('py-lmdb: Using system version of liblmdb (single engine).')
    # engine.c provides the lmdb_api_sys vtable for the CPython extension;
    # the cffi implementation talks to the system library directly.
    extra_sources = [os.path.join(HERE, 'lmdb', 'engine.c')]
    libraries = ['lmdb']
    extra_compile_args += ['-DLMDB_ENGINE_SYS=1']
    engine_specs.append(dict(
        name='sys',
        sources=[],
        include_dirs=[],
        define='LMDB_ENGINE_SYS',
    ))
else:
    if apply_patches:
        print('py-lmdb: Using bundled liblmdb with py-lmdb patches; override with LMDB_FORCE_SYSTEM=1 or LMDB_PURE=1.')
        extra_compile_args += ['-DHAVE_PATCHED_LMDB=1']
    else:
        print('py-lmdb: Using bundled unmodified liblmdb; override with LMDB_FORCE_SYSTEM=1.')

    if sys.platform.startswith('win'):
        try:
            import patch_ng as patch
        except ImportError:
            raise Exception('Building py-lmdb from source on Windows requires the "patch-ng" python module.')

    # Clean the CFFI verify() cache so it recompiles against the freshly
    # patched sources.  verify() hashes the cdef/csource strings and compile
    # args but NOT the content of extra_sources, so a stale .so can persist
    # even after the LMDB source changes.
    _cffi_cache = os.path.join('lmdb', '__pycache__')
    if os.path.isdir(_cffi_cache):
        for _f in os.listdir(_cffi_cache):
            if _f.startswith('lmdb_cffi'):
                try:
                    os.remove(os.path.join(_cffi_cache, _f))
                except OSError:
                    pass  # On Windows, .pyd may be locked by a running process

    extra_sources = []
    libraries = []
    for engine in ENGINES:
        prepare_engine_tree(engine, apply_patches)
        dest = engine['dest']
        plain = dest + '-plain'
        sources = [os.path.join(dest, 'mdb.c'),
                   os.path.join(dest, 'midl.c'),
                   os.path.join(dest, 'engine.c')]
        extra_sources += sources
        extra_compile_args += ['-D%s=1' % engine['define']]
        engine_specs.append(dict(
            name=engine['name'],
            sources=[os.path.join(plain, 'mdb.c'),
                     os.path.join(plain, 'midl.c')],
            include_dirs=[plain],
            define=engine['define'],
        ))

    # cpython.c and the cffi csource include "lmdb.h" for types and
    # constants: use the newest bundled header (a superset of 0.9's API).
    # Each engine TU picks up its own tree's header via quoted-include
    # resolution in its build directory, so only the v10 dir may appear on
    # the global include path.
    extra_include_dirs += [ENGINES[-1]['dest']]

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
        extra_include_dirs += [os.path.join(HERE, 'lib', 'win32-stdint')]
    extra_include_dirs += [os.path.join(HERE, 'lib', 'win32')]
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
        # Per-engine unrenamed sources for the cffi implementation, which
        # builds one verifier module per bundled LMDB version.
        ('engines', engine_specs),
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
    install_requires = ['cffi>=0.8; implementation_name=="cpython"']
    print('Using cffi, building extension module.')
    # Ensure the source directory is on sys.path so that `import lmdb.cffi`
    # works in build-isolated environments where pip runs setup.py from a
    # temporary directory.
    _source_dir = os.path.dirname(os.path.abspath(__file__))
    if _source_dir not in sys.path:
        sys.path.insert(0, _source_dir)
    try:
        import lmdb.cffi
        # One compiled verifier module per LMDB engine.
        ext_modules = list(lmdb.cffi._verifier_extensions)
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
    license='OLDAP-2.8',
    url='http://github.com/jnwatson/py-lmdb/',
    packages=['lmdb'],
    package_data={'lmdb': ['py.typed', '*.pyi']},

    classifiers=[
        "Programming Language :: Python",
        "Programming Language :: Python :: Implementation :: CPython",
        "Programming Language :: Python :: Implementation :: PyPy",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Programming Language :: Python :: 3.13",
        "Programming Language :: Python :: 3.14",
        "Topic :: Database",
        "Topic :: Database :: Database Engines/Servers",
    ],
    python_requires='>=3.9',
    ext_package='lmdb',
    ext_modules=ext_modules,
    install_requires=install_requires,
)
