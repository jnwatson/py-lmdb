# Copyright 2013 The Python-lmdb authors, all rights reserved.
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

from distutils.core import setup
from distutils.extension import Extension

# Install Cython's builder if available, otherwise use the pre-generated C file
# in the repository.
try:
    import Cython.Distutils
    kwargs = dict(cmdclass={
        'build_ext': Cython.Distutils.build_ext
    })
    mod_filename = 'lmdb.pyx'
except ImportError:
    kwargs = {}
    mod_filename = 'lmdb.c'

ext_modules = [
    Extension("lmdb",
        sources=[mod_filename, "lib/mdb.c", "lib/midl.c"],
        include_dirs=['lib'],
    )
]

setup(
    name = 'lmdb',
    version = '0.5',
    description = "Python wrapper for OpenLDAP MDB 'Lightning Database' B-tree library",
    author = 'David Wilson',
    license = 'OpenLDAP BSD',
    url = 'http://github.com/dw/py-lmdb/',
    ext_modules = ext_modules,
    **kwargs
)
