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

"""
cffi wrapper for OpenLDAP's "Lightning" MDB database.

Please see https://lmdb.readthedocs.io/
"""

import os
import sys

def _reading_docs():
    # Hack: disable speedups while testing or reading docstrings.
    # Don't check for basename for embedded python - variable 'argv' does not exists there.
    if not(hasattr(sys, 'argv')):
        return False

    basename = os.path.basename(sys.argv[0])
    return any(x in basename for x in ('sphinx-build', 'pydoc'))

try:
    if _reading_docs() or os.getenv('LMDB_FORCE_CFFI') is not None:
        raise ImportError
    from lmdb.cpython import *
    from lmdb.cpython import open
    from lmdb.cpython import __all__
except ImportError:
    from lmdb.cffi import *
    from lmdb.cffi import open
    from lmdb.cffi import __all__
    from lmdb.cffi import __doc__

__version__ = '0.91'

# Hack to support Python v2.5 'python -mlmdb'
if __name__ == '__main__':
    import lmdb.tool
    import atexit
    atexit.register(lmdb.tool.main)
