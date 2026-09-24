#
# Copyright 2013-2026 The py-lmdb authors, all rights reserved.
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

"""
Identity of an environment's files, for the "already open in this process"
guard shared by the CPython and CFFI implementations.

LMDB forbids opening the same environment twice in one process: its locks
are POSIX fcntl() locks, which belong to the (process, file) pair, so a
second open silently shares the first one's locks and closing it releases
them.  What must not be opened twice is therefore a *file*, not a path.

Keying on the path got this wrong in both directions.  It refused an
environment that had been deleted and re-created at the same path while
the old one was still open, although the new files are unrelated to the
old ones (issue #491).  And it missed the same files reached through two
different paths, such as a hard link to a ``subdir=False`` data file.

So an environment is identified by the ``(st_dev, st_ino)`` of its data
file and of its lock file.  Both matter: deleting only the data file and
re-creating the environment would leave the new one sharing the old lock
file, whose reader table and fcntl locks belong to the environment still
open.  Where a filesystem reports no inode number, as some do on Windows,
the resolved path stands in for it.
"""

import os


def _file_key(path):
    try:
        st = os.stat(path)
    except OSError:
        # Missing (not created yet, or deleted): no open environment can be
        # using a file that is not there.  Any other failure will resurface
        # from mdb_env_open.
        return None
    if st.st_ino:
        return ('ino', st.st_dev, st.st_ino)
    return ('path', os.path.normcase(os.path.realpath(path)))


def env_keys(path, subdir):
    """Return a tuple of hashable keys identifying the files of the
    environment at `path`.  Files that do not exist yet contribute no key.
    """
    path = os.fsdecode(path)
    if subdir:
        names = (os.path.join(path, 'data.mdb'),
                 os.path.join(path, 'lock.mdb'))
    else:
        names = (path, path + '-lock')
    return tuple(k for k in map(_file_key, names) if k is not None)
