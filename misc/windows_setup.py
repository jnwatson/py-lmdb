#
# Copyright 2014 The py-lmdb authors, all rights reserved.
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
import os
import urllib

from windows_build import interp_has_module
from windows_build import interp_path
from windows_build import INTERPS
from windows_build import run
from windows_build import run_or_false

EZSETUP_URL = ('https://bitbucket.org/pypa/setuptools'
               '/raw/bootstrap/ez_setup.py')


def ezsetup_path():
    path = os.path.join(os.environ['TEMP'], 'ez_setup.py')
    if not os.path.exists(path):
        fp = urllib.urlopen(EZSETUP_URL)
        with open(path, 'wb') as fp2:
            fp2.write(fp.read())
        fp.close()
    return path


def easy_install_path(interp):
    return os.path.join(os.path.dirname(interp),
                        'scripts', 'easy_install.exe')


def main():
    for interp, is_cffi in INTERPS:
        path = interp_path(interp)
        run_or_false(path, '-m', 'ensurepip')
        if not interp_has_module(path, 'easy_install'):
            run(path, ezsetup_path())
        for pkg in 'pip', 'cffi', 'pytest', 'wheel':
            modname = 'py.test' if pkg == 'pytest' else pkg
            if not interp_has_module(path, modname):
                run(easy_install_path(path), pkg)


if __name__ == '__main__':
    main()
