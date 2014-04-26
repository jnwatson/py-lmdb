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
import shutil
import subprocess
import tempfile

INTERPS = (
    ('Python27', False),
    ('Python27-64', False),
    #('Python31', False),
    #('Python31-64', False),
    ('Python32', False),
    ('Python32-64', False),
    ('Python33', False),
    ('Python33-64', False),
    ('Python34', False),
    ('Python34-64', False),
)


def interp_path(interp):
    return r'C:\%s\Python' % (interp,)


def interp_has_module(path, module):
    return run_or_false(path, '-c', 'import ' + module)


def run(*args):
    if os.path.exists('build'):
        shutil.rmtree('build')
    try:
        subprocess.check_call(args)
    except:
        print '!!! COMMAND WAS:', args
        raise


def run_or_false(*args):
    try:
        run(*args)
    except subprocess.CalledProcessError:
        return False
    return True


def main():
    run('git', 'clean', '-dfx', 'dist')
    for interp, is_cffi in INTERPS:
        path = interp_path(interp)
        run('git', 'clean', '-dfx', 'build', 'temp', 'lmdb')
        run(path, '-mpip', 'install', '-e', '.')
        #run(path, '-mpy.test')
        run(path, 'setup.py', 'bdist_egg', 'upload')
        run(path, 'setup.py', 'bdist_wheel', 'upload')


if __name__ == '__main__':
    main()
