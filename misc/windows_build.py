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
import glob
import os
import shutil
import subprocess
import sys
import tempfile

INTERPS = (
    ('Python26', False),
    ('Python26-64', False),
    ('Python27', False),
    ('Python27-64', False),
    #('Python31', False),
    #('Python31-64', False),
    #('Python32', False),
    ('Python32-64', False),
    ('Python33', False),
    ('Python33-64', False),
    ('Python34', False),
    ('Python34-64', False),
    ('Python35', False),
    ('Python35-64', False),
    ('Python36', False),
    ('Python36-64', False),
)


def interp_path(interp):
    return r'C:\%s\Python' % (interp,)

def pip_path(interp):
    return os.path.join(os.path.dirname(interp),
                        'scripts', 'pip.exe')

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
        run(pip_path(path), 'install', '-e', '.')
        if is_cffi:
            os.environ['LMDB_FORCE_CFFI'] = '1'
            os.environ.pop('LMDB_FORCE_CPYTHON', '')
        else:
            os.environ['LMDB_FORCE_CPYTHON'] = '1'
            os.environ.pop('LMDB_FORCE_CFFI', '')
        if os.path.exists('lmdb\\cpython.pyd'):
            os.unlink('lmdb\\cpython.pyd')
        #run(path, '-mpy.test')
        run(path, 'setup.py', 'bdist_egg')
        run(path, 'setup.py', 'bdist_wheel')
    run(sys.executable, '-m', 'twine', 'upload',
        '--skip-existing', *glob.glob('dist/*'))

if __name__ == '__main__':
    main()
