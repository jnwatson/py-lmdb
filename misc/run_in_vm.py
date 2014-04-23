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
import atexit
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile


def run(*args):
    if os.path.exists('build'):
        shutil.rmtree('build')
    try:
        subprocess.check_call(args)
    except:
        print '!!! COMMAND WAS:', args
        raise


def qmp_write(fp, o):
    buf = json.dumps(o) + '\n'
    fp.write(buf.replace('{', '{ '))


def qmp_read(fp):
    s = fp.readline()
    return json.loads(s)


def qmp_say_hello(fp):
    assert 'QMP' in qmp_read(fp)
    qmp_write(fp, {'execute': 'qmp_capabilities'})
    assert qmp_read(fp)['return'] == {}


def qmp_command(fp, name, args):
    qmp_write(fp, {'execute': name, 'arguments': args})
    while True:
        o = qmp_read(fp)
        if 'return' not in o:
            print 'skip', o
            continue
        print 'cmd out', o
        return o['return']


def qmp_monitor(fp, cmd):
    return qmp_command(fp, 'human-monitor-command', {
        'command-line': cmd
    })


def main():
    vm = sys.argv[1]
    cmdline = sys.argv[2:]

    rsock, wsock = socket.socketpair()
    rfp = rsock.makefile('r+b', 1)

    qemu_path = '/usr/local/bin/qemu-system-x86_64'
    qemu_args = ['sudo', qemu_path, '-enable-kvm', '-m', '1024',
                 '-qmp', 'stdio', '-nographic', '-S',
                 '-vnc', '127.0.0.1:0',
                 '-net', 'user,hostfwd=tcp:127.0.0.1:9422-:22',
                 '-net', 'nic,model=virtio',
                 '-drive', 'file=%s,if=virtio' % (vm,)]
    print ' '.join(qemu_args).replace('qmp', 'monitor')
    exit()
    proc = subprocess.Popen(qemu_args,
        stdin=wsock.fileno(), stdout=wsock.fileno()
        )

    qmp_say_hello(rfp)
    assert '' == qmp_monitor(rfp, 'loadvm 1')
    assert '' == qmp_monitor(rfp, 'cont')
    import time
    time.sleep(100)
    qmp_monitor(rfp, 'quit')

if __name__ == '__main__':
    main()
