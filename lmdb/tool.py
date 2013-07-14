"""
Basic tools for working with LMDB.

    dump: Dump one or more databases to disk in 'cdbmake' format.
        Usage: dump [<db1> [<dbN> [..]]]

        If no databases are given, dumps the main database.

    restore: Read one or more database from disk in 'cdbmake' format.
        %prog restore <-e ...>

    copy: Consistent high speed backup an environment.

    get: Read one or more values from a database.
        %prog get [<key1> [<keyN> [..]]]

    edit: Add/delete/replace values from a database.
        %prog edit --set key=value --set-file key=/path \\
                   --add key=value --add-file key=/path/to/file \\
                   --delete key
"""

from __future__ import absolute_import
import contextlib
import optparse
import string
import sys

# Python3.x bikeshedded trechery.
try:
    from io import BytesIO as StringIO
except ImportError:
    try:
        from cStringIO import StringIO
    except ImportError:
        from StringIO import StringIO

import lmdb


ENV = None
DB = None


def isprint(c):
    """Return ``True`` if the character `c` can be printed visibly and without
    adversely affecting printing position (e.g. newline)."""
    return c in string.printable and ord(c) > 16


def xxd(s):
    """Return a vaguely /usr/bin/xxd formatted representation of the bytestring
    `s`."""
    sio = StringIO.StringIO()
    for idx, ch in enumerate(s):
        if not (idx % 16):
            if idx:
                sio.write('  ')
                sio.write(pr)
                sio.write('\n')
            sio.write('%07x:' % idx)
            pr = ''
        if not (idx % 2):
            sio.write(' ')
        sio.write('%02x' % (ord(ch),))
        pr += ch if isprint(ch) else '.'

    if idx % 16:
        need = 15 - (idx % 16)
        # fill remainder of last line.
        sio.write('  ' * need)
        sio.write(' ' * (need / 2))
        sio.write('  ')
        sio.write(pr)

    sio.write('\n')
    return sio.getvalue()


def make_parser():
    parser = optparse.OptionParser()
    parser.prog = 'python -mlmdb.tool'
    parser.usage = '%prog [options] <command>\n' + __doc__.rstrip()
    parser.add_option('-e', '--env', help='Environment file to open')
    parser.add_option('-d', '--db', help='Database to open (default: main)')
    parser.add_option('-r', '--read', help='Open environment read-only')
    parser.add_option('-S', '--map_size', type='int', default='10',
                      help='Map size in megabytes (default: 10)')
    parser.add_option('-a', '--all', action='store_true',
                      help='Make "dump" dump all databases')
    parser.add_option('-T', '--txn_size', type='int', default=1000,
                     help='Writes per transaction (default: 1000)')
    parser.add_option('-E', '--target_env',
                     help='Target environment file for "dumpfd"')
    parser.add_option('-x', '--xxd', help='Print values in xxd format')

    group = parser.add_option_group('Options for "edit" command')
    group.add_option('--set', action='append',
                     help='List of key=value pairs to set.')
    group.add_option('--set-file', action='append',
                     help='List of key pairs to read from files.')
    group.add_option('--add', action='append',
                     help='List of key=value pairs to add.')
    group.add_option('--add-file', action='append',
                     help='List of key pairs to read from files.')
    group.add_option('--delete', action='append',
                     help='List of key=value pairs to delete.')

    return parser


def die(fmt, *args):
    if args:
        fmt %= args
    sys.stderr.write('lmdb.tool: %s\n' % (fmt,))
    raise SystemExit(1)


def cdbdump_cursor_to_fp(cursor, fp):
    for key, value in cursor:
        fp.write('+%d,%d:' % (len(key), len(value)))
        fp.write(key)
        fp.write('->')
        fp.write(value)
        fp.write('\n')


def cmd_dump(opts, args):
    args = args or [None]
    dbs = dict((name, ENV.open_db(name)) for name in args)

    with ENV.begin(buffers=True) as txn:
        for arg in args:
            path = '%s.cdbmake' % ((arg or 'main'),)
            with file(path, 'w', 1048576) as fp:
                print 'Dumping to %r...' % (path,)
                cursor = txn.cursor(db=dbs[arg])
                cdbdump_cursor_to_fp(cursor, fp)


def cmd_restore(opts, args):
    read = self.stdin.read
    rec_nr = 0

    while True:
        rec_nr += 1
        plus = read(1)
        if plus == '\n':
            return
        elif plus != '+':
            self.die('bad or missing plus, line/record #%d', rec_nr)

        bad = False
        try:
            klen = int(''.join(iter(partial(read, 1), ',')), 10)
            dlen = int(''.join(iter(partial(read, 1), ':')), 10)
        except ValueError, e:
            self.die('bad or missing length, line/record #%d', rec_nr)

        key = read(klen)
        if read(2) != '->':
            self.die('bad or missing separator, line/record #%d', rec_nr)

        data = read(dlen)
        if (len(key) + len(data)) != (klen + dlen):
            self.die('short key or data, line/record #%d', rec_nr)

        if read(1) != '\n':
            self.die('bad line ending, line/record #%d', rec_nr)

        self.writer.put(key, data)


def cmd_edit(opts, args):
    if args:
        die('Edit command only takes options, not arguments (see --help)')

    with ENV.begin(write=True) as txn:
        cursor = txn.cursor(db=DB)
        for elem in opts.add or []:
            key, _, value = elem.partition('=')
            cursor.put(key, value, overwrite=False)

        for elem in opts.set or []:
            key, _, value = elem.partition('=')
            cursor.put(key, value)



def main():
    parser = make_parser()
    opts, args = parser.parse_args()

    if not args:
        die('Please specify a command (see --help)')
    if not opts.env:
        die('Please specify environment (--env)')

    global ENV
    ENV = lmdb.open(opts.env, map_size=opts.map_size*1048576)

    if opts.db:
        global DB
        DB = ENV.open_db(opts.db)

    func = globals().get('cmd_' + args[0])
    if not func:
        die('No such command: %r' % (args[0],))

    func(opts, args[1:])


if __name__ == '__main__':
    main()
