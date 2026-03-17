#
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
#

from __future__ import absolute_import

import os
import shlex
import sys
import unittest
from io import BytesIO

import lmdb
import lmdb.tool
import testlib


def call_tool(cmdline):
    if sys.platform == 'win32':
        args = cmdline.split()
    else:
        args = shlex.split(cmdline)
    try:
        return lmdb.tool.main(args)
    finally:
        if lmdb.tool.ENV is not None:
            lmdb.tool.ENV.close()
            lmdb.tool.ENV = None
        lmdb.tool.DB = None


# ---------------------------------------------------------------------------
# Unit tests for helper functions
# ---------------------------------------------------------------------------

class ToBytes(testlib.LmdbTest):
    def test_str(self):
        self.assertEqual(lmdb.tool._to_bytes('hello'), b'hello')

    def test_int(self):
        self.assertEqual(lmdb.tool._to_bytes(42), b'42')

    def test_empty(self):
        self.assertEqual(lmdb.tool._to_bytes(''), b'')

    def test_unicode(self):
        self.assertEqual(lmdb.tool._to_bytes('\u00e9'), '\u00e9'.encode('utf-8'))


class IsprintTest(testlib.LmdbTest):
    def test_letter(self):
        self.assertTrue(lmdb.tool.isprint('A'))

    def test_space(self):
        self.assertTrue(lmdb.tool.isprint(' '))

    def test_newline(self):
        self.assertFalse(lmdb.tool.isprint('\n'))

    def test_null(self):
        self.assertFalse(lmdb.tool.isprint('\x00'))

    def test_tab(self):
        self.assertFalse(lmdb.tool.isprint('\t'))


class XxdTest(testlib.LmdbTest):
    def test_empty(self):
        result = lmdb.tool.xxd(b'')
        self.assertEqual(result, '\n')

    def test_short(self):
        result = lmdb.tool.xxd(b'AB')
        self.assertIn('0000000:', result)
        self.assertIn('4142', result)

    def test_16_bytes(self):
        result = lmdb.tool.xxd(b'ABCDEFGHIJKLMNOP')
        # Should have exactly one full line (16 bytes)
        self.assertIn('0000000:', result)
        # Printable chars should appear at the end of the line
        self.assertIn('ABCDEFGHIJKLMNOP', result)

    def test_17_bytes(self):
        result = lmdb.tool.xxd(b'ABCDEFGHIJKLMNOPQ')
        # Two lines: one full, one partial
        self.assertIn('0000000:', result)
        self.assertIn('0000010:', result)

    def test_nonprintable(self):
        result = lmdb.tool.xxd(b'\x00\x01\x02')
        # Non-printable chars should show as dots
        self.assertIn('...', result)


class DeltaTest(testlib.LmdbTest):
    def test_basic(self):
        self.assertEqual(lmdb.tool.delta([1, 3, 6, 10]), [2, 3, 4])

    def test_two_elements(self):
        self.assertEqual(lmdb.tool.delta([5, 8]), [3])

    def test_single_element(self):
        self.assertEqual(lmdb.tool.delta([5]), [])


class DieTest(testlib.LmdbTest):
    def test_raises_system_exit(self):
        with self.assertRaises(SystemExit) as cm:
            lmdb.tool.die('error %s', 'msg')
        self.assertEqual(cm.exception.code, 1)

    def test_no_args(self):
        with self.assertRaises(SystemExit):
            lmdb.tool.die('simple error')


class MakeParserTest(testlib.LmdbTest):
    def test_returns_parser(self):
        parser = lmdb.tool.make_parser()
        self.assertIsNotNone(parser)

    def test_defaults(self):
        parser = lmdb.tool.make_parser()
        opts, args = parser.parse_args([])
        self.assertEqual(opts.map_size, 10)
        self.assertEqual(opts.max_dbs, 128)
        self.assertEqual(opts.interval, 1)
        self.assertEqual(opts.window, 10)
        self.assertFalse(opts.compact)
        self.assertFalse(opts.csv)
        self.assertIsNone(opts.env)

    def test_parse_env(self):
        parser = lmdb.tool.make_parser()
        opts, args = parser.parse_args(['-e', '/tmp/test', 'stat'])
        self.assertEqual(opts.env, '/tmp/test')
        self.assertEqual(args, ['stat'])


# ---------------------------------------------------------------------------
# Dump / restore format tests
# ---------------------------------------------------------------------------

class DumpCursorToFpTest(testlib.LmdbTest):
    def test_roundtrip(self):
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'key1', b'val1')
            txn.put(b'key2', b'val2')

        fp = BytesIO()
        with env.begin() as txn:
            cursor = txn.cursor()
            lmdb.tool.dump_cursor_to_fp(cursor, fp)

        content = fp.getvalue()
        # cdbmake format: +klen,vlen:key->value\n
        self.assertIn(b'+4,4:key1->val1\n', content)
        self.assertIn(b'+4,4:key2->val2\n', content)
        # Ends with empty line
        self.assertTrue(content.endswith(b'\n\n'))

    def test_empty_db(self):
        path, env = testlib.temp_env()
        fp = BytesIO()
        with env.begin() as txn:
            cursor = txn.cursor()
            lmdb.tool.dump_cursor_to_fp(cursor, fp)
        self.assertEqual(fp.getvalue(), b'\n')


class RestoreCursorFromFpTest(testlib.LmdbTest):
    def test_basic(self):
        path, env = testlib.temp_env()
        data = b'+4,4:key1->val1\n+4,4:key2->val2\n\n'
        with env.begin(write=True) as txn:
            count = lmdb.tool.restore_cursor_from_fp(txn, BytesIO(data), None)
        self.assertEqual(count, 3)  # 2 records + final newline iteration
        with env.begin() as txn:
            self.assertEqual(txn.get(b'key1'), b'val1')
            self.assertEqual(txn.get(b'key2'), b'val2')

    def test_empty(self):
        path, env = testlib.temp_env()
        data = b'\n'
        with env.begin(write=True) as txn:
            count = lmdb.tool.restore_cursor_from_fp(txn, BytesIO(data), None)
        self.assertEqual(count, 1)

    def test_bad_plus(self):
        path, env = testlib.temp_env()
        data = b'X4,4:key1->val1\n\n'
        with env.begin(write=True) as txn:
            with self.assertRaises(SystemExit):
                lmdb.tool.restore_cursor_from_fp(txn, BytesIO(data), None)

    def test_bad_separator(self):
        path, env = testlib.temp_env()
        data = b'+4,4:key1XXval1\n\n'
        with env.begin(write=True) as txn:
            with self.assertRaises(SystemExit):
                lmdb.tool.restore_cursor_from_fp(txn, BytesIO(data), None)

    def test_bad_length(self):
        path, env = testlib.temp_env()
        data = b'+abc,4:key1->val1\n\n'
        with env.begin(write=True) as txn:
            with self.assertRaises(SystemExit):
                lmdb.tool.restore_cursor_from_fp(txn, BytesIO(data), None)

    def test_short_data(self):
        path, env = testlib.temp_env()
        # Claim 10 bytes for key but only provide 4
        data = b'+10,4:key1->val1\n\n'
        with env.begin(write=True) as txn:
            with self.assertRaises(SystemExit):
                lmdb.tool.restore_cursor_from_fp(txn, BytesIO(data), None)

    def test_bad_line_ending(self):
        path, env = testlib.temp_env()
        # Missing newline at end of record (truncated)
        data = b'+4,4:key1->val1X\n'
        with env.begin(write=True) as txn:
            with self.assertRaises(SystemExit):
                lmdb.tool.restore_cursor_from_fp(txn, BytesIO(data), None)

    def test_dump_restore_roundtrip(self):
        path, env = testlib.temp_env()
        with env.begin(write=True) as txn:
            txn.put(b'alpha', b'one')
            txn.put(b'beta', b'two')
            txn.put(b'gamma', b'three')

        fp = BytesIO()
        with env.begin() as txn:
            lmdb.tool.dump_cursor_to_fp(txn.cursor(), fp)

        # Restore into a fresh env
        path2, env2 = testlib.temp_env()
        fp.seek(0)
        with env2.begin(write=True) as txn:
            lmdb.tool.restore_cursor_from_fp(txn, fp, None)

        with env2.begin() as txn:
            self.assertEqual(txn.get(b'alpha'), b'one')
            self.assertEqual(txn.get(b'beta'), b'two')
            self.assertEqual(txn.get(b'gamma'), b'three')


# ---------------------------------------------------------------------------
# Integration tests via main() / call_tool()
# ---------------------------------------------------------------------------

class ToolTestBase(testlib.LmdbTest):
    """Base class that creates a populated environment."""
    def setUp(self):
        self.path, self.env = testlib.temp_env()
        with self.env.begin(write=True) as txn:
            txn.put(b'key1', b'value1')
            txn.put(b'key2', b'value2')
            txn.put(b'key3', b'value3')



class MainErrorTest(testlib.LmdbTest):
    def test_no_command(self):
        with self.assertRaises(SystemExit):
            lmdb.tool.main([])

    def test_no_env(self):
        with self.assertRaises(SystemExit):
            lmdb.tool.main(['stat'])

    def test_bad_command(self):
        path, env = testlib.temp_env()
        env.close()
        with self.assertRaises(SystemExit):
            lmdb.tool.main(['-e', path, 'nonexistent_command'])


class CmdStatTest(ToolTestBase):
    def test_stat(self):
        self.env.close()
        call_tool('-e %s stat' % self.path)


class CmdGetTest(ToolTestBase):
    def test_get_existing(self):
        self.env.close()
        call_tool('-e %s get key1' % self.path)

    def test_get_missing(self):
        self.env.close()
        call_tool('-e %s get missing_key' % self.path)

    def test_get_multiple(self):
        self.env.close()
        call_tool('-e %s get key1 key2 key3' % self.path)

    def test_get_xxd(self):
        self.env.close()
        call_tool('-e %s -x get key1' % self.path)

    def test_get_subdb(self):
        db = self.env.open_db(b'mydb')
        with self.env.begin(write=True, db=db) as txn:
            txn.put(b'subkey', b'subval', db=db)
        self.env.close()
        call_tool('-e %s -d mydb get subkey' % self.path)


class CmdCopyTest(ToolTestBase):
    def test_copy(self):
        self.env.close()
        target = testlib.temp_dir(create=False)
        call_tool('-e %s copy %s' % (self.path, target))
        self.assertTrue(os.path.exists(target))
        # Verify the copy is valid
        copy_env = lmdb.open(target, readonly=True)
        with copy_env.begin() as txn:
            self.assertEqual(txn.get(b'key1'), b'value1')
        copy_env.close()

    def test_copy_compact(self):
        self.env.close()
        target = testlib.temp_dir(create=False)
        call_tool('-e %s --compact copy %s' % (self.path, target))
        self.assertTrue(os.path.exists(target))

    def test_copy_existing_target(self):
        self.env.close()
        target = testlib.temp_dir()  # Already exists
        with self.assertRaises(SystemExit):
            call_tool('-e %s copy %s' % (self.path, target))

    def test_copy_no_args(self):
        self.env.close()
        with self.assertRaises(SystemExit):
            call_tool('-e %s copy' % self.path)


class CmdCopyfdTest(ToolTestBase):
    def test_copyfd(self):
        self.env.close()
        outpath = os.path.join(testlib.temp_dir(), 'data.mdb')
        fd = os.open(outpath, os.O_WRONLY | os.O_CREAT, 0o644)
        try:
            call_tool('-e %s --out-fd %d copyfd' % (self.path, fd))
        finally:
            os.close(fd)
        self.assertTrue(os.path.getsize(outpath) > 0)

    def test_copyfd_with_args(self):
        self.env.close()
        with self.assertRaises(SystemExit):
            call_tool('-e %s copyfd extra_arg' % self.path)

    def test_copyfd_bad_fd(self):
        self.env.close()
        with self.assertRaises(SystemExit):
            call_tool('-e %s --out-fd 9999 copyfd' % self.path)


class CmdDumpRestoreTest(ToolTestBase):
    def test_dump_main(self):
        self.env.close()
        dump_dir = testlib.temp_dir()
        old_cwd = os.getcwd()
        try:
            os.chdir(dump_dir)
            call_tool('-e %s dump' % self.path)
            self.assertTrue(os.path.exists('main.cdbmake'))
            with open('main.cdbmake', 'rb') as f:
                content = f.read()
            self.assertIn(b'key1', content)
            self.assertIn(b'value1', content)
        finally:
            os.chdir(old_cwd)

    def test_dump_named(self):
        db = self.env.open_db(b'mydb')
        with self.env.begin(write=True, db=db) as txn:
            txn.put(b'skey', b'sval', db=db)
        self.env.close()

        dump_dir = testlib.temp_dir()
        dump_file = os.path.join(dump_dir, 'mydb.cdbmake')
        call_tool('-e %s dump mydb=%s' % (self.path, dump_file))
        self.assertTrue(os.path.exists(dump_file))

    def test_dump_restore_roundtrip(self):
        self.env.close()
        dump_dir = testlib.temp_dir()
        dump_file = os.path.join(dump_dir, 'main.cdbmake')

        # Dump
        call_tool('-e %s dump :main:=%s' % (self.path, dump_file))

        # Restore into new env
        restore_path, restore_env = testlib.temp_env()
        restore_env.close()
        call_tool('-e %s restore :main:=%s' % (restore_path, dump_file))

        # Verify
        verify_env = lmdb.open(restore_path, readonly=True)
        with verify_env.begin() as txn:
            self.assertEqual(txn.get(b'key1'), b'value1')
            self.assertEqual(txn.get(b'key2'), b'value2')
            self.assertEqual(txn.get(b'key3'), b'value3')
        verify_env.close()

    def test_dump_subdb_roundtrip(self):
        db = self.env.open_db(b'sub1')
        with self.env.begin(write=True, db=db) as txn:
            txn.put(b'a', b'1', db=db)
            txn.put(b'b', b'2', db=db)
        self.env.close()

        dump_dir = testlib.temp_dir()
        dump_file = os.path.join(dump_dir, 'sub1.cdbmake')
        call_tool('-e %s dump sub1=%s' % (self.path, dump_file))

        restore_path, restore_env = testlib.temp_env()
        restore_env.open_db(b'sub1')
        restore_env.close()
        call_tool('-e %s restore sub1=%s' % (restore_path, dump_file))

        verify_env = lmdb.open(restore_path, max_dbs=10, readonly=True)
        db2 = verify_env.open_db(b'sub1')
        with verify_env.begin(db=db2) as txn:
            self.assertEqual(txn.get(b'a'), b'1')
            self.assertEqual(txn.get(b'b'), b'2')
        verify_env.close()


class DbMapFromArgsTest(testlib.LmdbTest):
    def test_no_args(self):
        path, env = testlib.temp_env()
        lmdb.tool.ENV = env
        try:
            db_map = lmdb.tool.db_map_from_args([])
            self.assertIn(':main:', db_map)
            self.assertEqual(db_map[':main:'][1], 'main.cdbmake')
        finally:
            lmdb.tool.ENV = None

    def test_missing_equals(self):
        path, env = testlib.temp_env()
        lmdb.tool.ENV = env
        try:
            with self.assertRaises(SystemExit):
                lmdb.tool.db_map_from_args(['badspec'])
        finally:
            lmdb.tool.ENV = None

    def test_duplicate_db(self):
        path, env = testlib.temp_env()
        lmdb.tool.ENV = env
        try:
            with self.assertRaises(SystemExit):
                lmdb.tool.db_map_from_args([':main:=a.cdb', ':main:=b.cdb'])
        finally:
            lmdb.tool.ENV = None


class CmdDropTest(testlib.LmdbTest):
    def test_drop_subdb(self):
        path, env = testlib.temp_env()
        db = env.open_db(b'todrop')
        with env.begin(write=True, db=db) as txn:
            txn.put(b'x', b'y', db=db)
        env.close()
        call_tool('-e %s drop todrop' % path)
        # Verify it's gone
        env2 = lmdb.open(path, max_dbs=10)
        with env2.begin() as txn:
            # The sub-db should be dropped; trying to open it creates empty
            db2 = env2.open_db(b'todrop')
            with env2.begin(db=db2) as txn2:
                self.assertIsNone(txn2.get(b'x'))
        env2.close()

    def test_drop_no_args(self):
        path, env = testlib.temp_env()
        env.close()
        with self.assertRaises(SystemExit):
            call_tool('-e %s drop' % path)

    def test_drop_main_fails(self):
        path, env = testlib.temp_env()
        env.close()
        with self.assertRaises(SystemExit):
            call_tool('-e %s drop :main:' % path)


class CmdEditTest(ToolTestBase):
    def test_set(self):
        self.env.close()
        call_tool('-e %s edit --set newkey=newval' % self.path)
        env2 = lmdb.open(self.path, readonly=True)
        with env2.begin() as txn:
            self.assertEqual(txn.get(b'newkey'), b'newval')
        env2.close()

    def test_set_overwrite(self):
        self.env.close()
        call_tool('-e %s edit --set key1=updated' % self.path)
        env2 = lmdb.open(self.path, readonly=True)
        with env2.begin() as txn:
            self.assertEqual(txn.get(b'key1'), b'updated')
        env2.close()

    def test_add_no_overwrite(self):
        self.env.close()
        call_tool('-e %s edit --add key1=should_not_replace' % self.path)
        env2 = lmdb.open(self.path, readonly=True)
        with env2.begin() as txn:
            # add uses overwrite=False, so original value is kept
            self.assertEqual(txn.get(b'key1'), b'value1')
        env2.close()

    def test_add_new_key(self):
        self.env.close()
        call_tool('-e %s edit --add brand_new=yes' % self.path)
        env2 = lmdb.open(self.path, readonly=True)
        with env2.begin() as txn:
            self.assertEqual(txn.get(b'brand_new'), b'yes')
        env2.close()

    def test_delete(self):
        self.env.close()
        call_tool('-e %s edit --delete key2' % self.path)
        env2 = lmdb.open(self.path, readonly=True)
        with env2.begin() as txn:
            self.assertIsNone(txn.get(b'key2'))
        env2.close()

    def test_set_file(self):
        import tempfile
        fd, fpath = tempfile.mkstemp()
        os.write(fd, b'file_content')
        os.close(fd)
        self.env.close()
        try:
            call_tool('-e %s edit --set-file filekey=%s' % (self.path, fpath))
        finally:
            os.unlink(fpath)
        env2 = lmdb.open(self.path, readonly=True)
        with env2.begin() as txn:
            self.assertEqual(txn.get(b'filekey'), b'file_content')
        env2.close()

    def test_add_file(self):
        import tempfile
        fd, fpath = tempfile.mkstemp()
        os.write(fd, b'added_content')
        os.close(fd)
        self.env.close()
        try:
            call_tool('-e %s edit --add-file addfilekey=%s' % (self.path, fpath))
        finally:
            os.unlink(fpath)
        env2 = lmdb.open(self.path, readonly=True)
        with env2.begin() as txn:
            self.assertEqual(txn.get(b'addfilekey'), b'added_content')
        env2.close()

    def test_edit_with_positional_args_fails(self):
        self.env.close()
        with self.assertRaises(SystemExit):
            call_tool('-e %s edit somearg' % self.path)

    def test_multiple_operations(self):
        self.env.close()
        call_tool('-e %s edit --set a=1 --set b=2 --delete key1' % self.path)
        env2 = lmdb.open(self.path, readonly=True)
        with env2.begin() as txn:
            self.assertEqual(txn.get(b'a'), b'1')
            self.assertEqual(txn.get(b'b'), b'2')
            self.assertIsNone(txn.get(b'key1'))
        env2.close()


class CmdReadersTest(ToolTestBase):
    def test_readers(self):
        self.env.close()
        call_tool('-e %s readers' % self.path)

    def test_readers_clean(self):
        self.env.close()
        call_tool('-e %s -c readers' % self.path)


class CmdRewriteTest(ToolTestBase):
    def test_rewrite_main(self):
        self.env.close()
        target = testlib.temp_dir()
        call_tool('-e %s -E %s rewrite' % (self.path, target))
        env2 = lmdb.open(target, readonly=True)
        with env2.begin() as txn:
            self.assertEqual(txn.get(b'key1'), b'value1')
            self.assertEqual(txn.get(b'key2'), b'value2')
        env2.close()

    def test_rewrite_subdb(self):
        db = self.env.open_db(b'subdb')
        with self.env.begin(write=True, db=db) as txn:
            txn.put(b'sk', b'sv', db=db)
        self.env.close()
        target = testlib.temp_dir()
        call_tool('-e %s -E %s rewrite subdb' % (self.path, target))
        env2 = lmdb.open(target, max_dbs=10, readonly=True)
        db2 = env2.open_db(b'subdb')
        with env2.begin(db=db2) as txn:
            self.assertEqual(txn.get(b'sk'), b'sv')
        env2.close()

    def test_rewrite_no_target(self):
        self.env.close()
        with self.assertRaises(SystemExit):
            call_tool('-e %s rewrite' % self.path)


class CmdWarmTest(ToolTestBase):
    def test_warm(self):
        self.env.close()
        call_tool('-e %s warm' % self.path)

    def test_warm_single_file(self):
        path = testlib.temp_file(create=False)
        env = lmdb.open(path, subdir=False)
        with env.begin(write=True) as txn:
            txn.put(b'k', b'v')
        env.close()
        call_tool('-e %s -S 10 --use-single-file warm' % path)


class CmdWatchTest(ToolTestBase):
    def _patch_for_watch(self):
        """Patch time.sleep to raise KeyboardInterrupt and disable DiskStatter."""
        import lmdb.tool as tool_mod
        self._orig_sleep = tool_mod.time.sleep
        self._orig_find = tool_mod._find_diskstat
        tool_mod.time.sleep = lambda _: (_ for _ in ()).throw(KeyboardInterrupt)
        # Disable DiskStatter to avoid /sys/block parsing issues in test envs
        tool_mod._find_diskstat = lambda path: None

    def _unpatch_for_watch(self):
        import lmdb.tool as tool_mod
        tool_mod.time.sleep = self._orig_sleep
        tool_mod._find_diskstat = self._orig_find

    def test_watch_csv_interrupt(self):
        """Test watch with CSV output, interrupted immediately."""
        self.env.close()
        self._patch_for_watch()
        try:
            call_tool('-e %s --csv watch' % self.path)
        finally:
            self._unpatch_for_watch()

    def test_watch_terminal_interrupt(self):
        """Test watch with terminal output, interrupted immediately."""
        self.env.close()
        self._patch_for_watch()
        try:
            call_tool('-e %s watch' % self.path)
        finally:
            self._unpatch_for_watch()


class MainDispatchTest(testlib.LmdbTest):
    def test_stat_via_main(self):
        path, env = testlib.temp_env()
        env.close()
        call_tool('-e %s stat' % path)

    def test_subdb_option(self):
        path, env = testlib.temp_env()
        db = env.open_db(b'mydb')
        with env.begin(write=True, db=db) as txn:
            txn.put(b'k', b'v', db=db)
        env.close()
        call_tool('-e %s -d mydb get k' % path)

    def test_map_size_option(self):
        path, env = testlib.temp_env()
        env.close()
        call_tool('-e %s -S 20 stat' % path)

    def test_readonly_option(self):
        path, env = testlib.temp_env()
        env.close()
        call_tool('-e %s -r READ stat' % path)


if __name__ == '__main__':
    unittest.main()
