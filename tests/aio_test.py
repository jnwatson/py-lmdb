#!/usr/bin/env python
# Copyright 2013-2025 The py-lmdb authors, all rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted only as authorized by the OpenLDAP
# Public License.

"""Tests for lmdb.aio async wrappers."""

import asyncio
import inspect
import unittest

import lmdb
import lmdb.aio
import testlib


def run(coro):
    """Run a coroutine to completion."""
    return asyncio.new_event_loop().run_until_complete(coro)


class WrapTest(testlib.LmdbTest):
    def tearDown(self):
        testlib.cleanup()

    def test_wrap_returns_async_env(self):
        _, env = testlib.temp_env()
        aenv = lmdb.aio.wrap(env)
        self.assertIsInstance(aenv, lmdb.aio.AsyncEnvironment)

    def test_wrap_custom_executor(self):
        from concurrent.futures import ThreadPoolExecutor
        _, env = testlib.temp_env()
        pool = ThreadPoolExecutor(1)
        aenv = lmdb.aio.wrap(env, executor=pool)
        self.assertEqual(aenv._executor, pool)
        pool.shutdown(wait=False)


class AsyncEnvTest(testlib.LmdbTest):
    def tearDown(self):
        testlib.cleanup()

    def test_context_manager(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv:
                await aenv.stat()
        run(go())

    def test_stat(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            st = await aenv.stat()
            self.assertIn('entries', st)
            self.assertIn('psize', st)
        run(go())

    def test_info(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            info = await aenv.info()
            self.assertIn('map_size', info)
        run(go())

    def test_sync_methods_not_awaitable(self):
        """path(), max_key_size() etc. return directly, not coroutines."""
        _, env = testlib.temp_env()
        aenv = lmdb.aio.wrap(env)
        # These should be plain values/callables, not coroutines
        self.assertIsInstance(aenv.path(), str)
        self.assertIsInstance(aenv.max_key_size(), int)


class AsyncTxnTest(testlib.LmdbTest):
    def tearDown(self):
        testlib.cleanup()

    def test_put_get(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                await txn.put(b'hello', b'world')
            async with aenv.begin() as txn:
                val = await txn.get(b'hello')
                self.assertEqual(val, b'world')
        run(go())

    def test_delete(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                await txn.put(b'k', b'v')
            async with aenv.begin(write=True) as txn:
                self.assertTrue(await txn.delete(b'k'))
                self.assertIsNone(await txn.get(b'k'))
        run(go())

    def test_replace(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                await txn.put(b'k', b'old')
            async with aenv.begin(write=True) as txn:
                old = await txn.replace(b'k', b'new')
                self.assertEqual(old, b'old')
            async with aenv.begin() as txn:
                self.assertEqual(await txn.get(b'k'), b'new')
        run(go())

    def test_pop(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                await txn.put(b'k', b'v')
            async with aenv.begin(write=True) as txn:
                val = await txn.pop(b'k')
                self.assertEqual(val, b'v')
                self.assertIsNone(await txn.get(b'k'))
        run(go())

    def test_stat(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                await txn.put(b'k', b'v')
            async with aenv.begin() as txn:
                st = await txn.stat(env.open_db())
                self.assertEqual(st['entries'], 1)
        run(go())

    def test_id_sync(self):
        """id() is a sync accessor — should return directly."""
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin() as txn:
                tid = txn.id()
                self.assertIsInstance(tid, int)
        run(go())

    def test_abort_on_exception(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            try:
                async with aenv.begin(write=True) as txn:
                    await txn.put(b'k', b'v')
                    raise ValueError('boom')
            except ValueError:
                pass
            # The put should have been rolled back
            async with aenv.begin() as txn:
                self.assertIsNone(await txn.get(b'k'))
        run(go())


class AsyncCursorTest(testlib.LmdbTest):
    def tearDown(self):
        testlib.cleanup()

    def test_cursor_put_and_navigate(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                async with txn.cursor() as cur:
                    await cur.put(b'a', b'1')
                    await cur.put(b'b', b'2')
                    await cur.put(b'c', b'3')

            async with aenv.begin() as txn:
                async with txn.cursor() as cur:
                    self.assertTrue(await cur.first())
                    self.assertEqual(cur.key(), b'a')
                    self.assertTrue(await cur.next())
                    self.assertEqual(cur.key(), b'b')
                    self.assertTrue(await cur.last())
                    self.assertEqual(cur.key(), b'c')
                    self.assertTrue(await cur.prev())
                    self.assertEqual(cur.key(), b'b')
        run(go())

    def test_set_key(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                await txn.put(b'x', b'10')
                await txn.put(b'y', b'20')
            async with aenv.begin() as txn:
                async with txn.cursor() as cur:
                    self.assertTrue(await cur.set_key(b'y'))
                    self.assertEqual(cur.value(), b'20')
                    self.assertFalse(await cur.set_key(b'z'))
        run(go())

    def test_set_range(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                await txn.put(b'aa', b'1')
                await txn.put(b'cc', b'3')
            async with aenv.begin() as txn:
                async with txn.cursor() as cur:
                    self.assertTrue(await cur.set_range(b'bb'))
                    self.assertEqual(cur.key(), b'cc')
        run(go())

    def test_iternext(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                await txn.put(b'a', b'1')
                await txn.put(b'b', b'2')
                await txn.put(b'c', b'3')
            async with aenv.begin() as txn:
                async with txn.cursor() as cur:
                    await cur.first()
                    items = await cur.iternext()
                    self.assertEqual(items, [
                        (b'a', b'1'), (b'b', b'2'), (b'c', b'3'),
                    ])
        run(go())

    def test_iterprev(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                await txn.put(b'a', b'1')
                await txn.put(b'b', b'2')
            async with aenv.begin() as txn:
                async with txn.cursor() as cur:
                    await cur.last()
                    items = await cur.iterprev()
                    self.assertEqual(items, [(b'b', b'2'), (b'a', b'1')])
        run(go())

    def test_iternext_keys_only(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                await txn.put(b'x', b'1')
                await txn.put(b'y', b'2')
            async with aenv.begin() as txn:
                async with txn.cursor() as cur:
                    await cur.first()
                    keys = await cur.iternext(keys=True, values=False)
                    self.assertEqual(keys, [b'x', b'y'])
        run(go())

    def test_count(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            db = env.open_db(b'dup', dupsort=True)
            async with aenv.begin(write=True, db=db) as txn:
                await txn.put(b'k', b'a')
                await txn.put(b'k', b'b')
                await txn.put(b'k', b'c')
            async with aenv.begin(db=db) as txn:
                async with txn.cursor() as cur:
                    await cur.set_key(b'k')
                    self.assertEqual(await cur.count(), 3)
        run(go())

    def test_cursor_delete(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                await txn.put(b'a', b'1')
                await txn.put(b'b', b'2')
            async with aenv.begin(write=True) as txn:
                async with txn.cursor() as cur:
                    await cur.set_key(b'a')
                    await cur.delete()
            async with aenv.begin() as txn:
                self.assertIsNone(await txn.get(b'a'))
                self.assertEqual(await txn.get(b'b'), b'2')
        run(go())

    def test_key_value_item_sync(self):
        """key(), value(), item() should be direct (not awaitable)."""
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                await txn.put(b'k', b'v')
            async with aenv.begin() as txn:
                async with txn.cursor() as cur:
                    await cur.first()
                    # These are sync — return values directly
                    self.assertEqual(cur.key(), b'k')
                    self.assertEqual(cur.value(), b'v')
                    self.assertEqual(cur.item(), (b'k', b'v'))
        run(go())


class AsyncConcurrencyTest(testlib.LmdbTest):
    """Verify that async operations can overlap."""

    def tearDown(self):
        testlib.cleanup()

    def test_concurrent_reads(self):
        async def go():
            _, env = testlib.temp_env()
            aenv = lmdb.aio.wrap(env)
            async with aenv.begin(write=True) as txn:
                for i in range(100):
                    await txn.put(str(i).encode(), str(i).encode())

            async def read_one(key):
                async with aenv.begin() as txn:
                    return await txn.get(key)

            results = await asyncio.gather(
                *[read_one(str(i).encode()) for i in range(100)]
            )
            for i, val in enumerate(results):
                self.assertEqual(val, str(i).encode())

        run(go())


class IntrospectionTest(unittest.TestCase):
    """Proxied methods must be real class attributes (dir/inspect/stubtest)."""

    def test_methods_are_real_attributes(self):
        for cls, names in (
            (lmdb.aio.AsyncEnvironment, ('path', 'stat', 'info')),
            (lmdb.aio.AsyncTransaction, ('id', 'get', 'put', 'commit')),
            (lmdb.aio.AsyncCursor, ('key', 'get', 'iternext', 'getmulti')),
        ):
            for name in names:
                self.assertIn(name, dir(cls))
                self.assertIn(name, vars(cls))

    def test_all_public_methods_proxied(self):
        for wrapped, wrapper in (
            (lmdb.Environment, lmdb.aio.AsyncEnvironment),
            (lmdb.Transaction, lmdb.aio.AsyncTransaction),
            (lmdb.Cursor, lmdb.aio.AsyncCursor),
        ):
            public = {
                n for n in dir(wrapped)
                if not n.startswith('_') and callable(getattr(wrapped, n))
            }
            missing = public - set(vars(wrapper))
            self.assertFalse(missing, f"missing: {sorted(missing)}")

    def test_async_method_metadata(self):
        get = vars(lmdb.aio.AsyncTransaction)['get']
        self.assertTrue(inspect.iscoroutinefunction(get))
        self.assertIs(get.__wrapped__, lmdb.Transaction.get)
        self.assertEqual(get.__doc__, lmdb.Transaction.get.__doc__)

    def test_passthrough_is_not_a_coroutine(self):
        path = vars(lmdb.aio.AsyncEnvironment)['path']
        self.assertFalse(inspect.iscoroutinefunction(path))
        self.assertIs(path.__wrapped__, lmdb.Environment.path)


class GetattrFallbackTest(testlib.LmdbTest):
    """Non-callable attributes proxy through `__getattr__` (CFFI-only)."""

    def tearDown(self):
        testlib.cleanup()

    def test_fallback(self):
        _, env = testlib.temp_env()
        if not hasattr(env, 'readonly'):
            self.skipTest('CFFI-only')

        aenv = lmdb.aio.wrap(env)
        self.assertEqual(getattr(aenv, 'readonly'), env.readonly)

        async def go():
            async with aenv.begin() as txn:
                self.assertIs(txn.env, txn._txn.env)
                async with txn.cursor() as cur:
                    self.assertIs(cur.db, cur._cursor.db)
                    self.assertIs(cur.txn, cur._cursor.txn)
        run(go())


if __name__ == '__main__':
    unittest.main()
