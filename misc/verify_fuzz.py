#!/usr/bin/env python3
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

"""Fuzz harness for the offline verifier's central claim:

    a file that passes ``lmdb.verify`` will not crash or corrupt the engine.

It builds a normal database, then repeatedly mutates a copy at random and runs
the mutated file through ``lmdb.verify``.  Every file the verifier *accepts* is
then handed to the real C engine in a **separate process** (so a segfault or
SIGBUS is caught as a signal rather than taking the harness down) which opens
it and walks every database.  A file that verify passed but the engine died on
is a counterexample -- printed and counted.

This is the fuzz counterpart described in docs/verifier-plan.md: it validates
the "passes verify => safe" direction and keeps the verifier honest against
upstream drift.  It is deliberately not part of the CI suite (random, slow);
run it by hand:

    python misc/verify_fuzz.py --engine 0 --iters 5000
    python misc/verify_fuzz.py --engine 1 --iters 5000

House style follows misc/md_pad_repro.py: engine-selectable, subprocess-isolated
so a crash is visible as a signal.
"""

import argparse
import os
import random
import shutil
import signal
import struct
import sys
import tempfile

# Run against the in-tree package.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def build(path):
    import lmdb
    env = lmdb.open(path, max_dbs=8, map_size=16 * 1024 * 1024)
    try:
        with env.begin(write=True) as t:
            for i in range(800):
                t.put(b'k%05d' % i, b'v' * (i % 40))
        d = env.open_db(b'dups', dupsort=True)
        f = env.open_db(b'df', dupsort=True, dupfixed=True)
        with env.begin(write=True) as t:
            for i in range(40):
                for j in range(20):
                    t.put(b'k%03d' % i, b'dv%04d' % j, db=d)
                for j in range(30):
                    t.put(b'k%03d' % i, struct.pack('<Q', j), db=f)
        with env.begin(write=True) as t:                  # populate the freeDB
            for i in range(0, 800, 3):
                t.delete(b'k%05d' % i)
        env.sync(True)
    finally:
        env.close()


def engine_walk(data_path):
    """Child process: open the file through the C engine and traverse it.
    Exit 0 if it survives (even if it raises lmdb.Error), nonzero only on a
    Python exception we did not expect.  A hard crash shows up as a negative
    exit / signal to the parent."""
    import lmdb
    env = None
    try:
        # A generous max_dbs: on a mutated file, main-DB keys can spuriously
        # open as sub-DBs and each consumes a handle slot; too small a limit
        # would abort the walk early and mask a crash further in.
        env = lmdb.open(os.path.dirname(data_path), max_dbs=1024, readonly=True,
                        lock=False, create=False)
        with env.begin() as t:
            for k, _ in t.cursor():
                try:
                    sub = env.open_db(k, txn=t)
                except lmdb.Error:
                    continue
                with env.begin(db=sub) as st:
                    for _ in st.cursor():
                        pass
            for _ in t.cursor():
                pass
    except lmdb.Error:
        pass
    finally:
        if env is not None:
            env.close()


# Child exit codes: 0 = engine survived; 3 = an unexpected Python-level
# exception in the harness itself (NOT an engine crash -- do not count it as a
# counterexample); a negative wait status / signal is a real engine crash.
CHILD_OK = 0
CHILD_HARNESS_ERROR = 3
CHILD_TIMEOUT = 60   # seconds before a hung engine is killed


def run_child(data_path):
    """Fork a child to run engine_walk; return (survived, detail).

    survived is True (engine fine), False (engine crashed), or None (the
    harness itself errored -- not a counterexample)."""
    pid = os.fork()
    if pid == 0:
        # Silence the child's stderr noise from expected corruption errors.
        try:
            devnull = os.open(os.devnull, os.O_WRONLY)
            os.dup2(devnull, 2)
            os.close(devnull)
            # A corrupt file can send the engine into an infinite loop; a
            # hung child would otherwise block the whole run forever.
            signal.alarm(CHILD_TIMEOUT)
            engine_walk(data_path)
            os._exit(CHILD_OK)
        except BaseException:
            os._exit(CHILD_HARNESS_ERROR)
    _, status = os.waitpid(pid, 0)
    if os.WIFSIGNALED(status):
        sig = os.WTERMSIG(status)
        if sig == signal.SIGALRM:
            return False, 'timeout (engine hung >%ds)' % CHILD_TIMEOUT
        return False, 'signal %d' % sig
    if os.WIFEXITED(status):
        code = os.WEXITSTATUS(status)
        if code == CHILD_OK:
            return True, ''
        if code == CHILD_HARNESS_ERROR:
            return None, 'harness error (not a counterexample)'
        return False, 'exit %d' % code
    return False, 'unknown wait status %d' % status


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--engine', choices=['0', '1'], default='0')
    ap.add_argument('--iters', type=int, default=3000)
    ap.add_argument('--maxflips', type=int, default=4)
    ap.add_argument('--seed', type=int, default=1234)
    args = ap.parse_args()
    args.maxflips = max(1, args.maxflips)   # randint(1, 0) would raise
    os.environ['LMDB_DEFAULT_LIB_VERSION'] = args.engine
    rnd = random.Random(args.seed)

    from lmdb import verify as V

    base = tempfile.mkdtemp(prefix='fuzz_base_')
    build(base)
    with open(os.path.join(base, 'data.mdb'), 'rb') as f:
        pristine = f.read()

    accepted = crashed = rejected = harness_errors = 0
    counterexamples = []
    work = tempfile.mkdtemp(prefix='fuzz_work_')
    data = os.path.join(work, 'data.mdb')
    try:
        for it in range(args.iters):
            raw = bytearray(pristine)
            for _ in range(rnd.randint(1, args.maxflips)):
                raw[rnd.randrange(len(raw))] = rnd.randrange(256)
            with open(data, 'wb') as f:
                f.write(raw)
            for lk in ('lock.mdb',):                      # fresh lock each time
                p = os.path.join(work, lk)
                if os.path.exists(p):
                    os.unlink(p)
            try:
                errors = V.verify(work)
            except V.VerifyError:
                rejected += 1
                continue
            if errors:
                rejected += 1
                continue
            accepted += 1
            survived, detail = run_child(data)
            if survived is None:
                # The harness itself failed; not an engine counterexample.
                harness_errors += 1
                print('HARNESS ERROR: %s' % detail)
            elif not survived:
                crashed += 1
                ce = os.path.join(tempfile.mkdtemp(prefix='fuzz_ce_'), 'data.mdb')
                shutil.copy(data, ce)
                counterexamples.append((detail, ce))
                print('COUNTEREXAMPLE (%s): verify passed but engine died -> %s'
                      % (detail, ce))
            if (it + 1) % 1000 == 0:
                print('  %d/%d  accepted=%d rejected=%d crashed=%d'
                      % (it + 1, args.iters, accepted, rejected, crashed))
    finally:
        shutil.rmtree(base, ignore_errors=True)
        shutil.rmtree(work, ignore_errors=True)

    print('engine=%s iters=%d accepted=%d rejected=%d '
          'engine-crashes-among-accepted=%d harness-errors=%d'
          % (args.engine, args.iters, accepted, rejected, crashed,
             harness_errors))
    sys.exit(1 if counterexamples else 0)


if __name__ == '__main__':
    main()
