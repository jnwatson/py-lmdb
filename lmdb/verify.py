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

"""Offline structural verifier for LMDB data files (``verify-then-trust``).

This is a pure-Python byte walker.  It never hands the file to liblmdb, so it
is safe to run on a file from an untrusted source: nothing here can be made to
crash or corrupt the process by hostile input the way opening the file through
the engine could.  That is the whole point -- it is the tool that lets a user
turn "a file I do not control" into "a file I have checked and may now trust".

It verifies *every* invariant the LMDB write paths assume, against a global
view of the file at rest:

  * both meta pages parse and the committed one is selected as the engine would;
  * every page reachable from the committed roots (main DB, the freeDB, every
    named sub-DB, dup sub-trees, inline sub-pages and overflow extents) is
    structurally sound, stores its own page number, and is reachable exactly
    once;
  * keys are ordered under the comparator each DB's flags imply, and each DB's
    page/entry/depth counters match what the walk actually finds;
  * no reachable page carries a dirty marker (0.9 ``P_DIRTY``; 1.0 an
    ``mp_txnid`` newer than the committed meta);
  * freeDB records are structurally valid, and -- the global check that only an
    offline pass can make -- the free set and the live set are disjoint and
    together cover exactly ``[NUM_METAS, next_pgno)``.

Scope and caveats (see docs/verifier-plan.md for the full threat model):

  * LMDB data files are architecture-specific.  This verifier answers "is this
    file safe for the LMDB engine running on *this* host", assuming the host's
    native byte order and a 64-bit build (8-byte pgno/txnid/size fields).  A
    32-bit file, or one written on the other endianness, is reported as
    unsupported rather than silently mis-parsed.
  * Verification is a point-in-time statement.  It proves nothing unless the
    file cannot be touched by an untrusted writer afterwards (verify a private
    copy; regenerate ``lock.mdb``; treat every later co-writer as trusted).
"""

import os
import struct
import sys


class VerifyError(Exception):
    """Raised for conditions that stop verification before it can even begin
    (not an LMDB data file, an unsupported build, unreadable meta pages).
    Ordinary corruption findings are collected and returned, not raised."""


# --- On-disk constants (shared by both engines) --------------------------

MDB_MAGIC = 0xBEEFC0DE
NUM_METAS = 2
CURSOR_STACK = 32
SIZEOF_MDB_DB = 48
P_INVALID = (1 << 64) - 1

# Page flags.
P_BRANCH = 0x01
P_LEAF = 0x02
P_OVERFLOW = 0x04
P_META = 0x08
P_DIRTY_09 = 0x10       # 0.9 only; on 1.0 this header bit is unused
P_LEAF2 = 0x20
P_SUBP = 0x40

# Node flags.
F_BIGDATA = 0x01
F_SUBDATA = 0x02
F_DUPDATA = 0x04

# DB flags.
MDB_REVERSEKEY = 0x02
MDB_DUPSORT = 0x04
MDB_INTEGERKEY = 0x08
MDB_DUPFIXED = 0x10
MDB_INTEGERDUP = 0x20
MDB_REVERSEDUP = 0x40
MDB_VALID = 0x8000
PERSISTENT_FLAGS = 0xffff & ~MDB_VALID
DUPSORT_ONLY_FLAGS = MDB_DUPFIXED | MDB_INTEGERDUP | MDB_REVERSEDUP

# Per-engine facts, keyed by detected page-header size.
#   PAGEHDRSZ 16 -> LMDB 0.9 (data version 1), PAGEBASE 0
#   PAGEHDRSZ 24 -> LMDB 1.0 (data version 3), PAGEBASE = PAGEHDRSZ
_ENGINES = {
    16: {'name': '0.9', 'version': 1, 'pagebase': 0,  'max_pagesize': 0x8000,
         'has_txnid': False},
    24: {'name': '1.0', 'version': 3, 'pagebase': 24, 'max_pagesize': 0x10000,
         'has_txnid': True},
}

# Native byte order: LMDB writes integers in host order, and files do not
# travel between architectures.
_ORDER = sys.byteorder
_E = '<' if _ORDER == 'little' else '>'
_U16 = struct.Struct(_E + 'H')
_U32 = struct.Struct(_E + 'I')
_U64 = struct.Struct(_E + 'Q')


def _even(n):
    return (n + 1) & ~1


# --- Comparators (mirror mdb.c's built-in md_cmp/md_dcmp) -----------------
#
# The verifier must order keys exactly as the engine does, or a valid file
# fails.  py-lmdb only ever installs LMDB's built-in comparators, selected by
# the DB's persistent flags (mdb_default_cmp).  We reproduce those.

def _cmp_lexical(a, b):
    return (a > b) - (a < b)


def _cmp_reverse(a, b):
    # mdb_cmp_memnr: compare byte strings from the end backwards.
    ra, rb = a[::-1], b[::-1]
    return (ra > rb) - (ra < rb)


def _cmp_int(a, b):
    # mdb_cmp_int/cint/long: fixed-width integers in native order.  Integer
    # DBs hold equal-length keys, so a plain native-endian integer compare
    # reproduces every one of them.
    ia = int.from_bytes(a, _ORDER)
    ib = int.from_bytes(b, _ORDER)
    return (ia > ib) - (ia < ib)


def _key_cmp(flags):
    # Priority matches mdb_default_cmp: REVERSEKEY wins over INTEGERKEY.
    if flags & MDB_REVERSEKEY:
        return _cmp_reverse
    if flags & MDB_INTEGERKEY:
        return _cmp_int
    return _cmp_lexical


def _data_cmp(flags):
    # md_dcmp for a DUPSORT DB (mdb_default_cmp); None for a non-DUPSORT DB.
    if not (flags & MDB_DUPSORT):
        return None
    if flags & MDB_INTEGERDUP:
        return _cmp_int
    if flags & MDB_REVERSEDUP:
        return _cmp_reverse
    return _cmp_lexical


def _bad_db_flags(flags):
    return bool((flags & DUPSORT_ONLY_FLAGS) and not (flags & MDB_DUPSORT))


# --- On-disk records -----------------------------------------------------

class _Db:
    """An MDB_db record (48 bytes on 64-bit, identical on both engines)."""
    __slots__ = ('pad', 'flags', 'depth', 'branch_pages', 'leaf_pages',
                 'overflow_pages', 'entries', 'root')

    def __init__(self, buf, off):
        (self.pad,) = _U32.unpack_from(buf, off)
        (self.flags,) = _U16.unpack_from(buf, off + 4)
        (self.depth,) = _U16.unpack_from(buf, off + 6)
        (self.branch_pages,) = _U64.unpack_from(buf, off + 8)
        (self.leaf_pages,) = _U64.unpack_from(buf, off + 16)
        (self.overflow_pages,) = _U64.unpack_from(buf, off + 24)
        (self.entries,) = _U64.unpack_from(buf, off + 32)
        (self.root,) = _U64.unpack_from(buf, off + 40)


class _Meta:
    """An MDB_meta record, located at PAGEHDRSZ within a meta page."""
    __slots__ = ('pgno', 'flags', 'magic', 'version', 'mapsize', 'psize',
                 'free', 'main', 'last_pg', 'txnid')

    def __init__(self, buf, base, hdrsz):
        (self.pgno,) = _U64.unpack_from(buf, base)
        (self.flags,) = _U16.unpack_from(buf, base + hdrsz - 6)
        m = base + hdrsz
        (self.magic,) = _U32.unpack_from(buf, m)
        (self.version,) = _U32.unpack_from(buf, m + 4)
        # m+8 mm_address (pointer, ignored), m+16 mm_mapsize
        (self.mapsize,) = _U64.unpack_from(buf, m + 16)
        self.free = _Db(buf, m + 24)
        self.main = _Db(buf, m + 24 + SIZEOF_MDB_DB)
        self.psize = self.free.pad     # mm_psize aliases free-DB md_pad
        (self.last_pg,) = _U64.unpack_from(buf, m + 24 + 2 * SIZEOF_MDB_DB)
        (self.txnid,) = _U64.unpack_from(buf, m + 24 + 2 * SIZEOF_MDB_DB + 8)


# A pending sub-tree discovered during a walk: a separate B-tree to verify with
# its own comparators once the current tree is done.
class _SubTree:
    __slots__ = ('name', 'db', 'key_cmp', 'dup_cmp')

    def __init__(self, name, db, key_cmp, dup_cmp):
        self.name = name
        self.db = db
        self.key_cmp = key_cmp
        self.dup_cmp = dup_cmp


# --- The verifier --------------------------------------------------------

class _Verifier:
    MAX_ERRORS = 200

    def __init__(self, fd, filesize):
        self.fd = fd
        self.filesize = filesize
        self.errors = []

        self.eng = None
        self.hdrsz = None
        self.pagebase = None
        self.psize = None
        self.meta = None
        self.next_pgno = None
        self.live = None        # bytearray bitmap, 1 == reachable
        self.free = None        # bytearray bitmap, 1 == listed in freeDB
        self.free_records = []  # (txnid, idl_bytes) collected from the freeDB

    # -- error handling --

    def err(self, msg):
        n = len(self.errors)
        if n < self.MAX_ERRORS:
            self.errors.append(msg)
        elif n == self.MAX_ERRORS:
            self.errors.append('... (further errors suppressed)')

    @property
    def full(self):
        return len(self.errors) > self.MAX_ERRORS

    # -- raw page access (seek/read, never mmap: a short or shrinking file must
    #    give a clean error, not SIGBUS the verifier) --

    def read_page(self, pgno):
        """Return psize bytes for page `pgno`, or None if it lies beyond EOF
        (legal only for a free page, which is never read)."""
        off = pgno * self.psize
        if off + self.psize > self.filesize:
            return None
        buf = os.pread(self.fd, self.psize, off)
        return buf if len(buf) == self.psize else None

    def read_extent(self, pgno, npages):
        off = pgno * self.psize
        length = npages * self.psize
        if off + length > self.filesize:
            return None
        buf = os.pread(self.fd, length, off)
        return buf if len(buf) == length else None

    # -- engine detection & meta pages --

    def detect_engine(self):
        head = os.pread(self.fd, 64, 0)
        hdrsz = None
        for off in range(0, 64, 4):
            if off + 4 <= len(head) and _U32.unpack_from(head, off)[0] == MDB_MAGIC:
                hdrsz = off
                break
        if hdrsz is None:
            raise VerifyError(
                'not an LMDB data file (MDB_MAGIC not found in first page), or '
                'an unsupported 32-bit / foreign-endian file')
        eng = _ENGINES.get(hdrsz)
        if eng is None:
            raise VerifyError(
                'unsupported LMDB layout: page-header size %d (this verifier '
                'handles 64-bit 0.9 and 1.0 files only)' % hdrsz)
        self.hdrsz = hdrsz
        self.pagebase = eng['pagebase']
        self.eng = eng

    def _read_meta(self, pgno, psize):
        """Read page `pgno` as a meta page.  Returns a _Meta, or raises
        VerifyError if it cannot be parsed as one."""
        off = pgno * psize
        want = self.hdrsz + 160
        if off + self.hdrsz + 136 > self.filesize:
            raise VerifyError('meta page %d is beyond the end of the file' % pgno)
        buf = os.pread(self.fd, want, off)
        if len(buf) < self.hdrsz + 136:
            raise VerifyError('meta page %d is truncated' % pgno)
        meta = _Meta(buf, 0, self.hdrsz)
        if not (meta.flags & P_META):
            raise VerifyError('page %d is not marked P_META' % pgno)
        return meta

    def load_metas(self):
        """Validate both meta pages the way mdb_env_read_header does, then
        select the committed one as mdb_env_pick_meta would."""
        # Meta 0 is at file offset 0 regardless of page size; its psize field
        # is authoritative for the file.
        m0 = self._read_meta(0, 1)
        psize = m0.psize
        min_psize = self.hdrsz + 136   # a meta page must not overlap its twin
        if (psize == 0 or (psize & (psize - 1)) or psize > self.eng['max_pagesize']
                or psize < 256 or psize < min_psize):
            raise VerifyError(
                'meta page 0 has an implausible page size %d (want a power of '
                'two in [%d, %d])' % (psize, max(256, min_psize),
                                      self.eng['max_pagesize']))
        self.psize = psize

        m1 = self._read_meta(1, psize)

        for idx, m in ((0, m0), (1, m1)):
            if m.magic != MDB_MAGIC:
                raise VerifyError('meta page %d has bad magic 0x%x' % (idx, m.magic))
            if m.version != self.eng['version']:
                raise VerifyError(
                    'meta page %d is data version %d, this engine expects %d'
                    % (idx, m.version, self.eng['version']))
        if m1.psize != psize:
            raise VerifyError(
                'meta pages disagree on page size (%d vs %d)' % (psize, m1.psize))

        # These are structural findings, not fatal-to-parse: record and go on.
        if m0.pgno != 0:
            self.err('meta page 0 stores mp_pgno %d, not 0' % m0.pgno)
        if m1.pgno != 1:
            self.err('meta page 1 stores mp_pgno %d, not 1' % m1.pgno)
        # Commit parity: txn N writes slot N&1 stamping mm_txnid=N.  So slot 0
        # holds an even id, slot 1 an odd one (both 0 in a fresh file), and two
        # equal nonzero ids are impossible.
        if m0.txnid and (m0.txnid & 1):
            self.err('meta page 0 has odd txnid %d (parity violation)' % m0.txnid)
        if m1.txnid and not (m1.txnid & 1):
            self.err('meta page 1 has even txnid %d (parity violation)' % m1.txnid)
        if m0.txnid and m0.txnid == m1.txnid:
            self.err('meta pages share nonzero txnid %d' % m0.txnid)

        # mdb_env_pick_meta: the higher txnid wins (no MDB_PREVSNAPSHOT here).
        self.meta = m1 if m0.txnid < m1.txnid else m0
        self.next_pgno = self.meta.last_pg + 1
        if self.next_pgno < NUM_METAS:
            raise VerifyError('committed meta has mm_last_pg %d < %d'
                              % (self.meta.last_pg, NUM_METAS - 1))
        # The committed frontier must be backed by the file.  In practice the
        # file is never shorter than next_pgno*psize (a page is only "final and
        # free" after the file was extended to it), so the format's allowance
        # for a short free tail costs at most a generous slack here.  Bounding
        # next_pgno keeps a forged mm_last_pg from driving a huge bitmap
        # allocation -- the verifier must not itself fall over on hostile input.
        pages_in_file = self.filesize // self.psize
        cap = pages_in_file + max(pages_in_file, 1 << 20)
        if self.next_pgno > cap:
            raise VerifyError(
                'committed meta claims next_pgno %d, far beyond the %d page(s) '
                'a %d-byte file can hold' % (self.next_pgno, pages_in_file,
                                             self.filesize))

    # -- bitmap helpers --

    def _mark_live(self, pgno, what):
        """Mark `pgno` reachable.  Returns False (recording an error) if it is
        out of range or was already reachable -- a page reachable twice, or as
        a meta page, is corruption."""
        if pgno < NUM_METAS or pgno >= self.next_pgno:
            self.err('%s references out-of-range page %d' % (what, pgno))
            return False
        if self.live[pgno]:
            self.err('page %d is reachable more than once (via %s)' % (pgno, what))
            return False
        self.live[pgno] = 1
        return True

    # -- page-header field access (base = byte offset of the page/sub-page) --

    def _pg_pgno(self, buf, base):
        return _U64.unpack_from(buf, base)[0]

    def _pg_flags(self, buf, base):
        return _U16.unpack_from(buf, base + self.hdrsz - 6)[0]

    def _pg_lower(self, buf, base):
        return _U16.unpack_from(buf, base + self.hdrsz - 4)[0]

    def _pg_upper(self, buf, base):
        return _U16.unpack_from(buf, base + self.hdrsz - 2)[0]

    def _pg_pad(self, buf, base):
        return _U16.unpack_from(buf, base + self.hdrsz - 8)[0]

    def _pg_pages(self, buf, base):
        # mp_pages (uint32) overlaps mp_lower/mp_upper for overflow pages.
        return _U32.unpack_from(buf, base + self.hdrsz - 4)[0]

    def _pg_txnid(self, buf, base):
        return _U64.unpack_from(buf, base + 8)[0]     # 1.0 only

    def _numkeys(self, lower):
        return (lower - (self.hdrsz - self.pagebase)) >> 1

    def _node_off(self, buf, base, i):
        ptr = _U16.unpack_from(buf, base + self.hdrsz + 2 * i)[0]
        return base + ptr + self.pagebase

    def _check_btree_bounds(self, buf, base, span, where):
        """Validate mp_lower/mp_upper for a B-tree page or sub-page whose body
        occupies `span` bytes from `base`.  Returns (upper, numkeys) or None."""
        lower = self._pg_lower(buf, base)
        upper = self._pg_upper(buf, base)
        floor = self.hdrsz - self.pagebase
        if lower < floor or upper > span - self.pagebase or lower > upper:
            self.err('%s: bad mp_lower/mp_upper (%d/%d, span %d)'
                     % (where, lower, upper, span))
            return None
        return upper, self._numkeys(lower)

    # -- top-level drive --

    def verify(self):
        self.detect_engine()
        self.load_metas()

        self.live = bytearray(self.next_pgno)
        self.free = bytearray(self.next_pgno)

        # Core-DB flag/pad sanity (the engine refuses the file otherwise).
        if _bad_db_flags(self.meta.free.flags):
            self.err('freeDB has invalid flag combination 0x%x' % self.meta.free.flags)
        if _bad_db_flags(self.meta.main.flags):
            self.err('main DB has invalid flag combination 0x%x' % self.meta.main.flags)
        if self.meta.main.pad > self.psize - self.hdrsz:
            self.err('main DB md_pad %d exceeds usable page space' % self.meta.main.pad)

        # Walk the freeDB and the main DB (each may enqueue sub-trees).  The
        # freeDB uses the hardwired native-integer key comparator regardless of
        # its stored flags (mdb_cmp_long on FREE_DBI).
        self._verify_tree('freeDB', self.meta.free, _cmp_int, None, is_free=True)
        self._verify_tree('main', self.meta.main,
                          _key_cmp(self.meta.main.flags),
                          _data_cmp(self.meta.main.flags), is_free=False)

        # Fold in the free-page lists collected from freeDB values.
        for txnid, data in self.free_records:
            self._decode_idl(txnid, data)

        # Global disjoint-and-cover check over [NUM_METAS, next_pgno).
        self._check_cover()
        return self.errors

    def _verify_tree(self, name, db, key_cmp, dup_cmp, is_free):
        """Validate one B-tree and cross-check its counters.

        Returns (branch, leaf): the page totals *including* any promoted dup
        sub-trees, since a DUPSORT DB's md_branch/leaf_pages are that aggregate
        (mdb_subdb_adjust folds each dup sub-tree's deltas into its parent).
        A named sub-DB is a wholly independent tree instead: it does not fold
        into its container and is verified on its own afterwards."""
        if db.root == P_INVALID:
            if (db.branch_pages or db.leaf_pages or db.overflow_pages or
                    db.entries or db.depth):
                self.err('%s DB is empty (no root) but has nonzero counters' % name)
            return 0, 0
        if db.depth > CURSOR_STACK:
            self.err('%s DB md_depth %d exceeds %d' % (name, db.depth, CURSOR_STACK))
            return 0, 0
        if db.root < NUM_METAS or db.root >= self.next_pgno:
            self.err('%s DB root page %d out of range' % (name, db.root))
            return 0, 0

        ctx = {
            'name': name, 'db': db, 'is_free': is_free,
            'key_cmp': key_cmp, 'dup_cmp': dup_cmp,
            'branch': 0, 'leaf': 0, 'overflow': 0, 'entries': 0,
            'maxdepth': 0, 'last_key': None, 'named_subtrees': [],
            'dup_branch': 0, 'dup_leaf': 0,
        }
        self._walk_page(ctx, db.root, 1)

        total_branch = ctx['branch'] + ctx['dup_branch']
        total_leaf = ctx['leaf'] + ctx['dup_leaf']
        self._check_counts(name, db, total_branch, total_leaf,
                           ctx['overflow'], ctx['entries'], ctx['maxdepth'])

        for sub in ctx['named_subtrees']:
            self._verify_tree(sub.name, sub.db, sub.key_cmp, sub.dup_cmp,
                              is_free=False)
        return total_branch, total_leaf

    def _check_counts(self, name, db, branch, leaf, overflow, entries, depth):
        for label, walked, claimed in (
                ('branch pages', branch, db.branch_pages),
                ('leaf pages', leaf, db.leaf_pages),
                ('overflow pages', overflow, db.overflow_pages),
                ('entries', entries, db.entries),
                ('depth', depth, db.depth)):
            if walked != claimed:
                self.err('%s DB: walked %d %s, record says %d'
                         % (name, walked, label, claimed))

    def _walk_page(self, ctx, pgno, depth):
        if self.full:
            return
        name = ctx['name']
        if not self._mark_live(pgno, '%s DB' % name):
            return
        buf = self.read_page(pgno)
        if buf is None:
            self.err('%s DB: reachable page %d is beyond EOF or truncated'
                     % (name, pgno))
            return

        if self._pg_pgno(buf, 0) != pgno:
            self.err('%s DB: page %d stores mp_pgno %d'
                     % (name, pgno, self._pg_pgno(buf, 0)))

        flags = self._pg_flags(buf, 0)
        self._check_not_dirty(name, pgno, buf, flags)

        if flags & P_META:
            self.err('%s DB: meta page %d reachable as a tree page' % (name, pgno))
            return
        if flags & P_SUBP:
            self.err('%s DB: standalone P_SUBP page %d' % (name, pgno))
            return

        if flags & P_BRANCH:
            if flags & (P_LEAF | P_LEAF2 | P_OVERFLOW):
                self.err('%s DB: page %d has mixed page flags 0x%x' % (name, pgno, flags))
                return
            ctx['maxdepth'] = max(ctx['maxdepth'], depth)
            ctx['branch'] += 1
            self._walk_branch(ctx, pgno, buf, depth)
        elif flags & P_LEAF2:
            ctx['maxdepth'] = max(ctx['maxdepth'], depth)
            ctx['leaf'] += 1
            self._walk_leaf2(ctx, pgno, buf)
        elif flags & P_LEAF:
            ctx['maxdepth'] = max(ctx['maxdepth'], depth)
            ctx['leaf'] += 1
            self._walk_leaf(ctx, pgno, buf)
        else:
            self.err('%s DB: page %d has no valid page type (flags 0x%x)'
                     % (name, pgno, flags))

    def _check_not_dirty(self, name, pgno, buf, flags):
        if self.eng['has_txnid']:
            txnid = self._pg_txnid(buf, 0)
            if txnid > self.meta.txnid:
                self.err('%s DB: page %d has mp_txnid %d newer than committed '
                         'meta txnid %d' % (name, pgno, txnid, self.meta.txnid))
        elif flags & P_DIRTY_09:
            self.err('%s DB: page %d carries P_DIRTY at rest' % (name, pgno))

    def _walk_branch(self, ctx, pgno, buf, depth):
        name = ctx['name']
        b = self._check_btree_bounds(buf, 0, self.psize, '%s DB page %d' % (name, pgno))
        if b is None:
            return
        upper, numkeys = b
        if numkeys < 1:
            self.err('%s DB: branch page %d has no children' % (name, pgno))
            return
        if depth >= CURSOR_STACK:
            self.err('%s DB: tree deeper than %d levels' % (name, CURSOR_STACK))
            return
        prev_key = None
        for i in range(numkeys):
            o = self._node_off(buf, 0, i)
            if o + 8 > self.psize or o < upper + self.pagebase:
                self.err('%s DB: branch page %d node %d out of bounds' % (name, pgno, i))
                continue
            lo = _U16.unpack_from(buf, o)[0]
            hi = _U16.unpack_from(buf, o + 2)[0]
            nflags = _U16.unpack_from(buf, o + 4)[0]
            ksize = _U16.unpack_from(buf, o + 6)[0]
            child = lo | (hi << 16) | (nflags << 32)
            if o + 8 + ksize > self.psize:
                self.err('%s DB: branch page %d node %d key overruns page'
                         % (name, pgno, i))
                continue
            # Node 0's separator key is empty by construction; skip ordering.
            if i > 0 and ksize:
                key = bytes(buf[o + 8:o + 8 + ksize])
                if prev_key is not None and ctx['key_cmp'](key, prev_key) <= 0:
                    self.err('%s DB: branch page %d separator keys out of order '
                             'at node %d' % (name, pgno, i))
                prev_key = key
            self._walk_page(ctx, child, depth + 1)

    def _walk_leaf(self, ctx, pgno, buf):
        name = ctx['name']
        b = self._check_btree_bounds(buf, 0, self.psize, '%s DB page %d' % (name, pgno))
        if b is None:
            return
        upper, numkeys = b
        for i in range(numkeys):
            o = self._node_off(buf, 0, i)
            if o + 8 > self.psize or o < upper + self.pagebase:
                self.err('%s DB: leaf page %d node %d header out of bounds'
                         % (name, pgno, i))
                continue
            lo = _U16.unpack_from(buf, o)[0]
            hi = _U16.unpack_from(buf, o + 2)[0]
            nflags = _U16.unpack_from(buf, o + 4)[0]
            ksize = _U16.unpack_from(buf, o + 6)[0]
            dsize = lo | (hi << 16)
            koff = o + 8
            if koff + ksize > self.psize:
                self.err('%s DB: leaf page %d node %d key overruns page' % (name, pgno, i))
                continue
            key = bytes(buf[koff:koff + ksize])
            if ctx['last_key'] is not None and ctx['key_cmp'](key, ctx['last_key']) <= 0:
                self.err('%s DB: leaf keys out of order at page %d node %d'
                         % (name, pgno, i))
            ctx['last_key'] = key
            self._leaf_value(ctx, pgno, i, buf, o, nflags, ksize, dsize, key)

    def _leaf_value(self, ctx, pgno, i, buf, o, nflags, ksize, dsize, key):
        name, db = ctx['name'], ctx['db']

        if (nflags & F_BIGDATA) and (nflags & F_DUPDATA):
            self.err('%s DB: leaf page %d node %d has F_BIGDATA|F_DUPDATA'
                     % (name, pgno, i))
            return
        if (nflags & F_DUPDATA) and not (db.flags & MDB_DUPSORT):
            self.err('%s DB: leaf page %d node %d has F_DUPDATA in a non-DUPSORT DB'
                     % (name, pgno, i))
            return
        if ctx['is_free'] and (nflags & (F_SUBDATA | F_DUPDATA)):
            self.err('freeDB: page %d node %d value has sub-DB/dup flags 0x%x'
                     % (pgno, i, nflags))
            return

        doff = o + 8 + (_even(ksize) if self.eng['has_txnid'] else ksize)

        if nflags & F_BIGDATA:
            data = self._bigdata(ctx, pgno, i, buf, doff, dsize, want_bytes=ctx['is_free'])
            ctx['entries'] += 1
            if ctx['is_free']:
                self._free_record(pgno, i, key, data)
            return

        if doff + dsize > self.psize:
            self.err('%s DB: leaf page %d node %d data overruns page' % (name, pgno, i))
            return
        data = bytes(buf[doff:doff + dsize])

        if ctx['is_free']:
            ctx['entries'] += 1
            self._free_record(pgno, i, key, data)
            return

        if nflags & F_SUBDATA:
            if dsize != SIZEOF_MDB_DB:
                self.err('%s DB: leaf page %d node %d F_SUBDATA size %d != %d'
                         % (name, pgno, i, dsize, SIZEOF_MDB_DB))
                return
            subdb = _Db(buf, doff)
            if nflags & F_DUPDATA:
                # A promoted dup set.  Its items count toward this DUPSORT DB's
                # entry total; its pages fold into this DB's md_branch/leaf
                # counters (mdb_subdb_adjust); and it is ordered by *this* DB's
                # data comparator -- the sub-DB record's own flags drive only
                # its LEAF2 layout.  The record is an engine-internal dummy, not
                # a user DB, so it follows different flag/pad rules.
                self._validate_dup_subdb(name, pgno, i, subdb)
                ctx['entries'] += subdb.entries
                subname = '%s[dup@p%d.n%d]' % (name, pgno, i)
                sb, sl = self._verify_tree(subname, subdb, ctx['dup_cmp'], None,
                                           is_free=False)
                ctx['dup_branch'] += sb
                ctx['dup_leaf'] += sl
            else:
                # A named database: one entry here; a wholly separate tree with
                # its own comparators, not folded into this DB's page counts.
                self._validate_named_subdb(name, pgno, i, subdb)
                ctx['entries'] += 1
                subname = '%s/%s' % (name, _printable(key))
                ctx['named_subtrees'].append(_SubTree(
                    subname, subdb, _key_cmp(subdb.flags), _data_cmp(subdb.flags)))
            return

        if nflags & F_DUPDATA:
            ctx['entries'] += self._walk_subpage(ctx, pgno, i, buf, doff, dsize)
            return

        ctx['entries'] += 1     # plain single value

    def _bigdata(self, ctx, pgno, i, buf, doff, dsize, want_bytes):
        """Validate an F_BIGDATA overflow extent, mark its pages, and return
        the data bytes when `want_bytes` (freeDB IDLs need them) else None."""
        name = ctx['name']
        if self.eng['has_txnid']:
            if doff + 24 > self.psize:
                self.err('%s DB: page %d node %d F_BIGDATA node too small' % (name, pgno, i))
                return None
            op_pgno = _U64.unpack_from(buf, doff)[0]
            op_pages = _U64.unpack_from(buf, doff + 16)[0]
        else:
            if doff + 8 > self.psize:
                self.err('%s DB: page %d node %d F_BIGDATA node too small' % (name, pgno, i))
                return None
            op_pgno = _U64.unpack_from(buf, doff)[0]
            op_pages = None     # 0.9 carries the count only in the page header

        need = (self.hdrsz - 1 + dsize) // self.psize + 1
        if op_pages is not None and op_pages != need:
            self.err('%s DB: page %d node %d overflow op_pages %d != OVPAGES(%d)=%d'
                     % (name, pgno, i, op_pages, dsize, need))
        if need < 1 or op_pgno < NUM_METAS or op_pgno >= self.next_pgno \
                or op_pgno + need > self.next_pgno:
            self.err('%s DB: page %d node %d overflow extent [%d,+%d) out of range'
                     % (name, pgno, i, op_pgno, need))
            return None

        first = self.read_page(op_pgno)
        if first is None:
            self.err('%s DB: overflow page %d beyond EOF' % (name, op_pgno))
            return None
        if self._pg_pgno(first, 0) != op_pgno:
            self.err('%s DB: overflow page %d stores mp_pgno %d'
                     % (name, op_pgno, self._pg_pgno(first, 0)))
        oflags = self._pg_flags(first, 0)
        if not (oflags & P_OVERFLOW):
            self.err('%s DB: page %d node %d points at non-overflow page %d'
                     % (name, pgno, i, op_pgno))
        self._check_not_dirty(name, op_pgno, first, oflags)
        hdr_pages = self._pg_pages(first, 0)
        if hdr_pages != need:
            self.err('%s DB: overflow page %d header mp_pages %d != OVPAGES(%d)=%d'
                     % (name, op_pgno, hdr_pages, dsize, need))
        for k in range(need):
            self._mark_live(op_pgno + k, '%s DB overflow' % name)
        ctx['overflow'] += need

        if want_bytes:
            ext = self.read_extent(op_pgno, need)
            if ext is not None:
                return bytes(ext[self.hdrsz:self.hdrsz + dsize])
        return None

    def _walk_subpage(self, ctx, pgno, i, buf, doff, dsize):
        """Validate an inline P_SUBP sub-page (a key's duplicate set) and
        return the number of data items it holds."""
        name = ctx['name']
        if dsize < self.hdrsz:
            self.err('%s DB: page %d node %d sub-page smaller than a header'
                     % (name, pgno, i))
            return 0
        base = doff
        sflags = self._pg_flags(buf, base)
        if not (sflags & P_SUBP):
            self.err('%s DB: page %d node %d sub-page missing P_SUBP' % (name, pgno, i))
        end = base + dsize
        dup_cmp = ctx['dup_cmp']
        if sflags & P_LEAF2:
            pad = self._pg_pad(buf, base)
            numkeys = self._numkeys(self._pg_lower(buf, base))
            usable = dsize - self.hdrsz
            if pad == 0 or pad > usable or numkeys * pad > usable:
                self.err('%s DB: page %d node %d LEAF2 sub-page bad key size %d '
                         '(%d keys, %d usable)' % (name, pgno, i, pad, numkeys, usable))
                return numkeys
            last = None
            k0 = base + self.hdrsz
            for j in range(numkeys):
                dk = bytes(buf[k0 + j * pad:k0 + j * pad + pad])
                if dup_cmp and last is not None and dup_cmp(dk, last) <= 0:
                    self.err('%s DB: page %d node %d LEAF2 dup out of order at %d'
                             % (name, pgno, i, j))
                last = dk
            return numkeys

        b = self._check_btree_bounds(buf, base, dsize,
                                     '%s DB page %d node %d sub-page' % (name, pgno, i))
        if b is None:
            return 0
        upper, numkeys = b
        last = None
        for j in range(numkeys):
            o = self._node_off(buf, base, j)
            if o + 8 > end or o < base + upper + self.pagebase:
                self.err('%s DB: page %d node %d sub-page item %d out of bounds'
                         % (name, pgno, i, j))
                continue
            dksize = _U16.unpack_from(buf, o + 6)[0]
            if o + 8 + dksize > end:
                self.err('%s DB: page %d node %d sub-page item %d overruns'
                         % (name, pgno, i, j))
                continue
            dk = bytes(buf[o + 8:o + 8 + dksize])
            if dup_cmp and last is not None and dup_cmp(dk, last) <= 0:
                self.err('%s DB: page %d node %d dup data out of order at %d'
                         % (name, pgno, i, j))
            last = dk
        return numkeys

    def _walk_leaf2(self, ctx, pgno, buf):
        # A LEAF2 page belongs to a DUPFIXED (sub-)tree: fixed-size keys packed
        # after the header, no node pointers, no data.
        name, db = ctx['name'], ctx['db']
        pad = self._pg_pad(buf, 0)
        numkeys = self._numkeys(self._pg_lower(buf, 0))
        usable = self.psize - self.hdrsz
        if pad == 0 or pad > usable or numkeys * pad > usable:
            self.err('%s DB: LEAF2 page %d bad key size %d (%d keys, %d usable)'
                     % (name, pgno, pad, numkeys, usable))
            return
        if db.pad and pad != db.pad:
            self.err('%s DB: LEAF2 page %d key size %d disagrees with md_pad %d'
                     % (name, pgno, pad, db.pad))
        k0 = self.hdrsz
        last = ctx['last_key']
        for j in range(numkeys):
            key = bytes(buf[k0 + j * pad:k0 + j * pad + pad])
            if last is not None and ctx['key_cmp'](key, last) <= 0:
                self.err('%s DB: LEAF2 page %d keys out of order at %d' % (name, pgno, j))
            last = key
        ctx['last_key'] = last
        ctx['entries'] += numkeys

    def _validate_named_subdb(self, name, pgno, i, subdb):
        # A user-visible named database (F_SUBDATA only).  Its record follows
        # the same rules the engine enforces for the core DBs.  Note its md_pad
        # is legitimately 0 even when DUPFIXED: the DUPFIXED-ness lives in its
        # dup sub-trees' LEAF2 pages, not in its own main-tree pages.
        if subdb.flags & ~PERSISTENT_FLAGS:
            self.err('%s DB: page %d node %d named sub-DB has invalid flags 0x%x'
                     % (name, pgno, i, subdb.flags))
        if _bad_db_flags(subdb.flags):
            self.err('%s DB: page %d node %d named sub-DB bad flag combination 0x%x'
                     % (name, pgno, i, subdb.flags))
        if subdb.pad > self.psize - self.hdrsz:
            self.err('%s DB: page %d node %d named sub-DB md_pad %d too large'
                     % (name, pgno, i, subdb.pad))
        if subdb.depth > CURSOR_STACK:
            self.err('%s DB: page %d node %d named sub-DB md_depth %d too deep'
                     % (name, pgno, i, subdb.depth))

    def _validate_dup_subdb(self, name, pgno, i, subdb):
        # An engine-internal record for a promoted duplicate set (mdb_cursor_put
        # "prep_subDB").  Its flags are exactly one of {0, MDB_DUPFIXED,
        # MDB_DUPFIXED|MDB_INTEGERKEY}; a DUPFIXED one carries the fixed key
        # size in md_pad and must be nonzero.  BAD_DB_FLAGS does NOT apply here
        # -- DUPFIXED without DUPSORT is exactly how these are written.
        allowed = (0, MDB_DUPFIXED, MDB_DUPFIXED | MDB_INTEGERKEY)
        if subdb.flags not in allowed:
            self.err('%s DB: page %d node %d dup sub-DB has unexpected flags 0x%x'
                     % (name, pgno, i, subdb.flags))
        if subdb.pad > self.psize - self.hdrsz:
            self.err('%s DB: page %d node %d dup sub-DB md_pad %d too large'
                     % (name, pgno, i, subdb.pad))
        if (subdb.flags & MDB_DUPFIXED) and not subdb.pad:
            self.err('%s DB: page %d node %d DUPFIXED dup sub-DB has md_pad 0'
                     % (name, pgno, i))
        if subdb.depth > CURSOR_STACK:
            self.err('%s DB: page %d node %d dup sub-DB md_depth %d too deep'
                     % (name, pgno, i, subdb.depth))

    # -- freeDB records --

    def _free_record(self, pgno, i, key, data):
        if len(key) != 8:
            self.err('freeDB: page %d node %d key size %d != 8' % (pgno, i, len(key)))
            return
        txnid = int.from_bytes(key, _ORDER)
        if txnid < 1 or txnid > self.meta.txnid:
            self.err('freeDB: record txnid %d outside [1, %d]' % (txnid, self.meta.txnid))
        if data is None:
            self.err('freeDB: page %d node %d value unreadable (overflow)' % (pgno, i))
            return
        self.free_records.append((txnid, data))

    def _decode_idl(self, txnid, data):
        if len(data) < 8 or len(data) % 8 != 0:
            self.err('freeDB: record %d value size %d is not a positive multiple '
                     'of 8' % (txnid, len(data)))
            return
        count = int.from_bytes(data[0:8], _ORDER)
        if count != len(data) // 8 - 1:
            self.err('freeDB: record %d count %d does not match value size %d'
                     % (txnid, count, len(data)))
            return
        prev = None
        for k in range(1, count + 1):
            pg = int.from_bytes(data[8 * k:8 * k + 8], _ORDER)
            if prev is not None and pg >= prev:
                self.err('freeDB: record %d page list not strictly descending at '
                         'index %d' % (txnid, k))
            prev = pg
            if pg < NUM_METAS or pg >= self.next_pgno:
                self.err('freeDB: record %d lists out-of-range page %d' % (txnid, pg))
                continue
            if self.free[pg]:
                self.err('freeDB: page %d listed in more than one record' % pg)
            self.free[pg] = 1

    def _check_cover(self):
        both = neither = 0
        first_both = first_neither = None
        for pg in range(NUM_METAS, self.next_pgno):
            live, free = self.live[pg], self.free[pg]
            if live and free:
                both += 1
                if first_both is None:
                    first_both = pg
            elif not live and not free:
                neither += 1
                if first_neither is None:
                    first_neither = pg
        if both:
            self.err('%d page(s) are both reachable and in the freeDB (first: '
                     'page %d)' % (both, first_both))
        if neither:
            self.err('%d page(s) are neither reachable nor in the freeDB -- a '
                     'leak (first: page %d)' % (neither, first_neither))


def _printable(key):
    try:
        s = key.decode('ascii')
    except Exception:
        return key.hex()
    return s if s.isprintable() else key.hex()


# --- Public API ----------------------------------------------------------

def resolve_data_path(path, subdir=None):
    """Return the path to the actual ``data.mdb`` file.

    If `subdir` is None it is inferred: a directory is treated as an LMDB
    environment directory (``<path>/data.mdb``); anything else is the data file
    itself (an environment opened with ``subdir=False``)."""
    if subdir is None:
        subdir = os.path.isdir(path)
    if subdir:
        return os.path.join(path, 'data.mdb')
    return path


def verify(path, subdir=None):
    """Fully verify an LMDB data file offline.

    `path` is an environment (a directory, or a single data file); see
    `resolve_data_path`.  Returns a list of human-readable problem strings; an
    empty list means the file passed every invariant checked.

    Raises `VerifyError` only when verification cannot begin at all."""
    data_path = resolve_data_path(path, subdir)
    fd = os.open(data_path, os.O_RDONLY)
    try:
        v = _Verifier(fd, os.fstat(fd).st_size)
        return v.verify()
    finally:
        os.close(fd)
