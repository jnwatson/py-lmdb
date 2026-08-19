# Detecting a forged freeDB record

Status: **open**. Two confirmed defects in `mdb_page_alloc()` (Tier 0 below)
are unfixed on both engines. They are more severe than issue #484, cheaper to
fix, and independent of it — they should be filed separately and fixed first.

Written up from an investigation into issue #484's residual freeDB caveat: the
1.0 `mp_txnid` fix proposed there proves a page is "mine" via the allocation
invariant, which reaches back into freeDB records that a hostile file also
controls. The question this document answers is how such a record could be
detected.

## The short answer: detection has to start one level lower

Looking for a way to detect a *semantically* forged freeDB record — one that
lists a live page — turned up the fact that nothing validates freeDB records
**structurally** either. No patch in either series touches `mdb_page_alloc`. A
record read from disk is trusted completely, and this is exploitable well
beyond the MVCC problem in #484.

Two independent defects, both engines, confirmed by reproducer:

**The pgno values are unbounded.** The reuse path does
`pgno = mop[i]; goto search_done;` (`build/lib09/mdb.c:2339`,
`build/lib10/mdb.c:2833`) — and that `goto` jumps *past* the
`pgno + num >= env->me_maxpg` bound at `lib09:2422` / `lib10:2924`, which
guards only the fresh-page path. So a pgno taken from the freeDB is never
range-checked at all. Building a normal database, shifting every pgno in its
freeDB record up by 100000 against a 10 MB map (`me_maxpg` = 2560), and
reopening:

| | result |
| --- | --- |
| `writemap=True` | **SIGSEGV** — the store `np->mp_pgno = pgno` lands at `me_map + 4096×100002` |
| `writemap=False` | **completes silently**; `mdb_page_flush` pwrites at `pgno×psize`, growing `data.mdb` from 237,568 to **409,653,248 bytes** |

Identical on 0.9 and 1.0. Note the non-writemap case: an attacker-controlled
64-bit file offset, with `map_size` bypassed entirely, and *no error raised*.

**The element count is unvalidated.** `i = idl[0]` (`lib09:2396`,
`lib10:2898`) is never checked against `data.mv_size`, and
`mdb_midl_xmerge` then reads `idl[1..idl[0]]`. Forging count 54 → 200000 on a
440-byte record gives **SIGBUS** (reading past EOF inside the mapping) and
merges garbage pgnos into `me_pghead`. This one is independent of the first —
it needs no bad pgno.

As a side effect, forging duplicate pgnos trips `mdb_tassert(txn, rc == 0)` in
`mdb_page_dirty()`, which calls `abort()`. That is a denial of service where an
`MDB_CORRUPTED` return belongs.

## How to detect a forged record, in four tiers

**Tier 0 — structural validation, essentially free, always-on.** Right after
`mdb_node_read` returns the record in `mdb_page_alloc`, one linear pass over
the IDL before it reaches `mdb_midl_xmerge`: `data.mv_size` must match
`(idl[0]+1) * sizeof(MDB_ID)`; the entries must be strictly descending (which
gives duplicate detection within the record for free); and every pgno must lie
in `[NUM_METAS, txn->mt_next_pgno)`. That pass is O(n) over an array the merge
is about to walk anyway, so it roughly doubles a cost already being paid, on a
path that runs once per freeDB record consumed rather than per page. This
closes both confirmed defects and needs no new flag or API.

**Tier 1 — cheap semantic invariants, still always-on, partial coverage.**
After the merge, `me_pghead` must remain strictly descending — adjacent equals
mean two different records freed the same page without an intervening
reallocation, which cannot happen in a sane file. On 1.0 only, a page listed in
the record keyed by txnid K must satisfy `mp_txnid <= K`, since the page's last
writer necessarily precedes its freeing; that catches an attacker trying to
free a recently-written page. 0.9 cannot do this — its `MDB_page` header has no
txnid field, only `mp_pad` and `mp_flags`. Also convert the `mdb_page_dirty`
assertion to a `MDB_CORRUPTED` return.

**Tier 2 — page accounting.** This already exists: `mdb_audit()` at
`lib09:1831` / `lib10:2224` checks exactly
`freecount + count + NUM_METAS == mt_next_pgno`, summing freeDB record counts
against each DB's `md_branch_pages + md_leaf_pages + md_overflow_pages`. Adding
a live page's pgno to a freeDB record without removing it from the tree makes
that sum overshoot. But it is `#if (MDB_DEBUG) > 2`, it only `fprintf`s rather
than failing, it needs all named DBs open to be correct, and — the real limit —
it trusts `md_*_pages`, which the attacker also controls and can simply
decrement to rebalance. It catches lazy forgeries, not deliberate ones.

**Tier 3 — reachability traversal, the only sound detector.** Walk from the
meta page through every DB root (main, freeDB, and every `F_SUBDATA` sub-DB),
marking a bitmap of live pages, then assert that the freeDB and the live set
are disjoint and together cover `[NUM_METAS, next_pgno)`. This trusts nothing
but page structure, so it defeats even a fully coherent forgery. Cost is
O(database pages) plus a bitmap of `next_pgno` bits — 32 MB for a 1 TB database
at 4 K pages, which is fine. It cannot be a per-transaction check.

## On the "no new API" constraint

Tiers 0 through 2 are unconditional validation, consistent with how the rest of
the hardening series works, and introduce no caller-visible surface. Tier 3 is
inherently opt-in, but it does not have to be the "declare this file untrusted"
open-time flag that #484 wanted to avoid — it fits naturally as a verb rather
than a policy: a `verify` subcommand in `lmdb/tool.py`, or an
`Environment.verify()` method. That is an offline audit someone runs on a file
they do not trust, not a mode that changes how every open behaves.

## What this does to issue #484

The freeDB caveat on the proposed 1.0 `mp_txnid` fix survives, but shrinks a
lot. With Tier 0 in place, a pgno arriving from the freeDB is at least
guaranteed to be a real, in-range page, so the residual is narrowed to a
coherent forgery naming a genuinely live page — a consistency and durability
failure, no longer a memory-safety one. Detecting that specific case still
requires Tier 3.

## Upstream

Per #484's framing, upstream does not treat a crafted `data.mdb` as an attack
surface, so this likely stays a py-lmdb patch. An unbounded write with no error
raised is arguably severe enough to report anyway; undecided.

## Reproducers

`misc/freedb_repro.py` — self-contained, engine-selectable
(`--engine 0` / `--engine 1`), following the `misc/md_pad_repro.py` house
style. It builds a normal database, locates its freeDB IDL, forges only the
record bytes, and exercises each case in its own subprocess so a crash is
isolated and its signal is visible. On current master both engines report 4 of
5 cases not refused:

| case | result |
| --- | --- |
| out-of-range pgno, `writemap=False` | committed silently; `data.mdb` grows 237,568 -> 409,653,248 bytes |
| out-of-range pgno, `writemap=True` | SIGSEGV |
| unvalidated IDL count, `writemap=False` | SIGBUS |
| unvalidated IDL count, `writemap=True` | `MDB_CORRUPTED` (incidental downstream wrong-page-type check) |
| duplicate pgno, `writemap=False` | SIGABRT (`mdb_page_dirty` assertion) |

A minimal C reproducer and an OpenLDAP-format bug report have not been prepared.
