# Plan: offline verifier (verify-then-trust)

Status: **implemented**. Landed as `lmdb/verify.py` (a pure-Python byte
walker), the `python -m lmdb verify` subcommand in `lmdb/tool.py`, and
`lmdb.verify.verify(path)`. Tests in `tests/verify_test.py` cover both engines
(positive round-trips on plain/dupsort/dupfixed/named/overflow/deletion
databases, plus the documented forgeries including the Tier-3 live-page-in-
freeDB signature). User-facing docs carry the threat model and operational
caveats under "Trust model and offline verification" in `docs/index.rst`. The
sections below are retained as the design record.

It is the counterpart to the runtime-hardening work tracked separately — see
`freedb-record-validation.md` for the Tier 0 defects that hardening closes.

## What shipped vs. the plan

The invariant catalog below was re-derived against the actual write paths and
the read-hardening patch series before implementation; the shipped checker adds
several invariants the outline stated only generically, all confirmed against
`mdb.c`:

- Both meta pages must parse (magic, version, `P_META`) before the committed
  one is picked by `mm_txnid`; the two must agree on `mm_psize` (the staggered
  header read anchors meta 1 at `meta0.mm_psize`); `mm_psize` must be a power of
  two that fits a meta page; commit-parity holds (`meta[0]` even txnid,
  `meta[1]` odd or zero, never equal-nonzero); each page stores its own
  `mp_pgno`.
- `BAD_DB_FLAGS` (DUPFIXED/INTEGERDUP/REVERSEDUP require DUPSORT), `md_depth <=
  CURSOR_STACK`, `md_root` in range, and the LEAF2 relation
  `NUMKEYS*md_pad <= usable` with the page's `mp_pad` agreeing with the record.
- A **DUPSORT DB's `md_branch/leaf_pages` are the aggregate** of its own tree
  plus every promoted dup sub-tree (`mdb_subdb_adjust`), and a promoted dup
  sub-DB record legitimately carries `MDB_DUPFIXED` **without** `MDB_DUPSORT`
  and is ordered by the *parent's* data comparator, not its own flags — both
  were false-positive traps a naive reading would have hit.
- Overflow: `OVPAGES(NODEDSZ) == mp_pages` (and, on 1.0, the node's `op_pages`),
  extent within `next_pgno`, and `F_BIGDATA|F_DUPDATA` rejected.

The one caveat carried forward: `verify` assumes the host's native byte order
and a 64-bit build (8-byte `pgno`/`txnid`/size fields), and reports anything
else as unsupported rather than mis-parsing it.

## Decision: reframe the threat model

No per-transaction check is 100% forge-proof short of revalidating the entire
database on every open. Against an adversary, detection coverage is
weakest-link: a 99% semantic detector buys nothing, because the attacker simply
routes through the uncovered 1%. Chasing the freeDB residual — or any semantic
forgery — inside a write transaction is therefore a losing game.

So we stop chasing per-transaction semantic detection and adopt **verify-then-trust**:

- Tell users, in user-facing documentation, not to open files they do not
  control.
- Provide an offline tool that fully verifies a file. Once a file passes, the
  user may treat it as trusted.

Everything undetectable per-transaction is trivially detectable at rest, because
at rest there is no legitimate dirty state to be ambiguous about: a reachable
page carrying a dirty marker, a live page listed in the freeDB, an `mp_txnid`
above the committed meta's — each is simply invalid for a verifier with a global
view. The online problem is hard because of ambiguity that does not exist
offline.

This retires the in-engine semantic #484 fixes previously proposed (the 0.9
dirty-list sort and the 1.0 allocation invariant). Those are exactly the
per-transaction semantic-detection layer this reframe replaces; the 1.0 one was
already carrying the freeDB caveat that motivated the reframe.

## Two layers, crisp identities

- **Engine (runtime hardening, worked separately):** guarantees no crash, no
  out-of-bounds access, no unbounded write — in O(1) per guard, constant-time
  and side-effect-free, at point of use. Anything requiring iteration over
  file-derived data does *not* belong here.
- **`verify` (this plan):** owns every global invariant and everything that
  requires looping over the file.

The read-hardening series is the floor `verify` stands on: opening an arbitrary
file to inspect or verify it must itself be survivable. The reframe does not
make that series a mistake — it is the precondition for the tool.

## Deliverable shape: a verb, not a policy

- `python -m lmdb verify <path>` subcommand in `lmdb/tool.py`, and/or an
  `Environment.verify()` method.
- An offline audit someone runs on a file they do not trust — **not** an
  open-time flag that changes how every caller behaves. This is the "untrusted
  file" surface #484 wanted to avoid, reframed as an action rather than a mode.
- Being Python-side, it covers `LMDB_PURE`, system-lib, and CFFI builds, which
  the patch series never reaches.
- Precedent: SQLite's `PRAGMA integrity_check`, BerkeleyDB's `db_verify`.
  Verify-then-trust is the established model for file-format trust.

## What "fully verify" must check (invariant catalog)

"Fully verify" must mean *every invariant the write paths assume*, or the tool
licenses trust the engine does not actually get — and the 99% problem reappears
one level up, now stamped as approved. At minimum:

- Meta-page selection matching each engine's open semantics.
- Full reachability: main DB, every `F_SUBDATA` sub-DB, dup sub-trees, overflow
  chains, and the freeDB itself — marking a bitmap of live pages.
- The structural bounds the read patches already enforce (`mp_lower`/`mp_upper`,
  LEAF2 pad, etc.).
- Key ordering and `md_entries` / `md_*_pages` consistency. The write paths
  assume a sorted tree; an unsorted tree that otherwise "verifies" is still
  hostile.
- No reachable dirty markers: on 0.9, no reachable `P_DIRTY`; on 1.0, no
  reachable page with `mp_txnid` greater than the committed meta's txnid (this is
  the #484 residual, trivially caught offline).
- freeDB rules: the structural pass (strictly descending, in range, count
  matches record size) plus the global check — the freeDB set and the live set
  must be disjoint and together cover `[NUM_METAS, next_pgno)`.
- Crash-consistency aware: a post-crash file legitimately contains unreachable
  garbage and freeDB entries for pages a dead transaction touched, and must
  still pass against the committed meta.

## Operational caveats (document these, or the model is unsound)

- **TOCTOU:** verify a private copy and ensure no untrusted writer can touch the
  file afterward, or verification proves nothing.
- **Lock file:** discard and regenerate `lock.mdb` rather than trusting one that
  arrived with the data file.
- **Co-writers:** "trusted" extends to every process that holds write access
  from then on.

## Design decisions to make deliberately

- **Verifier trust base:** a pure-Python byte-walker cannot be owned by hostile
  input but is slow on huge maps; an engine-backed walk is fast but spends the
  read-hardening budget as its safety margin. Pick with eyes open.
- **Fuzz harness:** mutate files, filter them through `verify`, then exercise the
  engine — to validate the central claim ("passes verify ⇒ engine will not crash
  or corrupt on it") and keep the verifier honest against upstream drift over
  time.
- **Cost:** O(database pages) plus a bitmap of `next_pgno` bits — ~32 MB for a
  1 TB database at 4 K pages. Inherently not a per-transaction check.

## Documentation changes

- **Express the threat model in user-facing documentation for the first time.**
  It is currently only implicit in patch notes. State it as a layered scope:
  - Reads of arbitrary files will not crash or corrupt the process (hardened,
    best-effort).
  - The semantic honesty of a file is a trust property, established by provenance
    or by `verify`.
  - The read-hardening series is the floor this stands on.
- Document the operational caveats above alongside the `verify` tool.
- Update `freedb-record-validation.md`: Tier 3 becomes this plan; note Tier 0 is
  handled in the runtime-hardening path.
- `ChangeLog` entry when it lands.

## Relationship to issue #484

Close #484 by documentation plus a pointer to `verify`, and drop the in-engine
#484 fixes. The freeDB residual that survived the online approach collapses
offline: a live page named in a freeDB record is simply a failed
disjoint-and-cover assertion.

## Out of scope for this plan

- **Tier 0 runtime hardening** — the other path, worked now (loop-free,
  constant-time guards; see `freedb-record-validation.md`).
- **Tier 2 page accounting** — skipped; it catches lazy forgeries, not
  deliberate ones, and trusts `md_*_pages`, which the attacker controls.
