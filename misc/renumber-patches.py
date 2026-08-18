#!/usr/bin/env python
"""Renumber a patch series after editing a patch that is not last in it.

  misc/renumber-patches.py <patch-name>

Each patch in an ENGINES series is a diff against the tree state after all
preceding patches, so every hunk header is relative to that state.  Editing
a patch early in the series shifts the ones after it, and they then apply
with offsets -- or, once the drift is large enough, against the wrong code.

This replays each engine's series one patch at a time, and from <patch-name>
onward rewrites each patch's `@@` headers to match where its hunks actually
land now.  Only the headers change: the tool refuses to touch a patch whose
body differs from what the replay produces, because that means the edit
changed text the later patch anchors on, and no amount of renumbering will
fix it.  Repair that patch by hand and run this again.

Run `setup.py build_ext` afterwards and check the output is free of "offset"
and "fuzz" -- that is the real confirmation.
"""
import os
import re
import shutil
import subprocess
import sys

HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def series():
    """The ordered patch names for each engine, read from setup.py."""
    src = open(os.path.join(HERE, 'setup.py')).read()
    out = []
    for m in re.finditer(r"name='(v\d+)',\s*\n\s*tree='(\w+)',", src):
        rest = src[m.end():]
        block = rest[rest.index('patch_names=['):]
        block = block[:block.index(']')]
        out.append((m.group(1), m.group(2),
                    re.findall(r"'([a-z0-9\-]+)'", block)))
    return out


def canon(text):
    """Patch text with hunk line numbers and blank-context spelling erased.

    diff(1) writes the context line for a blank source line as a single
    space; parts of the series write it bare.  patch(1) accepts either, so
    the difference is not one worth reporting.
    """
    out = []
    for line in text.split('\n'):
        if line.startswith('@@'):
            line = '@@'
        elif line == ' ':
            line = ''
        out.append(line)
    # diff(1) ends its output with a newline and the hand-written patches
    # do not always, which is not a difference in the patch either.
    return '\n'.join(out).rstrip('\n')


def sections(text):
    """Split a patch into its per-file sections.

    Returns [(path, body)] in file order, where `path` is the file the
    section patches (taken from its `+++ b/...` line, basename only: the
    series diffs against upstream's `libraries/liblmdb/` layout and is
    applied with -p3) and `body` is that section's text from its first
    hunk header to the start of the next section.  Only `env-copy-txn`
    has more than one section, but renumbering from the first patch in a
    series has to handle it.
    """
    out = []
    path = body = None

    def flush():
        if path is not None:
            out.append((path, '\n'.join(body or [])))

    for line in text.split('\n'):
        if line.startswith('diff --git '):
            flush()
            path = body = None
        elif line.startswith('+++ '):
            path = os.path.basename(line[4:].split('\t')[0])
        elif line.startswith('@@') and body is None:
            body = [line]
        elif body is not None:
            body.append(line)
    flush()
    return out


def replay(tree, names, start, work):
    """Apply the series in order, renumbering from `start` onward."""
    cur = os.path.join(work, 'cur')
    shutil.rmtree(cur, ignore_errors=True)
    shutil.copytree(os.path.join(HERE, tree), cur,
                    ignore=shutil.ignore_patterns('py-lmdb'))
    a, b = os.path.join(work, 'a.c'), os.path.join(work, 'b.c')
    touched = []

    for i, name in enumerate(names):
        patchfile = os.path.join(HERE, tree, 'py-lmdb', name + '.patch')
        old = open(patchfile).read()
        parts = sections(old)
        before = {path: open(os.path.join(cur, path)).read()
                  for path, _ in parts}
        with open(patchfile, 'rb') as fp:
            r = subprocess.run(['patch', '-N', '-p3', '-d', cur],
                               stdin=fp, capture_output=True, text=True)
        if r.returncode:
            sys.exit('%s/%s does not apply:\n%s%s'
                     % (tree, name, r.stdout, r.stderr))
        if i < start:
            continue

        heads = []
        for path, body in parts:
            open(a, 'w').write(before[path])
            open(b, 'w').write(open(os.path.join(cur, path)).read())
            diff = subprocess.run(['diff', '-u', a, b],
                                  capture_output=True, text=True).stdout
            # Drop diff(1)'s ---/+++ lines; the patch keeps its own header.
            fresh = '\n'.join(diff.split('\n')[2:])
            if canon(fresh) != canon(body):
                sys.exit('%s/%s (%s): body differs from the replay, not '
                         'just line numbers.\nThe edit changed text this '
                         'patch anchors on; fix it by hand, then re-run.'
                         % (tree, name, path))
            heads += [l for l in fresh.split('\n') if l.startswith('@@')]

        heads = iter(heads)
        new = '\n'.join(next(heads) if l.startswith('@@') else l
                         for l in old.split('\n'))
        if new != old:
            open(patchfile, 'w').write(new)
            touched.append(name)
    return touched


def main():
    if len(sys.argv) != 2:
        sys.exit(__doc__.strip())
    target = sys.argv[1]
    work = os.path.join(HERE, 'build', 'renumber')
    os.makedirs(work, exist_ok=True)

    for tag, tree, names in series():
        if target not in names:
            print('%s: %s is not in this series, skipped' % (tag, target))
            continue
        touched = replay(tree, names, names.index(target), work)
        print('%s: renumbered %d of %d patches from %s onward%s'
              % (tag, len(touched), len(names) - names.index(target),
                 target, (': ' + ', '.join(touched)) if touched else ''))
    shutil.rmtree(work, ignore_errors=True)


if __name__ == '__main__':
    main()
