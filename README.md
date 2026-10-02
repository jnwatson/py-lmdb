This is a universal Python binding for the LMDB ‘Lightning’ Database.

See [the documentation](https://lmdb.readthedocs.io) for more information.

### LMDB 1.x and 0.9.x

py-lmdb bundles both LMDB release lines, 0.9.x and 1.x, whose database
formats are mutually unreadable. Existing databases in either format open
transparently, and new ones default to the 0.9.x format unless you choose
otherwise with `lib_version=`. See
[LMDB versions](https://lmdb.readthedocs.io/en/latest/#lmdb-versions) in the
documentation for details.

### CI State
[![master](https://github.com/jnwatson/py-lmdb/workflows/Build%20and%20test%20py-lmdb/badge.svg)](https://github.com/jnwatson/py-lmdb/actions/workflows/python-package.yml)

# Python Version Support Statement

This project has been around for a while.  Previously, it supported all the
way back to before Python 2.5.  Currently, py-lmdb supports Python >= 3.9
and pypy.

The last version of py-lmdb that supported Python 2.7 was 1.4.1.
