This is a universal Python binding for the LMDB ‘Lightning’ Database.

See [the documentation](https://lmdb.readthedocs.io) for more information.

### CI State

| Platform | Branch | Status |
| -------- | ------ | ------ |
| UNIX | ``master`` | [![master](https://travis-ci.org/jnwatson/py-lmdb.png?branch=master)](https://travis-ci.org/jnwatson/py-lmdb/branches) |
| Windows | ``master`` | [![master](https://ci.appveyor.com/api/projects/status/cx2sau39bufi3t0t/branch/master?svg=true)](https://ci.appveyor.com/project/NicWatson/py-lmdb/branch/master) |
| UNIX | ``release`` | [![release](https://travis-ci.org/jnwatson/py-lmdb.png?branch=release)](https://travis-ci.org/jnwatson/py-lmdb/branches) |
| Windows | ``release`` | [![release](https://ci.appveyor.com/api/projects/status/cx2sau39bufi3t0t/branch/release?svg=true)](https://ci.appveyor.com/project/NicWatson/py-lmdb/branch/release) |

If you care whether the tests are passing, check out the repository and execute
the tests under your desired target Python release, as the Travis CI build has
a bad habit of breaking due to external factors approximately every 3 months.

# Python Version Support Statement

This project has been around for a while.  Previously, it supported all the way back to before 2.5.  Currently py-lmdb
supports Python 2.7, Python >= 3.4, and pypy.

Python 2.7 is now end-of-life.  If you are still using Python 2.7, you should strongly considering porting to Python
3.

That said, this project will continue to support running on Python 2.7 until Travis CI or Appveyor remove support for
it.

# Sponsored by The Vertex Project

My current employer, [The Vertex Project](https://vertex.link/) is generously sponsoring my time to maintain py-lmdb.
If you like open source and systems programming in Python, check us out.
