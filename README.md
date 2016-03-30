
### CI State

Release: [![release](https://travis-ci.org/dw/py-lmdb.png?branch=release)](https://travis-ci.org/dw/py-lmdb/branches)

Master: [![master](https://travis-ci.org/dw/py-lmdb.png?branch=master)](https://travis-ci.org/dw/py-lmdb/branches)

Note: owing to the prevailing diabolically cavelier attitudes of the Python
community (including the setuptools maintainers) towards compatibility issues,
it is quite possible the Travis icons indicate failure even though the tests
are passing. In most cases this is due to yet another facile breakage
introduced on older Python versions upstream (frivolous use of with: statement,
except x as y, Python 3.1-incompatible 3rd party progress bars, ...), and I
haven't yet wasted the pointless hours necessary to pair up working versions
again, a situation that occurs approximately every 3 months. So chances are, if
the build is broken, it's not because the tests have caught something.

If you care whether the tests are passing, check out the repository and execute
the tests under your desired target Python release.

All Python releases from 2.5 should be supported, except the below, for which I
have thrown my hands up in horror trying to maintain any kind of working build,
despite py-lmdb itself having no good reason to be incompatible with:

* Python 3.0
* Python 3.1
