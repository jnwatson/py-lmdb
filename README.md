
# py-lmdb Needs a Maintainer!

I simply don't have time for this project right now, and still the issues keep
piling in. Are you a heavy py-lmdb user and understand most bits of the API?
Got some spare time to give a binding you use a little love? Dab hand at C and
CFFI? Access to a Visual Studio build machine? Please drop me an e-mail: dw at
botanicus dot net. TLC and hand-holding will be provided as necessary, I just
have no bandwidth left to write new code.


### CI State

| Platform | Branch | Status |
| -------- | ------ | ------ |
| UNIX | ``master`` | [![master](https://travis-ci.org/dw/py-lmdb.png?branch=master)](https://travis-ci.org/dw/py-lmdb/branches) |
| Windows | ``master`` | [![master](https://ci.appveyor.com/api/projects/status/cx2sau39bufi3t0t/branch/master?svg=true)](https://ci.appveyor.com/project/dw/py-lmdb/branch/master) |
| UNIX | ``release`` | [![release](https://travis-ci.org/dw/py-lmdb.png?branch=release)](https://travis-ci.org/dw/py-lmdb/branches) |
| Windows | ``release`` | [![release](https://ci.appveyor.com/api/projects/status/cx2sau39bufi3t0t/branch/release?svg=true)](https://ci.appveyor.com/project/dw/py-lmdb/branch/release) |

If you care whether the tests are passing, check out the repository and execute
the tests under your desired target Python release, as the Travis CI build has
a bad habit of breaking due to external factors approximately every 3 months.
