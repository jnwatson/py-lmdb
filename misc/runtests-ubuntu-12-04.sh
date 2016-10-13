#!/bin/bash -ex

source misc/helpers.sh

native python2.5
native python2.6
native python2.7
native python3.3
cffi pypy
cffi python2.6
cffi python2.7
# cffi python3.1
cffi python3.2
cffi python3.3

[ "$fail" ] && exit 1
exit 0
