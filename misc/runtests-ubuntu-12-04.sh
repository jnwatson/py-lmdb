#!/bin/bash -ex

source misc/helpers.sh

native python2.5
native python2.6
cffi python2.6


[ "$fail" ] && exit 1
exit 0
