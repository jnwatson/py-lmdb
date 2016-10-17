#!/bin/bash -ex

source misc/helpers.sh

quiet() {
    "$@" > /tmp/$$ || { cat /tmp/$$; return 1; }
}

# Delete Travis PyPy or it'll supercede the PPA version.
rm -rf /usr/local/pypy/bin /usr/local/lib/pypy2.7
find /usr/lib -name '*setuptools*' | xargs rm -rf
find /usr/local/lib -name '*setuptools*' | xargs rm -rf

quiet add-apt-repository -y ppa:fkrull/deadsnakes
quiet add-apt-repository -y ppa:pypy
quiet apt-get -qq update
quiet apt-get install --force-yes -qq python{2.5,2.6}-dev libffi-dev gdb

wget -qO ez_setup_24.py \
    https://raw.githubusercontent.com/pypa/setuptools/bootstrap-py24/ez_setup.py
wget -q https://raw.githubusercontent.com/pypa/setuptools/bootstrap/ez_setup.py

quiet python2.5 ez_setup_24.py
quiet python2.6 ez_setup.py

quiet python2.5 -measy_install py==1.4.20 pytest==2.5.2
quiet python2.6 -measy_install py==1.4.20 pytest==2.5.2 cffi

native python2.5
native python2.6
cffi python2.6


[ "$fail" ] && exit 1
exit 0
