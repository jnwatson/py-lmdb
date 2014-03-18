#!/bin/bash -ex

quiet() {
    "$@" > /tmp/$$ || { cat /tmp/$$; return 1; }
}

# Delete Travis PyPy or it'll supercede the PPA version.
rm -rf /usr/local/pypy/bin
find /usr/lib -name '*setuptools*' | xargs rm -rf
find /usr/local/lib -name '*setuptools*' | xargs rm -rf

quiet add-apt-repository -y ppa:fkrull/deadsnakes
quiet add-apt-repository -y ppa:pypy/pypy-weekly
quiet apt-get -qq update
quiet apt-get install --force-yes -qq \
    python{2.5,2.6,2.7,3.1,3.2,3.3}-dev \
    pypy-dev \
    libffi-dev

wget -qO ez_setup_24.py \
    https://bitbucket.org/pypa/setuptools/raw/bootstrap-py24/ez_setup.py
wget -q https://bitbucket.org/pypa/setuptools/raw/bootstrap/ez_setup.py

quiet python2.5 ez_setup_24.py
quiet python2.6 ez_setup.py
quiet python2.7 ez_setup.py
quiet python3.1 ez_setup.py
quiet python3.2 ez_setup.py
quiet python3.3 ez_setup.py
quiet pypy ez_setup.py

quiet python2.5 -measy_install pytest
quiet python2.6 -measy_install pytest cffi
quiet python2.7 -measy_install pytest cffi
quiet python3.1 -measy_install pytest cffi argparse
quiet python3.2 -measy_install pytest cffi
quiet python3.3 -measy_install pytest cffi
quiet pypy -measy_install pytest

source .runtests-ubuntu-12-04.sh
