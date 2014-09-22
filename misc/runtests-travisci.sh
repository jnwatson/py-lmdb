#!/bin/bash -ex

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

quiet python2.5 -measy_install py==1.4.20 pytest==2.5.2
quiet python2.6 -measy_install py==1.4.20 pytest==2.5.2 cffi
quiet python2.7 -measy_install py==1.4.20 pytest==2.5.2 cffi
quiet python3.1 -measy_install py==1.4.20 pytest==2.5.2 cffi argparse
quiet python3.2 -measy_install py==1.4.20 pytest==2.5.2 cffi
quiet python3.3 -measy_install py==1.4.20 pytest==2.5.2 cffi
quiet pypy -measy_install py==1.4.20 pytest==2.5.2

source misc/runtests-ubuntu-12-04.sh
