#!/bin/bash
# Prepare Travis CI base image for CI. This ensures latest PyPy is installed
# (if required), and Python3.1 is installed from launchpad.net.

ENV="$1"
PKGS="$PKGS libffi-dev"

#[[ "$ENV" == *'31'* ]] && {
    # pip 1.4.1 broken with Python3.1, need the dev version in this case.
 #   echo "Replacing pip with dev version.."
 #   pip install -U 'https://github.com/pypa/pip/archive/02fff2b7a00a71ce8c8a98b671a5127490770093.zip#egg=pip'
 #   hash -r
 #   REPOS="$REPOS ppa:fkrull/deadsnakes"
 #   PKGS="$PKGS python3.1 python3.1-dev"
#}

[[ "$ENV" == *'pypy'* ]] && {
    REPOS="$REPOS ppa:pypy/ppa"
    PKGS="$PKGS pypy"
    # Delete Travis PyPy or it'll supercede the PPA version.
    rm -rf /usr/local/pypy/bin
}

[ "$REPOS" ] && {
    for repo in $REPOS; do 
        echo "Add repository $REPOS..."
        add-apt-repository -y $repo
    done
}

[ "$PKGS" ] && {
    echo "apt-get update..."
    apt-get -qq update
    echo "Installing $PKGS..."
    apt-get --force-yes -qq install $PKGS
}

pip install -U pip
pip install tox
