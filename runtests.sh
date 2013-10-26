#!/bin/sh

PYTHONPATH=tests python -munittest "$@" crash_test
