# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

py-lmdb is a Python binding for the LMDB (Lightning Memory-Mapped Database) key-value store. It provides two interchangeable implementations: a native CPython C extension (`lmdb/cpython.c`) and a CFFI wrapper (`lmdb/cffi.py`) for PyPy support.

## Build & Development

```bash
# Install in development mode (builds the C extension)
python setup.py develop

# Force CFFI variant instead of C extension
LMDB_FORCE_CFFI=1 python setup.py develop

# Use system liblmdb instead of bundled source
LMDB_FORCE_SYSTEM=1 python setup.py develop

# Use bundled LMDB source without py-lmdb patches
LMDB_PURE=1 python setup.py develop

# Enable compiler warnings (disabled by default with -w)
LMDB_MAINTAINER=1 python setup.py develop
```

## Running Tests

```bash
# Full test suite
python -m pytest

# Single test file
python -m pytest tests/env_test.py

# Single test
python -m pytest tests/env_test.py::EnvTest::test_open_default

# Via unittest directly
python -m unittest discover -s tests -p "*_test.py"
```

CI runs tests across a matrix: Python 3.8/3.11/3.13/3.14 + PyPy-3.10, on Linux/Windows/macOS, with both cpython and cffi implementations, and both patched and pure LMDB variants.

### Testing on Windows

This development environment is WSL, so Windows is directly accessible. There is a checkout at `/mnt/c/Users/Nic/proj/py-lmdb` and Windows Python at `C:\Users\Nic\AppData\Local\Programs\Python\Python312\python.exe`. To build and test on Windows:

```bash
# Copy changed files to the Windows checkout, then:
powershell.exe -NoProfile -Command "Set-Location C:\Users\Nic\proj\py-lmdb; C:\Users\Nic\AppData\Local\Programs\Python\Python312\python.exe setup.py develop 2>&1"
powershell.exe -NoProfile -Command "Set-Location C:\Users\Nic\proj\py-lmdb; C:\Users\Nic\AppData\Local\Programs\Python\Python312\python.exe -m pytest tests/ -x -q 2>&1"
```

Note: use `powershell.exe` with `Set-Location` rather than `cmd.exe`, since `cmd.exe` does not support UNC paths from WSL. Pip's upgrade notice goes to stderr, which PowerShell may report as an error even when the build succeeds — check the actual output.

## Architecture

### Dual Implementation Pattern

`lmdb/__init__.py` selects at import time between:
- **`lmdb/cpython.c`** (~4000 lines) — C extension for CPython. Defines `EnvObject`, `TransObject`, `CursorObject`, `DbObject`, `IterObject` as Python type structs. This is the default and performance-critical path.
- **`lmdb/cffi.py`** (~2400 lines) — Pure Python using CFFI. Same API surface. Used for PyPy or when `LMDB_FORCE_CFFI=1`.

Both implementations must expose an identical API. Changes to one typically require corresponding changes to the other.

### Bundled LMDB Library

The C library source lives in `lib/` (mdb.c, lmdb.h, midl.c, midl.h). Patches in `lib/py-lmdb/` are applied during build via `setup.py`:
- `env-copy-txn.patch` — enables copy/backup under active transactions
- `cursor-next-prev-uninitialized.patch` — fix for next/prev on uninitialized cursors after delete
- `win32-semaphore-lock.patch` — replace recursive Windows Mutexes with non-recursive Semaphores

`setup.py` handles patching, compiler flag configuration, and selecting between bundled vs system LMDB. On Windows, patches are applied via `patch_ng`; on other platforms, via the `patch` command.

**Upstream changes**: Never modify `lib/mdb.c`, `lib/midl.c`, `lib/lmdb.h`, or `lib/midl.h` directly. Instead, create a patch file in `lib/py-lmdb/` and add it to `setup.py`'s patch list (both the Windows `patch_ng` loop and the Unix `patch` commands). Patches must use git diff format (with a `diff --git` header line) so `patch_ng` correctly strips `a/`/`b/` prefixes. The patch must apply cleanly after all preceding patches. When fixing an upstream LMDB bug, also prepare a minimal C reproducer and a bug report to file with the LMDB project. Bug reports must conform to OpenLDAP bug-writing guidelines (https://bugs.openldap.org/page.cgi?id=bug-writing.html): one issue per report, concise summary (~60 chars), steps to reproduce, actual vs expected results, and build/platform info. Distinguish facts from speculation.

### Test Structure

Tests use unittest (`tests/testlib.py` provides `LmdbTest` base class with temp directory cleanup). Key test modules: `env_test.py`, `txn_test.py`, `cursor_test.py`, `crash_test.py`, `iteration_test.py`, `getmulti_test.py`. Test helpers include `putData()`, `putBigData()`, and pre-defined `KEYS`/`ITEMS`/`VALUES` constants.

### CLI Tool

`lmdb/tool.py` provides command-line database utilities (dump, restore, edit, etc.), invokable via `python -m lmdb`.

## Cutting a Release

1. **Update `ChangeLog`** — Add a new entry at the top: `YYYY-MM-DD X.Y.Z` followed by bullet points summarizing changes since the last release.
2. **Bump `__version__`** in `lmdb/__init__.py`.
3. **Update `setup.py` classifiers** if new Python versions need to be added.
4. **Commit** — Message: `Bump version to X.Y.Z for release`. Should touch `ChangeLog`, `lmdb/__init__.py`, and optionally `setup.py`.
5. **Tag** — Lightweight tag: `git tag py-lmdb_X.Y.Z`
6. **Push** — `git push origin master --tags`. The tag triggers the `publish` workflow job which builds wheels (CPython via cibuildwheel, PyPy via CFFI) and an sdist, then uploads to PyPI via trusted publishing. The push to master also triggers `publish-to-test-pypi`.
