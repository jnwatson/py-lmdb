
lmdb
====

`http://github.com/dw/py-lmdb <http://github.com/dw/py-lmdb>`_

.. currentmodule:: lmdb

.. toctree::
    :hidden:
    :maxdepth: 2

This is a wrapper around the OpenLDAP MDB 'Lightning Database' library. The
wrapper is not yet thoroughly documented or heavily tested, but it already
works well.

Since it uses the `cffi <http://cffi.readthedocs.org/en/>`_ module to wrap MDB,
it is compatible with CPython and PyPy.

More information on MDB can be found at:

* `Howard Chu's web site <http://symas.com/mdb/>`_
* `UKUUG introductory paper <http://symas.com/mdb/20120322-UKUUG-MDB-txt.pdf>`_
* `git://git.openldap.org/openldap.git <git://git.openldap.org/openldap.git>`_
  (branch ``mdb.master``)

To install the Python module, ensure a C compiler and `pip` or `easy_install`
are available, and type:

    ::

        pip install lmdb
        # or
        easy_install lmdb

*Note:* the cffi library depends on ``libffi``, so you may need to install the
development package for it. On Debian/Ubuntu:

    ::

        apt-get install libffi-dev


Introduction
++++++++++++

MDB is interesting because:

* Like SQLite and LevelDB it is small, transactional, and supports multi-reader
  single-writer access within a process.
* Like SQLite but unlike LevelDB, it supports multi-reader single-writer within
  a host.
* Like LevelDB but unlike SQLite, it exports an ordered-map interface.
* Like SQLite but unlike LevelDB, it supports multiple namespaces per database.
* Like SQLite 3.x prior to WAL mode, and most certainly unlike LevelDB, no
  surprising background processing or sporadic maintainance must occur in order
  to continue functioning under heavy write load, nor are writes throttled
  except by contention.
* Like SQLite and unlike LevelDB, predictable latency variance and runtime
  profile (no background threads).
* Similar to SQLite and unlike LevelDB, relies exclusively on the OS buffer
  cache to achieve good performance.
* Unlike SQLite and LevelDB, it is 32kb of object code and 6kLOC of C.
* Unlike SQLite and LevelDB, it is exclusively memory mapped and thus limited
  to 2GB databases on 32bit (e.g. ARM), however the resulting performance is
  excellent.
* Library and CPython extension (this package) are 120kb of object code.


Duplicate keys and fixed address mappings aren't done yet. Duplicate keys would
be nice to have, however fixed mappings interact badly with ASLR at least on OS
X and will not be supported, and it's really not a useful feature for scripting
languages anyway.

In future it would be nice to return buffers instead of strings to exploit the
zero copy nature of MDB's design, for example to allow in-place parsing of
JSON/XML documents or zero-copy serving HTTP clients directly from the OS
buffer cache.

As no packages are available the MDB library itself is currently bundled in
this repository and built statically into the module.


Interface
+++++++++

It is recommended that you also refer to the
`excellent Doxygen comments in the MDB source code <http://www.openldap.org/devel/gitweb.cgi?p=openldap.git;a=blob;f=libraries/liblmdb/lmdb.h>`_,
particularly with regard to thread safety.

.. autofunction:: lmdb.connect


Environment class
#################

.. autoclass:: lmdb.Environment
    :members:


Transaction class
#################

.. autoclass:: lmdb.Transaction
    :members:


Database class
##############

**Note:** unless working with sub-databases, you never need to explicitly
handle the :py:class:`Database` class, as all :py:class:`Transaction` methods
default to the main database.

.. autoclass:: lmdb.Database
    :members:


Cursor class
############

.. autoclass:: lmdb.Cursor
    :members:


Exceptions
##########

.. autoclass:: lmdb.Error
