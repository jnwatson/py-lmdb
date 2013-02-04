
lmdb
====

`http://github.com/dw/py-lmdb <http://github.com/dw/py-lmdb>`_

.. toctree::
    :hidden:
    :maxdepth: 2

This is a quick wrapper around the OpenLDAP MDB 'Lightning Database' library.
The wrapper is not yet thread-safe, thoroughly documented or tested
particularly heavily, but it already works well.

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
* Unlike SQLite or LevelDB, the code can be read in a single late evening.
* Library and CPython extension (this package) are 120kb of object code.


MDB may be unattractive because:

* Like SQLite but unlike LevelDB, the resulting database is not always
  optimally packed.
* Source is very traditional #ifdef soup.
* Logging and diagnostics are somewhat sparse.

Duplicate keys and fixed address mappings aren't done yet. Duplicate keys would
be nice to have, however fixed mappings interact badly with ASLR at least on OS
X and will not be supported, and it's really not a useful feature for scripting
languages anyway.

In future it would be nice to buffers instead of strings to exploit the zero
copy nature of MDB's design, for example to allow in-place parsing of JSON/XML
documents or zero-copy serving HTTP clients directly from the OS buffer cache.

As no packages are available the MDB library itself is currently bundled in
this repository and built statically into the module.



Interface
+++++++++

It is recommended that you also refer to the
`excellent Doxygen comments in the MDB source code <http://www.openldap.org/devel/gitweb.cgi?p=openldap.git;a=blob;f=libraries/liblmdb/lmdb.h>`_,
particularly with regard to thread safety.


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

.. autoclass:: lmdb.Database
    :members:


Cursor class
###########

.. autoclass:: lmdb.Cursor
    :members:


Exceptions
##########

.. autoclass:: lmdb.Error


