lmdb
----

Dirty wrapper around the OpenLDAP MDB library. It is not thread-safe,
documented, or tested particularly heavily, but it already works well.

More information on MDB can be found at:

    * `Howard Chu's web site <http://symas.com/mdb/>`_
    * `UKUUG introductory paper <http://symas.com/mdb/20120322-UKUUG-MDB-txt.pdf>`_
    * `git://git.openldap.org/openldap.git <git://git.openldap.org/openldap.git>`_
      (branch ``mdb.master``)

MDB is interesting because:

    * Like SQLite and LevelDB it is small, transactional, and supports
      multi-reader single-writer access within a process.
    * Like SQLite but unlike LevelDB, supports multi-reader single-writer
      within a single host.
    * Like LevelDB but unlike SQLite, it exports an ordered-map interface.
    * Like SQLite but unlike LevelDB, it supports multiple namespaces per
      database.
    * Like SQLite 3.x prior to WAL mode, and most certainly unlike LevelDB, no
      surprising quantities of background processing or sporadic maintainance
      must occur in order to continue functioning under heavy write load, nor
      are writes throttled except by contention.
    * Like SQLite and unlike LevelDB, predictable latency variance and
      runtime profile (no background threads).
    * Similar to SQLite and unlike LevelDB, modest (nonexistent) RAM
      requirements to achieve good write performance.
    * Unlike SQLite and LevelDB, it is 32kb of object code and 6kLOC of C.
    * Unlike SQLite and LevelDB, it is exclusively memory mapped and thus
      limited to 2GB databases on 32bit.

MDB sucks because:

    * Like SQLite but unlike LevelDB, the resulting database is not always
      optimally packed.
    * Its source code is #ifdef soup straight out of the 80s.
    * It desn't waste bytes on useful things like diagnostics.

Some features like duplicate keys and fixed address mappings aren't done yet.
Duplicate keys would be useful to have, however fixed mappings interact badly
with ASLR at least on OS X and will not be supported, and it's really not a
useful feature for scripting language anyway.

In future it would be nice to return buffer objects instead of strings, to
exploit the zero copy nature of MDB's design, for example to allow in-place
parsing of JSON/XML documents.

As no packages are available it is currently bundled in this repository and
built statically.
