
lmdb
====

`http://github.com/dw/py-lmdb <http://github.com/dw/py-lmdb>`_

.. currentmodule:: lmdb

.. toctree::
    :hidden:
    :maxdepth: 2

This is a universal Python binding for the `LMDB 'Lightning' Database
<http://symas.com/mdb/>`_. Two implementations are provided and automatically
selected during installation, depending on host environment: a `cffi
<http://cffi.readthedocs.org/en/release-0.5/>`_ implementation that supports
`PyPy <http://www.pypy.org/>`_ and all versions of CPython >=2.6, and a custom
module that supports CPython 2.5-2.7 and >=3.3. Both implementations provide
the same interface.

LMDB is a tiny database with some excellent properties:

* Ordered-map interface (keys are always sorted)
* Reader/writer transactions: readers don't block writers and writers don't
  block readers. Each environment supports one concurrent write transaction.
* Read transactions are extremely cheap.
* Environments may be opened by multiple processes on the same host, making it
  ideal for working around Python's `GIL
  <http://wiki.python.org/moin/GlobalInterpreterLock>`_.
* Multiple named databases may be created with transactions covering all
  named databases.
* Memory mapped, allowing for zero copy lookup and iteration. This is
  optionally exposed to Python using the :py:func:`buffer` interface.
* Maintenance requires no external process or background threads.
* No application-level caching is required: LMDB relies entirely on the
  operating system's buffer cache.

Significant effort has been made to ensure the binding is as user-friendly as
possible, in particular by raising exceptions instead of crashing when
impossible operations are attempted, such as iterating a cursor when its
transaction has been aborted, or deleting a key when the environment has been
closed.

Installation
++++++++++++

For convenience, a supported version of LMDB is bundled with the binding and
built statically by default. If your system distribution includes LMDB, set the
``LMDB_FORCE_SYSTEM`` environment variable, and optionally ``LMDB_INCLUDEDIR``
and ``LMDB_LIBDIR`` prior to invoking ``setup.py``.

*Note:* on PyPy the wrapper depends on cffi which in turn depends on
``libffi``, so you may need to install the development package for it. Both
wrappers additionally depend on the CPython development headers when running
under CPython. On Debian/Ubuntu:

    ::

        apt-get install libffi-dev python-dev build-essential

To install the Python module, ensure a C compiler and `pip` or `easy_install`
are available and type:

    ::

        pip install lmdb
        # or
        easy_install lmdb

You may also use the cffi version on CPython. This is accomplished by setting
the ``LMDB_FORCE_CFFI`` environment variable before installation or before
module import with an existing installation:

    ::

        >>> import os
        >>> os.environ['LMDB_FORCE_CFFI'] = '1'

        >>> # cffi version is loaded.
        >>> import lmdb


Named Databases
+++++++++++++++

To use named databases you must call :py:func:`lmdb.open` or
:py:class:`lmdb.Environment` with a `max_dbs=` parameter set to the number of
databases required. This must be done by the first process or thread opening
the environment as it is used to allocate resources kept in shared memory.

.. caution::

    Named databases are implemented by *storing a special descriptor in the
    main database*. All databases in an environment *share the same file*.
    Because the descriptor is present in the main database, attempts to create
    a named database will fail if a key matching the database's name already
    exists. Furthermore *the key is visible to lookups and enumerations*. If
    your main database keyspace conflicts with the names you use for named
    databases, then move the contents of your main database to another named
    database.

    ::

        >>> env = lmdb.open('/tmp/test', max_dbs=2)
        >>> with env.begin(write=True) as txn
        ...     txn.put('somename', 'somedata')

        >>> # Error: database cannot share name of existing key!
        >>> subdb = env.open_db('somename')


Storage efficiency & limits
+++++++++++++++++++++++++++

LMDB groups records in pages matching the operating system memory manager's
page size which is usually 4096 bytes. In order to maintain its internal
structure each page must contain a minimum of 2 records, in addition to 8 bytes
per record and a 16 byte header. Due to this the engine is most space-efficient
when the combined size of any (8+key+value) combination does not exceed 2040
bytes.

When an attempt to store a record would exceed the maximum size, its value part
is written separately to one or more pages. Since the trailer of the last page
containing the record value cannot be shared with other records, it is more
efficient when large values are an approximate multiple of 4096 bytes, minus 16
bytes for an initial header.

Space usage can be monitored using :py:meth:`Environment.stat`:

        ::

            >>> pprint(env.stat())
            {'branch_pages': 1040L,
             'depth': 4L,
             'entries': 3761848L,
             'leaf_pages': 73658L,
             'overflow_pages': 0L,
             'psize': 4096L}

This database contains 3,761,848 records and no values were spilled
(``overflow_pages``).

By default record keys are limited to 511 bytes in length, however this can be
adjusted by rebuilding the library.


Buffers
+++++++

Since LMDB is memory mapped it is possible to access record data without keys
or values ever being copied by the kernel, database library, or application. To
exploit this the library can be instructed to return :py:func:`buffer` objects
instead of strings by passing `buffers=True` to :py:meth:`Environment.begin` or
:py:class:`Transaction`.

In Python :py:func:`buffer` objects can be used in many places where strings
are expected. In every way they act like a regular sequence: they support
slicing, indexing, iteration, and taking their length. Many Python APIs will
automatically convert them to bytestrings as necessary, since they also
implement ``__str__()``:

    ::

        >>> txn = env.begin(buffers=True)
        >>> buf = txn.get('somekey')
        >>> buf
        <read-only buffer ptr 0x12e266010, size 4096 at 0x10d93b970>

        >>> len(buf)
        4096
        >>> buf[0]
        'a'
        >>> buf[:2]
        'ab'
        >>> value = str(buf)
        >>> len(value)
        4096
        >>> type(value)
        <type 'str'>

It is also possible to pass buffers directly to many native APIs, for example
:py:meth:`file.write`, :py:meth:`socket.send`, :py:meth:`zlib.decompress` and
so on.

A buffer may be sliced without copying by passing it to :py:func:`buffer`:

    ::

        >>> # Extract bytes 10 through 210:
        >>> sub_buf = buffer(buf, 10, 200)
        >>> len(sub_buf)
        200

.. caution::

    In CPython buffers returned by :py:class:`Transaction` and
    :py:class:`Cursor` are reused, so that consecutive calls to
    :py:class:`Transaction.get` or any of the :py:class:`Cursor` methods will
    overwrite the objects that have already been returned. To preserve a value
    returned in a buffer, convert it to a string using :py:func:`str`.

    ::

        >>> txn = env.begin(write=True, buffers=True)
        >>> txn.put('key1', 'value1')
        >>> txn.put('key2', 'value2')

        >>> val1 = txn.get('key1')
        >>> vals1 = str(val1)
        >>> vals1
        'value1'
        >>> val2 = txn.get('key2')
        >>> str(val2)
        'value2'

        >>> # Caution: the buffer object is reused!
        >>> str(val1)
        'value2'

        >>> # But our string copy was preserved!
        >>> vals1
        'value1'

    In both PyPy and CPython, returned buffer objects *must be discarded* after
    their generating transaction has completed.


``writemap`` mode
+++++++++++++++++

When :py:class:`Environment` or :py:func:`open` is invoked with
``writemap=True``, the library will use a writeable memory mapping to directly
update storage. This improves performance at a cost to safety: it is possible
(though fairly unlikely) for buggy C code in the Python process to accidentally
overwrite the map, resulting in database corruption.

.. caution::

    This option may cause filesystems that don't support sparse files, such as
    OSX, to immediately preallocate `map_size=` bytes of underlying storage.


Transaction management
++++++++++++++++++++++

``MDB_NOTLS`` mode is used exclusively, which allows read transactions to
freely migrate across threads and for a single thread to maintain multiple read
transactions. This enables mostly care-free use of read transactions, for
example when using `gevent <http://www.gevent.org/>`_.

.. caution::

    While any reader exists, writers cannot reuse space in the database file
    that has become unused in later versions. Due to this, continual use of
    long-lived read transactions may cause the database to grow without bound.
    A lost reference to a read transaction will simply be aborted (and its
    reader slot freed) when the :py:class:`Transaction` is eventually garbage
    collected. This should occur immediately on CPython, but may be deferred
    indefinitely on PyPy.

    However the same is *not* true for write transactions: losing a reference
    to a write transaction can lead to deadlock, particularly on PyPy, since if
    the same process that lost the :py:class:`Transaction` reference
    immediately starts another write transaction, it will deadlock on its own
    lock. Subsequently the lost transaction may never be garbage collected
    (since the process is now blocked on itself) and the database will become
    unusable.

These problems are easily avoided by always wrapping :py:class:`Transaction` in
a ``with`` statement somewhere on the stack:

.. code-block:: python

    # Even if this crashes, txn will be correctly finalized.
    with env.begin() as txn:
        if txn.get('foo'):
            function_that_stashes_away_txn_ref(txn)
            function_that_leaks_txn_refs(txn)
            crash()


Interface
+++++++++

.. py:function:: lmdb.open(path, **kwargs)
   
   Shortcut for :py:class:`Environment` constructor.

.. autofunction:: lmdb.version


Environment class
#################

.. autoclass:: lmdb.Environment
    :members:


Transaction class
#################

.. autoclass:: lmdb.Transaction
    :members:


Cursor class
############

.. autoclass:: lmdb.Cursor
    :members:


Exceptions
##########

.. autoclass:: lmdb.Error ()
.. autoclass:: lmdb.KeyExistsError ()
.. autoclass:: lmdb.NotFoundError ()
.. autoclass:: lmdb.PageNotFoundError ()
.. autoclass:: lmdb.CorruptedError ()
.. autoclass:: lmdb.PanicError ()
.. autoclass:: lmdb.VersionMismatchError ()
.. autoclass:: lmdb.InvalidError ()
.. autoclass:: lmdb.MapFullError ()
.. autoclass:: lmdb.DbsFullError ()
.. autoclass:: lmdb.ReadersFullError ()
.. autoclass:: lmdb.TlsFullError ()
.. autoclass:: lmdb.TxnFullError ()
.. autoclass:: lmdb.CursorFullError ()
.. autoclass:: lmdb.PageFullError ()
.. autoclass:: lmdb.MapResizedError ()
.. autoclass:: lmdb.IncompatibleError ()
.. autoclass:: lmdb.BadRslotError ()
.. autoclass:: lmdb.BadTxnError ()
.. autoclass:: lmdb.BadValsizeError ()
.. autoclass:: lmdb.ReadonlyError ()
.. autoclass:: lmdb.InvalidParameterError ()
.. autoclass:: lmdb.LockError ()
.. autoclass:: lmdb.MemoryError ()
.. autoclass:: lmdb.DiskError ()


Threading control
#################

.. autofunction:: lmdb.enable_drop_gil


Command line tools
++++++++++++++++++

A rudimentary interface to most of the binding's functionality is provided.
These functions are useful for e.g. backup jobs.

::

    $ python -mlmdb --help
    Usage: python -mlmdb [options] <command>

    Basic tools for working with LMDB.

        copy: Consistent high speed backup an environment.
            python -mlmdb copy -e source.lmdb target.lmdb

        copyfd: Consistent high speed backup an environment to stdout.
            python -mlmdb copyfd -e source.lmdb > target.lmdb/data.mdb

        drop: Delete one or more named databases.
            python -mlmdb drop db1

        dump: Dump one or more databases to disk in 'cdbmake' format.
            Usage: dump [db1=file1.cdbmake db2=file2.cdbmake]

            If no databases are given, dumps the main database to 'main.cdbmake'.

        edit: Add/delete/replace values from a database.
            python -mlmdb edit --set key=value --set-file key=/path \
                       --add key=value --add-file key=/path/to/file \
                       --delete key

        get: Read one or more values from a database.
            python -mlmdb get [<key1> [<keyN> [..]]]

        readers: Display readers in the lock table
            python -mlmdb readers -e /path/to/db [-c]

            If -c is specified, clear stale readers.

        restore: Read one or more database from disk in 'cdbmake' format.
            python -mlmdb restore db1=file1.cdbmake db2=file2.cdbmake

            The special db name ":main:" may be used to indicate the main DB.

        rewrite: Re-create an environment using MDB_APPEND
            python -mlmdb rewrite -e src.lmdb -E dst.lmdb [<db1> [<dbN> ..]]

            If no databases are given, rewrites only the main database.

        shell: Open interactive console with ENV set to the open environment.

        stat: Print environment statistics.

        warm: Read environment into page cache sequentially.

        watch: Show live environment statistics

    Options:
      -h, --help            show this help message and exit
      -e ENV, --env=ENV     Environment file to open
      -d DB, --db=DB        Database to open (default: main)
      -r READ, --read=READ  Open environment read-only
      -S MAP_SIZE, --map_size=MAP_SIZE
                            Map size in megabytes (default: 10)
      -a, --all             Make "dump" dump all databases
      -T TXN_SIZE, --txn_size=TXN_SIZE
                            Writes per transaction (default: 1000)
      -E TARGET_ENV, --target_env=TARGET_ENV
                            Target environment file for "dumpfd"
      -x, --xxd             Print values in xxd format
      -M MAX_DBS, --max-dbs=MAX_DBS
                            Maximum open DBs (default: 128)
      --out-fd=OUT_FD       "copyfd" command target fd

      Options for "edit" command:
        --set=SET           List of key=value pairs to set.
        --set-file=SET_FILE
                            List of key pairs to read from files.
        --add=ADD           List of key=value pairs to add.
        --add-file=ADD_FILE
                            List of key pairs to read from files.
        --delete=DELETE     List of key=value pairs to delete.

      Options for "readers" command:
        -c, --clean         Clean stale readers? (default: no)

      Options for "watch" command:
        --csv               Generate CSV instead of terminal output.
        --interval=INTERVAL Interval size (default: 1sec)
        --window=WINDOW     Average window size (default: 10)



Implementation Notes
++++++++++++++++++++


Iterators
#########

It was tempting to make :py:class:`Cursor` directly act as an iterator, however
that would require overloading its `next()` method to mean something other than
the natural definition of `next()` on an LMDB cursor. It would additionally
introduce unintuitive state tied to the cursor that does not exist in LMDB:
such as iteration direction and the type of value yielded.

Instead a separate iterator is produced by `__iter__()`, `iternext()`, and
`iterprev()`, with easily described semantics regarding how they interact with
the cursor.


Memsink Protocol
################

If the ``memsink`` package is available during installation of the CPython
extension, then the resulting module's :py:class:`Transaction` object will act
as a `source` for the `Memsink Protocol
<https://github.com/dw/acid/issues/23>`_. This is an experimental protocol to
allow extension of LMDB's zero-copy design outward to other C types, without
requiring explicit management by the user.

This design is a work in progress; if you have an application that would
benefit from it, please leave a comment on the ticket above.


Deviations from LMDB API
########################

`mdb_dbi_close()`:
    This is not exposed since its use is perilous at best. Users must ensure
    all activity on the DBI has ceased in all processes before closing the
    handle. Failure to do this could result in "impossible" errors, or the
    DBI slot becoming reused, resulting in operations being serviced by the
    wrong named database. Leaving handles open wastes a tiny amount of memory,
    which seems a good price to avoid subtle data corruption.

:py:meth:`lmdb.Cursor.replace`, :py:meth:`lmdb.Cursor.pop`:
    There are no native equivalents to these calls, they just implement common
    operations in C to avoid a chunk of error prone, boilerplate Python from
    having to do the same.


Technology
##########

The binding is implemented twice: once using `cffi`, and once as native C
extension. This is since a `cffi` binding is necessary for PyPy, but its
performance on CPython is very poor. For good performance on CPython, only
Cython and a native extension are viable options. Initially Cython was used,
however this was abandoned due to the effort and relative mismatch involved
compared to writing a native extension.

Cython offers no lightweight ability to track object dependencies, and so the
best method to ensure crash-safety is managing a dict/list of weakrefs. While
it is technically possible to maintain inline lists with Cython, the result is
unnatural and much of the original benefit of Cython is lost.

Another problem with Cython is that good performance requires static typing,
and frequent visits to the autogenerated C files to figure out a performance
problem. In some places Cython makes it difficult to avoid conversions that
produce new objects, even though CPython provides macros for conversion-free
access. The choice is paying a needless performance cost, or intermixing Cython
code with chunks of C/Python API calls.


Invalidation lists
##################

Much effort has gone into avoiding crashes: when some object is invalidated
(e.g. due to :py:meth:`Transaction.abort`), child objects are updated to ensure
they don't access memory of the no-longer-existent resource, and that they
correspondingly free their own resources. On CPython this is accomplished by
weaving a linked list into all ``PyObject`` structures. This avoids the need to
maintain a separate heap-allocated structure, or produce excess ``weakref``
objects (which internally simply manage their own lists).

On `cffi` this isn't possible. Instead each object has a ``_deps`` dict that
maps dependent object IDs to corresponding weakrefs. Prior to invalidation
``_deps`` is walked to notify each dependent that the resource is about to
disappear.

Finally, each object may either store an explicit ``_invalid`` attribute and
check it prior to every operation, or rely on another mechanism to avoid the
crash resulting from using an invalidated resource. Instead of performing these
explicit tests continuously, on `cffi` a magic
``Some_LMDB_Resource_That_Was_Deleted_Or_Closed`` object is used. During
invalidation, all native handles are replaced with an instance of this object.
Since `cffi` cannot convert the magical object to a C type, any attempt to make
a native call will raise ``TypeError`` with a nice descriptive type name
indicating the problem. Hacky but efficient, and mission accomplished.


Argument parsing
################

The CPython module `parse_args()` may look "special", at best. The alternative
`PyArg_ParseTupleAndKeywords` performs continuous heap allocations and string
copies, resulting in a difference of 10,000 lookups/sec slowdown in a
particular microbenchmark. The 10k/sec slowdown could potentially disappear
given a sufficiently large application, so this decision needs revisited at
some stage.


Buffer mutation
###############

This violates the CPython API, and it really is a huge hack. Since the buffer
objects never change, the 2-tuple wrapping those objects for
:py:meth:`Cursor.item` need not change either, so a good deal of heap churn
can be avoided via the hack. The efficiency benefit of this hack may be
negligable at best.

There is a second motivation for this hack, and that is to neutralize any
returned `buffer` objects prior to a transaction ending or a cursor mutation.
Without such a hack, the user may attempt to read from the `buffer` later on
and become surprised at its content, or the resulting crash.

Still it really is candidate for eventual removal, after the effects of the
removal are tested.


ChangeLog
+++++++++

.. include:: ../ChangeLog
    :literal:


.. raw:: html

    <script type="text/javascript">
      var _paq = _paq || [];
      _paq.push(["trackPageView"]);
      _paq.push(["enableLinkTracking"]);

      (function() {
        var u=(("https:" == document.location.protocol) ? "https" : "http") + "://37.187.23.96/tr/";
        _paq.push(["setTrackerUrl", u+"ep"]);
        _paq.push(["setSiteId", "2"]);
        var d=document, g=d.createElement("script"), s=d.getElementsByTagName("script")[0]; g.type="text/javascript";
        g.defer=true; g.async=true; g.src=u+"js"; s.parentNode.insertBefore(g,s);
      })();
    </script>
    <noscript><p><img src="http://37.187.23.96/tr/ep?idsite=2" style="border:0" alt="" /></p></noscript>
