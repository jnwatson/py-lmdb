
lmdb
====

.. currentmodule:: lmdb

.. toctree::
    :hidden:
    :maxdepth: 2

This is a universal Python binding for the `LMDB 'Lightning' Database
<http://symas.com/mdb/>`_. Two variants are provided and automatically selected
during install: a `CFFI <https://cffi.readthedocs.io/en/release-0.5/>`_ variant
that supports `PyPy <http://www.pypy.org/>`_ and all versions of CPython >=2.7,
and a C extension that supports CPython >=2.7 and >=3.4. Both variants
provide the same interface.

LMDB is a tiny database with some excellent properties:

* Ordered map interface (keys are always lexicographically sorted).
* Reader/writer transactions: readers don't block writers, writers don't block
  readers. Each environment supports one concurrent write transaction.
* Read transactions are extremely cheap.
* Environments may be opened by multiple processes on the same host, making it
  ideal for working around Python's `GIL
  <http://wiki.python.org/moin/GlobalInterpreterLock>`_.
* Multiple named databases may be created with transactions covering all
  named databases.
* Memory mapped, allowing for zero copy lookup and iteration. This is
  optionally exposed to Python using the :py:func:`buffer` interface.
* Maintenance requires no external process or background threads.
* No application-level caching is required: LMDB fully exploits the operating
  system's buffer cache.


Installation: Windows
+++++++++++++++++++++

Binary eggs and wheels are published via PyPI for Windows, allowing the binding
to be installed via pip and easy_install without the need for a compiler to be
present. The binary releases statically link against the bundled version of
LMDB.

Initially 32-bit and 64-bit binaries are provided for Python 2.7; in future
binaries will be published for all supported versions of Python.

To install, use a command like:

    ::

        C:\Python27\python -mpip install lmdb

Or:

    ::

        C:\Python27\python -measy_install lmdb


Installation: UNIX
++++++++++++++++++

For convenience, a supported version of LMDB is bundled with the binding and
built statically by default. If your system distribution includes LMDB, set the
``LMDB_FORCE_SYSTEM`` environment variable, and optionally ``LMDB_INCLUDEDIR``
and ``LMDB_LIBDIR`` prior to invoking ``setup.py``.

The CFFI variant depends on CFFI, which in turn depends on ``libffi``, which
may need to be installed from a package. On CPython, both variants additionally
depend on the CPython development headers. On Debian/Ubuntu:

    ::

        apt-get install libffi-dev python-dev build-essential

To install the C extension, ensure a C compiler and `pip` or `easy_install` are
available and type:

    ::

        pip install lmdb
        # or
        easy_install lmdb

The CFFI variant may be used on CPython by setting the ``LMDB_FORCE_CFFI``
environment variable before installation, or before module import with an
existing installation:

    ::

        >>> import os
        >>> os.environ['LMDB_FORCE_CFFI'] = '1'

        >>> # CFFI variant is loaded.
        >>> import lmdb


Getting Help
++++++++++++

Before getting in contact, please ensure you have thoroughly reviewed this
documentation, and if applicable, the associated
`official Doxygen documentation <http://symas.com/mdb/doc/>`_.

If you have found a bug, please report it on the `GitHub issue tracker
<https://github.com/dw/py-lmdb/issues>`_, or mail it to the list below if
you're allergic to GitHub.

For all other problems and related discussion, please direct it to
`the py-lmdb@freelists.org mailing list <http://www.freelists.org/list/py-lmdb>`_.
You must be subscribed to post. The `list archives
<http://www.freelists.org/archive/py-lmdb/>`_ are also available.


Named Databases
+++++++++++++++

Named databases require the `max_dbs=` parameter to be provided when calling
:py:func:`lmdb.open` or :py:class:`lmdb.Environment`. This must be done by the
first process or thread opening the environment.

Once a correctly configured :py:class:`Environment` is created, new named
databases may be created via :py:meth:`Environment.open_db`.


Storage efficiency & limits
+++++++++++++++++++++++++++

Records are grouped into pages matching the operating system's VM page size,
which is usually 4096 bytes. Each page must contain at least 2 records, in
addition to 8 bytes per record and a 16 byte header. Due to this the engine is
most space-efficient when the combined size of any (8+key+value) combination
does not exceed 2040 bytes.

When an attempt to store a record would exceed the maximum size, its value part
is written separately to one or more dedicated pages. Since the trailer of the
last page containing the record value cannot be shared with other records, it
is more efficient when large values are an approximate multiple of 4096 bytes,
minus 16 bytes for an initial header.

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
adjusted by rebuilding the library. The compile-time key length can be queried
via :py:meth:`Environment.max_key_size()`.


Memory usage
++++++++++++

Diagnostic tools often overreport the memory usage of LMDB databases, since the
tools poorly classify that memory. The Linux ``ps`` command ``RSS`` measurement
may report a process as having an entire database resident, causing user alarm.
While the entire database may really be resident, it is half the story.

Unlike heap memory, pages in file-backed memory maps, such as those used by
LMDB, may be efficiently reclaimed by the OS at any moment so long as the pages
in the map are `clean`. `Clean` simply means that the resident pages' contents
match the associated pages that live in the disk file that backs the mapping. A
clean mapping works exactly like a cache, and in fact it is a cache: the `OS
page cache <http://en.wikipedia.org/wiki/Page_cache>`_.

On Linux, the ``/proc/<pid>/smaps`` file contains one section for each memory
mapping in a process. To inspect the actual memory usage of an LMDB database,
look for a ``data.mdb`` entry, then observe its `Dirty` and `Clean` values.

When no write transaction is active, all pages in an LMDB database should be
marked `clean`, unless the Environment was opened with `sync=False`, and no
explicit :py:meth:`Environment.sync` has been called since the last write
transaction, and the OS writeback mechanism has not yet opportunistically
written the dirty pages to disk.


Bytestrings
+++++++++++

This documentation uses `bytestring` to mean either the Python<=2.7
:py:func:`str` type, or the Python>=3.0 :py:func:`bytes` type, depending on the
Python version in use.

Due to the design of Python 2.x, LMDB will happily accept Unicode instances
where :py:func:`str` instances are expected, so long as they contain only ASCII
characters, in which case they are implicitly encoded to ASCII. You should not
rely on this behaviour! It results in brittle programs that often break the
moment they are deployed in production. Always explicitly encode and decode any
Unicode values before passing them to LMDB.

This documentation uses :py:func:`bytes` in examples. In Python 3.x this is a
distinct type, whereas in Python 2.7 it is simply an alias for
:py:func:`str`.


Buffers
+++++++

Since LMDB is memory mapped it is possible to access record data without keys
or values ever being copied by the kernel, database library, or application. To
exploit this the library can be instructed to return :py:func:`buffer` objects
instead of bytestrings by passing `buffers=True` to
:py:meth:`Environment.begin` or :py:class:`Transaction`.

In Python :py:func:`buffer` objects can be used in many places where
bytestrings are expected. In every way they act like a regular sequence: they
support slicing, indexing, iteration, and taking their length. Many Python APIs
will automatically convert them to bytestrings as necessary:

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
        >>> value = bytes(buf)
        >>> len(value)
        4096
        >>> type(value)
        <type 'bytes'>

It is also possible to pass buffers directly to many native APIs, for example
:py:meth:`file.write`, :py:meth:`socket.send`, :py:meth:`zlib.decompress` and
so on. A buffer may be sliced without copying by passing it to
:py:func:`buffer`:

    ::

        >>> # Extract bytes 10 through 210:
        >>> sub_buf = buffer(buf, 10, 200)
        >>> len(sub_buf)
        200

In both PyPy and CPython, returned buffers *must be discarded* after their
producing transaction has completed or been modified in any way. To preserve
buffer's contents, copy it using :py:func:`bytes`:

    .. code-block:: python

        with env.begin(write=True, buffers=True) as txn:
            buf = txn.get('foo')           # only valid until the next write.
            buf_copy = bytes(buf)          # valid forever
            txn.delete('foo')              # this is a write!
            txn.put('foo2', 'bar2')        # this is also a write!

            print('foo: %r' % (buf,))      # ERROR! invalidated by write
            print('foo: %r' % (buf_copy,)) # OK

        print('foo: %r' % (buf,))          # ERROR! also invalidated by txn end
        print('foo: %r' % (buf_copy,))     # still OK


``writemap`` mode
+++++++++++++++++

When :py:class:`Environment` or :py:func:`open` is invoked with
``writemap=True``, the library will use a writeable memory mapping to directly
update storage. This improves performance at a cost to safety: it is possible
(though fairly unlikely) for buggy C code in the Python process to accidentally
overwrite the map, resulting in database corruption.

.. caution::

    This option may cause filesystems that don't support sparse files, such as
    OSX, to immediately preallocate `map_size=` bytes of underlying storage
    when the environment is opened or closed for the first time.

.. caution::

    This option may cause filesystems that don't support sparse files, such as
    OSX, to immediately preallocate `map_size=` bytes of underlying storage
    when the environment is opened or closed for the first time.


Resource Management
+++++++++++++++++++

:py:class:`Environment`, :py:class:`Transaction`, and :py:class:`Cursor`
support the context manager protocol, allowing for robust resource cleanup in
the case of exceptions.

.. code-block:: python

    with env.begin() as txn:
        with txn.cursor() as curs:
            # do stuff
            print 'key is:', curs.get('key')

On CFFI it is important to use the :py:class:`Cursor` context manager, or
explicitly call :py:meth:`Cursor.close` if many cursors are created within a
single transaction. Failure to close a cursor on CFFI may cause many dead
objects to accumulate until the parent transaction is aborted or committed.


Transaction management
++++++++++++++++++++++

While any reader exists, writers cannot reuse space in the database file that
has become unused in later versions. Due to this, continual use of long-lived
read transactions may cause the database to grow without bound. A lost
reference to a read transaction will simply be aborted (and its reader slot
freed) when the :py:class:`Transaction` is eventually garbage collected. This
should occur immediately on CPython, but may be deferred indefinitely on PyPy.

However the same is *not* true for write transactions: losing a reference to a
write transaction can lead to deadlock, particularly on PyPy, since if the same
process that lost the :py:class:`Transaction` reference immediately starts
another write transaction, it will deadlock on its own lock. Subsequently the
lost transaction may never be garbage collected (since the process is now
blocked on itself) and the database will become unusable.

These problems are easily avoided by always wrapping :py:class:`Transaction` in
a ``with`` statement somewhere on the stack:

.. code-block:: python

    # Even if this crashes, txn will be correctly finalized.
    with env.begin() as txn:
        if txn.get('foo'):
            function_that_stashes_away_txn_ref(txn)
            function_that_leaks_txn_refs(txn)
            crash()


Threads
+++++++

``MDB_NOTLS`` mode is used exclusively, which allows read transactions to
freely migrate across threads and for a single thread to maintain multiple read
transactions. This enables mostly care-free use of read transactions, for
example when using `gevent <http://www.gevent.org/>`_.

Most objects can be safely called by a single caller from a single thread, and
usually it only makes sense to to have a single caller, except in the case of
:py:class:`Environment`.

Most :py:class:`Environment` methods are thread-safe, and may be called
concurrently, except for :py:meth:`Environment.close`.  Running `close` at the
same time as other database operations may crash the interpreter.

A write :py:class:`Transaction` may only be used from the thread it was created
on.

A read-only :py:class:`Transaction` can move across threads, but it cannot be
used concurrently from multiple threads.

:py:class:`Cursor` is not thread-safe, but it does not make sense to use it on
any thread except the thread that currently owns its associated
:py:class:`Transaction`.

Limitations running on 32-bit Processes
+++++++++++++++++++++++++++++++++++++++
32-bit processes (for example 32-bit builds of Python on Windows) are severely
limited in the amount of virtual memory that can be mapped in.  This is
particularly true for any 32-bit process but is particularly true for
Python running on Windows and long running processes.

Virtual address space fragmentation is a significant issue for mapping files
into memory, a requirement for lmdb, as lmdb requires a contiguous range of
virtual addresses. See
https://web.archive.org/web/20170701204304/http://forthescience.org/blog/2014/08/16/python-and-memory-fragmentation
for more information and a solution that potentially gives another 50% of
virtual address space on Windows.

Importantly, using a 32-bit instance of Python (even with the OS being 64-bits)
means that the maximum size file that can be ever be mapped into memory is
around 1.1 GiB, and that number decreases as the python process lives and
allocates/deallocates memory.  That means the DB file you can open now might not
be the DB file you can open in a hour, given the same process.

On Windows, You can see the see the precise maximum mapping size by using the
SysInternals tool VMMap, then selecting your Python process, then selecting the
"free" row, then sorting by size.

This is not a problem at all for 64-bit processes.

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
.. autoclass:: lmdb.BadDbiError ()
.. autoclass:: lmdb.BadRslotError ()
.. autoclass:: lmdb.BadTxnError ()
.. autoclass:: lmdb.BadValsizeError ()
.. autoclass:: lmdb.ReadonlyError ()
.. autoclass:: lmdb.InvalidParameterError ()
.. autoclass:: lmdb.LockError ()
.. autoclass:: lmdb.MemoryError ()
.. autoclass:: lmdb.DiskError ()


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

        drop: Delete one or more sub-databases.
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
      -E TARGET_ENV, --target_env=TARGET_ENV
                            Target environment file for "dumpfd"
      -x, --xxd             Print values in xxd format
      -M MAX_DBS, --max-dbs=MAX_DBS
                            Maximum open DBs (default: 128)
      --out-fd=OUT_FD       "copyfd" command target fd

      Options for "copy" command:
        --compact           Perform compaction while copying.

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
        --interval=INTERVAL
                            Interval size (default: 1sec)
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
    all activity on the DBI has ceased in all threads before closing the
    handle. Failure to do this could result in "impossible" errors, or the DBI
    slot becoming reused, resulting in operations being serviced by the wrong
    named database. Leaving handles open wastes a tiny amount of memory, which
    seems a good price to avoid subtle data corruption.

:py:meth:`Cursor.replace`, :py:meth:`Cursor.pop`:
    There are no native equivalents to these calls, they just implement common
    operations in C to avoid a chunk of error prone, boilerplate Python from
    having to do the same.

`mdb_set_compare()`, `mdb_set_dupsort()`:
    Neither function is exposed for a variety of reasons. In particular,
    neither can be supported safely, since exceptions cannot be propagated
    through LMDB callbacks, and can lead to database corruption if used
    incorrectly. Secondarily, since both functions are repeatedly invoked for
    every single lookup in the LMDB read path, most of the performance benefit
    of LMDB is lost by introducing Python interpreter callbacks to its hot path.

    There are a variety of workarounds that could make both functions useful,
    but not without either punishing binding users who do not require these
    features (especially on CFFI), or needlessly complicating the binding for
    what is essentially an edge case.

    In all cases where `mdb_set_compare()` might be useful, use of a special
    key encoding that encodes your custom order is usually desirable. See
    `issue #79 <https://github.com/dw/py-lmdb/issues/79>`_ for example
    approaches.

    The answer is not so clear for `mdb_set_dupsort()`, since a custom encoding
    there may necessitate wasted storage space, or complicating record decoding
    in an application's hot path. Please file a ticket if you think you have a
    use for `mdb_set_dupsort()`.


Technology
##########

The binding is implemented twice: once using CFFI, and once as native C
extension. This is since a CFFI binding is necessary for PyPy, but its
performance on CPython is very poor. For good performance on CPython, only
Cython and a native extension are viable options. Initially Cython was used,
however this was abandoned due to the effort and relative mismatch involved
compared to writing a native extension.


Invalidation lists
##################

Much effort has gone into avoiding crashes: when some object is invalidated
(e.g. due to :py:meth:`Transaction.abort`), child objects are updated to ensure
they don't access memory of the no-longer-existent resource, and that they
correspondingly free their own resources. On CPython this is accomplished by
weaving a linked list into all ``PyObject`` structures. This avoids the need to
maintain a separate heap-allocated structure, or produce excess ``weakref``
objects (which internally simply manage their own lists).

With CFFI this isn't possible. Instead each object has a ``_deps`` dict that
maps dependent object IDs to the corresponding objects. Weakrefs are avoided
since they are very inefficient on PyPy. Prior to invalidation ``_deps`` is
walked to notify each dependent that the resource is about to disappear.

Finally, each object may either store an explicit ``_invalid`` attribute and
check it prior to every operation, or rely on another mechanism to avoid the
crash resulting from using an invalidated resource. Instead of performing these
explicit tests continuously, on CFFI a magic
``Some_LMDB_Resource_That_Was_Deleted_Or_Closed`` object is used. During
invalidation, all native handles are replaced with an instance of this object.
Since CFFI cannot convert the magical object to a C type, any attempt to make a
native call will raise ``TypeError`` with a nice descriptive type name
indicating the problem. Hacky but efficient, and mission accomplished.


Argument parsing
################

The CPython module `parse_args()` may look "special", at best. The alternative
`PyArg_ParseTupleAndKeywords` performs continuous heap allocations and string
copies, resulting in a difference of 10,000 lookups/sec slowdown in a
particular microbenchmark. The 10k/sec slowdown could potentially disappear
given a sufficiently large application, so this decision needs revisited at
some stage.


ChangeLog
+++++++++

.. include:: ../ChangeLog
    :literal:


License
+++++++

.. include:: ../LICENSE
    :literal:
