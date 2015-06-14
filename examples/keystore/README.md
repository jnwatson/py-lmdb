
## Ultra minimal async IO example

This requires a recent version of Twisted to run. From the parent directory,
execute it as:

    python -m keystore.main

It wraps LMDB in a very basic REST API, demonstrating how to handle database IO
using a thread pool without blocking the main thread's select loop.

The internals are structured relatively sanely, but the `keystore.lmdb`
implementation itself is fairly horrid: a single GET will always cause a large
amount of machinery to run. However, given a large and slow enough (non RAM,
spinning rust) DB, this example will successfully keep the main loop responsive
at all times even though multiple LMDB `mdb_get()` invocations are running
concurrently.

Not shown here, but may be added sometime later to demonstrate the techniques:

* Combining multiple reads into a single transaction and context switch.
* Combining writes into a single write transaction and context switch.


## Calling LMDB synchronously

This example never calls LMDB directly from the main loop, even though in some
restricted circumstances that may be completely safe. Such a situation might
look like:

* The database is guaranteed to always be in RAM.
* Database writes are never contended.
* Disk IO is very fast, or `sync=False` is used.

In almost every case, it is likely better to design an application that handles
the possibility that calls into LMDB will trigger slow IO, if not now then at
some point in 10 years when all the original developers have left your project.
