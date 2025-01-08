import lmdb
import os
import time

print("      python: Opening environment")
env = lmdb.Environment('db.lmdb')
print("      python: Opened environment %r" % env)
print()

print("      python: Starting transaction")
txn = env.begin(write=False)
print("      python: Started transaction %r" % txn)
print()

print("      python: Deleting transaction")
del txn
print("      python: Deleted transaction")
print()

if (os.fork() != 0):
    os.wait()
    print("      python: Exited forked process")
    print()
    print("      python: Starting transaction (no. 2)")
    txn = env.begin(write=False)
    print("      python: Started transaction (no. 2) %r" % txn)
else:
    print("      python: Inside forked process")
