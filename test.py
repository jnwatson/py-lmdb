
import lmdb

env = lmdb.Environment("x")
print 'here'
t = lmdb.Transaction(env)
db = t.db()
