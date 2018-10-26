
import lmdb

# Open (and create if necessary) our database environment. Must specify
# max_dbs=... since we're opening subdbs.
env = lmdb.open('/tmp/address-book.lmdb', max_dbs=10)

# Now create subdbs for home and business addresses.
home_db = env.open_db(b'home')
business_db = env.open_db(b'business')


# Add some telephone numbers to each DB:
with env.begin(write=True) as txn:
    txn.put(b'mum', b'012345678', db=home_db)
    txn.put(b'dad', b'011232211', db=home_db)
    txn.put(b'dentist', b'044415121', db=home_db)
    txn.put(b'hospital', b'078126321', db=home_db)

    txn.put(b'vendor', b'0917465628', db=business_db)
    txn.put(b'customer', b'0553211232', db=business_db)
    txn.put(b'coworker', b'0147652935', db=business_db)
    txn.put(b'boss', b'0123151232', db=business_db)
    txn.put(b'manager', b'0644810485', db=business_db)


# Iterate each DB to show the keys are sorted:
with env.begin() as txn:
    for name, db in ('home', home_db), ('business', business_db):
        print('DB:', name)
        for key, value in txn.cursor(db=db):
            print('  ', key, value)
        print()


# Now let's update some phone numbers. We can specify the default subdb when
# starting the transaction, rather than pass it in every time:
with env.begin(write=True, db=home_db) as txn:
    print('Updating number for dentist')
    txn.put(b'dentist', b'099991231')

    print('Deleting number for hospital')
    txn.delete(b'hospital')
    print()

    print('Home DB is now:')
    for key, value in txn.cursor():
        print('  ', key, value)
    print()


# Now let's look up a number in the business DB
with env.begin(db=business_db) as txn:
    print('Boss telephone number:', txn.get(b'boss'))
    print()


# We got fired, time to delete all keys from the business DB.
with env.begin(write=True) as txn:
    print('Deleting all numbers from business DB:')
    txn.drop(business_db, delete=False)

    print('Adding number for recruiter to business DB')
    txn.put(b'recruiter', b'04123125324', db=business_db)

    print('Business DB is now:')
    for key, value in txn.cursor(db=business_db):
        print('  ', key, value)
    print()
