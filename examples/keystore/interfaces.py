
from __future__ import absolute_import
import zope.interface


class IKeyStoreSync(zope.interface.Interface):
    def get(key):
        """
        Fetch a key
        @return None or bytestring value.
        """

    def put(key, value):
        """
        Create or overwrite a key.
        @param key Bytestring key.
        @param value Bytestring alue.
        @return deferred producing None or error.
        """

    def delete(key):
        """
        Delete a key.
        @param key Bytestring key.
        @return deferred producing None or error.
        """

    def seek(key):
        """
        Seek to a key, or the next highest key.
        @param key Bytestring key.
        """

    def first():
        """
        Seek to the first key in the store.
        @return True if positioned on a key.
        """

    def last():
        """
        Seek to the last key in the store.
        @return True if positioned on a key.
        """

    def next():
        """
        Seek to the next key in the store.
        @return True if positioned on a key.
        """

    def prev():
        """
        Seek to the previous key in the store.
        @return True if positioned on a key.
        """


class IKeyStore(zope.interface.Interface):
    def get(key):
        """
        Fetch a key.
        @return deferred producing None or bytestring value.
        """

    def getKeys(key, count):
        """
        Fetch a list of keys.
        @param key
            Bytestring first key to return, or None for first/last key
            in space.
        @param count Number of keys including first key to return.
        """

    def getKeysReverse(key, count):
        """
        Fetch a list of keys, walking in reverse.
        @param key
            Bytestring first key to return, or None for first/last key
            in space.
        @param count Number of keys including first key to return.
        """

    def getItems(key, count):
        """
        Fetch a list of (key, value) tuples.
        @param key
            Bytestring first key to return, or None for first/last key
            in space.
        @param count Number of keys including first key to return.
        """

    def getItemsReverse(key, count):
        """
        Fetch a list of (key, value) tuples.
        @param key
            Bytestring first key to return, or None for first/last key
            in space.
        @param count Number of keys including first key to return.
        """

    def put(key):
        """
        Create or overwrite a key.

        @param key Bytestring key.
        @param value Bytestring alue.
        @return deferred producing None or error.
        """

    def delete(key):
        """
        Delete a key.
        @param key Bytestring key.
        @return deferred producing None or error.
        """

    def putGroup(func):
        """
        Execute a function in the context of synchronous (IKeyStoreSync)
        transaction, in a private thread.

        @param func Function accepting IKeyStoreSync parameter.
        @returns deferred producing None or error.
        """
