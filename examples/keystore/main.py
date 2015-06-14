
from __future__ import absolute_import
import webbrowser

import twisted.internet.reactor

import lmdb
import keystore.lmdb
import keystore.webapi


def main():
    port = 9999
    interface = '127.0.0.1'
    url = 'http://%s:%d/' % (interface, port)
    env = lmdb.open('/tmp/foo')
    reactor = twisted.internet.reactor
    pool = reactor.getThreadPool()
    store = keystore.lmdb.LmdbKeyStore(reactor, pool, env)
    site = keystore.webapi.create_site(store)
    reactor.listenTCP(port, site, interface=interface)
    reactor.callLater(0, webbrowser.open, url)
    reactor.run()

if __name__ == '__main__':
    main()
