
from __future__ import absolute_import
import pkg_resources
import keystore.web
import twisted.web.server


def read_resource(path):
    return pkg_resources.resource_string('keystore', path)


class KeyResource(keystore.web.DeferrableResource):
    isLeaf = True

    def __init__(self, store):
        keystore.web.DeferrableResource.__init__(self)
        self.store = store

    def _get_done(self, value):
        if value is None:
            return keystore.web.Response(status=404)
        else:
            return keystore.web.Response(body=value)

    def render_GET(self, request):
        d = self.store.get(request.path)
        d.addCallback(self._get_done)
        return d

    def render_PUT(self, request):
        value = request.content.read()
        d = self.store.put(request.path, value)
        d.addCallback(lambda _: keystore.web.Response(status=202))
        return d

    def render_DELETE(self, request):
        d = self.store.delete(request.path)
        d.addCallback(lambda _: keystore.web.Response(status=204))
        return d


class NamespaceResource(keystore.web.DeferrableResource):
    isLeaf = False

    def __init__(self, store):
        keystore.web.DeferrableResource.__init__(self)
        self.store = store

    def getChild(self, path, request):
        return KeyResource(self.store)


class StaticResource(twisted.web.resource.Resource):
    def __init__(self, pkg_path):
        twisted.web.resource.Resource.__init__(self)
        self.data = read_resource(pkg_path)

    def render(self, request):
        return self.data


def create_site(store):
    root = twisted.web.resource.Resource()
    root.putChild('', StaticResource('static/index.html'))
    root.putChild('db', NamespaceResource(store))
    return twisted.web.server.Site(root)
