
from __future__ import absolute_import
import twisted.python.failure
import twisted.python.log
import twisted.web.http
import twisted.web.resource
import twisted.web.server


class Response(object):
    def __init__(self, body=None, headers=None, status=None):
        self.body = body
        self.headers = headers or {}
        self.status = status


class DeferrableResourceRender(object):
    def __init__(self, resource_, request):
        self.resource = resource_
        self.request = request
        self.closed = False

    def _notify_finish(self, resp):
        self.closed = resp

    def _on_render_error(self, failure):
        twisted.python.log.err(failure)
        if not self.closed:
            self.request.setResponseCode(twisted.web.http.INTERNAL_SERVER_ERROR)
            self.request.finish()

    def _on_render_done(self, resp):
        if not isinstance(resp, Response):
            exc = TypeError("render() didn't return Response, got %r" % (resp,))
            self._on_render_error(twisted.python.failure.Failure(exc))
            return

        if resp.body is not None and type(resp.body) is not bytes:
            exc = TypeError("render() returned %r, not bytes" %
                            (type(resp.body),))
            self._on_render_error(twisted.python.failure.Failure(exc))
            return

        if self.closed:
            return

        if resp.status is not None:
            self.request.setResponseCode(resp.status)
        for key, value in resp.headers.iteritems():
            self.request.setHeader(key, value)
        if resp.body is not None:
            self.request.setHeader('Content-Length', len(resp.body))
            self.request.write(resp.body)
        self.request.finish()

    def start_render(self):
        method = self.request.method
        handler = getattr(self.resource, 'render_' + method, None)
        if handler is None:
            self._on_render_done(Response(status=405))
            return

        self.request.notifyFinish().addBoth(self._notify_finish)
        d = twisted.internet.defer.maybeDeferred(handler, self.request)
        d.addCallbacks(self._on_render_done, self._on_render_error)
        return twisted.web.server.NOT_DONE_YET


class DeferrableResource(twisted.web.resource.Resource):
    def render(self, request):
        render = DeferrableResourceRender(self, request)
        return render.start_render()
