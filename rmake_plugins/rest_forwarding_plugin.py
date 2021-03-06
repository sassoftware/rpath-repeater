#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import base64
import logging

from conary.lib import cfg as cny_cfg
from conary.lib import cfgtypes
from conary.lib.http.request import URL

from rmake3.lib import chutney
from rmake3.lib import logger
from rmake3.lib.jabberlink import message
from rmake3.lib.jabberlink.handlers import link

from rmake3.core import types
from rmake3.core import plug_dispatcher

from rmake3.worker import plug_worker

from twisted.web import client
from twisted.web import server
from twisted.web import resource
from twisted.internet import ssl
from twisted.internet import reactor

NS = 'http://rpath.com/permanent/xmpp/repeater-1.0'
log = logging.getLogger(__name__)


class RestForwardingConfig(cny_cfg.ConfigFile):
    # Launcher
    key                     = (cfgtypes.CfgString, None)
    cert                    = (cfgtypes.CfgString, None)
    httpPort                = (cfgtypes.CfgInt, None)
    httpsPort               = (cfgtypes.CfgInt, None)

    # Dispatcher
    repeaterTarget          = (cfgtypes.CfgString, None)


class RestForwardingPlugin(plug_dispatcher.DispatcherPlugin,
    plug_worker.LauncherPlugin):

    def launcher_post_setup(self, launcher):
        """ The Sputnik end of the rMake topology """
        endpoint = EndPoint(launcher.bus)

        cfg = self.populateConfigFromOptions(RestForwardingConfig())
        if cfg.httpPort:
            reactor.listenTCP(cfg.httpPort,
                    server.Site(resource.IResource(endpoint)))
        if cfg.httpsPort:
            reactor.listenSSL(cfg.httpsPort,
                    server.Site(resource.IResource(endpoint)),
                    ssl.DefaultOpenSSLContextFactory(cfg.key, cfg.cert))

    def dispatcher_post_setup(self, dispatcher):
        """ The rBuilder end of the rMake topology """

        cfg = self.populateConfigFromOptions(RestForwardingConfig())
        if cfg.repeaterTarget:
            dispatcher.bus.link.addMessageHandler(
                    RepeaterMessageHandler(cfg.repeaterTarget,
                        dispatcher.workers))


class RepeaterMessageHandler(message.MessageHandler):
    namespace = NS
    XHeader = 'X-rPath-Management-Zone'
    XRepeaterHeader = 'X-rPath-Repeater'

    def __init__(self, host, workers):
        self.targetUrl = URL(host)
        self.workers = workers

    def getManagementZone(self, neighbor):
        jid = link.toJID(neighbor.jid.full())
        worker = self.workers.get(jid)
        if worker is None:
            return None
        zones = self.getWorkerZones(worker)
        if not zones:
            return None
        # We only care about one zone for now
        return base64.b64encode(zones[0])

    @classmethod
    def getWorkerZones(cls, worker):
        # XXX it would be nice if this was a property of the worker
        zoneNames = [ x.zoneName for x in worker.caps
            if isinstance(x, types.ZoneCapability) ]
        return zoneNames

    def onMessage(self, neighbor, msg):
        reqDict = chutney.loads(msg.payload)
        method = reqDict['method']
        url = reqDict['url']
        body = reqDict['body']
        rawHeaders = reqDict['headers']
        headers = client.Headers(rawHeaders)
        headers.removeHeader(self.XHeader)
        managementZone = self.getManagementZone(neighbor)
        if managementZone is not None:
            headers.addRawHeader(self.XHeader, managementZone)
        # This header flags a request as _not_ being originated from localhost
        # Some of the management interfaces require localhost access, but
        # everything forwarded through the repeater looks like it's
        # originating from localhost, unless this header is present
        headers.addRawHeader(self.XRepeaterHeader, 'remote')
        # XXX this is where multi-valued headers go down the drain
        headers = dict((k.lower(), v[-1])
            for (k, v) in headers.getAllRawHeaders())

        fact = HTTPClientFactory(url, method=method.upper(), postdata=body,
            headers=headers)
        @fact.deferred.addCallback
        def processResult(args):
            (status, statusMessage, headers, body) = args
            reply = dict(
                status = int(status),
                message = statusMessage,
                headers = headers,
                body = body,
            )
            neighbor.send(message.Message(self.namespace, chutney.dumps(reply),
                                           in_reply_to=msg))
            return args

        @fact.deferred.addErrback
        def processError(error):
            logger.logFailure(error, "Error in proxied REST request:")
            reply = dict(
                    status=500,
                    message='Internal Server Error',
                    headers={},
                    body='',
                    )
            neighbor.send(message.Message(self.namespace, chutney.dumps(reply),
                                           in_reply_to=msg))

        host, port = self.targetUrl.hostport
        if self.targetUrl.scheme == 'https':
            reactor.connectSSL(str(host), port, fact)
        else:
            reactor.connectTCP(str(host), port, fact)


class EndPoint(resource.Resource):
    isLeaf=True

    def __init__(self, bus):
        self.bus = bus

    def addMessageHandler(self, messageHandler):
        self.bus.addHandler(messageHandler)

    def render_GET(self, request):
        self.sendMsg(request, 'GET')
        return server.NOT_DONE_YET

    def render_POST(self, request):
        self.sendMsg(request, 'POST')
        return server.NOT_DONE_YET

    def render_PUT(self, request):
        self.sendMsg(request, 'PUT')
        return server.NOT_DONE_YET

    def render_DELETE(self, request):
        self.sendMsg(request, 'DELETE')
        return server.NOT_DONE_YET

    def getChild(self, path, request):
        return self

    def sendMsg(self, request, method):
        request.content.seek(0, 0)
        request.requestHeaders.setRawHeaders('x-forwarded-for',
                [request.getClientIP()])
        request.requestHeaders.setRawHeaders('x-forwarded-proto',
                ['https' if request.isSecure() else 'http'])
        body = request.content.read()

        content = {
            'url': request.uri,
            'method': method.upper(),
            'body': body,
            'headers': dict(request.requestHeaders.getAllRawHeaders()),
        }

        content = chutney.dumps(content)
        msg = message.Message(NS, content)

        d = self.bus.link.sendWithDeferred(self.bus.targetJID, msg)

        @d.addCallback
        def on_reply(replies):
            for reply in replies:
                dict = chutney.loads(reply.payload)
                request.setResponseCode(dict['status'])
                for key, values in dict.get('headers', {}).items():
                    if key.lower() in ('connection', 'transfer-encoding'):
                        continue
                    request.responseHeaders.setRawHeaders(key, values)

                responseBody = dict['body']
                if responseBody:
                    request.write(responseBody)

                if not request._disconnected:
                    request.finish()

        return d


class HTTPPageGetter(client.HTTPPageGetter):

    def handleStatusDefault(self):
        # Pass through all HTTP responses regardless of status
        pass


class HTTPClientFactory(client.HTTPClientFactory):
    protocol = HTTPPageGetter

    def __init__(self, url, *args, **kwargs):
        client.HTTPClientFactory.__init__(self, url, *args, **kwargs)
        self.status = None
        self.deferred.addCallback(
            lambda data: (self.status, self.message, self.response_headers,
                data))
