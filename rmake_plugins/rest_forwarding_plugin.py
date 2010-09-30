#
# Copyright (c) 2010 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import base64
import httplib
import logging

from rmake3.lib import chutney
from rmake3.lib.jabberlink import message
from rmake3.lib.jabberlink.handlers import link

from rmake3.core import plug_dispatcher
from rmake3.core import types
from rmake3.lib import logger
from rmake3.worker import plug_worker

from twisted.internet import reactor, ssl
from twisted.web import resource, server, client

NS = 'http://rpath.com/permanent/xmpp/repeater-1.0'
log = logging.getLogger(__name__)

class RestForwardingPlugin(plug_dispatcher.DispatcherPlugin, plug_worker.LauncherPlugin):

    def launcher_post_setup(self, launcher):
        """ The Sputnik end of the rMake topology """
        endpoint = EndPoint(launcher.bus)
        
        # get configuration options
        if self.__class__.__name__ in launcher.cfg.pluginOption:
            options = launcher.cfg.pluginOption[self.__class__.__name__]
            for option in options:
                key, value = option.split()
                if key == 'key':
                    self.sslkey = value
                    
                elif key == 'cert':
                    self.sslcert = value
                
                elif key == 'httpPort':
                    reactor.listenTCP(int(value), server.Site(resource.IResource(endpoint)))
                    
                elif key == 'httpsPort':
                    reactor.listenSSL(int(value), server.Site(resource.IResource(endpoint)),
                                      ssl.DefaultOpenSSLContextFactory(self.sslkey, self.sslcert))
    
    def dispatcher_post_setup(self, dispatcher):
        """ The rBuilder end of the rMake topology """
        
        # get configuration options
        if self.__class__.__name__ in dispatcher.cfg.pluginOption:
            options = dispatcher.cfg.pluginOption[self.__class__.__name__]
            for option in options:
                key, value = option.split()
                
                if key == 'repeaterTarget':
                    dispatcher.bus.link.addMessageHandler(
                        RepeaterMessageHandler(value, dispatcher.workers))


class RepeaterMessageHandler(message.MessageHandler):
    namespace = NS
    XHeader = 'X-rPath-Management-Zone'

    def __init__(self, host, workers):
        self.host = host
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
            # There was an error talking upstream. Return a 502 Bad Gateway
            # XXX Ideally we want to explain what the original error was
            reply = { 'status' : 502, 'body' : '' }
            neighbor.send(message.Message(self.namespace, chutney.dumps(reply),
                                           in_reply_to=msg))

        reactor.connectTCP(self.host, 80, fact)

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
        body = request.content.read()


        content = {'url':request.uri,
                   'method':method.upper(),
                   'body': body,
                   'headers' : dict(request.requestHeaders.getAllRawHeaders()),
                   }

        content = chutney.dumps(content)
        msg = message.Message(NS, content)

        d = self.bus.link.sendWithDeferred(self.bus.targetJID, msg)

        @d.addCallback
        def on_reply(replies):
            for reply in replies:
                dict = chutney.loads(reply.payload)

                request.setResponseCode(dict['status'])

                responseBody = dict['body']
                if responseBody:
                    request.write(responseBody)

                if not request._disconnected:
                    request.finish()

        return d

class HTTPClientFactory(client.HTTPClientFactory):
    def __init__(self, url, *args, **kwargs):
        client.HTTPClientFactory.__init__(self, url, *args, **kwargs)
        self.status = None
        self.deferred.addCallback(
            lambda data: (self.status, self.message, self.response_headers, data))
