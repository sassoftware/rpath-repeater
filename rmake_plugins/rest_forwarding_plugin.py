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

import httplib
import logging

from jabberlink import message

from rmake.core import plug_dispatcher
from rmake.worker import plug_worker

from twisted.internet import defer, reactor
from twisted.web import resource, server

NS = 'http://rpath.com/permanent/xmpp/repeater-1.0'
logger = logging.getLogger(__name__)

class RestForwardingPlugin(plug_dispatcher.DispatcherPlugin, plug_worker.LauncherPlugin):

    def launcher_post_setup(self, launcher):
        """ The Sputnik end of the rMake topology """
        endpoint = EndPoint(launcher.bus)
        
        # get configuration options
        if self.__class__.__name__ in launcher.cfg.pluginOption:
            options = launcher.cfg.pluginOption[self.__class__.__name__]
            for option in options:
                key, value = option.split()
                
                if key == 'httpPort':
                    reactor.listenTCP(int(value), server.Site(resource.IResource(endpoint)))
    
    def dispatcher_post_setup(self, dispatcher):
        """ The rBuilder end of the rMake topology """
        
        # get configuration options
        if self.__class__.__name__ in dispatcher.cfg.pluginOption:
            options = dispatcher.cfg.pluginOption[self.__class__.__name__]
            for option in options:
                key, value = option.split()
                
                if key == 'repeaterTarget':
                    repeater = HttpRepeater(value)
                    dispatcher.bus.link.addMessageHandler(RepeaterMessageHandler(repeater))
    

class RepeaterMessageHandler(message.MessageHandler):
    namespace = NS
    
    def __init__(self, repeater):
        self.repeater = repeater

    def onMessage(self, neighbor, msg):
        
        rows = msg.payload.split('\n')

        header = eval(rows[0])
        body = "".join(rows[1:])
        
        if self.repeater:
            response = self.repeater.dispatch(header['method'],
                         header['url'], body, 
                         {'X-rpathManagementNetworkNode': neighbor.jid})
            headers = {'status':response.status, 'headers':response.getheaders()}
            reply = "%s\n%s" % (headers, response.read())
        
            neighbor.send(message.Message(self.namespace, reply, in_reply_to=msg))
        else:
            raise

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
        print request
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
        msg = request.content.read()
        
        headers = {'url':request.uri,
                   'method':method.upper()}
        
        msg = "%s\n%s" % (headers, msg)
        msg = message.Message(NS, msg)
                
        d = self.bus.link.sendWithDeferred(self.bus.targetJID, msg)
        
        @d.addCallback
        def on_reply(replies):
            for reply in replies:
                rows = reply.payload.split('\n')     
                header = eval(rows[0])
                body = "".join(rows[1:])

                request.setResponseCode(header['status'])
                if body:
                    request.write(body)
                request.finish()
        
        return d
    
class HttpRepeater(object):
    
    def __init__(self, host):
        self.host = host
        
   
    def dispatch(self, method, url, msg, headers):
        d = defer.Deferred()
        
        self.method = method.upper()
        self.url = url
        self.msg = msg
        self.headers = headers
        self.response = None
        
        @d.addCallback
        def connect(result):
            
            self.conn = httplib.HTTPConnection(self.host)
            
        @d.addCallback
        def send(result):
                   
            if self.conn:
                self.conn.request(self.method, self.url, self.msg, self.headers)

                self.response = self.conn.getresponse()
                
                self.conn.close()
 
        @d.addErrback
        def errorHandler(failure):
            print failure
            logger.logFailure(failure)
        
        d.callback(self)
        
        return self.response              
