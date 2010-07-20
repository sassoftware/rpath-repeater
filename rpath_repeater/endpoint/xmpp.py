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

from jabberlink.client import LinkClient
from jabberlink.cred import XmppClientCredentials
from jabberlink import logger
from jabberlink import message
from jabberlink.handlers.link import Message

from rpath_repeater.endpoint import interfaces

from twisted.application import service

from zope.interface import implements

NS = 'http://rpath.com/permanent/xmpp/repeater-1.0'

class RepeaterMessageHandler(message.MessageHandler):
    namespace = NS
    
    def __init__(self, repeater):
        self.repeater = repeater

    def onMessage(self, neighbor, msg):
        rows = msg.payload.split('\n')
        
        header = eval(rows[0])
        body = "".join(rows[1:])
        
        if self.repeater:
            response = self.repeater.dispatch(header['method'], header['url'], body, {})
            headers = {'status':response.status, 'headers':response.getheaders()}
            reply = "%s\n%s" % (headers, response.read())
        
            neighbor.send(message.Message(self.namespace, reply, in_reply_to=msg))
        else:
            raise Fault

class EndPointXMPPService(service.Service):
    implements(interfaces.IRepeaterPublishService)
    
    def __init__(self, cfg):
        self.cfg = cfg
       
        creds = XmppClientCredentials(cfg.credentialPath)
        
        if self.cfg.xmppUsername and self.cfg.xmppPassword:
            creds.set(self.cfg.xmppUsername, self.cfg.xmppDomain, self.cfg.xmppPassword)
        
        self.client = LinkClient(cfg.xmppDomain, creds)
        self.client.logTraffic = True
 
        self.addNeighbors(self.cfg.neighbors)
        
        self.client.startService()  

        self.sender = creds.get(cfg.xmppDomain)[0]
        self.recipient = self.cfg.neighbors[0]
        
    def addNeighbors(self, neighbors):
        if neighbors:
            for idx, neighbor in enumerate(neighbors):
                if self.cfg.repeaterHub and idx == 0:
                    self.client.connectNeighbor(neighbor)      
                else:
                    self.client.listenNeighbor(neighbor)
        
    def addMessageHandler(self, messageHandler):
        self.client.link.addMessageHandler(messageHandler)
             
    def sendMsg(self, request, method, recipient = None):
        request.content.seek(0, 0)
        msg = request.content.read()
        
        headers = {'url':request.uri, 'sender':self.sender,
                   'method':method.upper(), 'endpoint': request.getHost().host}
        
        msg = "%s\n%s" % (headers, msg)
        msg = message.Message(NS, msg)
        
        if not recipient:
            recipient = self.recipient
        
        d = self.client.link.sendWithDeferred(recipient, msg)
        
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
    