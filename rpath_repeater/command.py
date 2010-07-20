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

import os
import sys
import time
import StringIO

from conary.lib import command
from conary.lib import options

from rpath_repeater import logger
from rpath_repeater.endpoint import xmpp
from rpath_repeater.repeaters import httprepeater

from twisted.internet import reactor
#from twisted.application import internet, service
from twisted.web import resource, server

class repeaterCommand(command.AbstractCommand):
    def runCommand(self, *args, **kw):
        pass

class XMPPCommand(repeaterCommand):
    commands = ['XMPP']
    help = 'Use '
    requireConfig = True
    
    def createHttpRepeater(self, host):
        return httprepeater.HttpRepeater(host)
    
    def createMessageHandler(self, repeater):
        return xmpp.RepeaterMessageHandler(repeater)
    
    def addHttpListener(self, port, root):
        reactor.listenTCP(port, server.Site(resource.IResource(root)))
    
    def runCommand(self, cfg, argSet, args):
        self.cfg = cfg
        endpoint = xmpp.EndPointXMPPService(cfg)
        
        if cfg.httpPort:
            self.addHttpListener(cfg.httpPort, endpoint)
        
        if 'HTTP' in cfg.repeaterTypes:
            idx = cfg.repeaterTypes.index('HTTP')
            host = cfg.repeaterDestinations[idx]
            repeater = self.createHttpRepeater(host)
            endpoint.addMessageHandler(self.createMessageHandler(repeater))
            
        reactor.run()
        return 0