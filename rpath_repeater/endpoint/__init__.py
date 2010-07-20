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

from rpath_repeater.endpoint import interfaces

from twisted.python import components
from twisted.web import resource, server

class EndPoint(resource.Resource):
    """ """
    
    isLeaf=True
    
    def __init__(self, service):

        resource.Resource.__init__(self)
        self.service = service
        
    def render_GET(self, request):
        
        self.service.sendMsg(request, 'GET')        
        return server.NOT_DONE_YET
        
    def render_POST(self, request):
        self.service.sendMsg(request, 'POST')
        return server.NOT_DONE_YET
    
    def render_PUT(self, request):

        self.service.sendMsg(request, 'PUT')
        return server.NOT_DONE_YET
    
    def render_DELETE(self, request):

        self.service.sendMsg(request, 'DELETE')
        return server.NOT_DONE_YET
        
    def getChild(self, path, request):
        
        return self
        
components.registerAdapter(EndPoint, interfaces.IRepeaterPublishService,
                           resource.IResource)