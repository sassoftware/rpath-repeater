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

from rpath_repeater import logger

from twisted.internet import defer

import httplib

class HttpRepeater(object):
    
    def __init__(self, host):
        self.host = host
        
   
    def dispatch(self, method, endpoint, msg, headers):
        d = defer.Deferred()
        
        self.method = method.upper()
        self.endpoint = endpoint
        self.msg = msg
        self.headers = headers
        self.response = None
        
        @d.addCallback
        def connect(result):
            self.conn = httplib.HTTPConnection(self.host)
            
        @d.addCallback
        def send(result):
            #fixme - handle the case when connections can't be made          
            if self.conn:
                self.conn.request(self.method, self.endpoint, self.msg, self.headers)

                self.response = self.conn.getresponse()
                
                self.conn.close()
 
        @d.addErrback
        def errorHandler(failure):
            logger.logFailure(failure)
        
        d.callback(self)
        
        return self.response
        