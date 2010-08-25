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

import sys, time
from rmake3.client import RmakeClient
from rmake3.lib import uuid as RmakeUuid
from rmake3.core.types import RmakeJob

from rpath_repeater.utils.immutabledict import FrozenImmutableDict

class RepeaterClient(object):
    
    __CIM_PLUGIN_NS = 'com.rpath.sputnik.cimplugin'
    __PRESENCE_PLUGIN_NS = 'com.rpath.sputnik.presence'
    
    def __init__(self, address = None):
        if not address:
            address = 'http://localhost:9998/'
            
        self.client = RmakeClient(address)
        
    def __callDispatcher(self, data):
        data=FrozenImmutableDict(data)

        job = RmakeJob(RmakeUuid.uuid4(), self.__CIM_PLUGIN_NS, owner='nobody',
                       data=data, 
                       ).freeze()

        uuid = job.job_uuid
        job = self.client.createJob(job)

        return (uuid, job.thaw())
        
    def register(self, host, zone, port = None, requiredNetwork=None):
        data = dict(host=host, port = port, zone = zone,
            requiredNetwork=requiredNetwork)
        data.update(method = 'register')

        return self.__callDispatcher(data)
    
    def shutdown(self, host, zone, port = None):
        data = dict(host=host, port = port, zone = zone)
        data.update(method = 'shutdown')
        
        return self.__callDispatcher(data)
    
    def update(self, host, zone, sources, port = None):
        data = dict(host=host, port = port, zone = zone,
                    sources = sources)
        data.update(method = 'update')
        
        return self.__callDispatcher(data)      
    
    def retireNode(self, node, zone, port = None):
        """ This is a temporary large hammer for handling the retirement
            of a management node.
        """
        return self.shutdown(node, zone, port)
    
    def getNodes(self):
        return self.client.getWorkerList()

    def poll(self, host, zone, resultsLocation=None):
        data = dict(host=host, resultsLocation = resultsLocation)
        data.update(method='polling')

        return self.__callDispatcher(data)

    def getJob(self, uuid):
        return self.client.getJob(uuid).thaw()

def main():
    if len(sys.argv) < 2:
        print "Usage: %s system" % sys.argv[0]
        return 1
    system = sys.argv[1]
    sputnik = 'sputnik1'
    cli = RepeaterClient()
    if 1:
        uuid, job = cli.register(system, sputnik,)# requiredNetwork="1.1.1.1")
    else:
        uuid, job = cli.poll(system, sputnik,
            resultsLocation=dict(path='/adfadf', port=1234),
            requiredNetwork='1.2.3.4')
    import time
    while 1:
        job = cli.getJob(uuid)
        if job.status.completed:
            break
        time.sleep(1)
    #import epdb; epdb.st()
 
if __name__ == "__main__":
    main()
