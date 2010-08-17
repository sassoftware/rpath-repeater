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

from rmake3.client import RmakeClient
from rmake3.lib import uuid as RmakeUuid
from rmake3.core.types import RmakeJob

from rpath_repeater.utils.immutabledict import FrozenImmutableDict

class RepeaterClient(object):
    
    __PLUGIN_NS = 'com.rpath.sputnik.cimplugin'
    
    def __init__(self, address = None):
        if not address:
            address = 'http://localhost:9998/'
            
        self.client = RmakeClient(address)
        
    def __callDispatcher(self, data):
        data=FrozenImmutableDict(data)

        job = RmakeJob(RmakeUuid.uuid4(), self.__PLUGIN_NS, owner='nobody',
                       data=data, 
                       ).freeze()

        uuid = job.job_uuid
        job = self.client.createJob(job)

        return (uuid, job.thaw())
        
    def activate(self, host, port = None):
        data = dict(host=host, port = port)
        data.update(method = 'ractivate')

        return self.__callDispatcher(data)
    
    def shutdown(self, host, port = None):
        data = dict(host=host, port = port)
        data.update(method = 'shutdown')
        
        return self.__callDispatcher(data)
    
    def getJob(self, uuid):
        return self.client.getJob(uuid)
    
def main():
    cli = RepeaterClient()
    cli.activate('dhcp236.eng.rpath.com')
 
if __name__ == "__main__":
    main()
