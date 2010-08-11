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

from rmake.client import RmakeClient
from rmake.lib import uuid
from rmake.core.types import RmakeJob

from rpath_repeater.utils.immutabledict import FrozenImmutableDict

class RepeaterClient(object):
    
    def __init__(self, address = None):
        if not address:
            address = 'http://localhost:9999/'
            
        self.client = RmakeClient(address)
        
    def activate(self, host, port = None):
        data = dict(host=host, port = port)
        data.update(method = 'rActivate')
        data=FrozenImmutableDict(data)

        job = RmakeJob(uuid.uuid4(), 'com.rpath.sputnik.cimplugin', owner='nobody',
                       data=data, 
                       ).freeze()

        juuid = job.job_uuid
        job = self.client.createJob(job)

        return (juuid, job)
        