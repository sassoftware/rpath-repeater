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

from rmake.core import handler
from rmake.core import plug_dispatcher
from rmake.core import types
from rmake.worker import plug_worker


PREFIX = 'com.rpath.sputnik.cimplugin'
CIM_JOB = PREFIX
CIM_TASK_RACTIVATE = PREFIX + '.ractivate'
CIM_TASK_POLLING = PREFIX + '.poll'


class CimForwardingPlugin(plug_dispatcher.DispatcherPlugin, plug_worker.WorkerPlugin):
    
    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(CimHandler)

    def worker_get_task_types(self):
        return {
                CIM_TASK_RACTIVATE: RactivateTask,
                CIM_TASK_POLLING: PollingTask,
                }
        
class CimHandler(handler.JobHandler):
    
    timeout = 3600
    port = 5989
        
    jobType = CIM_JOB
    firstState = 'ractivate'
       
    def ractivate(self):
        self.setStatus(102, "Starting the rActivation {1/2}")
        params = CimParams(self.timeout)
        data = eval(self.getData())
        task = self.newTask('rActivate', CIM_TASK_RACTIVATE,
                RactivateData(params, data['host'], self.port))
        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! cim ractivation response: %s" % (result))
            return 'done'
        return self.gatherTasks([task], cb_gather)
    
    def polling(self):
        self.setStatus(102, "Starting the polling {1/2}")
        params = CimParams(self.cert, self.key)
        task = self.newTask('Polling', CIM_TASK_POLLING,
                PollingData(params, data['host'], self.port))
        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! cim polling of %s" % (result))
            return 'done'
        return self.gatherTasks([task], cb_gather)    
    
CimParams = types.slottype('CimParams', 'timeout')
# These are just the starting point attributes
RactivateData = types.slottype('RactivateData', 'p host port response')
PollingData = types.slottype('PollingData', 'p host port response')
    
class RactivateTask(plug_worker.TaskHandler):
    
    

    def run(self):
        data = self.getData()
        self.sendStatus(101, "Contacting host %s on port %d to rActivate itself" % (
            data.host, data.port))

        #send CIM rActivate request
        data.response = "<system/>"

        self.setData(data)
        self.sendStatus(200, "Host %s will try to rActivate itself" % data.host)


class PollingTask(plug_worker.TaskHandler):

    def run(self):
        data = self.getData()
        self.sendStatus(101, "Contacting host %s on port %d to Poll it for info" % (
            data.host, data.port))

        #send CIM poll request
        data.response = ""

        self.setData(data)
        self.sendStatus(200, "Host %s has been polled" % data.host)
    
