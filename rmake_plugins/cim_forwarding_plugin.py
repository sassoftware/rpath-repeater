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

from rmake3.core import handler
from rmake3.core import plug_dispatcher
from rmake3.core import types
from rmake3.worker import plug_worker

from rpath_repeater.utils import nodeinfo, wbemlib

PREFIX = 'com.rpath.sputnik.cimplugin'
CIM_JOB = PREFIX
CIM_TASK_RACTIVATE = PREFIX + '.ractivate'
CIM_TASK_SHUTDOWN = PREFIX + '.shutdown'
CIM_TASK_POLLING = PREFIX + '.poll'


class CimForwardingPlugin(plug_dispatcher.DispatcherPlugin, plug_worker.WorkerPlugin):
    
    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(CimHandler)

    def worker_get_task_types(self):
        return {
                CIM_TASK_RACTIVATE: RactivateTask,
                CIM_TASK_SHUTDOWN: ShutdownTask,
                CIM_TASK_POLLING: PollingTask,
                }
        
class CimHandler(handler.JobHandler):
    
    timeout = 7200
    port = 5989
        
    jobType = CIM_JOB
    firstState = 'cimCall'
    
    def setup (self):
        cfg = self.dispatcher.cfg
        
        # get configuration options
        if self.__class__.__name__ in cfg.pluginOption:
            options = cfg.pluginOption[self.__class__.__name__]
            for option in options:
                key, value = option.split()
                
                if key == 'timeout':
                    self.timeout = int(value)
                elif key == 'port':
                    self.port = int(value)

    def cimCall(self):
        self.setStatus(101, "Starting the CIM call {0/2}")
        data = self.getData().thaw().getDict()
        method = data['method']
        host = data['host']
        
        params = CimParams(host, self.port)
        
        self.setStatus(102, "Starting to probe the host: %s" % (host))
        try:
            nodeinfo.probe_host(host, self.port)
        except self.ProbeHostError:
            self.setStatus(404, "CIM not found on host: %s port: %d" % (host, self.port))
            return 
        
        if hasattr(self, method, None):
            getattr(self, method)(params, data)
        else:
            self.setStatus(405, "Method does not exist: %s" % (method))

    def ractivate(self, params, data):
        self.setStatus(103, "Starting the rActivation {1/2}")
        
        task = self.newTask('rActivate', CIM_TASK_RACTIVATE,
                RactivateData(params, nodeinfo.get_hostname() +':8443'))
        
        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! cim ractivation response: %s" % (result))
            return 'done'
        return self.gatherTasks([task], cb_gather)
    
    def shutdown(self, params, data):
        self.setStatus(103, "Shutting down the managed server")
        
        task = self.newTask('shutdown', CIM_TASK_SHUTDOWN,
                CimData(params))
        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! cim shutdown of %s" % (result))
            return 'done'
        return self.gatherTasks([task], cb_gather) 
    
    def polling(self, params, data):
        self.setStatus(103, "Starting the polling {1/2}")

        task = self.newTask('Polling', CIM_TASK_POLLING,
                CimData(params))
        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! cim polling of %s" % (result))
            return 'done'
        return self.gatherTasks([task], cb_gather)    
    
CimParams = types.slottype('CimParams', 'host port')
# These are just the starting point attributes
CimData = types.slottype('CimData', 'p response')
RactivateData = types.slottype('RactivateData', 'p node response')
    
class RactivateTask(plug_worker.TaskHandler):
    
    def run(self):
        data = self.getData()
        self.sendStatus(104, "Contacting host %s on port %d to rActivate itself" % (
            data.host, data.port))

        #send CIM rActivate request
        server = wbemlib.WBEMServer("https://" + data.host)
        cimInstances = server.RPATH_ComputerSystem.EnumerateInstanceNames()
        server.conn.callMethod(cimInstances[0], 'RemoteActivation', ManagementNodeAddresses = [data.node])
        data.response = ""

        self.setData(data)
        self.sendStatus(200, "Host %s will try to rActivate itself" % data.host)
        
class ShutdownTask(plug_worker.TaskHandler):
    
    def run(self):
        data = self.getData()
        self.sendStatus(101, "Contacting host %s to shut itself down" % (
            data.host))

        #send CIM Shutdown request
        server = wbemlib.WBEMServer("https://" + data.host)
        cimInstances = server.RPATH_ComputerSystem.EnumerateInstanceNames()
        value, args = server.conn.callMethod(cimInstances[0], 'Shutdown')
        data.response = str(value)

        self.setData()
        if not value:
            self.sendStatus(200, "Host %s will now shutdown" % data.host)
        else:
            self.sendStatus(401, "Could not shutdown host %s" % data.host)

class PollingTask(plug_worker.TaskHandler):

    def run(self):
        data = self.getData()
        self.sendStatus(101, "Contacting host %s on port %d to Poll it for info" % (
            data.host, data.port))

        #send CIM poll request
        data.response = ""

        self.setData(data)
        self.sendStatus(200, "Host %s has been polled" % data.host)
    
