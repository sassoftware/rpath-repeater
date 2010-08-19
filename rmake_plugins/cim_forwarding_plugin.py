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

from twisted.web import client
from twisted.internet import reactor

from rmake3.core import handler
from rmake3.core import plug_dispatcher
from rmake3.core import types
from rmake3.worker import plug_worker

from rpath_repeater.utils import nodeinfo, wbemlib
from rpath_repeater.utils.immutabledict import FrozenImmutableDict

PREFIX = 'com.rpath.sputnik'
PRESENCE_JOB = PREFIX + '.presence'
CIM_JOB = PREFIX + '.cimplugin'
CIM_TASK_RACTIVATE = PREFIX + '.ractivate'
CIM_TASK_SHUTDOWN = PREFIX + '.shutdown'
CIM_TASK_POLLING = PREFIX + '.poll'
CIM_TASK_UPDATE = PREFIX + '.update'

class CimForwardingPlugin(plug_dispatcher.DispatcherPlugin, plug_worker.WorkerPlugin):
    
    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(CimHandler)
        handler.registerHandler(PresenceHandler)

    def worker_get_task_types(self):
        return {
                CIM_TASK_RACTIVATE: RactivateTask,
                CIM_TASK_SHUTDOWN: ShutdownTask,
                CIM_TASK_POLLING: PollingTask,
                CIM_TASK_UPDATE: UpdateTask,
                }
        
class PresenceHandler(handler.JobHandler):
    
    jobType = PRESENCE_JOB
    firstState = 'neighbors'
    
    def neighbors(self):
        self.setStatus(100, "Fetching neighbors")
        neighbors = {}

        for key, value in self.dispatcher.bus.link.neighbors.items():
            neighbors[key] =value.isAvailable
        
        self.job.data = types.FrozenObject.fromObject(neighbors)
        self.setStatus(200, "Got neighbors")
        return
        
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
        
        self.data = self.getData().thaw().getDict()
        self.method = self.data['method']
        self.host = self.data['host']
        self.resultsLocation = self.data.pop('resultsLocation', {})
        
        self.params = CimParams(self.host, self.port)
        
        self.setStatus(102, "Starting to probe the host: %s" % (self.host))
        try:
            nodeinfo.probe_host(self.host, self.port)
        except nodeinfo.ProbeHostError:
            self.setStatus(404, "CIM not found on host: %s port: %d" % (self.host, self.port))
            return 
        
        if hasattr(self, self.method):
            return self.method
        
        self.setStatus(405, "Method does not exist: %s" % (self.method))
        return   

    def ractivate(self):
        self.setStatus(103, "Starting the rActivation {1/2}")
        
        task = self.newTask('rActivate', CIM_TASK_RACTIVATE,
                RactivateData(self.params, nodeinfo.get_hostname() +':8443'))
        
        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! cim ractivation response: %s" % (result))
            return 'done'
        return self.gatherTasks([task], cb_gather)
    
    def shutdown(self):
        self.setStatus(103, "Shutting down the managed server")
        
        task = self.newTask('shutdown', CIM_TASK_SHUTDOWN,
                CimData(self.params))
        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! cim shutdown of %s" % (result))
            return 'done'
        return self.gatherTasks([task], cb_gather) 
    
    def polling(self):
        self.setStatus(103, "Starting the polling {1/2}")

        task = self.newTask('Polling', CIM_TASK_POLLING,
                CimData(self.params))
        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! cim polling of %s" % (result))
            host = 'localhost'
            port = self.resultsLocation.get('port', 80)
            path = self.resultsLocation.get('path')
            if path:
                data = str(self.job.data.thaw().getObject())
                headers = { 'Content-Type' : 'application/xml; charset="utf-8"' }
                agent = "rmake-plugin/1.0"
                fact = HTTPClientFactory(path, method='POST', postdata=data,
                    headers = headers, agent = agent)
                @fact.deferred.addCallback
                def processResult(result):
                    print "Received result for", host, result
                    return result

                @fact.deferred.addErrback
                def processError(error):
                    print "Error!", error.getErrorMessage()

                print "Connecting to http://%s:%s%s" % (host, port, path)
                reactor.connectTCP(host, port, fact)
            return 'done'
        return self.gatherTasks([task], cb_gather)    
    
    def update(self):
        self.setStatus(103, "Starting the updating {1/2}")

        sources = self.data['sources']

        task = self.newTask('Update', CIM_TASK_UPDATE,
                UpdateData(self.params, sources))
        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! cim update got a result of: %s" % (result))
            return 'done'
        return self.gatherTasks([task], cb_gather)   
    
CimParams = types.slottype('CimParams', 'host port')
# These are just the starting point attributes
CimData = types.slottype('CimData', 'p response')
RactivateData = types.slottype('RactivateData', 'p node response')
UpdateData = types.slottype('UpdateData', 'p sources response')
    
class CIMTaskHandler(plug_worker.TaskHandler):
    def getWbemConnection(self, data):
        server = wbemlib.WBEMServer("https://" + data.p.host)
        return server

class RactivateTask(CIMTaskHandler):
    
    def run(self):
        data = self.getData()
        self.sendStatus(104, "Contacting host %s on port %d to rActivate itself" % (
            data.p.host, data.p.port))

        #send CIM rActivate request
        server = self.getWbemConnection(data)
        cimInstances = server.RPATH_ComputerSystem.EnumerateInstanceNames()
        server.conn.callMethod(cimInstances[0], 'RemoteActivation', ManagementNodeAddresses = [data.node])
        data.response = ""

        self.setData(data)
        self.sendStatus(200, "Host %s will try to rActivate itself" % data.p.host)
        
class ShutdownTask(CIMTaskHandler):
    
    def run(self):
        data = self.getData()
        self.sendStatus(101, "Contacting host %s to shut itself down" % (
            data.p.host))

        #send CIM Shutdown request
        server = self.getWbemConnection(data)
        cimInstances = server.Linux_OperatingSystem.EnumerateInstanceNames()
        value, args = server.conn.callMethod(cimInstances[0], 'Shutdown')
        data.response = str(value)

        self.setData(data)
        if not value:
            self.sendStatus(200, "Host %s will now shutdown" % data.p.host)
        else:
            self.sendStatus(401, "Could not shutdown host %s" % data.p.host)

class PollingTask(CIMTaskHandler):

    def run(self):
        data = self.getData()
        self.sendStatus(101, "Contacting host %s on port %d to Poll it for info" % (
            data.p.host, data.p.port))

        server = self.getWbemConnection(data)
        swVersions = self._getSoftwareVersions(server)
        uuids = self._getUuids(server)

        #send CIM poll request
        data.response = dict(ComputerSystem = uuids,
            SoftwareVersions = swVersions)

        self.setData(data)
        self.sendStatus(200, "Host %s has been polled" % data.p.host)

    def _getUuids(self, server):
        cs = server.RPATH_ComputerSystem.EnumerateInstances()
        if not cs:
            return {}
        cs = cs[0]
        return dict(LocalUUID=cs['LocalUUID'], GeneratedUUID=cs['GeneratedUUID'])

    def _getSoftwareVersions(self, server):
        # Fetch instances of the ElementSoftwareIdentity association.
        # We need to figure out which SoftwareIdentity instances are installed
        # We do this by filtering the state
        states = set([2, 6])
        esi = server.RPATH_ElementSoftwareIdentity.EnumerateInstances()
        installedSofwareIdentityNames = set(g['Antecedent']['InstanceID']
            for g in esi
                if states.issubset(g.properties['ElementSoftwareStatus'].value))
        # Now fetch all SoftwareIdentity elements and filter out the ones not
        # installed (i.e. InstanceID not in installedSofwareIdentityNames)
        siList = server.RPATH_SoftwareIdentity.EnumerateInstances()
        siList = [ si for si in siList
            if si['InstanceID'] in installedSofwareIdentityNames ]
        ret = [ "%s=%s" % (si['name'], si['VersionString'])
            for si in siList ]
        return ret
    
class UpdateTask(CIMTaskHandler):

    def run(self):
        data = self.getData()
        self.sendStatus(101, "Contacting host %s on port %d to Update it for info" % (
            data.p.host, data.p.port))

        #send CIM poll request
        data.response = "*handwave*"

        self.setData(data)
        self.sendStatus(200, "Host %s has been updated" % data.p.host)

class HTTPClientFactory(client.HTTPClientFactory):
    def __init__(self, url, *args, **kwargs):
        client.HTTPClientFactory.__init__(self, url, *args, **kwargs)
        self.status = None
        self.deferred.addCallback(
            lambda data: (data, self.status, self.response_headers))
