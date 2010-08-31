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

from xml.dom import minidom
from conary import conaryclient
from conary import versions

from twisted.web import client
from twisted.internet import reactor

from rmake3.core import handler
from rmake3.core import plug_dispatcher
from rmake3.core import types
from rmake3.worker import plug_worker

from rpath_repeater.utils import nodeinfo, wbemlib
from rpath_repeater.utils.immutabledict import FrozenImmutableDict

PREFIX = 'com.rpath.sputnik'
CIM_JOB = PREFIX + '.cimplugin'
CIM_TASK_REGISTER = PREFIX + '.register'
CIM_TASK_SHUTDOWN = PREFIX + '.shutdown'
CIM_TASK_POLLING = PREFIX + '.poll'
CIM_TASK_UPDATE = PREFIX + '.update'

class CimForwardingPlugin(plug_dispatcher.DispatcherPlugin, plug_worker.WorkerPlugin):
    
    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(CimHandler)

    def worker_get_task_types(self):
        return {
                CIM_TASK_REGISTER: RegisterTask,
                CIM_TASK_SHUTDOWN: ShutdownTask,
                CIM_TASK_POLLING: PollingTask,
                CIM_TASK_UPDATE: UpdateTask,
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
        
        self.data = self.getData().thaw().getDict()
        self.zone = self.data.pop('zone', None)
        self.cimParams = CimParams(**self.data.pop('cimParams', {}))
        self.method = self.data['method']
        self.methodArguments = self.data.pop('methodArguments', {})
        self.resultsLocation = self.data.pop('resultsLocation', {})
        self.eventUuid = self.data.pop('eventUuid', None)

        # XXX FIXME this should not happen here, but on the endpoint
        cp = self.cimParams
        self.setStatus(102, "Starting to probe the host: %s" % (cp.host))
        try:
            nodeinfo.probe_host(cp.host, cp.port)
        except nodeinfo.ProbeHostError:
            self.setStatus(404, "CIM not found on host: %s port: %d" % (
                cp.host, cp.port))
            self.postFailure()
            return

        if hasattr(self, self.method):
            return self.method

        self.setStatus(405, "Method does not exist: %s" % (self.method))
        self.postFailure()
        return

    def register(self):
        self.setStatus(103, "Starting the registration {1/2}")
        
        args = RactivateData(self.cimParams, nodeinfo.get_hostname() +':8443',
            self.methodArguments.get('requiredNetwork'))
        task = self.newTask('register', CIM_TASK_REGISTER, args, zone=self.zone)

        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! CIM Registration has been kicked off. {2/2}")
            return 'done'
        return self.gatherTasks([task], cb_gather)
    
    def shutdown(self):
        self.setStatus(103, "Shutting down the managed server")

        args = CimData(self.cimParams)
        task = self.newTask('shutdown', CIM_TASK_SHUTDOWN, args, zone=self.zone)
        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! cim shutdown of %s" % (result))
            return 'done'
        return self.gatherTasks([task], cb_gather) 
    
    def polling(self):
        self.setStatus(103, "Starting the polling {1/2}")

        args = CimData(self.cimParams)
        task = self.newTask('Polling', CIM_TASK_POLLING, args, zone=self.zone)
        def cb_gather(results):
            task, = results
            result = task.task_data.getObject()
            self.job.data = result
            self.setStatus(200, "Done! cim polling of %s" % (result))
            self.postResults()
            return 'done'
        return self.gatherTasks([task], cb_gather)

    def postFailure(self):
        T = XML.Text
        el = XML.Element("system",
            T("event_uuid", self.cimParams.eventUuid))
        self.postResults(el)

    def postResults(self, elt=None):
        host = self.resultsLocation.get('host', 'localhost')
        port = self.resultsLocation.get('port', 80)
        path = self.resultsLocation.get('path')
        if not path:
            return
        if elt is None:
            dom = minidom.parseString(self.job.data)
            elt = dom.firstChild
        self.addJobInfo(elt)
        data = self.toXml(elt)
        headers = {
            'Content-Type' : 'application/xml; charset="utf-8"',
            'Host' : host, }
        agent = "rmake-plugin/1.0"
        fact = HTTPClientFactory(path, method='PUT', postdata=data,
            headers = headers, agent = agent)
        @fact.deferred.addCallback
        def processResult(result):
            print "Received result for", host, result
            return result

        @fact.deferred.addErrback
        def processError(error):
            print "Error!", error.getErrorMessage()

        reactor.connectTCP(host, port, fact)

    def addJobInfo(self, elt):
        # Parse the data, we need to insert the job uuid
        T = XML.Text
        jobStateMap = { False : 'Failed', True : 'Completed' }
        jobStateString = jobStateMap[self.job.status.completed]
        job = XML.Element("job",
            T("job_uuid", self.job.job_uuid),
            T("job_state", jobStateString),
        )
        elt.appendChild(XML.Element("system_jobs", job))

    def toXml(self, elt):
        return elt.toxml(encoding="UTF-8").encode("utf-8")

    def update(self):
        self.setStatus(103, "Starting the updating {1/2}")

        sources = self.methodArguments['sources']

        args = UpdateData(self.cimParams, sources)
        task = self.newTask('Update', CIM_TASK_UPDATE,args, zone=self.zone)

        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! cim update got a result of: %s" % (result))
            return 'done'
        return self.gatherTasks([task], cb_gather)   
    
CimParams = types.slottype('CimParams',
    'host port clientCert clientKey eventUuid')
# These are just the starting point attributes
CimData = types.slottype('CimData', 'p response')
RactivateData = types.slottype('RactivateData', 'p node requiredNetwork response')
UpdateData = types.slottype('UpdateData', 'p sources response')

class CIMTaskHandler(plug_worker.TaskHandler):
    def getWbemConnection(self, data):
        server = wbemlib.WBEMServer("https://" + data.p.host)
        return server

class RegisterTask(CIMTaskHandler):
    
    def run(self):
        data = self.getData()
        self.sendStatus(104, "Contacting host %s on port %d to rActivate itself" % (
            data.p.host, data.p.port))

        #send CIM rActivate request
        server = self.getWbemConnection(data)
        cimInstances = server.RPATH_ComputerSystem.EnumerateInstanceNames()
        arguments = dict(
            ManagementNodeAddresses = [data.node])
        if data.p.eventUuid:
            arguments.update(EventUUID = data.p.eventUuid)
        if data.requiredNetwork:
            arguments.update(RequiredNetwork = data.requiredNetwork)
        server.conn.callMethod(cimInstances[0], 'RemoteRegistration',
            **arguments)
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
        children = self._getUuids(server)
        children.append(self._getSoftwareVersions(server))
        children.extend(self._getEventUuid(data))

        el = XML.Element("system", *children)

        self.setData(el.toxml(encoding="UTF-8"))
        self.sendStatus(200, "Host %s has been polled" % data.p.host)

    def _getUuids(self, server):
        cs = server.RPATH_ComputerSystem.EnumerateInstances()
        if not cs:
            return {}
        cs = cs[0]
        T = XML.Text
        return [ T("localUuid", cs['LocalUUID']),
            T("generatedUuid", cs['GeneratedUUID']) ]

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

        # Start creating the XML document
        troves = [ self._trove(si) for si in siList ]
        return XML.Element("installedSoftware", *troves)

    def _getEventUuid(self, data):
        if not data.p.eventUuid:
            return []
        return [ XML.Text("eventUuid", data.p.eventUuid) ]

    @classmethod
    def _trove(cls, si):
        Text = XML.Text
        nvf = "%s=%s" % (si['name'], si['VersionString'])
        n, v, f = conaryclient.cmdline.parseTroveSpec(nvf)

        name = Text("name", n)
        version = cls._version(v, f)
        flavor = Text("flavor", str(f))

        return XML.Element("trove", name, version, flavor)

    @classmethod
    def _version(cls, v, f):
        thawed_v = versions.ThawVersion(v)
        Text = XML.Text
        full = Text("full", str(thawed_v))
        ordering = Text("ordering", thawed_v.timeStamps()[0])
        revision = Text("revision", str(thawed_v.trailingRevision()))
        label = Text("label", str(thawed_v.trailingLabel()))
        flavor = Text("flavor", str(f))
        return XML.Element("version", full, label, revision, ordering, flavor)

class XML(object):
    @classmethod
    def Text(cls, tagName, text):
        txt = minidom.Text()
        txt.data = text
        return cls.Element(tagName, txt)

    @classmethod
    def Element(cls, tagName, *children, **attributes):
        node = cls._Node(tagName, minidom.Element)
        for child in children:
            node.appendChild(child)
        for k, v in attributes.items():
            node.setAttribute(k, unicode(v).encode("utf-8"))
        return node

    @classmethod
    def _Node(cls, tagName, factory):
        node = factory(tagName)
        return node

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
