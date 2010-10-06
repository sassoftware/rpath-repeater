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

import sys
import StringIO

from xml.dom import minidom
from conary.lib.formattrace import formatTrace

from twisted.internet import reactor

from rmake3.core import handler
from rmake3.core import types

from rpath_repeater.utils import nodeinfo, wbemlib, cimupdater
from rpath_repeater.utils.base_forwarding_plugin import BaseHandler, \
    BaseTaskHandler, BaseForwardingPlugin, XML, HTTPClientFactory, Options, \
    exposed

PREFIX = 'com.rpath.sputnik'
CIM_JOB = PREFIX + '.cimplugin'
CIM_TASK_REGISTER = PREFIX + '.register'
CIM_TASK_SHUTDOWN = PREFIX + '.shutdown'
CIM_TASK_POLLING = PREFIX + '.poll'
CIM_TASK_UPDATE = PREFIX + '.update'

class CimForwardingPlugin(BaseForwardingPlugin):

    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(CimHandler)

    def worker_get_task_types(self):
        return {
                CIM_TASK_REGISTER: RegisterTask,
                CIM_TASK_SHUTDOWN: ShutdownTask,
                CIM_TASK_POLLING: PollingTask,
                CIM_TASK_UPDATE: UpdateTask,
                CIM_TASK_SHUTDOWN: ShutdownTask,
                }


class CimHandler(BaseHandler):
    timeout = 7200
    port = 5989

    jobType = CIM_JOB
    firstState = 'cimCall'

    X_Event_Uuid_Header = 'X-rBuilder-Event-UUID'

    def setup (self):
        BaseHandler.setup()

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
        self.setStatus(101, "Initiating CIM call")
        self.initCall()
        self.cimParams = CimParams(**self.data.pop('cimParams', {}))

        if not self.zone:
            self.setStatus(400, "CIM call requires a zone")
            self.postFailure()
            return

        cp = self.cimParams
        if self.method in self.Meta.exposed:
            self.setStatus(102, "CIM call: %s %s:%s" %
                (self.method, cp.host, cp.port))
            return self.method

        self.setStatus(405, "Method does not exist: %s" % (self.method, ))
        self.postFailure()
        return

    def _handleTask(self, task):
        """
        Handle responses for a task execution
        """
        d = self.waitForTask(task)
        d.addCallbacks(self._handleTaskCallback, self._handleTaskError)
        return d

    def _handleTaskCallback(self, task):
        if task.status.failed:
            self.setStatus(task.status.code, "Failed")
            self.postFailure()
        else:
            response = task.task_data.getObject().response
            self.job.data = response
            self.setStatus(200, "Done")
            self.postResults()
        return 'done'

    def _handleTaskError(self, reason):
        """
        Error callback that gets invoked if rmake failed to handle the job.
        Clean errors from the repeater do not see this function.
        """
        d = self.failJob(reason)
        self.postFailure()
        return d

    @exposed
    def register(self):
        self.setStatus(103, "Creating task")

        nodes = [x + ':8443' for x in self._getZoneAddresses()]
        args = RactivateData(self.cimParams, nodes,
                self.methodArguments.get('requiredNetwork'))
        task = self.newTask('register', CIM_TASK_REGISTER, args, zone=self.zone)
        return self._handleTask(task)

    @exposed
    def shutdown(self):
        self.setStatus(103, "Creating task")

        args = CimData(self.cimParams)
        task = self.newTask('shutdown', CIM_TASK_SHUTDOWN, args, zone=self.zone)
        return self._handleTask(task)

    @exposed
    def polling(self):
        self.setStatus(103, "Creating task")

        args = CimData(self.cimParams)
        task = self.newTask('Polling', CIM_TASK_POLLING, args, zone=self.zone)
        return self._handleTask(task)

    def postResults(self, elt=None):
        host = self.resultsLocation.get('host', 'localhost')
        port = self.resultsLocation.get('port', 80)
        path = self.resultsLocation.get('path')
        if not path:
            return
        if elt is None:
            dom = minidom.parseString(self.job.data)
            elt = dom.firstChild
        self.addEventInfo(elt)
        self.addJobInfo(elt)
        data = self.toXml(elt)
        headers = {
            'Content-Type' : 'application/xml; charset="utf-8"',
            'Host' : host, }
        eventUuid = self.cimParams.eventUuid
        if eventUuid:
            headers[self.X_Event_Uuid_Header] = eventUuid.encode('ascii')
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

    def addEventInfo(self, elt):
        if not self.cimParams.eventUuid:
            return
        elt.appendChild(XML.Text("event_uuid", self.cimParams.eventUuid))
        return elt

    def addJobInfo(self, elt):
        # Parse the data, we need to insert the job uuid
        T = XML.Text
        jobStateMap = { False : 'Failed', True : 'Completed' }
        jobStateString = jobStateMap[self.job.status.completed]
        job = XML.Element("job",
            T("job_uuid", self.job.job_uuid),
            T("job_state", jobStateString),
        )
        elt.appendChild(XML.Element("jobs", job))

    def toXml(self, elt):
        return elt.toxml(encoding="UTF-8").encode("utf-8")

    @exposed
    def update(self):
        self.setStatus(103, "Creating task")

        sources = self.methodArguments['sources']

        args = UpdateData(self.cimParams, sources)
        task = self.newTask('Update', CIM_TASK_UPDATE,args, zone=self.zone)
        return self._handleTask(task)

CimParams = types.slottype('CimParams',
    'host port clientCert clientKey eventUuid instanceId targetName targetType')
# These are just the starting point attributes
CimData = types.slottype('CimData', 'p response')
RactivateData = types.slottype('RactivateData',
        'p nodes requiredNetwork response')
UpdateData = types.slottype('UpdateData', 'p sources response')

class CIMTaskHandler(BaseTaskHandler):

    def getWbemConnection(self, data):
        x509Dict = {}
        if None not in [ data.p.clientCert, data.p.clientKey ]:
            self._clientCertFile = self._tempFile("client-cert-",
                data.p.clientCert)
            self._clientKeyFile = self._tempFile("client-key-",
                data.p.clientKey)
            x509Dict = dict(cert_file=self._clientCertFile.name,
                            key_file=self._clientKeyFile.name)

        # Do the probing early, since WBEMServer does not do proper timeouts
        # May raise ProbeHostError, which we catch in run()
        self._serverCert = nodeinfo.probe_host_ssl(data.p.host, data.p.port,
            **x509Dict)
        server = wbemlib.WBEMServer("https://" + data.p.host, x509=x509Dict)
        return server

    def run(self):
        """
        Exception handing for the _run method doing the real work
        """
        data = self.getData()
        try:
            self._run(data)
        except nodeinfo.ProbeHostError, e:
            self.sendStatus(404, "CIM not found on %s:%d: %s" % (
                data.p.host, data.p.port, str(e)))
        except:
            typ, value, tb = sys.exc_info()
            out = StringIO.StringIO()
            formatTrace(typ, value, tb, stream = out, withLocals = False)
            out.write("\nFull stack:\n")
            formatTrace(typ, value, tb, stream = out, withLocals = True)

            self.sendStatus(450, "Error in CIM call: %s" % str(value),
                    out.getvalue())

    def _getServerCert(self):
        return [ XML.Text("ssl_server_certificate", self._serverCert) ]

    def _getUuids(self, server):
        cs = server.RPATH_ComputerSystem.EnumerateInstances()
        if not cs:
            return []
        cs = cs[0]
        T = XML.Text
        return [ T("local_uuid", cs['LocalUUID']),
            T("generated_uuid", cs['GeneratedUUID']) ]

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
        return XML.Element("installed_software", *troves)


class RegisterTask(CIMTaskHandler):

    def _run(self, data):
        self.sendStatus(104, "Contacting host %s on port %d to rActivate itself" % (
            data.p.host, data.p.port))

        #send CIM rActivate request
        server = self.getWbemConnection(data)
        cimInstances = server.RPATH_ComputerSystem.EnumerateInstanceNames()
        arguments = dict(
            ManagementNodeAddresses = sorted(data.nodes))
        if data.p.eventUuid:
            arguments.update(EventUUID = data.p.eventUuid)
        if data.requiredNetwork:
            arguments.update(RequiredNetwork = data.requiredNetwork)
        ret = server.conn.callMethod(cimInstances[0], 'RemoteRegistration',
            **arguments)
        data.response = "<system/>"
        self.setData(data)

        retVal, outParams = ret
        if retVal == 0:
            self.sendStatus(200, "Host %s registration initiated" % data.p.host)
        else:
            errorSummary = outParams.get('errorSummary', '')
            errorDetails = outParams.get('errorDetails', '')
            self.sendStatus(451, "Host %s registration failed: %s" %
                (data.p.host, errorSummary), errorDetails)

class ShutdownTask(CIMTaskHandler):

    def _run(self, data):
        self.sendStatus(101, "Contacting host %s to shut itself down" % (
            data.p.host))

        #send CIM Shutdown request
        server = self.getWbemConnection(data)
        cimInstances = server.Linux_OperatingSystem.EnumerateInstanceNames()
        value, args = server.conn.callMethod(cimInstances[0], 'Shutdown')
        data.response = "<system/>"

        self.setData(data)
        if not value:
            self.sendStatus(200, "Host %s will now shutdown" % data.p.host)
        else:
            self.sendStatus(401, "Could not shutdown host %s" % data.p.host)

class PollingTask(CIMTaskHandler):

    def _run(self, data):
        self.sendStatus(101, "Contacting host %s on port %d to Poll it for info" % (
            data.p.host, data.p.port))

        server = self.getWbemConnection(data)
        children = self._getUuids(server)
        children.extend(self._getServerCert())
        children.append(self._getSoftwareVersions(server))

        el = XML.Element("system", *children)

        data.response = el.toxml(encoding="UTF-8")
        self.setData(data)
        self.sendStatus(200, "Host %s has been polled" % data.p.host)

class UpdateTask(CIMTaskHandler):

    def _run(self, data):
        self.sendStatus(101, "Contacting host %s on port %d to update it" % (
            data.p.host, data.p.port))

        server = self.getWbemConnection(data)
        self._applySoftwareUpdate(server, data.sources)
        children = self._getUuids(server)
        children.extend(self._getServerCert())
        children.append(self._getSoftwareVersions(server))

        el = XML.Element("system", *children)

        data.response = el.toxml(encoding="UTF-8")
        self.setData(data)
        self.sendStatus(200, "Host %s has been updated" % data.p.host)

    def _applySoftwareUpdate(self, server, sources):
        cimUpdater = cimupdater.CIMUpdater(server)
        cimUpdater.applyUpdate(sources)
        return None
