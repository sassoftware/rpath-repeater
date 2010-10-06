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

from rpath_repeater.utils import nodeinfo, wbemlib, wmiupdater
from rpath_repeater.utils.base_forwarding_plugin import PREFIX, BaseHandler, \
    BaseTaskHandler, BaseForwardingPlugin, XML, HTTPClientFactory

WMI_JOB = PREFIX + '.wmiplugin'
WMI_TASK_REGISTER = PREFIX + '.register'
WMI_TASK_SHUTDOWN = PREFIX + '.shutdown'
WMI_TASK_POLLING = PREFIX + '.poll'
WMI_TASK_UPDATE = PREFIX + '.update'

class WmiForwardingPlugin(BaseForwardingPlugin):

    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(WmiHandler)

    def worker_get_task_types(self):
        return {
                WMI_TASK_REGISTER: RegisterTask,
                WMI_TASK_SHUTDOWN: ShutdownTask,
                WMI_TASK_POLLING: PollingTask,
                WMI_TASK_UPDATE: UpdateTask,
                WMI_TASK_SHUTDOWN: ShutdownTask,
                }

class WmiHandler(BaseHandler):

    timeout = 7200
    port = 5989

    jobType = WMI_JOB
    firstState = 'wmiCall'

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

    def wmiCall(self):
        self.setStatus(101, "Starting the WMI call {0/2}")
        self.initCall()

        if not self.zone:
            self.setStatus(400, "WMI call requires a zone")
            self.postFailure()
            return

        cp = self.wmiParams
        self.setStatus(102, "WMI call for %s:%s" %
            (cp.host, cp.port))

        if hasattr(self, self.method):
            return self.method

        self.setStatus(405, "Method does not exist: %s" % (self.method))
        self.postFailure()
        return

    def register(self):
        self.setStatus(103, "Starting the registration {1/2}")

        nodes = [x + ':8443' for x in self._getZoneAddresses()]
        args = RactivateData(self.wmiParams, nodes,
                self.methodArguments.get('requiredNetwork'))
        task = self.newTask('register', WMI_TASK_REGISTER, args, zone=self.zone)

        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! WMI Registration has been kicked off. {2/2}")
            return 'done'
        return self.gatherTasks([task], cb_gather)

    def shutdown(self):
        self.setStatus(103, "Shutting down the managed server")

        args = WmiData(self.wmiParams)
        task = self.newTask('shutdown', WMI_TASK_SHUTDOWN, args, zone=self.zone)
        def cb_gather(results):
            task, = results
            result = task.task_data.getObject().response
            self.job.data = types.FrozenObject.fromObject(result)
            self.setStatus(200, "Done! wmi shutdown of %s" % (result))
            return 'done'
        return self.gatherTasks([task], cb_gather)

    def polling(self):
        self.setStatus(103, "Starting the polling {1/2}")

        args = WmiData(self.wmiParams)
        task = self.newTask('Polling', WMI_TASK_POLLING, args, zone=self.zone)
        def cb_gather(results):
            task, = results
            result = task.task_data.getObject()
            self.job.data = result
            self.setStatus(200, "Done! wmi polling of %s" % (result))
            self.postResults()
            return 'done'
        return self.gatherTasks([task], cb_gather)

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
        eventUuid = self.wmiParams.eventUuid
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
        if not self.wmiParams.eventUuid:
            return
        elt.appendChild(XML.Text("event_uuid", self.wmiParams.eventUuid))
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

    def update(self):
        self.setStatus(103, "Starting the updating {1/2}")

        sources = self.methodArguments['sources']

        args = UpdateData(self.wmiParams, sources)
        task = self.newTask('Update', WMI_TASK_UPDATE,args, zone=self.zone)

        def cb_gather(results):
            task, = results
            result = task.task_data.getObject()
            self.job.data = result
            self.setStatus(200, "Done! wmi updating of %s" % (result))
            self.postResults()
            return 'done'
        return self.gatherTasks([task], cb_gather)

WmiParams = types.slottype('WmiParams',
    'host port clientCert clientKey eventUuid')
# These are just the starting point attributes
WmiData = types.slottype('WmiData', 'p response')
RactivateData = types.slottype('RactivateData',
        'p nodes requiredNetwork response')
UpdateData = types.slottype('UpdateData', 'p sources response')

class WMITaskHandler(BaseTaskHandler):

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
            self.sendStatus(404, "WMI not found on %s:%d: %s" % (
                data.p.host, data.p.port, str(e)))
        except:
            typ, value, tb = sys.exc_info()
            out = StringIO.StringIO()
            formatTrace(typ, value, tb, stream = out, withLocals = False)
            out.write("\nFull stack:\n")
            formatTrace(typ, value, tb, stream = out, withLocals = True)

            self.sendStatus(450, "Error in WMI call: %s" % str(value),
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


class RegisterTask(WMITaskHandler):

    def _run(self, data):
        self.sendStatus(104, "Contacting host %s on port %d to rActivate itself" % (
            data.p.host, data.p.port))

        #send WMI rActivate request
        server = self.getWbemConnection(data)
        wmiInstances = server.RPATH_ComputerSystem.EnumerateInstanceNames()
        arguments = dict(
            ManagementNodeAddresses = sorted(data.nodes))
        if data.p.eventUuid:
            arguments.update(EventUUID = data.p.eventUuid)
        if data.requiredNetwork:
            arguments.update(RequiredNetwork = data.requiredNetwork)
        server.conn.callMethod(wmiInstances[0], 'RemoteRegistration',
            **arguments)
        data.response = ""

        self.setData(data)
        self.sendStatus(200, "Host %s will try to rActivate itself" % data.p.host)

class ShutdownTask(WMITaskHandler):

    def _run(self, data):
        self.sendStatus(101, "Contacting host %s to shut itself down" % (
            data.p.host))

        #send WMI Shutdown request
        server = self.getWbemConnection(data)
        wmiInstances = server.Linux_OperatingSystem.EnumerateInstanceNames()
        value, args = server.conn.callMethod(wmiInstances[0], 'Shutdown')
        data.response = str(value)

        self.setData(data)
        if not value:
            self.sendStatus(200, "Host %s will now shutdown" % data.p.host)
        else:
            self.sendStatus(401, "Could not shutdown host %s" % data.p.host)

class PollingTask(WMITaskHandler):

    def _run(self, data):
        self.sendStatus(101, "Contacting host %s on port %d to Poll it for info" % (
            data.p.host, data.p.port))

        server = self.getWbemConnection(data)
        children = self._getUuids(server)
        children.extend(self._getServerCert())
        children.append(self._getSoftwareVersions(server))

        el = XML.Element("system", *children)

        self.setData(el.toxml(encoding="UTF-8"))
        self.sendStatus(200, "Host %s has been polled" % data.p.host)

class UpdateTask(WMITaskHandler):

    def _run(self, data):
        self.sendStatus(101, "Contacting host %s on port %d to update it" % (
            data.p.host, data.p.port))

        server = self.getWbemConnection(data)
        self._applySoftwareUpdate(data.p.host, data.sources)
        children = self._getUuids(server)
        children.extend(self._getServerCert())
        children.append(self._getSoftwareVersions(server))

        el = XML.Element("system", *children)

        self.setData(el.toxml(encoding="UTF-8"))
        self.sendStatus(200, "Host %s has been updated" % data.p.host)

    def _applySoftwareUpdate(self, host, sources):
        wmiUpdater = wmiupdater.WMIUpdater("https://" + host)
        wmiUpdater.applyUpdate(sources)
        return None
