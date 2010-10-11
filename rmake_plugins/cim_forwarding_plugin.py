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

from rmake3.core import types
from rmake3.core import handler

from rpath_repeater.utils import wbemlib
from rpath_repeater.utils import nodeinfo
from rpath_repeater.utils import cimupdater

from rpath_repeater.utils import base_forwarding_plugin as bfp

XML = bfp.XML

CIM_JOB = bfp.PREFIX + '.cimplugin'
CIM_TASK_REGISTER = CIM_JOB + '.register'
CIM_TASK_SHUTDOWN = CIM_JOB + '.shutdown'
CIM_TASK_POLLING = CIM_JOB + '.poll'
CIM_TASK_UPDATE = CIM_JOB + '.update'

CimParams = types.slottype('CimParams',
    'host port clientCert clientKey eventUuid instanceId targetName targetType')
# These are just the starting point attributes
CimData = types.slottype('CimData', 'p response')
RactivateData = types.slottype('RactivateData',
        'p nodes requiredNetwork response')
UpdateData = types.slottype('UpdateData', 'p sources response')

class CimForwardingPlugin(bfp.BaseForwardingPlugin):
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


class CimHandler(bfp.BaseHandler):
    timeout = 7200
    port = 5989

    jobType = CIM_JOB
    firstState = 'cimCall'

    def setup (self):
        bfp.BaseHandler.setup(self)

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
        self.eventUuid = self.cimParams.eventUuid

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

    @bfp.exposed
    def register(self):
        self.setStatus(103, "Creating task")

        nodes = [x + ':8443' for x in self._getZoneAddresses()]
        args = RactivateData(self.cimParams, nodes,
                self.methodArguments.get('requiredNetwork'))
        task = self.newTask(CIM_TASK_REGISTER, CIM_TASK_REGISTER, args,
            zone=self.zone)
        return self._handleTask(task)

    @bfp.exposed
    def shutdown(self):
        self.setStatus(103, "Creating task")

        args = CimData(self.cimParams)
        task = self.newTask(CIM_TASK_SHUTDOWN, CIM_TASK_SHUTDOWN, args,
            zone=self.zone)
        return self._handleTask(task)

    @bfp.exposed
    def polling(self):
        self.setStatus(103, "Creating task")

        args = CimData(self.cimParams)
        task = self.newTask(CIM_TASK_POLLING, CIM_TASK_POLLING, args,
            zone=self.zone)
        return self._handleTask(task)

    @bfp.exposed
    def update(self):
        self.setStatus(103, "Creating task")

        sources = self.methodArguments['sources']

        args = UpdateData(self.cimParams, sources)
        task = self.newTask(CIM_TASK_UPDATE, CIM_TASK_UPDATE, args,
            zone=self.zone)
        return self._handleTask(task)


class CIMTaskHandler(bfp.BaseTaskHandler):
    InterfaceName = "CIM"

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

    def _trove(self, si):
        troveSpec = "%s=%s" % (si['name'], si['VersionString'])
        return bfp.BaseTaskHandler._trove(self, troveSpec)

class RegisterTask(CIMTaskHandler):
    def _run(self, data):
        self.sendStatus(104, "Contacting host %s on port %d to rActivate itself"
            % (data.p.host, data.p.port))

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
        self.sendStatus(101, "Contacting host %s on port %d to Poll it for info"
            % (data.p.host, data.p.port))

        server = self.getWbemConnection(data)
        children = self._getUuids(server)
        children.extend(self._getServerCert())
        children.append(self._getSoftwareVersions(server))

        el = XML.Element("system", *children)

        data.response = XML.toString(el)
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

        data.response = XML.toString(el)
        self.setData(data)
        self.sendStatus(200, "Host %s has been updated" % data.p.host)

    def _applySoftwareUpdate(self, server, sources):
        cimUpdater = cimupdater.CIMUpdater(server)
        cimUpdater.applyUpdate(sources)
        return None
