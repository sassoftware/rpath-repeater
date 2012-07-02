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

from rpath_repeater.models import CimParams
from rpath_repeater.codes import Codes as C, NS
from rpath_repeater.utils import wbemlib
from rpath_repeater.utils import nodeinfo
from rpath_repeater.utils import cimupdater
from rpath_repeater.utils import surveyscanner

from rpath_repeater.utils import base_forwarding_plugin as bfp

XML = bfp.XML

# These are just the starting point attributes
CimData = types.slottype('CimData', 'p nodes response')

class CimForwardingPlugin(bfp.BaseForwardingPlugin):
    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(CimHandler)

    @classmethod
    def worker_get_task_types(cls):
        return {
            NS.CIM_TASK_REGISTER: RegisterTask,
            NS.CIM_TASK_SHUTDOWN: ShutdownTask,
            NS.CIM_TASK_POLLING: PollingTask,
            NS.CIM_TASK_UPDATE: UpdateTask,
            NS.CIM_TASK_CONFIGURATION: ConfigurationTask,
            NS.CIM_TASK_SURVEY_SCAN: SurveyScanTask,
        }


class CimHandler(bfp.BaseHandler):

    jobType = NS.CIM_JOB
    firstState = 'cimCall'

    RegistrationTaskNS = NS.CIM_TASK_REGISTER

    def setup (self):
        bfp.BaseHandler.setup(self)

    @classmethod
    def initParams(cls, data):
        return CimParams(**data.pop('cimParams', {}))

    def cimCall(self):
        self.setStatus(C.MSG_START, "Initiating CIM call")
        self.initCall()
        self.cimParams = self.initParams(self.data)
        self.eventUuid = self.cimParams.eventUuid

        if not self.zone:
            self.setStatus(C.ERR_ZONE_MISSING, "CIM call requires a zone")
            self.postFailure()
            return

        cp = self.cimParams
        if self.method in self.Meta.exposed:
            self.setStatus(C.MSG_CALL, "CIM call: %s %s:%s" %
                (self.method, cp.host, cp.port))
            return self.method

        self.setStatus(C.ERR_METHOD_NOT_ALLOWED,
            "Method does not exist: %s" % (self.method, ))
        self.postFailure()
        return

    @classmethod
    def _getArgs(cls, taskType, params, methodArguments, zoneAddresses):
        if taskType in [ NS.CIM_TASK_REGISTER, NS.CIM_TASK_SHUTDOWN, NS.CIM_TASK_POLLING, NS.CIM_TASK_SURVEY_SCAN ]:
            return CimData(params, zoneAddresses)
        if taskType in [ NS.CIM_TASK_UPDATE ]:
            sources = methodArguments['sources']
            arguments = dict(sources=sources, test=methodArguments.get('test', False))
            return bfp.GenericData(params, zoneAddresses, arguments)
        if taskType in [ NS.CIM_TASK_CONFIGURATION ]:
            configuration = methodArguments['configuration']
            return bfp.GenericData(params, zoneAddresses, configuration)
        raise Exception("Unhandled task type %s" % taskType)

    def _method(self, taskType):
         self.setStatus(C.MSG_NEW_TASK, "Creating task")
         args = self._getArgs(taskType, self.cimParams, self.methodArguments,
            self.zoneAddresses)
         task = self.newTask(taskType, taskType, args, zone=self.zone)
         return self._handleTask(task)

    @bfp.exposed
    def register(self):
        return self._method(NS.CIM_TASK_REGISTER)

    @bfp.exposed
    def shutdown(self):
        return self._method(NS.CIM_TASK_SHUTDOWN)

    @bfp.exposed
    def poll(self):
        return self._method(NS.CIM_TASK_POLLING)

    @bfp.exposed
    def update(self):
        return self._method(NS.CIM_TASK_UPDATE)

    @bfp.exposed
    def configuration(self):
        return self._method(NS.CIM_TASK_CONFIGURATION)

    @bfp.exposed
    def survey_scan(self):
        return self._method(NS.CIM_TASK_SURVEY_SCAN)

    def postprocessXmlNode(self, elt):
        # XXX we really should split the handlers and make this nicer
        if self.currentTask.task_type in [NS.CIM_TASK_SURVEY_SCAN, NS.CIM_TASK_UPDATE]:
            return self.postprocessXmlNodeAsJob(elt)
        return super(CimHandler, self).postprocessXmlNode(elt)

    def postprocessXmlNodeAsJob(self, elt):
        job = self.newJobElement()
        self.addJobResults(job, elt)
        return job

class CIMTaskHandler(bfp.BaseTaskHandler):
    InterfaceName = "CIM"
    WBEMServerFactory = wbemlib.WBEMServer

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
        self._serverCert = self._probeHost(data.p.host, data.p.port, x509Dict)
        server = self.WBEMServerFactory("https://" + data.p.host, x509=x509Dict)
        return server

    def _probeHost(self, host, port, x509Dict):
        return nodeinfo.probe_host_ssl(host, port, **x509Dict)

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
        return bfp.BaseTaskHandler._trove(troveSpec)

class RegisterTask(CIMTaskHandler):
    def _run(self, data):
        self.sendStatus(C.MSG_REGISTRATION_REQ,
            "Contacting host %s on port %d to rActivate itself"
                % (data.p.host, data.p.port))

        #send CIM rActivate request
        server = self.getWbemConnection(data)
        cimInstances = server.RPATH_ComputerSystem.EnumerateInstanceNames()
        arguments = dict(
            ManagementNodeAddresses = sorted(data.nodes))
        if data.p.eventUuid:
            arguments.update(EventUUID = data.p.eventUuid)
        if data.p.requiredNetwork:
            arguments.update(RequiredNetwork = data.p.requiredNetwork)
        ret = server.conn.callMethod(cimInstances[0], 'RemoteRegistration',
            **arguments)
        data.response = "<system/>"
        self.setData(data)

        retVal, outParams = ret
        if retVal == 0:
            self.sendStatus(C.OK, "Host %s registration initiated" %
                data.p.host)
        else:
            errorSummary = outParams.get('errorSummary', '')
            errorDetails = outParams.get('errorDetails', '')
            self.sendStatus(C.ERR_GENERIC,
                "Host %s registration failed: %s" %
                    (data.p.host, errorSummary), errorDetails)


class ShutdownTask(CIMTaskHandler):
    def _run(self, data):
        self.sendStatus(C.MSG_START, "Contacting host %s to shut itself down" % (
            data.p.host))

        #send CIM Shutdown request
        server = self.getWbemConnection(data)
        cimInstances = server.Linux_OperatingSystem.EnumerateInstanceNames()
        value, args = server.conn.callMethod(cimInstances[0], 'Shutdown')
        data.response = "<system/>"

        self.setData(data)
        if not value:
            self.sendStatus(C.OK, "Host %s will now shutdown" % data.p.host)
        else:
            self.sendStatus(C.ERR_GENERIC,
                "Could not shutdown host %s" % data.p.host)


class PollingTask(CIMTaskHandler):
    def _run(self, data):
        self.sendStatus(C.MSG_START, "Contacting host %s on port %d to Poll it for info"
            % (data.p.host, data.p.port))

        arguments = dict(
            ManagementNodeAddresses = sorted(data.nodes))
        if data.p.requiredNetwork:
            arguments.update(RequiredNetwork = data.p.requiredNetwork)

        server = self.getWbemConnection(data)
        cimInstances = server.RPATH_ComputerSystem.EnumerateInstanceNames()
        server.conn.callMethod(cimInstances[0], 'UpdateManagementConfiguration',
            **arguments)

        children = self._getUuids(server)
        children.extend(self._getServerCert())
        children.append(self._getSoftwareVersions(server))

        el = XML.Element("system", *children)

        data.response = XML.toString(el)
        self.setData(data)
        self.sendStatus(C.OK, "Host %s has been polled" % data.p.host)


class UpdateTask(CIMTaskHandler):
    def _run(self, data):
        self.sendStatus(C.MSG_START, "Contacting host %s on port %d to update it" % (
            data.p.host, data.p.port))

        server = self.getWbemConnection(data)
        self._applySoftwareUpdate(server, data.argument, sorted(data.nodes))
#        children = self._getUuids(server)
#        children.extend(self._getServerCert())
#        children.append(self._getSoftwareVersions(server))
#
#        el = XML.Element("system", *children)
#
#        data.response = XML.toString(el)
        data.response = "<ignored/>"
        self.setData(data)
        self.sendStatus(C.OK, "Host %s has been updated" % data.p.host)

    def _applySoftwareUpdate(self, server, arguments, nodes):
        cimUpdater = cimupdater.CIMUpdater(server)
        cimUpdater.applyUpdate(nodes=nodes, **arguments)
        return None

class ConfigurationTask(CIMTaskHandler):
    def _run(self, data):
        self.sendStatus(C.MSG_START,
            "Contacting host %s on port %d to trigger configuration change" % (
                data.p.host, data.p.port))

        server = self.getWbemConnection(data)
        results = self._applyConfigurationChange(server, data.argument)
        succeeded = results[0]
        children = self._getUuids(server)

        el = XML.Element("system", *children)

        data.response = XML.toString(el)
        self.setData(data)

        # concatenate result streams from CIM operations
        # which is not supported on older conary-cim-configuration versions
        logResults = ""
        if len(results) > 0:
            logs = results[1].get('operationlogs', None)
            if logs:
                logResults = logs[0] + logs[1]

        if succeeded == 0:
            self.sendStatus(C.OK, "Host %s configuration applied: %s" % (data.p.host, logResults))
        else:
            self.sendStatus(C.ERR_GENERIC,
                "Host %s configuration failed to apply: %s" % (data.p.host, logResults))

    def _applyConfigurationChange(self, server, configuration):
        import pywbem
        op = pywbem.CIMInstanceName('RPATH_Configuration',
            keybindings=dict(SettingID='/var/lib/iconfig/values.xml'))
        instance = server.RPATH_Configuration.GetInstance(op)
        instance.properties['Value'] = pywbem.CIMProperty('Value',
            configuration, type="string")
        server.RPATH_Configuration.ModifyInstance(instance)

        return server.conn.callMethod(instance.path, 'ApplyToMSE')

class SurveyScanTask(CIMTaskHandler):
    def _run(self, data):
        self.sendStatus(C.MSG_START,
            "Contacting host %s on port %d to trigger scan" % (
                data.p.host, data.p.port))

        server = self.getWbemConnection(data)

        scanner = surveyscanner.CIMSurveyScanner(server)
        job = scanner.scan()
        surveys = list(job.properties['JobResults'].value)
        succeeded = True

        children = [ XML.fromString(s) for s in surveys ]
        el = XML.Element("surveys", *children)

        data.response = XML.toString(el)
        self.setData(data)

        if succeeded:
            self.sendStatus(C.OK, "Host %s scanned" % data.p.host)
        else:
            self.sendStatus(C.ERR_GENERIC,
                "Host %s failed to scan" % data.p.host)
