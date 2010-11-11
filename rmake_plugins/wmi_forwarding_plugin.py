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

from conary.lib import digestlib

from rmake3.lib import uuid
from rmake3.core import types
from rmake3.core import handler

from rpath_repeater.codes import Codes as C
from rpath_repeater.utils import windowsUpdate
from rpath_repeater.utils import base_forwarding_plugin as bfp

XML = bfp.XML

WMI_JOB = bfp.PREFIX + '.wmiplugin'
WMI_TASK_REGISTER = WMI_JOB + '.register'
WMI_TASK_SHUTDOWN = WMI_JOB + '.shutdown'
WMI_TASK_POLLING = WMI_JOB + '.poll'
WMI_TASK_UPDATE = WMI_JOB + '.update'
WMI_TASK_CONFIGURATION = WMI_JOB + '.configuration'

WmiParams = types.slottype('WmiParams',
    'host port username password domain eventUuid')
# These are just the starting point attributes
WmiData = types.slottype('WmiData', 'p response')

class WmiForwardingPlugin(bfp.BaseForwardingPlugin):

    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(WmiHandler)

    @classmethod
    def worker_get_task_types(cls):
        return {
            WMI_TASK_REGISTER: RegisterTask,
            WMI_TASK_SHUTDOWN: ShutdownTask,
            WMI_TASK_POLLING: PollingTask,
            WMI_TASK_UPDATE: UpdateTask,
            WMI_TASK_SHUTDOWN: ShutdownTask,
            WMI_TASK_CONFIGURATION: ConfigurationTask,
        }


class WmiHandler(bfp.BaseHandler):
    timeout = 7200

    jobType = WMI_JOB
    firstState = 'wmiCall'

    RegistrationTaskNS = WMI_TASK_REGISTER

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

    @classmethod
    def initParams(cls, data):
        return WmiParams(**data.pop('wmiParams', {}))

    def wmiCall(self):
        self.setStatus(C.MSG_START, "Initiating WMI call")
        self.initCall()
        self.wmiParams = self.initParams(self.data)
        self.eventUuid = self.wmiParams.eventUuid

        if not self.zone:
            self.setStatus(C.ERR_ZONE_MISSING, "WMI call requires a zone")
            self.postFailure()
            return

        cp = self.wmiParams
        if self.method in self.Meta.exposed:
            self.setStatus(C.MSG_CALL, "WMI call: %s %s" %
                           (self.method, cp.host))
            return self.method

        self.setStatus(C.ERR_METHOD_NOT_ALLOWED,
            "Method does not exist: %s" % (self.method, ))
        self.postFailure()
        return

    @classmethod
    def _getArgs(cls, taskType, params, methodArguments, zoneAddresses):
        if taskType in [ WMI_TASK_REGISTER, WMI_TASK_SHUTDOWN,
                WMI_TASK_POLLING ]:
            return WmiData(params)
        if taskType in [ WMI_TASK_UPDATE ]:
            sources = methodArguments['sources']
            return bfp.GenericData(params, zoneAddresses, sources)
        if taskType in [ WMI_TASK_CONFIGURATION ]:
            configuration = methodArguments['configuration']
            return bfp.GenericData(params, zoneAddresses, configuration)
        raise Exception("Unhandled task type %s" % taskType)

    def _method(self, taskType):
        self.setStatus(C.MSG_NEW_TASK, "Creating task")
        args = self._getArgs(taskType, self.wmiParams, self.methodArguments,
            self.zoneAddresses)
        task = self.newTask(taskType, taskType, args, zone=self.zone)
        return self._handleTask(task)

    @bfp.exposed
    def register(self):
        return self._method(WMI_TASK_REGISTER)

    @bfp.exposed
    def shutdown(self):
        return self._method(WMI_TASK_SHUTDOWN)

    @bfp.exposed
    def poll(self):
        return self._method(WMI_TASK_POLLING)

    @bfp.exposed
    def update(self):
        return self._method(WMI_TASK_UPDATE)

    @bfp.exposed
    def configuration(self):
        return self._method(WMI_TASK_CONFIGURATION)


class WMITaskHandler(bfp.BaseTaskHandler):
    InterfaceName = "WMI"

    WmiClientFactory = windowsUpdate.wmiClient

    @classmethod
    def _getWmiClient(cls, data):
        wc = cls.WmiClientFactory(data.p.host, data.p.domain,
                                  data.p.username, data.p.password)
        cls._validateCredentials(wc)
        return wc

    def _getWmiSystemData(self, wc):
        children = self._getUuids(wc)
        children.extend(self._getComputerName(wc))
        children.append(self._getSoftwareVersions(wc))
        children.append(self._getNetworkInfo(wc))
        return children

    @classmethod
    def _validateCredentials(cls, wc):
        # Validate credentials
        rc, _ = wc.getRegistryKey(r'HARDWARE\DESCRIPTION\System\BIOS', 'BaseBoardManufacturer')
        if rc:
            raise bfp.AuthenticationError()

    @classmethod
    def _getComputerName(cls, wmiClient):
        rc, computername = wmiClient.getRegistryKey(
            r'SYSTEM\CurrentControlSet\Control\ComputerName\ActiveComputerName', 'ComputerName')
        if rc:
            return []

        return [ XML.Text("hostname", computername.strip()) ]

    @classmethod
    def _getUuids(cls, wmiClient):
        rc, localUUID = wmiClient.getRegistryKey(r'SOFTWARE\rPath\inventory',
                                                 'local_uuid')
        rc, generatedUUID = wmiClient.getRegistryKey(
            r'SOFTWARE\rPath\inventory', 'generated_uuid')
        if rc:
            return []

        T = XML.Text
        return [T("local_uuid", localUUID.strip()),
                T("generated_uuid", generatedUUID.strip())]

    @classmethod
    def _getSoftwareVersions(cls, wmiClient):
        rc, siList = wmiClient.getRegistryKey(r"SOFTWARE\rPath\conary",
                                              "polling_manifest")
        siList = [ x.strip() for x in siList.split('\n') ]
        # Start creating the XML document
        troves = [ cls._trove(tspec) for tspec in siList if tspec ]
        return XML.Element("installed_software", *troves)

    @classmethod
    def _getNetworkInfo(cls, wmiClient):
        rc, netInfo = wmiClient.queryNetwork()
        nets = [x.strip().split(',') for x in netInfo.split('\n') if x]

        nodes = []
        for n in nets:
            n = [x.strip() for x in n]
            device_name, ipaddr, netmask, hostname, domain = n
            hostname = hostname.lower()
            ip_address = ipv6_address = None
            if ":" in ipaddr:
                ipv6_address = ipaddr
            else:
                ip_address = ipaddr
                ints = [int(x) for x in netmask.split('.') if int(x)]
                netmask = 0
                for i in ints:
                    while i:
                        netmask = netmask + (i & 1)
                        i = i >> 1
            dns_name = "%s.%s" % (hostname, domain)
            required = str((ip_address==wmiClient.target) or \
                (dns_name==wmiClient.target)).lower()

            T = XML.Text
            if ipv6_address:
                nodes.append(XML.Element("network",
                                         T("device_name", device_name),
                                         T("ipv6_address", ipv6_address),
                                         T("netmask", netmask),
                                         T("dns_name", dns_name),
                                         T("required", required)))
            else:
                nodes.append(XML.Element("network",
                                         T("device_name", device_name),
                                         T("ip_address", ip_address),
                                         T("netmask", netmask),
                                         T("dns_name", dns_name),
                                         T("required", required)))

            return XML.Element("networks",*nodes)

    @classmethod
    def _getRegistryKey(cls, wc, keyPath, key):
        rc, results = wc.getRegistryKey(keyPath, key)
        if rc:
            raise bfp.GenericError(r'Error accessing key %s\%s: %s' %
                (keyPath, key, results))
        return results

    @classmethod
    def _setRegistryKey(cls, wc, keyPath, key, value):
        rc, results = wc.setRegistryKey(keyPath, key, value)
        if rc:
            raise bfp.GenericError(r'Failed to set key %s\%s: %s' %
                    (keyPath, key, results))
        return results

    def _setUUIDs(self, wc, generated_uuid, local_uuid):
        keyPath = r'SOFTWARE\rPath\inventory'
        self._setRegistryKey(wc, keyPath, 'generated_uuid', generated_uuid)
        self._setRegistryKey(wc, keyPath, 'local_uuid', local_uuid)
        self.sendStatus(C.MSG_GENERIC, 'Stored UUIDs on Windows system')

class RegisterTask(WMITaskHandler):
    def _run(self, data):
        # fetch a registry key that has admin only access
        wc = self._getWmiClient(data)

        self.sendStatus(C.MSG_CREDENTIALS_VALIDATION,
            "Contacting host %s to validate credentials" % (data.p.host, ))

        # Check to see if rTIS is installed
        rc, _ = wc.queryService('rPath Tools Install Service')
        if rc:
            self.sendStatus(C.MSG_GENERIC, 'Installing rPath Tools')
            if not windowsUpdate.doBootstrap(wc):
                raise bfp.AuthenticationError(
                    'Credentials provided do not have permission to '
                            'install rPath Tools')
            wc.unmount()

        # Generate a UUID for the system.
        self.sendStatus(C.MSG_GENERIC, 'Generating UUIDs')

        generated_uuid = self._createGeneratedUuid()
        rc, local_uuid = wc.queryUUID()

        self._setUUIDs(wc, generated_uuid, local_uuid)

        children = [ XML.Text('local_uuid', local_uuid),
                  XML.Text('generated_uuid', generated_uuid) ]
        children.extend(self._getComputerName(wc))

        el = XML.Element('system', *children)
        data.response = XML.toString(el)
        self.setData(data)

        self.sendStatus(C.OK, "Registration Complete for %s" % data.p.host)

    def _createGeneratedUuid(self):
        return str(uuid.uuid4())

class ShutdownTask(WMITaskHandler):
    def _run(self, data):
        self.sendStatus(C.ERR_METHOD_NOT_ALLOWED,
            "Shutting down Windows System %s is not supported" % (data.p.host))


class PollingTask(WMITaskHandler):
    def _run(self, data):
        self.sendStatus(C.MSG_START, "Contacting host %s on port %d to Poll it for info"
            % (data.p.host, data.p.port))

        wc = self._getWmiClient(data)
        children = self._getWmiSystemData(wc)

        el = XML.Element("system", *children)

        data.response = XML.toString(el)
        self.setData(data)
        self.sendStatus(C.OK, "Host %s has been polled" % data.p.host)

class UpdateTask(WMITaskHandler):
    def _run(self, data):
        self.sendStatus(C.MSG_START, "Contacting host %s on port %d to update it" % (
            data.p.host, data.p.port))
        wc = self._getWmiClient(data)
        try:
            windowsUpdate.doUpdate(wc, data.argument,
                str(self.task.job_uuid), self.sendStatus)
        finally:
            wc.unmount()

        self.sendStatus(C.MSG_GENERIC, 'Update Complete. Gathering results.')
        children = self._getWmiSystemData(wc)
        el = XML.Element("system", *children)

        data.response = XML.toString(el)
        self.setData(data)
        self.sendStatus(C.OK, "Host %s has been updated" % data.p.host)

class ConfigurationTask(WMITaskHandler):
    def _run(self, data):
        self.sendStatus(C.ERR_METHOD_NOT_ALLOWED,
            "Configuration changes of Windows System %s is not supported" % (data.p.host))
