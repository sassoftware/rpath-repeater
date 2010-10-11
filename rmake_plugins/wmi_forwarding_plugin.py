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

from rpath_repeater.utils import windowsUpdate
from rpath_repeater.utils import base_forwarding_plugin as bfp

XML = bfp.XML

WMI_JOB = bfp.PREFIX + '.wmiplugin'
WMI_TASK_REGISTER = WMI_JOB + '.register'
WMI_TASK_SHUTDOWN = WMI_JOB + '.shutdown'
WMI_TASK_POLLING = WMI_JOB + '.poll'
WMI_TASK_UPDATE = WMI_JOB + '.update'

WmiParams = types.slottype('WmiParams',
    'host port username password domain eventUuid')
# These are just the starting point attributes
WmiData = types.slottype('WmiData', 'p response')
UpdateData = types.slottype('UpdateData', 'p sources response')

class WmiForwardingPlugin(bfp.BaseForwardingPlugin):

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


class WmiHandler(bfp.BaseHandler):
    timeout = 7200

    jobType = WMI_JOB
    firstState = 'wmiCall'

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

    def wmiCall(self):
        self.setStatus(101, "Initiating WMI call")
        self.initCall()
        self.wmiParams = WmiParams(**self.data.pop('wmiParams', {}))
        self.eventUuid = self.wmiParams.eventUuid

        if not self.zone:
            self.setStatus(400, "WMI call requires a zone")
            self.postFailure()
            return

        cp = self.wmiParams
        if self.method in self.Meta.exposed:
            self.setStatus(102, "WMI call: %s %s" %
                           (self.method, cp.host))
            return self.method

        self.setStatus(405, "Method does not exist: %s" % (self.method, ))
        self.postFailure()
        return

    @bfp.exposed
    def register(self):
        self.setStatus(103, "Creating task")

        args = WmiData(self.wmiParams)
        task = self.newTask(WMI_TASK_REGISTER, WMI_TASK_REGISTER, args,
            zone=self.zone)
        return self._handleTask(task)

    @bfp.exposed
    def shutdown(self):
        self.setStatus(103, "Creating task")

        args = WmiData(self.wmiParams)
        task = self.newTask(WMI_TASK_SHUTDOWN, WMI_TASK_SHUTDOWN, args,
            zone=self.zone)
        return self._handleTask(task)

    @bfp.exposed
    def polling(self):
        self.setStatus(103, "Creating task")

        args = WmiData(self.wmiParams)
        task = self.newTask(WMI_TASK_POLLING, WMI_TASK_POLLING, args,
            zone=self.zone)
        return self._handleTask(task)

    @bfp.exposed
    def update(self):
        self.setStatus(103, "Creating task")

        sources = self.methodArguments['sources']

        args = UpdateData(self.wmiParams, sources)
        task = self.newTask(WMI_TASK_UPDATE, WMI_TASK_UPDATE, args,
            zone=self.zone)
        return self._handleTask(task)


class WMITaskHandler(bfp.BaseTaskHandler):
    InterfaceName = "WMI"

    def _getUuids(self, wmiClient):
        rc, localUUID = wmiClient.getRegistryKey(r'SOFTWARE\rPath\Inventory',
                                                 'local_uuid')
        rc, generatedUUID = wmiClient.getRegistryKey(r'SOFTWARE\rPath\Inventory',
                                                     'generated_uuid')
        if not rc:
            return []

        T = XML.Text
        return [T("local_uuid", localUUID),
                T("generated_uuid", generatedUUID)]

    def _getSoftwareVersions(self, wmiClient):
        rc, siList = wmiClient.getRegistryKey(r"SOFTWARE\rPath\conary",
                                              "conary_manifest")
        siList = [ x.strip() for x in siList.split('\n') ]
        # Start creating the XML document
        troves = [ self._trove(tspec) for tspec in siList if tspec ]
        return XML.Element("installed_software", *troves)

    def _getLocalUUID(self, wc, generated_uuid):
        def getKey(keyPath, key):
            rc, results = wc.getRegistryKey(keyPath, key)
            if rc:
                self.sendStatus(400, r'Error accessing key %s\%s: %s'
                    % (keyPath, key, results))
            return rc, results

        # Get some data from the target machine so that we can generate a
        # local uuid
        # FIXME: Should use SMBIOS interface once available to get real
        #        information.
        keyPath = r'HARDWARE\DESCRIPTION\System\BIOS'
        rc, baseBoard = getKey(keyPath, 'BaseBoardManufacturer')
        if rc: return

        rc, biosMajorRelease = getKey(keyPath, 'BiosMajorRelease')
        if rc: return

        rc, biosMinorRelease = getKey(keyPath, 'BiosMinorRelease')
        if rc: return

        sha1 = digestlib.sha1()
        sha1.update(baseBoard)
        sha1.update(biosMajorRelease)
        sha1.update(biosMinorRelease)
        sha1.update(generated_uuid)
        bytes = sha1.digest()[:16]
        local_uuid = str(uuid.UUID(bytes=bytes))
        return local_uuid

    def _setUUIDs(self, wc, generated_uuid, local_uuid):
        def setKey(keyPath, key, value):
            rc, results = wc.setRegistryKey(keyPath, key, value)
            if rc:
                self.sendStatus(400, r'Failed to set key %s\%s: %s' %
                    (keyPath, key, results))
            return rc, results

        keyPath = r'SOFTWARE\rPath\Inventory'
        rc, results = setKey(keyPath, 'generated_uuid', generated_uuid)
        if rc: return

        rc, results = setKey(keyPath, 'local_uuid', local_uuid)
        if rc: return

        self.sendStatus(106, 'Stored UUIDs on Windows system')


class RegisterTask(WMITaskHandler):
    def _run(self, data):
        # fetch a registry key that has admin only access
        wc = windowsUpdate.wmiClient(data.p.host, data.p.domain,
                                     data.p.username, data.p.password)

        self.sendStatus(104, "Contacting host %s validate credentials" % (
            data.p.host, ))

        # FIXME: Validate creds by accessng a key that only admin should be able
        #        to get to.
        #rc, _ = wc.getRegistryKey(SOME_PATH,SOME_KEY)

        self.sendStatus(105, 'Generating UUIDs')

        # Generate a UUID for the system.
        generated_uuid = str(uuid.uuid4())

        # Generate local UUID based on system data
        local_uuid = self._getLocalUUID(wc, generated_uuid)

        self._setUUIDs(wc, generated_uuid, local_uuid)

        uuids = [ XML.Text('local_uuid', local_uuid),
                  XML.Text('generated_uuid', generated_uuid), ]

        el = XML.Element('system', *uuids)
        data.response = XML.toString(el)
        self.setData(data)

        self.sendStatus(200, "Registration Complete for %s" % data.p.host)


class ShutdownTask(WMITaskHandler):
    def _run(self, data):
        self.sendStatus(401, "Shutting down Windows System %s is not supported"
                        % (data.p.host))


class PollingTask(WMITaskHandler):
    def _run(self, data):
        self.sendStatus(101, "Contacting host %s on port %d to Poll it for info"
            % (data.p.host, data.p.port))

        try:
            wc = windowsUpdate.wmiClient( data.p.host, data.p.domain,
                                          data.p.username, data.p.password)
            children = self._getUuids(wc)
            children.append(self._getSoftwareVersions(wc))
        finally:
            wc.unmount()

        el = XML.Element("system", *children)

        data.response = XML.toString(el)
        self.setData(data)
        self.sendStatus(200, "Host %s has been polled" % data.p.host)

class UpdateTask(WMITaskHandler):
    def _run(self, data):
        self.sendStatus(101, "Contacting host %s on port %d to update it" % (
            data.p.host, data.p.port))

        try:
            wc = windowsUpdate.wmiClient( data.p.host, data.p.domain,
                                          data.p.username, data.p.password)
            windowsUpdate.doUpdate(wc, data.sources)
            children = self._getUuids(wc)
            children.append(self._getSoftwareVersions(wc))
        finally:
            wc.unmount()

        el = XML.Element("system", *children)

        data.response = XML.toString(el)
        self.setData(data)
        self.sendStatus(200, "Host %s has been updated" % data.p.host)
