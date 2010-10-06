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

from conary.lib.formattrace import formatTrace

from rmake3.core import handler
from rmake3.core import types

from rpath_repeater import windowsUpdate
from rpath_repeater.utils import nodeinfo, wmiupdater
from rpath_repeater.utils.base_forwarding_plugin import PREFIX, BaseHandler, \
    BaseTaskHandler, BaseForwardingPlugin, XML, exposed

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

    def wmiCall(self):
        self.setStatus(101, "Initiating WMI call")
        self.initCall()
        self.wmiParams = WmiParams(**self.data.pop('wmiParams', {}))

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

        # FIXME
        nodes = [x + ':8443' for x in self._getZoneAddresses()]
        task = self.newTask('register', WMI_TASK_REGISTER, args, zone=self.zone)
        return self._handleTask(task)

    @exposed
    def shutdown(self):
        self.setStatus(103, "Creating task")

        args = WmiData(self.wmiParams)
        task = self.newTask('shutdown', WMI_TASK_SHUTDOWN, args, zone=self.zone)
        return self._handleTask(task)

    @exposed
    def polling(self):
        self.setStatus(103, "Creating task")

        args = WmiData(self.wmiParams)
        task = self.newTask('Polling', WMI_TASK_POLLING, args, zone=self.zone)
        return self._handleTask(task)

    @exposed
    def update(self):
        self.setStatus(103, "Creating task")

        sources = self.methodArguments['sources']

        args = UpdateData(self.wmiParams, sources)
        task = self.newTask('Update', WMI_TASK_UPDATE,args, zone=self.zone)
        return self._handleTask(task)


WmiParams = types.slottype('WmiParams',
    'host port user password domain eventUuid')
# These are just the starting point attributes
WmiData = types.slottype('WmiData', 'p response')
UpdateData = types.slottype('UpdateData', 'p sources response')

class WMITaskHandler(BaseTaskHandler):

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
        self.sendStatus(104, "Contacting host %s validate credentials" % (
            data.p.host, ))

        # fetch a registry key that has admin only access
        wc = windowsUpdate. wmiClient( data.p.host, data.p.domain,
                                       data.p.user, data.p.password)
        rc, _ = wc.getRegistryKey(SOME_PATH,SOME_KEY)

        if not rc:
            self.sendStatus(200, "Registration Complete for %s" % data.p.host)

class ShutdownTask(WMITaskHandler):

    def _run(self, data):
        self.sendStatus(401, "Shutting down Windows System %s is not supported"
                        % (data.p.host))

class PollingTask(WMITaskHandler):

    def _run(self, data):
        self.sendStatus(101, "Contacting host %s to Poll it for info" % (
            data.p.host))

        server = self.getWbemConnection(data)
        children = self._getUuids(server)
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
