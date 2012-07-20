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

from lxml import etree
from lxml.builder import ElementMaker

from rmake3.core import types
from rmake3.core import handler

from rpath_repeater.utils import windows
from rpath_repeater.models import WmiParams
from rpath_repeater.codes import Codes as C, NS
from rpath_repeater.utils import base_forwarding_plugin as bfp

# These are just the starting point attributes
WmiData = types.slottype('WmiData', 'p response')

class WmiForwardingPlugin(bfp.BaseForwardingPlugin):

    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(WmiHandler)

    @classmethod
    def worker_get_task_types(cls):
        return {
            NS.WMI_TASK_REGISTER: RegisterTask,
            NS.WMI_TASK_SHUTDOWN: ShutdownTask,
            NS.WMI_TASK_POLLING: PollingTask,
            NS.WMI_TASK_UPDATE: UpdateTask,
            NS.WMI_TASK_CONFIGURATION: ConfigurationTask,
            NS.WMI_TASK_SURVEY_SCAN: SurveyScanTask,
        }


class WmiHandler(bfp.BaseHandler):
    timeout = 7200

    jobType = NS.WMI_JOB
    firstState = 'wmiCall'

    RegistrationTaskNS = NS.WMI_TASK_REGISTER

    def setup (self):
        bfp.BaseHandler.setup(self)

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
        if taskType in [ NS.WMI_TASK_REGISTER, NS.WMI_TASK_SHUTDOWN,
                NS.WMI_TASK_POLLING ]:
            return WmiData(params)
        if taskType in [ NS.WMI_TASK_SURVEY_SCAN]:
            arguments = dict(desiredTopLevelItems=methodArguments.get(
                'desiredTopLevelItems', None))
            return bfp.GenericData(params, zoneAddresses, arguments)
        if taskType in [ NS.WMI_TASK_UPDATE ]:
            args = dict(
                sources=methodArguments.get('sources'),
                test=methodArguments.get('test', False),
            )
            return bfp.GenericData(params, zoneAddresses, args)
        if taskType in [ NS.WMI_TASK_CONFIGURATION ]:
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
        return self._method(NS.WMI_TASK_REGISTER)

    @bfp.exposed
    def shutdown(self):
        return self._method(NS.WMI_TASK_SHUTDOWN)

    @bfp.exposed
    def poll(self):
        return self._method(NS.WMI_TASK_POLLING)

    @bfp.exposed
    def update(self):
        return self._method(NS.WMI_TASK_UPDATE)

    @bfp.exposed
    def configuration(self):
        return self._method(NS.WMI_TASK_CONFIGURATION)

    @bfp.exposed
    def survey_scan(self):
        return self._method(NS.WMI_TASK_SURVEY_SCAN)

    def postprocessXmlNode(self, elt):
        return self.postprocessXmlNodeAsJob(elt)

    def postprocessXmlNodeAsJob(self, elt):
        job = self.newJobElement()
        self.addJobResults(job, elt)
        return job

class WMITaskHandler(bfp.BaseTaskHandler):
    InterfaceName = "WMI"

    def getSystem(self, data):
        authInfo = windows.WindowsAuthInfo(data.p.host, data.p.domain,
            data.p.username, data.p.password)
        system = windows.WindowsSystem(authInfo, self.sendStatus)
        return system

    def _trove(self, trvSpec):
        xml = bfp.BaseTaskHandler._trove(trvSpec).toxml()
        doc = etree.fromstring(xml)
        return doc

    def _poll(self, system):
        uuids, hostname, softwareVersions, netInfo = system.poll()

        e = ElementMaker()

        networks = e.networks()
        for intf in netInfo:
            network = e.network(
                e.device_name(intf.name),
            )

            if intf.isv6:
                network.append(e.ipv6_address(intf.ip_address))
            else:
                network.append(e.ip_address(intf.ip_address))

            network.extend([
                e.netmask(str(intf.cidr)),
                e.dns_name(intf.dns_name),
                e.required(str(intf.required).lower()),
            ])
            networks.append(network)

        return e.system(
            e.local_uuid(uuids[0]),
            e.generated_uuid(uuids[1]),
            e.hostname(hostname),
            e.installed_software(*[ self._trove(x) for x in softwareVersions ]),
            networks,
        )


class RegisterTask(WMITaskHandler):
    def _run(self, data):
        system = self.getSystem(data)
        system.callback.start()

        localUUID, generatedUUID, computerName = system.register()

        e = ElementMaker()

        data.response = etree.tostring(e.system(
            e.local_uuid(localUUID),
            e.generated_uuid(generatedUUID),
            e.hostname(computerName),
        ))

        self.setData(data)
        system.callback.done()


class ShutdownTask(WMITaskHandler):
    def _run(self, data):
        self.sendStatus(C.ERR_METHOD_NOT_ALLOWED,
            "Shutting down Windows System %s is not supported" % (data.p.host))


class PollingTask(WMITaskHandler):
    def _run(self, data):
        system = self.getSystem(data)
        system.callback.start()

        data.response = etree.tostring(self._poll(system))
        self.setData(data)

        system.callback.done()

class UpdateTask(WMITaskHandler):
    def _run(self, data):
        system = self.getSystem(data)
        system.callback.start()

        results, preview = system.update(data.argument.get('sources'),
            str(self.task.job_uuid), test=data.argument.get('test'))

        data.response = preview
        self.setData(data)

        if not results:
            system.callback.info('no updates to apply')
            system.callback.done()
            return

        for op, nvf, status in results:
            code = C.MSG_GENERIC
            if not status:
                code = C.ERR_GENERIC
                msg = ('Failed to find update status code. This normally means '
                    'that the rPathTools Installer Service failed to run for '
                    'some reason. Please check the remote machine for details. '
                    'Logs can be found in C:\ProgramFiles (x86)\rPath\rTIS.NET '
                    'and the event viewer.')
            else:
                msg = status.get('status')
                if status.get('exitCode') not in (None, '0'):
                    msg += ' with exit code %s' % status.get('exitCode')
                    msg += ' (' + status.get('exitCodeDescription') + ')'
                    code = C.ERR_GENERIC
            self.sendStatus(code, '%s of %s %s' % (op, nvf, msg))

        system.callback.done()

class ConfigurationTask(WMITaskHandler):
    def _run(self, data):
        system = self.getSystem(data)
        system.callback.start()

        values = etree.fromstring(data.argument).getchildren()
        results = system.configure(str(self.task.job_uuid), values)

        data.response = etree.tostring(self._poll(system))
        self.setData(data)

        errors = [ x for x in results if x[0] != 0 ]

        if errors:
            self.sendStatus(C.ERR_GENERIC, '%s Applying configuration failed:'
                % data.p.host)
            for rc, handler, msg in errors:
                self.sendStatus(C.ERR_GENERIC, '%s: Handler %s exited with a '
                    'return code of % and the following message %s'
                    % (data.p.host, handler, rc, msg))
        else:
            self.sendStatus(C.OK, 'Host %s has been configured successfully'
                % data.p.host)

class SurveyScanTask(WMITaskHandler):
    def _run(self, data):
        system = self.getSystem(data)
        system.callback.start()

        status, statusDetail, survey = system.scan(str(self.task.job_uuid),
            troveSpecs=data.argument.get('desiredTopLevelItems'))

        if status == 'completed':
            data.response = survey
            self.setData(data)
            self.sendStatus(C.OK, statusDetail)
        else:
            self.sendStatus(C.ERR_GENERIC, 'Failed to scan remote windows '
                'system with the following error: %s' % statusDetail)

        system.callback.info(statusDetail)
        system.callback.done()

