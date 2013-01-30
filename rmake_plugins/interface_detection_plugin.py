#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import sys
import StringIO

from rmake3.core import types
from rmake3.core import handler

from conary.lib.formattrace import formatTrace

from rpath_repeater.codes import Codes as C
from rpath_repeater.utils import nodeinfo
from rpath_repeater.utils import base_forwarding_plugin as bfp

PREFIX = 'com.rpath.sputnik'
INTERFACE_JOB = PREFIX + '.interfacedetectionplugin'
INTERFACE_DETECT_TASK = PREFIX + '.detect_management_interface'

IDParams = types.slottype(
    'IDParams', 'host interfacesList')

IDData = types.slottype('IDData', 'p response')

class InterfaceDetectionForwardPlugin(bfp.BaseForwardingPlugin):
    """
    Setup dispatcher side of the interface detection.
    """

    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(InterfaceDetectionHandler)

    def worker_get_task_types(self):
        return {
            INTERFACE_DETECT_TASK: DetectInterfaceTask,
        }


class InterfaceDetectionHandler(bfp.BaseHandler):
    """
    Dispatcher plugin.
    """

    jobType = INTERFACE_JOB
    firstState = 'callDetectInterface'

    def setup(self):
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

    def initCall(self):
        bfp.BaseHandler.initCall(self)
        self.params = self.data.pop('params', None)
        self.interfacesList = self.params.pop('interfacesList', None)
        self.eventUuid = self.params.pop('eventUuid', None)

    def callDetectInterface(self):
        self.setStatus(C.MSG_START, 'Initializing Interface Detection')
        self.initCall()

        if not self.zone:
            self.setStatus(C.ERR_ZONE_MISSING, 'Interface detection call requires a zone')
            self.postFailure()
            return

        if not self.interfacesList:
            self.setStatus(C.ERR_BAD_ARGS,
                'Interface detection requires a list of interfaces')
            self.postFailure()
            return

        return 'detect_management_interface'

    def detect_management_interface(self):
        self.setStatus(C.MSG_NEW_TASK, 'Creating task')

        args = IDData(IDParams(self.params['host'], self.interfacesList))
        task = self.newTask('detect_management_interface',
            INTERFACE_DETECT_TASK, args, zone=self.zone)
        return self._handleTask(task)


class DetectInterfaceTask(bfp.BaseTaskHandler):
    """
    Task that runs on the rUS to query the target systems.
    """

    def run(self):
        try:
            self._run()
        except:
            typ, value, tb = sys.exc_info()
            out = StringIO.StringIO()
            formatTrace(typ, value, tb, stream = out, withLocals = False)
            out.write("\nFull stack:\n")
            formatTrace(typ, value, tb, stream = out, withLocals = True)

            self.sendStatus(C.ERR_GENERIC,
                "Error in Interface Detection call: %s"
                    % str(value), out.getvalue())

    def _run(self):
        """
        Probe the machine to determine which management interface is available.
        """

        self.sendStatus(C.MSG_PROBE, 'Detecting Management Interface')

        data = self.getData()
        host = data.p.host
        for params in data.p.interfacesList:
            port = params['port']
            interfaceHref = params['interfaceHref']
            self.sendStatus(C.MSG_PROBE, 'Checking %s:%s' % (host, port))
            if self._queryService(host, port):
                self._sendResponse(data, interfaceHref, port)
                self.sendStatus(C.OK, 'Found management interface on %s:%s'
                    % (host, port))
                return

        self._sendResponse(data)
        self.sendStatus(C.OK_1, 'No management interface discovered')

    def _sendResponse(self, data, interfaceHref=None, port=None):
        if interfaceHref:
            children = [ bfp.XML.Element('management_interface',
                href=interfaceHref) ]
            children.append(bfp.XML.Text('agent_port', str(port)))
        else:
            children = []
        el = bfp.XML.Element("system", *children)
        data.response = bfp.XML.toString(el)
        self.setData(data)

    def _queryService(self, host, port):
        try:
            nodeinfo.probe_host(host, port)
            return True
        except nodeinfo.ProbeHostError, e:
            self.sendStatus(C.MSG_GENERIC, 'Error probing %s:%s %s'
                % (host, port, str(e)))
            return False
