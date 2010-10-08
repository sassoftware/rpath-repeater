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

from rmake3.core import types
from rmake3.core import handler

from conary.lib.formattrace import formatTrace

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
        self.setStatus(101, 'Initializing Interface Detection')
        self.initCall()

        if not self.zone:
            self.setStatus(400, 'Interface detection call requires a zone')
            self.postFailure()
            return

        if not self.interfacesList:
            self.setStatus(401, 'Interface detection requires a list of interfaces')
            self.postFailure()
            return

        return 'detect_management_interface'

    def detect_management_interface(self):
        self.setStatus(103, 'Creating task')

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

            self.sendStatus(450, "Error in Interface Detection call: %s"
                % str(value), out.getvalue())

    def _run(self):
        """
        Probe the machine to determine which management interface is available.
        """

        self.sendStatus(104, 'Detecting Management Interface')

        data = self.getData()
        host = data.p.host
        for params in data.p.interfacesList:
            port = params['port']
            interfaceHref = params['interfaceHref']
            self.sendStatus(105, 'Checking %s:%s' % (host, port))
            if self._queryService(host, port):
                self._sendResponse(data, interfaceHref, port)
                self.sendStatus(200, 'Found management interface on %s:%s'
                    % (host, port))
                return

        self._sendResponse(data)
        self.sendStatus(201, 'No management interface discovered')

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
            self.sendStatus(106, 'Error probing %s:%s %s'
                % (host, port, str(e)))
            return False
