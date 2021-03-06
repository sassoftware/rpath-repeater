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


import logging

log = logging.getLogger(__name__)

import StringIO
import socket
import sys
import tempfile
from twisted.internet.defer import maybeDeferred

from conary.lib.formattrace import formatTrace

from rmake3.core import types
from rmake3.core import handler
from rmake3.core import plug_dispatcher
from rmake3.lib.logger import logFailure
from rmake3.worker import plug_worker

from rpath_repeater.codes import Codes as C, NS
from rpath_repeater.utils import nodeinfo
from rpath_repeater import models
from rpath_repeater.utils.xmlutils import XML
from rpath_repeater.utils.reporting import ReportingMixIn

PREFIX = NS.PREFIX

GenericData = types.slottype('GenericData', 'p nodes argument response')

class BaseException(Exception):
    def __init__(self, error=None):
        self.error = error
        Exception.__init__(self)

class AuthenticationError(BaseException):
    "Authentication error"

class RegistryAccessError(BaseException):
    "Registry Access error"

class CIFSMountError(BaseException):
    "CIFS Mount error"

class WindowsServiceError(BaseException):
    "Windows Service error"

class GenericError(BaseException):
    "Error"

class WmiError(BaseException):
    "Wmi Error"

class BaseForwardingPlugin(plug_dispatcher.DispatcherPlugin,
                           plug_worker.WorkerPlugin):
    pass


def exposed(func):
    """Decorator that exposes a method as being externally callable"""
    func.exposed = True
    return func


class Options(object):
    __slots__ = ('exposed', )

    def __init__(self):
        self.exposed = set()

    def addExposed(self, name):
        self.exposed.add(name)


class BaseHandler(handler.JobHandler, ReportingMixIn):
    X_Event_Uuid_Header = 'X-rBuilder-Event-UUID'
    X_Job_Token_Header = 'X-rBuilder-Job-Token'
    RegistrationTaskNS = None
    ReportingXmlTag = "system"
    slotType = 'inventory'

    class __metaclass__(type):
        def __new__(cls, name, bases, attrs):
            ret = type.__new__(cls, name, bases, attrs)
            ret.Meta = Options()
            for attrName, attrVal in attrs.items():
                if getattr(attrVal, 'exposed', None):
                    ret.Meta.addExposed(attrName)
            return ret

    def setup(self):
        self._taskStatusCodeWatchers = {}

    def addTaskStatusCodeWatcher(self, code, watcher):
        self._taskStatusCodeWatchers[code] = watcher

    def initCall(self):
        self.data = self.getData().thaw().getDict()
        self.zone = self.data.pop('zone', None)
        self.method = self.data.get('method')
        self.methodArguments = self.data.pop('methodArguments', {})
        self.resultsLocation = self.data.pop('resultsLocation', {})
        self.eventUuid = self.data.pop('eventUuid', None)
        self.authToken = self.data.pop('authToken')
        jobUrl = self.data.pop('jobUrl')
        if jobUrl:
            self.jobUrl = models.URL.fromString(jobUrl, host='localhost', port=80)
        else:
            self.jobUrl = None
        self.zoneAddresses = [x + ':8443' for x in self._getZoneAddresses()]

    def newTask(self, *args, **kwargs):
        "Create a new task, and update the job with the task's status changes"
        task = handler.JobHandler.newTask(self, *args, **kwargs)
        self.watchTask(task, self.jobUpdateCallback)
        return task

    def postprocessXmlNode(self, elt):
        self.addEventInfo(elt)
        self.addJobInfo(elt)
        return elt

    def postprocessHeaders(self, elt, headers):
        eventUuid = self.eventUuid
        if eventUuid:
            headers[self.X_Event_Uuid_Header] = eventUuid.encode('ascii')
        if self.authToken:
            headers[self.X_Job_Token_Header] = self.authToken

    def addEventInfo(self, elt):
        if not self.eventUuid:
            return
        elt.append(XML.Text("event_uuid", self.eventUuid))
        return elt

    def addJobInfo(self, elt):
        # Parse the data, we need to insert the job uuid
        job = self.newJobElement()
        elt.append(XML.Element("jobs", job))

    def _getZoneAddresses(self):
        """Return set of IP addresses of all nodes in this zone."""
        needed = set([
            types.TaskCapability(self.RegistrationTaskNS),
            types.ZoneCapability(self.zone),
            ])
        addresses = set()
        for worker in self.dispatcher.workers.values():
            if worker.supports(needed):
                # Only save the ipv4 address
                for address in worker.addresses:
                    try:
                        socket.inet_pton(socket.AF_INET, address)
                    except socket.error:
                        continue
                    addresses.update([address,])
        return addresses

    def jobUpdateCallback(self, task):
        status = task.status.thaw()
        if status.final:
            # We don't have to do anything, _handleTaskCallback will do that
            # for us
            return
        self.setStatus(status)
        self.postStatus()
        watcher = self._taskStatusCodeWatchers.get(status.code)
        if watcher is not None:
            watcher(task)

    def _handleTask(self, task):
        """
        Handle responses for a task execution
        """
        self.currentTask = task
        d = self.waitForTask(task)
        d.addCallbacks(self._handleTaskCallback, self._handleTaskError)
        return d

    def _handleTaskCallback(self, task):
        if task.status.failed:
            self.setStatus(task.status.thaw())
            self.postFailure()
            return 'done'
        else:
            return self._handleTaskComplete(task)

    def _handleTaskComplete(self, task):
        response = task.task_data.getObject().response
        self.job.data = types.FrozenObject.fromObject(response)
        self._taskStatusCodeWatchers.clear()
        # Post results first, if results processing fails then set the job as
        # failed and try to post the failure.
        self.job.status = types.JobStatus(C.OK, "Done")
        d = maybeDeferred(self.postResults, failHard=True)
        d.addCallback(lambda _: self.setStatus(self.job.status))
        d.addCallback(lambda _: 'done')
        @d.addErrback
        def _postFailed(reason):
            log.error("Error posting results for job %s of type %s: %s",
                    self.job.job_uuid, self.job.job_type,
                    reason.getErrorMessage())
            return self._handleTaskError(reason)
        d.addErrback(logFailure)
        d.addBoth(lambda _: 'done')
        return d

    def _handleTaskError(self, reason):
        """
        Error callback that gets invoked if rmake failed to handle the job.
        Clean errors from the repeater do not see this function.
        """
        d = self.failJob(reason)
        self.postFailure()
        self._taskStatusCodeWatchers.clear()
        return d


class BaseTaskHandler(plug_worker.TaskHandler):
    TemporaryDir = "/dev/shm"
    InterfaceName = None

    def run(self):
        """
        Exception handing for the _run method doing the real work
        """
        data = self.getData()
        try:
            self._run(data)
        except nodeinfo.ProbeHostError, e:
            self.sendStatus(C.ERR_NOT_FOUND, "Management interface not found on %s: %s" % (data.p.host, str(e)))
        except AuthenticationError, e:
            if e.error:
                errmsg = e.error
            else:
                _t = 'Credentials provided do not have permission to make %s calls on %s'
                errmsg = _t % (self.InterfaceName, data.p.host)
            self.sendStatus(C.ERR_AUTHENTICATION, errmsg)
        except BaseException, e:
            typ, value, tb = sys.exc_info()
            # no epdb on the rbuilder, this breaks stuff!
            #import epdb; epdb.post_mortem(tb, typ, value)
            if e.error:
                errmsg = e.error
            else:
                errmsg = "Error: %s" % e.__class__.__name__
            self.sendStatus(C.ERR_GENERIC, errmsg)
        except:
            typ, value, tb = sys.exc_info()
            # no epdb on the rbuilder, this breaks stuff!
            #import epdb; epdb.post_mortem(tb, typ, value)
            out = StringIO.StringIO()
            formatTrace(typ, value, tb, stream = out, withLocals = False)
            out.write("\nFull stack:\n")
            formatTrace(typ, value, tb, stream = out, withLocals = True)
            formatTrace(typ, value, tb, stream = sys.stderr, withLocals = True)

            log.error(out.getvalue())
            self.sendStatus(C.ERR_GENERIC, "Error: %s" % str(value),
                out.getvalue())

    @classmethod
    def _tempFile(cls, prefix, contents):
        # NamedTemporaryFile will conveniently go *poof* when it gets closed
        tmpf = tempfile.NamedTemporaryFile(dir=cls.TemporaryDir, prefix=prefix)
        tmpf.write(contents)
        # Flush the contents on the disk, so python's ssl lib can see them
        tmpf.flush()
        return tmpf

    @classmethod
    def _trove(cls, troveSpec):
        return models.Trove.fromTroveSpec(troveSpec).toXmlDom()
