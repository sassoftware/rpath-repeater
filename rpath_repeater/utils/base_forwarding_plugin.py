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

import StringIO
import sys
import tempfile
from xml.dom import minidom

from conary import versions
from conary import conaryclient
from conary.lib.formattrace import formatTrace

from twisted.web import client
from twisted.internet import reactor

from rmake3.core import types
from rmake3.core import handler
from rmake3.core import plug_dispatcher
from rmake3.worker import plug_worker

PREFIX = 'com.rpath.sputnik'
BASE_JOB = PREFIX + '.baseplugin'
BASE_TASK_REGISTER = PREFIX + '.register'

from rpath_repeater.codes import Codes as C
from rpath_repeater.utils import nodeinfo

class BaseException(Exception):
    def __init__(self, error=None):
        self.error = error
        Exception.__init__(self)

class AuthenticationError(BaseException):
    "Authentication error"

class GenericError(BaseException):
    "Error"

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


class BaseHandler(handler.JobHandler):
    X_Event_Uuid_Header = 'X-rBuilder-Event-UUID'

    class __metaclass__(type):
        def __new__(cls, name, bases, attrs):
            ret = type.__new__(cls, name, bases, attrs)
            ret.Meta = Options()
            for attrName, attrVal in attrs.items():
                if getattr(attrVal, 'exposed', None):
                    ret.Meta.addExposed(attrName)
            return ret

    def setup(self):
        pass

    def initCall(self):
        self.data = self.getData().thaw().getDict()
        self.zone = self.data.pop('zone', None)
        self.method = self.data.get('method')
        self.methodArguments = self.data.pop('methodArguments', {})
        self.resultsLocation = self.data.pop('resultsLocation', {})
        self.eventUuid = self.data.pop('eventUuid', None)

    def newTask(self, *args, **kwargs):
        "Create a new task, and update the job with the task's status changes"
        task = handler.JobHandler.newTask(self, *args, **kwargs)
        self.watchTask(task, self.jobUpdateCallback)
        return task

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
        eventUuid = self.eventUuid
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

    def postFailure(self):
        el = XML.Element("system")
        self.postResults(el)

    def addEventInfo(self, elt):
        if not self.eventUuid:
            return
        elt.appendChild(XML.Text("event_uuid", self.eventUuid))
        return elt

    def addJobInfo(self, elt):
        # Parse the data, we need to insert the job uuid
        T = XML.Text
        jobStateMap = { False : 'Failed', True : 'Completed' }
        jobStateString = jobStateMap[self.job.status.completed]
        children = [
            T("job_uuid", self.job.job_uuid),
            T("job_state", jobStateString),
            T("status_code", self.job.status.code),
            T("status_text", self.job.status.text),
        ]
        if self.job.status.detail:
            children.append(T("status_detail", self.job.status.detail))
        job = XML.Element("job", *children)
        elt.appendChild(XML.Element("jobs", job))

    @classmethod
    def toXml(cls, elt):
        return XML.toString(elt)

    def _getZoneAddresses(self):
        """Return set of IP addresses of all nodes in this zone."""
        needed = set([
            types.TaskCapability(BASE_TASK_REGISTER),
            types.ZoneCapability(self.zone),
            ])
        addresses = set()
        for worker in self.dispatcher.workers.values():
            if worker.supports(needed):
                addresses.update(worker.addresses)
        return addresses

    def jobUpdateCallback(self, task):
        status = task.status.thaw()
        if status.final:
            # We don't have to do anything, _handleTaskCallback will do that
            # for us
            return
        self.setStatus(status)

    def _handleTask(self, task):
        """
        Handle responses for a task execution
        """
        d = self.waitForTask(task)
        d.addCallbacks(self._handleTaskCallback, self._handleTaskError)
        return d

    def _handleTaskCallback(self, task):
        if task.status.failed:
            self.setStatus(task.status.thaw())
            self.postFailure()
        else:
            self._handleTaskComplete(task)
        return 'done'

    def _handleTaskComplete(self, task):
        response = task.task_data.getObject().response
        self.job.data = response
        self.setStatus(C.OK, "Done")
        self.postResults()

    def _handleTaskError(self, reason):
        """
        Error callback that gets invoked if rmake failed to handle the job.
        Clean errors from the repeater do not see this function.
        """
        d = self.failJob(reason)
        self.postFailure()
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
            self.sendStatus(C.ERR_NOT_FOUND, "%s not found on %s:%d: %s" % (
                self.InterfaceName, data.p.host, data.p.port, str(e)))
        except AuthenticationError, e:
            if e.error:
                errmsg = e.error
            else:
                _t = 'Credentials provided do not have permission to make %s calls on %s'
                errmsg = _t % (self.InterfaceName, data.p.host)
            self.sendStatus(C.ERR_AUTHENTICATION, errmsg)
        except BaseException, e:
            if e.error:
                errmsg = e.error
            else:
                errmsg = "Error"
            self.sendStatus(C.ERR_GENERIC, errmsg)
        except:
            typ, value, tb = sys.exc_info()
            out = StringIO.StringIO()
            formatTrace(typ, value, tb, stream = out, withLocals = False)
            out.write("\nFull stack:\n")
            formatTrace(typ, value, tb, stream = out, withLocals = True)

            self.sendStatus(C.ERR_GENERIC, "Error in %s call: %s" %
                    (self.InterfaceName, str(value)),
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
        Text = XML.Text
        n, v, f = conaryclient.cmdline.parseTroveSpec(troveSpec)

        name = Text("name", n)
        version = cls._version(v, f)
        flavor = cls._flavor(f)
        return XML.Element("trove", name, version, flavor)

    @classmethod
    def _flavor(cls, flavor):
        if flavor is None:
            return XML.Element("flavor")
        return XML.Text("flavor", str(flavor))

    @classmethod
    def _version(cls, v, f):
        thawed_v = versions.ThawVersion(v)
        Text = XML.Text
        full = Text("full", str(thawed_v))
        ordering = Text("ordering", thawed_v.timeStamps()[0])
        revision = Text("revision", str(thawed_v.trailingRevision()))
        label = Text("label", str(thawed_v.trailingLabel()))
        flavor = cls._flavor(f)
        return XML.Element("version", full, label, revision, ordering, flavor)


class XML(object):
    @classmethod
    def Text(cls, tagName, text):
        txt = minidom.Text()
        txt.data = text
        return cls.Element(tagName, txt)

    @classmethod
    def Element(cls, tagName, *children, **attributes):
        node = cls._Node(tagName, minidom.Element)
        for child in children:
            node.appendChild(child)
        for k, v in attributes.items():
            node.setAttribute(k, unicode(v).encode("utf-8"))
        return node

    @classmethod
    def _Node(cls, tagName, factory):
        node = factory(tagName)
        return node

    @classmethod
    def toString(cls, elt):
        return elt.toxml(encoding="UTF-8").encode("utf-8")

class HTTPClientFactory(client.HTTPClientFactory):
    def __init__(self, url, *args, **kwargs):
        client.HTTPClientFactory.__init__(self, url, *args, **kwargs)
        self.status = None
        self.deferred.addCallback(
            lambda data: (data, self.status, self.response_headers))
