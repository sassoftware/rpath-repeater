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

import logging

log = logging.getLogger(__name__)

import os
import collections
import StringIO
import socket
import sys
import tempfile
import time
from xml.dom import minidom

from conary import versions
from conary import conaryclient
from conary.lib import digestlib
from conary.lib.formattrace import formatTrace

from twisted.internet import defer, protocol, reactor
from twisted.web import client, iweb
from zope.interface import implements

from rmake3.core import types
from rmake3.core import handler
from rmake3.core import plug_dispatcher
from rmake3.worker import plug_worker

PREFIX = 'com.rpath.sputnik'

from rpath_repeater.codes import Codes as C
from rpath_repeater.utils import nodeinfo

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


class BaseHandler(handler.JobHandler):
    X_Event_Uuid_Header = 'X-rBuilder-Event-UUID'
    RegistrationTaskNS = None

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
        self.zoneAddresses = [x + ':8443' for x in self._getZoneAddresses()]

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
        fact = HTTPClientFactory(path, method='PUT', postdata=data,
            headers = headers)
        @fact.deferred.addCallback
        def processResult(result):
            log.debug("Received result for %s: %s", host, result)
            return result

        @fact.deferred.addErrback
        def processError(error):
            log.error("Error: %s", error.getErrorMessage())

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
            self.sendStatus(C.ERR_NOT_FOUND, "Management interface not found on %s: %s" % (data.p.host, str(e)))
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
    USER_AGENT = "rmake-plugin/1.0"

    def __init__(self, url, *args, **kwargs):
        if 'agent' not in kwargs:
            kwargs.update(agent=self.USER_AGENT)
        client.HTTPClientFactory.__init__(self, url, *args, **kwargs)
        self.status = None
        self.deferred.addCallback(
            lambda data: (data, self.status, self.response_headers))

def ImageUpload(image, statusReportURL, putFilesURL):
    dl = []
    for imageFile in image.files:
        dl.append(ImageFileUpload(imageFile, statusReportURL))
    deferred = defer.DeferredList(dl)

    @deferred.addCallback
    def cb(resultList):
        imageFiles = [ x[1] for x in resultList ]
        image.files = imageFiles
        setImageStatus(image, statusReportURL, putFilesURL)

def _getImageStatusXML(image):
    files = [
        XML.Element("file",
            XML.Text("title", i.title),
            XML.Text("size", str(i.size)),
            XML.Text("sha1", i.sha1),
            XML.Text("fileName", i.fileName))
        for i in image.files ]
    root = XML.Element("files", *files)
    data = BaseHandler.toXml(root)
    return data

def setImageStatus(image, statusReportURL, setFilesURL):
    data = _getImageStatusXML(image)
    fact = ProgressReporter.createFactory(setFilesURL, "PUT", data)
    ProgressReporter.registerFactory(setFilesURL, fact)

    ProgressReporter.publishProgress(statusReportURL,
        code=300, message="Finished")

def ImageFileUpload(imageFile, statusReportURL):
    deferred = defer.Deferred()
    @deferred.addCallback
    def cb((sha1, size)):
        imageFile.sha1 = sha1
        imageFile.size = size
        return imageFile
    Splicer(imageFile.url, imageFile.destination, statusReportURL, deferred)
    return deferred


class Splicer(object):
    USER_AGENT = HTTPClientFactory.USER_AGENT

    def __init__(self, urlsrc, urldest, progressUrl, consumerFinished):
        agent1 = client.Agent(reactor)
        headers = {
            'User-Agent' : [ self.USER_AGENT ],
        }

        usrc = urlsrc.asString()
        deferred = agent1.request("GET", usrc, client.Headers(headers), None)

        @deferred.addCallback
        def cb(response):
            log.debug("Source response received: HTTP status %s", response.code)
            PipingAgent(urldest, headers.copy(), response, progressUrl,
                consumerFinished)

def _toList(val):
    if not isinstance(val, list):
        val = [ val ]
    return val

def PipingAgent(url, headers, response, progressUrl, consumerFinished):
    agent = client.Agent(reactor)
    finished = defer.Deferred()
    @finished.addCallback
    def cb_bodyProducer(response):
        log.debug("body produced")
        return "Succeeded"
    bodyProducer = BodyProducer(response, finished, url, progressUrl,
        consumerFinished)

    headers.update({
        'Content-Type' : [ 'application/octet-string' ],
    })
    headers.update((x, _toList(y)) for (x, y) in (url.headers or {}).items())
    udst = url.asString()
    deferred = agent.request("PUT", udst, client.Headers(headers),
        bodyProducer)

    @deferred.addCallback
    def cb_response(response):
        log.debug("Response received: HTTP code %s", response.code)
        return response
    return consumerFinished

class SplicingProtocol(protocol.Protocol):
    """
    A protocol that splices data to a consumer
    """
    def __init__(self, finished):
        self.finished = finished
        self._consumer = None

    def setConsumer(self, consumer):
        self._consumer = consumer

    def dataReceived(self, bytes):
        self._consumer.write(bytes)

    def connectionLost(self, reason):
        log.debug('Finished receiving body: %s', reason.getErrorMessage())
        if self._consumer:
            self._consumer.close()
            self._consumer = None
        self.finished.callback(None)

class ProgressReporter(object):
    @classmethod
    def publishProgress(cls, progressUrl, code, message):
        root = XML.Element('imageStatus',
            XML.Text('code', str(code)),
            XML.Text('message', message))

        data = BaseHandler.toXml(root)
        fact = cls.createFactory(progressUrl, "PUT", data)
        @fact.deferred.addCallback
        def cb(data):
            log.debug("Finished uploading status")
        cls.registerFactory(progressUrl, fact)

    @classmethod
    def createFactory(cls, url, method, data, headers=None):
        fheaders = { 'Content-Type' : 'application/xml',
            'Host' : url.host, }
        if url.headers:
            fheaders.update(url.headers)
        if headers:
            fheaders.update(headers)
        log.debug("Headers: %s; data: %s", fheaders, data)
        fact = HTTPClientFactory(url.unparsedPath, method=method,
            postdata=data, headers=fheaders)
        return fact

    @classmethod
    def registerFactory(cls, url, factory):
        port = int(url.port or 80)
        reactor.connectTCP(url.host, port, factory)

class BufferedConsumer(object):
    BUFFER_SIZE = 10
    PROGRESS_TIMEOUT = 2
    def __init__(self, protocol, length, url, progressUrl, finished):
        self._readproto = protocol
        self._consumer = None
        self._buf = collections.deque()
        self._bytesDownloaded = 0
        self._paused = True
        self._length = length
        self._url = url
        self.nextProgressCall = 0
        self.progressUrl = progressUrl
        self._ctx = digestlib.sha1()
        self._finished = finished

    def setConsumer(self, consumer):
        self._consumer = consumer
        self._paused = False

    def write(self, data):
        log.debug("Producer: produced %d bytes", len(data))
        if data:
            self._buf.append(data)
        self._consume()

    def _consume(self):
        if self._consumer is None:
            log.debug("Waiting for a consumer")
            self.producer_pauseProducing()
            return
        log.debug("Consuming; buffer length: %d", len(self._buf))
        self.flush()

    def flush(self):
        now = time.time()
        while self._buf and not self._paused:
            data = self._buf.popleft()
            self._bytesDownloaded += len(data)
            self._consumer.write(data)
            self._ctx.update(data)

        if now > self.nextProgressCall or self._bytesDownloaded == self._length:
            self.nextProgressCall = now + self.PROGRESS_TIMEOUT
            self.progressCallback(self._bytesDownloaded, self._length)
        log.debug(" flush: buffer length %d", len(self._buf))

    def close(self):
        self.flush()
        if not self._buf:
            self.producer_stopProducing()

    def producer_pauseProducing(self):
        if self._readproto is None:
            return
        log.debug("readproto: pauseProducing")
        self._paused = True
        if len(self._buf) < self.BUFFER_SIZE:
            return
        log.debug("readproto: really pauseProducing")
        self._readproto.transport.pauseProducing()

    def producer_resumeProducing(self):
        if self._readproto is None:
            return
        log.debug("readproto: resumeProducing")
        self._readproto.transport.resumeProducing()
        self._paused = False
        self.flush()

    def producer_stopProducing(self):
        log.debug("readproto: stopProducing")
        if self._readproto:
            self._readproto.transport.stopProducing()
            self._readproto = None
            sha1 = self._ctx.hexdigest()
            self._finished.callback((sha1, self._length))

    def progressCallback(self, bytesDownloaded, bytesTotal):
        if not self.progressUrl:
            return
        msg = "%s: Downloaded %s/%s (%d%%)" % (
            os.path.basename(self._url.path),
            bytesDownloaded, bytesTotal,
            int(bytesDownloaded * 100 / bytesTotal))
        code = 100
        self._progressCallback(code, msg)

    def _progressCallback(self, code, message):
        return ProgressReporter.publishProgress(self.progressUrl, code, message)

class BodyProducer(object):
    implements(iweb.IBodyProducer)

    def __init__(self, response, finished, url, progressUrl, consumerFinished):
        self.length = response.length
        self.finished = finished
        protocol = SplicingProtocol(finished)
        self._consumerFinished = consumerFinished
        self._consumer = BufferedConsumer(protocol, self.length, url,
            progressUrl, self._consumerFinished)
        protocol.setConsumer(self._consumer)
        # Send the response's body to the protocol.
        # This calls the protocol's dataReceived, which will buffer the read
        # and then attempt to consume it.
        response.deliverBody(protocol)

    @property
    def consumerFinished(self):
        return self._consumerFinished

    def startProducing(self, consumer):
        self._consumer.setConsumer(consumer)
        d = defer.Deferred()
        @d.addCallback
        def cb(res):
            log.debug("Produced")
        return d

    def stopProducing(self):
        if self._consumer:
            self._consumer.producer_stopProducing()
            self._consumer = None

    def pauseProducing(self):
        self._consumer.producer_pauseProducing()

    def resumeProducing(self):
        self._consumer.producer_resumeProducing()
