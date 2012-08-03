#
# Copyright (c) 2010-2011 rPath, Inc.
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

from twisted.internet import reactor
from twisted.internet import task as ti_task
from twisted.web import error as tw_error
from xml.dom import minidom

from rmake3.lib.twisted_extras import tools
from rpath_repeater.utils.http import HTTPClientFactory
from rpath_repeater.utils.xmlutils import XML

class ReportingMixIn(object):
    """
    Assumes:
        resultsLocation
        ReportingXmlTag (class variable)
    """
    _serializer = None
    retryCount = 5
    retryInterval = 3

    def postResults(self, elt=None, method=None, location=None,
            collapsible=False, retry=True):
        if method is None:
            method = 'PUT'
        if location is None:
            location = self.getResultsUrl()
        host, port, path = self._getResultsLocation(location)
        if not path:
            return
        if elt is None:
            dom = minidom.parseString(self.job.data)
            elt = dom.firstChild
        if isinstance(elt, basestring):
            # We were given an XML string, no need to postprocess it
            data = elt
        else:
            elt = self.postprocessXmlNode(elt)
            data = self.toXml(elt)
        headers = {
            'Content-Type' : 'application/xml; charset="utf-8"',
            'Host' : host, }
        self.postprocessHeaders(elt, headers)
        # Serialize posts to make sure they arrive in chronological order
        if self._serializer is None:
            self._serializer = tools.Serializer()
        connArgs = (host, port)
        factArgs = dict(url=path, method=method, postdata=data,
                headers=headers)
        retries = self.retryCount if retry else 0
        return self._serializer.call(self._doPost, collapsible=collapsible,
                args=(connArgs, factArgs, retries))

    def _doPost(self, connArgs, factArgs, retries=0):
        fact = HTTPClientFactory(**factArgs)
        @fact.deferred.addCallback
        def processResult(result):
            log.debug("Received result for %s: %s", host, result)
            return result
        @fact.deferred.addErrback
        def processError(error):
            if (retries and error.check(tw_error.Error)
                    and error.value.status == '401'):
                log.debug("Got authorization error posting status update, "
                        "trying again")
                return ti_task.deferLater(reactor, self.retryInterval,
                        self._doPost, connArgs, factArgs, retries - 1)
            log.error("Error posting status update for job %s of type %s: %s",
                    self.job.job_uuid, self.job.job_type,
                    error.getErrorMessage())
        host, port = connArgs
        reactor.connectTCP(host, port, fact)
        return fact.deferred

    def getResultsUrl(self):
        if self.resultsLocation:
            return self.resultsLocation
        return self.jobUrl

    def _getResultsLocation(self, location):
        if location is None:
            return None, None, None
        if hasattr(location, 'host'):
            return location.host, location.port, location.path
        host = location.get('host', 'localhost')
        port = location.get('port', 80)
        path = location.get('path')
        return host, port, path

    def postStatus(self):
        el = self.newJobElement()
        return self.postResults(el, location=self.jobUrl)

    def postFailure(self, method=None):
        el = XML.Element(self.ReportingXmlTag)
        return self.postResults(el, method=method)

    def postprocessXmlNode(self, elt):
        return elt

    def postprocessHeaders(self, elt, headers):
        pass

    def newJobElement(self):
        T = XML.Text
        status = self.job.status
        if status.failed:
            state = 'Failed'
        elif status.completed:
            state = 'Completed'
        else:
            state = 'Running'
        return XML.Element("job",
            T("job_uuid", self.job.job_uuid),
            T("job_state", state),
            T("status_code", status.code),
            T("status_text", status.text),
            T("status_detail", status.detail or ''),
            )

    def addJobResults(self, job, results):
        resultsNode = XML.Element("results", results)
        job.appendChild(resultsNode)
        return job

    @classmethod
    def toXml(cls, elt):
        return XML.toString(elt)
