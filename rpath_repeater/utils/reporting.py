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
from xml.dom import minidom

from rpath_repeater.utils.http import HTTPClientFactory
from rpath_repeater.utils.xmlutils import XML

class ReportingMixIn(object):
    """
    Assumes:
        resultsLocation
        ReportingXmlTag (class variable)
    """
    def postResults(self, elt=None, method=None):
        if method is None:
            method = 'PUT'
        host, port, path = self.getResultsLocation()
        if not path:
            return
        if elt is None:
            dom = minidom.parseString(self.job.data)
            elt = dom.firstChild
        elt = self.postprocessXmlNode(elt)
        data = self.toXml(elt)
        headers = {
            'Content-Type' : 'application/xml; charset="utf-8"',
            'Host' : host, }
        self.postprocessHeaders(elt, headers)
        fact = HTTPClientFactory(path, method=method, postdata=data,
            headers = headers)
        @fact.deferred.addCallback
        def processResult(result):
            log.debug("Received result for %s: %s", host, result)
            return result

        @fact.deferred.addErrback
        def processError(error):
            log.error("Error: %s", error.getErrorMessage())

        reactor.connectTCP(host, port, fact)

    def getResultsLocation(self):
        host = self.resultsLocation.get('host', 'localhost')
        port = self.resultsLocation.get('port', 80)
        path = self.resultsLocation.get('path')
        return host, port, path

    def postFailure(self, method=None):
        el = XML.Element(self.ReportingXmlTag)
        self.postResults(el, method=method)

    def postprocessXmlNode(self, elt):
        return elt

    def postprocessHeaders(self, elt, headers):
        pass

    def newJobElement(self):
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
        return job

    def addJobResults(self, job, results):
        resultsNode = XML.Element("results", results)
        job.appendChild(resultsNode)
        return job

    @classmethod
    def toXml(cls, elt):
        return XML.toString(elt)
