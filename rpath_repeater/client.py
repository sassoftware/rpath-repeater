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
import time

from conary.lib import util

from rmake3.client import RmakeClient
from rmake3.lib import uuid as RmakeUuid

from rmake3.core.types import RmakeJob
from rmake3.core.types import SlotCompare

from rpath_repeater.utils.immutabledict import FrozenImmutableDict
from rpath_repeater import models

class RepeaterClient(object):
    __WMI_PLUGIN_NS = 'com.rpath.sputnik.wmiplugin'
    __CIM_PLUGIN_NS = 'com.rpath.sputnik.cimplugin'
    __LAUNCH_PLUGIN_NS = 'com.rpath.sputnik.launchplugin'
    __MGMT_IFACE_PLUGIN_NS = 'com.rpath.sputnik.interfacedetectionplugin'
    __IMAGE_UPLOAD_PLUGIN_NS = 'com.rpath.sputnik.imageuploadplugin'

    CimParams = models.CimParams
    WmiParams = models.WmiParams
    ManagementInterfaceParams = models.ManagementInterfaceParams
    URL = models.URL
    ResultsLocation = models.ResultsLocation
    ImageFile = models.ImageFile

    @classmethod
    def makeUrl(cls, url, headers=None):
        scheme, user, passwd, host, port, path, query, fragment = util.urlSplit(
            url)
        # Join back query and fragment
        unparsedPath = path
        if query:
            unparsedPath = "%s?%s" % (unparsedPath, query)
        if fragment:
            unparsedPath = "%s#%s" % (unparsedPath, fragment)
        return cls.URL(scheme=scheme, username=user, password=passwd,
            host=host, port=port, path=path, query=query, fragment=fragment,
            unparsedPath=unparsedPath, headers=headers)

    def __init__(self, address=None, zone=None):
        if not address:
            address = 'http://localhost:9998/'

        self.client = RmakeClient(address)
        self.zone = zone

    def _callParams(self, method, resultsLocation, zone, **kwargs):
        params = dict(method=method, zone=zone or self.zone)
        if kwargs:
            params['methodArguments'] = kwargs
        if resultsLocation is not None:
            assert isinstance(resultsLocation, self.ResultsLocation)
            params['resultsLocation'] = resultsLocation.toDict()
        data = FrozenImmutableDict(params)
        return params

    def _launchRmakeJob(self, namespace, params):
        data = FrozenImmutableDict(params)
        job = RmakeJob(RmakeUuid.uuid4(), namespace, owner='nobody',
                       data=data,
                       ).freeze()

        uuid = job.job_uuid
        job = self.client.createJob(job)

        return (uuid, job.thaw())

    def _cimCallDispatcher(self, method, cimParams, resultsLocation, zone,
            **kwargs):
        params = self._callParams(method, resultsLocation, zone, **kwargs)
        assert isinstance(cimParams, self.CimParams)
        if cimParams.port is None:
            cimParams.port = 5989
        params['cimParams'] = cimParams.toDict()
        return self._launchRmakeJob(self.__CIM_PLUGIN_NS, params)

    def _wmiCallDispatcher(self, method, wmiParams, resultsLocation, zone,
            **kwargs):
        params = self._callParams(method, resultsLocation, zone, **kwargs)
        assert isinstance(wmiParams, self.WmiParams)
        if wmiParams.port is None:
            wmiParams.port = 135
        params['wmiParams'] = wmiParams.toDict()
        return self._launchRmakeJob(self.__WMI_PLUGIN_NS, params)

    def register_cim(self, cimParams, resultsLocation=None, zone=None,
            requiredNetwork=None):
        method = 'register'
        return self._cimCallDispatcher(method, cimParams, resultsLocation, zone,
            requiredNetwork=requiredNetwork)

    def register_wmi(self, wmiParams, resultsLocation=None, zone=None,
            requiredNetwork=None):
        method = 'register'
        return self._wmiCallDispatcher(method, wmiParams, resultsLocation, zone,
            requiredNetwork=requiredNetwork)

    def shutdown_cim(self, cimParams, resultsLocation=None, zone=None):
        method = 'shutdown'
        return self._cimCallDispatcher(method, cimParams, resultsLocation, zone)

    def shutdown_wmi(self, cimParams, resultsLocation=None, zone=None):
        method = 'shutdown'
        raise NotImplementedError(method)

    def update_cim(self, cimParams, resultsLocation=None, zone=None, sources=None):
        method = 'update'
        return self._cimCallDispatcher(method, cimParams, resultsLocation, zone,
            sources=sources)

    def update_wmi(self, wmiParams, resultsLocation=None, zone=None, sources=None):
        method = 'update'
        return self._wmiCallDispatcher(method, wmiParams, resultsLocation, zone,
            sources=sources)

    def configuration_cim(self, cimParams, resultsLocation=None, zone=None, configuration=None):
        method = 'configuration'
        return self._cimCallDispatcher(method, cimParams, resultsLocation, zone,
            configuration=configuration)

    def configuration_wmi(self, wmiParams, resultsLocation=None, zone=None, configuration=None):
        method = 'configuration'
        raise NotImplementedError(method)
        return self._wmiCallDispatcher(method, wmiParams, resultsLocation, zone,
            configuration=configuration)

    def retireNode(self, node, zone, port = None):
        """ This is a temporary large hammer for handling the retirement
            of a management node.
        """
        return self.shutdown(node, zone, port)

    def getNodes(self):
        return self.client.getWorkerList()

    def poll_cim(self, cimParams, resultsLocation=None, zone=None):
        method = 'poll'
        return self._cimCallDispatcher(method, cimParams, resultsLocation, zone)

    def poll_wmi(self, wmiParams, resultsLocation=None, zone=None):
        method = 'poll'
        return self._wmiCallDispatcher(method, wmiParams, resultsLocation, zone)

    def launchWaitForNetwork(self, cimParams, resultsLocation=None, zone=None,
                             **kwargs):
        params = dict(zone=zone or self.zone)
        if kwargs:
            params['methodArguments'] = kwargs
        assert isinstance(cimParams, self.CimParams)
        params['cimParams'] = cimParams.toDict()
        if resultsLocation is not None:
            assert isinstance(resultsLocation, self.ResultsLocation)
            params['resultsLocation'] = resultsLocation.toDict()

        data = FrozenImmutableDict(params)
        job = RmakeJob(RmakeUuid.uuid4(), self.__LAUNCH_PLUGIN_NS, 
                       owner='nobody',
                       data=data,
                       ).freeze()

        uuid = job.job_uuid
        job = self.client.createJob(job)

        return (uuid, job.thaw())

    def detectMgmtInterface(self, mgmtParams, resultsLocation=None,
            zone=None):
        """
        ifaceParamList is a list of ManagementInterfaceParams to be probed
        """
        params = dict(zone=zone or self.zone, params=mgmtParams.toDict())

        if resultsLocation is not None:
            assert isinstance(resultsLocation, self.ResultsLocation)
            params['resultsLocation'] = resultsLocation.toDict()

        data = FrozenImmutableDict(params)
        job = RmakeJob(RmakeUuid.uuid4(), self.__MGMT_IFACE_PLUGIN_NS,
                       owner='nobody',
                       data=data,
                       ).freeze()

        uuid = job.job_uuid
        job = self.client.createJob(job)

        return (uuid, job.thaw())

    def download_images(self, token, imageList):
        """
        """
        params = dict(
            token = token,
            imageList = [ x.toDict() for x in imageList ],)
        data = FrozenImmutableDict(params)
        job = RmakeJob(RmakeUuid.uuid4(), self.__IMAGE_UPLOAD_PLUGIN_NS,
                       owner='nobody',
                       data=data,
                       ).freeze()

        uuid = job.job_uuid
        job = self.client.createJob(job)

        return (uuid, job.thaw())


    def getJob(self, uuid):
        return self.client.getJob(uuid).thaw()


def main():
    if len(sys.argv) < 2:
        print "Usage: %s system" % sys.argv[0]
        return 1
    system = sys.argv[1]
    zone = 'Local rBuilder'
    cli = RepeaterClient()
    eventUuid = "0xDeadBeef"
    resultsLocation = cli.ResultsLocation(path="/adfadf", port=1234)
    cimParams = cli.CimParams(host=system,
        eventUuid=eventUuid,
        #requiredNetwork="1.1.1.1",
        #clientCert=file("/tmp/reinhold.crt").read(),
        #clientKey=file("/tmp/reinhold.key").read(),
        zone=zone,
    )
    wmiParams = cli.WmiParams(host=system, port=135,
        eventUuid = eventUuid,
        username="Administrator",
        password="password",
        domain=system)
    if 0:
        uuid, job = cli.register_cim(cimParams)
    elif 0:
        uuid, job = cli.poll_cim(cimParams, resultsLocation=resultsLocation,
            zone=zone)
    elif 0:
        params = cli.ManagementInterfaceParams(host=system,
            eventUuid = eventUuid,
            interfacesList = [
                dict(interfaceHref='/api/inventory/management_interfaces/2',
                     port=1234),
                dict(interfaceHref='/api/inventory/management_interfaces/1',
                     port=5989),
            ])
        uuid, job = cli.detectMgmtInterface(params,
            resultsLocation = resultsLocation,
            zone = zone)
    elif 0:
        uuid, job = cli.register_wmi(wmiParams,
            resultsLocation = resultsLocation,
            zone=zone)
    elif 0:
        uuid, job = cli.poll_wmi(wmiParams,
            resultsLocation = resultsLocation,
            zone = zone)
    elif 0:
        uuid, job = cli.update_wmi(wmiParams,
            resultsLocation = resultsLocation,
            zone = zone,
            sources = [
                'group-windemo-appliance=/windemo.eng.rpath.com@rpath:windemo-1-devel/1-2-1[]',
            ],
            )
    else:
        headers = {'X-rBuilder-OutputToken' : 'aaa'}
        images = [
            cli.ImageFile(url=cli.makeUrl('http://george.rdu.rpath.com/CentOS/5.5/isos/x86_64/CentOS-5.5-x86_64-bin-1of8.iso'),
                destination=cli.makeUrl('http://localhost:1234/uploadImage/1',
                    headers=headers),
                progress=cli.makeUrl('http://localhost:1234/foo/1',
                    headers=headers)),
            cli.ImageFile(url=cli.makeUrl('http://george.rdu.rpath.com/CentOS/5.5/isos/x86_64/CentOS-5.5-x86_64-bin-2of8.iso'),
                destination=cli.makeUrl('http://localhost:1234/uploadImage/2',
                    headers=headers),
                progress=cli.makeUrl('http://localhost:1234/foo/2',
                    headers=headers)),
        ]
        uuid, job = cli.download_images('token', images)
    while 1:
        job = cli.getJob(uuid)
        if job.status.final:
            break
        time.sleep(1)
    print "Failed: %s" % job.status.failed
    #import epdb; epdb.st()

if __name__ == "__main__":
    main()
