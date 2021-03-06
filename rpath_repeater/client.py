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
import time
import weakref

from conary.lib import util

from rmake3.client import RmakeClient
from rmake3.lib import uuid as RmakeUuid

from rmake3.core.types import RmakeJob

from rpath_repeater.utils.immutabledict import FrozenImmutableDict
from rpath_repeater import codes, models

class BaseCommand(object):
    def __init__(self, client):
        self.client = weakref.ref(client)

    def getClient(self):
        return self.client()

class TargetCommand(BaseCommand):
    TargetConfiguration = models.TargetConfiguration
    TargetUserCredentials = models.TargetUserCredentials
    def __init__(self, client):
        BaseCommand.__init__(self, client)
        self._targetConfig = None
        self._userCredentials = None

    def configure(self, zone, targetConfiguration, userCredentials=None,
            allUserCredentials=None):
        self._zone = zone
        self._targetConfig = targetConfiguration
        self._userCredentials = userCredentials
        self._allUserCredentials = allUserCredentials

    def checkCreate(self, **kwargs):
        return self._invoke(codes.NS.TARGET_TEST_CREATE, **kwargs)

    def checkCredentials(self, **kwargs):
        return self._invoke(codes.NS.TARGET_TEST_CREDENTIALS, **kwargs)

    def listImages(self, imageIds=None, **kwargs):
        return self._invoke(codes.NS.TARGET_IMAGES_LIST, imageIds=imageIds, **kwargs)

    def listInstances(self, instanceIds=None, **kwargs):
        return self._invoke(codes.NS.TARGET_INSTANCES_LIST,
            instanceIds=instanceIds, **kwargs)

    def imageDeploymentDescriptor(self, **kwargs):
        return self._invoke(codes.NS.TARGET_IMAGE_DEPLOY_DESCRIPTOR,
                jobUrl=None, **kwargs)

    def systemLaunchDescriptor(self, **kwargs):
        return self._invoke(codes.NS.TARGET_SYSTEM_LAUNCH_DESCRIPTOR,
                jobUrl=None, **kwargs)

    def deployImage(self, params, **kwargs):
        return self._invoke(codes.NS.TARGET_IMAGE_DEPLOY, params=params,
            **kwargs)

    def launchSystem(self, params, **kwargs):
        return self._invoke(codes.NS.TARGET_SYSTEM_LAUNCH, params=params,
            **kwargs)

    def _invoke(self, ns, **kwargs):
        client = self.getClient()

        # Use the supplied jobUuid if possible
        jobUuid = kwargs.pop('uuid', RmakeUuid.uuid4())
        if 'jobUrl' in kwargs:
            jobUrl = kwargs.pop('jobUrl')
        elif client.jobUrlTemplate:
            jobUrl = client.jobUrlTemplate % dict(job_uuid=jobUuid)
        else:
            jobUrl = None
        params = models.TargetCommandArguments(
            targetConfiguration=self._targetConfig,
            targetUserCredentials=self._userCredentials,
            args=kwargs,
            targetAllUserCredentials=self._allUserCredentials,
        )
        # authToken is the "cookie" that will be used for posting data
        # back to the REST interface
        params = dict(zone=self._zone,
            authToken=RmakeUuid.uuid4(),
            jobUrl=jobUrl,
            params=params)
        data = FrozenImmutableDict(params)
        return client._createRmakeJob(ns, data, uuid=jobUuid)

class RepeaterClient(object):
    __WMI_PLUGIN_NS = codes.NS.WMI_JOB
    __CIM_PLUGIN_NS = codes.NS.CIM_JOB
    # FIXME: the following is probably unused
    __ASSIMILATOR_PLUGIN_NS = 'com.rpath.sputnik.assimilatorplugin'
    __LAUNCH_PLUGIN_NS = 'com.rpath.sputnik.launchplugin'
    __MGMT_IFACE_PLUGIN_NS = 'com.rpath.sputnik.interfacedetectionplugin'

    CimParams = models.CimParams
    WmiParams = models.WmiParams
    AssimilatorParams = models.AssimilatorParams
    ManagementInterfaceParams = models.ManagementInterfaceParams
    URL = models.URL
    ResultsLocation = models.ResultsLocation
    Image = models.Image
    ImageFile = models.ImageFile

    TargetCommandClass = TargetCommand

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

    def __init__(self, address=None, zone=None, jobUrlTemplate=None):
        """
        jobUrlTemplate is a URL that will be completed by filling in
        job_uuid
        """
        if not address:
            address = 'http://localhost:9998/'

        self.client = RmakeClient(address)
        self.zone = zone
        self.targets = self.TargetCommandClass(self)
        if jobUrlTemplate is None:
            jobUrlTemplate = "http://localhost/api/v1/jobs/%(job_uuid)s"
        self.jobUrlTemplate = jobUrlTemplate

    def _callParams(self, method, resultsLocation, zone, jobToken, **kwargs):
        params = dict(
                method=method,
                zone=zone or self.zone,
                authToken=jobToken,
                )
        if kwargs:
            params['methodArguments'] = kwargs
        if resultsLocation is not None:
            assert isinstance(resultsLocation, self.ResultsLocation)
            params['resultsLocation'] = resultsLocation.toDict()
        return params

    def _launchRmakeJob(self, namespace, params, uuid=None):
        if self.jobUrlTemplate and uuid:
            jobUrl = self.jobUrlTemplate % dict(job_uuid=uuid)
            params['jobUrl'] = jobUrl
        if not params.get('authToken'):
            params['authToken'] = RmakeUuid.uuid4()
        data = FrozenImmutableDict(params)
        return self._createRmakeJob(namespace, data, uuid=uuid)

    def _createRmakeJob(self, namespace, data, uuid=None,
            expiresAfter='1 day'):
        if uuid is None:
            uuid = RmakeUuid.uuid4()
        job = RmakeJob(uuid, namespace, owner='nobody',
                       data=data,
                       )
        # Repeater job results are copied somewhere else on completion, so
        # expire them from the rmake database shortly thereafter
        job.times.expires_after = expiresAfter

        uuid = job.job_uuid
        job = self.client.createJob(job.freeze())

        return (uuid, job.thaw())

    def bootstrap(self, assimilatorParams, resultsLocation=None, zone=None,
            uuid=None, jobToken=None, **kwargs):
        '''this will only be valid for Linux, and adopts an unmanaged system'''
        params = self._callParams('bootstrap', resultsLocation, zone, jobToken)
        assert isinstance(assimilatorParams, self.AssimilatorParams)
        if assimilatorParams.port is None:
            assimilatorParams.port = 22
        params['assimilatorParams'] = assimilatorParams.toDict()
        return self._launchRmakeJob(self.__ASSIMILATOR_PLUGIN_NS, params, uuid=uuid)

    def getNodes(self):
        return self.client.getWorkerList()

    def launchWaitForNetwork(self, cimParams, resultsLocation=None, zone=None,
            uuid=None, jobToken=None, **kwargs):
        params = dict(
                zone=zone or self.zone,
                jobToken=jobToken,
                )
        if kwargs:
            params['methodArguments'] = kwargs
        assert isinstance(cimParams, self.CimParams)
        params['cimParams'] = cimParams.toDict()
        if resultsLocation is not None:
            assert isinstance(resultsLocation, self.ResultsLocation)
            params['resultsLocation'] = resultsLocation.toDict()

        uuid, job = self._launchRmakeJob(self.__LAUNCH_PLUGIN_NS,
           params, uuid=uuid)

        return (uuid, job)

    def getJob(self, uuid):
        return self.client.getJob(uuid).thaw()


def main():
    if len(sys.argv) < 2:
        print "Usage: %s system" % sys.argv[0]
        return 1
    zone = 'Local rBuilder'
    cli = RepeaterClient(jobUrlTemplate="http://localhost:1234/api/v1/jobs/%(job_uuid)s")
    targetConfiguration = cli.targets.TargetConfiguration(
        'vmware', 'vsphere.eng.rpath.com', 'vsphere', config={})
    userCredentials = cli.targets.TargetUserCredentials(credentials=dict(
        username="eng", password="password"),
        rbUser="dontcare", rbUserId=1, isAdmin=False, opaqueCredentialsId=1)
    if 1:
        cli.targets.configure(zone, targetConfiguration)
        uuid, job = cli.targets.checkCreate()
    elif 0:
        cli.targets.configure(zone, targetConfiguration, userCredentials)
        uuid, job = cli.targets.checkCredentials()
    elif 0:
        cli.targets.configure(zone, targetConfiguration, userCredentials)
        uuid, job = cli.targets.launchSystem({
            'imageFileInfo' : {
                'name': 'celery-1-x86_64.ova',
                'sha1': '851cbe6c3f1e5fe47c41df6f3a3d2947a3d8c384',
                'size' : '155248640',
                'fileId' : 5,
                'baseFileName' : 'celery-1-x86_64',
            },
            'descriptorData': """\
<descriptor_data>
  <imageId>5</imageId>
  <instanceName>My Deployed System</instanceName>
  <instanceDescription>My Deployed System - description</instanceDescription>
  <dataCenter>datacenter-6098</dataCenter>
  <vmfolder-datacenter-6098>group-v6099</vmfolder-datacenter-6098>
  <cr-datacenter-6098>domain-c19781</cr-datacenter-6098>
  <network-datacenter-6098>dvportgroup-19802</network-datacenter-6098>
  <dataStore-domain-c19781>datastore-19907</dataStore-domain-c19781>
  <resourcePool-domain-c19781>resgroup-19782</resourcePool-domain-c19781>
</descriptor_data>""",
            'systemsCreateUrl' : 'http://localhost:12347/a/b/c',
            'imageDownloadUrl': 'http://localhost/cgi-bin/cobbler-clone.ova',
            'imageFileUpdateUrl': 'http://localhost:12346/api/v1/images/5/build_files/5',
            'targetImageXmlTemplate': '<file>\n  <target_images>\n    <target_image>\n      <target id="/api/v1/targets/1"/>\n      %(image)s\n    </target_image>\n  </target_images>\n</file>',
            'targetImageIdList' : ['aaa', 'bbb', 'ccc', '4234ba5a-6d51-e826-5940-ad5a122b0109', ],
        })
    elif 0:
        cli.targets.configure(zone, targetConfiguration, userCredentials)
        uuid, job = cli.targets.systemLaunchDescriptor()
    elif 1:
        cli.targets.configure(zone, targetConfiguration, None, [ userCredentials ])
        uuid, job = cli.targets.listInstances()
    elif 0:
        cli.targets.configure(zone, targetConfiguration, userCredentials)
        uuid, job = cli.targets.deployImage({
            'imageFileInfo' : {
                'name': 'celery-1-x86_64.ova',
                'sha1': '851cbe6c3f1e5fe47c41df6f3a3d2947a3d8c384',
                'size' : '155248640',
                'fileId' : 5,
                'baseFileName' : 'celery-1-x86_64',
            },
            'descriptorData': """\
<descriptor_data>
  <imageId>5</imageId>
  <imageName>My Deployed Image</imageName>
  <imageDescription>My Deployed Image - description</imageDescription>
  <dataCenter>datacenter-6098</dataCenter>
  <vmfolder-datacenter-6098>group-v6099</vmfolder-datacenter-6098>
  <cr-datacenter-6098>domain-c19781</cr-datacenter-6098>
  <network-datacenter-6098>dvportgroup-19802</network-datacenter-6098>
  <dataStore-domain-c19781>datastore-19907</dataStore-domain-c19781>
  <resourcePool-domain-c19781>resgroup-19782</resourcePool-domain-c19781>
</descriptor_data>""",
            'imageDownloadUrl': 'http://localhost/cgi-bin/cobbler-clone.ova',
            'imageFileUpdateUrl': 'http://localhost:12346/api/v1/images/5/build_files/5',
            'targetImageXmlTemplate': '<file>\n  <target_images>\n    <target_image>\n      <target id="/api/v1/targets/1"/>\n      %(image)s\n    </target_image>\n  </target_images>\n</file>'
        })
    elif 0:
        cli.targets.configure(zone, targetConfiguration, userCredentials)
        uuid, job = cli.targets.imageDeploymentDescriptor()
    elif 0:
        cli.targets.configure(zone, targetConfiguration, userCredentials)
        uuid, job = cli.targets.listImages(imageIds=None)
    while 1:
        job = cli.getJob(uuid)
        if job.status.final:
            break
        time.sleep(1)
    print "Failed: %s; %s" % (job.status.failed, job.status.text)

if __name__ == "__main__":
    main()
