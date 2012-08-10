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

    def checkCreate(self):
        return self._invoke(codes.NS.TARGET_TEST_CREATE)

    def checkCredentials(self):
        return self._invoke(codes.NS.TARGET_TEST_CREDENTIALS)

    def listImages(self, imageIds=None):
        return self._invoke(codes.NS.TARGET_IMAGES_LIST, imageIds=imageIds)

    def listInstances(self, instanceIds=None):
        return self._invoke(codes.NS.TARGET_INSTANCES_LIST, instanceIds=instanceIds)

    def captureSystem(self, instanceId, params):
        return self._invoke(codes.NS.TARGET_SYSTEM_CAPTURE,
            instanceId=instanceId, params=params)

    def imageDeploymentDescriptor(self):
        return self._invoke(codes.NS.TARGET_IMAGE_DEPLOY_DESCRIPTOR,
                jobUrl=None)

    def systemLaunchDescriptor(self):
        return self._invoke(codes.NS.TARGET_SYSTEM_LAUNCH_DESCRIPTOR,
                jobUrl=None)

    def deployImage(self, params):
        return self._invoke(codes.NS.TARGET_IMAGE_DEPLOY, params=params)

    def launchSystem(self, params):
        return self._invoke(codes.NS.TARGET_SYSTEM_LAUNCH, params=params)

    def _invoke(self, ns, **kwargs):
        client = self.getClient()

        jobUuid = RmakeUuid.uuid4()
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

    def _cimCallDispatcher(self, method, cimParams, resultsLocation=None,
            zone=None, uuid=None, jobToken=None, **kwargs):
        if uuid is None:
            uuid = RmakeUuid.uuid4()
        params = self._callParams(method, resultsLocation, zone, jobToken,
                **kwargs)
        assert isinstance(cimParams, self.CimParams)
        if cimParams.port is None:
            cimParams.port = 5989
        params['cimParams'] = cimParams.toDict()
        return self._launchRmakeJob(self.__CIM_PLUGIN_NS, params, uuid=uuid)

    def _wmiCallDispatcher(self, method, wmiParams, resultsLocation=None,
            zone=None, uuid=None, jobToken=None, **kwargs):
        if uuid is None:
            uuid = RmakeUuid.uuid4()
        params = self._callParams(method, resultsLocation, zone, jobToken,
                **kwargs)
        assert isinstance(wmiParams, self.WmiParams)
        if wmiParams.port is None:
            wmiParams.port = 135
        params['wmiParams'] = wmiParams.toDict()
        return self._launchRmakeJob(self.__WMI_PLUGIN_NS, params, uuid=uuid)

    def register_cim(self, cimParams, **kwargs):
        method = 'register'
        return self._cimCallDispatcher(method, cimParams, **kwargs)

    def register_wmi(self, wmiParams, **kwargs):
        method = 'register'
        return self._wmiCallDispatcher(method, wmiParams, **kwargs)

    def bootstrap(self, assimilatorParams, resultsLocation=None, zone=None,
            uuid=None, jobToken=None, **kwargs):
        '''this will only be valid for Linux, and adopts an unmanaged system'''
        params = self._callParams('bootstrap', resultsLocation, zone, jobToken)
        assert isinstance(assimilatorParams, self.AssimilatorParams)
        if assimilatorParams.port is None:
            assimilatorParams.port = 22
        params['assimilatorParams'] = assimilatorParams.toDict()
        return self._launchRmakeJob(self.__ASSIMILATOR_PLUGIN_NS, params, uuid=uuid)

    def shutdown_cim(self, cimParams, **kwargs):
        method = 'shutdown'
        return self._cimCallDispatcher(method, cimParams, **kwargs)

    def shutdown_wmi(self, cimParams, **kwargs):
        method = 'shutdown'
        raise NotImplementedError(method)

    def update_cim(self, cimParams, sources=None, **kwargs):
        method = 'update'
        return self._cimCallDispatcher(method, cimParams, sources=sources, **kwargs)

    def update_wmi(self, wmiParams, sources=None, **kwargs):
        method = 'update'
        return self._wmiCallDispatcher(method, wmiParams, sources=sources, **kwargs)

    def configuration_cim(self, cimParams, configuration=None, **kwargs):
        method = 'configuration'
        return self._cimCallDispatcher(method, cimParams,
            configuration=configuration, **kwargs)

    def configuration_wmi(self, wmiParams, configuration=None, **kwargs):
        method = 'configuration'
        return self._wmiCallDispatcher(method, wmiParams,
            configuration=configuration, **kwargs)

    def survey_scan_cim(self, cimParams, **kwargs):
        method = 'survey_scan'
        return self._cimCallDispatcher(method, cimParams, **kwargs)

    def survey_scan_wmi(self, wmiParams, **kwargs):
        method = 'survey_scan'
        return self._wmiCallDispatcher(method, wmiParams, **kwargs)

    def retireNode(self, node, zone, port = None):
        """ This is a temporary large hammer for handling the retirement
            of a management node.
        """
        return self.shutdown(node, zone, port)

    def getNodes(self):
        return self.client.getWorkerList()

    def poll_cim(self, cimParams, **kwargs):
        method = 'poll'
        return self._cimCallDispatcher(method, cimParams, **kwargs)

    def poll_wmi(self, wmiParams, **kwargs):
        method = 'poll'
        return self._wmiCallDispatcher(method, wmiParams, **kwargs)

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

    def detectMgmtInterface(self, mgmtParams, resultsLocation=None,
            zone=None, uuid=None, jobToken=None, **kwargs):
        """
        ifaceParamList is a list of ManagementInterfaceParams to be probed
        """
        params = dict(
                zone=zone or self.zone,
                authToken=jobToken,
                params=mgmtParams.toDict(),
                )

        if resultsLocation is not None:
            assert isinstance(resultsLocation, self.ResultsLocation)
            params['resultsLocation'] = resultsLocation.toDict()

        uuid, job = self._launchRmakeJob(self.__MGMT_IFACE_PLUGIN_NS,
            params, uuid=uuid)

        return (uuid, job)

    def getJob(self, uuid):
        return self.client.getJob(uuid).thaw()


def main():
    if len(sys.argv) < 2:
        print "Usage: %s system" % sys.argv[0]
        return 1
    system = sys.argv[1]
    zone = 'Local rBuilder'
    cli = RepeaterClient(jobUrlTemplate="http://localhost:1234/api/v1/jobs/%(job_uuid)s")
    eventUuid = "0xDeadBeef"
    resultsLocation = cli.ResultsLocation(path="/adfadf", port=1234)
    cimParams = cli.CimParams(host=system,
        eventUuid=eventUuid,
        #requiredNetwork="1.1.1.1",
        #clientCert=file("/tmp/reinhold.crt").read(),
        #clientKey=file("/tmp/reinhold.key").read(),
    )
    wmiParams = cli.WmiParams(host=system, port=135,
        eventUuid = eventUuid,
        username="Administrator",
        password="password",
        domain=system)
    targetConfiguration = cli.targets.TargetConfiguration(
        'vmware', 'vsphere.eng.rpath.com', 'vsphere', config={})
    userCredentials = cli.targets.TargetUserCredentials(credentials=dict(
        username="eng", password="password"),
        rbUser="dontcare", rbUserId=1, isAdmin=False, opaqueCredentialsId=1)
    if 1:
        uuid, job = cli.survey_scan_cim(cimParams, zone=zone)
    elif 0:
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
    elif 1:
        cli.targets.configure(zone, targetConfiguration, userCredentials)
        instanceId = ""
        params = dict(
            outputToken='afe25c82-94e9-44c3-ae47-b352f06ee3aa',
            imageUploadUrl="http://dhcp155.eng.rpath.com/uploadBuild/5",
            imageFilesCommitUrl="http://dhcp155.eng.rpath.com/api/products/celery/images/5/files",
            imageTitle='Image Title',
            imageName="cobbler-clone.ova",
            image_id="http://dhcp155.eng.rpath.com/api/v1/images/5",
        )
        params.update({'metadata.owner': 'Owner', 'metadata.admin' : 'Admin'})
        instanceId = '42345ce9-8a1e-7239-ceb9-3cfbcc8c6667'
        uuid, job = cli.targets.captureSystem(instanceId, params)
    elif 0:
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
       
        keyData = file("/root/.ssh/id_rsa.pub").read() 
        assimilatorParams = cli.AssimilatorParams(host=system, port=22,
            eventUuid='eventUuid',
            # normally filled in by rBuilder
            caCert=file("/srv/rbuilder/pki/hg_ca.crt").read(),
            # normally filled in by plugin
            platformLabels={
                'centos-5' : [ 'jules.eng.rpath.com@rpath:centos-5-stable',
                               'centos.rpath.com@rpath:centos-5-common' ],
                'sles-11'  : [ 'jules.eng.rpath.com@rpath:sles-11-stable', 
                               'sles.rpath.com@rpath:sles-11-common' ]
            },
            sshAuth = [
                           { 
                               'sshUser'     : 'root', 
                               'sshKey'      : keyData,
                               'sshPassword' : 'letmein',
                           },
                           { 
                               'sshUser'     : 'root', 
                               'sshPassword' : 'wrong1' 
                           },
                           { 
                               'sshUser'     : 'root', 
                               'sshPassword' : 'password' 
                           },
            ]
        )

        uuid, job = cli.bootstrap(assimilatorParams,
            resultsLocation = resultsLocation,
            zone = zone)
    while 1:
        job = cli.getJob(uuid)
        if job.status.final:
            break
        time.sleep(1)
    print "Failed: %s; %s" % (job.status.failed, job.status.text)

if __name__ == "__main__":
    main()
