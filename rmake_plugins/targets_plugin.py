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

from xobj import xobj2
from lxml import etree
import sys
import StringIO
import weakref

from rmake3.core import handler

from conary.lib.formattrace import formatTrace

from catalogService import errors
from catalogService import storage

from catalogService.rest.models import xmlNode

from rpath_repeater import models
from rpath_repeater.codes import Codes as C
from rpath_repeater.codes import NS
from rpath_repeater.utils import base_forwarding_plugin as bfp

class Authorization(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class TargetsPlugin(bfp.BaseForwardingPlugin):
    """
    Setup dispatcher side of the interface detection.
    """

    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(TargetsTestCreateHandler)
        handler.registerHandler(TargetsTestCredentialsHandler)
        handler.registerHandler(TargetsImageListHandler)
        handler.registerHandler(TargetsInstanceListHandler)
        handler.registerHandler(TargetsInstanceCaptureHandler)
        handler.registerHandler(TargetsImageDeployHandler)
        handler.registerHandler(TargetsSystemLaunchHandler)
        handler.registerHandler(TargetsImageDeployDescriptorHandler)
        handler.registerHandler(TargetsSystemLaunchDescriptorHandler)

    def worker_get_task_types(self):
        return {
            NS.TARGET_TEST_CREATE: TargetsTestCreate,
            NS.TARGET_TEST_CREDENTIALS: TargetsTestCredentials,
            NS.TARGET_IMAGES_LIST: TargetsImageListTask,
            NS.TARGET_INSTANCES_LIST: TargetsInstanceListTask,
            NS.TARGET_SYSTEM_CAPTURE: TargetsInstanceCaptureTask,
            NS.TARGET_IMAGE_DEPLOY: TargetsImageDeployTask,
            NS.TARGET_SYSTEM_LAUNCH: TargetsSystemLaunchTask,
            NS.TARGET_IMAGE_DEPLOY_DESCRIPTOR: TargetsImageDeployDescriptorTask,
            NS.TARGET_SYSTEM_LAUNCH_DESCRIPTOR: TargetsSystemLaunchDescriptorTask,
        }


class BaseHandler(bfp.BaseHandler):
    firstState = 'callRun'

    def setup(self):
        bfp.BaseHandler.setup(self)

    def callRun(self):
        self.initCall()
        return self._run()

    def initCall(self):
        bfp.BaseHandler.initCall(self)
        if not self.zone:
            self.setStatus(C.ERR_ZONE_MISSING, 'Required argument zone missing')
            self.postFailure()
            return
        self.authToken = self.data.pop('authToken')
        jobUrl = self.data.pop('jobUrl')
        if jobUrl:
            self.jobUrl = models.URL.fromString(jobUrl, host='localhost', port=80)
        else:
            self.jobUrl = None

    def _run(self):
        self.setStatus(C.MSG_NEW_TASK, 'Creating task')
        task = self.newTask(self.jobType, self.jobType, self.data, zone=self.zone)
        return self._handleTask(task)

    def getResultsUrl(self):
        return self.jobUrl

    def postprocessHeaders(self, elt, headers):
        if self.authToken:
            headers['X-rBuilder-Job-Token'] = self.authToken

    def postprocessXmlNode(self, elt):
        job = self.newJobElement()
        self.addJobResults(job, elt)
        return job

class TargetsTestCreateHandler(BaseHandler):
    jobType = NS.TARGET_TEST_CREATE

class TargetsTestCredentialsHandler(BaseHandler):
    jobType = NS.TARGET_TEST_CREDENTIALS

class TargetsImageListHandler(BaseHandler):
    jobType = NS.TARGET_IMAGES_LIST

class TargetsInstanceListHandler(BaseHandler):
    jobType = NS.TARGET_INSTANCES_LIST

class TargetsInstanceCaptureHandler(BaseHandler):
    jobType = NS.TARGET_SYSTEM_CAPTURE

class TargetsImageDeployHandler(BaseHandler):
    jobType = NS.TARGET_IMAGE_DEPLOY

    def setup(self):
        BaseHandler.setup(self)
        self.addTaskStatusCodeWatcher(C.PART_RESULT_1,
            self.linkImage)

    def linkImage(self, task):
        params = self.data['params'].args['params']
        targetImageXmlTemplate = params['targetImageXmlTemplate']
        response = task.task_data.thaw().getObject()
        imageXml = response.response
        targetImageXml = targetImageXmlTemplate % dict(image=imageXml)
        imageFileUpdateUrl = params['imageFileUpdateUrl']
        location = models.URL.fromString(imageFileUpdateUrl, port=80)
        self.postResults(targetImageXml, location=location)

class TargetsSystemLaunchHandler(TargetsImageDeployHandler):
    jobType = NS.TARGET_SYSTEM_LAUNCH

    def setup(self):
        TargetsImageDeployHandler.setup(self)
        self.addTaskStatusCodeWatcher(C.PART_RESULT_2,
            self.uploadSystems)

    def uploadSystems(self, task):
        params = self.data['params'].args['params']
        systemsCreateUrl = params['systemsCreateUrl']
        response = task.task_data.thaw().getObject()
        systemsXml = response.response
        location = models.URL.fromString(systemsCreateUrl, port=80)
        self.postResults(systemsXml, method='POST', location=location)

class TargetsImageDeployDescriptorHandler(BaseHandler):
    jobType = NS.TARGET_IMAGE_DEPLOY_DESCRIPTOR

class TargetsSystemLaunchDescriptorHandler(BaseHandler):
    jobType = NS.TARGET_SYSTEM_LAUNCH_DESCRIPTOR

class RestDatabase(object):
    __slots__ = [ 'cfg', 'auth', 'taskHandler', 'targetMgr', ]
    class Auth(object):
        __slots__ = [ 'auth', ]

    class Cfg(object):
        __slots__ = [ 'proxy', ]
        def __init__(self, **kwargs):
            for s in self.__slots__:
                setattr(self, s, kwargs.get(s, None))

    class TargetManager(object):
        def __init__(self, taskHandler):
            self.taskHandler = taskHandler

        def linkTargetImageToImage(self, targetTypeName, targetName,
                rbuilderImageId, targetImageId):
            self.taskHandler.linkTargetImageToImage(rbuilderImageId, targetImageId)

    def __init__(self, taskHandler):
        self.taskHandler = weakref.proxy(taskHandler)
        # XXX Proxy information will have to be read from conary's
        # config object (which is set by the rAPA plugin, both on the
        # rUS and on the rbuilder).
        self.cfg = self.Cfg(proxy={})
        self.auth = self.Auth()
        self.targetMgr = self.TargetManager(self.taskHandler)

class BaseTaskHandler(bfp.BaseTaskHandler):
    """
    Task that runs on the rUS to query the target systems.
    """
    RestDatabaseClass = RestDatabase

    def run(self):
        self._initConfig()
        try:
            self._initTarget()
            self._run()
        except:
            typ, value, tb = sys.exc_info()
            out = StringIO.StringIO()
            formatTrace(typ, value, tb, stream = out, withLocals = False)
            out.write("\nFull stack:\n")
            formatTrace(typ, value, tb, stream = out, withLocals = True)

            self.sendStatus(C.ERR_GENERIC,
                "Error in target call: %s"
                    % str(value), out.getvalue())

    def _initConfig(self):
        self.data = self.getData()
        params = self.data.pop('params')
        self.targetConfig = params.targetConfiguration
        self.userCredentials = params.targetUserCredentials
        self.cmdArgs = params.args

    def _initTarget(self):
        driverName = self.targetConfig.targetType
        # xen enterprise is a one-off
        if driverName == 'xen-enterprise':
            driverName = 'xenent'

        moduleName = "catalogService.rest.drivers.%s" % driverName
        BaseDriverClass = __import__(moduleName, {}, {}, '.driver').driver

        class Driver(BaseDriverClass):
            def _getCloudCredentialsForUser(slf):
                return self.userCredentials.credentials
            def _getStoredTargetConfiguration(slf):
                config = self.targetConfig.config.copy()
                config.update(alias=self.targetConfig.alias)
                return config
            def _checkAuth(slf):
                return True

        restDb = self._createRestDatabase()
        scfg = storage.StorageConfig(storagePath="/srv/rbuilder/catalog")
        self.driver = Driver(scfg, driverName, cloudName=self.targetConfig.targetName,
            db=restDb, inventoryHandler=InventoryHandler(weakref.ref(self)))
        self.driver._nodeFactory.baseUrl = '/'

    def finishCall(self, node, msg, code=C.OK):
        if node is not None:
            xml = self.toXml(node)
            data = models.Response(response=xml)
            self.setData(data)
        self.sendStatus(code, msg)

    @classmethod
    def toXml(cls, node):
        if hasattr(node, 'toXml'):
            return node.toXml()
        hndlr = xmlNode.Handler()
        return hndlr.toXml(node)

    def _createRestDatabase(self):
        db = self.RestDatabaseClass(self)
        if self.userCredentials is not None:
            db.auth.auth = Authorization(authorized=True,
                userId=self.userCredentials.rbUserId,
                admin=bool(self.userCredentials.isAdmin))
        return db

class TargetsTestCreate(BaseTaskHandler):
    def _run(self):
        """
        Validate we can talk to the target (if the driver supports that)
        """
        try:
            self.driver.drvVerifyCloudConfiguration(self.targetConfig.config)
        except errors.PermissionDenied:
            return self.finishCall(None, "Invalid target configuration",
                code=C.ERR_BAD_ARGS)
        target = models.Target()
        self.finishCall(target, "Target validated")

class TargetsTestCredentials(BaseTaskHandler):
    def _run(self):
        """
        Validate we can talk to the target using these credentials
        """
        try:
            self.driver.drvValidateCredentials(self.userCredentials.credentials)
        except errors.PermissionDenied:
            return self.finishCall(None, "Invalid target credentials",
                code=C.ERR_AUTHENTICATION)
        target = models.Target()
        self.finishCall(target, "Target credentials validated")

class TargetsImageListTask(BaseTaskHandler):
    def _run(self):
        """
        List target images
        """
        images = self.driver.getImagesFromTarget(None)
        self.finishCall(images, "Retrieved list of images")

class TargetsInstanceListTask(BaseTaskHandler):
    def _run(self):
        """
        List target instances
        """
        instances = self.driver.getAllInstances()
        self.finishCall(instances, "Retrieved list of instances")

class JobProgressTaskHandler(BaseTaskHandler):
    class Job(object):
        def __init__(self, msgMethod):
            self.msgMethod = msgMethod

        def addHistoryEntry(self, *args):
            self.msgMethod(C.MSG_PROGRESS, ' '.join(args))

class TargetsInstanceCaptureTask(JobProgressTaskHandler):
    def _run(self):
        """
        List target instances
        """
        instanceId = self.cmdArgs['instanceId']
        params = self.cmdArgs['params']
        # Look at captureSystem to figure out which params are really
        # used
        job = self.Job(self.sendStatus)
        self.driver.captureSystem(job, instanceId, params)
        imageRef = models.ImageRef(params['image_id'])
        self.finishCall(imageRef, "Instance captured")

class TargetsImageDeployTask(JobProgressTaskHandler):
    def _run(self):
        img = self._deployImage()
        self.finishCall(img, "Image deployed")

    def _deployImage(self):
        params = self.cmdArgs['params']
        job = self.Job(self.sendStatus)
        imageFileInfo = params['imageFileInfo']
        descriptorData = params['descriptorData']
        imageDownloadUrl = params['imageDownloadUrl']
        imgObj = self.driver.imageFromFileInfo(imageFileInfo, imageDownloadUrl)
        self.image = imgObj
        img = self.driver.deployImageFromUrl(job, imgObj, descriptorData)
        return img

    def linkTargetImageToImage(self, rbuilderImageId, targetImageId):
        imageXml = etree.tostring(self.image.getElementTree(),
            xml_declaration=False)

        io = XmlStringIO(imageXml)
        self.finishCall(io, "Linking image", C.PART_RESULT_1)

class System(object):
    def __init__(self, **kwargs):
        self.__dict__.update(kwargs)

class Systems(object):
    _xobjMeta = xobj2.XObjMetadata(
        tag = 'systems',
        elements = [ xobj2.Field("system", [ System ]) ])

class InventoryHandler(object):
    System = System
    Systems = Systems

    def __init__(self, parent):
        self.parent = parent
        self.systems = self.Systems()
        self.systems.system = []
        # The driver is not yet initialized, so don't try to access it
        # in the constructor

    @property
    def log_info(self):
        return self.parent().log_info

    def addSystem(self, systemFields, dnsName=None, withNetwork=True):
        parent = self.parent()
        system = self.System(**systemFields)
        system.dnsName = dnsName
        system.targetName = parent.driver.cloudName
        system.targetType = parent.driver.cloudType
        self.systems.system.append(system)

    def reset(self):
        del self.systems.system[:]

    def commit(self):
        taskHandler = self.parent()
        if taskHandler is None:
            return
        doc = xobj2.Document(root=self.systems)
        io = XmlStringIO(doc.toxml())
        taskHandler.finishCall(io, "Systems created", C.PART_RESULT_2)

class TargetsSystemLaunchTask(TargetsImageDeployTask):
    def _run(self):
        params = self.cmdArgs['params']
        job = self.Job(self.sendStatus)
        imageFileInfo = params['imageFileInfo']
        descriptorData = params['descriptorData']
        imageDownloadUrl = params['imageDownloadUrl']
        img = self._isImageDeployed()
        if img is None:
            params = self.cmdArgs['params']
            img = self.driver.imageFromFileInfo(imageFileInfo, imageDownloadUrl)
        self.image = img
        instanceIdList = self.driver.launchSystemSynchronously(job, img, descriptorData)
        io = XmlStringIO(xobj2.Document.serialize(self.driver.inventoryHandler.systems))
        self.finishCall(io, "Systems launched")

    def _isImageDeployed(self):
        targetImageIdList = self.cmdArgs['params']['targetImageIdList']
        if targetImageIdList is None:
            return None
        images = self.driver.getImagesFromTarget(targetImageIdList)
        if images:
            return images[0]
        return None

class XmlStringIO(StringIO.StringIO):
    def toXml(self):
        return self.getvalue()

class TargetsImageDeployDescriptorTask(BaseTaskHandler):
    def _run(self):
        """
        Fetch image deployment descriptor
        """
        descr = self.driver.getImageDeploymentDescriptor()
        io = XmlStringIO(etree.tounicode(descr.getElementTree()))
        self.finishCall(io, "Descriptor generated")

class TargetsSystemLaunchDescriptorTask(BaseTaskHandler):
    def _run(self):
        """
        Fetch system launch descriptor
        """
        descr = self.driver.getLaunchDescriptor()
        io = XmlStringIO()
        descr.serialize(io)
        self.finishCall(io, "Descriptor generated")
