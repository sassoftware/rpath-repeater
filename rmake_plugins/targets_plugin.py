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

from rmake3.core import handler

from conary.lib.formattrace import formatTrace

from mint import users
from catalogService import errors
from catalogService.rest.models import xmlNode

from rpath_repeater import models
from rpath_repeater.codes import Codes as C
from rpath_repeater.codes import NS
from rpath_repeater.utils import base_forwarding_plugin as bfp

class TargetsPlugin(bfp.BaseForwardingPlugin):
    """
    Setup dispatcher side of the interface detection.
    """

    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(TargetsTestCreateHandler)
        handler.registerHandler(TargetsTestCredentialsHandler)
        handler.registerHandler(TargetsImageListHandler)
        handler.registerHandler(TargetsInstanceListHandler)

    def worker_get_task_types(self):
        return {
            NS.TARGET_TEST_CREATE: TargetsTestCreate,
            NS.TARGET_TEST_CREDENTIALS: TargetsTestCredentials,
            NS.TARGET_IMAGES_LIST: TargetsImageListTask,
            NS.TARGET_INSTANCES_LIST: TargetsInstanceListTask,
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
            self.jobUrl = models.URL.fromString(jobUrl)
        else:
            self.jobUrl = None

    def _run(self):
        self.setStatus(C.MSG_NEW_TASK, 'Creating task')
        task = self.newTask(self.jobType, self.jobType, self.data, zone=self.zone)
        return self._handleTask(task)

    def getResultsLocation(self):
        if self.jobUrl is None:
            return None, None, None
        host = self.jobUrl.host or 'localhost'
        port = self.jobUrl.port or 80
        path = self.jobUrl.unparsedPath
        return host, port, path

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

class RestDatabase(object):
    __slots__ = [ 'cfg', 'auth', ]
    class Auth(object):
        __slots__ = [ 'auth', ]
    def __init__(self):
        self.cfg = None
        self.auth = self.Auth()


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
                "Error in Interface Detection call: %s"
                    % str(value), out.getvalue())

    def _initConfig(self):
        self.data = self.getData()
        params = self.data.pop('params')
        self.targetConfig = params.targetConfiguration
        self.userCredentials = params.targetUserCredentials
        self.cmdArgs = params.args

    def _initTarget(self):
        targetType = self.targetConfig.targetType
        moduleName = "catalogService.rest.drivers.%s" % targetType
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
        self.driver = Driver(None, targetType, cloudName=self.targetConfig.targetName,
            db=restDb)
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
        db = self.RestDatabaseClass()
        if self.userCredentials is not None:
            db.auth.auth = users.Authorization(authorized=True,
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
        instances = self.driver.getInstancesFromTarget(None)
        self.finishCall(instances, "Retrieved list of instances")

