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
import time
import StringIO

from conary.lib.formattrace import formatTrace

from rmake3.core import types
from rmake3.core import handler

from rmake3.worker import plug_worker

from catalogService import storage
from catalogService.rest.database import RestDatabase

from mint import users
from mint import config
from mint.db import database
from mint.rest.db import authmgr
from mint.django_rest.rbuilder.inventory import models

from catalogService.rest.api import clouds

PREFIX = 'com.rpath.sputnik'
LAUNCH_JOB = PREFIX + '.launchplugin'
LAUNCH_TASK_WAIT_FOR_NETWORK = PREFIX + '.waitForNetwork'

from rpath_repeater.codes import Codes as C

CimParams = types.slottype('CimParams',
    'host port clientCert clientKey eventUuid instanceId targetName targetType launchWaitTime')
LaunchData = types.slottype('LaunchData', 'p response')


class LaunchHandler(handler.JobHandler):
    timeout = 7200

    jobType = LAUNCH_JOB

    def setup(self):
        pass

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
        else:
            response = task.task_data.getObject().response
            self.job.data = response
            self.setStatus(C.OK, "Done. %s" % response)
            jobState = models.JobState.objects.get(name=models.JobState.COMPLETED)
            job = models.Job.objects.get(job_uuid=self.job.job_uuid)
            job.job_state = jobState
            job.status_code = self.job.status.code
            job.status_text = self.job.status.text
            job.status_detail = self.job.status.detail
            job.save()
        return 'done'

    def _handleTaskError(self, reason):
        """
        Error callback that gets invoked if rmake failed to handle the job.
        Clean errors from the repeater do not see this function.
        """
        d = self.failJob(reason)
        self.postFailure()
        return d

    def waitForNetwork(self):
        self.setStatus(C.MSG_NEW_TASK, "Creating task")

        args = LaunchData(self.cimParams)
        task = self.newTask('waitForNetwork', LAUNCH_TASK_WAIT_FOR_NETWORK,
            args, zone=self.zone)
        return self._handleTask(task)

    def starting(self):
        self.data = self.getData().thaw().getDict()
        self.zone = self.data.pop('zone', None)
        self.cimParams = CimParams(**self.data.pop('cimParams', {}))
        self.resultsLocation = self.data.pop('resultsLocation', {})
        self.eventUuid = self.data.pop('eventUuid', None)

        self.setStatus(C.MSG_START,
            "Waiting for the network information to become "
            "available for instance %s" % self.cimParams.instanceId)
        return 'waitForNetwork'


class WaitForNetworkTask(plug_worker.TaskHandler):

    taskType = LAUNCH_TASK_WAIT_FOR_NETWORK
    TemporaryDir = "/dev/shm"

    def loadTargetDriverClasses(self):
        for driverName in clouds.SUPPORTED_MODULES:
            driverClass = __import__('catalogService.rest.drivers.%s' % (driverName),
                                      {}, {}, ['driver']).driver
            yield driverClass

    def loadTargetDrivers(self, restdb):
        storagePath = '/tmp'
        storageConfig = storage.StorageConfig(storagePath=storagePath)
        for driverClass in self.loadTargetDriverClasses():
            targetType = driverClass.cloudType
            targets = restdb.targetMgr.getUniqueTargetsForUsers(targetType)
            for ent in targets:
                userId, userName, targetName = ent[:3]
                driver = driverClass(storageConfig, targetType,
                    cloudName=targetName, userId=userName, db=restdb)
                if not driver.isDriverFunctional():
                    continue
                driver._nodeFactory.baseUrl = "https://localhost"
                yield driver

    def _run(self, data):        
        instanceId = data.p.instanceId
        targetType = data.p.targetType
        launchWaitTime = data.p.launchWaitTime
        cfg = config.MintConfig()
        cfg.read(config.RBUILDER_CONFIG)

        db = database.Database(cfg)
        authToken = (cfg.authUser, cfg.authPass)
        cu = db.cursor()
        cu.execute("SELECT MIN(userId) from userGroupMembers "
                   "WHERE is_admin = true")
        ret = cu.fetchall()
        userId = ret[0][0]
        mintAuth = users.Authorization(
                username=cfg.authUser,
                token=authToken,
                admin=True,
                userId=userId,
                authorized=True)
        auth = authmgr.AuthenticationManager(cfg, db)
        auth.setAuth(mintAuth, authToken)
        restdb = RestDatabase(cfg, db)

        # do i need these?
        restdb.auth.userId = userId
        restdb.auth.setAuth(mintAuth, authToken)

        from mint.django_rest.rbuilder.manager import rbuildermanager
        mgr = rbuildermanager.RbuilderManager()
        targetDrivers = [d for d in self.loadTargetDrivers(restdb) \
                         if d.cloudType == targetType]
        td = targetDrivers[0]

        hasDnsName = False
        setDnsName = False
        sleptTime = 0

        while sleptTime < launchWaitTime:
            system = mgr.getSystemByTargetSystemId(instanceId)
            networks = system.networks.all()
            if networks:
                network = networks[0]
                if network.dns_name:
                    hasDnsName = True
                    break

            instance = td.getInstance(instanceId)
            dnsName = instance.getPublicDnsName()
            if dnsName:
                system = mgr.getSystemByTargetSystemId(instanceId)
                networks = system.networks.all()
                if networks:
                    network = networks[0]
                    network.dns_name = dnsName
                    system.save()
                else:
                    network = models.Network(dns_name=dnsName)
                    system.networks.add(network)
                system.save()
                setDnsName = True
                break

            time.sleep(5)
            sleptTime += 5

        if setDnsName:
            mgr.scheduleSystemDetectMgmtInterfaceEvent(system)
            response = ("dns name for %s updated to %s.  Scheduled Management "
                "interface detection event" % (instanceId, dnsName))
            data.response = response
            self.setData(data)
            self.sendStatus(C.OK, response)
        elif hasDnsName:
            response = ("dns name for %s became avaiable. no longer checking "
                "target"  % instanceId)
            data.response = response
            self.setData(data)
            self.sendStatus(C.OK, response)
        else:
            response = ("timed out waiting for dns name for instance %s"
                % instanceId)
            data.response = response
            self.setData(data)
            self.sendStatus(C.ERR_GENERIC, response)

    def run(self):
        """
        Exception handing for the _run method doing the real work
        """
        data = self.getData()
        try:
            self._run(data)
        except:
            typ, value, tb = sys.exc_info()
            out = StringIO.StringIO()
            formatTrace(typ, value, tb, stream = out, withLocals = False)
            out.write("\nFull stack:\n")
            formatTrace(typ, value, tb, stream = out, withLocals = True)

            self.sendStatus(C.ERR_GENERIC,
                "Error in launch wait task: %s" % value,
                 out.getvalue())
