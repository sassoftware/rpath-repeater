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

import os
import time
import popen2
import tempfile
import itertools

from conary.lib import log
from conary import conarycfg
from conary import conaryclient
from conary.conaryclient import systemmodel

log.setVerbosity(log.INFO)

def runModel(client, cache, modelText):
    model = systemmodel.SystemModelText(client.cfg)
    model.parse(modelText)

    updJob = client.newUpdateJob()
    ts = client.systemModelGraph(model)
    client._updateFromTroveSetGraph(updJob, ts, cache)
    return updJob.getJobs()

def modelsToJobs(cache, client, oldModel, newModel):
    newTroves = []
    for jobSet in runModel(client, cache, oldModel):
        newTroves += [ (x[0], x[2][0], x[2][1]) for x in jobSet ]

    trvs = cache.getTroves(newTroves)
    db = client.getDatabase()
    for trv in trvs:
        # this doesn't add files; we don't need them
        troveId = db.addTrove(trv)
        db.addTroveDone(troveId)

    db.commit()

    finalJobs = runModel(client, cache, newModel)

    oldTroves = [ (x[0], x[1][0], x[1][1]) for x in itertools.chain(*finalJobs)
                  if x[1][0] is not None ]
    newTroves = [ (x[0], x[2][0], x[2][1]) for x in itertools.chain(*finalJobs)
                  if x[2][0] is not None ]

    return oldTroves, newTroves


class wmiClient(object):
    def __init__(self, target, domain, user, password):
        self.baseCmd = ('/usr/bin/wmic --host %(host)s --user %(user)s '
            '--password %(password)s --domain %(domain)s' % {'host': target,
            'user': user, 'password':password, 'domain': (domain or target)})

        self.mountCmd = ("/bin/mount -t cifs -o user=%s,password=%s '//%s/c$' "
            % (user,password,target))

        self._rootDir = None
        self._rootMounted = False

    def unmount(self):
        # unmount and delete the root file system
        if self._rootDir and self._rootMounted:
            os.system('/bin/umount ' + self._rootDir)
            os.rmdir(self._rootDir)
            self._rootMounted = False

    def _wmiCall(self, cmd):
        p = popen2.Popen3(cmd,True)
        rc = p.wait()

        if rc:
            return rc, p.childerr.read()
        return rc, p.fromchild.read()

    def _wmiServiceRequest( self, action, service):
        wmicmd = "%s service %s '%s' " % (self.baseCmd, service)
        return self._wmiCall(wmicmd)

    def startService(self, service):
        self._wmiServiceRequest('start', service)

    def stopService(self, service):
        self._wmiServiceRequest('stop', service)

    def queryService(self, service):
        self._wmiServiceRequest('getstatus', service)

    def waitForServiceToStop(self, service):
        # query the service until is is no longer active
        while (self.queryService('rPath Tools Install Service')[1]
            != 'Service Not Active\n'):
            time.sleep(5.0)

    def getRegistryKey(self, keyPath, key):
        wmicmd = "%s registry getkey '%s' '%s'" % (self.baseCmd, keyPath, key)
        return self._wmiCall(wmicmd)

    def setRegistryKey(self, keyPath, key, valueList):
        if type(valueList) is str:
            valueList = [valueList]
        valueStr =  ' '.join(["'%s'" % x for x in valueList])
        wmicmd = "%s registry setkey '%s' '%s' %s" % (self.baseCmd, keyPath,
                                                      key, valueStr)
        return self._wmiCall(wmicmd)

    def createRegistryKey(self, keyPath, key):
        wmicmd = "%s registry createkey '%s' '%s'" % (
            self.baseCmd, keyPath, key)
        return self._wmiCall(wmicmd)

    def runCmd(self, cmd):
        # WRITE ME
        wmicmd = "%s registry runcmd '%s'" % (self.baseCmd, cmd)
        return self._wmiCall(wmicmd)

    def checkProcess(self, pid):
        # WRITE ME
        wmicmd = "%s registry checkprocess '%s'" % (self.baseCmd, pid)
        return self._wmiCall(wmicmd)

    def mount(self):
        if not self._rootMounted:
            self._rootMounted = True
            self._rootDir = tempfile.mkdtemp()
            return self._rootDir, os.system(self.mountCmd + self._rootDir)


def getConaryClient():
    cfg = conarycfg.ConaryConfiguration()
    cfg.initializeFlavors()
    cfg.dbPath = ':memory:'

    # HACK, FIX ME!
    cfg.configLine('repositoryMap windemo.eng.rpath.com '
        'http://rbatrunk.eng.rpath.com/repos/windemo/')
    cfg.configLine('repositoryMap windows.rpath.com '
        'https://windows.eng.rpath.com/conary/')
    cfg.configLine('repositoryMap omni-components.eng.rpath.com '
        'http://rbatrunk.eng.rpath.com/repos/omni-components/')
    cfg.configLine('installLabelPath windows.rpath.com@rpath:windows-common')
    return conaryclient.ConaryClient(cfg = cfg)
