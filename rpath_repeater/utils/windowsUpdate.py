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

from lxml import etree
from lxml.builder import ElementMaker

#from conary.lib import log
from conary import conarycfg
from conary import conaryclient
from conary.conaryclient import modelupdate, systemmodel

#log.setVerbosity(log.INFO)

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


def doUpdate(wc, sources):
    bootstrap=False

    client = getConaryClient()

    rc, status = wc.queryService('rPath Tools Install Service')
    if rc:
        bootstrap=True
    else:
        wc.waitForServiceToStop('rPath Tools Install Service')

        if not bootstrap:
            # fetch old manifest
            rc, oldManifest = wc.getRegistryKey(r"SOFTWARE\rPath\conary",
                                                "manifest")
            assert(not rc)

            oldManifest = oldManifest.split('\n')
            oldModel = ['install ' + p for p in oldManifest if p]
        else:
            oldModel = oldManifest = ''

    # mount the windows filesystem
    rootDir, rc = wc.mount()
    assert(not rc)

    # Set the rtis root dir
    rtisDir = os.path.join(rootDir,'Windows/RTIS')
    rtisWinDir = 'C:\\Windows\\RTIS'
    if not os.path.exists(rtisDir):
        os.mkdir(rtisDir)
    conaryManifestPath = os.path.join(rtisDir,'conary_manifest')
    if os.path.exists(conaryManifestPath):
        oldConaryManifest = open(conaryManifestPath).read()
        oldConaryManifest = oldConaryManifest.split('\n')
        oldModel = ['install ' + p for p in oldConaryManifest if p]
    else:
        oldConaryManifest = ''
    # determine the new packages to install
    cache = modelupdate.SystemModelTroveCache(
        client.getDatabase(), client.getRepos())
    newModel = ['install ' + s for s in sources if s]
    oldTroves, newTroves = modelsToJobs(cache, client, oldModel, newModel)
    newMsiTroves = [x for x in newTroves if x[0].endswith(':msi')]
    oldMsiTroves = [x for x in oldTroves if x[0].endswith(':msi')]
    oldMsiDict = dict(zip([x[0] for x in oldMsiTroves],[x[1:] for x
                                                        in oldMsiTroves]))

    # fetch the new packages
    trvs = client.repos.getTroves(newMsiTroves, withFiles=True)
    filesToGet = []
    for t in trvs:
        filesToGet.append((list(t.iterFileList(capsules=True))[0], t))

    contents = client.repos.getFileContents([(f[0][2],f[0][3])
                                             for f in filesToGet],
                                             compressed=False)

    # bootstrap if we need to
    if bootstrap:
        # fetch the rTIS MSI
        nvf = client.repos.findTrove(None, ('rtis',
            '/windows.rpath.com@rpath:windows-common',None))
        trv = client.repos.getTrove(*nvf[0])
        f = (list(trv.iterFileList(capsules=True)))[0]
        contents = client.repos.getFileContents(((f[2],f[3]),),
                                                compressed=False)
        contents = contents[0]
        # copy it to the target machine
        contentsPath = os.path.join(rtisDir,f[1])
        winContentsPath = 'C:\\Windows\\RTIS\\' + f[1]
        winLogPath = 'C:\\Windows\\RTIS\\' + 'rPath_Tools_Install.log'
        open(contentsPath,'w').write(contents.f.read())
        rc, _ = wc.runCmd(r'msiexec.exe /i %s /quiet /l*vx %s' %
                              (winContentsPath, winLogPath))
        assert(not rc)

    # Set the update dir
    updateDir = tempfile.mkdtemp('', 'update', rtisDir)
    updateBaseDir = os.path.basename(updateDir)

    # write the files and installation instructions
    E = ElementMaker()
    UPDATE = E.update
    SEQUENCE = E.sequence
    LOGFILE = E.logFile
    UPDATE_JOBS = E.updateJobs
    UPDATE_JOB = E.updateJob
    PACKAGES = E.packages
    PACKAGE = E.package
    TYPE = E.type
    OPERATION = E.operation
    PRODUCT_CODE = E.productCode
    PRODUCT_NAME = E.productName
    PRODUCT_VERSION = E.productVersion
    FILE = E.file
    MANIFEST = E.manifestEntry
    PREV_MANIFEST = E.previousManifestEntry

    xmlDocStr = '''UPDATE(
        LOGFILE('install.log'),
        UPDATE_JOBS(

            UPDATE_JOB(
                SEQUENCE('0'),
                PACKAGES(
                    %s
                    )
                )
            )
        )'''

    pkgTemplate = '''PACKAGE(
        TYPE('msi'),
        SEQUENCE('%s'),
        LOGFILE('install.log'),
        OPERATION('install'),
        PRODUCT_CODE("%s"),
        PRODUCT_NAME("%s"),
        PRODUCT_VERSION("%s"),
        FILE("%s"),
        MANIFEST("%s"),
        PREV_MANIFEST("%s")
        )'''
    pkgStr = ''
    for s, ((f, t),c) in enumerate(zip(filesToGet,contents)):
        if t.name() in oldMsiDict:
            o = oldMsiDict[t.name()]
            oldManifestName = '%s=%s[%s]' % (t.name(),str(o[0]),str(o[1]))
        else:
            oldManifestName = ''
        values = (str(s),
                  t.troveInfo.capsule.msi.productCode(),
                  t.troveInfo.capsule.msi.name(),
                  t.troveInfo.capsule.msi.version(),
                  f[1],
                  '%s=%s[%s]' % (t.name(),str(t.version()),str(t.flavor())),
                  '%s' % oldManifestName)
        pkgStr = pkgStr + pkgTemplate % values + ',\n'

        # write contents
        packageDir = os.path.join(updateDir,
                                  t.troveInfo.capsule.msi.productCode())
        os.mkdir(packageDir)
        contentsPath = os.path.join(packageDir,f[1])
        open(contentsPath,'w').write(c.f.read())

    # write servicing.xml
    xmlDocStr = xmlDocStr % pkgStr
    xmlDoc = eval(xmlDocStr)
    open(os.path.join(updateDir,'servicing.xml'),'w').write(
            etree.tostring(xmlDoc,pretty_print=True))

    # write the new conary_manifest
    conaryManifestPath = os.path.join(rtisDir,'conary_manifest')
    f = open(conaryManifestPath,'w').write(
        '\n'.join([ '%s=%s[%s]' %
          (x[0],str(x[1]),str(x[2])) for x in newTroves]))

    # we're now done with the windows fs
    wc.unmount()

    if bootstrap:
        wc.waitForServiceToStop('rPath Tools Install Service')

    # set the registry keys
    #rc, _ = uc.setRegistryKey(
    #    r"SYSTEM\CurrentControlSet\Services\rPath Tools Install Service\Parameters",
    #    'Root', rtisWinDir)
    #assert(not rc)
    commandValue = ["job=0", "update=%s" % updateBaseDir]
    rc, _ = wc.setRegistryKey(
        r"SYSTEM\CurrentControlSet\Services\rPath Tools Install Service\Parameters",
        'Commands', commandValue)
    assert(not rc)

    # start the service
    rc, _ = wc.startService("rPath Tools Install Service")
    assert(not rc)

    # wait until completed
    wc.waitForServiceToStop('rPath Tools Install Service')
