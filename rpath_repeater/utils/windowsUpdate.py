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
import statvfs
import subprocess

from lxml import etree
from lxml.builder import ElementMaker

#from conary.lib import log
from conary import conarycfg
from conary import conaryclient
from conary import versions
from conary.conaryclient import modelupdate, cml, cmdline
from conary.deps import deps

from rpath_repeater.codes import Codes as C
from rpath_repeater.utils import base_forwarding_plugin as bfp
#log.setVerbosity(log.INFO)

def runModel(client, cache, modelText):
    model = cml.CML(client.cfg)
    model.parse(modelText)

    updJob = client.newUpdateJob()
    ts = client.cmlGraph(model)
    client._updateFromTroveSetGraph(updJob, ts, cache)
    return updJob.getJobs()

def modelsToJobs(cache, client, oldJobSets, newModel):
    newTroves = []
    for jobSet in oldJobSets:
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
    removeTroves = [ (x[0], x[1][0], x[1][1]) for x in
                     itertools.chain(*finalJobs) if x[2][0] is None ]

    return oldTroves, newTroves, removeTroves


class wmiClient(object):
    QuerySleepInterval = 5.0
    def __init__(self, target, domain, user, password):
        self.target = target
        self.domain = domain
        self.user = user
        self.password = password
        self.baseCmd = ['/usr/bin/wmic', '--host', target, '--user', user,
            '--password', password, '--domain', domain or target]

        # Older mount.cifs don't seem to support passing the user via an
        # environment variable
        self.mountCmd = [ "/bin/mount", "-t", "cifs", "-o", "user=%s" % user,
            "//%s/c$" % target ]
        self.mountEnv = dict(PASSWD=password)

        self._rootDir = None

    def _wmiCall(self, cmd):
        p = popen2.Popen3(cmd,True)
        rc = p.wait()

        if rc:
            return rc, p.childerr.read()
        return rc, p.fromchild.read()

    def _wmiServiceRequest( self, action, service):
        wmicmd = self.baseCmd + ['service', action, service]
        return self._wmiCall(wmicmd)

    def _wmiQueryRequest( self, action):
        wmicmd = self.baseCmd + ['query', action]
        return self._wmiCall(wmicmd)

    def startService(self, service):
        return self._wmiServiceRequest('start', service)

    def stopService(self, service):
        return self._wmiServiceRequest('stop', service)

    def queryService(self, service):
        return self._wmiServiceRequest('getstatus', service)

    def queryNetwork(self):
        return self._wmiQueryRequest('network')

    def queryUUID(self):
        return self._wmiQueryRequest('uuid')

    def waitForServiceToStop(self, service):
        # query the service until is is no longer active
        while 1:
            ret = self.queryService('rPath Tools Install Service')[1]
            if ret.strip() == 'Service Not Active':
                return
            time.sleep(self.QuerySleepInterval)

    def getRegistryKey(self, keyPath, key):
        wmicmd = self.baseCmd + ["registry", "getkey", keyPath, key]
        return self._wmiCall(wmicmd)

    def setRegistryKey(self, keyPath, key, valueList):
        if not isinstance(valueList, list):
            valueList = [valueList]
        wmicmd = self.baseCmd + ["registry", "setkey", keyPath, key] + valueList
        return self._wmiCall(wmicmd)

    def createRegistryKey(self, keyPath, key):
        wmicmd = self.baseCmd + ["registry", "createkey", keyPath, key]
        return self._wmiCall(wmicmd)

    def runCmd(self, cmd):
        wmicmd = self.baseCmd + ["process", "create", cmd]
        return self._wmiCall(wmicmd)

    def checkProcess(self, pid):
        # WRITE ME
        wmicmd = self.baseCmd + ["process", "status", pid]
        return self._wmiCall(wmicmd)

    def mount(self):
        if not self._rootDir:
            self._rootDir = tempfile.mkdtemp()
            rc = self._doMount()
            if rc != 0:
                os.rmdir(self._rootDir)
                self._rootDir = None
            return self._rootDir, rc

    def _doMount(self):
        cmd = self.mountCmd + [ self._rootDir ]
        stdout = stderr = file("/dev/null", "w")
        # stdout = stderr = subprocess.PIPE
        p = subprocess.Popen(cmd, stdout=stdout, stderr=stderr,
            env=self.mountEnv)
        rc = p.wait()
        return rc

    def unmount(self):
        # unmount and delete the root file system
        if self._rootDir:
            self._doUnmount()
            os.rmdir(self._rootDir)
            self._rootDir = None

    def _doUnmount(self):
        os.system('/bin/umount ' + self._rootDir)

def getConaryClient(flavors = []):
    cfg = conarycfg.ConaryConfiguration()
    cfg.initializeFlavors()
    cfg.dbPath = ':memory:'
    cfg.flavor.extend(flavors)

    # FIXME: this only will work when the repeater is running on the RBA
    from socket import gethostname
    hostname = gethostname()
    cfg.configLine('conaryProxy https://%s/' % hostname)
    cfg.configLine('repositoryMap windows.rpath.com https://windows.eng.rpath.com/conary/')
    return conaryclient.ConaryClient(cfg = cfg)

def doBootstrap(wc):

    client = getConaryClient()
    # fetch the rTIS MSI
    nvf = client.repos.findTrove(None, ('rTIS:msi',
            '/windows.rpath.com@rpath:windows-common',None))
    trv = client.repos.getTrove(*nvf[0])
    f = (list(trv.iterFileList(capsules=True)))[0]
    contents = client.repos.getFileContents(((f[2],f[3]),),
                                            compressed=False)
    contents = contents[0]
    # copy it to the target machine
    try:
        rootDir, rc = wc.mount()
        if rc:
            raise bfp.CIFSMountError('Cannot mount remote filesystem')
        contentsPath = os.path.join(rootDir, 'Windows/Temp', f[1])
        winContentsPath = 'C:\\Windows\\Temp\\' + f[1]
        winLogPath = 'C:\\Windows\\Temp\\rPath_Tools_Install.log'
        open(contentsPath,'w').write(contents.f.read())
        rc, _ = wc.runCmd(r'msiexec.exe /i %s /quiet /l*vx %s' %
                          (winContentsPath, winLogPath))
        if rc:
            return False
        wc.waitForServiceToStop('rPath Tools Install Service')
    finally:
        wc.unmount()
    return True


def doUpdate(wc, sources, jobid, statusCallback):
    statusCallback(C.MSG_GENERIC, 'Waiting for previous job to complete')
    wc.waitForServiceToStop('rPath Tools Install Service')

    statusCallback(C.MSG_GENERIC, 'Retrieving the current system state')
    # fetch old sys model
    key, value = r"SOFTWARE\rPath\conary", "system_model"
    rc, oldModel = wc.getRegistryKey(key,value)
    if rc:
        raise bfp.RegistryAccessError(
            'Cannot access registry key %s value %s.\n%s' %
            (key,value,oldModel))
    oldModel = [l.strip() for l in oldModel.split('\n') if l]

    # fetch old msi manifest
    key, value = r"SOFTWARE\rPath\conary", "manifest"
    rc, currManifest = wc.getRegistryKey(key,value)
    if rc:
        raise bfp.RegistryAccessError(
            'Cannot access registry key %s value %s.\n%s' %
            (key,value,oldModel))
    currManifest = currManifest.split('\n')
    currManifestTups = [cmdline.parseTroveSpec(t) for t in currManifest if t]
    currManifestDict = dict([(t.name, t) for t in currManifestTups ])

    statusCallback(C.MSG_GENERIC, 'Mounting the filesystem')
    # mount the windows filesystem
    rootDir, rc = wc.mount()
    if rc:
        raise bfp.CIFSMountError('Cannot mount remote filesystem')

    # Set the rtis root dir
    rtisDir = os.path.join(rootDir, r'Program Files/rPath/Updates')
    if not os.path.exists(rtisDir):
        os.makedirs(rtisDir)

    # FIXME: This is hardcoded for the moment until we work out wmiClient
    # limitations
    #rtisWinDir = 'C:\\Program Files\\rPath\\Updates'
    #rc, _ = wc.setRegistryKey(
    #    r"SYSTEM\CurrentControlSet\Services\rPath Tools Install Service\Parameters",
    #    'Root', rtisWinDir)
    #assert(not rc)

    statusCallback(C.MSG_GENERIC,
                   'Determining the packages that need to be upgraded')
    newTrvTups = [cmdline.parseTroveSpec(name) for name in sources if name]
    newModel = [str('install %s=%s'%(p[0],p[1])) for p in newTrvTups]

    client = getConaryClient(flavors = [newTrvTups[0][2]])
    cache = modelupdate.CMLTroveCache(client.getDatabase(),
                                              client.getRepos())
    # use msi manifest to "correct" the state defined by the old model if needed
    additionalInstalls = []
    oldJobSets = runModel(client, cache, oldModel)
    for job in oldJobSets:
        for t in job:
            currTrv = currManifestDict.pop(t[0], None)
            if currTrv:
                # we might have a different version in the manifest
                v = versions.ThawVersion(currTrv.version)
                f = deps.parseFlavor(currTrv.flavor)
                if v != t[2][0] or f != t[2][1]:
                    # this package is different than the intent of the model
                    additionalInstalls.append(str('install %s=%s' %
                                                  (t[0],str(v))))

    # add additional packages that we have install but are not expressed by
    # the model
    for ts in currManifestDict.values():
        additionalInstalls.append('install %s=%s' %
            (ts.name, str(versions.ThawVersion(ts.version))))

    if additionalInstalls:
        oldModel.extend(additionalInstalls)
        oldJobSets = runModel(client, cache, oldModel)

    # determine what new packages to install
    oldTroves, newTroves, removeTroves = modelsToJobs(cache, client,
                                                      oldJobSets, newModel)
    newMsiTroves = [x for x in newTroves if x[0].endswith(':msi')]
    oldMsiTroves = [x for x in oldTroves if x[0].endswith(':msi')]
    removeMsiTroves = [x for x in removeTroves if x[0].endswith(':msi')]

    if newMsiTroves or removeMsiTroves:
        statusCallback(C.MSG_GENERIC,
                       'Fetching new packages from the repository')
        # fetch the old troves
        oldTrvs = client.repos.getTroves(oldMsiTroves, withFiles=False)
        oldMsiDict = dict(zip([x.name() for x in oldTrvs], oldTrvs))

        # fetch the new packages
        trvs = client.repos.getTroves(newMsiTroves, withFiles=True)
        filesToGet = []
        for t in trvs:
            filesToGet.append((list(t.iterFileList(capsules=True))[0], t))
        contents = client.repos.getFileContents([(f[0][2],f[0][3])
                                                 for f in filesToGet],
                                                compressed=False)

        removeTrvs = client.repos.getTroves(removeMsiTroves, withFiles=False)

        # Set the update dir
        updateBaseDir = 'job-%s' % jobid
        updateDir = os.path.join(rtisDir, updateBaseDir)

        statusCallback(C.MSG_GENERIC, 'Writing packages and install instructions')
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
        removeTemplate = '''PACKAGE(
            TYPE('msi'),
            SEQUENCE('%s'),
            LOGFILE('uninstall.log'),
            OPERATION('uninstall'),
            PRODUCT_CODE("%s"),
            PRODUCT_NAME("%s"),
            PRODUCT_VERSION("%s"),
            MANIFEST("%s"),
            )'''
        pkgStr = ''
        seqNum = 0
        for ((f, t),c) in zip(filesToGet,contents):
            if t.name() in oldMsiDict:
                ot = oldMsiDict[t.name()]
                # skip the upgrade if we have the same msi
                if ot.troveInfo.capsule.msi.productCode() == \
                        t.troveInfo.capsule.msi.productCode():
                    continue
                oldManifestName = '%s=%s[%s]' % (ot.name(),
                                                 ot.version().freeze(),
                                                 str(ot.flavor()))
            else:
                oldManifestName = ''
            values = (str(seqNum),
                      t.troveInfo.capsule.msi.productCode(),
                      t.troveInfo.capsule.msi.name(),
                      t.troveInfo.capsule.msi.version(),
                      f[1],
                      '%s=%s[%s]' % (t.name(), t.version().freeze(),
                                     str(t.flavor())),
                      '%s' % oldManifestName)
            pkgStr = pkgStr + pkgTemplate % values + ',\n'

            # verify free space on the target drive
            packageDir = os.path.join(updateDir,
                                      t.troveInfo.capsule.msi.productCode())
            os.makedirs(packageDir)
            stat = os.statvfs(packageDir)
            fsSize = stat[statvfs.F_BFREE] * stat[statvfs.F_BSIZE]
            cSize = c.get().fileobj.size
            if (fsSize < cSize * 3):
                    raise bfp.GenericError(
                        r'Not enough space on the drive to install %s'
                        % t.troveInfo.capsule.msi.name())

            # write the contents
            contentsPath = os.path.join(packageDir,f[1])
            open(contentsPath,'w').write(c.f.read())

            seqNum = seqNum + 1

        # write remove instructions
        for s, t in enumerate(removeTrvs):
            values = (str(s + seqNum),
                      t.troveInfo.capsule.msi.productCode(),
                      t.troveInfo.capsule.msi.name(),
                      t.troveInfo.capsule.msi.version(),
                      '%s=%s[%s]' % (t.name(), t.version().freeze(),
                                     str(t.flavor())),)
            pkgStr = pkgStr + removeTemplate % values + ',\n'

        # write servicing.xml
        xmlDocStr = xmlDocStr % pkgStr
        xmlDoc = eval(xmlDocStr)
        open(os.path.join(updateDir,'servicing.xml'),'w').write(
                etree.tostring(xmlDoc,pretty_print=True))

        statusCallback(C.MSG_GENERIC,
                       'Waiting for the package installation(s) to finish')

        # set the registry keys
        commandValue = ["job=0", "update=%s" % updateBaseDir]
        key = r"SYSTEM\CurrentControlSet\Services\rPath Tools Install Service\Parameters"
        value = 'Commands'
        rc, tb = wc.setRegistryKey(key, value, commandValue)
        if rc:
            raise bfp.RegistryAccessError(
                'Cannot write to registry key %s value %s.\n%s' %
                (key,value,tb))

        # start the service
        rc, _ = wc.startService("rPath Tools Install Service")
        if rc:
            raise bfp.WindowsServiceError(
                'Cannot start rPath Tools Install Service')

        # wait until completed
        wc.waitForServiceToStop('rPath Tools Install Service')

    # TODO: Check for Errors

    statusCallback(C.MSG_GENERIC,
                   'Updating state information in the registry')
    # write the new system_model
    rc, _ = wc.setRegistryKey(r"SOFTWARE\rPath\conary",
                              "system_model", newModel)

    # write the new polling manifest
    pollManifest = []
    for t in newTrvTups:
        trv = client.repos.getTrove(t[0],versions.VersionFromString(t[1]),t[2])
        s = "%s=%s[%s]" % (trv.getName(), trv.getVersion().freeze(),
                           str(trv.getFlavor()))
        pollManifest.append(s)
    rc, _ = wc.setRegistryKey(r"SOFTWARE\rPath\conary",
                              "polling_manifest", pollManifest)

    # we're now done with the windows fs
    wc.unmount()

