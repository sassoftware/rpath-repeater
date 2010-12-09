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

from xobj import xobj

#from conary.lib import log
from conary import conarycfg
from conary import conaryclient
from conary import versions
from conary.conaryclient import modelupdate, cml, cmdline
from conary.deps import deps
from conary.errors import TroveSpecsNotFound

from rpath_repeater.codes import Codes as C
from rpath_repeater.codes import WmiCodes as WC
from rpath_repeater.utils import base_forwarding_plugin as bfp
#log.setVerbosity(log.INFO)

REBOOT_TIMEOUT =  600 #  seconds
CRITICAL_PACKAGES = set(('rTIS:msi',))

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
        self.mountCmd = [ "/bin/mount", "-n", "-t", "cifs", "-o",
                          "user=%s" % user, "//%s/c$" % target ]
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
        rc, rtxt = self._wmiCall(wmicmd)
        if rc:
            raise bfp.WindowsServiceError(WC.errorMessage(rc, rtxt,
                               message='Failure to access windows service %s' % service,
                               params={'action':action }))
        return rc, rtxt

    def _wmiQueryRequest( self, action):
        wmicmd = self.baseCmd + ['query', action]
        rc, rtxt = self._wmiCall(wmicmd)
        if rc:
            raise bfp.WmiError(WC.errorMessage(rc, rtxt,
                               message='Failure to query target via WMI interface',
                               params={'action':action }))
        return rc, rtxt


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

    def waitForServiceToStop(self, service, statusCallback=None):
        rebootStartTime = 0
        while 1:
            key = r"SYSTEM\CurrentControlSet\Services\rPath Tools Install Service\Parameters"
            value = 'Running'
            rc, status = self.getRegistryKey(key, value, ignoreExceptions=True)
            status = status.strip()
            if not rc and status == "stopped":
                return
            elif not rc and status == "running":
                rebootStartTime = 0
            elif not rc and status == "rebooting" and not rebootStartTime:
                if statusCallback:
                    statusCallback(C.MSG_GENERIC, 'Waiting For Machine To Reboot')
                rebootStartTime = time.time()
            elif rebootStartTime and time.time() - rebootStartTime > REBOOT_TIMEOUT:
                raise bfp.GenericError('Unable to contact target system after reboot.')
            time.sleep(self.QuerySleepInterval)

    def getRegistryKey(self, key, value, ignoreExceptions=False):
        wmicmd = self.baseCmd + ["registry", "getkey", key, value]
        rc, results = self._wmiCall(wmicmd)
        if rc and not ignoreExceptions:
            raise bfp.RegistryAccessError(WC.errorMessage(rc, results,
                 params={'registry_key': key,
                         'registry_value': value,
                         'operation': 'read',
                         }))

        return rc, results

    def setRegistryKey(self, key, value, data):
        if not isinstance(data, list):
            data = [data]
        wmicmd = self.baseCmd + ["registry", "setkey", key, value] + data
        rc, results = self._wmiCall(wmicmd)
        if rc:
            raise bfp.RegistryAccessError(WC.errorMessage(rc, results,
                 params={'registry_key': key,
                         'registry_value': value,
                         'operation': 'write',
                         }))
        return rc, results

    def createRegistryKey(self, keyPath, key):
        wmicmd = self.baseCmd + ["registry", "createkey", keyPath, key]
        return self._wmiCall(wmicmd)

    def runCmd(self, cmd):
        wmicmd = self.baseCmd + ["process", "create", cmd]
        rc, rtxt = self._wmiCall(wmicmd)
        if rc:
            raise bfp.WmiError(WC.errorMessage(rc, rtxt,
                               message='Failed to remotely execute command on target',
                               params={'command':cmd }))
        return rc, rtxt

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
                raise bfp.CIFSMountError('Cannot mount remote filesystem')
            return rc, self._rootDir

    def _doMount(self):
        cmd = self.mountCmd + [ self._rootDir ]
        stdout = stderr = file("/dev/null", "w")
        # stdout = stderr = subprocess.PIPE
        p = subprocess.Popen(cmd, stdout=stdout, stderr=stderr,
            env=self.mountEnv)
        rc = p.wait()
        return rc

    def unmount(self):
        try:
            # unmount and delete the root file system
            self._doUnmount()
            os.rmdir(self._rootDir)
            self._rootDir = None
        except:
            pass

    def _doUnmount(self):
        os.system('/bin/umount -n ' + self._rootDir)

def getConaryClient(flavors = []):
    cfg = conarycfg.ConaryConfiguration()
    cfg.initializeFlavors()
    cfg.dbPath = ':memory:'
    cfg.flavor.extend(flavors)

    from socket import gethostname
    hostname = gethostname()
    cfg.readUrl('http://%s/conaryrc' % hostname)
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
    rc, rootDir = wc.mount()
    contentsPath = os.path.join(rootDir, 'Windows/Temp', f[1])
    winContentsPath = 'C:\\Windows\\Temp\\' + f[1]
    winLogPath = 'C:\\Windows\\Temp\\rPath_Tools_Install.log'
    open(contentsPath,'w').write(contents.f.read())
    cmd = r'msiexec.exe /i %s /quiet /l*vx %s' % \
        (winContentsPath, winLogPath)
    rc, rtxt = wc.runCmd(cmd)
    wc.waitForServiceToStop('rPath Tools Install Service')

    # record the installation to the manifest
    key, value = r"SOFTWARE\rPath\conary", "manifest"
    rc, currManifest = wc.getRegistryKey(key,value)
    currManifest = currManifest.split('\n')
    installedTs = '%s=%s[%s]' % (trv.name(), trv.version().freeze(),
                                 str(trv.flavor()))
    currManifest.append(installedTs)
    rc, rtxt = wc.setRegistryKey(key, value, [x for x in currManifest if x])
    return True

def processPackages(updateDir, files, contents=None, oldMsiDict=None,
                    critical=False, remove=False, seqNum=0):
    if not contents:
        contents = len(files) * [None]
    pkgList = []
    E = ElementMaker()
    for ((f, t),c) in zip(files, contents):
        if oldMsiDict and t.name() in oldMsiDict:
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

        # create the xml for this package
        pkgXml = E.package(
            E.type('msi'),
            E.sequence(str(seqNum)),
            E.productCode(t.troveInfo.capsule.msi.productCode()),
            E.productName(t.troveInfo.capsule.msi.name()),
            E.productVersion(t.troveInfo.capsule.msi.version()),
            E.file(f[1]),
            E.manifestEntry('%s=%s[%s]' % (t.name(), t.version().freeze(),
                                      str(t.flavor()))),
            )
        if remove:
            pkgXml.append(E.logFile('uninstall.log'))
            pkgXml.append(E.operation('uninstall'))
        else:
            pkgXml.append(E.logFile('install.log'))
            pkgXml.append(E.operation('install'))
            pkgXml.append(E.prevManifestEntry(oldManifestName))

        pkgXml.append(E.critical(str(critical).lower()))

        seqNum = seqNum + 1
        pkgList.append(pkgXml)

        if remove:
            continue

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

    return pkgList

def doUpdate(wc, sources, jobid, statusCallback):
    statusCallback(C.MSG_GENERIC, 'Waiting for previous job to complete')
    wc.waitForServiceToStop('rPath Tools Install Service', statusCallback)

    statusCallback(C.MSG_GENERIC, 'Retrieving the current system state')
    # fetch old sys model
    key, value = r"SOFTWARE\rPath\conary", "system_model"
    rc, oldModel = wc.getRegistryKey(key,value)
    oldModel = [l.strip() for l in oldModel.split('\n') if l]

    # fetch old msi manifest
    key, value = r"SOFTWARE\rPath\conary", "manifest"
    rc, currManifest = wc.getRegistryKey(key,value)
    currManifest = currManifest.split('\n')
    currManifestTups = [cmdline.parseTroveSpec(t) for t in currManifest if t]
    currManifestDict = dict([(t.name, t) for t in currManifestTups ])

    statusCallback(C.MSG_GENERIC, 'Mounting the filesystem')
    # mount the windows filesystem
    rc, rootDir = wc.mount()

    # Set the rtis root dir
    rtisDir = os.path.join(rootDir, r'Program Files/rPath/Updates')
    if not os.path.exists(rtisDir):
        os.makedirs(rtisDir)

    rtisWinDir = 'C:\\Program Files\\rPath\\Updates'
    rc, _ = wc.setRegistryKey(
        r"SYSTEM\CurrentControlSet\Services\rPath Tools Install Service\Parameters",
        'Root', rtisWinDir)

    statusCallback(C.MSG_GENERIC,
                   'Determining the packages that need to be upgraded')
    newTrvTups = [cmdline.parseTroveSpec(name) for name in sources if name]
    newModel = [str('install %s=%s'%(p[0],p[1])) for p in newTrvTups]

    client = getConaryClient(flavors = [newTrvTups[0][2]])
    cache = modelupdate.CMLTroveCache(client.getDatabase(),
                                              client.getRepos())

    # create jobs sets from old system model
    try:
        oldJobSets = runModel(client, cache, oldModel)
    except TroveSpecsNotFound, e:
        raise bfp.GenericError(
            r'This system is alread associated with an appliance (%s) which is not accessable.' % str(e.specList[0]))
    # use msi manifest to "correct" the state defined by the old model if needed
    additionalInstalls = []
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

    # add additional packages that we have installed but are not expressed by
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
    stdPkgs = []
    critPkgs = []
    for m in [x for x in newTroves if x[0].endswith(':msi')]:
        if m[0] in CRITICAL_PACKAGES:
            critPkgs.append(m)
        else:
            stdPkgs.append(m)
    oldPkgs = [x for x in oldTroves if x[0].endswith(':msi')]
    removePkgs = [x for x in removeTroves if x[0].endswith(':msi')]

    # fetch the old troves
    oldTrvs = client.repos.getTroves(oldPkgs, withFiles=False)
    oldMsiDict = dict(zip([x.name() for x in oldTrvs], oldTrvs))

    if critPkgs:
        statusCallback(C.MSG_GENERIC,
                       'Installing critical updates')
        winLogPath = 'C:\\Windows\\Temp\\'
        ctrvs = client.repos.getTroves(critPkgs, withFiles=True)
        critFilesToGet = []
        for t in ctrvs:
            critFilesToGet.append((list(t.iterFileList(capsules=True))[0], t))
        critContents = client.repos.getFileContents([(f[0][2],f[0][3])
                                                     for f in critFilesToGet],
                                                    compressed=False)
        for ((f, t),c) in zip(critFilesToGet, critContents):
            logName = t.name().split(':')[0]
            if oldMsiDict and t.name() in oldMsiDict:
                ot = oldMsiDict[t.name()]
                # skip the upgrade if we have the same msi
                if ot.troveInfo.capsule.msi.productCode() == \
                        t.troveInfo.capsule.msi.productCode():
                    continue
                else:
                    # remove the old version
                    cmd = r'msiexec.exe /uninstall %s /quiet /l*vx %s' % \
                        (ot.troveInfo.capsule.msi.productCode(),
                         winLogPath + logName + '_Uninstall.log')
                    rc, rtxt = wc.runCmd(cmd)
            # install the new version
            contentsPath = os.path.join(rootDir, 'Windows/Temp', f[1])
            winContentsPath = 'C:\\Windows\\Temp\\' + f[1]
            open(contentsPath,'w').write(c.f.read())
            cmd = r'msiexec.exe /i %s /quiet /l*vx %s' % \
                (winContentsPath, winLogPath + logName + '_Install.log')
            rc, rtxt = wc.runCmd(cmd)
            wc.waitForServiceToStop('rPath Tools Install Service')

    if stdPkgs or removePkgs:
        statusCallback(C.MSG_GENERIC,
                       'Fetching new packages from the repository')
        # fetch the new packages
        trvs = client.repos.getTroves(stdPkgs, withFiles=True)
        filesToGet = []
        for t in trvs:
            filesToGet.append((list(t.iterFileList(capsules=True))[0], t))
        contents = client.repos.getFileContents([(f[0][2],f[0][3])
                                                 for f in filesToGet],
                                                compressed=False)

        # fetch the packages to remove
        removeTrvs = client.repos.getTroves(removePkgs, withFiles=True)
        filesToRemove = []
        for t in removeTrvs:
            filesToRemove.append((list(t.iterFileList(capsules=True))[0], t))

        # Set the update dir
        updateBaseDir = 'job-%s' % jobid
        updateDir = os.path.join(rtisDir, updateBaseDir)

        statusCallback(C.MSG_GENERIC, 'Writing packages and install instructions')

        # write the files and installation instructions
        E = ElementMaker()

        stdPkgList = processPackages(updateDir, filesToGet, contents,
                                     oldMsiDict)
        rmPkgList = processPackages(updateDir, filesToRemove,
                                    remove=True, seqNum=len(stdPkgList))

        updateJobs = []
        currJob = 0
        if stdPkgList or rmPkgList:
            updateJobs.append(E.updateJob(
                    E.sequence(str(currJob)),
                    E.packages(*(stdPkgList + rmPkgList))
                    ))
            currJob = currJob + 1

        servicingXml = E.update(
            E.logFile('install.log'),
            E.updateJobs(*updateJobs))

        # write servicing.xml
        if not os.path.exists(updateDir):
            os.makedirs(updateDir)
        open(os.path.join(updateDir,'servicing.xml'),'w').write(
                etree.tostring(servicingXml,pretty_print=True))

        statusCallback(C.MSG_GENERIC,
                       'Waiting for the package installation(s) to finish')

        # set the registry keys
        commandValue = ["update=%s" % updateBaseDir]
        key = r"SYSTEM\CurrentControlSet\Services\rPath Tools Install Service\Parameters"
        value = 'Commands'
        rc, tb = wc.setRegistryKey(key, value, commandValue)

        # start the service
        rc, _ = wc.startService("rPath Tools Install Service")

        # wait until completed
        wc.waitForServiceToStop('rPath Tools Install Service', statusCallback)

        # verify that things installed correctly
        #x1.update.updateJobs.updateJob.packages.package.packageStatus.exitCode
        failedPkgs = {}
        notInstPkgs = []
        xml = xobj.parse(
            open(os.path.join(updateDir,'servicing.xml')).read())

        joblst = xml.update.updateJobs.updateJob
        if type(joblst) is not list:
            joblst = [joblst]
        for j in joblst:
            pkglst = j.packages.package
            if type(pkglst) is not list:
                pkglst = [pkglst]
            for p in pkglst:
                if str(p.packageStatus.status) != 'completed':
                    # operation failed, fetch the log
                    logPath = '%s/%s/%s' % (
                        updateDir, str(p.productCode), str(p.logFile))
                    n = "%s (%s)" % (str(p.productName), str(p.manifestEntry))
                    if os.path.exists(logPath):
                        failedPkgs[n] = open(logPath).read().decode('utf-16')
                    else:
                        notInstPkgs.append(n)

        if failedPkgs or notInstPkgs:
            msg = ['Some packages did not install\n\n']
            if failedPkgs:
                msg.append(
                    'These packages failed to install (see below for the complete log):\n' \
                        '%s\n' % ' '.join(failedPkgs.keys()))
                for p in failedPkgs:
                    msg.append('Log for %s' % p)
                    msg.append(failedPkgs[p])
            if notInstPkgs:
                msg = msg.append('No attempt was made to install these packages (likely because the installation was aborted due to the above failure(s):\n' \
                                     '%s\n\n' % ' '.join(notInstPkgs))
            raise bfp.GenericError('\n'.join(msg))

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

