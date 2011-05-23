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
import socket

from cStringIO import StringIO

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
        self.target = socket.getaddrinfo(target,None)[0][4][0]
        self.domain = domain
        self.user = user
        self.password = password
        self.baseCmd = ['/usr/bin/wmic', '--host', self.target, '--user',
                        self.user, '--password', self.password, '--domain',
                        self.domain or self.target]

        # Older mount.cifs don't seem to support passing the user via an
        # environment variable
        self.mountCmd = [ "/bin/mount", "-n", "-t", "cifs", "-o",
                          "user=%s" % self.user, "//%s/c$" % self.target ]
        self.mountEnv = dict(PASSWD=self.password)

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
                               params={'cmd':wmicmd }))
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

    def waitForServiceToStop(self, service, statusCallback=None,
                             allowReboot=True):
        rebootStartTime = 0
        serviceQueries = 0
        serviceQueryRetries = 3
        while 1:
            key = r"SYSTEM\CurrentControlSet\Services\rPath Tools Install Service\Parameters"
            value = 'Running'
            try:
                serviceQueries = serviceQueries+1
                rc, status = self.getRegistryKey(key, value)
                status = status.strip()
            except:
                if serviceQueries <= serviceQueryRetries:
                    # things can be a bit strange when the service is first
                    # installed so we eat the first few exceptions and let
                    # things settle
                    pass
                elif rebootStartTime:
                    # we're in the middle of a reboot
                    if time.time() - rebootStartTime > REBOOT_TIMEOUT:
                        raise bfp.GenericError(
                            'Unable to contact target system after reboot.')
                elif allowReboot:
                    # we might have just started a reboot
                    rebootStartTime = time.time()
                    self.unmount()
                    if statusCallback:
                        statusCallback(C.MSG_GENERIC,
                                       'Waiting for target system')
                else:
                    raise
                time.sleep(self.QuerySleepInterval)
                continue

            if status == "stopped":
                return
            elif status == "running":
                rebootStartTime = 0
            elif status == "rebooting":
                rebootStartTime = 0
                if statusCallback:
                    statusCallback(C.MSG_GENERIC,
                                   'Reboot complete. '
                                   'Waiting for installation to finish.')
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
        if not data:
            data = ['']
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
        rc = 0
        if not self._rootDir:
            self._rootDir = tempfile.mkdtemp()
            rc = self._doMount()
            if rc != 0:
                os.rmdir(self._rootDir)
                self._rootDir = None
                raise bfp.CIFSMountError(
                    'Cannot mount remote filesystem\ncommand: %s' %
                    self.mountCmd + [self._rootDir] )
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
        os.system('/bin/umount ' + self._rootDir)

    def getWinPath(self, *paths):
        if not self._rootDir:
            self.mount()
        return os.path.join(self._rootDir, *paths)

    def getManifest(self):
        key, value = r"SOFTWARE\rPath\conary", "manifest"
        rc, manifest = self.getRegistryKey(key,value)
        manifest = manifest.split('\n')
        manifestTups = [cmdline.parseTroveSpec(t) for t in manifest if t]
        return dict([(t.name, t) for t in manifestTups ])

    def setManifest(self, manifestDict):
        key, value = r"SOFTWARE\rPath\conary", "manifest"
        manifest = ['%s=%s[%s]' % (x[0], x[1], str(x[2]))
                    for x in manifestDict.values()]
        manifest.sort()
        return self.setRegistryKey(key,value, manifest)

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

    keyPath = r'SYSTEM\CurrentControlSet\Control\Session Manager\Environment'
    key = 'PROCESSOR_ARCHITECTURE'
    rc, value = wc.getRegistryKey(keyPath, key)
    value = value.strip()

    if value == 'AMD64':
       flavor = deps.parseFlavor('is: x86_64')
    else:
       flavor = deps.parseFlavor('is: x86')

    # fetch the rTIS MSI
    nvf = client.repos.findTrove(None, ('rTIS:msi',
            '/windows.rpath.com@rpath:windows-common', flavor))
    trv = client.repos.getTrove(*nvf[0])
    f = (list(trv.iterFileList(capsules=True)))[0]
    contents = client.repos.getFileContents(((f[2],f[3]),),
                                            compressed=False)
    contents = contents[0]

    # copy it to the target machine
    contentsPath = wc.getWinPath('Windows/Temp', f[1])
    winContentsPath = 'C:\\Windows\\Temp\\' + f[1]
    winLogPath = 'C:\\Windows\\Temp\\rTIS_Install.log'
    open(contentsPath,'w').write(contents.f.read())
    cmd = r'msiexec.exe /i %s /quiet /l*vx %s' % \
        (winContentsPath, winLogPath)
    rc, rtxt = wc.runCmd(cmd)
    wc.waitForServiceToStop('rPath Tools Install Service', allowReboot=False)

    # record the installation to the system model
    key, value = r"SOFTWARE\rPath\conary", "system_model"
    rc, currModel = wc.getRegistryKey(key,value)
    currModel = currModel.split('\n')
    installedTs = 'install %s=%s' % (trv.name(), str(trv.version()))
    currModel.append(installedTs)
    rc, rtxt = wc.setRegistryKey(key, value, [x for x in currModel if x])

    # record the installation to the manifest
    key, value = r"SOFTWARE\rPath\conary", "manifest"
    rc, currManifest = wc.getRegistryKey(key,value)
    currManifest = currManifest.split('\n')
    currManifest.sort()
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
        if hasattr(t.troveInfo.capsule.msi,'msiArgs'):
            pkgXml.append(E.msiArguments(t.troveInfo.capsule.msi.msiArgs()))

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
    wc.waitForServiceToStop('rPath Tools Install Service', statusCallback,
                            allowReboot=False)

    statusCallback(C.MSG_GENERIC, 'Retrieving the current system state')

    # fetch old sys model
    key, value = r"SOFTWARE\rPath\conary", "system_model"
    rc, oldModel = wc.getRegistryKey(key,value)
    oldModel = [l.strip() for l in oldModel.split('\n') if l]

    # fetch old msi manifest
    currManifestDict = wc.getManifest()

    statusCallback(C.MSG_GENERIC, 'Mounting the filesystem')

    # Set the rtis root dir
    rtisDirBase = r'Program Files/rPath/Updates'
    if not os.path.exists(wc.getWinPath(rtisDirBase)):
        os.makedirs(wc.getWinPath(rtisDirBase))

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

    newPollingManifest = []
    for t in newTrvTups:
        trv = cache.getTrove(t[0],versions.VersionFromString(t[1]),t[2],
                                    withFiles=False)
        s = "%s=%s[%s]" % (trv.getName(), trv.getVersion().freeze(),
                           str(trv.getFlavor()))
        newPollingManifest.append(s)

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
    removePkgs = [x for x in removeTroves if x[0].endswith(':msi') and
                  x[0] not in CRITICAL_PACKAGES]

    # fetch the old troves
    oldTrvs = cache.getTroves(oldPkgs, withFiles=False)
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
            ot = None
            if oldMsiDict and t.name() in oldMsiDict:
                ot = oldMsiDict[t.name()]
                manifestDict = wc.getManifest()

                # remove the old version
                del manifestDict[t.name()]
                if ot.troveInfo.capsule.msi.productCode() != \
                        t.troveInfo.capsule.msi.productCode():
                    cmd = r'msiexec.exe /uninstall %s /quiet /l*vx %s' % \
                        (ot.troveInfo.capsule.msi.productCode(), \
                             winLogPath + logName + '_Uninstall.log')
                    rc, rtxt = wc.runCmd(cmd)
                # update the manifest
                wc.setManifest(manifestDict)

            # install the new version
            manifestDict = wc.getManifest()
            manifestDict[t.name()] = (t.name(), t.version().freeze(),
                                      t.flavor())
            if not ot or ot.troveInfo.capsule.msi.productCode() != \
                    t.troveInfo.capsule.msi.productCode():
                contentsPath = wc.getWinPath('Windows/Temp',
                                            f[1])
                winContentsPath = 'C:\\Windows\\Temp\\' + f[1]
                open(contentsPath,'w').write(c.f.read())
                cmd = r'msiexec.exe /i %s /quiet /l*vx %s' % \
                    (winContentsPath, winLogPath + logName + '_Install.log')
                rc, rtxt = wc.runCmd(cmd)
            # update the manifest
            wc.setManifest(manifestDict)

            wc.waitForServiceToStop('rPath Tools Install Service', allowReboot=False)

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
        manifestDict = wc.getManifest()
        for t in removeTrvs:
            name = t.name()

            if name not in manifestDict:
                continue

            version = versions.ThawVersion(manifestDict[name].version)
            flavor = manifestDict[name].flavor

            # we only remove it if it is installed
            if version == t.version() and flavor == t.flavor():
                filesToRemove.append(
                    (list(t.iterFileList(capsules=True))[0], t))

        # Set the update dir
        updateBase = 'job-%s' % jobid
        updateDirBase = os.path.join(rtisDirBase, updateBase)

        statusCallback(C.MSG_GENERIC, 'Writing packages and install instructions')

        # write the files and installation instructions
        E = ElementMaker()

        stdPkgList = processPackages(wc.getWinPath(updateDirBase),
                                     filesToGet, contents,
                                     oldMsiDict)
        rmPkgList = processPackages(wc.getWinPath(updateDirBase),
                                    filesToRemove,
                                    remove=True, seqNum=len(stdPkgList))

        updateJobs = []
        currJob = 0
        if stdPkgList or rmPkgList:
            updateJobs.append(E.updateJob(
                    E.sequence(str(currJob)),
                    E.logFile('install.log'),
                    E.packages(*(stdPkgList + rmPkgList))
                    ))
            currJob = currJob + 1

        servicingXml = E.update(
            E.logFile('install.log'),
            E.systemModel('\n'.join(newModel)),
            E.pollingManifest('\n'.join(newPollingManifest)),
            E.updateJobs(*updateJobs))

        # write servicing.xml
        if not os.path.exists(wc.getWinPath(updateDirBase)):
            os.makedirs(wc.getWinPath(updateDirBase))
        open(wc.getWinPath(updateDirBase,'servicing.xml'),'w').write(
                etree.tostring(servicingXml,pretty_print=True))

        statusCallback(C.MSG_GENERIC,
                       'Waiting for the package installation(s) to finish')

        # set the registry keys
        commandValue = ["update=%s" % updateBase]
        key = r"SYSTEM\CurrentControlSet\Services\rPath Tools Install Service\Parameters"
        value = 'Commands'
        rc, tb = wc.setRegistryKey(key, value, commandValue)

        # start the service
        rc, _ = wc.startService("rPath Tools Install Service")

        # wait until completed
        wc.waitForServiceToStop('rPath Tools Install Service', statusCallback,
                                allowReboot=True)

        # verify that things installed correctly
        #x1.update.updateJobs.updateJob.packages.package.packageStatus.exitCode
        failedPkgs = {}
        notInstPkgs = []
        xml = xobj.parse(
            open(wc.getWinPath(updateDirBase,'servicing.xml')).read())

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
                    logPath = wc.getWinPath(updateDirBase, str(p.productCode),
                                           str(p.logFile))
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

    # The following is for backwards compatability and can be removed in
    # the future.
    rc, _ = wc.setRegistryKey(r"SOFTWARE\rPath\conary",
                              "system_model", newModel)
    rc, _ = wc.setRegistryKey(r"SOFTWARE\rPath\conary",
                              "polling_manifest", newPollingManifest)

def doConfiguration(wc, values, jobid, statusCallback):
    def status(msg):
        statusCallback(C.MSG_GENERIC, msg)

    # Wait for any previous install or config jobs to complete.
    wc.waitForServiceToStop('rPath Tools Install Service', statusCallback,
                            allowReboot=False)

    status('Mounting the filesystem')

    # Set the rtis root dir
    rtisDirBase = r'Program Files/rPath/Updates'
    if not os.path.exists(wc.getWinPath(rtisDirBase)):
        os.makedirs(wc.getWinPath(rtisDirBase))

    rtisWinDir = 'C:\\Program Files\\rPath\\Updates'
    rc, _ = wc.setRegistryKey(
        r"SYSTEM\CurrentControlSet\Services\rPath Tools Install Service\Parameters",
        'Root', rtisWinDir)

    status('Preparing job configuration data')

    # Set the update dir
    updateBase = 'job-%s' % jobid
    updateDirBase = os.path.join(rtisDirBase, updateBase)
    updateDir = wc.getWinPath(updateDirBase)

    e = ElementMaker()

    # Parse values into elements
    valuesElement = etree.parse(StringIO(values)).getroot()

    currJob = 0
    updateJobs = []
    updateJobs.append(
        e.configJob(e.sequence(str(currJob)),
                    e.logFile('setup.log'),
                    e.values(*[ x for x in valuesElement.iterchildren()
                                if x.tag != 'id' ]),
        )
    )

    servicing = e.update(e.logFile('setup.log'), e.updateJobs(*updateJobs))

    # write servicing.xml
    if not os.path.exists(updateDir):
        os.makedirs(updateDir)
    servicingFn = os.path.join(updateDir, 'servicing.xml')
    open(servicingFn,'w').write(etree.tostring(servicing, pretty_print=True))

    status('Applying Configuration')

    # set the registry keys
    commandValue = ["update=%s" % updateBase]
    key = r"SYSTEM\CurrentControlSet\Services\rPath Tools Install Service\Parameters"
    value = 'Commands'
    rc, tb = wc.setRegistryKey(key, value, commandValue)

    # start the service
    rc, _ = wc.startService("rPath Tools Install Service")

    # wait until completed
    wc.waitForServiceToStop('rPath Tools Install Service', statusCallback,
                            allowReboot=True)

    status('Gathering Results')

    # Get the modified servicing.xml from the machine.
    results = []
    xml = xobj.parse(open(servicingFn).read())
    updateJobs = xml.update.updateJobs

    # extract configJob and updateJob information
    updateJob = []
    if hasattr(updateJobs, 'updateJob'):
        updateJob = updateJobs.updateJob
        if not isinstance(updateJob, list):
            updateJob = [ updateJob, ]

    configJob = []
    if hasattr(updateJobs, 'configJob'):
        configJob = updateJobs.configJob
        if not isinstance(configJob, list):
            configJob = [ configJob, ]

    jobs = [ z for y, z in sorted((int(x.sequence), x)
        for x in itertools.chain(updateJob, configJob)) ]

    # extract handler results
    for job in jobs:
        handlers = []
        if hasattr(job, 'handlers') and hasattr(job.handlers, 'handler'):
            handlers = job.handlers.handler
            if not isinstance(handlers, list):
                handlers = [ handlers, ]

        for hdlr in handlers:
            results.append((int(hdlr.exitCode), hdlr.name,
                            hdlr.exitCodeDescription))

    return results
