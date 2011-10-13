# Copyright (c) 2011 rPath, Inc.
# Assimilates different families of non-managed systems into an rBuilder
# used by rmake_plugins/assimilator_plugin.py

import exceptions
import os
import re
import subprocess
import hashlib
import shutil
import tarfile
from conary.lib import util
from conary.conaryclient import ConaryClient
from conary.conarycfg import ConaryConfiguration
from conary.versions import Label
from conary.deps import deps
from rpath_repeater.codes import Codes as C

class LinuxAssimilator(object):
    """
    Assimiles a Linux system
    
    usage:
        sshConn = utils.SshConnection(...)
        asim = LinuxAssimilator(sshConnector=sshConn, caCert=str, 
            zoneAddresses=rus_hosts_with_ports)
        asim.assimilate()
    """

    def __init__(self, sshConnector=None, zoneAddresses=None, 
        caCert=None, status=None, platformLabels=None, 
        installTrove=None, projectLabel=None, eventUuid=-1):

        self._status               = status
        self.ssh                   = sshConnector
        self.platformLabels        = platformLabels
        self.osFamily, self.flavor = self._discoverFamilyAndFlavor()
        self.zoneAddresses         = zoneAddresses
        self.caCert                = caCert
        self.eventUuid             = eventUuid
        self.installTrove          = installTrove
        self.projectLabel          = projectLabel

        self.builder = LinuxAssimilatorBuilder(
            osFamily       = self.osFamily,
            caCert         = caCert,
            flavor         = self.flavor,
            status         = self.status,
            platformLabels = platformLabels
        )
        self.payload       = self._makePayload()

    def status(self, code, msg):
       '''Share a status message'''
       if self._status:
           self._status(code, msg)

    def _makePayload(self):
        '''what tarball to deploy? stubbed out for easier mock testing'''
        return self.builder.getAssimilator()

    def _discoverFamilyAndFlavor(self):
        '''what kind of Linux OS is this?'''
        self.status(C.MSG_GENERIC, 'detecting flavor')
        rc, output = self.ssh.execCommand('uname -a')
        flavor = 'x86'
        if output.find("x86_64") != -1:
            flavor = 'x86_64'
        self.status(C.MSG_GENERIC, 'detecting release')
        rc, output = self.ssh.execCommand('cat /etc/redhat-release')
        if rc == 0:
            if output.find("Red Hat") != -1:
                return (self._versionFromRedHatRelease(output), flavor)
            else:
                return (self._versionFromCentOSRelease(output), flavor)
        else:
            rc, output = self.ssh.execCommand('cat /etc/SuSE-release')
            if rc == 0:
                return (self._versionFromSuseRelease(output), flavor)
            else:
                raise Exception("unable to detect OS family")

    def _versionFromRedHatRelease(self, output):
        '''Parse RHEL version from /etc/redhat-release data'''
        matches = re.findall('\d+', output.split("\n")[0])
        return ('RHEL', matches[0])

    def _versionFromCentOSRelease(self, output):
        '''Parse CentOS version from /etc/redhat-release data'''
        matches = re.findall('\d+', output.split("\n")[0])
        return ('CentOS', matches[0])

    def _versionFromSuseRelease(self, output):
        '''Parse SuSE version from /etc/SuSE-release data'''
        matches = re.findall('\d+', output.split("\n")[0])
        return ('SLES', matches[0])
        
    def _commands(self):
        '''
        Once an assimilation payload has been deployed on a system
        we have to run some commands to take it the rest of the
        way with registration.  This is likely not family specific
        but it might be, if it IS family specific the preferred way
        of handling it is a different bootstrap.sh in the payload.
        '''
        commands = []
        addrs = " ".join(self.zoneAddresses)
        commands.append("cd /; tar -xf /tmp/rpath_assimilator.tar")
        script = "/usr/conary/bin/python /usr/share/bin/rpath_bootstrap.py"
        commands.append("%s %s %s %s %s" % (script, self.eventUuid, self.projectLabel, self.installTrove, addrs))
        return commands

    def runCmd(self, cmd):
        ''' 
        Run command via SSH, logging into buffer
        '''
        output = "\n%s\n" % cmd
        rc, cmdOutput = self.ssh.execCommand(cmd)
        output += "%s\n" % cmdOutput
        return rc, output

    def assimilate(self):
        '''
        An SSH connection has been passed in and we now know what payloads
        we want to deploy onto the system, and what post commands to run.
        Make it happen.  Returns (return_code, concatenated_output) as a 
        tuple. If there are no exceptions (and the tarball was correct) the
        system is now enslaved.   Node_addrs are rmake3 worker nodes.
        '''

        allOutput = ""
        # place the deploy tarball onto the system
        self.status(C.MSG_GENERIC, 'transferring archive')
        self.ssh.putFile(self.payload, "/tmp/rpath_assimilator.tar")
        self.status(C.MSG_GENERIC, 'assimilating system')
        
        commands  = self._commands()

        # run the series of assimilation commands
        for cmd in commands:
            rc, output = self.runCmd(cmd)
            allOutput += "\n%s" % output
            if rc != 0:
                self.ssh.close()
                return (rc, allOutput)

        # all commands successful
        self.ssh.close()
        return (0, allOutput)

class LinuxAssimilatorBuilder(object):
    """
    Returns the assimilator payload needed to assimilate a platform, creating
    if neccessary.

    Usage:
        lab = LinuxAssimilatorBuilder(['CentOS','5'])
        path = lab.getAssimilator()
    """
     

    # shell script to launch after extracting tarball
    # FIXME: zoneAddress parameters probably not passed in 
    # correctly as implemented right now
    BOOTSTRAP_SCRIPT = '''
#!/usr/conary/bin/python

# rPath Linux assimilation bootstrap script
# usage: rpath_bootstrap.py eventUuid worker:port worker:port ...

import subprocess
import sys
import os
import time
import logging
import socket

logger = logging.getLogger('bootstrap')
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s : %(message)s')
formatter2 = logging.Formatter('%(message)s')
handler = logging.FileHandler('/var/log/bootstrap.log')
handler2 = logging.StreamHandler()
logger.addHandler(handler)
logger.addHandler(handler2)
handler.setFormatter(formatter)
handler2.setFormatter(formatter2)

def runCmd(cmd, must_succeed=False):
    logger.info("(command): %s" % cmd)
    p = subprocess.Popen(cmd, shell=True, 
        stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, err = p.communicate()
    if out and out != "" and out !="\\n":
        logger.info(out)
    rc = p.returncode
    if rc != 0:
        if err and err !="" and err !="\\n":
            logger.error(err)   
        logger.error("command failed (rc=%s)" % rc)
        if must_succeed:
            sys.exit(rc)

logger.info("processing tag scripts")
runCmd("sh /var/spool/tmp/assimilate_tags.sh", must_succeed=True)

logger.info("params: %s" % " ... ".join(sys.argv))
logger.info("configuring conaryProxy")
proxyFile = open('/etc/conary/config.d/rpath-tools-conaryProxy','w')
# using the worker node address
server, port = sys.argv[4].split(":")
logger.info("destination: server=%s, port=%s" % (server, port))
proxyFile.write("conaryProxy https://%s\\n" % server)
proxyFile.close()

logger.info("updating packages")
cmd = "conary update conary sblim-sfcb-conary sblim-sfcb-schema-conary"
cmd = cmd + " sblim-cmpi-network-conary sblim-cmpi-base-conary"
cmd = cmd + " conary-cim cmpi-bindings-conary iconfig rpm:python"
cmd = cmd + " m2crypto-conary openslp-conary info-sfcb --no-deps"
runCmd(cmd)

logger.info("configuring zones")
try:
    os.makedirs("/etc/conary/rpath-tools/config.d")
except OSError:
    pass
fd = open("/etc/conary/rpath-tools/config.d/directMethod", "w+")
fd.write("directMethod []\\n")
projectLabel = sys.argv[2]
installTrove = sys.argv[3]
for zoneAddr in sys.argv[4:]:
    fd.write("directMethod %s\\n" % zoneAddr)
fd.close()

logger.info("adding project to search path")
data = file("/etc/conary/config.d/assimilator").read()
data = data.replace("installLabelPath", "installLabelPath %s " % projectLabel)
conary_cfg = file("/etc/conary/config.d/assimilator", "w")
conary_cfg.write(data)
conary_cfg.close()

logger.info("computing cert location")
cmd = 'openssl x509 -in /etc/conary/rpath-tools/certs/rbuilder-hg.pem  -noout -hash'
sslpath = os.popen(cmd).read().strip()
logger.info("linking certs")
cmd = "ln -s /etc/conary/rpath-tools/certs/rbuilder-hg.pem"
cmd = cmd + " /etc/conary/rpath-tools/certs/%s.0" % sslpath
runCmd(cmd)

logger.info("(re)starting CIM")
runCmd("service conary-cim start")
runCmd("service sfcb-conary restart")

logger.info("waiting for CIM to come online")
s = socket.socket()
socket_ok = False
time.sleep(5)
for x in range(0,10):
    try:
        s.connect(('127.0.0.1', 5989))
        socket_ok=True
    except Exception, e:
        time.sleep(3)
if not socket_ok:
    logger.error('CIM wait timeout exceeded, registration failure expected')
logger.info("CIM online")
time.sleep(5)

logger.info("registering")

if projectLabel != "None":
    runCmd("conary migrate %s=%s --replace-unmanaged-files" % (installTrove, projectLabel), must_succeed=True)
runCmd("rpath-register --event-uuid=%s" % sys.argv[1], must_succeed=True)

sys.exit(0)
'''

    def __init__(self, osFamily=None, caCert=None, 
        flavor='x86', status=None, forceRebuild=False, platformLabels=None):

        '''Does not build the payload, just gets parameters ready'''
        self._status = status    
        if osFamily is None:
           raise Exception("osFamily is required")
        if caCert is None:
           raise Exception("caCert is required")
        platDir = "-".join(osFamily)
        self.osFamily = osFamily
        self.flavor = flavor
        self.platformLabels = platformLabels
        self.buildRoot = "/tmp/rpath_assimilate_%s_%s_build" % (platDir, flavor)
        self.buildResult = "/tmp/rpath_assimilate_%s_%s.tar" % (platDir, flavor) 
        self.groups = [
            "rpath-models",
            "group-rpath-tools",
            "rpath-tools",
            "m2crypto-conary", # not in group-rpath-tools, this is a bug
            "pywbem-conary",
            "rpm:python", # for encapsulated packages
        ]
        self.caCert = caCert
        self.rLabels = self._install_labels(osFamily)
        self.forceRebuild = forceRebuild
        self.conaryClient = self._conaryClient()

    def status(self, code, msg):
       ''' Share a status message'''
       if self._status:
           self._status(code, msg)

    def _conaryClient(self):
        '''
        Return a conary client handle that will download packages
        into the buildroot, not resolving deps, and is otherwise
        appropriate for running on a rmake worker node.
        '''
        try:
            os.makedirs(self.buildRoot)
        except:
            # make extra sure it's here first (FIXME)
            pass

        conaryCfg = ConaryConfiguration(False)
        conaryCfg.flavor = [deps.parseFlavor("is: %s" % self.flavor)]
        conaryCfg.initializeFlavors()
        # conaryCfg.dbPath=':memory'
        conaryCfg.readUrl('http://localhost/conaryrc')
        conaryCfg.root = self.buildRoot
        conaryCfg.autoResolve = False
        conaryCfg.configLine('trustedKeys []')
        conaryCfg.configLine('trustThreshold 0')
         
        return ConaryClient(conaryCfg)

    def _install_labels(self, osFamily):
        '''Where do the conary packages come from?'''
        make, model = osFamily
        make = make.lower()
        model = model.lower()
        combined = "%s-%s" % (make, model)
        labels = self.platformLabels.get(combined, None)
        if labels is None:
           raise Exception("'pluginOption assimilator_plugin platformLabel %s <LABEL>' is not configured in rmake3 server config" % combined)
        return labels

    def getAssimilator(self):
        '''
        Return the path to the assimilator tarball for transferring
        to the remote client, rebuilding if needed.  Supports building
        for multiple flavors (saving as different filenames)
        '''
        self.status(C.MSG_GENERIC, 'determining archive applicability')
        (digestVersion, shouldRebuild, trovesNeeded) = self._makeRebuildDecision()
        if shouldRebuild:
            self._buildTarball(digestVersion, trovesNeeded)
        self.status(C.MSG_GENERIC, 'archive ready')
        return self.buildResult

    def _downloadConaryPackages(self, trovesNeeded):

        #os.mkdir(os.path.join(fsRoot, 'root'))
        #def localCB(msg):
        #    print msg
        util.mkdirChain(os.path.dirname(self.conaryClient.db.dbpath))
        #  self.conaryClient.setUpdateCallback(UpdateCallback(localCB))
        job = self.conaryClient.newUpdateJob()
        jobTups = [(n, (None, None), (v, f), True)
                for (n, v, f) in trovesNeeded]
        #print jobTups
        tagScriptPath=os.path.join(self.buildRoot, 'var/spool/tmp')
        util.mkdirChain(tagScriptPath)
        tagScriptFile=os.path.join(tagScriptPath, 'assimilate_tags.sh')
        self.conaryClient.prepareUpdateJob(job, jobTups, resolveDeps=False)
        self.conaryClient.applyUpdate(job, tagScript=tagScriptFile)
        self.conaryClient.close()

    def _lastDigestVersion(self):
        '''
        What digest version corresponds to the last time the tarball
        was built?  Returns None if unable to determine.  If this version
        does not match the current digest version we will rebuild
        the tarball.
        '''
        digestFn = os.path.join(self.buildRoot, 'var/spool/rpath/assimilator.digest')
        if not os.path.exists(digestFn):
            return None
        digestFile = open(digestFn)
        data = digestFile.read().strip()
        digestFile.close()
        return data

    def _makeRebuildDecision(self):
        '''
        Does the assimilator payload need a rebuild?  Returns a tuple
        of the current digest version and True/False answering that
        question.
        '''
        pm = PayloadCalculator(client=self.conaryClient, labels=self.rLabels, 
            troves=self.groups, flavor=self.flavor)
        digestVersion = pm.digestVersion()
        trovesNeeded  = pm.matched
        if self.forceRebuild or not os.path.exists(self.buildResult):
            return (digestVersion, True, trovesNeeded)
        lastDigestVersion = self._lastDigestVersion()
        if lastDigestVersion is None or lastDigestVersion != digestVersion:
            return (digestVersion, True, trovesNeeded)
        return (digestVersion, False, trovesNeeded)

    def _writeFileInBuildRoot(self, path, filename, contents):
        '''
        Create a file inside of the tarball build root
        Adding intermediate paths as needed.
        '''
        buildPath = os.path.join(self.buildRoot, path)
        util.mkdirChain(buildPath)
        filePath = os.path.join(buildPath, filename)
        handle = open(filePath, "w+") 
        handle.write(contents)
        handle.close()

    def _writeConfigFiles(self, digestVersion):
        '''
        Create config files for conary & registration

        rpath_bootstrap.sh -- the bootstrap script
        assimilator -- sets the install label path for the system

        TODO: add default to --no-deps
        '''
        self._writeFileInBuildRoot(
            'usr/share/bin', 'rpath_bootstrap.py',
            LinuxAssimilatorBuilder.BOOTSTRAP_SCRIPT,
        )
        assimConfig = ""
        labels = " ".join(self.rLabels)
        assimConfig = "installLabelPath %s\n" % labels
        assimConfig = assimConfig + "ignoreDependencies abi soname file" + \
             " trove userinfo groupinfo CIL java python perl ruby php rpm" + \
             " rpmlib\n"
        self._writeFileInBuildRoot(
            'etc/conary/config.d', 'assimilator',
            assimConfig
        )
        
        self._writeFileInBuildRoot(
            'etc/conary/rpath-tools/certs', 'rbuilder-hg.pem',
            self.caCert
        )
        self._writeFileInBuildRoot(
            'var/spool/rpath', 'assimilator.digest',
            digestVersion,
        )
 
    def _buildTarball(self, digestVersion, trovesNeeded):
        '''
        Builds assimilator tarball on the worker node
        '''

        # if buildroot already exists, unlink it
        # then create empty buildroot
        shutil.rmtree(self.buildRoot, ignore_errors=True)
        try: 
            os.makedirs(self.buildRoot)
        except OSError:
            pass

        self.status(C.MSG_GENERIC, 'preparing archive contents')
        self._downloadConaryPackages(trovesNeeded)

        self._writeConfigFiles(digestVersion)
        self.status(C.MSG_GENERIC, 'building archive')
        tar = tarfile.TarFile(self.buildResult, 'w')
        tar.add(self.buildRoot, '/')
        tar.close()
        return self.buildResult

class PayloadCalculator(object):
    '''
    Helps decides when the payload needs to rebuilt by computing
    a hash representing all of the packages contained within it.
    Plus the source code of the BOOTSTRAP_SCRIPT.  Zone addresses
    are not included as they are passed as arguments to the bootstrap
    script.
    '''

    def __init__(self, client=None, labels=None, troves=[], flavor='x86'):
        self.conaryClient = client
        self.troves       = troves
        for label in labels:
            print "my label = %s" % label
            lc = Label(label)

        self.labels       = [ Label(label) for label in labels ]
        self.flavor       = flavor # string 
        self.repos        = self.conaryClient.repos  
        self.matched      = self._allMatchingTroves()               

    def _allMatchingTroves(self):
        ''' 
        Returns matched troves (name, version, label) sorted by
        name (first priority) and then version (second).
        '''
        troves = [ (name, None, None) for name in self.troves ] 
        results = self.repos.findTroves(self.labels, troves,
            defaultFlavor=deps.parseFlavor("is: %s" % self.flavor))
        withVersions = [sorted(x)[-1] for x in results.values()]
        return withVersions

    def digestVersion(self):
        '''
        compute a hash of required trove names/versions identifying the
        current value of the set at this point in time
        '''
        longstr = ";".join(["%s=%s" % (m[0], m[1]) for m in self.matched])
        hasher = hashlib.sha512()
        hasher.update(longstr)
        hasher.update(LinuxAssimilatorBuilder.BOOTSTRAP_SCRIPT)
        return hasher.hexdigest()

# from conary.deps import deps; cfg.flavor = [deps.parseFlavor('is: x86')]; cfg.initializeFlavors(); ... etc
# add defaultFlavor=cfg.Flavor[0] to findTroves

if __name__ == '__main__':
 
    # sample test build of just the tarball ...
    builder = LinuxAssimilatorBuilder(
        osFamily=['CentOS','5'],
        caCert=file("/srv/rbuilder/pki/hg_ca.crt").read(),
        forceRebuild=True, # False
    )
    print builder.getAssimilator()
