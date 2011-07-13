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
from conary.conaryclient import ConaryClient
from conary.conarycfg import ConaryConfiguration
from conary.versions import Label

class LinuxAssimilator(object):
    """
    Assimiles a Linux system
    
    usage:
        sshConn = utils.SshConnection(...)
        asim = LinuxAssimilator(sshConnector=sshConn, caCert=str, 
            zoneAddresses=rus_hosts_with_ports)
        asim.assimilate()
    """

    def __init__(self, sshConnector=None, zoneAddresses=None, caCert=None):
        self.ssh           = sshConnector
        self.osFamily      = self._discoverFamily()
        self.zoneAddresses = zoneAddresses
        self.caCert        = caCert
        self.builder = LinuxAssimilatorBuilder(
            osFamily = self.osFamily,
            caCert   = caCert
        )
        self.payload       = self._makePayload()

    def _makePayload(self):
        '''what tarball to deploy? stubbed out for easier mock testing'''
        return self.builder.getAssimilator()

    def _discoverFamily(self):
        '''what kind of Linux OS is this?'''
        rc, output = self.ssh.execCommand('cat /etc/redhat-release')
        if rc == 0:
            if output.find("Red Hat") != -1:
                return self._versionFromRedHatRelease(output)
            else:
                return self._versionFromCentOSRelease(output)
        else:
            rc, output = self.ssh.execCommand('cat /etc/SuSE-release')
            if rc == 0:
                return self._versionFromSuseRelease(output)
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
        commands.append("python /usr/share/bin/rpath_bootstrap.py %s" % addrs)
        return commands

    def runCmd(self, cmd):
       ''' 
       Run command via SSH, logging into buffer
       '''
       output = "\n(running) %s:\n" % cmd
       rc, cmdOutput = self.ssh.execCommand(cmd)
       output += "\n%s" % cmdOutput
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
        self.ssh.putFile(self.payload, "/tmp/rpath_assimilator.tar")
        
        commands  = self._commands()

        # run the series of assimilation commands
        for cmd in commands:
            rc, output = self.runCmd(cmd)
            allOutput += "\n%s" % output
            if rc != 0:
                raise Exception("Assimilator failed nonzero (%s, %s), thus far=%s"  
                    % (cmd, rc, allOutput))

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
#!/usr/bin/python
# rPath Linux assimilation bootstrap script

import subprocess
import sys
import os
import time

print "- updating packages"
cmd = "conary update conary sblim-sfcb-conary sblim-sfcb-schema-conary"
cmd = cmd + " sblim-cmpi-network-conary sblim-cmpi-base-conary"
cmd = cmd + " m2crypto-conary openslp-conary info-sfcb --no-deps"
print cmd
rc = subprocess.call(cmd, shell=True)

print "- configuring zones"
try:
    os.makedirs("/etc/conary/rpath-tools/config.d")
except OSError:
    pass
fd = open("/etc/conary/rpath-tools/config.d/directMethod", "w+")
fd.write("directMethod []")
for zoneAddr in sys.argv[1:]:
    fd.write("directMethod %s" % zoneAddr)
fd.close()

print "- computing cert location"
cmd = 'openssl x509 -in /etc/conary/rpath-tools/certs/rbuilder-hg.pem  -noout -hash'
sslpath = os.popen(cmd).read().strip()

print "- linking certs"
cmd = "ln -s /etc/conary/rpath-tools/certs/rbuilder-hg.pem"
cmd = cmd + " /etc/conary/rpath-tools/certs/%s.0" % sslpath
print cmd
rc = subprocess.call(cmd, shell=True)
if rc != 0:
   raise Exception("command failed: %s" % cmd)

print "- (re)starting CIM"
cmd = "/etc/rc.d/init.d/sfcb-conary restart"
print cmd
rc = subprocess.call(cmd, shell=True)

print "- waiting 5 seconds for CIM to come online"
time.sleep(5)

print "- registering"
cmd = "rpath-register"
rc = subprocess.call(cmd, shell=True)
print cmd
if not rc == 0:
   raise Exception("rpath-register failed")

sys.exit(0)
'''

     def __init__(self, osFamily=None, caCert=None, forceRebuild=False):
         '''Does not build the payload, just gets parameters ready'''
         if osFamily is None:
            raise Exception("osFamily is required")
         if caCert is None:
            raise Exception("caCert is required")
         platDir = "-".join(osFamily)
         self.buildRoot = "/tmp/rpath_assimilate_%s_build" % platDir
         self.buildResult = "/tmp/rpath_assimilate_%s.tar" % platDir 
         self.groups = [
             "rpath-models",
             "group-rpath-tools",
             "rpath-tools",
             "m2crypto-conary", # not in group-rpath-tools, this is a bug
             "pywbem-conary" # looks like we need this too?
         ]
         self.caCert = caCert
         self.rLabel = self._install_label(osFamily)
         self.cmdFlags = "--no-deps --no-restart --no-interactive"
         self.cmdRoot  = "--root %s" % self.buildRoot
         self.cmdLabel = "--install-label %s" % self.rLabel
         self.cmdGroups = " ".join(self.groups)
         self.forceRebuild = forceRebuild
         self.config = "--config \"includeConfigFile http://localhost/conaryrc\""

     def _install_label(self, osFamily):
         '''Where do the conary packages come from?'''
         make, model = osFamily
         make = make.lower()
         model = model.lower()
         return "%s.rpath.com@rpath:%s-%s-common" % (make, make, model)

     def getAssimilator(self):
          '''
          Return the path to the assimilator tarball for transferring
          to the remote client, rebuilding if needed.  Supports building
          for multiple flavors (saving as different filenames)
          '''
          (digestVersion, should_rebuild) = self._makeRebuildDecision()
          if should_rebuild:
              self._buildTarball(digestVersion)
          return self.buildResult

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
          pm = PayloadMath(label=self.rLabel, troves=self.groups)
          digestVersion = pm.digestVersion()
          if self.forceRebuild or not os.path.exists(self.buildResult):
              return (digestVersion, True)
          lastDigestVersion = self._lastDigestVersion()
          if lastDigestVersion is None or lastDigestVersion != digestVersion:
              return (digestVersion, True)
          return (digestVersion, False)

     def _writeFileInBuildRoot(self, path, filename, contents):
          '''
          Create a file inside of the tarball build root
          Adding intermediate paths as needed.
          '''
          buildPath = os.path.join(self.buildRoot, path)
          try:
              os.makedirs(buildPath)
          except OSError:
              pass
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
          TODO: write CIM cert
          '''
          self._writeFileInBuildRoot(
              'usr/share/bin', 'rpath_bootstrap.py',
              LinuxAssimilatorBuilder.BOOTSTRAP_SCRIPT,
          )
          self._writeFileInBuildRoot(
              'etc/conary/config.d', 'assimilator',
              "installLabelPath %s\n" % self.rLabel
          )
          self._writeFileInBuildRoot(
              'etc/conary/rpath-tools/certs', 'rbuilder-hg.pem',
              self.caCert
          )
          self._writeFileInBuildRoot(
               'var/spool/rpath', 'assimilator.digest',
               digestVersion,
          )

     def _buildTarball(self, digestVersion):
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

          # run conary to extract contents inside build root
          # conary will return failure on no-updates so we'll ignore
          # the return code.
          cmd = "conary update %s %s %s %s %s" % (self.cmdFlags, self.cmdRoot, 
              self.cmdLabel, self.cmdGroups, self.config)
          print "XDEBUG: cmd=%s\n" % cmd 
          subprocess.call(cmd, shell=True)

          self._writeConfigFiles(digestVersion)
          tar = tarfile.TarFile(self.buildResult, 'w')
          tar.add(self.buildRoot, '/')
          return self.buildResult

class PayloadMath(object):
    '''
    Helps decides when the payload needs to rebuilt by computing
    a hash representing all of the packages contained within it.
    Plus the source code of the BOOTSTRAP_SCRIPT.  Zone addresses
    are not included as they are passed as arguments to the bootstrap
    script.
    '''

    def __init__(self, label=None, troves=[]):
        self.conaryCfg = ConaryConfiguration(False)
        self.conaryCfg.initializeFlavors()
        self.conaryCfg.dbPath = ':memory:'
        self.conaryCfg.readUrl('http://localhost/conaryrc')
        self.conaryClient = ConaryClient(self.conaryCfg)
        self.troves       = troves
        self.label        = Label(label)
        self.repos        = self.conaryClient.repos  
        self.matched      = self._allMatchingTroves()               

    def _allMatchingTroves(self):
        ''' 
        Returns matched troves (name, version, label) sorted by
        name (first priority) and then version (second).
        '''
        results = []
        for name in self.troves:
            matches = self.repos.findTrove(self.label, (name, None, None))
            results.extend(matches)        
        results.sort(key= lambda x: x[1])
        results.sort(key = lambda x: x[0])
        return results

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

if __name__ == '__main__':
 
    # sample test build of just the tarball ...
    builder = LinuxAssimilatorBuilder(
        osFamily=['CentOS','5'],
        caCert=file("/srv/rbuilder/pki/hg_ca.crt").read(),
        forceRebuild=True, # False
    )
    print builder.getAssimilator()
