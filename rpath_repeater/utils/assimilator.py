# Copyright (c) 2011 rPath, Inc.
# Assimilates different families of non-managed systems into an rBuilder
# used by rmake_plugins/assimilator_plugin.py

import exceptions
import os
import re
import subprocess

class LinuxAssimilator(object):
    """
    Assimiles a Linux system
    
    usage:
        sshConn = utils.SshConnection(...)
        asim = LinuxAssimilator(sshConn)
        asim.assimilate()
    """

    def __init__(self, sshConnector):
        self.ssh       = sshConnector
        self.osFamily  = self._discoverFamily()
        self.builder   = LinuxAssimilatorBuilder(osFamily=self.osFamily)
        self.payload   = self._makePayload()

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
        
    def _commands(self, node_addrs):
        '''
        Once an assimilation payload has been deployed on a system
        we have to run some commands to take it the rest of the
        way with registration.  This is likely not family specific
        but it might be, if it IS family specific the preferred way
        of handling it is a different bootstrap.sh in the payload.
        '''
        node_list = ",".join(node_addrs)
        commands = []
        commands.append("cd /; tar -xf /tmp/rpath_assimilator.tar")
        commands.append("sh /tmp/bootstrap.sh %s" % node_list)
        return commands

    def runCmd(self, cmd):
       ''' 
       Run command via SSH, logging into buffer
       '''
       output = "\n(running) %s:\n" % cmd
       rc, cmdOutput = self.ssh.execCommand(cmd)
       output += "\n%s" % cmdOutput
       return rc, output

    def assimilate(self, node_addrs):
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
        
        commands  = self._commands(node_addrs)

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
     BOOTSTRAP_SH = '''
# rPath Linux assimilation bootstrap script
conary install conary sblim-sfcb-conary sblim-sfcb-schema-conary \
    sblim-cmpi-network-conary openslp-conary info-sfcb
rpath-register "$@"
'''

     def __init__(self, osFamily=None, forceRebuild=False):
          '''Does not build the payload, just gets parameters ready'''
          platDir = "-".join(osFamily)
          self.buildRoot = "/tmp/rpath_assimilate_%s_build" % platDir
          self.buildResult = "/tmp/rpath_assimilate_%s.tar" % platDir 
          self.groups = [
              "group-rpath-tools",
              "rpath-tools",
          ]
          self.rLabel = self._install_label(osFamily)
          self.cmdFlags = "--no-deps --no-restart --no-interactive"
          self.cmdRoot  = "--root %s" % self.buildRoot
          self.cmdLabel = "--install-label %s" % self.rLabel
          self.cmdGroups = " ".join(self.groups)
          self.forceRebuild = forceRebuild
     
     def _install_label(self, osFamily):
         '''Where do the conary packages come from?'''
         make, model = osFamily
         make = make.lower()
         model = model.lower()
         return "%s.rpath.com@rpath:%s-%s-common" % (make, make, model)

     def getAssimilator(self):
          '''Return the path to the assimilator, rebuilding if needed'''
          if self._isStaleOrMissing():
              self._buildTarball()
          return self.buildResult

     def _isStaleOrMissing(self):
          '''Does the assimilator need a rebuild?'''
          if not os.path.exists(self.buildResult):
              return 1
          if self.forceRebuild:
              os.unlink(self.buildResult)
              return 1
          # FIXME: add staleness detection code
          # here, which will delete the file & buildRoot
          return 0

     def _buildTarball(self):
          '''Builds assimilator on the worker node'''
          # create build directory
          try: 
              os.makedirs(self.buildRoot)
          except OSError:
              pass

          # run conary to extract contents inside build root
          cmd = "conary update %s %s %s %s" % (self.cmdFlags, self.cmdRoot, 
              self.cmdLabel, self.cmdGroups)
          # print "XDEBUG: cmd=%s" % cmd
          rc = subprocess.call(cmd, shell=True)
          if rc != 0:
              # no exception because return codes seem unreliable?
              print "conary failed: %s, %s" % (cmd, rc)

          # create bootstrap script in /tmp
          tmp = os.path.join(self.buildRoot, 'tmp')
          try:
              os.makedirs(tmp)
          except OSError:
              pass
          bootstrap = os.path.join(tmp, 'bootstrap.sh')
          # print "XDEBUG: bootstrap file=%s" % bootstrap
          bootstrapFh = open(bootstrap, 'w+')
          bootstrapFh.write(LinuxAssimilatorBuilder.BOOTSTRAP_SH)
          bootstrapFh.close()
 
          # tar up data and return the path
          working = os.getcwd()
          os.chdir(self.buildRoot)
          cmd = "tar cvf %s *" % (self.buildResult)
          # print "XDEBUG: cmd=%s" % cmd
          rc = subprocess.call(cmd, shell=True)
          if rc != 0:
              raise Exception("tar failed: %s" % cmd)
          if not os.path.exists(self.buildResult):
              raise Exception("creation failed")
          os.chdir(working)
          return self.buildResult

if __name__ == '__main__':
    # sample test build...
    # FIXME: tests need to use this same platform convention
    builder = LinuxAssimilatorBuilder(
        osFamily=['CentOS','5'],
        forceRebuild=True
    )
    print builder.getAssimilator()
  

