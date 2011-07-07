# Copyright (c) 2011 rPath, Inc.
# Assimilates different families of non-managed systems into an rBuilder
# used by rmake_plugins/assimilator_plugin.py

import exceptions
import os
import re

class LinuxAssimilator(object):
    """
    Assimiles a Linux system
    
    usage:
        sshConn = utils.SshConnection(...)
        asim = LinuxAssimilator(sshConn)
        asim.assimilate()
    """

    # paths that payloads are stored on the workers.  It is intended
    # that they'll be fetched from the rbuilders if not present
    # TODO: probably should be more like /var/lib/...
    EL5_PAYLOAD    = "/tmp/assimilator_EL5_payload.tar"
    EL6_PAYLOAD    = "/tmp/assimilator_EL6_payload.tar"
    SLES10_PAYLOAD = "/tmp/assimilator_SLES10_payload.tar"
    SLES11_PAYLOAD = "/tmp/assimilator_SLES11_payload.tar"

    def __init__(self, sshConnector):
        self.ssh       = sshConnector
        self.osFamily  = self._discoverFamily()
        self.payload   = self._payloadForFamily()

    def _discoverFamily(self):
        '''what kind of Linux OS is this?'''
        rc, output = self.ssh.execCommand('cat /etc/redhat-release')
        if rc == 0:
            return self._versionFromRedHatRelease(output)
        else:
            rc, output = self.ssh.execCommand('cat /etc/SuSE-release')
            if rc == 0:
                return self._versionFromSuseRelease(output)
            else:
                raise Exception("unable to detect OS family")

    def _versionFromRedHatRelease(self, output):
        '''Parse CentOS/RHEL version from /etc/redhat-release data'''
        matches = re.findall('\d+', output.split("\n")[0])
        return "EL%s" % matches[0]

    def _versionFromSuseRelease(self, output):
        '''Parse SuSE version from /etc/SuSE-release data'''
        matches = re.findall('\d+', output.split("\n")[0])
        return "SLES%s" % matches[0]
        
    def _payloadForFamily(self):
        ''' 
        Determine the appropriate payload file to transfer to the remote 
        system.  If the payload doesn't exist, we'll likely build it on 
        the worker.  Raises an error for unsupported payloads.
        '''
        PAYLOAD_MAP = dict(
            EL5    = LinuxAssimilator.EL5_PAYLOAD,
            EL6    = LinuxAssimilator.EL6_PAYLOAD,
            SLES10 = LinuxAssimilator.SLES10_PAYLOAD,
            SLES11 = LinuxAssimilator.SLES11_PAYLOAD,
        )
        payload = PAYLOAD_MAP.get(self.osFamily, None)
        if payload is None:
            raise Exception("no payload for family: " + self.osFamily)	
        self._preparePayloadIfNeeded(self.osFamily, payload)
        return payload

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
        commands.append("cd /tmp; tar -xf rpath_assimilator.tar")
        commands.append("cd /tmp/assimilator; " + \
            "sh /tmp/assimilator/bootstrap.sh %s" % node_list)
        return commands

    def _preparePayloadIfNeeded(self, family, payload):
        '''
        Decides whether to download the payload
        '''
        if not os.path.exists(payload):
            self._preparePayload(family, payload)

    def _preparePayload(self, family, payload):
       '''
       Retrieve the assimilation payload on the worker 
       (from the rbuilder) if it does not already exist.
       '''
       raise exceptions.NotImplementedError( \
           "worker can't download a payload yet`")

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
                raise Exception("Assimilator failed nonzero (%s, %s), " + 
                    "thus far=\n%s" % (cmd, rc, allOutput))

        # all commands successful
        self.ssh.close()
        return (0, allOutput)

