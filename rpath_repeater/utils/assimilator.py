# Copyright (c) 2011 rPath, Inc.
# Assimilates different flavors of non-managed systems into an rBuilder
# used by rmake_plugins/assimilator_plugin.py

import exceptions
import os

class LinuxAssimilator(object):
    """
    Assimiles a Linux system
    
    ssh_conn = utils.SshConnection(...)
    flavor = 'RHEL5' # FIXME: constant

    usage:  asim = LinuxAssimilator(ssh_conn, flavor)
            asim.assimilate()
    """

    # paths that payloads are stored on the workers.  It is intended
    # that the system will create them here if they do not exist
    # (see prepare_payload function) by triggering a build from this
    # module?   Probably should be more like /var/lib/...
    EL5_PAYLOAD    = "/tmp/assimilator_EL5_payload.tar"
    EL6_PAYLOAD    = "/tmp/assimilator_EL6_payload.tar"
    SLES10_PAYLOAD = "/tmp/assimilator_SLES10_payload.tar"
    SLES11_PAYLOAD = "/tmp/assimilator_SLES11_payload.tar"

    def __init__(self, ssh_connector, flavor):
        self.ssh       = ssh_connector
        self.flavor    = flavor
        self.payload   = self._payload_for_flavor()
        self.commands  = self._commands_for_flavor()

    def _payload_for_flavor(self):
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
	payload = PAYLOAD_MAP.get(self.flavor, None)
	if payload is None:
	    raise Exception("no payload for flavor: " + self.flavor)	
	self._prepare_payload_if_needed(self.flavor, payload)
	return payload

    def _commands_for_flavor(self):
	'''
	Once an assimilation payload has been deployed on a system
	we have to run some commands to take it the rest of the
	way with registration.  This is likely not flavor specific
	but it might be, if it IS flavor specific the preferred way
	of handling it is a different bootstrap.sh in the payload.
	'''
	commands = []
        commands.append("cd /tmp; tar -xvf rpath_assimilator.tar")
	commands.append("cd /tmp/assimilator; " +
	     "sh /tmp/assimilator/bootstrap.sh")
	return commands

    def _prepare_payload_if_needed(self, flavor, payload):
        '''
	If the payload file does not exist yet on the worker,
	we may build it.  QUESTION: what if we need to build a new 
	payload version?  should we rebuild every so often based on dates?
        '''
	if not os.path.exists(payload):
            self._prepare_payload(flavor, payload)
            if not os.path.exists(payload):
                raise Exception('payload preparation failed')

    def _prepare_payload(self, flavor, payload):
	'''
	Build the assimilation payload on the worker if it does not 
	already exist.
	'''
        raise exceptions.NotImplementedError("worker can't build a payload yet`")

    def run_cmd(self, cmd):
	''' 
	Run command via SSH, logging into buffer
	'''
	output = "\n(running) %s:\n" % cmd
	rc, cmd_output = self.ssh.exec_command(cmd)
	output += "\n%s" % cmd_output
	return rc, output

    def assimilate(self):
        '''
	An SSH connection has been passed in and we now know what payloads
	we want to deploy onto the system, and what post commands to run.
	Make it happen.  Returns (return_code, concatenated_output) as a 
	tuple. If there are no exceptions (and the tarball was correct) the
	system is now enslaved.
	'''

	all_output = ""
	# place the deploy tarball onto the system
        self.ssh.put_file(self.payload, "/tmp/rpath_assimilator.tar")

        # run the series of assimilation commands
        for cmd in self.commands:
	    rc, output = self.run_cmd(cmd)
	    all_output += "\n%s" % output
	    if rc != 0:
		raise Exception("Assimilator failed nonzero (%s, %s), " + 
		    "thus far=\n%s" % (cmd, rc, all_output))

        # all commands successful
        return (0, all_output)

