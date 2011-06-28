# Copyright (c) 2011 rPath, Inc.
#
# Bootstraps a Linux system by attempting a login over SSH via 
# username/password or SSH key, and then pushing a registration
# tarball (RPM?) onto it.

from rmake3.core import types
from rmake3.core import handler

from rpath_repeater.models import SshParams
from rpath_repeater.codes import Codes as C
from rpath_repeater.utils import base_forwarding_plugin as bfp

import sys
import socket
import paramiko
import exceptions
import os
from contextlib import contextmanager

# various plugin boilerplate...
XML = bfp.XML
SSH_JOB = bfp.PREFIX + '.sshplugin'
SSH_TASK_BOOTSTRAP = SSH_JOB + '.bootstrap'
SshData = types.slottype('SshData', 'p nodes response') # where p is a SshParams

###########################################################################    

class SshPlugin(bfp.BaseForwardingPlugin):
    """
    The SSH plugin is very minimal and only supports the BOOTSTRAP task.   Bootstrapping a node is intended
    to install (via SSH/SCP) the rpath tools on the node in such a way that CIM path will work later.  Thus,
    probing the node has a priority of (CIM/WMI, SSH), and the degenerate SSH plugin is only used if CIM
    is unreachable, and the only role of the SSH plugin is to install the CIM plugin.
    """

    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(SshHandler)
    
    @classmethod
    def worker_get_task_types(cls):
        return {
            SSH_TASK_BOOTSTRAP: BootstrapTask,
        }

###########################################################################    

class SshHandler(bfp.BaseHandler):
    """
    This is the server side (rmake3) handler for the remote job.  The handler can ONLY create a boostrap task
    and as such does not pay attention to the 'method' parameter like the CIM and WMI plugins.
    """

    jobType = SSH_JOB
    firstState = 'sshCall' # NEEDED?
    #RegistrationTaskNS = CIM_TASK_REGISTER  # NEEDED?

    def setup (self):
        bfp.BaseHandler.setup(self)

    @classmethod
    def initParams(cls, data):
        # FIXME: need to implement this
        return SshParams(**data.pop('sshParams', {}))

    @classmethod
    def _getArgs(cls, taskType, params, methodArguments, zoneAddresses):
        # resist the urge to remove this method -- tests need it
        if taskType in [ SSH_TASK_BOOTSTRAP ]:
            return SshData(params, zoneAddresses)
        raise Exception("Unhandled task type %s" % taskType)

    def sshCall(self):
        '''Invokes the bootstrap task when handler is called'''
        # common task boilerplate, would be nice to refactor
        self.setStatus(C.MSG_START, "Initiating SSH call")
        self.initCall()
        self.sshParams = self.initParams(self.data)
        self.eventUuid = self.sshParams.eventUuid

        if not self.zone:
            self.setStatus(C.ERR_ZONE_MISSING, "SSH call requires a zone")
            self.postFailure()
            return

        # does not pay attention to self.method, you only get TASK_BOOTSTRAP
        self.setStatus(C.MSG_CALL, "SSH call: %s %s:%s" % ('bootstrap', 
            self.sshParams.host, self.sshParams.port))
        self.setStatus(C.MSG_NEW_TASK, "Creating task")
        args = self._getArgs(SSH_TASK_BOOTSTRAP, self.sshParams, 
            self.methodArguments, self.zoneAddresses)
        # args = self._getArgs()
        #SshData(self.sshParams, self.zoneAddresses)
        task = self.newTask(SSH_TASK_BOOTSTRAP, SSH_TASK_BOOTSTRAP, args, 
            zone=self.zone)
        return self._handleTask(task)

###########################################################################    

class SshTaskHandler(bfp.BaseTaskHandler):
    '''Ssh specific subclass of generic worker task'''
    InterfaceName = "SSH"

###########################################################################    
                                                                          
class BootstrapTask(SshTaskHandler):

    '''
    This runs on the worker and runs the actual bootstrapping against the 
    remote managed node
    '''

    def _run(self, data):

        '''
        Task entry point, including normal task boilerplate 
        (TODO: refactor)
        '''

        self.sendStatus(C.MSG_BOOTSTRAP_REQ, # FIXME: NEED TO DEFINE THIS? YES
            "Contacting host %s on port %d to bootstrap"
                % (data.p.host, data.p.port))

        p = data.p
        retVal, outParams = self._bootstrap(host=p.host, port=p.port, \
            user=p.sshUser, password=p.sshPassword, key=p.sshKey, \
            uuid=p.eventUuid)

        # FIXME/TODO: construct meaningful return XML here
        data.response = "<system/>"
        self.setData(data)

        if retVal == 0:
            self.sendStatus(C.OK, "Host %s bootstrap successful" %
                data.p.host)
        else:
            errorSummary = outParams.get('errorSummary', '')
            errorDetails = outParams.get('errorDetails', '')
            self.sendStatus(C.ERR_GENERIC,
                "Host %s bootstrap failed: %s" %
                    (data.p.host, errorSummary), errorDetails)

    def _bootstrap(self,host=None,port=None,user=None, 
        password=None, key=None, uuid=None):

        conn = SshPluginConnector(host=host, port=port, user=user, password=password, key=key)
        conn.close()

        '''Guts of actual bootstrap code goes here...'''
        outParams = dict(
           errorSummary = '',
           errorDetails = ''
        )
        return (1, outParams)
        
class SshPluginConnector(object):
 
    """
    Paramiko SSH connection, adapted to our own validation needs.
    Heavily based off the paramiko demo directory version, but non-interactive.

    Usage: 
        sconn = SshConnector(host='foo.example.org',...)
        sconn.command('...')
        sconn.close()
    """  

    def __init__(self, host=None, port=22, user='root', password='password', key=None):
       self.host      = host
       self.port      = port
       self.user      = user
       self.password  = password
       self.key       = key
       #self.socket    = self._gen_socket()
       #self.transport = self._gen_transport()
       # NOTE: no ssh host key validation is done here as we expect these to be reprovisioned
       # constantly.  Do *NOT* use for runtime management.
       self.client    = self._gen_client()

    def _gen_client(self):
       client = paramiko.SSHClient()
       client.load_system_host_keys()
       client.set_missing_host_key_policy(paramiko.WarningPolicy())
       if self.key:
           try:
               client.connect(self.host, port=self.port, 
                   password=self.password, key_filename=self.key)
           except paramiko.PasswordRequiredException:
               raise exceptions.NotImplementedError("invalid key password")
       else:
           client.connect(self.host, port=self.port, username=self.user, 
               password=self.password)
            
       return client

    def close(self):
        self.client.close()

    def exec_command(self, cmd): 
        '''Runs a non-interactive command and returns both the exit code & output'''
        cmd = cmd + "; echo $?"
        sin, sout, serr = self.client.exec_command(cmd)
        results = sout.read()
        lines = results.split("\n")
        status = int(lines[-2])
        results = results[0:-2].strip()
        return (status, results)

    @contextmanager
    def _closing(self, thing):
        try:
            yield thing
        finally:
            thing.close

    def _sftp(self):
        '''Create a SFTP connection'''
        return paramiko.SFTPClient.from_transport(self.client.get_transport())

    def put_file(self, local_file, remote_file):
        '''place a file on the remote system'''
        with self._closing(self._sftp()) as sftp:
            sftp.put(local_file, remote_file)

    def get_file(self, remote_file, local_file):
        '''download a remote file'''
        with self._closing(self._sftp()) as sftp:
            sftp.get(remote_file, local_file)

    def unlink(self, remote_file):
        '''delete a remote file'''
        with self._closing(self._sftp()) as sftp:
            sftp.unlink(remote_file)
     

def testing_main():
    '''
    Attempt some Paramiko operations...
    '''
    # by username/password ...

    conn = SshPluginConnector(host='127.0.0.1',port=22,user='root',
          password='password') 
    #conn = SshPluginConnector(host='127.0.0.1',port=22,
    #       key=os.path.expanduser('~/.ssh/id_rsa'),
    #       password='ssh_unlock_password') 
    status, results = conn.exec_command("cat /etc/passwd")
    print "status = " + str(status)
    print "results = " + results
    conn.put_file("/tmp/foo","/tmp/bar")
    conn.get_file("/tmp/bar","/tmp/baz")
    conn.unlink("/tmp/baz")
    conn.close()
    return status


if __name__ == '__main__':
    testing_main()


