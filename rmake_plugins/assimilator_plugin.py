# Copyright (c) 2011 rPath, Inc.
#
# Bootstraps a Linux system by attempting a login over SSH via 
# username/password or SSH key, and then pushing a registration
# tarball (RPM?) onto it.  May learn other tricks later.

from rmake3.core import types
from rmake3.core import handler
from rpath_repeater.models import AssimilatorParams
from rpath_repeater.codes import Codes as C
from rpath_repeater.utils import base_forwarding_plugin as bfp
from rpath_repeater.utils.ssh import SshConnector

import paramiko
import exceptions

# various plugin boilerplate...
XML = bfp.XML
ASSIMILATOR_JOB = bfp.PREFIX + '.assimilatorplugin'
ASSIMILATOR_TASK_BOOTSTRAP = ASSIMILATOR_JOB + '.bootstrap'
AssimilatorData = types.slottype('AssimilatorData', 
    'p nodes response') # where p is a AssimilatorParam

###########################################################################    

class AssimilatorPlugin(bfp.BaseForwardingPlugin):
    """
    The Assimilator plugin is very minimal and only supports the BOOTSTRAP task.   
    Bootstrapping a node is intended to install (via SSH/SFTP) the rpath 
    tools on the node in such a way that CIM path will work later.  Thus,
    probing the node has a priority of (CIM/WMI, SSH), and the degenerate 
    SSH plugin is only used if CIM is unreachable, and the only role of 
    the SSH plugin is to install the CIM plugin.
    """

    def dispatcher_pre_setup(self, dispatcher):
        handler.registerHandler(AssimilatorHandler)
    
    @classmethod
    def worker_get_task_types(cls):
        return {
            ASSIMILATOR_TASK_BOOTSTRAP: BootstrapTask,
        }

###########################################################################    

class AssimilatorHandler(bfp.BaseHandler):
    """
    This is the server side (rmake3) handler for the remote job.  The 
    handler can ONLY create a boostrap task and as such does not pay 
    attention to the 'method' parameter like the CIM and WMI plugins.
    This may change in the future if additional jobs are required.
    """

    jobType = ASSIMILATOR_JOB
    firstState = 'sshCall' 

    def setup (self):
        bfp.BaseHandler.setup(self)

    @classmethod
    def initParams(cls, data):
        return AssimilatorParams(**data.pop('sshParams', {}))

    @classmethod
    def _getArgs(cls, taskType, params, methodArguments, zoneAddresses):
        # resist the urge to remove this method -- tests need it
        if taskType in [ ASSIMILATOR_TASK_BOOTSTRAP ]:
            return AssimilatorData(params, zoneAddresses)
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
        args = self._getArgs(ASSIMILATOR_TASK_BOOTSTRAP, self.sshParams, 
            self.methodArguments, self.zoneAddresses)
        task = self.newTask(ASSIMILATOR_TASK_BOOTSTRAP, 
            ASSIMILATOR_TASK_BOOTSTRAP, args, zone=self.zone)
        return self._handleTask(task)

###########################################################################    

class AssimilatorTaskHandler(bfp.BaseTaskHandler):
    '''Assimilator specific subclass of generic worker task'''
    InterfaceName = "SSH"

###########################################################################    
                                                                          
class BootstrapTask(AssimilatorTaskHandler):
    '''
    This runs on the worker and runs the actual bootstrapping against the 
    remote managed node
    '''

    def _run(self, data):
        '''
        Task entry point, including normal task boilerplate 
        FIXME: refactor
        '''

        self.sendStatus(C.MSG_BOOTSTRAP_REQ,
            "Contacting host %s on port %d to bootstrap"
                % (data.p.host, data.p.port))
        p = data.p

        # do actual boostraping heavy lifting:
        retVal, outParams = self._bootstrap(host=p.host, port=p.port, \
            user=p.sshUser, password=p.sshPassword, key=p.sshKey, \
            uuid=p.eventUuid)

        # FIXME: return appropriate XML
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
        '''
        Guts of actual bootstrap code goes here...
        FIXME: finish this, add a utils/bootstrapper class that uses
        the utils.ssh.SshConnector
        '''

        conn = SshConnector(host=host, port=port, user=user, 
            password=password, key=key)
        conn.close()

        outParams = dict(
           errorSummary = '',
           errorDetails = ''
        )
        return (1, outParams)
        
def testing_main():
    '''
    Attempt some Paramiko operations...
    '''
    # by username/password ...
    conn = SshConnector(host='127.0.0.1',port=22,user='root',
          password='password') 
    # by key...
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


