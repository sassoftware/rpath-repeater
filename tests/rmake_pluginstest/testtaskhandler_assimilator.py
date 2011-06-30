# Copyright (C) 2011 rPath, Inc.
#
# Mock tests for (Linux) Assimilation Plugin
# does not actually SSH here, see scripts/demo_assimilate.py for that

import testsuite
testsuite.setup()
from testtaskhandler import TestBase
import assimilator_plugin
from rpath_repeater.utils.ssh import SshConnector
from rpath_repeater.utils.assimilator import LinuxAssimilator 
import paramiko

class MockStream(object):
    '''Mocks up stdout, stderr objects coming back from paramiko'''

    def __init__(self, command, should_pass=True):
        self.command     = command
        self.should_pass = should_pass

    def read(self, *args):
        '''
        due to our implementation and the way we run commands
        the last line is an exit code
        '''
        # TODO: this should vary based on the command, right?
        if self.should_pass:
            return "it worked\n0\n"
        else:
            return "something failed\n1\n"

class MockSshClient(paramiko.SSHClient):
    '''Fake paramiko SSH client, records details of commands sent'''
    
    # counts of commands executed
    COUNT_LOAD_HOST_KEYS = 0
    COUNT_CONNECT        = 0
    COUNT_EXEC_COMMAND   = 0
    COUNT_CLOSE          = 0
    COUNT_GET_TRANSPORT  = 0

    def __init__(self):
        self.all_commands_run = []

    def load_system_host_keys(self):
        MockSshClient.COUNT_LOAD_HOST_KEYS+=1

    def connect(self, *args, **kwargs):
        MockSshClient.COUNT_CONNECT=+1

    def exec_command(self, cmd):
        '''keep a list of all commands run'''
        MockSshClient.COUNT_EXEC_COMMAND=+1
        self.all_commands_run.extend(cmd)
        return (MockStream(cmd), MockStream(cmd), MockStream(cmd))

    def close(self):
        MockSshClient.COUNT_CLOSE+=1

    def get_transport(self):
        MockSshClient.COUNT_GET_TRANSPORT+=1
        return None
    
    @classmethod
    def tests_ok(cls, test):
        test.failUnless(cls.COUNT_LOAD_HOST_KEYS >  0, 'uses host keys')
        test.failUnless(cls.COUNT_CONNECT        >  0, 'connects')
        test.failUnless(cls.COUNT_EXEC_COMMAND   >  0, 'runs some commands')
        test.failUnless(cls.COUNT_CLOSE          >  0, 'remembers to close')
        test.failUnless(cls.COUNT_GET_TRANSPORT  >  0, 'gets a transport')

class MockSftpClient(object):
    '''Fake SFTP client, generated by SSHConnection'''

    COUNT_PUT    = 0
    COUNT_GET    = 0
    COUNT_UNLINK = 0
    COUNT_CLOSE  = 0
    COUNT_FROM_TRANSPORT = 0

    def __init__(self):
        pass

    def put(self, local, remote):
        MockSftpClient.COUNT_PUT+=1

    def get(self, remote, local):
        MockSftpClient.COUNT_GET+=1

    def unlink(self, remote):
        MockSftpClient.COUNT_UNLINK+=1

    @classmethod
    def from_transport(cls, transport):
        MockSftpClient.COUNT_FROM_TRANSPORT+=1
        return MockSftpClient()

    def close(self):
        MockSftpClient.COUNT_CLOSE+=1

    @classmethod
    def tests_ok(cls, test):
        test.failUnless(cls.COUNT_PUT   > 0,  'puts a file')
        # FIXME: -- decorator in SSHConnector not working?, needs to be fixed
        #print "CLOSE COUNT = " + str(cls.COUNT_CLOSE)
        #test.failUnless(cls.COUNT_CLOSE > 0,  'remembers to close')


class AssimilatorTest(TestBase):
    ''' 
    Tests for the assimilator plugin, which enslaves new Linux Systems
    that do not have any rPath management tools installed.
    '''

    # These cannot be defined in setUp, they are needed in the base class
    taskDispatcher = assimilator_plugin.AssimilatorPlugin.worker_get_task_types()
    baseNamespace = assimilator_plugin.ASSIMILATOR_JOB
    handlerClass = assimilator_plugin.AssimilatorHandler

    # this is all mocked up, so won't really be used
    HOST = '127.0.0.1'
    PORT = 22
    
    def _params(self, **kwargs):
        ''' 
        default parameters for mock tests, 
        we likely want to test key/pw auth seperately
	    '''

        defaults = dict(
            host=AssimilatorTest.HOST,
            port=AssimilatorTest.PORT,
            sshUser='root',
            sshPassword='root_password',
            sshKey='',
            eventUuid='deadbeef',
            osFamily='EL6'
        )   
        defaults.update(kwargs)
        return self.client.AssimilatorParams(**defaults)

    def testLowLevelMockedConnect(self):
        '''
        make a fake SSH connection, observe the counts of various
        operations we expect to run.  
        '''
        conn = SshConnector(host='192.0.0.1', 
            port=22,
            user='root', 
            password='root_password_imaginary',
            key='',
            clientClass=MockSshClient, 
            sftpClass=MockSftpClient,
        )
        osFamily = 'EL6'
        asim = LinuxAssimilator(conn, osFamily)
        rc, allOutput = asim.assimilate()
        #print "ASSIMILATOR OUTPUT : %s\n" % all_output
        self.failUnlessEqual(rc, 0, 'successful assimilator return code')
        # call more tests here, defined in classes above
        MockSshClient.tests_ok(self)
        MockSftpClient.tests_ok(self)


    #def testBootstrapViaClientLib(self):
    #    '''
    #    test kicking off a bootstrap task
    #    FIXME: reinstate tests once mocked up properly
    #    '''
    #    params = self._params()
    #    self.client.bootstrap(params)

        #expected_response = '<system/>'

        # NOTE -- this will fail because we haven't mocked up the SSH connection yet
        # so it will try a REAL connection.

        #self.failUnlessEqual(
        #    [ (x.status.code, x.status.text) for x in self.results.bootstrap ],
        #    [
        #        (104, "Contacting host %s on port %s to bootstrap" % (SshTest.HOST,SshTest.PORT)),
        #        (200, "Host %s bootstrap successful" % (SshTest.HOST)),
        #    ])
        #
        #taskData = self.results.bootstrap[-1].task_data.thaw()
        #
        #self.assertXMLEquals(taskData.object.response, expected_response)

testsuite.main()

