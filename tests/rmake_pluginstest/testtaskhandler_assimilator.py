# Copyright (C) 2011 rPath, Inc.

# rpath modules
import testsuite
testsuite.setup()
import assimilator_plugin
from testtaskhandler import TestBase

# stock modules
import exceptions
    
class AssimilatorTest(TestBase):
    ''' 
    Tests for the assimilator plugin, which enslaves new Linux Systems
    that do not have any rPath management tools installed.
    '''

    # These cannot be defined in setUp, they are needed in the base class
    taskDispatcher = assimilator_plugin.AssimilatorPlugin.worker_get_task_types()
    baseNamespace = assimilator_plugin.ASSIMILATOR_JOB
    handlerClass = assimilator_plugin.AssimilatorHandler

    HOST = '1.2.3.4'
    PORT = 22

    def _params(self, **kwargs):
        ''' 
        default parameters for mock tests, 
        we likely want to test key/pw auth seperately
        '''
        defaults = dict(
            host=AssimilatorTest.HOST,
            port=AssimilatorTest.PORT,
            sshUser='user',
            sshPassword='pass',
            sshKey='',
            eventUuid='deadbeef',
        )   
        defaults.update(kwargs)
        return self.client.AssimilatorParams(**defaults)

    def testBootstrap(self):
        '''
        test kicking off a bootstrap task
        FIXME: reinstate tests once mocked up properly
        '''
        params = self._params()
        self.client.bootstrap(params)

        expected_response = '<system/>'

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

