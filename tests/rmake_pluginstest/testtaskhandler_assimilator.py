# Copyright (C) 2011 rPath, Inc.

# rpath modules
import testsuite
testsuite.setup()
import assimilator_plugin
from rpath_repeater.utils.ssh import SshConnector
from rpath_repeater.utils.assimilator import LinuxAssimilator
from testtaskhandler import TestBase

class AssimilatorTest(TestBase):
    ''' 
    Tests for the assimilator plugin, which enslaves new Linux Systems
    that do not have any rPath management tools installed.
    '''

    # These cannot be defined in setUp, they are needed in the base class
    taskDispatcher = assimilator_plugin.AssimilatorPlugin.worker_get_task_types()
    baseNamespace = assimilator_plugin.ASSIMILATOR_JOB
    handlerClass = assimilator_plugin.AssimilatorHandler

    HOST = '127.0.0.1'
    PORT = 22

    def _params(self, **kwargs):
        ''' 
        default parameters for mock tests, 
        we likely want to test key/pw auth seperately
	# FIXME: this needs to be mocked up somewhat
	'''

        defaults = dict(
            host=AssimilatorTest.HOST,
            port=AssimilatorTest.PORT,
            sshUser='root',
            sshPassword='root_password',
            sshKey='',
            eventUuid='deadbeef',
	    # FIXME: add flavor
        )   
        defaults.update(kwargs)
        return self.client.AssimilatorParams(**defaults)

    def testNoop(self):
	# FIXME: add some mock tests here, until then
	# see scripts/demo_assimilator.py
        pass

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

