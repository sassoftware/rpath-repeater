# Copyright (C) 2010 rPath, Inc.

import testsuite
testsuite.setup()

from rmake_plugins import wmi_forwarding_plugin

from testtaskhandler import TestBase

class WmiTest(TestBase):
    # These cannot be defined in setUp, they are needed in the base class
    taskDispatcher = wmi_forwarding_plugin.WmiForwardingPlugin.worker_get_task_types()
    baseNamespace = wmi_forwarding_plugin.WMI_JOB
    handlerClass = wmi_forwarding_plugin.WmiHandler

    class K:
        baseBoardManufacturer = ('registry', 'getkey',
            'HARDWARE\\DESCRIPTION\\System\\BIOS', 'BaseBoardManufacturer')
        getStatusTuple = ('service', 'getstatus', 'rPath Tools Install Service')
        biosMajorRelease = ('registry', 'getkey',
            'HARDWARE\\DESCRIPTION\\System\\BIOS', 'BiosMajorRelease')
        biosMinorRelease = ('registry', 'getkey',
            'HARDWARE\\DESCRIPTION\\System\\BIOS', 'BiosMinorRelease')
        generatedUuid = ('registry', 'getkey', 'SOFTWARE\\rPath\\inventory',
            'generated_uuid')
        setGeneratedUuid =  ('registry', 'setkey',
            'SOFTWARE\\rPath\\inventory', 'generated_uuid', "feeddeadbeef")
        localUuid = ('registry', 'getkey', 'SOFTWARE\\rPath\\inventory',
            'local_uuid')
        setLocalUuid = ('registry', 'setkey', 'SOFTWARE\\rPath\\inventory',
            'local_uuid', '6947ee3b-4776-e11b-5d98-5b8284d4f810')
        computerName = ('registry', 'getkey',
            'SYSTEM\\CurrentControlSet\\Control\\ComputerName\\ActiveComputerName',
            'ComputerName')
        pollingManifest = ('registry', 'getkey', 'SOFTWARE\\rPath\\conary',
            'polling_manifest')

    _defaultData = {
        K.baseBoardManufacturer: "Intel corporation\n",
        K.getStatusTuple: "blah\n",
        K.biosMajorRelease: "1\n",
        K.biosMinorRelease: "1\n",
        K.setGeneratedUuid: "blah\n",
        K.setLocalUuid: "blah\n",
        K.computerName: "my very own computer\n",
        K.localUuid: "6947ee3b-4776-e11b-5d98-5b8284d4f810\n",
        K.generatedUuid: "  feeddeadbeef\n\n",
        K.pollingManifest: """
            group-foo=/conary.rpath.com@rpl:2/123.45:1-2-3
            group-bar=/conary.rpath.com@rpl:2/923.45:9-2-3
"""
    }

    class WmiClient(wmi_forwarding_plugin.WMITaskHandler.WmiClientFactory):
        _data = {}
        def _wmiCall(self, cmd):
            cmd = WmiTest.parseCommandLine(cmd)
            key = tuple(cmd.args)
            val = self._data.get(key)
            if val is None:
                raise Exception("mock me!", key)
            if not isinstance(val, tuple):
                return 0, val
            return val

    class CommandLine(object):
        def __init__(self, options, args):
            self.options = options
            self.args = args

    @classmethod
    def parseCommandLine(cls, cmd):
        options = {}
        oname = None
        args = []
        for a in cmd[1:]:
            if oname is not None:
                options[oname] = a
                oname = None
                continue
            if a.startswith('--'):
                oname = a[2:]
                continue
            args.append(a)
        return cls.CommandLine(options, args)

    def getClassOverrides(self, namespace):
        wmiClientClass = self.WmiClient
        wmiClientClass._data.clear()
        wmiClientClass._data.update(self._defaultData)
        # Here we allow individual tests to override some of the data
        self._data = wmiClientClass._data
        return dict(WmiClientFactory=self.WmiClient,
            _createGeneratedUuid=lambda x: 'feeddeadbeef')

    def setUp(self):
        TestBase.setUp(self)
        pluginManager = None

    def _wmiParams(self, **kwargs):
        defaults = dict(
            host='1.2.3.4', port=8135,
            username="Jean Valjean", password="cosette", domain="Paris",
            eventUuid="deadbeef")
        defaults.update(kwargs)
        return self.client.WmiParams(**defaults)

    def testRegister(self):
        params = self._wmiParams()
        self.client.register_wmi(params)
        self.failUnlessEqual(
            [ (x.status.code, x.status.text) for x in self.results.register ],
            [
                (105, 'Contacting host 1.2.3.4 to validate credentials'),
                (110, 'Generating UUIDs'),
                (110, 'Stored UUIDs on Windows system'),
                (200, 'Registration Complete for 1.2.3.4'),
            ])

        taskData = self.results.register[-1].task_data.thaw()
        self.assertXMLEquals(taskData.object.response, """
<system>
  <local_uuid>6947ee3b-4776-e11b-5d98-5b8284d4f810</local_uuid>
  <generated_uuid>feeddeadbeef</generated_uuid>
  <hostname>my very own computer</hostname>
</system>
""")

    def testPoll(self):
        params = self._wmiParams()
        self.client.poll_wmi(params)
        self.failUnlessEqual(
            [ (x.status.code, x.status.text) for x in self.results.poll ],
            [
                (101, 'Contacting host 1.2.3.4 on port 8135 to Poll it for info'),
                (200, 'Host 1.2.3.4 has been polled'),
            ])
        taskData = self.results.poll[-1].task_data.thaw()
        self.assertXMLEquals(taskData.object.response, """
<system>
  <local_uuid>6947ee3b-4776-e11b-5d98-5b8284d4f810</local_uuid>
  <generated_uuid>feeddeadbeef</generated_uuid>
  <hostname>my very own computer</hostname>
  <installed_software>
    <trove>
      <name>group-foo</name>
      <version>
        <full>/conary.rpath.com@rpl:2/1-2-3</full>
        <label>conary.rpath.com@rpl:2</label>
        <revision>1-2-3</revision>
        <ordering>123.45</ordering>
        <flavor/>
      </version>
      <flavor/>
    </trove>
    <trove>
      <name>group-bar</name>
      <version>
        <full>/conary.rpath.com@rpl:2/9-2-3</full>
        <label>conary.rpath.com@rpl:2</label>
        <revision>9-2-3</revision>
        <ordering>923.45</ordering>
        <flavor/>
      </version>
      <flavor/>
    </trove>
  </installed_software>
</system>""")

    def testShutdown(self):
        params = self._wmiParams()
        self.failUnlessRaises(NotImplementedError,
            self.client.shutdown_wmi, params)

    def testUpdate(self):
        raise testsuite.SkipTestException("Need to mock conary and test counters")
        params = self._wmiParams()
        self.client.update_wmi(params)
        self.failUnlessEqual(
            [ (x.status.code, x.status.text) for x in self.results.update ],
            [
            ])
        taskData = self.results.update[-1].task_data.thaw()
        self.assertXMLEquals(taskData.object.response, """
""")

testsuite.main()
