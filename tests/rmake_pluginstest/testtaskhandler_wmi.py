# Copyright (C) 2010 rPath, Inc.

import testsuite
testsuite.setup()

import uuid

import wmiclient

import wmi_forwarding_plugin

from testtaskhandler import TestBase


class WmiTest(TestBase):
    # These cannot be defined in setUp, they are needed in the base class
    taskDispatcher = wmi_forwarding_plugin.WmiForwardingPlugin.worker_get_task_types()
    baseNamespace = wmi_forwarding_plugin.WMI_JOB
    handlerClass = wmi_forwarding_plugin.WmiHandler

    class K:
        rtisPath = 'SOFTWARE\\rPath\\rTIS.NET\\parameters'
        inventoryPath = 'SOFTWARE\\rPath\\rTIS.NET\\inventory'
        conaryPath = 'SOFTWARE\\rPath\\rTIS.NET\\conary'

        baseBoardManufacturer = ('registry', 'getkey',
            'HARDWARE\\DESCRIPTION\\System\\BIOS', 'BaseBoardManufacturer')
        getStatusTuple = ('service', 'getstatus', 'rPath Tools Installer Service')
        biosMajorRelease = ('registry', 'getkey',
            'HARDWARE\\DESCRIPTION\\System\\BIOS', 'BiosMajorRelease')
        biosMinorRelease = ('registry', 'getkey',
            'HARDWARE\\DESCRIPTION\\System\\BIOS', 'BiosMinorRelease')
        generatedUuid = ('registry', 'getkey', inventoryPath, 'generated_uuid')
        setGeneratedUuid =  ('registry', 'setkey', inventoryPath, 'generated_uuid', "feeddeadbeef")
        localUuid = ('registry', 'getkey', inventoryPath, 'local_uuid')
        setLocalUuid = ('registry', 'setkey', inventoryPath, 'local_uuid', '6947ee3b-4776-e11b-5d98-5b8284d4f810')
        computerName = ('registry', 'getkey',
            'SYSTEM\\CurrentControlSet\\Control\\ComputerName\\ActiveComputerName',
            'ComputerName')
        pollingManifest = ('registry', 'getkey', conaryPath, 'polling_manifest')
        manifest = ('registry', 'getkey', conaryPath, 'manifest')
        queryNetwork = ('query', 'network')
        queryUUID = ('query', 'uuid')
        appData = ('registry', 'getkey', 'SOFTWARE\\Microsoft\\Windows\\CurrentVersion\\Explorer\\Shell Folders', 'Common AppData')
        systemModel = ('registry', 'getkey', conaryPath, 'system_model')
        running = ('registry', 'getkey', rtisPath, 'Running')
        setRoot = ('registry', 'setkey', rtisPath, 'Root', 'C:\\Program Files\\rPath\\Updates')
        getFlavor = ('registry', 'getkey', 'SYSTEM\\CurrentControlSet\\Control\\Session Manager\\Environment', 'PROCESSOR_ARCHITECTURE')

    class MultiChoice(object):
        def __init__(self, choices):
            self._counter = 0
            self.choices = choices

        def get(self):
            if self._counter >= len(self.choices):
                raise Exception("Mock more values!!!")
            val = self.choices[self._counter]
            self._counter += 1
            return val


    _defaultData = {
        K.baseBoardManufacturer: "Intel corporation\n",
        K.getStatusTuple: "blah\n",
        K.biosMajorRelease: "1\n",
        K.biosMinorRelease: "1\n",
        K.setGeneratedUuid: "blah\n",
        K.setLocalUuid: "blah\n",
        K.computerName: "my very own computer\n",
        K.localUuid: "6947ee3b-4776-e11b-5d98-5b8284d4f810\n",
        K.generatedUuid: "feeddeadbeef",
        K.pollingManifest: [
            'group-foo=/conary.rpath.com@rpl:2/123.45:1-2-3[is: x86]',
            'group-bar=/conary.rpath.com@rpl:2/923.45:9-2-3[is: x86_64]'
        ],
        K.manifest: [
            'group-foo=/conary.rpath.com@rpl:2/123.45:1-2-3[is: x86]',
            'group-bar=/conary.rpath.com@rpl:2/123.45:9-2-3[is: x86_64]'
        ],
        K.systemModel: [
            'install group-foo=conary.rpath.com@rpl:2/123.45:1-2-3[is: x86]',
            'install group-bar=conary.rpath.com@rpl:2/123.45:1-4-2[is: x86_64]'
        ],
        K.queryNetwork: "65539, 172.16.175.218, 255.255.240.0, ENG-E1DA0E00778, eng.rpath.com",
        K.queryUUID: "6947ee3b-4776-e11b-5d98-5b8284d4f810",
        K.running: "stopped",
        K.setRoot: 'bla',
        K.appData: 'ProgramData',
        K.getFlavor: 'x64',
    }

    class WMICommand(object):
        _data = {}

        def __init__(self, *args, **kwargs):
            pass

        def execute(self, *cmd):
            key = tuple(cmd)
            val = self._data.get(key)
            if val is None:
                raise Exception("mock me!", key)
            if isinstance(val, WmiTest.MultiChoice):
                val = val.get()
            if not isinstance(val, (tuple, list)):
                val = [val, ]
            return wmiclient.WMICResults(None, 0, val, None)

    class CommandLine(object):
        def __init__(self, options, args):
            self.options = options
            self.args = args

    def setUp(self):
        TestBase.setUp(self)
        import socket
        self.mock(socket, 'gethostbyaddr', lambda a: (None, None, (a, )))

        import wmiclient
        self.mock(wmiclient, 'InteractiveCommand', self.WMICommand)

        from rpath_repeater.utils.windows.rtis import rTIS
        self.mock(rTIS, 'setup', lambda a: None)

        from rpath_repeater.utils.windows.inventory import Inventory
        self.mock(Inventory, 'setup', lambda a: None)

        from rpath_repeater.utils.windows.smbclient import SMBClient
        def mount(selfish):
            selfish._rootdir = self.workDir
        self.mock(SMBClient, '_mount', mount)

    def getClassOverrides(self, namespace):
        WMICommandClass = self.WMICommand
        WMICommandClass._data.clear()
        WMICommandClass._data.update(self._defaultData)
        # Here we allow individual tests to override some of the data
        self._data = WMICommandClass._data
        return dict(_createGeneratedUuid=lambda x: 'feeddeadbeef')

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
            [(101, '1.2.3.4: '),
             (110, '1.2.3.4: Registering System'),
             (110, '1.2.3.4: Registration Complete'),
             (200, '1.2.3.4: ')]
            )

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
            [(101, '1.2.3.4: '),
             (110, '1.2.3.4: Polling System'),
             (110, '1.2.3.4: Retrieving polling manifest'),
             (110, '1.2.3.4: Polling Complete'),
             (200, '1.2.3.4: ')]
            )
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
        <flavor>is: x86</flavor>
      </version>
      <flavor>is: x86</flavor>
    </trove>
    <trove>
      <name>group-bar</name>
      <version>
        <full>/conary.rpath.com@rpl:2/9-2-3</full>
        <label>conary.rpath.com@rpl:2</label>
        <revision>9-2-3</revision>
        <ordering>923.45</ordering>
        <flavor>is: x86_64</flavor>
      </version>
      <flavor>is: x86_64</flavor>
    </trove>
  </installed_software>
  <networks>
    <network>
      <device_name>65539</device_name>
      <ip_address>172.16.175.218</ip_address>
      <netmask>20</netmask>
      <dns_name>eng-e1da0e00778.eng.rpath.com</dns_name>
      <required>false</required>
    </network>
  </networks>
</system>""")

    def testShutdown(self):
        params = self._wmiParams()
        self.failUnlessRaises(NotImplementedError,
            self.client.shutdown_wmi, params)

    def testUpdate(self):
        raise testsuite.SkipTestException("Need to mock conaryclient")
        self._data[self.K.getStatusTuple] = self.MultiChoice([
            "some stuff", 'Service Not Active\n', "some more stuff"])
        params = self._wmiParams()
        sources = [ "group-top1=/conary.rpath.com@rpl:2/1-1-1",
            "group-top2=/conary.rpath.com@rpl:2/2-2-2" ]
        self.client.update_wmi(params, sources=sources)
        lastTask = self.results.update[-1]
        self.failIf(lastTask.status.detail, lastTask.status.detail)
        self.failUnlessEqual(
            [ (x.status.code, x.status.text) for x in self.results.update ],
            [
            ])
        taskData = lastTask.task_data.thaw()
        self.assertXMLEquals(taskData.object.response, """
""")

    def testConfiguration(self):
        raise testsuite.SkipTestException('need to mock more')
        params = self._wmiParams()
        configuration = '<values><foo>bar</foo></values>'

        command = 'job-%s' % uuid.UUID(int=self.client._counter+1)
        key = ('registry', 'setkey', self.K.rtisPath, 'Commands', command)
        self._data[key] = ''

        self._data[('service', 'start', 'rPath Tools Installer Service')] = 'Success'

        self.client.configuration_wmi(params, configuration=configuration)

        task = self.results.configuration[-1].thaw()
        self.failIf(task.status.failed)
        self.failUnlessEqual(task.status.text, 'Host 1.2.3.4 has been '
            'configured successfully')

testsuite.main()
