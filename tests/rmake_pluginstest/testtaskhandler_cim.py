# Copyright (C) 2010 rPath, Inc.

import testsuite
testsuite.setup()

import pywbem

import cim_forwarding_plugin

from testtaskhandler import TestBase

CIMProperty = pywbem.CIMProperty
CIMInstanceName = pywbem.CIMInstanceName
CIMInstance = pywbem.CIMInstance
Uint16 = pywbem.Uint16

class CimTest(TestBase):
    # These cannot be defined in setUp, they are needed in the base class
    taskDispatcher = cim_forwarding_plugin.CimForwardingPlugin.worker_get_task_types()
    baseNamespace = cim_forwarding_plugin.CIM_JOB
    handlerClass = cim_forwarding_plugin.CimHandler

    class OP(object):
        ComputerSystem = CIMInstanceName('RPATH_ComputerSystem',
            keybindings=dict(name="mysystem.example.com"))
        ElementSoftwareIdentity = CIMInstanceName('RPATH_ElementSoftwareIdentity')
        SoftwareIdentity = CIMInstanceName('RPATH_SoftwareIdentity')
        OperatingSystem = CIMInstanceName('Linux_SoftwareIdentity',
            keybindings=dict(name="mysystem.example.com"))

    _defaultData = dict(
        intrinsic=dict(
            EnumerateInstances = dict(
                RPATH_ComputerSystem = [
                    CIMInstance(OP.ComputerSystem,
                        properties=dict(LocalUUID='6947ee3b-4776-e11b-5d98-5b8284d4f810',
                            GeneratedUUID='feeddeadbeef',),
                        path=OP.ComputerSystem),
                ],
                RPATH_ElementSoftwareIdentity = [
                    CIMInstance(OP.ElementSoftwareIdentity,
                        properties=dict(
                            Antecedent = CIMInstanceName('blabbedy',
                                keybindings = dict(InstanceID = 'instid1')),
                            ElementSoftwareStatus = [ Uint16(2), Uint16(6) ]),
                        path=OP.ElementSoftwareIdentity),
                    CIMInstance(OP.ElementSoftwareIdentity,
                        properties=dict(
                            Antecedent = CIMInstanceName('blabbedy',
                                keybindings = dict(InstanceID = 'instid2')),
                            ElementSoftwareStatus = [ Uint16(2), Uint16(6) ]),
                        path=OP.ElementSoftwareIdentity),
                ],
                RPATH_SoftwareIdentity = [
                    CIMInstance(OP.SoftwareIdentity,
                        properties = dict(
                            InstanceID = 'instid1',
                            name = 'group-foo',
                            versionString = '/conary.rpath.com@rpl:2/123.45:1-2-3[is: x86]',
                        ),
                        path=OP.SoftwareIdentity),
                    CIMInstance(OP.SoftwareIdentity,
                        properties = dict(
                            InstanceID = 'instid2',
                            name = 'group-bar',
                            versionString = '/conary.rpath.com@rpl:2/923.45:9-2-3[is: x86_64]',
                        ),
                        path=OP.SoftwareIdentity),
                ],
            ),
            EnumerateInstanceNames = dict(
                RPATH_ComputerSystem = [
                    OP.ComputerSystem,
                ],
                Linux_OperatingSystem = [
                    OP.OperatingSystem,
                ],
            ),
        ),
        extrinsic=dict(
            RemoteRegistration = (0, dict(errorSummary="", errorDetails="")),
            Shutdown = (0, dict()),
        ),
    )

    class WBEMServer(cim_forwarding_plugin.CIMTaskHandler.WBEMServerFactory):
        class WBEMConnectionFactory(cim_forwarding_plugin.CIMTaskHandler.WBEMServerFactory.WBEMConnectionFactory):
            _data = {}
            def imethodcall(self, methodname, namespace, **params):
                mmock = self._data['intrinsic'].get(methodname, None)
                if mmock is None:
                    raise Exception("Mock me: %s" % methodname)
                cn = params['ClassName']
                val = mmock.get(cn.classname, None)
                if val is None:
                    raise Exception("Mock me: %s %s" % (methodname,
                        cn.classname))
                return ('IMETHODRESPONSE', dict(NAME=methodname), val)
            def methodcall(self, methodname, localobject, **params):
                mmock = self._data['extrinsic'].get(methodname, None)
                if mmock is None:
                    raise Exception("Mock me: %s" % methodname)
                retval, outParams = mmock
                val = [ ('RETURNVALUE', dict(PARAMTYPE='uint16'), retval) ]
                for k, v in outParams.items():
                    if isinstance(v, basestring):
                        vtype = 'string'
                    elif isinstance(v, int):
                        vtype = 'uint16'
                    val.append((k, vtype, v))
                return val

    def getClassOverrides(self, namespace):
        wbemServerClass = self.WBEMServer
        wbemServerClass.WBEMConnectionFactory._data.clear()
        wbemServerClass.WBEMConnectionFactory._data.update(self._defaultData)
        # Here we allow individual tests to override some of the data
        self._data = wbemServerClass.WBEMConnectionFactory._data
        return dict(
            WBEMServerFactory=wbemServerClass,
            _probeHost=lambda x,host,port,x509: "server cert",
        )

    def _cimParams(self, **kwargs):
        defaults = dict(
            host='1.2.3.4', port=8135,
            clientCert="Client cert", clientKey="Client key",
            eventUuid="deadbeef")
        defaults.update(kwargs)
        return self.client.CimParams(**defaults)

    def testRegister(self):
        params = self._cimParams()
        self.client.register_cim(params)
        self.failUnlessEqual(
            [ (x.status.code, x.status.text) for x in self.results.register ],
            [
                (104, 'Contacting host 1.2.3.4 on port 8135 to rActivate itself'),
                (200, 'Host 1.2.3.4 registration initiated'),
            ])

        taskData = self.results.register[-1].task_data.thaw()
        self.assertXMLEquals(taskData.object.response, """
<system/>
""")

    def testPoll(self):
        params = self._cimParams()
        self.client.poll_cim(params)
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
  <ssl_server_certificate>server cert</ssl_server_certificate>
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
</system>""")

    def testShutdown(self):
        params = self._cimParams()
        self.client.shutdown_cim(params)
        self.failUnlessEqual(
            [ (x.status.code, x.status.text) for x in self.results.shutdown ],
            [
                (101, 'Contacting host 1.2.3.4 to shut itself down'),
                (200, 'Host 1.2.3.4 will now shutdown'),
            ])

    def testUpdate(self):
        params = self._cimParams()
        sources = [ "group-top1=/conary.rpath.com@rpl:2/1-1-1",
            "group-top2=/conary.rpath.com@rpl:2/2-2-2" ]
        self.client.update_cim(params, sources=sources)
        lastTask = self.results.update[-1]
        raise testsuite.SkipTestException("Need to mock conaryclient")
        self.failIf(lastTask.status.detail, lastTask.status.detail)
        self.failUnlessEqual(
            [ (x.status.code, x.status.text) for x in self.results.update ],
            [
            ])
        taskData = lastTask.task_data.thaw()
        self.assertXMLEquals(taskData.object.response, """
""")

testsuite.main()