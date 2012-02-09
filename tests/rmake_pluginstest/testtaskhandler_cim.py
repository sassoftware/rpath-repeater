# Copyright (C) 2010 rPath, Inc.

import testsuite
testsuite.setup()

import pywbem

import cim_forwarding_plugin

from testtaskhandler import TestBase

from rpath_repeater.codes import NS

CIMProperty = pywbem.CIMProperty
CIMInstanceName = pywbem.CIMInstanceName
CIMInstance = pywbem.CIMInstance
Uint16 = pywbem.Uint16

class CimTest(TestBase):
    # These cannot be defined in setUp, they are needed in the base class
    taskDispatcher = cim_forwarding_plugin.CimForwardingPlugin.worker_get_task_types()
    baseNamespace = NS.CIM_JOB
    handlerClass = cim_forwarding_plugin.CimHandler

    class OP(object):
        ComputerSystem = CIMInstanceName('RPATH_ComputerSystem',
            keybindings=dict(name="mysystem.example.com"))
        ElementSoftwareIdentity = CIMInstanceName('RPATH_ElementSoftwareIdentity')
        SoftwareIdentity = CIMInstanceName('RPATH_SoftwareIdentity')
        OperatingSystem = CIMInstanceName('Linux_SoftwareIdentity',
            keybindings=dict(name="mysystem.example.com"))
        Configuration = CIMInstanceName('RPATH_Configuration',
            keybindings=dict(SettingID='/var/lib/iconfig/values.xml'))
        UpdateJob = CIMInstanceName('RPATH_UpdateConcreteJob',
            keybindings=dict(InstanceID='a-b-c-d'))
        SurveyJob = CIMInstanceName('RPATH_SurveyConcreteJob',
            keybindings=dict(InstanceID='a-b-c-d'))

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
            GetInstance = dict(
                RPATH_Configuration = [
                    CIMInstance(OP.Configuration,
                        properties=dict(Value="<oldvalue/>"),
                        path=OP.Configuration),
                ],
                RPATH_UpdateConcreteJob = [
                    CIMInstance(OP.UpdateJob,
                        properties=dict(JobState=CIMProperty('JobState', 7, type='uint16')),
                        path=OP.UpdateJob),
                ],
                RPATH_SurveyConcreteJob = [
                    CIMInstance(OP.SurveyJob,
                        properties=dict(JobState=CIMProperty('JobState', 7, type='uint16'),
                            JobResults=CIMProperty('JobResults',
                            ['<survey><uuid>aa-bb-cc-dd</uuid></survey>', ])),
                        path=OP.SurveyJob),
                ],
            ),
            ModifyInstance = dict(
                RPATH_Configuration = [
                    CIMInstance(OP.Configuration,
                        properties=dict(Value="<newvalue/>"),
                        path=OP.Configuration),
                ],
            ),
        ),
        extrinsic=dict(
            RemoteRegistration = (0, dict(errorSummary="", errorDetails="")),
            UpdateManagementConfiguration = (0, dict(errorSummary="", errorDetails="")),
            Shutdown = (0, dict()),
            ApplyToMSE = (64, dict()),
            InstallFromNetworkLocations = (4096, dict(
                job=CIMInstanceName('RPATH_UpdateConcreteJob',
                    keybindings=dict(InstanceID='a-b-c-d')))),
            Scan = (4096, dict(
                job=CIMInstanceName('RPATH_SurveyConcreteJob',
                    keybindings=dict(InstanceID='a-b-c-d')))),
        ),
    )

    class WBEMServer(cim_forwarding_plugin.CIMTaskHandler.WBEMServerFactory):
        class WBEMConnectionFactory(cim_forwarding_plugin.CIMTaskHandler.WBEMServerFactory.WBEMConnectionFactory):
            _data = {}

            def _getAccessor(self, params):
                accessors =  ['ClassName', 'InstanceName', 'ModifiedInstance']
                for accessor in accessors:
                    obj = params.get(accessor, None)
                    if obj is not None:
                        return obj
                raise Exception("Mock me harder!")

            def imethodcall(self, methodname, namespace, **params):
                mmock = self._data['intrinsic'].get(methodname, None)
                if mmock is None:
                    raise Exception("Mock me: %s" % methodname)
                obj = self._getAccessor(params)
                classname = obj.classname
                if not isinstance(classname, basestring):
                    classname = classname.classname
                val = mmock.get(classname, None)
                if val is None:
                    raise Exception("Mock me: %s %s" % (methodname,
                        classname))
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
                    elif isinstance(v, CIMInstanceName):
                        val.append((k, 'reference', v))
                        continue
                    else:
                        raise Exception("Unknown type for %s" % v)
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
            eventUuid="deadbeef", requiredNetwork='1.1.1.1',
            )
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

    def testConfiguration(self):
        params = self._cimParams()
        configuration = "<configuration/>"
        self.client.configuration_cim(params, configuration=configuration)
        lastTask = self.results.configuration[-1]
        self.failIf(lastTask.status.detail, lastTask.status.detail)
        self.failUnlessEqual(
            [ (x.status.code, x.status.text) for x in self.results.update ],
            [
            ])
        taskData = lastTask.task_data.thaw()
        self.assertXMLEquals(taskData.object.response, """
<system><local_uuid>6947ee3b-4776-e11b-5d98-5b8284d4f810</local_uuid><generated_uuid>feeddeadbeef</generated_uuid></system>
""")

    def testScan(self):
        params = self._cimParams()
        self.client.survey_scan_cim(params)
        lastTask = self.results.scan[-1]
        self.failIf(lastTask.status.detail, lastTask.status.detail)
        self.failUnlessEqual(
            [ (x.status.code, x.status.text) for x in self.results.update ],
            [
            ])
        taskData = lastTask.task_data.thaw()
        self.assertXMLEquals(taskData.object.response, """
<surveys>
  <survey>
    <uuid>aa-bb-cc-dd</uuid>
  </survey>
</surveys>
""")
testsuite.main()
