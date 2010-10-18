# Copyright (C) 2010 rPath, Inc.

from testrunner import testcase

from rpath_repeater import client

from rmake3.lib import uuid
from rmake3.core.types import RmakeTask, FrozenObject

class TestBase(testcase.TestCaseWithWorkDir):
    class TestTaskMixin(object):
        @classmethod
        def _test_addResult(cls, result):
            cls._results.append(result)

        def _sendStatus(self):
            self._test_addResult(self.task.freeze())

    def setUp(self):
        testcase.TestCaseWithWorkDir.setUp(self)
        self.client = Client()
        self.client.baseNamespace = self.baseNamespace
        self.client.handlerClass = self.handlerClass
        taskDispatcher = {}

        results = {}
        for k, cls in self.taskDispatcher.items():
            # Here we override methods from the real task classes with the
            # ones from the mixin
            results[k.rsplit('.', 1)[-1]] = _results = []
            clsOverrides = dict(
                    _sendStatus=self.TestTaskMixin._sendStatus,
                    _results=_results,
                )
            clsOverrides.update(self.getClassOverrides(k))
            ncls = type('Test_' + cls.__name__, (cls, self.TestTaskMixin),
                clsOverrides)
            taskDispatcher[k] = ncls
        self.client.taskDispatcher = taskDispatcher
        self.results = type("TestResults", (object, ), results)

    def getClassOverrides(self, namespace):
        return {}

class Client(client.RepeaterClient):
    _counter = 0
    @classmethod
    def _uuidgen(cls):
        cls._counter += 1
        return uuid.UUID(int=cls._counter)

    class Wchild(object):
        cfg = None

    def _launchRmakeJob(self, namespace, params):
        methodName = params.pop('method')
        key = "%s.%s" % (self.baseNamespace, methodName)
        jobUuid = self._uuidgen()
        taskUuid = jobUuid

        taskParams = self.handlerClass.initParams(params)
        # XXX
        methodArguments = params.get('methodArguments')
        zoneAddresses = [ 'localhost:8443', 'remote:8443', ]
        args = self.handlerClass._getArgs(key, taskParams, methodArguments,
            zoneAddresses)

        task = RmakeTask(taskUuid, jobUuid, key, key, FrozenObject.fromObject(args))
        taskHandlerClass = self.taskDispatcher[key]
        taskHandler = taskHandlerClass(self.Wchild(), task)
        taskHandler.run()
