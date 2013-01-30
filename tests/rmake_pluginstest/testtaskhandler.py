#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


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

    def _launchRmakeJob(self, namespace, params, uuid=None):
        methodName = params.pop('method')
        key = "%s.%s" % (self.baseNamespace, methodName.replace('_', '.'))
        jobUuid = uuid
        if jobUuid is None:
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
