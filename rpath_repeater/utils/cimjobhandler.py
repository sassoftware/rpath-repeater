#!/usr/bin/python
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


import time

import wbemlib

WBEMException = wbemlib.WBEMException
createObjectPath = wbemlib.createObjectPath

class CIMJobHandler(object):
    '''
    Class for checking jobs of all kinds.
    Exposes both asynchronous and synchronous methods.
    '''

    DEFAULT_TIMEOUT = 3600
    WAIT_TIMEOUT = 15

    def __init__(self, server, logger=None):
        self.server = server
        self._jobStates = None
        self.logger = logger

    def _normalizeValueMap(self, values, valueMap, cimType):
        typeFunc = self._toPythonType(cimType)
        return dict(zip((typeFunc(x) for x in valueMap), values))

    class WrapType(object):
        def __init__(self, typeFunc):
            self.typeFunc = typeFunc

        def __call__(self, value):
            try:
                return self.typeFunc(value)
            except ValueError:
                return value

    @classmethod
    def _toPythonType(cls, cimType):
        if cimType.startswith('int') or cimType.startswith('uint'):
            return cls.WrapType(int)
        raise RuntimeError("Unhandled type %s" % cimType)

    def _getJobStates(self, force=False):
        '''
        Get the possible job states and format them as an easy to use
        dictionary.  Will only happen once per session and result is saved as
        a class property (unless the force parameter is used.)
        '''
        if not self._jobStates or force:
            cimClass = self.server.GetClass('VAMI_UpdateConcreteJob')
            jobStates = cimClass.properties['JobState'].qualifiers

            # Turn jobStates into key/value pairs of integers and
            # descriptions so that it's easy to work with.
            self._jobStates = self._normalizeValueMap(
                jobStates['Values'].value, jobStates['ValueMap'].value)

        return self._jobStates
    jobStates = property(_getJobStates)

    def _unexpectedReturnCode(self, CIMClassName, methodName, returnCode,
        expectedReturnCode):

        returnCodes = self.server.getMethodReturnCodes(CIMClassName, methodName)
        returnMsg = returnCodes[str(returnCode)]
        raise wbemlib.WBEMUnexpectedReturnException(
            expectedReturnCode, returnCode, returnMsg)

    def isJobComplete(self, instance):
        jobState = instance.properties['JobState'].value
        # Any state >= 7 (Completed) is final
        return (jobState >= 7), instance

    def isJobSuccessful(self, instance):
        if instance is None:
            return False
        jobState = instance.properties['JobState'].value
        return jobState == 7

    def pollJobForCompletion(self, job, timeout = None):
        '''
        Returns when the given job is complete, or when the specified timeout
        has passed.
        The call returns None on timeout, or the job instance otherwise.
        '''
        if timeout is None:
            timeout = self.DEFAULT_TIMEOUT
        timeEnd = time.time() + timeout
        waited = False
        while time.time() < timeEnd:

            # If querying for the job instance fails, wait one time and try
            # again.
            try:
                instance = self.server.GetInstance(job)
            except Exception, e:
                if not waited:
                    waited = True
                    time.sleep(self.WAIT_TIMEOUT)
                    continue
                else:
                    raise e

            jobCompleted, instance = self.isJobComplete(instance)
            if jobCompleted:
                return instance
            time.sleep(1)
        return None

    def callMethodAsync(self, cimClassName, methodName, methodKwargs):
        conn = self.server.conn
        result = conn.callMethod(cimClassName, methodName,
            **methodKwargs)
        if result[0] != 4096L:
            self._unexpectedReturnCode(cimClassName, methodName,
                result[0], 4096L)

        return result[1]['job']

    def callExtrinsicMethod(self, objectPath, methodName, methodKwargs=None):
        if methodKwargs is None:
            methodKwargs = {}
        conn = self.server.conn
        result = conn.InvokeMethod(methodName, objectPath, **methodKwargs)
        return result

    def handleJob(self, job, timeout=None):
        job = self.pollJobForCompletion(job, timeout = timeout)
        if not self.isJobSuccessful(job):
            error = self.server.getError(job)
            self.log_error(error)
            raise RuntimeError('Error while executing job. The error from '
                'the managed system was: %s' % error)
        return job

    def log_error(self, error):
        if self.logger:
            self.logger.error(error)
