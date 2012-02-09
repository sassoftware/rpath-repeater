#!/usr/bin/python
#
# Copyright (c) 2009-2012 rPath, Inc.
#
# This program is distributed under the terms of the Common Public License,
# version 1.0. A copy of this license should have been distributed with this
# source file in a file called LICENSE. If it is not present, the license
# is always available at http://www.rpath.com/permanent/licenses/CPL-1.0.
#
# This program is distributed in the hope that it will be useful, but
# without any warranty; without even the implied warranty of merchantability
# or fitness for a particular purpose. See the Common Public License for
# full details.
#

import time

import pywbem
import wbemlib

WBEMException = wbemlib.WBEMException

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

    def pollJobForCompletion(self, job, timeout = DEFAULT_TIMEOUT):
        '''
        Returns when the given job is complete, or when the specified timeout
        has passed.
        The call returns None on timeout, or the job instance otherwise.
        '''
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
            print ("jobCompleted", jobCompleted,
                instance.properties['JobState'].value)
            if jobCompleted:
                return instance
            time.sleep(1)
        return None

    def callMethodAsync(self, cimClassName, methodName, methodKwargs):
        conn = self.server.conn
        result = conn.callMethod(cimClassName, methodName,
            **methodKwargs)
        return result[1]['job']

    def handleJob(self, job, timeout=DEFAULT_TIMEOUT):
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
